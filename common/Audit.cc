// ----------------------------------------------------------------------
// File: Audit.cc
// Author: EOS Team - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include "common/Audit.hh"
#include "common/Logging.hh"

#include "proto/Audit.pb.h"
#include <google/protobuf/util/json_util.h>

#include <zstd.h>

#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <time.h>

#include <sstream>
#include <iomanip>
#include <vector>

EOSCOMMONNAMESPACE_BEGIN

namespace {
static inline time_t truncate_to_interval(time_t t, unsigned interval)
{
  if (!interval) return t;
  return t - (t % interval);
}

static inline std::string format_segment_filename(time_t t)
{
  struct tm tmval;
  localtime_r(&t, &tmval);
  char buf[64];
  // audit-YYYYmmdd-HHMMSS.zst (include seconds to support sub-minute rotations)
  if (strftime(buf, sizeof(buf), "audit-%Y%m%d-%H%M%S.zst", &tmval) == 0) {
    return "audit-unknown.zst";
  }
  return buf;
}

static inline bool mkdir_p(const std::string& path, mode_t mode)
{
  if (path.empty()) return false;

  size_t pos = 0;
  do {
    pos = path.find('/', pos + 1);
    std::string sub = path.substr(0, pos);
    if (sub.empty()) continue;
    if (mkdir(sub.c_str(), mode) == -1) {
      if (errno == EEXIST) continue;
      struct stat st;
      if (stat(sub.c_str(), &st) == 0 && S_ISDIR(st.st_mode)) continue;
      return false;
    }
  } while (pos != std::string::npos);
  return true;
}
}

Audit::Audit(const std::string& baseDirectory,
             unsigned rotationSeconds,
             int compressionLevel)
: mBaseDir(baseDirectory)
, mRotationSeconds(rotationSeconds ? rotationSeconds : 300)
, mCompressionLevel(compressionLevel)
, mZstdCctx(nullptr)
, mFd(-1)
, mCurrentSegmentStart(0)
{
}

Audit::~Audit()
{
  std::lock_guard<std::mutex> g(mMutex);
  closeWriterLocked();
}

void
Audit::setBaseDirectory(const std::string& baseDirectory)
{
  std::lock_guard<std::mutex> g(mMutex);
  if (mBaseDir == baseDirectory) {
    return;
  }
  mBaseDir = baseDirectory;
  closeWriterLocked();
}

void
Audit::audit(const eos::audit::AuditRecord& record)
{
  std::string json;
  google::protobuf::util::JsonPrintOptions opts;
  opts.add_whitespace = false;
  opts.always_print_primitive_fields = false;
  opts.preserve_proto_field_names = true;

  auto status = google::protobuf::util::MessageToJsonString(record, &json, opts);
  if (!status.ok()) {
    eos_static_err("msg=\"failed to serialize audit record to JSON\" err=%s",
                   status.ToString().c_str());
    return;
  }
  json.push_back('\n');

  const time_t now = time(nullptr);
  std::lock_guard<std::mutex> g(mMutex);
  rotateIfNeededLocked(now);

  if (mFd < 0 || !mZstdCctx) {
    // Failed to open writer; drop record
    return;
  }

  ZSTD_inBuffer in = { json.data(), json.size(), 0 };
  std::vector<char> outBuf(131072);

  while (in.pos < in.size) {
    ZSTD_outBuffer out = { outBuf.data(), outBuf.size(), 0 };
    size_t ret = ZSTD_compressStream2(reinterpret_cast<ZSTD_CCtx*>(mZstdCctx),
                                      &out, &in, ZSTD_e_continue);
    if (ZSTD_isError(ret)) {
      eos_static_err("msg=\"zstd compress error\" code=%s",
                     ZSTD_getErrorName(ret));
      break;
    }
    if (out.pos) {
      ssize_t w = write(mFd, outBuf.data(), out.pos);
      if (w < 0) {
        eos_static_err("msg=\"write error\" errno=%d err=\"%s\"",
                       errno, strerror(errno));
        break;
      }
    }
  }

  // Flush buffered data so small records are visible immediately
  {
    ZSTD_inBuffer fin = { nullptr, 0, 0 };
    size_t fret = 0;
    do {
      ZSTD_outBuffer out = { outBuf.data(), outBuf.size(), 0 };
      fret = ZSTD_compressStream2(reinterpret_cast<ZSTD_CCtx*>(mZstdCctx),
                                  &out, &fin, ZSTD_e_flush);
      if (ZSTD_isError(fret)) {
        eos_static_warning("msg=\"zstd flush error\" code=%s",
                           ZSTD_getErrorName(fret));
        break;
      }
      if (out.pos) {
        (void) ::write(mFd, outBuf.data(), out.pos);
      }
    } while (fret != 0);
  }
}

void
Audit::audit(eos::audit::Operation operation,
             const std::string& filename,
             const eos::common::VirtualIdentity& vid,
             const std::string& uuid,
             const std::string& tid,
             const std::string& svc,
             const std::string& target,
             const eos::audit::Stat* before,
             const eos::audit::Stat* after,
             const std::string& attr_name,
             const std::string& attr_before,
             const std::string& attr_after,
             const char* src_file,
             int src_line,
             const char* version)
{
  eos::audit::AuditRecord rec;
  rec.set_timestamp(time(nullptr));
  rec.set_path(filename);
  rec.set_operation(operation);
  rec.set_client_ip(vid.host);
  if (vid.name.length()) {
    rec.set_account(vid.name.c_str());
  } else if (!vid.uid_string.empty()) {
    rec.set_account(vid.uid_string);
  } else {
    rec.set_account(std::to_string(vid.uid));
  }
  rec.mutable_auth()->set_mechanism(vid.prot.length() ? vid.prot.c_str() : "local");
  if (vid.gateway) {
    (*rec.mutable_auth()->mutable_attributes())["gateway"] = "1";
  }
  if (vid.token && vid.token->Valid()) {
    rec.mutable_authorization()->add_reasons("token");
  } else {
    rec.mutable_authorization()->add_reasons("uidgid");
  }
  if (!uuid.empty() && uuid != "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx") {
    rec.set_uuid(uuid);
  }
  if (!tid.empty()) rec.set_tid(tid);
  if (!vid.app.empty()) rec.set_app(vid.app);
  if (!svc.empty()) rec.set_svc(svc);
  if (!target.empty()) rec.set_target(target);
  if (before) rec.mutable_before()->CopyFrom(*before);
  if (after) rec.mutable_after()->CopyFrom(*after);
  if (!attr_name.empty()) {
    auto* ac = rec.add_attrs();
    ac->set_name(attr_name);
    ac->set_before(attr_before);
    ac->set_after(attr_after);
  }
  if (src_file && *src_file) {
    const char* basename = strrchr(src_file, '/') ? strrchr(src_file, '/') + 1 : src_file;
    rec.set_src_file(basename);
  }
  if (src_line > 0) rec.set_src_line(static_cast<uint32_t>(src_line));
  if (version && *version) rec.set_version(version);
  audit(rec);
}

void
Audit::rotateIfNeededLocked(time_t now)
{
  const time_t seg = truncate_to_interval(now, mRotationSeconds);
  if (mFd >= 0 && mZstdCctx && seg == mCurrentSegmentStart) {
    return;
  }

  // Close current if any
  closeWriterLocked();

  // Open new
  (void)openWriterLocked(seg);
}

bool
Audit::openWriterLocked(time_t segmentStart)
{
  if (mBaseDir.empty()) {
    return false;
  }

  ensureDirectoryExistsLocked();

  const std::string filename = makeSegmentPath(segmentStart);

  mFd = ::open(filename.c_str(), O_CREAT | O_WRONLY | O_TRUNC | O_CLOEXEC, 0644);
  if (mFd < 0) {
    eos_static_err("msg=\"cannot open audit file\" path=\"%s\" errno=%d err=\"%s\"",
                   filename.c_str(), errno, strerror(errno));
    return false;
  }

  mZstdCctx = ZSTD_createCCtx();
  if (!mZstdCctx) {
    eos_static_err("msg=\"cannot create zstd context\"");
    ::close(mFd);
    mFd = -1;
    return false;
  }

  if (ZSTD_isError(ZSTD_CCtx_setParameter(reinterpret_cast<ZSTD_CCtx*>(mZstdCctx), ZSTD_c_compressionLevel, mCompressionLevel))) {
    eos_static_warning("msg=\"failed to set zstd compression level\" level=%d", mCompressionLevel);
  }

  // Ensure a valid ZSTD frame header is written so readers like zstdcat don't
  // fail on an empty, newly-rotated file. Flush pending header with empty input.
  {
    std::vector<char> outBuf(16384);
    ZSTD_inBuffer in = { nullptr, 0, 0 };
    ZSTD_outBuffer out = { outBuf.data(), outBuf.size(), 0 };
    size_t ret = ZSTD_compressStream2(reinterpret_cast<ZSTD_CCtx*>(mZstdCctx),
                                      &out, &in, ZSTD_e_flush);
    if (ZSTD_isError(ret)) {
      eos_static_warning("msg=\"zstd header flush error\" code=%s", ZSTD_getErrorName(ret));
    }
    if (out.pos) {
      (void)::write(mFd, outBuf.data(), out.pos);
    }
  }

  // Update symlink audit.zstd -> current file (best-effort)
  std::string linkPath = mBaseDir + "/audit.zstd";
  (void)::unlink(linkPath.c_str());
  (void)::symlink(filename.c_str(), linkPath.c_str());

  mCurrentSegmentStart = segmentStart;
  return true;
}

void
Audit::closeWriterLocked()
{
  if (mFd >= 0 && mZstdCctx) {
    std::vector<char> outBuf(65536);
    ZSTD_inBuffer in = { nullptr, 0, 0 };
    size_t ret = 0;
    do {
      ZSTD_outBuffer out = { outBuf.data(), outBuf.size(), 0 };
      ret = ZSTD_compressStream2(reinterpret_cast<ZSTD_CCtx*>(mZstdCctx),
                                 &out, &in, ZSTD_e_end);
      if (ZSTD_isError(ret)) {
        eos_static_err("msg=\"zstd endStream error\" code=%s",
                       ZSTD_getErrorName(ret));
        break;
      }
      if (out.pos) {
        (void)::write(mFd, outBuf.data(), out.pos);
      }
    } while (ret != 0);
  }

  if (mFd >= 0) {
    ::close(mFd);
    mFd = -1;
  }
  if (mZstdCctx) {
    ZSTD_freeCCtx(reinterpret_cast<ZSTD_CCtx*>(mZstdCctx));
    mZstdCctx = nullptr;
  }
  mCurrentSegmentStart = 0;
}

std::string
Audit::makeSegmentPath(time_t segmentStart) const
{
  std::ostringstream oss;
  oss << mBaseDir;
  if (!mBaseDir.empty() && mBaseDir.back() != '/') {
    oss << '/';
  }
  oss << format_segment_filename(segmentStart);
  return oss.str();
}

void
Audit::ensureDirectoryExistsLocked()
{
  struct stat st;
  if (stat(mBaseDir.c_str(), &st) == 0 && S_ISDIR(st.st_mode)) {
    return;
  }
  if (!mkdir_p(mBaseDir, 0755)) {
    eos_static_err("msg=\"failed to create audit directory\" dir=\"%s\" errno=%d err=\"%s\"",
                   mBaseDir.c_str(), errno, strerror(errno));
  }
}

EOSCOMMONNAMESPACE_END


