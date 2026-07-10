// EosEmbedFstFS: in-process cern-nfs VFS for the EOS FST (pNFS data server).
// Compiled into XrdEosFst, not libcernnfs — EOS FST headers are only available
// in the FST link context.

#include <cernnfs/ds_fh.hpp>
#include <cernnfs/vfs.hpp>

#include <sys/stat.h>
#include <sys/types.h>

#include <XrdOuc/XrdOucString.hh>
#include <XrdSec/XrdSecEntity.hh>
#include <XrdSfs/XrdSfsInterface.hh>

#include "common/FileId.hh"
#include "common/Fmd.hh"
#include "common/LayoutId.hh"
#include "common/SecEntity.hh"
#include "common/StringConversion.hh"
#include "fst/Config.hh"
#include "fst/XrdFstOfs.hh"
#include "fst/XrdFstOfsFile.hh"
#include "fst/filemd/FmdHandler.hh"
#include "fst/filemd/FmdMgm.hh"
#include "fst/storage/Storage.hh"
#include "proto/FileMd.pb.h"

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace cernnfs {

namespace {

constexpr uint32_t kEosEmbedDsFhType = 0xE05Fu;

eos::fst::XrdFstOfs* g_fst_ofs = nullptr;
std::unique_ptr<ds_fh::Keytab> g_ds_keytab;
std::mutex g_keytab_mu;

std::string
fst_normalize_path(const std::string& in)
{
  if (in.empty()) {
    return std::string("/");
  }

  std::string out;
  out.reserve(in.size() + 1);
  out.push_back('/');
  size_t i = 0;

  while (i < in.size()) {
    while (i < in.size() && in[i] == '/') {
      i++;
    }

    size_t start = i;

    while (i < in.size() && in[i] != '/') {
      i++;
    }

    size_t len = i - start;

    if (len == 0) {
      break;
    }

    std::string_view seg(in.c_str() + start, len);

    if (seg == ".") {
      continue;
    }

    if (seg == "..") {
      if (out.size() > 1) {
        size_t slash = out.find_last_of('/');

        if (slash > 0) {
          out.erase(slash);
        } else {
          out.erase(1);
        }
      }

      continue;
    }

    if (out.size() > 1) {
      out.push_back('/');
    }

    out.append(seg.data(), seg.size());
  }

  return out;
}

template<typename T>
Result<T>
fst_namespace_unsupported(const char* op)
{
  std::string msg = std::string("eos-embed-fst: ") + op
                    + ": namespace ops are MGM-only; route to EosEmbedMgmFS";
  return Result<T>::make_error(std::move(msg), ENOTSUP);
}

template<typename T>
Result<T>
fst_not_a_replica(const char* op)
{
  return Result<T>::make_error(
    std::string("eos-embed-fst: ") + op + ": not a replica path (expected /fsid/fid)",
    EINVAL);
}

eos::fst::XrdFstOfs*
fst_ofs()
{
  return g_fst_ofs ? g_fst_ofs : &eos::fst::gOFS;
}

std::string
keytab_path_from_env()
{
  const char* env = std::getenv("EOS_EMBED_NFS_KEYTAB");
  return (env && *env) ? env : "/etc/eos.nfs.keytab";
}

bool
ensure_keytab_loaded()
{
  std::lock_guard<std::mutex> lock(g_keytab_mu);

  if (g_ds_keytab && g_ds_keytab->size() > 0) {
    return true;
  }

  auto loaded = ds_fh::load_keytab_file(keytab_path_from_env());

  if (loaded.is_err()) {
    return false;
  }

  g_ds_keytab = std::make_unique<ds_fh::Keytab>(std::move(loaded.value()));
  return g_ds_keytab->size() > 0;
}

struct FstReplicaRef
{
  eos::common::FileSystem::fsid_t fsid = 0;
  eos::common::FileId::fileid_t fid = 0;
  std::uint64_t layout_epoch = 0;
  std::uint64_t generation = 0;
};

std::string
replica_path(eos::common::FileSystem::fsid_t fsid,
             eos::common::FileId::fileid_t fid)
{
  return std::string("/") + std::to_string(fsid) + "/"
         + eos::common::FileId::Fid2Hex(fid);
}

std::optional<FstReplicaRef>
parse_replica_path(const std::string& path)
{
  if (path == "/" || path.empty()) {
    return std::nullopt;
  }

  if (path.front() != '/') {
    return std::nullopt;
  }

  const auto slash = path.find('/', 1);

  if (slash == std::string::npos) {
    return std::nullopt;
  }

  const std::string fsid_str = path.substr(1, slash - 1);
  const std::string fid_hex = path.substr(slash + 1);

  if (fsid_str.empty() || fid_hex.empty()) {
    return std::nullopt;
  }

  char* end = nullptr;
  const long fsid = std::strtol(fsid_str.c_str(), &end, 10);

  if (end == fsid_str.c_str() || *end != '\0' || fsid <= 0) {
    return std::nullopt;
  }

  FstReplicaRef ref;
  ref.fsid = static_cast<eos::common::FileSystem::fsid_t>(fsid);
  ref.fid = eos::common::FileId::Hex2Fid(fid_hex.c_str());

  if (ref.fid == 0) {
    return std::nullopt;
  }

  return ref;
}

Result<FstReplicaRef>
decode_ds_handle(uint32_t handle_type, const std::vector<uint8_t>& handle)
{
  if (!ensure_keytab_loaded()) {
    return Result<FstReplicaRef>::make_error(
      "eos-embed-fst: DS keytab not loaded (set EOS_EMBED_NFS_KEYTAB)", ENOENT);
  }

  const bool raw_ds_fh =
    handle.size() == ds_fh::ENCODED_SIZE
    || handle_type == kEosEmbedDsFhType;

  if (!raw_ds_fh) {
    return Result<FstReplicaRef>::make_error("eos-embed-fst: unsupported handle type",
                                            EINVAL);
  }

  auto dec = ds_fh::decode_and_verify(handle, *g_ds_keytab);

  if (dec.is_err()) {
    return Result<FstReplicaRef>::make_error(dec.error(), dec.error_code());
  }

  const ds_fh::Header& hdr = dec.value();
  FstReplicaRef ref;
  ref.fid = static_cast<eos::common::FileId::fileid_t>(
    eos::common::FileId::InodeToFid(hdr.inode));
  ref.fsid = static_cast<eos::common::FileSystem::fsid_t>(hdr.flags);
  ref.layout_epoch = hdr.layout_epoch;
  ref.generation = hdr.generation;
  return Result<FstReplicaRef>(std::move(ref));
}

bool
fmd_needs_mgm_resync(const eos::common::FmdHelper* fmd)
{
  if (!fmd) {
    return true;
  }

  const auto& p = fmd->mProtoFmd;

  if (!p.has_lid() || p.lid() == 0) {
    return true;
  }

  if (p.layouterror() & eos::common::LayoutId::kUnregistered) {
    return true;
  }

  return false;
}

unsigned long
embed_effective_lid(unsigned long lid)
{
  if (lid != 0) {
    return lid;
  }

  // LayoutId::kPlain (0) leaves block-checksum bits unset; XrdFstOssFile treats
  // that as a blockxs type and fails to create the checksum object.
  return eos::common::LayoutId::GetId(eos::common::LayoutId::kPlain,
                                      eos::common::LayoutId::kAdler, 1, 0,
                                      eos::common::LayoutId::kNone);
}

Result<std::unique_ptr<eos::common::FmdHelper>>
fetch_mgm_fmd(const FstReplicaRef& ref)
{
  eos::ns::FileMdProto proto;
  const char* manager = eos::fst::gConfig.Manager.c_str();
  const int rc = eos::fst::FmdMgmHandler::GetMgmFmd(manager ? manager : "",
                                                    ref.fid, proto);

  if (rc == ENODATA) {
    return Result<std::unique_ptr<eos::common::FmdHelper>>::make_error(
      "eos-embed-fst: file not found on MGM", ENOENT);
  }

  if (rc != 0) {
    return Result<std::unique_ptr<eos::common::FmdHelper>>::make_error(
      "eos-embed-fst: failed to fetch MGM metadata", EIO);
  }

  auto fmd = std::make_unique<eos::common::FmdHelper>();
  eos::fst::FmdMgmHandler::NsFileProtoToFmd(std::move(proto), *fmd);
  fmd->mProtoFmd.set_layouterror(fmd->LayoutError(ref.fsid));
  return Result<std::unique_ptr<eos::common::FmdHelper>>(std::move(fmd));
}

bool
replica_data_file_exists(const FstReplicaRef& ref)
{
  eos::fst::XrdFstOfs* ofs = fst_ofs();

  if (!ofs || !ofs->Storage) {
    return false;
  }

  const std::string prefix = ofs->Storage->GetStoragePath(ref.fsid);

  if (prefix.empty()) {
    return false;
  }

  const std::string path = eos::common::FileId::FidPrefix2FullPath(
    eos::common::FileId::Fid2Hex(ref.fid).c_str(), prefix.c_str());
  struct stat st {};

  return !path.empty() && ::stat(path.c_str(), &st) == 0 && S_ISREG(st.st_mode);
}

Result<std::unique_ptr<eos::common::FmdHelper>>
fetch_embed_open_fmd(const FstReplicaRef& ref, bool is_write)
{
  eos::fst::XrdFstOfs* ofs = fst_ofs();

  if (!ofs || !ofs->mFmdHandler) {
    return Result<std::unique_ptr<eos::common::FmdHelper>>::make_error(
      "eos-embed-fst: FmdHandler not configured", ENXIO);
  }

  auto fmd = ofs->mFmdHandler->LocalGetFmd(ref.fid, ref.fsid, true, false);

  if (fmd && !fmd_needs_mgm_resync(fmd.get())) {
    return Result<std::unique_ptr<eos::common::FmdHelper>>(std::move(fmd));
  }

  // Do not ResyncMgm/create local FMD before the data file exists: FMD is
  // stored as an xattr on the replica and Commit fails with "file not
  // existing".  XrdFstOfsFile::open creates the file first, then resyncs.
  auto mgm_fmd = fetch_mgm_fmd(ref);

  if (mgm_fmd.is_err()) {
    return mgm_fmd;
  }

  const auto layout_err = mgm_fmd.value()->mProtoFmd.layouterror();

  if (is_write &&
      (layout_err & eos::common::LayoutId::kUnregistered) != 0) {
    return Result<std::unique_ptr<eos::common::FmdHelper>>::make_error(
      "eos-embed-fst: replica not registered on MGM", ENOENT);
  }

  if (is_write && (layout_err & eos::common::LayoutId::kOrphan) != 0) {
    return Result<std::unique_ptr<eos::common::FmdHelper>>::make_error(
      "eos-embed-fst: file layout missing on MGM", ENOENT);
  }

  return mgm_fmd;
}

Result<std::unique_ptr<eos::common::FmdHelper>>
load_fmd(const FstReplicaRef& ref, bool resync_if_missing = true)
{
  eos::fst::XrdFstOfs* ofs = fst_ofs();

  if (!ofs || !ofs->mFmdHandler) {
    return Result<std::unique_ptr<eos::common::FmdHelper>>::make_error(
      "eos-embed-fst: FmdHandler not configured", ENXIO);
  }

  auto fmd = ofs->mFmdHandler->LocalGetFmd(ref.fid, ref.fsid, true, false);

  if (fmd && !fmd_needs_mgm_resync(fmd.get())) {
    return Result<std::unique_ptr<eos::common::FmdHelper>>(std::move(fmd));
  }

  if (resync_if_missing && fmd_needs_mgm_resync(fmd.get()) &&
      replica_data_file_exists(ref)) {
    const char* manager = eos::fst::gConfig.Manager.c_str();

    if (ofs->mFmdHandler->ResyncMgm(ref.fsid, ref.fid, manager)) {
      fmd = ofs->mFmdHandler->LocalGetFmd(ref.fid, ref.fsid, true, false);
    }
  }

  if (fmd && !fmd_needs_mgm_resync(fmd.get())) {
    return Result<std::unique_ptr<eos::common::FmdHelper>>(std::move(fmd));
  }

  return fetch_mgm_fmd(ref);
}

XrdSecEntity*
embed_fst_client()
{
  static XrdSecEntity client{};
  static bool initialized = false;

  if (!initialized) {
    std::strncpy(client.prot, "sss", sizeof(client.prot) - 1);
    client.prot[sizeof(client.prot) - 1] = '\0';
    client.name = const_cast<char*>("eos");
    client.host = const_cast<char*>("localhost");
    client.tident = const_cast<char*>("cern-nfs:0@localhost");
    initialized = true;
  }

  return &client;
}

std::string
embed_ns_path(const FstReplicaRef& ref)
{
  return std::string("/.fxid:") + eos::common::FileId::Fid2Hex(ref.fid);
}

std::string
build_embed_open_opaque(const FstReplicaRef& ref,
                        const eos::common::FmdHelper* fmd,
                        bool is_write)
{
  uid_t uid = 0;
  gid_t gid = 0;
  unsigned long lid = 0;
  uint64_t cid = 0;

  if (fmd) {
    const auto& p = fmd->mProtoFmd;
    uid = p.has_uid() ? p.uid() : 0;
    gid = p.has_gid() ? p.gid() : 0;
    lid = p.has_lid() ? p.lid() : 0;
    cid = p.has_cid() ? p.cid() : 0;
  }

  lid = embed_effective_lid(lid);
  XrdOucString cap;
  cap = "eos.embed=1";
  cap += "&mgm.access=";
  cap += is_write ? "write" : "read";
  cap += "&mgm.ruid=";
  cap += static_cast<int>(uid);
  cap += "&mgm.rgid=";
  cap += static_cast<int>(gid);
  cap += "&mgm.uid=99";
  cap += "&mgm.gid=99";
  cap += "&mgm.fid=";
  cap += eos::common::FileId::Fid2Hex(ref.fid).c_str();
  cap += "&mgm.lid=";
  cap += static_cast<int>(lid);
  cap += "&mgm.cid=";
  XrdOucString cid_str;
  eos::common::StringConversion::GetSizeString(cid_str, cid);
  cap += cid_str.c_str();
  cap += "&mgm.manager=";
  cap += eos::fst::gConfig.Manager.c_str();
  cap += "&mgm.fsid=";
  cap += static_cast<int>(ref.fsid);
  cap += "&mgm.sec=";
  cap += eos::common::SecEntity::ToKey(embed_fst_client(), "cern-nfs").c_str();

  if (is_write) {
    cap += "&mgm.bookingsize=1073741824";
  }

  return std::string(cap.c_str());
}

Result<std::unique_ptr<eos::fst::XrdFstOfsFile>>
open_fst_ofs_file(const FstReplicaRef& ref, bool is_write)
{
  auto fmd_res = fetch_embed_open_fmd(ref, is_write);

  if (fmd_res.is_err()) {
    return Result<std::unique_ptr<eos::fst::XrdFstOfsFile>>::make_error(
      fmd_res.error(), fmd_res.error_code());
  }

  std::unique_ptr<eos::common::FmdHelper> fmd_holder = std::move(fmd_res.value());
  const eos::common::FmdHelper* fmd = fmd_holder.get();

  auto file = std::make_unique<eos::fst::XrdFstOfsFile>("cern-nfs");
  const std::string ns_path = embed_ns_path(ref);
  const std::string opaque = build_embed_open_opaque(ref, fmd, is_write);
  const XrdSfsFileOpenMode mode =
    is_write ? static_cast<XrdSfsFileOpenMode>(SFS_O_RDWR | SFS_O_CREAT)
             : SFS_O_RDONLY;

  if (file->open(ns_path.c_str(), mode, 0644, embed_fst_client(),
                 opaque.c_str()) != SFS_OK) {
    const int ec = errno ? errno : EIO;
    return Result<std::unique_ptr<eos::fst::XrdFstOfsFile>>::make_error(
      "eos-embed-fst: XrdFstOfsFile::open failed", ec);
  }

  return Result<std::unique_ptr<eos::fst::XrdFstOfsFile>>(std::move(file));
}

SystemTime
proto_time(std::uint32_t sec, std::uint32_t nsec)
{
  using namespace std::chrono;
  return system_clock::time_point(seconds(sec)) + nanoseconds(nsec);
}

uint64_t
fmd_size(const eos::common::FmdHelper& fmd)
{
  const auto& p = fmd.mProtoFmd;

  if (p.has_disksize() && p.disksize() != eos::common::FmdHelper::UNDEF) {
    return p.disksize();
  }

  if (p.has_mgmsize() && p.mgmsize() != eos::common::FmdHelper::UNDEF) {
    return p.mgmsize();
  }

  if (p.has_size() && p.size() != eos::common::FmdHelper::UNDEF) {
    return p.size();
  }

  return 0;
}

FileMetadata
fmd_to_metadata(const eos::common::FmdHelper& fmd, const FstReplicaRef& ref)
{
  const auto& p = fmd.mProtoFmd;
  FileMetadata m{};
  m.size = fmd_size(fmd);
  m.file_id = ref.fid;
  m.device_id = ref.fsid;
  m.mode = S_IFREG | 0644;
  m.num_links = 1;
  m.owner_uid = p.has_uid() ? p.uid() : 0;
  m.owner_gid = p.has_gid() ? p.gid() : 0;

  if (p.has_atime()) {
    m.access_time = proto_time(p.atime(), p.has_atime_ns() ? p.atime_ns() : 0);
  }

  if (p.has_mtime()) {
    m.modify_time = proto_time(p.mtime(), p.has_mtime_ns() ? p.mtime_ns() : 0);
  }

  if (p.has_ctime()) {
    m.create_time = proto_time(p.ctime(), p.has_ctime_ns() ? p.ctime_ns() : 0);
  }

  m.is_directory = false;
  m.is_regular_file = true;
  m.is_symlink = false;
  return m;
}

FileMetadata
root_metadata()
{
  FileMetadata m{};
  m.mode = S_IFDIR | 0755;
  m.num_links = 2;
  m.is_directory = true;
  m.is_regular_file = false;
  return m;
}

class FstOfsStreambuf final : public std::streambuf
{
public:
  explicit FstOfsStreambuf(std::unique_ptr<eos::fst::XrdFstOfsFile> file)
      : file_(std::move(file)), pos_(0)
  {
  }

  ~FstOfsStreambuf() override
  {
    if (file_) {
      file_->close();
    }
  }

protected:
  std::streamsize xsgetn(char* s, std::streamsize n) override
  {
    if (!file_ || n <= 0) {
      return 0;
    }

    const auto got = file_->read(pos_, s, static_cast<XrdSfsXferSize>(n));

    if (got < 0) {
      return -1;
    }

    pos_ += static_cast<XrdSfsFileOffset>(got);
    return static_cast<std::streamsize>(got);
  }

  std::streamsize xsputn(const char* s, std::streamsize n) override
  {
    if (!file_ || n <= 0) {
      return 0;
    }

    const auto wrote =
      file_->write(pos_, s, static_cast<XrdSfsXferSize>(n));

    if (wrote < 0) {
      return -1;
    }

    pos_ += static_cast<XrdSfsFileOffset>(wrote);
    return static_cast<std::streamsize>(wrote);
  }

  int sync() override
  {
    return 0;
  }

  std::streampos seekoff(std::streamoff off, std::ios_base::seekdir dir,
                         std::ios_base::openmode /*which*/) override
  {
    if (!file_) {
      return std::streampos(std::streamoff(-1));
    }

    XrdSfsFileOffset base = pos_;
    switch (dir) {
    case std::ios_base::beg:
      base = 0;
      break;
    case std::ios_base::cur:
      break;
    case std::ios_base::end:
      // pNFS READ/WRITE only seek from the beginning; size lookup is not
      // available cheaply on the hot path here.
      return std::streampos(std::streamoff(-1));
    default:
      return std::streampos(std::streamoff(-1));
    }

    const XrdSfsFileOffset newpos = base + static_cast<XrdSfsFileOffset>(off);
    if (newpos < 0) {
      return std::streampos(std::streamoff(-1));
    }

    pos_ = newpos;
    return std::streampos(static_cast<std::streamoff>(pos_));
  }

  std::streampos seekpos(std::streampos sp,
                         std::ios_base::openmode which) override
  {
    if (sp == std::streampos(std::streamoff(-1))) {
      return std::streampos(std::streamoff(-1));
    }

    return seekoff(sp, std::ios_base::beg, which);
  }

private:
  std::unique_ptr<eos::fst::XrdFstOfsFile> file_;
  XrdSfsFileOffset pos_;
};

class FstIstream final : public std::istream
{
public:
  FstIstream(std::shared_ptr<FstOfsStreambuf> buf)
      : std::istream(buf.get()), mBuf(std::move(buf))
  {
  }

private:
  std::shared_ptr<FstOfsStreambuf> mBuf;
};

class FstOstream final : public std::ostream
{
public:
  FstOstream(std::shared_ptr<FstOfsStreambuf> buf)
      : std::ostream(buf.get()), mBuf(std::move(buf))
  {
  }

private:
  std::shared_ptr<FstOfsStreambuf> mBuf;
};

class FstIostream final : public std::iostream
{
public:
  FstIostream(std::shared_ptr<FstOfsStreambuf> buf)
      : std::iostream(buf.get()), mBuf(std::move(buf))
  {
  }

private:
  std::shared_ptr<FstOfsStreambuf> mBuf;
};

} // namespace

bool
EosEmbedFstFS::configure_fst(void* fst_ofs)
{
  g_fst_ofs = static_cast<eos::fst::XrdFstOfs*>(fst_ofs);

  if (!g_fst_ofs) {
    g_fst_ofs = &eos::fst::gOFS;
  }

  if (!g_fst_ofs || !g_fst_ofs->Storage) {
    return false;
  }

  if (!ensure_keytab_loaded()) {
    return false;
  }

  return true;
}

std::shared_ptr<EosEmbedFstFS>
EosEmbedFstFS::open_root(const std::string& replica_root)
{
  if (!fst_ofs() || !fst_ofs()->Storage) {
    return nullptr;
  }

  return std::shared_ptr<EosEmbedFstFS>(
    new EosEmbedFstFS(fst_normalize_path(replica_root)));
}

EosEmbedFstFS::EosEmbedFstFS(std::string path)
    : path_(std::move(path))
{
}

std::string
EosEmbedFstFS::filename() const
{
  if (path_ == "/" || path_.empty()) {
    return std::string();
  }

  auto pos = path_.find_last_of('/');
  return pos == std::string::npos ? path_ : path_.substr(pos + 1);
}

std::shared_ptr<VfsPath>
EosEmbedFstFS::parent() const
{
  if (path_ == "/" || path_.empty()) {
    return nullptr;
  }

  auto pos = path_.find_last_of('/');
  std::string parent_path = (pos == 0 || pos == std::string::npos)
                              ? std::string("/")
                              : path_.substr(0, pos);
  return std::shared_ptr<EosEmbedFstFS>(new EosEmbedFstFS(std::move(parent_path)));
}

std::shared_ptr<VfsPath>
EosEmbedFstFS::join(const std::string& path) const
{
  std::string joined;

  if (!path.empty() && path.front() == '/') {
    joined = path;
  } else {
    joined = path_;

    if (joined.empty() || joined.back() != '/') {
      joined.push_back('/');
    }

    joined += path;
  }

  return std::shared_ptr<EosEmbedFstFS>(
    new EosEmbedFstFS(fst_normalize_path(joined)));
}

std::string
EosEmbedFstFS::as_str() const
{
  return path_;
}

Result<FileMetadata>
EosEmbedFstFS::metadata() const
{
  if (path_ == "/" || path_.empty()) {
    return Result<FileMetadata>(root_metadata());
  }

  auto ref = parse_replica_path(path_);

  if (!ref) {
    return fst_not_a_replica<FileMetadata>("metadata");
  }

  auto fmd = load_fmd(*ref);

  if (fmd.is_err()) {
    return Result<FileMetadata>::make_error(fmd.error(), fmd.error_code());
  }

  return Result<FileMetadata>(fmd_to_metadata(*fmd.value(), *ref));
}

Result<bool>
EosEmbedFstFS::exists() const
{
  if (path_ == "/" || path_.empty()) {
    return Result<bool>(true);
  }

  auto md = metadata();

  if (md.is_ok()) {
    return Result<bool>(true);
  }

  if (md.error_code() == ENOENT) {
    return Result<bool>(false);
  }

  return Result<bool>::make_error(md.error(), md.error_code());
}

Result<std::vector<uint8_t>>
EosEmbedFstFS::read() const
{
  auto ref = parse_replica_path(path_);

  if (!ref) {
    return fst_not_a_replica<std::vector<uint8_t>>("read");
  }

  auto fmd_res = load_fmd(*ref);

  if (fmd_res.is_err()) {
    return Result<std::vector<uint8_t>>::make_error(fmd_res.error(),
                                                    fmd_res.error_code());
  }

  auto file = open_fst_ofs_file(*ref, false);

  if (file.is_err()) {
    return Result<std::vector<uint8_t>>::make_error(file.error(), file.error_code());
  }

  const auto md = fmd_to_metadata(*fmd_res.value(), *ref);
  std::vector<uint8_t> buf;
  buf.resize(static_cast<size_t>(md.size));

  if (md.size == 0) {
    file.value()->close();
    return Result<std::vector<uint8_t>>(std::move(buf));
  }

  const auto got = file.value()->read(
    0, reinterpret_cast<char*>(buf.data()),
    static_cast<XrdSfsXferSize>(buf.size()));
  file.value()->close();

  if (got < 0) {
    return Result<std::vector<uint8_t>>::make_error("eos-embed-fst: read failed", EIO);
  }

  if (static_cast<uint64_t>(got) < md.size) {
    buf.resize(static_cast<size_t>(got));
  }

  return Result<std::vector<uint8_t>>(std::move(buf));
}

Result<void>
EosEmbedFstFS::write(const std::vector<uint8_t>& data)
{
  auto ref = parse_replica_path(path_);

  if (!ref) {
    return fst_not_a_replica<void>("write");
  }

  auto file = open_fst_ofs_file(*ref, true);

  if (file.is_err()) {
    return Result<void>::make_error(file.error(), file.error_code());
  }

  if (!data.empty()) {
    const auto wrote = file.value()->write(
      0, reinterpret_cast<const char*>(data.data()),
      static_cast<XrdSfsXferSize>(data.size()));

    if (wrote < 0 || static_cast<size_t>(wrote) != data.size()) {
      file.value()->close();
      return Result<void>::make_error("eos-embed-fst: write failed", EIO);
    }
  }

  file.value()->close();
  return Result<void>();
}

Result<std::shared_ptr<std::istream>>
EosEmbedFstFS::open_read() const
{
  auto ref = parse_replica_path(path_);

  if (!ref) {
    return fst_not_a_replica<std::shared_ptr<std::istream>>("open_read");
  }

  auto file = open_fst_ofs_file(*ref, false);

  if (file.is_err()) {
    return Result<std::shared_ptr<std::istream>>::make_error(file.error(),
                                                             file.error_code());
  }

  auto buf = std::make_shared<FstOfsStreambuf>(std::move(file.value()));
  return Result<std::shared_ptr<std::istream>>(
    std::make_shared<FstIstream>(std::move(buf)));
}

Result<std::shared_ptr<std::ostream>>
EosEmbedFstFS::open_write()
{
  auto ref = parse_replica_path(path_);

  if (!ref) {
    return fst_not_a_replica<std::shared_ptr<std::ostream>>("open_write");
  }

  auto file = open_fst_ofs_file(*ref, true);

  if (file.is_err()) {
    return Result<std::shared_ptr<std::ostream>>::make_error(file.error(),
                                                             file.error_code());
  }

  auto buf = std::make_shared<FstOfsStreambuf>(std::move(file.value()));
  return Result<std::shared_ptr<std::ostream>>(
    std::make_shared<FstOstream>(std::move(buf)));
}

Result<std::shared_ptr<std::iostream>>
EosEmbedFstFS::open_read_write()
{
  auto ref = parse_replica_path(path_);

  if (!ref) {
    return fst_not_a_replica<std::shared_ptr<std::iostream>>("open_read_write");
  }

  auto file = open_fst_ofs_file(*ref, true);

  if (file.is_err()) {
    return Result<std::shared_ptr<std::iostream>>::make_error(file.error(),
                                                              file.error_code());
  }

  auto buf = std::make_shared<FstOfsStreambuf>(std::move(file.value()));
  return Result<std::shared_ptr<std::iostream>>(
    std::make_shared<FstIostream>(std::move(buf)));
}

Result<void>
EosEmbedFstFS::set_size(uint64_t new_size)
{
  auto ref = parse_replica_path(path_);

  if (!ref) {
    return fst_not_a_replica<void>("set_size");
  }

  auto file = open_fst_ofs_file(*ref, true);

  if (file.is_err()) {
    return Result<void>::make_error(file.error(), file.error_code());
  }

  if (file.value()->truncate(static_cast<XrdSfsFileOffset>(new_size)) != SFS_OK) {
    file.value()->close();
    return Result<void>::make_error("eos-embed-fst: truncate failed", EIO);
  }

  file.value()->close();
  return Result<void>();
}

Result<uint32_t>
EosEmbedFstFS::access(uint32_t requested) const
{
  // MGM already authorized access when issuing the pNFS layout.
  return Result<uint32_t>(requested);
}

bool
EosEmbedFstFS::supports_pnfs_ds() const
{
  return true;
}

Result<KernelFh>
EosEmbedFstFS::kernel_handle() const
{
  auto ref = parse_replica_path(path_);

  if (!ref) {
    return fst_not_a_replica<KernelFh>("kernel_handle");
  }

  if (!ensure_keytab_loaded()) {
    return Result<KernelFh>::make_error(
      "eos-embed-fst: DS keytab not loaded (set EOS_EMBED_NFS_KEYTAB)", ENOENT);
  }

  ds_fh::Header hdr;
  hdr.inode = eos::common::FileId::FidToInode(
    static_cast<unsigned long long>(ref->fid));
  hdr.flags = static_cast<std::uint16_t>(ref->fsid);
  hdr.generation = ref->generation;
  hdr.layout_epoch = ref->layout_epoch;

  auto enc = ds_fh::encode(hdr, *g_ds_keytab);

  if (enc.is_err()) {
    return Result<KernelFh>::make_error(enc.error(), enc.error_code());
  }

  KernelFh kfh;
  kfh.handle_type = kEosEmbedDsFhType;
  kfh.handle = std::move(enc.value());
  return Result<KernelFh>(std::move(kfh));
}

Result<FileMetadata>
EosEmbedFstFS::metadata_from_kernel_handle(uint32_t handle_type,
                                           const std::vector<uint8_t>& handle) const
{
  auto ref = decode_ds_handle(handle_type, handle);

  if (ref.is_err()) {
    return Result<FileMetadata>::make_error(ref.error(), ref.error_code());
  }

  auto fmd = load_fmd(ref.value());

  if (fmd.is_err()) {
    return Result<FileMetadata>::make_error(fmd.error(), fmd.error_code());
  }

  return Result<FileMetadata>(fmd_to_metadata(*fmd.value(), ref.value()));
}

Result<std::string>
EosEmbedFstFS::path_from_kernel_handle(uint32_t handle_type,
                                       const std::vector<uint8_t>& handle) const
{
  auto ref = decode_ds_handle(handle_type, handle);

  if (ref.is_err()) {
    return Result<std::string>::make_error(ref.error(), ref.error_code());
  }

  return Result<std::string>(replica_path(ref.value().fsid, ref.value().fid));
}

Result<void>
EosEmbedFstFS::create_file()
{
  return fst_namespace_unsupported<void>("create_file");
}

Result<void>
EosEmbedFstFS::create_dir()
{
  return fst_namespace_unsupported<void>("create_dir");
}

Result<void>
EosEmbedFstFS::create_dir_all()
{
  return fst_namespace_unsupported<void>("create_dir_all");
}

Result<void>
EosEmbedFstFS::remove_file()
{
  return fst_namespace_unsupported<void>("remove_file");
}

Result<void>
EosEmbedFstFS::remove_dir()
{
  return fst_namespace_unsupported<void>("remove_dir");
}

Result<std::vector<DirEntry>>
EosEmbedFstFS::read_dir() const
{
  return fst_namespace_unsupported<std::vector<DirEntry>>("read_dir");
}

Result<void>
EosEmbedFstFS::rename_to(std::shared_ptr<VfsPath> /*new_parent*/,
                         const std::string& /*new_name*/)
{
  return fst_namespace_unsupported<void>("rename_to");
}

Result<void>
EosEmbedFstFS::set_mode(uint32_t /*mode*/)
{
  return fst_namespace_unsupported<void>("set_mode");
}

Result<void>
EosEmbedFstFS::set_times(const SystemTime* /*atime*/, const SystemTime* /*mtime*/)
{
  return fst_namespace_unsupported<void>("set_times");
}

Result<void>
EosEmbedFstFS::set_owner(uid_t /*uid*/, gid_t /*gid*/)
{
  return fst_namespace_unsupported<void>("set_owner");
}

Result<void>
EosEmbedFstFS::link_from(std::shared_ptr<VfsPath> /*source*/,
                         const std::string& /*new_name*/)
{
  return fst_namespace_unsupported<void>("link_from");
}

Result<void>
EosEmbedFstFS::create_symlink(const std::string& /*new_name*/,
                              const std::string& /*target*/)
{
  return fst_namespace_unsupported<void>("create_symlink");
}

Result<std::string>
EosEmbedFstFS::read_symlink_target() const
{
  return fst_namespace_unsupported<std::string>("read_symlink_target");
}

Result<std::vector<uint8_t>>
EosEmbedFstFS::get_acl() const
{
  return fst_namespace_unsupported<std::vector<uint8_t>>("get_acl");
}

Result<void>
EosEmbedFstFS::set_acl(const std::vector<uint8_t>& /*acl_blob*/)
{
  return fst_namespace_unsupported<void>("set_acl");
}

Result<std::string>
EosEmbedFstFS::readlink_from_kernel_handle(uint32_t /*handle_type*/,
                                           const std::vector<uint8_t>& /*handle*/) const
{
  return fst_namespace_unsupported<std::string>("readlink_from_kernel_handle");
}

} // namespace cernnfs
