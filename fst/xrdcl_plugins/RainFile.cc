//------------------------------------------------------------------------------
// File RainFile.cc
// Author Elvin Sindrilaru <esindril@cern.ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2014 CERN/Switzerland                                  *
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

#include "RainFile.hh"
#include "common/StringConversion.hh"
#include "fst/checksum/ChecksumPlugins.hh"
#include "fst/layout/RaidDpLayout.hh"
#include "fst/layout/RainMetaLayout.hh"
#include "fst/layout/ReedSLayout.hh"
#include <XrdCl/XrdClFileSystem.hh>
#include <XrdOuc/XrdOucEnv.hh>
#include <sys/time.h>

using namespace eos::common;
using namespace eos::fst;

namespace {
//------------------------------------------------------------------------------
// Convert XrdCl access mode to POSIX permission bits
//------------------------------------------------------------------------------
mode_t
MapModeXrdCl2Posix(Access::Mode mode)
{
  static const std::vector<std::pair<Access::Mode, mode_t>> mapping{
      {Access::UR, S_IRUSR}, {Access::UW, S_IWUSR}, {Access::UX, S_IXUSR},
      {Access::GR, S_IRGRP}, {Access::GW, S_IWGRP}, {Access::GX, S_IXGRP},
      {Access::OR, S_IROTH}, {Access::OW, S_IWOTH}, {Access::OX, S_IXOTH}};
  mode_t posix_mode = 0;

  for (const auto& [xrd_mode, pmode] : mapping) {
    if (mode & xrd_mode) {
      posix_mode |= pmode;
    }
  }

  return posix_mode;
}

//------------------------------------------------------------------------------
// Read callback used for rescanning the file checksum through the layout
//------------------------------------------------------------------------------
int
LayoutReadCB(CheckSum::ReadCallBack::callback_data_t* cbd)
{
  return static_cast<int>(static_cast<RainMetaLayout*>(cbd->caller)
                              ->Read(cbd->offset, cbd->buffer, cbd->size));
}
} // namespace

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
RainFile::RainFile():
  mIsOpen(false),
  pFile(0),
  pRainFile(0)
{
  eos_debug("calling constructor");
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
RainFile::~RainFile()
{
  eos_debug("calling destructor");

  // File written in PIO mode but never closed - clean up the stripes and the
  // namespace entry, the same as a gateway would do on client disconnect
  if (mIsOpen && mIsPioWrite && pRainFile) {
    eos_warning("%s", "msg=\"PIO written file not closed, clean up\"");
    (void)pRainFile->Remove();
    DropFromMgm();
  }

  if (pFile) {
    delete pFile;
  }

  if (pRainFile) {
    delete pRainFile;
  }
}


//------------------------------------------------------------------------------
// Open
//------------------------------------------------------------------------------
XRootDStatus
RainFile::Open(const std::string& url,
               OpenFlags::Flags flags,
               Access::Mode mode,
               ResponseHandler* handler,
               uint16_t timeout)
{
  eos_debug("url=%s", url.c_str());
  XRootDStatus st;

  if (mIsOpen) {
    st = XRootDStatus(stError, errInvalidOp);
    return st;
  }

  mUrl = url;

  // Only creation or overwrite of files is supported for PIO writes. If the
  // MGM refuses the PIO open e.g. anonymous client or non-RAIN layout then
  // we fall back to the gateway write.
  const bool is_write = (flags & (OpenFlags::Update | OpenFlags::Write | OpenFlags::New |
                                  OpenFlags::Delete));
  const bool is_pio_write = is_write && (flags & (OpenFlags::New | OpenFlags::Delete));

  if (is_pio_write) {
    st = OpenPio(url, true, flags, mode);

    if (st.IsOK()) {
      mIsOpen = true;
      XRootDStatus* ret_st = new XRootDStatus(st);
      handler->HandleResponse(ret_st, 0);
      return st;
    }

    // Fall back to the gateway mode, no namespace entry is left behind
    eos_warning("msg=\"failed PIO write open, fall back to gateway write\" "
                "url=\"%s\" err=\"%s\"",
                url.c_str(), st.ToString().c_str());
    delete pRainFile;
    pRainFile = nullptr;
    mIsPioWrite = false;
    mChecksum.reset();
    mPioCapability.clear();
  } else if (!is_write && ((flags & OpenFlags::Flags::Read) == OpenFlags::Flags::Read)) {
    // For reading try PIO mode
    st = OpenPio(url, false, flags, mode);

    if (st.IsOK()) {
      mIsOpen = true;
      XRootDStatus* ret_st = new XRootDStatus(st);
      handler->HandleResponse(ret_st, 0);
      return st;
    }

    // Fall back to the gateway mode
    eos_warning("msg=\"failed PIO read open, fall back to gateway read\" "
                "url=\"%s\" err=\"%s\"",
                url.c_str(), st.ToString().c_str());
    delete pRainFile;
    pRainFile = nullptr;
  }

  // Normal XrdCl file access
  pFile = new XrdCl::File(false);
  st = pFile->Open(url, flags, mode, handler, timeout);

  if (st.IsOK()) {
    mIsOpen = true;
  }

  return st;
}

//------------------------------------------------------------------------------
// Open file in PIO mode
//------------------------------------------------------------------------------
XRootDStatus
RainFile::OpenPio(const std::string& url, bool is_write, OpenFlags::Flags flags,
                  Access::Mode mode)
{
  XRootDStatus st;
  XrdCl::Buffer arg;
  XrdCl::Buffer* raw_response = nullptr;
  std::string fpath = url;
  size_t spos = fpath.rfind("//");

  if (spos == std::string::npos) {
    eos_err("msg=\"invalid url\" url=\"%s\"", url.c_str());
    return XRootDStatus(stError, errInvalidArgs, 0, "invalid url");
  }

  fpath.erase(0, spos + 1);
  std::string request = fpath;
  request += ((fpath.find('?') == std::string::npos) ? "?" : "&");
  request += "mgm.pcmd=open";

  if (is_write) {
    // Overwrite (create or truncate) or exclusive creation of the file
    std::ostringstream oss;
    oss << "&eos.client.openflags=" << ((flags & OpenFlags::Delete) ? "rw,tr" : "rw,cr")
        << "&eos.client.openmode=" << std::oct << MapModeXrdCl2Posix(mode);

    if (flags & OpenFlags::MakePath) {
      oss << "&eos.client.mkpath=1";
    }

    request += oss.str();
  }

  arg.FromString(request);
  std::string endpoint = url;
  endpoint.erase(spos + 1);
  URL Url(endpoint);
  XrdCl::FileSystem fs(Url);
  st = fs.Query(QueryCode::OpaqueFile, arg, raw_response);
  std::unique_ptr<XrdCl::Buffer> response(raw_response);

  if (!st.IsOK()) {
    eos_err("msg=\"error while doing PIO open request\" url=\"%s\" err=\"%s\"",
            url.c_str(), st.ToString().c_str());
    return XRootDStatus(stError, errNotImplemented, 0, "error PIO open request");
  }

  // Parse output
  XrdOucString tag;
  XrdOucString stripePath;
  std::vector<std::string> stripeUrls;
  XrdOucString origResponse = response->ToString().c_str();
  XrdOucString stringOpaque = origResponse;
  // Add the eos.app=rainplugin tag to all future PIO open requests
  origResponse += "&eos.app=rainplugin";

  while (stringOpaque.replace("?", "&")) {
  }

  while (stringOpaque.replace("&&", "&")) {
  }

  XrdOucEnv openOpaque(stringOpaque.c_str());
  const char* opaqueInfo = strstr(origResponse.c_str(), "&mgm.logid");

  if (!opaqueInfo) {
    eos_err("%s", "msg=\"no opaque info\"");
    return XRootDStatus(stError, errDataError, 0, "no opaque info");
  }

  opaqueInfo += 1;
  LayoutId::layoutid_t layout = openOpaque.GetInt("mgm.lid");

  for (unsigned int i = 0; i <= eos::common::LayoutId::GetStripeNumber(layout); i++) {
    tag = "pio.";
    tag += static_cast<int>(i);
    const char* stripe_host = openOpaque.Get(tag.c_str());

    if (!stripe_host) {
      eos_err("msg=\"missing stripe location\" tag=%s", tag.c_str());
      return XRootDStatus(stError, errDataError, 0, "missing stripe location");
    }

    stripePath = "root://";
    stripePath += stripe_host;
    stripePath += "/";
    stripePath += fpath.c_str();
    stripeUrls.push_back(stripePath.c_str());
  }

  if (is_write) {
    mMgmEndpoint = endpoint;
    mMgmLogId = (openOpaque.Get("mgm.logid") ? openOpaque.Get("mgm.logid") : "");
    std::vector<std::string> tokens;
    eos::common::StringConversion::Tokenize(stringOpaque.c_str(), tokens, "&");

    for (const auto& token : tokens) {
      if (token.find("cap.") == 0) {
        mPioCapability += "&";
        mPioCapability += token;
      }
    }

    if (mPioCapability.empty()) {
      eos_err("%s", "msg=\"missing capability in PIO write response\"");
      return XRootDStatus(stError, errDataError, 0, "missing capability");
    }

    SetLogId(mMgmLogId.c_str());
    // From now on the namespace entry exists and must be cleaned on failure
    mIsPioWrite = true;
  }

  if (LayoutId::GetLayoutType(layout) == LayoutId::kRaidDP) {
    pRainFile = new RaidDpLayout(NULL, layout, NULL, NULL, "", NULL);
  } else if ((LayoutId::IsRain(layout))) {
    try {
      pRainFile = new ReedSLayout(NULL, layout, NULL, NULL, "", NULL);
    } catch (const std::runtime_error& e) {
      pRainFile = nullptr;
    }
  } else {
    eos_warning("%s", "msg=\"unsupported PIO layout\"");
    st = XRootDStatus(stError, errNotSupported, 0, "unsupported PIO layout");
  }

  if (st.IsOK() && !pRainFile) {
    eos_err("%s", "msg=\"failed to create RAIN file object\"");
    st = XRootDStatus(stError, errInternal, 0, "no RAIN file allocated");
  }

  if (st.IsOK()) {
    XrdSfsFileOpenMode sfs_flags = SFS_O_RDONLY;
    mode_t sfs_mode = 0;

    if (is_write) {
      // Stripes are served by RAIN layouts at the FSTs and the local files are
      // always created or truncated, the same as for a gateway write
      pRainFile->SetPioWrRainStripes(true);
      sfs_flags = SFS_O_CREAT | SFS_O_RDWR | SFS_O_TRUNC;
      sfs_mode = MapModeXrdCl2Posix(mode);
      mChecksum = ChecksumPlugins::GetChecksumObject(layout);
    }

    if (pRainFile->OpenPio(stripeUrls, sfs_flags, sfs_mode, opaqueInfo)) {
      eos_err("msg=\"failed PIO open\" url=\"%s\"", url.c_str());
      st = XRootDStatus(stError, errInvalidOp, 0, "failed PIO open");
    }
  }

  if (!st.IsOK() && mIsPioWrite) {
    // Make sure any stripe already created is deleted on disconnect and
    // drop the namespace entry created by the PIO open
    if (pRainFile) {
      (void)pRainFile->Remove();
    }

    DropFromMgm();
  }

  return st;
}

//------------------------------------------------------------------------------
// Close
//------------------------------------------------------------------------------
XRootDStatus
RainFile::Close(ResponseHandler* handler,
                uint16_t timeout)
{
  eos_debug("calling close");
  XRootDStatus st;

  if (mIsOpen) {
    mIsOpen = false;

    if (pFile) {
      st = pFile->Close(handler, timeout);
    } else {
      if (mIsPioWrite) {
        st = ClosePioWrite();
      } else if (pRainFile->Close()) {
        st = XRootDStatus(stError, errUnknown);
      }

      if (st.IsOK()) {
        XRootDStatus* ret_st = new XRootDStatus(st);
        handler->HandleResponse(ret_st, 0);
      }
    }
  } else {
    // File already closed
    st = XRootDStatus(stError, errInvalidOp);
    XRootDStatus* ret_st = new XRootDStatus(st);
    handler->HandleResponse(ret_st, 0);
  }

  return st;
}

//------------------------------------------------------------------------------
// Close file written in PIO mode and commit it to the MGM. Same order as for
// a gateway write: first commit and then close the layout which flushes any
// remaining parity and the headers. On failure the file is dropped.
//------------------------------------------------------------------------------
XRootDStatus
RainFile::ClosePioWrite()
{
  XRootDStatus st;

  if (mHasWriteErr) {
    st = XRootDStatus(stError, errErrorResponse, EIO, "PIO write - previous write error");
  } else if (!FinalizeChecksum()) {
    st = XRootDStatus(stError, errErrorResponse, EIO,
                      "PIO write - failed checksum computation");
  } else {
    st = CommitToMgm();
  }

  if (!st.IsOK()) {
    // Stripes might not be (all) registered in the namespace, remove them
    // directly and not rely only on the drop at the MGM
    (void)pRainFile->Remove();
  }

  if (pRainFile->Close() && st.IsOK()) {
    eos_err("%s", "msg=\"failed PIO layout close\"");
    st = XRootDStatus(stError, errErrorResponse, EIO, "PIO write - failed layout close");
  }

  if (!st.IsOK()) {
    eos_err("msg=\"failed PIO write, drop file\" err=\"%s\"", st.ToString().c_str());
    DropFromMgm();
  }

  return st;
}

//------------------------------------------------------------------------------
// Compute final checksum, rescan the file if needed
//------------------------------------------------------------------------------
bool
RainFile::FinalizeChecksum()
{
  if (!mChecksum) {
    return true;
  }

  mChecksum->Finalize();

  // If the checksum does not cover the whole file e.g. truncate extended the
  // file or a write did not extend it then the checksum is dirty
  if (mHasWrite && ((uint64_t)mChecksum->GetMaxOffset() != mFileSize)) {
    mChecksum->SetDirty();
  }

  if (mChecksum->NeedsRecalculation()) {
    unsigned long long scan_size = 0ull;
    std::chrono::milliseconds scan_time{0};
    CheckSum::ReadCallBack::callback_data_t cbd;
    cbd.caller = static_cast<void*>(pRainFile);
    CheckSum::ReadCallBack cb(LayoutReadCB, cbd);

    if (!mChecksum->ScanFile(cb, scan_size, scan_time) || (scan_size != mFileSize)) {
      eos_err("msg=\"checksum rescan failed\" scan_size=%llu file_size=%llu", scan_size,
              mFileSize);
      return false;
    }

    eos_info("msg=\"rescanned checksum\" size=%llu duration_ms=%llu xs=%s", scan_size,
             scan_time.count(), mChecksum->GetHexChecksum());
  }

  return true;
}

//------------------------------------------------------------------------------
// Commit file written in PIO mode to the MGM. One commit registers all the
// stripes, the MGM takes the file id, path and stripe locations from the PIO
// write capability.
//------------------------------------------------------------------------------
XRootDStatus
RainFile::CommitToMgm()
{
  struct timeval tv;
  gettimeofday(&tv, nullptr);
  std::ostringstream oss;
  oss << "/?mgm.pcmd=commit&mgm.pio.commit=1"
      << "&mgm.size=" << mFileSize << "&mgm.mtime=" << tv.tv_sec
      << "&mgm.mtime_ns=" << tv.tv_usec * 1000 << "&mgm.logid=" << mMgmLogId;

  if (mHasWrite) {
    oss << "&mgm.modified=1";
  }

  if (mChecksum) {
    oss << "&mgm.checksum=" << mChecksum->GetHexChecksum();
  }

  oss << mPioCapability;
  XRootDStatus st = QueryMgm(oss.str());

  if (!st.IsOK()) {
    eos_err("msg=\"failed PIO commit\" err=\"%s\"", st.ToString().c_str());
    return st;
  }

  eos_info("msg=\"PIO commit done\" size=%llu", mFileSize);
  return st;
}

//------------------------------------------------------------------------------
// Drop file written in PIO mode from the MGM
//------------------------------------------------------------------------------
void
RainFile::DropFromMgm()
{
  if (mPioCapability.empty() || mMgmEndpoint.empty()) {
    return;
  }

  std::ostringstream oss;
  oss << "/?mgm.pcmd=drop&mgm.pio.drop=1&mgm.logid=" << mMgmLogId << mPioCapability;
  XRootDStatus st = QueryMgm(oss.str());

  if (!st.IsOK()) {
    eos_warning("msg=\"failed to drop file at manager\" err=\"%s\"",
                st.ToString().c_str());
  }

  // Drop only once
  mPioCapability.clear();
}

//------------------------------------------------------------------------------
// Send opaque query to the MGM
//------------------------------------------------------------------------------
XRootDStatus
RainFile::QueryMgm(const std::string& query)
{
  XrdCl::Buffer arg;
  XrdCl::Buffer* raw_response = nullptr;
  arg.FromString(query);
  XrdCl::FileSystem fs{URL(mMgmEndpoint)};
  XRootDStatus st = fs.Query(QueryCode::OpaqueFile, arg, raw_response);
  delete raw_response;
  return st;
}

//------------------------------------------------------------------------------
// Stat
//------------------------------------------------------------------------------
XRootDStatus
RainFile::Stat(bool force,
               ResponseHandler* handler,
               uint16_t timeout)
{
  eos_debug("calling stat");
  XRootDStatus st;

  if (pFile) {
    st = pFile->Stat(force, handler, timeout);
  } else {
    struct stat buf;
    int retc = pRainFile->Stat(&buf);

    if (retc) {
      eos_err("RAIN stat failed retc=%i", retc);
      st = XRootDStatus(stError, errUnknown);
    } else {
      StatInfo* sinfo = new StatInfo();
      std::ostringstream data;
      data << buf.st_dev << " " << buf.st_size << " "
           << buf.st_mode << " " << buf.st_mtime;

      if (!sinfo->ParseServerResponse(data.str().c_str())) {
        eos_err("error parsing stat info");
        delete sinfo;
        st = XRootDStatus(stError, errDataError);
      } else {
        eos_debug("stat parsing is ok:%i", st.IsOK());
        XRootDStatus* ret_st = new XRootDStatus(st);
        AnyObject* obj = new AnyObject();
        obj->Set(sinfo);
        handler->HandleResponse(ret_st, obj);
      }
    }
  }

  return st;
}


//------------------------------------------------------------------------------
// Read
//------------------------------------------------------------------------------
XRootDStatus
RainFile::Read(uint64_t offset,
               uint32_t size,
               void* buffer,
               ResponseHandler* handler,
               uint16_t timeout)
{
  eos_debug("offset=%ju, size=%ju", offset, size);
  XRootDStatus st;

  if (pFile) {
    st = pFile->Read(offset, size, buffer, handler, timeout);
  } else {
    int64_t retc = pRainFile->Read(offset, (char*)buffer, size);

    if (retc == -1) {
      st = XRootDStatus(stError, errUnknown);
    } else {
      XRootDStatus* ret_st = new XRootDStatus(st);
      ChunkInfo* chunkInfo = new ChunkInfo(offset, retc, buffer);
      AnyObject* obj = new AnyObject();
      obj->Set(chunkInfo);
      handler->HandleResponse(ret_st, obj);
    }
  }

  return st;
}


//------------------------------------------------------------------------------
// Write
//------------------------------------------------------------------------------
XRootDStatus
RainFile::Write(uint64_t offset,
                uint32_t size,
                const void* buffer,
                ResponseHandler* handler,
                uint16_t timeout)
{
  eos_debug("offset=%ju, size=%ju", offset, size);
  XRootDStatus st;

  if (pFile) {
    st = pFile->Write(offset, size, buffer, handler, timeout);
  } else if (!mIsPioWrite) {
    st = XRootDStatus(stError, errNotSupported, 0, "RAIN file not opened for writing");
  } else if (mHasWriteErr) {
    st =
        XRootDStatus(stError, errErrorResponse, EIO, "RAIN write - previous write error");
  } else {
    int64_t nwrite = pRainFile->Write(offset, static_cast<const char*>(buffer), size);

    if (nwrite != (int64_t)size) {
      eos_err("msg=\"RAIN write failed\" offset=%llu size=%u retc=%lli", offset, size,
              nwrite);
      mHasWriteErr = true;
      st = XRootDStatus(stError, errErrorResponse, EIO, "RAIN write failed");
    } else {
      if (mChecksum) {
        mChecksum->Add(static_cast<const char*>(buffer), size, offset);
      }

      mHasWrite = true;

      if (offset + size > mFileSize) {
        mFileSize = offset + size;
      }

      XRootDStatus* ret_st = new XRootDStatus(st);
      handler->HandleResponse(ret_st, 0);
    }
  }

  return st;
}


//------------------------------------------------------------------------------
// Sync
//------------------------------------------------------------------------------
XRootDStatus
RainFile::Sync(ResponseHandler* handler,
               uint16_t timeout)
{
  eos_debug("callnig sync");
  XRootDStatus st;

  if (pFile) {
    st = pFile->Sync(handler, timeout);
  } else {
    int retc = pRainFile->Sync();

    if (retc) {
      st = XRootDStatus(stError, errUnknown);
    } else {
      XRootDStatus* ret_st = new XRootDStatus(st);
      handler->HandleResponse(ret_st, 0);
    }
  }

  return st;
}


//------------------------------------------------------------------------------
// Truncate
//------------------------------------------------------------------------------
XRootDStatus
RainFile::Truncate(uint64_t size,
                   ResponseHandler* handler,
                   uint16_t timeout)
{
  eos_debug("offset=%ju", size);
  XRootDStatus st;

  if (pFile) {
    st = pFile->Truncate(size, handler, timeout);
  } else if (!mIsPioWrite) {
    st = XRootDStatus(stError, errNotSupported, 0, "RAIN file not opened for writing");
  } else if (pRainFile->Truncate(size)) {
    eos_err("msg=\"RAIN truncate failed\" size=%llu", size);
    mHasWriteErr = true;
    st = XRootDStatus(stError, errErrorResponse, EIO, "RAIN truncate failed");
  } else {
    // Checksum is recomputed at close if it does not cover the whole file
    if (mChecksum && ((uint64_t)mChecksum->GetMaxOffset() != size)) {
      mChecksum->SetDirty();
    }

    mHasWrite = true;
    mFileSize = size;
    XRootDStatus* ret_st = new XRootDStatus(st);
    handler->HandleResponse(ret_st, 0);
  }

  return st;
}


//------------------------------------------------------------------------------
// VectorRead
//------------------------------------------------------------------------------
XRootDStatus
RainFile::VectorRead(const ChunkList& chunks,
                     void* buffer,
                     ResponseHandler* handler,
                     uint16_t timeout)
{
  eos_debug("calling vread");
  XRootDStatus st;

  if (pFile) {
    st = pFile->VectorRead(chunks, buffer, handler, timeout);
  } else {
    // Compute total length of readv request
    uint32_t len = 0;

    for (auto it = chunks.begin(); it != chunks.end(); ++it) {
      len += it->length;
    }

    int64_t retc = pRainFile->ReadV(const_cast<ChunkList&>(chunks), len);

    if (retc == (int64_t)len) {
      XRootDStatus* ret_st = new XRootDStatus(st);
      AnyObject* obj = new AnyObject();
      VectorReadInfo* vReadInfo = new VectorReadInfo();
      vReadInfo->SetSize(len);
      ChunkList& vResp = vReadInfo->GetChunks();
      vResp = chunks;
      obj->Set(vReadInfo);
      handler->HandleResponse(ret_st, obj);
    } else {
      st = XRootDStatus(stError, errUnknown);
    }
  }

  return st;
}


//------------------------------------------------------------------------------
// Fcntl
//------------------------------------------------------------------------------
XRootDStatus
RainFile::Fcntl(const XrdCl::Buffer& arg,
                ResponseHandler* handler,
                uint16_t timeout)
{
  eos_debug("calling fcntl");
  XRootDStatus st;

  if (pFile) {
    st = pFile->Fcntl(arg, handler, timeout);
  } else {
    st = XRootDStatus(stError, errNotImplemented, 0, "RAIN fcntl not implemented");
  }

  return st;
}


//------------------------------------------------------------------------------
// Visa
//------------------------------------------------------------------------------
XRootDStatus
RainFile::Visa(ResponseHandler* handler,
               uint16_t timeout)
{
  eos_debug("calling visa");
  XRootDStatus st;

  if (pFile) {
    st = pFile->Visa(handler, timeout);
  } else {
    st = XRootDStatus(stError, errNotImplemented, 0, "RAIN visa not implemented");
  }

  return st;
}


//------------------------------------------------------------------------------
// IsOpen
//------------------------------------------------------------------------------
bool
RainFile::IsOpen() const
{
  return mIsOpen;
}


//------------------------------------------------------------------------------
// @see XrdCl::File::SetProperty
//------------------------------------------------------------------------------
bool
RainFile::SetProperty(const std::string& name,
                      const std::string& value)
{
  eos_debug("name=%s, value=%s", name.c_str(), value.c_str());

  if (pFile) {
    return pFile->SetProperty(name, value);
  } else {
    eos_err("op. not implemented for RAIN files");
    return false;
  }
}


//------------------------------------------------------------------------------
// @see XrdCl::File::GetProperty
//------------------------------------------------------------------------------
bool
RainFile::GetProperty(const std::string& name,
                      std::string& value) const
{
  eos_debug("name=%s", name.c_str());

  if (pFile) {
    return pFile->GetProperty(name, value);
  }

  // The file is accessed through the MGM in PIO mode, so the MGM is the one
  // that can answer the checksum query e.g. of xrdcp --cksum
  if (name == "LastURL") {
    value = mUrl;
    return true;
  }

  eos_err("op. not implemented for RAIN files");
  return false;
}


//------------------------------------------------------------------------------
//! @see XrdCl::File::GetDataServer
//------------------------------------------------------------------------------
std::string
RainFile::GetDataServer() const
{
  eos_debug("get data server");
  return std::string("");
}


//------------------------------------------------------------------------------
//! @see XrdCl::File::GetLastURL
//------------------------------------------------------------------------------
URL
RainFile::GetLastURL() const
{
  eos_debug("get last URL");
  return std::string("");
}


EOSFSTNAMESPACE_END
