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

/*----------------------------------------------------------------------------*/
#include "RainFile.hh"
#include "fst/layout/RainMetaLayout.hh"
#include "fst/layout/RaidDpLayout.hh"
#include "fst/layout/ReedSLayout.hh"
/*----------------------------------------------------------------------------*/

using namespace eos::common;
using namespace eos::fst;

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

  // For reading try PIO mode
  if ((flags & OpenFlags::Flags::Read) == OpenFlags::Flags::Read) {
    Buffer arg;
    Buffer* response = 0;
    std::string fpath = url;
    size_t spos = fpath.rfind("//");

    if (spos != std::string::npos) {
      fpath.erase(0, spos + 1);
    }

    std::string request = fpath;
    request += "?mgm.pcmd=open";
    arg.FromString(request);
    std::string endpoint = url;
    endpoint.erase(spos + 1);
    URL Url(endpoint);
    XrdCl::FileSystem fs(Url);
    st = fs.Query(QueryCode::OpaqueFile, arg, response);

    if (st.IsOK()) {
      // Parse output
      XrdOucString tag;
      XrdOucString stripePath;
      std::vector<std::string> stripeUrls;
      XrdOucString origResponse = response->GetBuffer();
      XrdOucString stringOpaque = response->GetBuffer();
      // Add the eos.app=rainplugin tag to all future PIO open requests
      origResponse += "&eos.app=rainplugin";

      while (stringOpaque.replace("?", "&")) { }

      while (stringOpaque.replace("&&", "&")) { }

      XrdOucEnv* openOpaque = new XrdOucEnv(stringOpaque.c_str());
      char* opaqueInfo = (char*) strstr(origResponse.c_str(), "&mgm.logid");

      if (opaqueInfo) {
        opaqueInfo += 1;
        LayoutId::layoutid_t layout = openOpaque->GetInt("mgm.lid");

        for (unsigned int i = 0; i <= (eos::common::LayoutId::GetStripeNumber(layout) + eos::common::LayoutId::GetExcessStripeNumber(layout));
             i++) {
          tag = "pio.";
          tag += static_cast<int>(i);
          stripePath = "root://";
          stripePath += openOpaque->Get(tag.c_str());
          stripePath += "/";
          stripePath += fpath.c_str();
          stripeUrls.push_back(stripePath.c_str());
        }

        if (LayoutId::GetLayoutType(layout) == LayoutId::kRaidDP) {
          pRainFile = new RaidDpLayout(NULL, layout, NULL, NULL, "");
        } else if ((LayoutId::IsRainLayout(layout))) {
          pRainFile = new ReedSLayout(NULL, layout, NULL, NULL, "");
        } else {
          eos_warning("unsupported PIO layout");
          return XRootDStatus(stError, errNotSupported, 0, "unsupported PIO layout");
        }

        if (pRainFile) {
          if (pRainFile->OpenPio(stripeUrls, SFS_O_RDONLY, mode, opaqueInfo)) {
            eos_err("failed PIO open for path=%s", url.c_str());
            delete pRainFile;
            st = XRootDStatus(stError, errInvalidOp, 0, "failed PIO open");
          }
        } else {
          eos_err("no RAIN file allocated");
          st = XRootDStatus(stError, errInternal, 0, "no RAIN file allocated");
        }
      } else {
        eos_err("no opaque info");
        st = XRootDStatus(stError, errDataError, 0, "no opaque info");
      }
    } else {
      eos_err("error while doing PIO read request");
      st = XRootDStatus(stError, errNotImplemented, 0, "error PIO read request");
    }

    if (st.IsOK()) {
      mIsOpen = true;
      XRootDStatus* ret_st = new XRootDStatus(st);
      handler->HandleResponse(ret_st, 0);
    }
  } else {
    // Normal XrdCl file access
    pFile = new XrdCl::File(false);
    st = pFile->Open(url, flags, mode, handler, timeout);

    if (st.IsOK()) {
      mIsOpen = true;
    }
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
      int retc = pRainFile->Close();

      if (retc) {
        st = XRootDStatus(stError, errUnknown);
      } else {
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
  } else {
    st = XRootDStatus(stError, errNotImplemented, 0, "RAIN write not implemented");
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
  } else {
    st = XRootDStatus(stError, errNotImplemented, 0,
                      "RAIN truncate not implemented");
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
      ChunkList vResp = vReadInfo->GetChunks();
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
RainFile::Fcntl(const Buffer& arg,
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
  } else {
    eos_err("op. not implemented for RAIN files");
    return false;
  }
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
