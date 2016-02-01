//------------------------------------------------------------------------------
// File LayoutWrapper.cc
// Author: Geoffray Adde <geoffray.adde@cern.ch> CERN
//------------------------------------------------------------------------------

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

#include "LayoutWrapper.hh"
#include "FileAbstraction.hh"
#include "common/Logging.hh"

//--------------------------------------------------------------------------
//! Utility function to import (key,value) from a cgi string to a map
//--------------------------------------------------------------------------
bool LayoutWrapper::ImportCGI(std::map<std::string,std::string> &m, const std::string &cgi)
{
  std::string::size_type eqidx=0,ampidx=(cgi[0]=='&'?0:-1);
  eos_static_info("START");
  do
  {
    //eos_static_info("ampidx=%d  cgi=%s",(int)ampidx,cgi.c_str());
    if( (eqidx=cgi.find('=',ampidx+1)) == std::string::npos)
      break;
    std::string key=cgi.substr(ampidx+1,eqidx-ampidx-1);
    ampidx=cgi.find('&',eqidx);
    std::string val=cgi.substr(eqidx+1,ampidx==std::string::npos?ampidx:ampidx-eqidx-1);
    m[key]= val;
  } while(ampidx!=std::string::npos);
  return true;
}

//--------------------------------------------------------------------------
//! Utility function to write the content of a(key,value) map to a cgi string
//--------------------------------------------------------------------------
bool LayoutWrapper::ToCGI(const std::map<std::string,std::string> &m , std::string &cgi)
{
  for(auto it=m.begin();it!=m.end();it++)
  {
    if(cgi.size()) cgi+="&";
    cgi+=it->first;
    cgi+="=";
    cgi+=it->second;
  }
  return true;
}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
LayoutWrapper::LayoutWrapper (eos::fst::Layout* file, bool localtimesoncistent) :
    mFile (file), mOpen (false), mDebugSize(0), mDebugHasWrite (false), mFabs(NULL),  mLocalTimeConsistent(localtimesoncistent), mDebugWasReopen(false)
{
  mLocalUtime[0].tv_sec = mLocalUtime[1].tv_sec = 0;
  mLocalUtime[0].tv_nsec = mLocalUtime[1].tv_nsec = 0;
}

//----------------------------------------------------------------------------
// Destructor
//----------------------------------------------------------------------------
LayoutWrapper::~LayoutWrapper ()
{
  delete mFile;
}

//--------------------------------------------------------------------------
// Make sure that the file layout is open
// Reopen it if needed using (almost) the same argument as the previous open
//--------------------------------------------------------------------------
int LayoutWrapper::MakeOpen ()
{
  eos_static_debug("makeopening file %s", mPath.c_str ());
  if (!mOpen)
  {
    if (mPath.size ())
    {
      if (Open (mPath, mFlags, mMode, mOpaque.c_str (),NULL))
      {
        eos_static_debug("error while openning");
        return -1;
      }
      else
      {
        eos_static_debug("successfully opened");
        mOpen = true;
        mDebugWasReopen = true;
        return 0;
      }
    }
    else
      return -1;
  }
  else
    eos_static_debug("already opened");
  return 0;
}

//--------------------------------------------------------------------------
// overloading member functions of FileLayout class
//--------------------------------------------------------------------------
const char*
LayoutWrapper::GetName ()
{
  MakeOpen ();
  return mFile->GetName ();
}

//--------------------------------------------------------------------------
// overloading member functions of FileLayout class
//--------------------------------------------------------------------------
const char*
LayoutWrapper::GetLocalReplicaPath ()
{
  MakeOpen ();
  return mFile->GetLocalReplicaPath ();
}

//--------------------------------------------------------------------------
// overloading member functions of FileLayout class
//--------------------------------------------------------------------------
unsigned int LayoutWrapper::GetLayoutId ()
{
  MakeOpen ();
  return mFile->GetLayoutId ();
}

//--------------------------------------------------------------------------
// overloading member functions of FileLayout class
//--------------------------------------------------------------------------
const std::string&
LayoutWrapper::GetLastUrl ()
{
  MakeOpen ();
  return mFile->GetLastUrl ();
}

//--------------------------------------------------------------------------
// overloading member functions of FileLayout class
//--------------------------------------------------------------------------
bool LayoutWrapper::IsEntryServer ()
{
  MakeOpen ();
  return mFile->IsEntryServer ();
}

//--------------------------------------------------------------------------
//! do the open on the mgm but not on the fst yet
//--------------------------------------------------------------------------
int LayoutWrapper::LazyOpen (const std::string& path, XrdSfsFileOpenMode flags, mode_t mode, const char* opaque, const struct stat *buf)
{
  // get path and url prefix
  XrdCl::URL u(path);
  std::string file_path = u.GetPath();
  u.SetPath("");
  u.SetParams("");
  std::string user_url = u.GetURL();

  // build request to send to mgm to get redirection url
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;
  XrdCl::XRootDStatus status;
  std::string request = file_path;
  std::string openflags;
  if (flags != SFS_O_RDONLY)
  {
    if ((flags & SFS_O_WRONLY) && !(flags & SFS_O_RDWR)) openflags += "wo";
    if (flags & SFS_O_RDWR) openflags += "rw";
    if (flags & SFS_O_CREAT) openflags += "cr";
    if (flags & SFS_O_TRUNC) openflags += "tr";
  }
  else
    openflags += "ro";

  char openmode[32];
  snprintf(openmode,32,"%o",mode);

  request += "?eos.app=fuse&mgm.pcmd=redirect";
  request += (std::string("&")+opaque);
  request += ("&eos.client.openflags="+openflags);
  request += "&eos.client.openmode=";
  request += openmode;
  arg.FromString(request);

  // add the authentication parameters back if they exist
  XrdOucEnv env(opaque);
  if(env.Get("xrd.wantprot"))
  {
    user_url += '?';
    if(env.Get("xrd.gsiusrpxy"))
    {
      user_url+="xrd.gsiusrpxy=";
      user_url+=env.Get("xrd.gsiusrpxy");
    }
    if(env.Get("xrd.k5ccname"))
    {
      user_url+="xrd.k5ccname=";
      user_url+=env.Get("xrd.k5ccname");
    }
  }

  // send the request for FsCtl
  u=XrdCl::URL(user_url);
  XrdCl::FileSystem fs(u);
  status = fs.Query(XrdCl::QueryCode::OpaqueFile, arg, response);

  if (!status.IsOK())
  {
    eos_static_err("failed to lazy open request %s at url %s",request.c_str(),user_url.c_str());
    return -1;
  }

  // split the reponse
  XrdOucString origResponse = response->GetBuffer();
  origResponse += "&eos.app=fuse";
  auto qmidx = origResponse.find("?");

  // insert back the cgi params that are not given back by the mgm
  std::map<std::string,std::string> m;
  ImportCGI(m,opaque);
  ImportCGI(m,origResponse.c_str()+qmidx+1);
  // drop authentication params as they would fail on the fst
  m.erase("xrd.wantprot"); m.erase("xrd.k5ccname"); m.erase("xrd.gsiusrpxy");
  mOpaque="";
  ToCGI(m,mOpaque);

  mPath.assign(origResponse.c_str(),qmidx);

  return 0;
}

//--------------------------------------------------------------------------
// overloading member functions of FileLayout class
//--------------------------------------------------------------------------
int LayoutWrapper::Open (const std::string& path, XrdSfsFileOpenMode flags, mode_t mode, const char* opaque, const struct stat *buf, bool doOpen)
{
  eos_static_debug("opening file %s, lazy open is %d", path.c_str (), (int)!doOpen);
  if (mOpen)
  {
    eos_static_debug("already open");
    return -1;
  }

  mPath = path;
  mFlags = flags;
  mMode = mode;
  mOpaque = opaque;

  if(!doOpen)
    return LazyOpen (path, flags, mode, opaque, buf);

  if (mFile->Open (path, flags, mode, opaque))
  {
    eos_static_err("error while openning");
    return -1;
  }
  else
  {
    eos_static_debug("successfully opened. LASTURL %s",mFile->GetLastUrl().c_str());
    mFlags = flags & ~(SFS_O_TRUNC | SFS_O_CREAT);  // we don't want to truncate the file in case we reopen it
    mOpen = true;
    struct stat s;
    mFile->Stat (&s);
    mDebugSize = s.st_size;

    if(mLocalTimeConsistent)
    {
    // if the file is newly created or truncated, update and commit the mtime to now
    if ((flags & SFS_O_TRUNC) || (!buf && (flags & SFS_O_CREAT)))
      UtimesToCommitNow ();
    // else we keep the timestamp of the existing file
    else if (buf) Utimes (buf);
    }

    return 0;
  }
}

//--------------------------------------------------------------------------
// overloading member functions of FileLayout class
//--------------------------------------------------------------------------
int64_t LayoutWrapper::Read (XrdSfsFileOffset offset, char* buffer, XrdSfsXferSize length, bool readahead)
{
  MakeOpen ();
  return mFile->Read (offset, buffer, length, readahead);
}

//--------------------------------------------------------------------------
// overloading member functions of FileLayout class
//--------------------------------------------------------------------------
int64_t LayoutWrapper::ReadV (XrdCl::ChunkList& chunkList, uint32_t len)
{
  MakeOpen ();
  return mFile->ReadV (chunkList, len);
}

//--------------------------------------------------------------------------
// overloading member functions of FileLayout class
//--------------------------------------------------------------------------
int64_t LayoutWrapper::Write (XrdSfsFileOffset offset, const char* buffer, XrdSfsXferSize length, bool touchMtime)
{
  int retc = 0;
  MakeOpen ();

  if (length >= 0)
  {
    if ((retc = mFile->Write (offset, buffer, length)) < 0)
    {
      eos_static_err("Error writng from wrapper : file %s  opaque %s", mPath.c_str (), mOpaque.c_str ());
      return -1;
    }
  }

  mDebugHasWrite = true;

  if (mLocalTimeConsistent && touchMtime) UtimesToCommitNow ();

  if (offset + length > mDebugSize) mDebugSize = offset + length + 1;

  return retc;
}

//--------------------------------------------------------------------------
// overloading member functions of FileLayout class
//--------------------------------------------------------------------------
int LayoutWrapper::Truncate (XrdSfsFileOffset offset, bool touchMtime)
{
  MakeOpen ();

  if(mFile->Truncate (offset))
    return -1;

  mDebugHasWrite = true;

  if(mLocalTimeConsistent && touchMtime)
    UtimesToCommitNow();

  mDebugSize = offset;
  return 0;
}

//--------------------------------------------------------------------------
// overloading member functions of FileLayout class
//--------------------------------------------------------------------------
int LayoutWrapper::Sync ()
{
  MakeOpen ();
  return mFile->Sync ();
}

//--------------------------------------------------------------------------
// overloading member functions of FileLayout class
//--------------------------------------------------------------------------
int LayoutWrapper::Close ()
{
  eos_static_debug("closing file %s  WASREOPEN %d", mPath.c_str (), (int)mDebugWasReopen);
  if (!mOpen)
  {
    eos_static_debug("already closed");
    return 0;
  }
  if (mFile->Close ())
  {
    eos_static_debug("error while closing");
    return -1;
  }
  else
  {
    mOpen = false;
    mDebugHasWrite = false;
    eos_static_debug("successfully closed");
    return 0;
  }
}

//--------------------------------------------------------------------------
// overloading member functions of FileLayout class
//--------------------------------------------------------------------------
int LayoutWrapper::Stat (struct stat* buf)
{
  MakeOpen ();
  if (mFile->Stat (buf)) return -1;

  // if we use the localtime consistency, replace the mtime on the fst by the mtime we keep in memory
  if (mLocalTimeConsistent)
  {
    buf->st_atim = mLocalUtime[0];
    buf->st_mtim = mLocalUtime[1];
    buf->st_atime = buf->st_atim.tv_sec;
    buf->st_mtime = buf->st_mtim.tv_sec;
  }

  return 0;
}

//--------------------------------------------------------------------------
// Set atime and mtime at current time
//--------------------------------------------------------------------------
void LayoutWrapper::UtimesToCommitNow ()
{
  clock_gettime (CLOCK_REALTIME, mLocalUtime);
  // set local Utimes
  mLocalUtime[1] = mLocalUtime[0];
  eos_static_debug("setting timespec  atime:%lu.%.9lu      mtime:%lu.%.9lu",mLocalUtime[0].tv_sec,mLocalUtime[0].tv_nsec,mLocalUtime[1].tv_sec,mLocalUtime[1].tv_nsec);
  // if using local time consistency, commit this time when the file closes
  if(mLocalTimeConsistent && mFabs)
    mFabs->SetUtimes (mLocalUtime);
}


//--------------------------------------------------------------------------
// Set atime and mtime at current time
//--------------------------------------------------------------------------
void LayoutWrapper::Utimes (const struct stat *buf)
{
  // set local Utimes
  mLocalUtime[0] = buf->st_atim;
  mLocalUtime[1] = buf->st_mtim;
  eos_static_debug("setting timespec  atime:%lu.%.9lu      mtime:%lu.%.9lu",mLocalUtime[0].tv_sec,mLocalUtime[0].tv_nsec,mLocalUtime[1].tv_sec,mLocalUtime[1].tv_nsec);
}

//--------------------------------------------------------------------------
// Get Last Opened Path
//--------------------------------------------------------------------------
std::string LayoutWrapper::GetLastPath ()
{
  return mPath;
}

//--------------------------------------------------------------------------
//! Is the file Opened
//--------------------------------------------------------------------------
bool LayoutWrapper::IsOpen ()
{
  return mOpen;
}
