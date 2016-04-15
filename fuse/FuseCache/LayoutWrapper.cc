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


XrdSysMutex LayoutWrapper::gCacheAuthorityMutex;
std::map<unsigned long long, LayoutWrapper::CacheEntry> LayoutWrapper::gCacheAuthority;

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
LayoutWrapper::LayoutWrapper (eos::fst::Layout* file) :
  mFile (file), mOpen (false), mFabs(NULL)
{
  mLocalUtime[0].tv_sec = mLocalUtime[1].tv_sec = 0;
  mLocalUtime[0].tv_nsec = mLocalUtime[1].tv_nsec = 0;
  mCanCache = false;
  mCacheCreator = false;
  mInode = 0;
  mMaxOffset = 0;
  mSize = 0;
}

//----------------------------------------------------------------------------
// Destructor
//----------------------------------------------------------------------------
LayoutWrapper::~LayoutWrapper ()
{
  if (mCacheCreator)
    (*mCache).resize(mMaxOffset);
  delete mFile;
}

//--------------------------------------------------------------------------
// Make sure that the file layout is open
// Reopen it if needed using (almost) the same argument as the previous open
//--------------------------------------------------------------------------
int LayoutWrapper::MakeOpen ()
{
  XrdSysMutexHelper mLock(mMakeOpenMutex);

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
  if (!mOpen)
    return mLazyUrl;
  
  return mFile->GetLastUrl();
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
    eos_static_err("failed to lazy open request %s at url %s code=%d errno=%d",request.c_str(),user_url.c_str(), status.code, status.errNo);
    return -1;
  }

  /*
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
  */
  
  // ==================================================================
  // ! we don't use the redirect on the actual open from a lazy open anynore.
  // there is now a split between RO and RW files in the FileAbstraction
  // to keep the consistency, we always go back to the mgm
  // the lazy open call is then just used than the open can happen
  // and possibly add the entry in the namespace if the file is being created
  // I guess we could still do safely that for rw open

  // split the reponse
  XrdOucString origResponse = response->GetBuffer ();
  origResponse += "&eos.app=fuse";
  auto qmidx = origResponse.find ("?");

  // insert back the cgi params that are not given back by the mgm
  std::map<std::string, std::string> m;
  ImportCGI (m, opaque);
  ImportCGI (m, origResponse.c_str () + qmidx + 1);
  // drop authentication params as they would fail on the fst
  m.erase ("xrd.wantprot");
  m.erase ("xrd.k5ccname");
  m.erase ("xrd.gsiusrpxy");

  // let the lazy open use an open by inode
  std::string fxid = m["mgm.id"];
  mOpaque += "&eos.lfn=fxid:";
  mOpaque += fxid;

  mInode = strtoull(fxid.c_str(), 0, 16);

  std::string LazyOpaque;
  ToCGI (m, LazyOpaque);

  mLazyUrl.assign (origResponse.c_str (), qmidx);
  mLazyUrl.append ("?");
  mLazyUrl.append (LazyOpaque);
  // ==================================================================

  mFlags = flags & ~(SFS_O_TRUNC | SFS_O_CREAT);  // we don't want to truncate the file in case we reopen it

  return 0;
}

//--------------------------------------------------------------------------
// overloading member functions of FileLayout class
//--------------------------------------------------------------------------
int LayoutWrapper::Open (const std::string& path, XrdSfsFileOpenMode flags, mode_t mode, const char* opaque, const struct stat *buf, bool doOpen, size_t owner_lifetime)
{
  int retc = 0;
  static int sCleanupTime=0;

  eos_static_debug("opening file %s, lazy open is %d flags=%x", path.c_str (), (int)!doOpen, flags);
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
  {
    retc =  LazyOpen (path, flags, mode, opaque, buf);
    if (retc < 0)
      return retc;
  }
  
  if (doOpen)
  {
    if ( (retc = mFile->Open (path, flags, mode, opaque)) )
    {
      eos_static_err("error while openning");
      return -1;
    }
    else
    {
      mFlags = flags & ~(SFS_O_TRUNC | SFS_O_CREAT);  // we don't want to truncate the file in case we reopen it
      mOpen = true;

      std::string lasturl = mFile->GetLastUrl();

      auto qmidx = lasturl.find ("?");
      lasturl.erase(0,qmidx);

      std::map<std::string, std::string> m;
      ImportCGI (m, lasturl);

      std::string fxid = m["mgm.id"];
      mInode = strtoull(fxid.c_str(), 0, 16);

      if (flags && buf) Utimes (buf);
      if (buf)
	mSize = buf->st_size;
    }
  }

  time_t now = time(0);

  XrdSysMutexHelper l(gCacheAuthorityMutex);
  if ( mInode && (!mCache.get()) )
  {
    if (flags & SFS_O_CREAT)
    {
      gCacheAuthority[mInode].mLifeTime = 0;
      gCacheAuthority[mInode].mOwnerLifeTime = owner_lifetime;
      gCacheAuthority[mInode].mCache = std::make_shared<Bufferll>();
      mCache = gCacheAuthority[mInode].mCache;
      mCanCache = true;
      mCacheCreator = true;
      mSize = gCacheAuthority[mInode].mSize;
      eos_static_notice("acquired cap owner-authority for file %s size=%d", path.c_str(), (*mCache).size());
    }
    else
    {
      if (gCacheAuthority.count(mInode) && ( (!gCacheAuthority[mInode].mLifeTime) || (now < gCacheAuthority[mInode].mLifeTime ) ) )
      {
	mCanCache = true;
	mCache = gCacheAuthority[mInode].mCache;
	mSize = gCacheAuthority[mInode].mSize;
	// we try to lazy open if we have somethign cached!
	if (doOpen && mSize)
	  doOpen = false;

	mMaxOffset = (*mCache).size();
	eos_static_notice("reusing cap owner-authority for file %s cache-size=%d file-size=%lu", path.c_str(), (*mCache).size(), mSize);
      }
    }
    eos_static_info("####### %s cache=%d flags=%x\n", path.c_str(), mCanCache, flags);
  }

  if (now > sCleanupTime)
  {
    for (auto it = gCacheAuthority.begin(); it != gCacheAuthority.end();)
    {
      if ( (it->second.mLifeTime) && (it->second.mLifeTime < now))
      {
	auto d = it;
	it++;
	eos_static_notice("released cap owner-authority for file inode=%lu", d->first);
	
	gCacheAuthority.erase(d);
      }
      else
	it++;
    }
    sCleanupTime = time(0) + 5;
  }

  return retc;
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

#ifdef XROOTD4
int64_t LayoutWrapper::ReadV (XrdCl::ChunkList& chunkList, uint32_t len)
{
  MakeOpen ();

  return mFile->ReadV (chunkList, len);
}
#endif

int64_t LayoutWrapper::ReadCache (XrdSfsFileOffset offset, char* buffer, XrdSfsXferSize length, off_t maxcache)
{
  if (!mCanCache)
    return -1;

  // this is not fully cached
  if ( (offset + length) > (off_t) maxcache )
    return -1;

  return (*mCache).readData(buffer, offset, length);
}

int64_t LayoutWrapper::WriteCache (XrdSfsFileOffset offset, const char* buffer, XrdSfsXferSize length, off_t maxcache)
{
  if (!mCanCache)
    return 0;

  {
    XrdSysMutexHelper l(gCacheAuthorityMutex);
    if (gCacheAuthority.count(mInode))
      if ((offset+length) >  gCacheAuthority[mInode].mSize)
	gCacheAuthority[mInode].mSize = (offset + length);
  }

  if ((*mCache).capacity() < (1*1024*1024))
  {
    // helps to speed up 
    (*mCache).resize(1*1024*1024);
  } 
  else
  {
    // don't exceed the maximum cachesize per file
    if ( (offset+length) > (off_t) maxcache)
      return 0;
  }

  if ( (offset+length) > mMaxOffset)
    mMaxOffset = offset + length;

  // store in cache
  return (*mCache).writeData(buffer, offset, length);
}


//--------------------------------------------------------------------------
// overloading member functions of FileLayout class
//--------------------------------------------------------------------------
int64_t LayoutWrapper::Write (XrdSfsFileOffset offset, const char* buffer, XrdSfsXferSize length, bool touchMtime)
{
  MakeOpen ();

  int retc = 0;

  if (length > 0)
  {
    if ((retc = mFile->Write (offset, buffer, length)) < 0)
    {
      eos_static_err("Error writing from wrapper : file %s  opaque %s", mPath.c_str (), mOpaque.c_str ());
      return -1;
    }
  }

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

  {
    if (gCacheAuthority.count(mInode))
      gCacheAuthority[mInode].mSize = (int64_t) offset;
  }

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
  XrdSysMutexHelper mLock(mMakeOpenMutex);

  eos_static_debug("closing file %s ", mPath.c_str ());;
  if (!mOpen)
  {
    eos_static_debug("already closed");
    return 0;
  }
  if (mCanCache && ( (mFlags & O_RDWR) || (mFlags & O_WRONLY) ) )
  {
    // define expiration of owner lifetime from close on
    XrdSysMutexHelper l(gCacheAuthorityMutex);
    time_t now = time(0);
    time_t expire = now + gCacheAuthority[mInode].mOwnerLifeTime;
    gCacheAuthority[mInode].mLifeTime = expire;
    eos_static_notice("define expiry of  cap owner-authority for file inode=%lu tst=%lu lifetime=%lu", mInode , expire, gCacheAuthority[mInode].mOwnerLifeTime);
  }

  if (mFile->Close ())
  {
    eos_static_debug("error while closing");
    return -1;
  }
  else
  {
    mOpen = false;
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

  return 0;
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
  XrdSysMutexHelper mLock(mMakeOpenMutex);

  return mOpen;
}


//--------------------------------------------------------------------------
//! Return last known size of a file we had a caps for
//--------------------------------------------------------------------------
long long
LayoutWrapper::CacheAuthSize(unsigned long long inode)
{
  time_t now = time(NULL);

  XrdSysMutexHelper l(gCacheAuthorityMutex);
  if ( inode )
  {
    eos_static_info("checking cap owner-authority for inode %x", inode);
    if (gCacheAuthority.count(inode))
    { 
      long long size = gCacheAuthority[inode].mSize;
      if ((!gCacheAuthority[inode].mLifeTime) || (now < gCacheAuthority[inode].mLifeTime))
      {
	eos_static_notice("reusing cap owner-authority for inode %x cache-file-size=%lld", inode, size);
	return size;
      }
      else
      {
	eos_static_info("foundexpired cap owner-authority for inode %x cache-file-size=%lld", inode, size);
      }
    }
  }
  return -1;
}
