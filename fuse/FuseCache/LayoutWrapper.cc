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
#include "../SyncResponseHandler.hh"
#include "common/Logging.hh"
#include "common/LayoutId.hh"
#include "fst/layout/PlainLayout.hh"

XrdSysMutex LayoutWrapper::gCacheAuthorityMutex;
std::map<unsigned long long, LayoutWrapper::CacheEntry>
LayoutWrapper::gCacheAuthority;

//--------------------------------------------------------------------------
//! Utility function to import (key,value) from a cgi string to a map
//------------------------------------------------------------------------------
bool LayoutWrapper::ImportCGI(std::map<std::string, std::string>& m,
                              const std::string& cgi)
{
  std::string::size_type eqidx = 0, ampidx = (cgi[0] == '&' ? 0 : -1);
  eos_static_info("START");

  do
  {
    //eos_static_info("ampidx=%d  cgi=%s",(int)ampidx,cgi.c_str());
    if ((eqidx = cgi.find('=', ampidx + 1)) == std::string::npos)
      break;

    std::string key = cgi.substr(ampidx + 1, eqidx - ampidx - 1);
    ampidx = cgi.find('&', eqidx);
    std::string val = cgi.substr(eqidx + 1,
                                 ampidx == std::string::npos ? ampidx : ampidx - eqidx - 1);
    m[key] = val;
  }
  while (ampidx != std::string::npos);

  return true;
}

//------------------------------------------------------------------------------
//! Utility function to write the content of a(key,value) map to a cgi string
//------------------------------------------------------------------------------
bool LayoutWrapper::ToCGI(const std::map<std::string, std::string>& m ,
                          std::string& cgi)
{
  for (auto it = m.begin(); it != m.end(); it++)
  {
    if (cgi.size()) cgi += "&";

    cgi += it->first;
    cgi += "=";
    cgi += it->second;
  }

  return true;
}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
LayoutWrapper::LayoutWrapper(eos::fst::Layout* file) :
    mFile(file), mOpen(false), mClose(false), mFabs(NULL), mDoneAsyncOpen(false),
    mOpenHandler(NULL)
{
  mLocalUtime[0].tv_sec = mLocalUtime[1].tv_sec = 0;
  mLocalUtime[0].tv_nsec = mLocalUtime[1].tv_nsec = 0;
  mCanCache = false;
  mCacheCreator = false;
  mInode = 0;
  mMaxOffset = 0;
  mSize = 0;
  mInlineRepair = false;
  mRestore = false;
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
LayoutWrapper::~LayoutWrapper()
{
  if (mCacheCreator)
    (*mCache).resize(mMaxOffset);

  delete mFile;
}

//------------------------------------------------------------------------------
// Make sure that the file layout is open. Reopen it if needed using
// (almost) the same argument as the previous open.
//------------------------------------------------------------------------------
int LayoutWrapper::MakeOpen()
{
  XrdSysMutexHelper mLock(mMakeOpenMutex);
  eos_static_debug("makeopening file %s", mPath.c_str());

  if (mClose)
  {
    eos_static_err("file %s is already closed - won't open", mPath.c_str());
    return -1;
  }

  if (!mOpen)
  {
    if (mPath.size())
    {
      if (Open(mPath, mFlags, mMode, mOpaque.c_str(), NULL))
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

//------------------------------------------------------------------------------
// overloading member functions of FileLayout class
//------------------------------------------------------------------------------
const char*
LayoutWrapper::GetName()
{
  MakeOpen();
  return mFile->GetName();
}

//------------------------------------------------------------------------------
// overloading member functions of FileLayout class
//------------------------------------------------------------------------------
const char*
LayoutWrapper::GetLocalReplicaPath()
{
  MakeOpen();
  return mFile->GetLocalReplicaPath();
}

//------------------------------------------------------------------------------
// overloading member functions of FileLayout class
//------------------------------------------------------------------------------
unsigned int LayoutWrapper::GetLayoutId()
{
  MakeOpen();
  return mFile->GetLayoutId();
}

//------------------------------------------------------------------------------
// overloading member functions of FileLayout class
//------------------------------------------------------------------------------
const std::string&
LayoutWrapper::GetLastUrl()
{
  if (!mOpen)
    return mLazyUrl;

  return mFile->GetLastUrl();
}

//------------------------------------------------------------------------------
// overloading member functions of FileLayout class
//------------------------------------------------------------------------------
bool LayoutWrapper::IsEntryServer()
{
  MakeOpen();
  return mFile->IsEntryServer();
}

//------------------------------------------------------------------------------
// Do the open on the mgm but not on the fst yet
//------------------------------------------------------------------------------
int LayoutWrapper::LazyOpen(const std::string& path, XrdSfsFileOpenMode flags,
                            mode_t mode, const char* opaque, const struct stat* buf)
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
  {
    openflags += "ro";
  }

  char openmode[32];
  snprintf(openmode, 32, "%o", mode);
  request += "?eos.app=fuse&mgm.pcmd=redirect";
  request += (std::string("&") + opaque);
  request += ("&eos.client.openflags=" + openflags);
  request += "&eos.client.openmode=";
  request += openmode;
  arg.FromString(request);
  // add the authentication parameters back if they exist
  XrdOucEnv env(opaque);

  if (env.Get("xrd.wantprot"))
  {
    user_url += '?';
    user_url += "xrd.wantprot=";
    user_url += env.Get("xrd.wantprot");

    if (env.Get("xrd.gsiusrpxy"))
    {
      user_url += "&xrd.gsiusrpxy=";
      user_url += env.Get("xrd.gsiusrpxy");
    }

    if (env.Get("xrd.k5ccname"))
    {
      user_url += "&xrd.k5ccname=";
      user_url += env.Get("xrd.k5ccname");
    }
  }

  // Send the request for FsCtl
  u = XrdCl::URL(user_url);
  XrdCl::FileSystem fs(u);

  SyncResponseHandler handler;
  fs.Query (XrdCl::QueryCode::OpaqueFile, arg, &handler);
  status = handler.Sync(response);

  if (!status.IsOK())
  {
    if ((status.errNo == kXR_FSError) && mInlineRepair &&
        (((flags & SFS_O_WRONLY) || (flags & SFS_O_RDWR)) && (!(flags & SFS_O_CREAT))))
    {
      // FS io error state for writing we try to recover the file on the fly
      if (!Repair(path, opaque))
      {
        eos_static_err("failed to lazy open request %s at url %s code=%d "
                       "errno=%d - repair failed", request.c_str(),
                       user_url.c_str(), status.code, status.errNo);
        return -1;
      }
      else
      {
        // Reissue the open
	SyncResponseHandler handler;
	fs.Query (XrdCl::QueryCode::OpaqueFile, arg, &handler);
	status = handler.Sync(response);

        if (!status.IsOK())
        {
          eos_static_err("failed to lazy open request %s at url %s code=%d "
                         "errno=%d - still unwritable after repair",
                         request.c_str(), user_url.c_str(), status.code,
                         status.errNo);
          return -1;
        }
      }
    }
    else
    {
      eos_static_err("failed to lazy open request %s at url %s code=%d "
                     "errno=%d", request.c_str(), user_url.c_str(),
                     status.code, status.errNo);
      return -1;
    }
  }

  // Split the reponse
  XrdOucString origResponse = response->GetBuffer();
  origResponse += "&eos.app=fuse";
  auto qmidx = origResponse.find("?");
  // insert back the cgi params that are not given back by the mgm
  std::map<std::string, std::string> m;
  ImportCGI(m, opaque);
  ImportCGI(m, origResponse.c_str() + qmidx + 1);
  // Drop authentication params as they would fail on the fst
  m.erase("xrd.wantprot");
  m.erase("xrd.k5ccname");
  m.erase("xrd.gsiusrpxy");
  // Let the lazy open use an open by inode
  std::string fxid = m["mgm.id"];
  mOpaque += "&eos.lfn=fxid:";
  mOpaque += fxid;
  mInode = strtoull(fxid.c_str(), 0, 16);
  std::string LazyOpaque;
  ToCGI(m, LazyOpaque);
  mLazyUrl.assign(origResponse.c_str(), qmidx);
  mLazyUrl.append("?");
  mLazyUrl.append(LazyOpaque);
  // ==================================================================
  // We don't want to truncate the file in case we reopen it
  mFlags = flags & ~(SFS_O_TRUNC | SFS_O_CREAT);

  delete response;
  return 0;
}

//------------------------------------------------------------------------------
// Repair a partially offline file
//------------------------------------------------------------------------------
bool
LayoutWrapper::Repair(const std::string& path, const char* opaque)
{
  eos_static_notice("path=\"%s\" opaque=\"%s\"", path.c_str(), opaque);
  // get path and url prefix
  XrdCl::URL u(path);
  std::string file_path = u.GetPath();

  if (file_path.substr(0, 2) == "//")
  {
    file_path.erase(0, 1);
  }

  std::string cmd = "mgm.cmd=file&mgm.subcmd=version&eos.app=fuse&"
                    "mgm.grab.version=-1&mgm.path=" + file_path + "&" + opaque;
  u.SetParams("");
  u.SetPath("/proc/user/");
  XrdCl::XRootDStatus status;
  std::unique_ptr<LayoutWrapper> file (new LayoutWrapper(new eos::fst::PlainLayout(
										   NULL, 0, NULL, NULL, eos::common::LayoutId::kXrdCl)));
  int retc = file->Open(u.GetURL().c_str(), (XrdSfsFileOpenMode)0,
                        (mode_t)0, cmd.c_str(), NULL, true, 0, false);

  if (retc)
  {
    eos_static_err("open failed for %s?%s : error code is %d",
                   u.GetURL().c_str(), cmd.c_str(), (int) errno);
    return false;
  }

  file->Close();
  return true;
}


//------------------------------------------------------------------------------
// Restore a file which didn't write/close properly
//------------------------------------------------------------------------------
bool
LayoutWrapper::Restore()
{
  mRestore = false;

  if (getenv("EOS_FUSE_NO_CACHE_RESTORE"))
    return false;

  off_t restore_size=0;
  {
    XrdSysMutexHelper l(gCacheAuthorityMutex);
    
    if (!mCanCache || (!gCacheAuthority.count(mInode)) || gCacheAuthority[mInode].mPartial)
    {
      eos_static_warning("unable to restore inode=%lu size=%llu partial=%d lifetime=%lu", mInode, gCacheAuthority[mInode].mSize, gCacheAuthority[mInode].mPartial, gCacheAuthority[mInode].mSize);
      return false;
    }
    eos_static_info("inode=%lu size=%llu partial=%d lifetime=%lu", mInode, gCacheAuthority[mInode].mSize, gCacheAuthority[mInode].mPartial, gCacheAuthority[mInode].mSize);
    restore_size = gCacheAuthority[mInode].mSize;
  }

  XrdCl::URL u(mPath.c_str());
  std::string params = "eos.atomic=1&eos.app=restore";
   
  XrdOucEnv env(mOpaque.c_str());

  if (env.Get("xrd.wantprot"))
  {
    params += '&';
    params += "xrd.wantprot=";
    params += env.Get("xrd.wantprot");

    if (env.Get("xrd.gsiusrpxy"))
    {
      params += "&xrd.gsiusrpxy=";
      params += env.Get("xrd.gsiusrpxy");
    }

    if (env.Get("xrd.k5ccname"))
    {
      params += "&xrd.k5ccname=";
      params += env.Get("xrd.k5ccname");
    }
  }

  if (env.Get("eos.encodepath"))
  {
    params += "&eos.encodepath=";
    params += env.Get("eos.encodepath");
  }
  
  u.SetParams(params.c_str());

  std::unique_ptr <eos::fst::PlainLayout> file (new eos::fst::PlainLayout(
									  NULL, 0, NULL, NULL, eos::common::LayoutId::kXrdCl));
  for (size_t i = 0; i < 3; ++i)
  {
    if ( file->Open(u.GetURL().c_str(), mFlags | SFS_O_CREAT, mMode, params.c_str()) )
    {
      XrdSysTimer sleeper;
      eos_static_warning("restore failed to open path=%s - snooze 5s ...", u.GetURL().c_str());
      sleeper.Snooze(5);
    }
    else 
    {
      size_t blocksize=4*1024*1024;
      int retc = 0 ;
      for (off_t offset = 0 ; offset < restore_size; offset += blocksize)
      {
	size_t length = blocksize;
	if ( (restore_size - offset) < (off_t)blocksize)
	{
	  length = restore_size - offset;
	}
	char* ptr;
	if (!(*mCache).peekData (ptr, offset, length))
	{
	  (*mCache).releasePeek();
	  eos_static_err("read-error while restoring : peekData failed");
	  return false;
	}

	(*mCache).releasePeek();

	if ( (retc = file->Write(offset, ptr, length)) < 0)
	{
	  eos_static_err("write-error while restoring : file %s  opaque %s",
			 mPath.c_str(), params.c_str());
	  file->Close();
	  break;
	}
	else
	{
	  eos_static_info("restored path=%s offset=%llu length=%lu", mPath.c_str(), offset, length);
	}
      }

      // retrieve the new inode
      std::string lasturl = file->GetLastUrl();
      auto qmidx = lasturl.find("?");
      lasturl.erase(0, qmidx);
      std::map<std::string, std::string> m;
      ImportCGI(m, lasturl);
      std::string fxid = m["mgm.id"];
      unsigned long long newInode = strtoull(fxid.c_str(), 0, 16);

      if (file->Close())
      {
	XrdSysTimer sleeper;
	eos_static_warning("restore failed to close path=%s - snooze 5s ...", u.GetURL().c_str());
	sleeper.Snooze(5);
      }
      else
      {
	XrdSysMutexHelper l(gCacheAuthorityMutex);
	if (gCacheAuthority.count(mInode))
	{
	  gCacheAuthority[mInode].mRestoreInode = newInode;
	}
	eos_static_notice("restored path=%s from cache length=%llu inode=%llu new-inode=%llu ", mPath.c_str(), restore_size, mInode, newInode);
	return true;
      }
    }
  }
  return false;
}
//------------------------------------------------------------------------------
// overloading member functions of FileLayout class
//------------------------------------------------------------------------------
int LayoutWrapper::Open(const std::string& path, XrdSfsFileOpenMode flags,
                        mode_t mode, const char* opaque, const struct stat* buf,
                        bool doOpen, size_t owner_lifetime, bool inlineRepair)
{
  int retc = 0;
  static int sCleanupTime = 0;

  if (inlineRepair)
    mInlineRepair = inlineRepair;

  eos_static_debug("opening file %s, lazy open is %d flags=%x inline-repair=%s",
                   path.c_str(), (int)!doOpen, flags, mInlineRepair ? "true" : "false");

  if (mOpen)
  {
    eos_static_debug("already open");
    return -1;
  }

  mPath = path;
  mFlags = flags;
  mMode = mode;
  mOpaque = opaque;
  
  if (buf)
    Utimes(buf);
  
  if (buf)
    mSize = buf->st_size;
  
  if (!doOpen)
  {
    retc = LazyOpen(path, flags, mode, opaque, buf);

    if (retc < 0)
      return retc;

    if (getenv("EOS_FUSE_ASYNC_OPEN")) 
    {
      // Do the async open on the FST and return
      eos::fst::PlainLayout* plain_layout = static_cast<eos::fst::PlainLayout*>(mFile);
      mOpenHandler = new eos::fst::AsyncLayoutOpenHandler(plain_layout);
      
      if (plain_layout->OpenAsync(path, flags, mode, mOpenHandler, opaque))
      {
	delete mOpenHandler;
	eos_static_err("error while async opening path=%s", path.c_str());
	return -1;
      }
      else
      {
	mDoneAsyncOpen = true;
      }
    }
  }
  else
  {
    // for latency simulation purposes
    if (getenv("EOS_FUSE_LAZY_LAG_OPEN") && mFlags)
    {
      eos_static_warning("lazy-lag configured - delay by %s ms", getenv("EOS_FUSE_LAZY_LAG_OPEN"));
      XrdSysTimer sleeper;
      sleeper.Wait(atoi(getenv("EOS_FUSE_LAZY_LAG_OPEN")));
    }

    bool retry = true;
    XrdOucString sopaque (opaque);
#ifdef STOPONREDIRECT
    if (sopaque.length ()) sopaque += '&';
    sopaque += "tried=";
#endif
    if (mDoneAsyncOpen)
    {
      // Wait for the async open response
      if (!static_cast<eos::fst::PlainLayout*>(mFile)->WaitOpenAsync())
      {
        XrdCl::URL url (mFile->GetLastUrl ());
        const std::string &username = url.GetUserName ();
        if (!username.empty () && username[0]!='*'
        && static_cast<eos::fst::PlainLayout*> (mFile)->GetLastErrNo () == kXR_NotAuthorized)
        {
          eos_static_notice("async open failed for path=%s because of authentication, credentials might have been lost on redirect. Trying to fix with a sync open", path.c_str ());
        }
        else
        {
          eos_static_err("async open failed for path=%s", path.c_str());
          return -1;
        }
#ifdef STOPONREDIRECT
        eos_static_notice("async open failed for path=%s, trying to fix it with other replicas", path.c_str ());
        XrdCl::URL url (mFile->GetLastUrl ());
        sopaque += url.GetHostName ().c_str ();
        sopaque.append (',');
#endif
      }
      else // async open ok, don't need a sync open
        retry = false;
    }

    std::string _lasturl,_path(path);
    size_t pos, _pos(0);
    while (retry)
    {
      eos_static_debug("Sync-open path=%s opaque=%s",
                       _path.c_str (),
                       sopaque.c_str ());
      // Do synchronous open
      if ((retc = mFile->Open (_path.c_str (), flags, mode, sopaque.c_str ())))
      {
        eos_static_debug("Sync-open got errNo=%d errCode=%d",
                         static_cast<eos::fst::PlainLayout*> (mFile)->GetLastErrNo (),
                         static_cast<eos::fst::PlainLayout*> (mFile)->GetLastErrCode ());
#ifdef STOPONREDIRECT
        /*
        =======================================================================================
        This is an alternative way to deal with the loss of credentials when getting redirected
        There we assume that we hit first the mgm, then an fst then a mgm, then a fst .....
        So we stopevery two redirects to reissue the open and get the credentials back
        This works because every time a channel has to be opened, it is opened with the krb5
        cgis by an explicit user (not the redirect open which is slightly different)
        =======================================================================================
        */
        if(static_cast<eos::fst::PlainLayout*>(mFile)->GetLastErrCode()==XrdCl::errRedirectLimit)
        {
          // if we fail because of too many redirects, try again
          // appending the last visited fst to the list of the tried
          retry = true;
          _lasturl = mFile->GetLastUrl();
          XrdCl::URL url(mFile->GetLastUrl());
          eos_static_debug("Last URL = %s",mFile->GetLastUrl().c_str());
          sopaque += url.GetHostName().c_str();
          sopaque += ',';
        }
#else
        XrdCl::URL url (mFile->GetLastUrl ());
        const std::string &username = url.GetUserName ();
        /*
        =======================================================================================
        This is a hackish fix to the loss of strong credentials while being redirected
        The open will follow redirects until we get a permission denied on an mgm because
        authenticated using unix
        At this point, it is too late to retry with the same connection which is using unix.
        We then try again with another connection incrementing the first letter of the username
        We iterate that process as long the failing location changes.
        NOTE1: this fixes only the redirect problem on open. It does not fix the problem for
        other access, especially XrdCl::FileSystem operations
        NOTE2: the main use case of this is a recoverable error on open on the fst ( fmd error
        on RO open for example ). We then get redirected to the mgm possibly with a different
        hostname and a new connection is being created and the credentials are lost).
        NOTE3: the previous use case is also fixed on server side using the standard XRootD
        fail recovery procedure on open which goes back to the mgm using the previous
        connection. This is implemented on server side starting from eos-citrine 4.0.20.
        =======================================================================================
        */
        if (!username.empty () && username[0]!='*'
        && static_cast<eos::fst::PlainLayout*> (mFile)->GetLastErrNo () == kXR_NotAuthorized
        && ( _lasturl.empty() || (_pos = _lasturl.find('@'))!=std::string::npos )
        && (pos=mFile->GetLastUrl().find('@'))!=std::string::npos)
        {
          // if it's the same url regardless of the username, we fail
          if (!strcmp (_lasturl.c_str () + _pos, mFile->GetLastUrl ().c_str () + pos))
          {
            eos_static_err("using a new connection did not fix at %s",
                           mFile->GetLastUrl ().c_str());
            errno = EPERM;
            return -1;
          }
          _lasturl = mFile->GetLastUrl ();
          _path = mFile->GetLastUrl();
          size_t p;
          // increment the first character of the login until we reach Z
          // it forces a new connection to be used , as the previous is most likely used by unix
          if ((p = _path.find ('@')) != std::string::npos)
          {
            if (_path[p-8] != 'Z') _path[p-8]++;
            else
            {
              eos_static_warning("reached maximum number of redirects for strong authentication");
              errno = EPERM;
              return -1;
            }
          }
          sopaque = "";
          eos_static_debug("authentication error at %s, try with a new connection to overcome strong credentials loss in redirects",
                           mFile->GetLastUrl ().c_str ());
        }
#endif
        else
        {
          eos_static_err("error while openning");
          return -1;
        }
      }
      else
        retry = false;
    }

    // We don't want to truncate the file in case we reopen it
    mFlags = flags & ~(SFS_O_TRUNC | SFS_O_CREAT);
    mOpen = true;
    std::string lasturl = mFile->GetLastUrl();
    auto qmidx = lasturl.find("?");
    lasturl.erase(0, qmidx);
    std::map<std::string, std::string> m;
    ImportCGI(m, lasturl);
    std::string fxid = m["mgm.id"];
    mInode = strtoull(fxid.c_str(), 0, 16);
  }

  time_t now = time(0);
  XrdSysMutexHelper l(gCacheAuthorityMutex);

  if (mInode && (!mCache.get()))
  {
    if ( (flags & SFS_O_CREAT) || (flags & SFS_O_TRUNC) )
    {
      gCacheAuthority[mInode].mLifeTime = 0;
      gCacheAuthority[mInode].mPartial = false;
      gCacheAuthority[mInode].mOwnerLifeTime = owner_lifetime;
      gCacheAuthority[mInode].mCache = std::make_shared<Bufferll>();
      mCache = gCacheAuthority[mInode].mCache;
      mCanCache = true;
      mCacheCreator = true;
      mSize = gCacheAuthority[mInode].mSize;
      eos_static_notice("acquired cap owner-authority for file %s size=%d ino=%lu create=%d truncate=%d",
                        path.c_str(), (*mCache).size(), mInode, flags & SFS_O_CREAT, flags & SFS_O_TRUNC);
    }
    else
    {
      if (gCacheAuthority.count(mInode) &&
	  gCacheAuthority[mInode].mCache.get() &&
          ((!gCacheAuthority[mInode].mLifeTime) ||
           (now < gCacheAuthority[mInode].mLifeTime)))
      {
        mCanCache = true;
        mCache = gCacheAuthority[mInode].mCache;
        mSize = gCacheAuthority[mInode].mSize;

        // we try to lazy open if we have somethign cached!
        if (doOpen && mSize)
          doOpen = false;

        mMaxOffset = (*mCache).size();
        eos_static_notice("reusing cap owner-authority for file %s cache-size=%d "
                          "file-size=%lu inode=%lu", path.c_str(), (*mCache).size(),
                          mSize, mInode);
      }
    }

    eos_static_info("####### %s cache=%d flags=%x\n", path.c_str(), mCanCache,
                    flags);
  }

  if (now > sCleanupTime)
  {
    for (auto it = gCacheAuthority.begin(); it != gCacheAuthority.end();)
    {
      if ((it->second.mLifeTime) && (it->second.mLifeTime < now))
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

//------------------------------------------------------------------------------
// Overloading member functions of FileLayout class
//------------------------------------------------------------------------------
int64_t LayoutWrapper::Read(XrdSfsFileOffset offset, char* buffer,
                            XrdSfsXferSize length, bool readahead)
{
  if (MakeOpen())
  {
    errno = EBADF;
    return -1;
  }
  return mFile->Read(offset, buffer, length, readahead);
}

//------------------------------------------------------------------------------
// Overloading member functions of FileLayout class
//------------------------------------------------------------------------------
#ifdef XROOTD4
int64_t LayoutWrapper::ReadV(XrdCl::ChunkList& chunkList, uint32_t len)
{
  if (MakeOpen())
  {
    errno = EBADF;
    return -1;
  }
  return mFile->ReadV(chunkList, len);
}
#endif

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
int64_t LayoutWrapper::ReadCache(XrdSfsFileOffset offset, char* buffer,
                                 XrdSfsXferSize length, off_t maxcache)
{
  if (!mCanCache)
    return -1;

  // This is not fully cached
  if ((offset + length) > (off_t) maxcache)
    return -1;

  return (*mCache).readData(buffer, offset, length);
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
int64_t LayoutWrapper::WriteCache(XrdSfsFileOffset offset, const char* buffer,
                                  XrdSfsXferSize length, off_t maxcache)
{
  if (!mCanCache)
    return 0;

  {
    XrdSysMutexHelper l(gCacheAuthorityMutex);

    if (gCacheAuthority.count(mInode))
      if ((offset + length) >  gCacheAuthority[mInode].mSize)
        gCacheAuthority[mInode].mSize = (offset + length);
  }

  if ((*mCache).capacity() < (4 * 1024))
  {
    // helps to speed up
    (*mCache).resize(4 * 1024);
  }
  
  // don't exceed the maximum cachesize per file
  if ((offset + length) > (off_t) maxcache)
  {
    if (gCacheAuthority.count(mInode))
      gCacheAuthority[mInode].mPartial = true;
    return 0;
  }

  if ((offset + length) > mMaxOffset)
    mMaxOffset = offset + length;

  // Store in cache
  return (*mCache).writeData(buffer, offset, length);
}

//------------------------------------------------------------------------------
// Overloading member functions of FileLayout class
//------------------------------------------------------------------------------
int64_t LayoutWrapper::Write(XrdSfsFileOffset offset, const char* buffer,
                             XrdSfsXferSize length, bool touchMtime)
{
  if (MakeOpen())
  {
    errno = EBADF;
    return -1;
  }
  int retc = 0;

  if (length > 0)
  {
    if ((retc = mFile->Write(offset, buffer, length)) < 0)
    {
      eos_static_err("Error writing from wrapper : file %s  opaque %s",
                     mPath.c_str(), mOpaque.c_str());
      return -1;
    }
  }

  return retc;
}

//------------------------------------------------------------------------------
// Overloading member functions of FileLayout class
//------------------------------------------------------------------------------
int LayoutWrapper::Truncate(XrdSfsFileOffset offset, bool touchMtime)
{
  if (MakeOpen())
  {
    errno = EBADF;
    return -1;
  }

  if (mFile->Truncate(offset))
    return -1;

  XrdSysMutexHelper l(gCacheAuthorityMutex);
  if (gCacheAuthority.count(mInode))
    gCacheAuthority[mInode].mSize = (int64_t) offset;

  return 0;
}

//------------------------------------------------------------------------------
// Overloading member functions of FileLayout class
//------------------------------------------------------------------------------
int LayoutWrapper::Sync()
{
  if (MakeOpen())
  {
    errno = EBADF;
    return -1;
  }
  return mFile->Sync();
}

//------------------------------------------------------------------------------
// Overloading member functions of FileLayout class
//------------------------------------------------------------------------------
int LayoutWrapper::Close()
{
  XrdSysMutexHelper mLock(mMakeOpenMutex);
  eos_static_debug("closing file %s ", mPath.c_str());;

  // for latency simulation purposes
  if (getenv("EOS_FUSE_LAZY_LAG_CLOSE") && mFlags)
  {
    eos_static_warning("lazy-lag configured - delay by %s ms", getenv("EOS_FUSE_LAZY_LAG_CLOSE"));
    XrdSysTimer sleeper;
    sleeper.Wait(atoi(getenv("EOS_FUSE_LAZY_LAG_CLOSE")));
  }
  
  mClose = true;

  if (!mOpen)
  {
    eos_static_debug("already closed");
    return 0;
  }

  if (mCanCache && ((mFlags & O_RDWR) || (mFlags & O_WRONLY)))
  {
    // define expiration of owner lifetime from close on
    XrdSysMutexHelper l(gCacheAuthorityMutex);
    time_t now = time(0);
    time_t expire = now + gCacheAuthority[mInode].mOwnerLifeTime;
    gCacheAuthority[mInode].mLifeTime = expire;
    eos_static_notice("define expiry of  cap owner-authority for file "
                      "inode=%lu tst=%lu lifetime=%lu", mInode, expire,
                      gCacheAuthority[mInode].mOwnerLifeTime);
  }

  int retc = 0; 
  if (mFile->Close())
  {
    eos_static_debug("error while closing");
    retc = -1;
  }
  else
  {
    mOpen = false;
    eos_static_debug("successfully closed");
    retc = 0;
  }
  if ( ((mFlags & O_RDWR) || (mFlags & O_WRONLY)) && (retc || mRestore))
  {
    if (Restore())
      retc = 0;
  }
  return retc;
}

//------------------------------------------------------------------------------
// Overloading member functions of FileLayout class
//------------------------------------------------------------------------------
int LayoutWrapper::Stat(struct stat* buf)
{
  if (MakeOpen())
  {
    errno = EBADF;
    return -1;
  }

  if (mFile->Stat(buf))
    return -1;
  else
    return 0;
}


//------------------------------------------------------------------------------
// Set atime and mtime at current time
//------------------------------------------------------------------------------
void LayoutWrapper::Utimes(const struct stat* buf)
{
  // set local Utimes
  mLocalUtime[0] = buf->st_atim;
  mLocalUtime[1] = buf->st_mtim;
  eos_static_debug("setting timespec  atime:%lu.%.9lu      mtime:%lu.%.9lu",
                   mLocalUtime[0].tv_sec, mLocalUtime[0].tv_nsec,
                   mLocalUtime[1].tv_sec, mLocalUtime[1].tv_nsec);
}

//------------------------------------------------------------------------------
// Get Last Opened Path
//------------------------------------------------------------------------------
std::string LayoutWrapper::GetLastPath()
{
  return mPath;
}

//------------------------------------------------------------------------------
// Is the file Opened
//------------------------------------------------------------------------------
bool LayoutWrapper::IsOpen()
{
  XrdSysMutexHelper mLock(mMakeOpenMutex);
  return mOpen;
}


//------------------------------------------------------------------------------
// Return last known size of a file we had a caps for
//------------------------------------------------------------------------------
long long
LayoutWrapper::CacheAuthSize(unsigned long long inode)
{
  // the variable mInode is actually using the EOS file ID
  inode = eos::common::FileId::InodeToFid(inode);
  time_t now = time(NULL);
  XrdSysMutexHelper l(gCacheAuthorityMutex);

  if (inode)
  {
    if (gCacheAuthority.count(inode))
    {
      long long size = gCacheAuthority[inode].mSize;

      if ((!gCacheAuthority[inode].mLifeTime)
          || (now < gCacheAuthority[inode].mLifeTime))
      {
        eos_static_debug("reusing cap owner-authority for inode %x cache-file-size=%lld",
                         inode, size);
        return size;
      }
      else
      {
        eos_static_debug("found expired cap owner-authority for inode %x cache-file-size=%lld",
                         inode, size);
      }
    }
  }

  return -1;
}

//------------------------------------------------------------------------------
// Migrate cache inode after a restore operation
//------------------------------------------------------------------------------
unsigned long long
LayoutWrapper::CacheRestore(unsigned long long inode)
{
  // the variable mInode is actually using the EOS file ID
  inode = eos::common::FileId::InodeToFid(inode);
  XrdSysMutexHelper l(gCacheAuthorityMutex);

  eos_static_debug("inode=%llu", inode);
  if (inode)
  {
    if (gCacheAuthority.count(inode))
    {
      unsigned long long new_ino = gCacheAuthority[inode].mRestoreInode;
      if (new_ino)
      {
	gCacheAuthority[new_ino] = gCacheAuthority[inode];
	auto d = gCacheAuthority.find(inode);
	gCacheAuthority.erase(d);
	gCacheAuthority[new_ino].mRestoreInode = 0;
	eos_static_notice("migrated cap owner-authority for file inode=%lu => inode=%lu", inode, new_ino);
	return eos::common::FileId::FidToInode(new_ino);
      }
    }
  }

  return 0;
}

//------------------------------------------------------------------------------
// Return last known size of a file we had a caps for
//------------------------------------------------------------------------------
void LayoutWrapper::CacheRemove(unsigned long long inode)
{
  inode = eos::common::FileId::InodeToFid(inode);
  XrdSysMutexHelper l(gCacheAuthorityMutex);


  {
    if (gCacheAuthority.count(inode))
    {
      auto d = gCacheAuthority.find(inode);
      gCacheAuthority.erase(d);
      eos_static_notice("removed cap owner-authority for file inode=%lu", d->first);
    }
  }
}
