
//------------------------------------------------------------------------------
//! @file data.cc
//! @author Andreas-Joachim Peters CERN
//! @brief data handling class
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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

#include "data/data.hh"
#include "kv/kv.hh"
#include "eosfuse.hh"
#include "data/cachesyncer.hh"
#include "data/journalcache.hh"
#include "misc/MacOSXHelper.hh"
#include "misc/fusexrdlogin.hh"
#include "common/Logging.hh"
#include <iostream>
#include <sstream>


bufferllmanager data::datax::sBufferManager;

/* -------------------------------------------------------------------------- */
data::data()
/* -------------------------------------------------------------------------- */
{
  XrdCl::Proxy::sRaBufferManager.configure(16, cachehandler::instance().get_config().default_read_ahead_size);
}

/* -------------------------------------------------------------------------- */
data::~data()
/* -------------------------------------------------------------------------- */
{
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
data::init()
/* -------------------------------------------------------------------------- */
{
  datamap.run();
}

/* -------------------------------------------------------------------------- */
data::shared_data
/* -------------------------------------------------------------------------- */
data::get(fuse_req_t req,
          fuse_ino_t ino,
          metad::shared_md md)
/* -------------------------------------------------------------------------- */
{
  XrdSysMutexHelper mLock(datamap);

  if (datamap.count(ino)) {
    shared_data io = datamap[ino];
    io->attach(); // client ref counting
    return io;
  } else {
    // protect against running out of file descriptors
    size_t openfiles = datamap.size();
    size_t openlimit = (EosFuse::Instance().Config().options.fdlimit-128)/2;

    while ( (openfiles=datamap.size()) > openlimit )
    {
      datamap.UnLock();
      eos_static_warning("open-files=%lu limit=%lu - waiting for release of file descriptors",
                         openfiles, openlimit);
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
      datamap.Lock();
    }

    shared_data io = std::make_shared<datax>(md);
    io->set_id(ino, req);
    datamap[(fuse_ino_t) io->id()] = io;
    io->attach();
    return io;
  }
}

/* -------------------------------------------------------------------------- */
bool
/* -------------------------------------------------------------------------- */
data::has(fuse_ino_t ino)
/* -------------------------------------------------------------------------- */
{
  XrdSysMutexHelper mLock(datamap);
  if (datamap.count(ino))
    return true;
  else
    return false;
}



/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
data::release(fuse_req_t req,
              fuse_ino_t ino)
/* -------------------------------------------------------------------------- */
{
  XrdSysMutexHelper mLock(datamap);

  if (datamap.count(ino)) {
    shared_data io = datamap[ino];
    io->detach();
    // the object is cleaned by the flush thread
  }

  if (datamap.count(ino + 0xffffffff)) {
    // in case this is an unlinked object
    shared_data io = datamap[ino + 0xffffffff];
    io->detach();
  }
}

void
/* -------------------------------------------------------------------------- */
data::update_cookie(uint64_t ino, std::string& cookie)
/* -------------------------------------------------------------------------- */
{
  XrdSysMutexHelper mLock(datamap);

  if (datamap.count(ino)) {
    shared_data io = datamap[ino];
    io->attach(); // client ref counting
    io->store_cookie(cookie);
    io->detach();
  }
}


/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
data::invalidate_cache(fuse_ino_t ino)
/* -------------------------------------------------------------------------- */
{
  XrdSysMutexHelper mLock(datamap);

  if (datamap.count(ino)) {
    shared_data io = datamap[ino];
    io->attach(); // client ref counting
    io->cache_invalidate();
    io->detach();
  }
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
data::unlink(fuse_req_t req, fuse_ino_t ino)
/* -------------------------------------------------------------------------- */
{
  XrdSysMutexHelper mLock(datamap);
  if (datamap.count(ino))
  {
    XrdSysMutexHelper helper(datamap[ino]->Locker());
    // wait for open in flight to be done
    datamap[ino]->WaitOpen();
    datamap[ino]->unlink(req);
    // put the unlinked inode in a high bucket, will be removed by the flush thread
    datamap[ino + 0xffffffff] = datamap[ino];
    datamap.erase(ino);
    eos_static_info("datacache::unlink size=%lu", datamap.size());
  } else {
    shared_data io = std::make_shared<datax>();
    io->set_id(ino, req);
    io->unlink(req);
  }
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
data::datax::flush(fuse_req_t req)
/* -------------------------------------------------------------------------- */
{
  eos_info("");
  XrdSysMutexHelper lLock(mLock);
  return flush_nolock(req, EosFuse::Instance().Config().options.flush_wait_open?true:false, false);
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
data::datax::flush_nolock(fuse_req_t req, bool wait_open, bool wait_writes)
/* -------------------------------------------------------------------------- */
{
  eos_info("");

  bool journal_recovery = false;

  if (mFile->journal() && mFile->has_xrdiorw(req)) {
    eos_info("flushing journal");

    ssize_t truncate_size = mFile->journal()->get_truncatesize();

    if (wait_open)
    {
      // wait atleast that we could open that file
      mFile->xrdiorw(req)->WaitOpen();
    }

    if ( (truncate_size != -1 )
	 || ( wait_writes  &&  mFile->journal()->size() ) )
    {
      // if there is a truncate to be done, we have to wait for the writes and truncate
      // if we are asked to wait for writes (when pwrite sees a journal full) we free the journal

      for (auto it = mFile->get_xrdiorw().begin();
	   it != mFile->get_xrdiorw().end(); ++it) {
	XrdCl::XRootDStatus status = it->second->WaitOpen();
	if (!status.IsOK()) {
	  journal_recovery = true;
	  eos_err("file not open");
	}

	status = it->second->WaitWrite();

	if (!status.IsOK()) {
	  journal_recovery = true;
	  eos_err("write error error=%s", status.ToStr().c_str());
	}
      }

      ssize_t truncate_size = mFile->journal()->get_truncatesize();
      if (!journal_recovery && (truncate_size != -1)) {
	// the journal might have a truncation size indicated, so we need to run a sync truncate in the end
	XrdCl::XRootDStatus status = mFile->xrdiorw(req)->Truncate(truncate_size);
	if (!status.IsOK()) {
	  journal_recovery = true;
	  eos_err("truncation failed");
	}
      }
      

      if (simulate_write_error_in_flush())
      {
	// force a 'fake' repair now for testing purposes
	journal_recovery = true;
      }

      if (journal_recovery) {
	eos_debug("try recovery");
	if (TryRecovery(req, true))
	{
	  eos_err("journal-flushing recovery failed");
	  errno = EREMOTEIO ;
	  return -1;
	} 
	else 
	{
	  if (journalflush(req))
	  {
	    eos_err("journal-flushing failed");
	    errno = EREMOTEIO ;
	    return -1;
	  }
	}
      } 

      // truncate the journal
      if (mFile->journal()->reset()) {
	char msg[1024];
	snprintf(msg, sizeof(msg), "journal reset failed - ino=%08lx", id());
	throw std::runtime_error(msg);
      }
    }
  }

  // check if the open failed
  XrdCl::Proxy* proxy = mFile->has_xrdioro(req) ? mFile->xrdioro(req) : mFile->xrdiorw(req);
  if (proxy)
  {
    if (proxy->stateTS() == XrdCl::Proxy::FAILED)
    {
      eos_debug("try recovery");
      if (TryRecovery(req, true))
      {
	eos_err("remote open failed - returning EREMOTEIO");
	errno = EREMOTEIO;
	return -1;
      }
      else
      {
	if (journalflush(req))
	{
	  eos_err("journal-flushing failed");
	  errno = EREMOTEIO ;
	  return -1;
	}
	else
	{
	  // truncate the journal
	  if (mFile->journal()->reset()) {
	    char msg[1024];
	    snprintf(msg, sizeof(msg), "journal reset failed - ino=%08lx", id());
	    throw std::runtime_error(msg);
	  }
	}
      }
    }
  }

  eos_info("retc=0");
  return 0;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
data::datax::journalflush(fuse_req_t req)
/* -------------------------------------------------------------------------- */
{
  // call this with a mLock locked
  eos_info("");

  // we have to push the journal now
  if (!mFile->xrdiorw(req)->WaitOpen().IsOK()) {
    eos_err("async journal-cache-wait-open failed - ino=%08lx", id());
    return -1;
  }

  eos_info("syncing cache");
  cachesyncer cachesync(*((XrdCl::File*)mFile->xrdiorw(req)));

  if ((mFile->journal())->remote_sync(cachesync)) {
    eos_err("async journal-cache-sync failed - ino=%08lx", id());
    return -1;
  }

  eos_info("retc=0");
  return 0;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
data::datax::journalflush(std::string cid)
/* -------------------------------------------------------------------------- */
{
  // call this with a mLock locked
  eos_info("");

  // we have to push the journal now
  if (!mFile->xrdiorw(cid)->WaitOpen().IsOK()) {
    eos_err("async journal-cache-wait-open failed - ino=%08lx", id());
    return -1;
  }

  if (mFile->journal()) {
    eos_info("syncing cache");
    cachesyncer cachesync(*((XrdCl::File*)mFile->xrdiorw(cid)));

    if ((mFile->journal())->remote_sync(cachesync)) {
      eos_err("async journal-cache-sync failed - ino=%08lx", id());
      return -1;
    }
  }

  eos_info("retc=0");
  return 0;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
data::datax::journalflush_async(std::string cid)
/* -------------------------------------------------------------------------- */
{
  // call this with a mLock locked
  eos_info("");

  // we have to push the journal now
  if (!mFile->xrdiorw(cid)->WaitOpen().IsOK()) {
    eos_err("async journal-cache-wait-open failed - ino=%08lx", id());
    return -1;
  }

  if (mFile->journal()) {
    eos_info("syncing cache asynchronously");

    if ((mFile->journal())->remote_sync_async(mFile->xrdiorw(cid)))
    {
      eos_err("async journal-cache-sync-async failed - ino=%08lx", id());
      return -1;
    }
  }

  eos_info("retc=0");
  return 0;
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
data::datax::set_id(uint64_t ino, fuse_req_t req)
/* -------------------------------------------------------------------------- */
{
  XrdSysMutexHelper mLock(Locker());
  mIno = ino;
  mReq = req;
  mFile = cachehandler::instance().get(ino);
  char lid[64];
  snprintf(lid, sizeof(lid), "logid:ino:%016lx", ino);
  SetLogId(lid);
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
data::datax::attach(fuse_req_t freq, std::string& cookie, int flags)
/* -------------------------------------------------------------------------- */
{
  XrdSysMutexHelper lLock(mLock);
  bool isRW = false;
  mFlags = flags;

  if (flags & (O_RDWR | O_WRONLY)) {
    isRW = true;
  }

  eos_info("cookie=%s flags=%o isrw=%d md-size=%d %s", cookie.c_str(), flags,
           isRW, mMd->size(),
           isRW ? mRemoteUrlRW.c_str() : mRemoteUrlRO.c_str());


  // store the currently known size here
  mSize = mMd->size();


  // set write error simulation flags
  if (mMd->name().find("#err_sim_flush#") != std::string::npos)
  {
    eos_crit("enabling error simulation on flush");
    mSimulateWriteErrorInFlush = true;
  } else if (mMd->name().find("#err_sim_flusher#") != std::string::npos)
  {
    eos_crit("enabling error simulation on flusher");
    mSimulateWriteErrorInFlusher = true;
  }

  if (flags & O_SYNC) {
    mFile->disable_caches();
  }

  int bcache = mFile->file() ? mFile->file()->attach(freq, cookie, isRW) : 0;
  int jcache = mFile->journal() ? mFile->journal()->attach(freq, cookie,
               isRW) : 0;

  if (bcache < 0) {
    char msg[1024];
    snprintf(msg, sizeof(msg), "attach to cache failed - ino=%08lx", id());
    throw std::runtime_error(msg);
  }

  if (jcache < 0) {
    char msg[1024];
    snprintf(msg, sizeof(msg), "attach to journal failed - ino=%08lx", id());
    throw std::runtime_error(msg);
  }

  if (isRW) {
    if (!mFile->has_xrdiorw(freq) || mFile->xrdiorw(freq)->IsClosing() ||
        mFile->xrdiorw(freq)->IsClosed()) {
      if (mFile->has_xrdiorw(freq) && (mFile->xrdiorw(freq)->IsClosing() ||
                                   mFile->xrdiorw(freq)->IsClosed())) {
        mFile->xrdiorw(freq)->WaitClose();
        mFile->xrdiorw(freq)->attach();
      } else {
        // attach an rw io object
        mFile->set_xrdiorw(freq, new XrdCl::Proxy());
        mFile->xrdiorw(freq)->attach();
        mFile->xrdiorw(freq)->set_id(id(), req());
      }

      XrdCl::OpenFlags::Flags targetFlags = XrdCl::OpenFlags::Update;
      XrdCl::Access::Mode mode = XrdCl::Access::UR | XrdCl::Access::UW |
                                 XrdCl::Access::UX;
      mFile->xrdiorw(freq)->OpenAsync(mRemoteUrlRW.c_str(), targetFlags, mode, 0);
    } else {
      if (mFile->xrdiorw(freq)->IsWaitWrite())
      {
	// re-open the file in the state machine
	mFile->xrdiorw(freq)->set_state_TS(XrdCl::Proxy::OPENED);
      }
      mFile->xrdiorw(freq)->attach();
    }
  } else {
    if (!mFile->has_xrdioro(freq) || mFile->xrdioro(freq)->IsClosing() ||
        mFile->xrdioro(freq)->IsClosed()) {
      if (mFile->has_xrdioro(freq) && (mFile->xrdioro(freq)->IsClosing() ||
                                       mFile->xrdioro(freq)->IsClosed())) {
        mFile->xrdioro(freq)->WaitClose();
        mFile->xrdioro(freq)->attach();
      } else {
        mFile->set_xrdioro(freq, new XrdCl::Proxy());
        mFile->xrdioro(freq)->attach();
        mFile->xrdioro(freq)->set_id(id(), req());

        if (!(flags & O_SYNC)) {
	  if (EOS_LOGS_DEBUG)
	    eos_debug("readhead: strategy=%s nom:%lu max:%lu",
		      cachehandler::instance().get_config().read_ahead_strategy.c_str(),
		      cachehandler::instance().get_config().default_read_ahead_size,
		      cachehandler::instance().get_config().max_read_ahead_size);

	  mFile->xrdioro(freq)->set_readahead_strategy(
						       XrdCl::Proxy::readahead_strategy_from_string(cachehandler::instance().get_config().read_ahead_strategy),
						       4096,
						       cachehandler::instance().get_config().default_read_ahead_size,
						       cachehandler::instance().get_config().max_read_ahead_size
						       );
        }
      }

      XrdCl::OpenFlags::Flags targetFlags = XrdCl::OpenFlags::Read;
      XrdCl::Access::Mode mode = XrdCl::Access::UR | XrdCl::Access::UX;
      // we might need to wait for a creation to go through
      WaitOpen();
      mFile->xrdioro(freq)->OpenAsync(mRemoteUrlRO.c_str(), targetFlags, mode, 0);
    } else {
      mFile->xrdioro(freq)->attach();
    }
  }

  return bcache | jcache;
}

/* -------------------------------------------------------------------------- */
bool
/* -------------------------------------------------------------------------- */
data::datax::prefetch(fuse_req_t req, bool lock)
/* -------------------------------------------------------------------------- */
{
  size_t file_size = mMd->size();

  eos_info("handler=%d file=%lx size=%lu md-size=%lu", mPrefetchHandler ? 1 : 0,
           mFile ? mFile->file() : 0,
           mFile ? mFile->file() ? mFile->file()->size() : 0 : 0,
           file_size);

  if (mFile && mFile->has_xrdiorw(req)) {
    // never prefetch on a wr open file
    return true;
  }

  if (lock) {
    mLock.Lock();
  }

  if (!mPrefetchHandler && mFile->file() && !mFile->file()->size() && file_size)
  {
    XrdCl::Proxy* proxy = mFile->has_xrdioro(req) ? mFile->xrdioro(req) : mFile->xrdiorw(req);

    if (proxy) {
      XrdCl::XRootDStatus status;

      size_t prefetch_size = std::min((size_t)file_size, (size_t)mFile->file()->prefetch_size());

      // send an async read request
      mPrefetchHandler = proxy->ReadAsyncPrepare(0, prefetch_size);
      status = proxy->PreReadAsync(0, prefetch_size, mPrefetchHandler, 0);
      if (!status.IsOK()) {
        eos_err("pre-fetch failed error=%s", status.ToStr().c_str());
        mPrefetchHandler = 0;
      }
    }
  }

  if (lock) {
    mLock.UnLock();
  }

  return mPrefetchHandler ? true : false;
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
data::datax::WaitPrefetch(fuse_req_t req, bool lock)
/* -------------------------------------------------------------------------- */
{
  eos_info("");

  if (lock) {
    mLock.Lock();
  }

  size_t file_size = mMd->size();

  if (mPrefetchHandler && mFile->file())
  {
    XrdCl::Proxy* proxy = mFile->has_xrdioro(req) ? mFile->xrdioro(req) : mFile->xrdiorw(req);

    if (mPrefetchHandler && proxy) {
      XrdCl::XRootDStatus status = proxy->WaitRead(mPrefetchHandler);
      if (status.IsOK())
      {
        eos_info("pre-read done with size=%lu md-size=%lu", mPrefetchHandler->vbuffer().size(), file_size);
        if ((mPrefetchHandler->vbuffer().size() == file_size) && mFile->file())
        {
          ssize_t nwrite = mFile->file()->pwrite(mPrefetchHandler->buffer(), mPrefetchHandler->vbuffer().size(), 0);
          eos_debug("nwb=%lu to local cache", nwrite);
        }
      } else {
        eos_err("pre-read failed error=%s", status.ToStr().c_str());
      }
    }
  }

  if (lock) {
    mLock.UnLock();
  }
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
data::datax::WaitOpen()
{
  // make sure there is no pending remote open.
  // this has to be done to avoid opening a file for read, which is not yet
  // created.
  for (auto fit = mFile->get_xrdiorw().begin();
       fit != mFile->get_xrdiorw().end(); ++fit) {
    if (fit->second->IsOpening()) {
      eos_info("status=pending url=%s", fit->second->url().c_str());
      fit->second->WaitOpen();
      eos_info("status=final url=%s", fit->second->url().c_str());
    } else {
      eos_info("status=final url=%s", fit->second->url().c_str());
    }
  }
}


/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
data::datax::TryRecovery(fuse_req_t req, bool iswrite)
{
  if (req && fuse_req_interrupted(req))
  {
    eos_warning("request interrupted");
    return EINTR;
  }

  if (mReadErrorStack.size() > 128)
  {
    std::string stack_dump;
    // we give up recovery if to many recoveries took place
    for (auto it = mReadErrorStack.begin(); it != mReadErrorStack.end(); ++it)
    {
      stack_dump += "\n";
      stack_dump += *it;
    }
    eos_err("giving up recovery - error-stack:%s", stack_dump.c_str());
    return EREMOTEIO;
  }


  if (iswrite)
  {
    // recover write failures
    if (!EosFuse::Instance().Config().recovery.write) // might be disabled
    {
      eos_warning("write recovery disabled");
      return EREMOTEIO; 
    }


    if (!mFile->has_xrdiorw(req))
    {
      eos_crit("no proxy object");
      return EFAULT;
    }
    
    XrdCl::Proxy* proxy = mFile->xrdiorw(req);

    switch (proxy->stateTS()) {    
    case XrdCl::Proxy::FAILED:
    case XrdCl::Proxy::OPENED:
    case XrdCl::Proxy::WAITWRITE:
    default:
      eos_crit("triggering write recovery");
      return recover_write(req);
      eos_crit("default action");
    }
  }
  else
  {
    // recover read failures
    if (!EosFuse::Instance().Config().recovery.read) // might be disabled
      return EREMOTEIO;

    // no way to recover
    if (!mFile->has_xrdioro(req)) 
    {
      eos_crit("no proxy object");
      return EFAULT;
    }

    XrdCl::Proxy* proxy = mFile->xrdioro(req);
    
    switch (proxy->stateTS()) {
    case XrdCl::Proxy::FAILED:
      mReadErrorStack.push_back("open-failed");
      return recover_ropen(req);
      
    case XrdCl::Proxy::OPENED:
      mReadErrorStack.push_back("read-failed");
      return recover_read(req);
      
    default:
      break;
    }
  }
  return EREMOTEIO;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
data::datax::recover_ropen(fuse_req_t req)
/* -------------------------------------------------------------------------- */
{
  XrdCl::Proxy* proxy = mFile->xrdioro(req);
  
  struct timespec ts;
  eos::common::Timing::GetTimeSpec(ts, true);
  
  while (1) 
  {
    eos_warning("recover read-open [%d]", EosFuse::Instance().Config().recovery.read_open);
    
    if (!EosFuse::Instance().Config().recovery.read_open) // might be disabled
      break;
    
    XrdCl::XRootDStatus status = proxy->opening_state();
    if (status.errNo == kXR_noserver)
    {
      eos_crit("recover read-open-noserver [%d]", EosFuse::Instance().Config().recovery.read_open_noserver);
      // there is no server to read that file
      if (!EosFuse::Instance().Config().recovery.read_open_noserver) // might be disabled
      {      
	return ENETUNREACH;
      }
    }
    
    eos_warning("recover reopening file for read");
    
    XrdCl::OpenFlags::Flags targetFlags = XrdCl::OpenFlags::Read;
    XrdCl::Access::Mode mode = XrdCl::Access::UR | XrdCl::Access::UX;

    // retrieve the 'tried' information to apply this for the file-reopening to exclude already knowns 'bad' locations
    std::string slasturl;
    proxy->GetProperty("LastURL", slasturl);

    XrdCl::URL lasturl(slasturl);
    XrdCl::URL newurl(mRemoteUrlRO);

    XrdCl::URL::ParamsMap last_cgi = lasturl.GetParams();
    XrdCl::URL::ParamsMap new_cgi = newurl.GetParams();

    std::string last_host = lasturl.GetHostName();

    if ( (lasturl.GetHostName()  != newurl.GetHostName()) ||
	 (lasturl.GetPort() != newurl.GetPort()) )
    {
      eos_warning("applying exclusion list: tried=%s,%s",last_host.c_str(), new_cgi["tried"].c_str());
      new_cgi["tried"] = last_host.c_str() + std::string(",") + last_cgi["tried"];
      newurl.SetParams(new_cgi);
      mRemoteUrlRO = newurl.GetURL();
    }
    else 
    {
      new_cgi.erase("tried");
      newurl.SetParams(new_cgi);
      mRemoteUrlRO = newurl.GetURL();
    }

    // issue a new open 
    XrdCl::Proxy* newproxy = new XrdCl::Proxy();
    newproxy->OpenAsync(mRemoteUrlRO.c_str(), targetFlags, mode, 0);
    // wait this time for completion
    
    if ((req && fuse_req_interrupted(req)) || (newproxy->WaitOpen(req)==EINTR))
    {
      eos_warning("request interrupted");
      return EINTR;
    }
    
    newproxy->inherit_attached(proxy);

    // replace the proxy object
    mFile->set_xrdioro(req, newproxy);

    // once all callbacks are there, this object can destroy itself since we don't track it anymore
    proxy->flag_selfdestructionTS();

    if (newproxy->stateTS() == XrdCl::Proxy::OPENED) // that worked !
    {
      eos_warning("recover reopened file successfully");
      return 0;
    }
    
    // that failed again ...
    status = proxy->opening_state();
    if (status.errNo == kXR_noserver)
    {
      double retry_time_sec = 1.0 * eos::common::Timing::GetCoarseAgeInNs(&ts,
									  0) / 1000000000.0;
      
      eos_warning("recover no server retry window [ %.02f/%.02f ]",
		  retry_time_sec, 
		  EosFuse::Instance().Config().recovery.read_open_noserver_retrywindow
		  );
      
      // check how long we are supposed to retry 
      if (retry_time_sec < EosFuse::Instance().Config().recovery.read_open_noserver_retrywindow)
      {
	eos_warning("recover no server retry in 5 seconds");
	for (auto i = 0; i< 50; ++i)
	{
	  // sleep for 5s and then try again
	  std::this_thread::sleep_for(std::chrono::milliseconds(100));
	  
	  if (req && fuse_req_interrupted(req)) 
	  {
	    eos_warning("request interrupted");
	    return EINTR;
	  }
	}
	// empty the tried= CGI tag
	new_cgi.erase("tried");
	newurl.SetParams(new_cgi);
	mRemoteUrlRO = newurl.GetURL();
	continue;
      }
      break;
    }
    break;
  }
  return EREMOTEIO;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
data::datax::try_ropen(fuse_req_t req, XrdCl::Proxy* &proxy, std::string open_url)
{
  struct timespec ts;
  eos::common::Timing::GetTimeSpec(ts, true);

  XrdCl::OpenFlags::Flags targetFlags = XrdCl::OpenFlags::Read;
  XrdCl::Access::Mode mode = XrdCl::Access::UR | XrdCl::Access::UX;
  
  proxy->OpenAsync(open_url, targetFlags, mode, 0);
  // wait this time for completion
  if ((req && fuse_req_interrupted(req)) || (proxy->WaitOpen(req)==EINTR))
  {
    eos_warning("request interrupted");
    return EINTR;
  }

  if (proxy->stateTS() == XrdCl::Proxy::OPENED) // that worked !  
  {
    return 0;
  }

  while (1) 
  {
    eos_warning("recover read-open [%d]", EosFuse::Instance().Config().recovery.read_open);
    
    if (!EosFuse::Instance().Config().recovery.read_open) // might be disabled
      break;
    
    XrdCl::XRootDStatus status = proxy->opening_state();
    if (status.errNo == kXR_noserver)
    {
      eos_crit("recover read-open-noserver [%d]", EosFuse::Instance().Config().recovery.read_open_noserver);
      // there is no server to read that file
      if (!EosFuse::Instance().Config().recovery.read_open_noserver) // might be disabled
      {      
	return ENETUNREACH;
      }
    }
    
    eos_warning("recover reopening file for read");
    
    // retrieve the 'tried' information to apply this for the file-reopening to exclude already knowns 'bad' locations
    std::string slasturl;
    proxy->GetProperty("LastURL", slasturl);

    XrdCl::URL lasturl(slasturl);
    XrdCl::URL newurl(open_url);

    XrdCl::URL::ParamsMap last_cgi = lasturl.GetParams();
    XrdCl::URL::ParamsMap new_cgi = newurl.GetParams();

    std::string last_host = lasturl.GetHostName();

    if ( (lasturl.GetHostName()  != newurl.GetHostName()) ||
	 (lasturl.GetPort() != newurl.GetPort()) )
    {
      eos_warning("applying exclusion list: tried=%s,%s",last_host.c_str(), new_cgi["tried"].c_str());
      new_cgi["tried"] = last_host.c_str() + std::string(",") + last_cgi["tried"];
      newurl.SetParams(new_cgi);
      open_url = newurl.GetURL();
    }
    else 
    {
      new_cgi.erase("tried");
      newurl.SetParams(new_cgi);
      open_url = newurl.GetURL();
    }

    // issue a new open 
    XrdCl::Proxy* newproxy = new XrdCl::Proxy();
    newproxy->OpenAsync(open_url.c_str(), targetFlags, mode, 0);
    // wait this time for completion
    
    if ((req && fuse_req_interrupted(req)) || (newproxy->WaitOpen(req)==EINTR))
    {
      eos_warning("request interrupted");
      return EINTR;
    }
    
    newproxy->inherit_attached(proxy);

    // once all callbacks are there, this object can destroy itself since we don't track it anymore
    proxy->flag_selfdestructionTS();

    // replace the proxy object
    proxy = newproxy;

    if (newproxy->stateTS() == XrdCl::Proxy::OPENED) // that worked !
    {
      eos_warning("recover reopened file successfully");
      return 0;
    }
    
    // that failed again ...
    status = proxy->opening_state();
    if (status.errNo == kXR_noserver)
    {
      double retry_time_sec = 1.0 * eos::common::Timing::GetCoarseAgeInNs(&ts,
									  0) / 1000000000.0;
      
      eos_warning("recover no server retry window [ %.02f/%.02f ]",
		  retry_time_sec, 
		  EosFuse::Instance().Config().recovery.read_open_noserver_retrywindow
		  );
      
      // check how long we are supposed to retry 
      if (retry_time_sec < EosFuse::Instance().Config().recovery.read_open_noserver_retrywindow)
      {
	eos_warning("recover no server retry in 5 seconds");
	for (auto i = 0; i< 50; ++i)
	{
	  // sleep for 5s and then try again
	  std::this_thread::sleep_for(std::chrono::milliseconds(100));
	  
	  if (req && fuse_req_interrupted(req)) 
	  {
	    eos_warning("request interrupted");
	    return EINTR;
	  }
	}
	// empty the tried= CGI tag
	new_cgi.erase("tried");
	newurl.SetParams(new_cgi);
	open_url = newurl.GetURL();
	continue;
      }
      break;
    }
    break;
  }
  return EREMOTEIO;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
data::datax::try_wopen(fuse_req_t req, XrdCl::Proxy* &proxy, std::string open_url)
{   
  struct timespec ts;
  eos::common::Timing::GetTimeSpec(ts, true);
  

  // try to open this file for writing
  XrdCl::OpenFlags::Flags targetFlags = XrdCl::OpenFlags::Update;
  XrdCl::Access::Mode mode = XrdCl::Access::UR | XrdCl::Access::UW |
    XrdCl::Access::UX;
  
  XrdCl::XRootDStatus status = proxy->OpenAsync(open_url.c_str(), targetFlags, mode, 0);

  if (proxy->stateTS() == XrdCl::Proxy::OPENED) // that worked !  
  {
    return 0;
  }

  while (1) 
  {
    eos_warning("recover write-open [%d]", EosFuse::Instance().Config().recovery.read_open);
    
    if (!EosFuse::Instance().Config().recovery.read_open) // might be disabled
      break;
    
    XrdCl::XRootDStatus status = proxy->opening_state();
    if (status.errNo == kXR_noserver)
    {
      eos_crit("recover write-open-noserver [%d]", EosFuse::Instance().Config().recovery.write_open_noserver);
      // there is no server to read that file
      if (!EosFuse::Instance().Config().recovery.write_open_noserver) // might be disabled
      {      
	return ENETUNREACH;
      }
    }
    
    eos_warning("recover reopening file for writing");
    
    // issue a new open 
    XrdCl::Proxy* newproxy = new XrdCl::Proxy();
    newproxy->OpenAsync(open_url.c_str(), targetFlags, mode, 0);
    // wait this time for completion
    
    if ((req && fuse_req_interrupted(req)) || (newproxy->WaitOpen(req)==EINTR))
    {
      eos_warning("request interrupted");
      return EINTR;
    }
    
    newproxy->inherit_attached(proxy);

    // once all callbacks are there, this object can destroy itself since we don't track it anymore
    proxy->flag_selfdestructionTS();

    // replace the proxy object
    proxy = newproxy;

    if (newproxy->stateTS() == XrdCl::Proxy::OPENED) // that worked !
    {
      eos_warning("recover reopened file successfully");
      return 0;
    }
    
    // that failed again ...
    status = proxy->opening_state();
    if (status.errNo == kXR_noserver)
    {
      double retry_time_sec = 1.0 * eos::common::Timing::GetCoarseAgeInNs(&ts,
									  0) / 1000000000.0;
      
      eos_warning("recover no server retry window [ %.02f/%.02f ]",
		  retry_time_sec, 
		  EosFuse::Instance().Config().recovery.read_open_noserver_retrywindow
		  );
      
      // check how long we are supposed to retry 
      if (retry_time_sec < EosFuse::Instance().Config().recovery.read_open_noserver_retrywindow)
      {
	eos_warning("recover no server retry in 5 seconds");
	for (auto i = 0; i< 50; ++i)
	{
	  // sleep for 5s and then try again
	  std::this_thread::sleep_for(std::chrono::milliseconds(100));
	  
	  if (req && fuse_req_interrupted(req)) 
	  {
	    eos_warning("request interrupted");
	    return EINTR;
	  }
	}
	continue;
      }
      break;
    }
    break;
  }
  return EREMOTEIO;
}


/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
data::datax::recover_read(fuse_req_t req)
/* -------------------------------------------------------------------------- */
{  
  XrdCl::Proxy* proxy = mFile->xrdioro(req);

  // recover a pread error
  XrdCl::XRootDStatus status = proxy->read_state();
  if (req && fuse_req_interrupted(req)) 
  {
    eos_warning("request interrupted");
    return EINTR;
  }
  
  // re-open the file
  int reopen = recover_ropen(req);

  if (!reopen)
  {
    eos_warning("recover reopened file successfully to re-read");
    // let's try again
    return 0;
  }

  return reopen;
}


/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
data::datax::recover_write(fuse_req_t req)
/* -------------------------------------------------------------------------- */
{
  eos_debug("");
  XrdCl::Proxy* proxy = mFile->xrdiorw(req);

  // try to open this file for reading

  XrdCl::XRootDStatus status;

  bool recover_from_file_cache = false;

  // check if the file has been created here and is still complete in the local caches
  if ( (mFlags & O_CREAT) &&
       (mSize <= mFile->file()->prefetch_size()) &&
       (mSize == (ssize_t)mFile->file()->size()) )
  {
    eos_debug("recover from file cache");
    // this file can be recovered from the file start cache
    recover_from_file_cache = true;
  }
  else 
  {
    // we have to recover this from remote
    eos_debug("recover from remote file");
    recover_from_file_cache = false;
  }

  XrdCl::Proxy* aproxy = new XrdCl::Proxy();
  
  if (!recover_from_file_cache)
  {
    // we need to open this file because it is not complete locally
    int rc = try_ropen(req, aproxy, mRemoteUrlRW);
    
    if (rc)
    {
      delete proxy;
      return rc;
    }
  }

  std::unique_ptr<XrdCl::Proxy> newproxy(aproxy);
  
  if (mFile->file())
  {
    std::string stagefile;
    mFile->file()->recovery_location(stagefile);
    off_t off = 0 ;
    uint32_t size = 1*1024*1024;
    
    bufferllmanager::shared_buffer buffer = sBufferManager.get_buffer(size);
    
    void* buf = (void*)buffer->ptr();
    
    // open local stagefile
    int fd = ::open(stagefile.c_str(), O_CREAT | O_RDWR, S_IRWXU);
    ::unlink (stagefile.c_str());
    
    if (fd < 0) 
    {
      sBufferManager.put_buffer(buffer);
      eos_crit("failed to open local stagefile %s", stagefile.c_str());
      return EREMOTEIO;
    }

    if (req && begin_flush(req))
    {
      eos_warning("failed to signal begin-flush");
    }
    
    if (recover_from_file_cache)
    {
      eos_debug("recovering from local start cache into stage file %s", stagefile.c_str());
      // recover file from the local start cache
      if (mFile->file()->pread(buf, mFile->file()->size(),0) < 0)
      {
	sBufferManager.put_buffer(buffer);
	close(fd);
	eos_crit("unable to read file for recovery from local file cache");

	if (req && end_flush(req))
	{
	  eos_warning("failed to signal end-flush");
	}
	return EIO;
      }
    }
    else
    {
      eos_debug("recovering from remote file into stage file %s", stagefile.c_str());
      // download all into local stagefile
      uint32_t bytesRead=0;
      do {
	status = newproxy->Read(off, size, buf, bytesRead);

	eos_debug("off=%lu bytesread=%u", off, bytesRead);
	if (!status.IsOK())
	{
	  sBufferManager.put_buffer(buffer);
	  eos_warning("failed to read remote file for recovery");
	  ::close(fd);
	  if (req && end_flush(req))
	  {
	    eos_warning("failed to signal end-flush");
	  }
	  return EREMOTEIO;
	}
	else
	{
	  off += bytesRead;
	}
	ssize_t wr = ::write(fd, buf, bytesRead);
	if (wr != bytesRead)
	{
	  sBufferManager.put_buffer(buffer);
	  eos_crit("failed to write to local stage file %s", stagefile.c_str());
	  ::close(fd);
	  if (req && end_flush(req))
	  {
	    eos_warning("failed to signal end-flush");
	  }
	  return EREMOTEIO;
	}
      } while (bytesRead>0);
    }
      
    // upload into identical inode using the drop & replace option (repair flag)
    XrdCl::Proxy* uploadproxy = new XrdCl::Proxy();
    
    uploadproxy->inherit_attached(proxy);


    // we have to remove the flush otherwise we cannot open this file even ourselfs
    if (req && end_flush(req))
    {
      eos_warning("failed to signal begin-flush");
    }

    // add the repair flag to drop existing locations and select new ones
    mRemoteUrlRW += "&eos.repair=1";
    
    eos_warning("re-opening with repair flag for recovery %s", mRemoteUrlRW.c_str());

    int rc = try_wopen(req, uploadproxy, mRemoteUrlRW);
    mRemoteUrlRW.erase(mRemoteUrlRW.length() - std::string("&eos.repair=1").length());

    // put back the flush indicator
    if (req && begin_flush(req))
    {
      eos_warning("failed to signal begin-flush");
    }

    if ( rc )
    {
      sBufferManager.put_buffer(buffer);
      ::close(fd);
      delete uploadproxy;
      if (req && end_flush(req))
      {
	eos_warning("failed to signal end-flush");
      }
      return rc;
    }      
    
    off_t upload_offset=0;
    ssize_t nr = 0;
    
    do {
      nr = ::pread(fd, buf, size, upload_offset);
      if (nr < 0)
      {
	sBufferManager.put_buffer(buffer);
	eos_crit("failed to read from local stagefile");
	::close(fd);
	if (req && end_flush(req))
	{
	  eos_warning("failed to signal end-flush");
	}
	return EREMOTEIO;
      }
      
      if (nr)
      {
	// send asynchronous upstream writes
	XrdCl::Proxy::write_handler handler = uploadproxy->WriteAsyncPrepare(nr, upload_offset, 0);
	uploadproxy->ScheduleWriteAsync(buf, handler);
	upload_offset += nr;
      }
    } while (nr>0);
    
    ::close(fd);

    // collect the writes to verify everything is alright now
    uploadproxy->WaitWrite(req);
    
    if ( !uploadproxy->write_state().IsOK())
    {
      sBufferManager.put_buffer(buffer);
      eos_crit("got failure when collecting outstanding writes from the upload proxy");
      ::close(fd);
      delete uploadproxy;
      if (req && end_flush(req))
      {
	eos_warning("failed to signal end-flush");
      }
      return EREMOTEIO;
    }
    
    sBufferManager.put_buffer(buffer);

    eos_notice("finished write recovery successfully");
    
    // replace the proxy object
    mFile->set_xrdiorw(req, uploadproxy);
    
    // re-open the file centrally for access
    if (req && end_flush(req))
    {
	eos_warning("failed to signal end-flush");
    }
    // once all callbacks are there, this object can destroy itself since we don't track it anymore
    proxy->flag_selfdestructionTS();
  }
  else
  {
    eos_crit("no local cache data for recovery");
    return EREMOTEIO;
  }

  return 0;
}


/* -------------------------------------------------------------------------- */
int
data::datax::begin_flush(fuse_req_t req)
/* -------------------------------------------------------------------------- */
{
  return EosFuse::Instance().mds.begin_flush(req, mMd,
					     std::string("repair")); // flag an ongoing flush centrally
}


/* -------------------------------------------------------------------------- */
int
data::datax::end_flush(fuse_req_t req)
/* -------------------------------------------------------------------------- */
{
  return EosFuse::Instance().mds.end_flush(req, mMd,
					   std::string("repair")); // unflag an ongoing flush centrally
}




/* -------------------------------------------------------------------------- */
int
data::datax::detach(fuse_req_t req, std::string& cookie, int flags)
/* -------------------------------------------------------------------------- */
{
  bool isRW = false;

  if (flags & (O_RDWR | O_WRONLY)) {
    isRW = true;
  }

  eos_info("cookie=%s flags=%o isrw=%d", cookie.c_str(), flags, isRW);
  int rflush = flush(req);
  XrdSysMutexHelper lLock(mLock);
  int bcache = mFile->file() ? mFile->file()->detach(cookie) : 0;
  int jcache = mFile->journal() ? mFile->journal()->detach(cookie) : 0;
  int xio = 0;

  if (isRW) {
    if (mFile->has_xrdiorw(req)) {
      mFile->xrdiorw(req)->detach();
    }
  } else {
    if (mFile->has_xrdioro(req)) {
      mFile->xrdioro(req)->detach();
    }
  }

  return rflush | bcache | jcache | xio;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
data::datax::store_cookie(std::string& cookie)
/* -------------------------------------------------------------------------- */
{
  eos_info("cookie=%s", cookie.c_str());
  int bc = mFile->file() ? mFile->file()->set_cookie(cookie) : 0;
  int jc = mFile->journal() ? mFile->journal()->set_cookie(cookie) : 0;
  return bc | jc;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
data::datax::unlink(fuse_req_t req)
/* -------------------------------------------------------------------------- */
{
  eos_info("");
  cachehandler::instance().rm(mIno);
  int bcache = mFile->file() ? mFile->file()->unlink() : 0;
  int jcache = mFile->journal() ? mFile->journal()->unlink() : 0;
  return bcache | jcache;
}
// IO bridge interface

/* -------------------------------------------------------------------------- */
ssize_t
/* -------------------------------------------------------------------------- */
data::datax::pread(fuse_req_t req, void* buf, size_t count, off_t offset)
/* -------------------------------------------------------------------------- */
{
  eos_info("offset=%llu count=%lu", offset, count);
  mLock.Lock();

  if (mFile->journal()) {
    ssize_t jts = ((mFile->journal()))->get_truncatesize();

    if (jts >= 0) {
      // reduce reads in case of truncation stored in the journal
      if ((ssize_t) offset > jts) {
        offset = 0;
        count = 0;
      } else {
        if ((ssize_t)(offset + count) > jts) {
          count = jts - offset;
        }
      }
    }
  }

  ssize_t br = 0;

  if (mFile->file()) {
    // read from file start cache
    br = mFile->file()->pread(buf, count, offset);
  }

  if (br < 0) {
    mLock.UnLock();
    return br;
  }

  if (br == (ssize_t) count) {
    mLock.UnLock();
    return br;
  }

  if (mFile->file() && (offset < mFile->file()->prefetch_size())) {
    mLock.UnLock();

    if (prefetch(req)) {
      WaitPrefetch(req);
      mLock.Lock();
      ssize_t br = mFile->file()->pread(buf, count, offset);

      if (br < 0) {
        return br;
      }

      if (br == (ssize_t) count) {
        return br;
      }
    } else {
      mLock.Lock();
    }
  }

  ssize_t jr = 0;

  if (mFile->journal()) {
    // read from journal cache
    jr = mFile->journal() ? mFile->journal()->pread((char*) buf + br, count - br,
         offset + br) : 0;
  }

  if (jr < 0) {
    mLock.UnLock();
    return jr;
  }

  if ((br + jr) == (ssize_t) count) {
    mLock.UnLock();
    return (br + jr);
  }

  // read the missing part remote
  XrdCl::Proxy* proxy = mFile->has_xrdioro(req) ? mFile->xrdioro(
                          req) : mFile->xrdiorw(req);

  if (proxy) {
    if (!mFile->is_caching()) {
      // if caching is disabled, we wait for outstanding writes
      XrdCl::XRootDStatus status = proxy->WaitWrite();
      // it is not obvious what we should do if there was a write error,
      // we just proceed
    }

    uint32_t bytesRead = 0;

    if (proxy->Read(offset + br ,
                    count - br ,
                    (char*) buf + br ,
                    bytesRead).IsOK()) {
      mLock.UnLock();
      std::vector<journalcache::chunk_t> chunks;

      if (mFile->journal()) {
        jr = 0;
        // retrieve all journal chunks matching our range
        chunks = ((mFile->journal()))->get_chunks(offset + br + jr, count - br - jr);

        for (auto it = chunks.begin(); it != chunks.end(); ++it) {
          eos_err("offset=%ld count=%lu overlay-chunk offset=%ld size=%lu\n", offset,
                  count, it->offset, it->size);
          // overlay journal contents again over the remote contents
          ssize_t ljr = mFile->journal()->pread((char*) buf + br + jr +
                                                (it->offset - offset - br - jr) , it->size, it->offset);

          if (ljr >= 0) {
            // check if the journal contents extends the remote read
            ssize_t chunkread = it->offset + it->size - offset - br;

            if (chunkread > bytesRead) {
              bytesRead = chunkread;
            }
          }
        }
      }
      return (br + jr + bytesRead);
    }
    else
    {
      mLock.UnLock();
      errno = EREMOTEIO;
      // IO error
      return -1;
    }
  }

  mLock.UnLock();
  errno = EFAULT;
  return -1;
}

/* -------------------------------------------------------------------------- */
ssize_t
/* -------------------------------------------------------------------------- */
data::datax::pwrite(fuse_req_t req, const void* buf, size_t count, off_t offset)
/* -------------------------------------------------------------------------- */
{
  XrdSysMutexHelper lLock(mLock);
  eos_info("offset=%llu count=%lu", offset, count);
  ssize_t dw = 0 ;

  if (mFile->file()) {
    if (mFile->file()->size() || (mFlags & O_CREAT))
    {
      // don't write into the file start cache, if it is currently empty and it is not a newly created file
      dw = mFile->file()->pwrite(buf, count, offset);
    }
  }

  if (dw < 0) {
    return dw;
  } else {
    if (mFile->journal()) {
      if (!mFile->journal()->fits(count)) {
	flush_nolock(req, true, true);
      }

      // now there is space to write for us
      ssize_t jw = mFile->journal()->pwrite(buf, count, offset);

      if (jw < 0) {
        return jw;
      }

      dw = jw;
    }

    // send an asynchronous upstream write, which does not wait for the file open to be done
    XrdCl::Proxy::write_handler handler =
      mFile->xrdiorw(req)->WriteAsyncPrepare(count, offset, 0);
    XrdCl::XRootDStatus status =
      mFile->xrdiorw(req)->ScheduleWriteAsync(buf, handler);


    if (!status.IsOK())
    {
      errno = XrdCl::Proxy::status2errno (status);
      eos_err("async remote-io failed msg=\"%s\"", status.ToString().c_str());
      return -1;
    }

    if (mFlags & O_SYNC) {
      // make sure the file gets opened
      XrdCl::XRootDStatus status = mFile->xrdiorw(req)->WaitOpen();
      if (!status.IsOK())
      {
	if (TryRecovery(req, true))
	{
	  errno = XrdCl::Proxy::status2errno (status);
	  eos_err("pseudo-sync remote-io failed msg=\"%s\"", status.ToString().c_str());
	  return -1;
	}
	else 
	{
	  // re-send the write again
	  XrdCl::Proxy::write_handler handler =
	    mFile->xrdiorw(req)->WriteAsyncPrepare(count, offset, 0);
	  XrdCl::XRootDStatus status =
	    mFile->xrdiorw(req)->ScheduleWriteAsync(buf, handler);
	}
      }

      // make sure all writes were successfull
      status = mFile->xrdiorw(req)->WaitWrite();
      if (!status.IsOK())
      {
	if (TryRecovery(req, true))
	{
	  errno = XrdCl::Proxy::status2errno (status);
	  eos_err("pseudo-sync remote-io failed msg=\"%s\"", status.ToString().c_str());
	  return -1;
	}
	else
	{
	  // re-send the write again                                                                                                                                          
	  XrdCl::Proxy::write_handler handler =
            mFile->xrdiorw(req)->WriteAsyncPrepare(count, offset, 0);
	  XrdCl::XRootDStatus status =
            mFile->xrdiorw(req)->ScheduleWriteAsync(buf, handler);
	  status = mFile->xrdiorw(req)->WaitWrite();
	  if (!status.IsOK())
	  {
	    errno = XrdCl::Proxy::status2errno (status);
	    eos_err("pseudo-sync remote-io failed msg=\"%s\"", status.ToString().c_str());
	    return -1;
	  }
	}
      }
    }
  }

  if ((off_t)(offset + count) > mSize) {
    mSize = count + offset;
  }

  eos_info("offset=%llu count=%lu result=%d", offset, count, dw);
  return dw;
}

/* -------------------------------------------------------------------------- */
ssize_t
/* -------------------------------------------------------------------------- */
data::datax::peek_pread(fuse_req_t req, char*& buf, size_t count, off_t offset)
/* -------------------------------------------------------------------------- */
{
  mLock.Lock();
  eos_info("offset=%llu count=%lu size=%lu", offset, count, mMd->size());

  if (mFile->journal()) {
    ssize_t jts = ((mFile->journal()))->get_truncatesize();

    if (jts >= 0) {
      // reduce reads in case of truncation stored in the journal
      if ((ssize_t) offset > jts) {
        offset = 0;
        count = 0;
      } else {
        if ((ssize_t)(offset + count) > jts) {
          count = jts - offset;
        }
      }
    }
  }

  buffer = sBufferManager.get_buffer(count);

  buf = buffer->ptr();

  ssize_t br = 0;

  if (mFile->file()) {
    br = mFile->file()->pread(buf, count, offset);

    if (br < 0) {
      return br;
    }

    if ((br == (ssize_t) count) || (br == (ssize_t) mMd->size())) {
      return br;
    }
  }

  if (mFile->file() && (offset < mFile->file()->prefetch_size())) {
    if (prefetch(req, false)) {
      WaitPrefetch(req, false);
      ssize_t br = mFile->file()->pread(buf, count, offset);

      if (br < 0) {
        return br;
      }

      if (br == (ssize_t) count) {
        return br;
      }
    }
  }

  ssize_t jr = 0;

  if (mFile->journal()) {
    jr = mFile->journal() ? mFile->journal()->pread(buf + br, count - br,
         offset + br) : 0;

    if (jr < 0) {
      return jr;
    }

    if ((br + jr) == (ssize_t) count) {
      return (br + jr);
    }
  }

  // read the missing part remote
  XrdCl::Proxy* proxy =  mFile->has_xrdioro(req) ? mFile->xrdioro(req) : mFile->xrdiorw(req);

  XrdCl::XRootDStatus status;
  eos_debug("ro=%d offset=%llu count=%lu br=%lu jr=%lu", mFile->has_xrdioro(req), offset, count, br, jr);

  if (proxy) {
    if (proxy->IsOpening())
    {
      status = proxy->WaitOpen();
    }

    if (mFile->has_xrdiorw(req)) {
      if (!status.IsOK())
      {
	// call recovery for an open
	if (TryRecovery(req, false))
	{
	  errno = XrdCl::Proxy::status2errno (status);
	  eos_err("sync remote-io failed msg=\"%s\"", status.ToString().c_str());
	  return -1;
	}
	else 
	{
	  // get the new proxy object, the recovery might exchange the file object
	  proxy = mFile->has_xrdioro(req) ? mFile->xrdioro(req) : mFile->xrdiorw(req); // recovery might change the proxy object
	}
      }
    }

    if (mFile->has_xrdiorw(req)) {
      XrdCl::Proxy* wproxy = mFile->xrdiorw(req);
      if (wproxy->OutstandingWrites())
      {
	status = wproxy->WaitWrite();
      }
      if (!status.IsOK())
      {
	errno = XrdCl::Proxy::status2errno (status);
	eos_err("sync remote-io failed msg=\"%s\"", status.ToString().c_str());
	return -1;
      }
    }

    uint32_t bytesRead = 0;
    int recovery = 0;

    do {
      proxy = mFile->has_xrdioro(req) ? mFile->xrdioro(req) : mFile->xrdiorw(req); // recovery might change the proxy object
      status = proxy->Read(offset + br + jr,
			   count - br - jr,
			   (char*) buf + br + jr,
			   bytesRead);
    } while (!status.IsOK() && (!(recovery = TryRecovery(req, false))));

    if (recovery)
    {
      errno = recovery;
      eos_err("sync remote-io recovery failed errno=%d", errno);
      return -1;
    }

    if (status. IsOK()) {
      std::vector<journalcache::chunk_t> chunks;

      if (mFile->journal()) {
        jr = 0;
        // retrieve all journal chunks matching our range
        chunks = ((mFile->journal()))->get_chunks(offset + br + jr, count - br - jr);

        for (auto it = chunks.begin(); it != chunks.end(); ++it) {
          eos_info("offset=%ld count=%lu overlay-chunk offset=%ld size=%lu", offset,
                   count, it->offset, it->size);
          // overlay journal contents again over the remote contents
          ssize_t ljr = mFile->journal()->pread((char*) buf + br + jr +
                                                (it->offset - offset - br - jr) , it->size, it->offset);

          if (ljr >= 0) {
            // check if the journal contents extends the remote read
            ssize_t chunkread = it->offset + it->size - offset - br;

            if (chunkread > bytesRead) {
              bytesRead = chunkread;
            }
          }
        }
      }
      return (br + jr + bytesRead);
    }
    else
    {
      errno = XrdCl::Proxy::status2errno (status);

      eos_err("sync remote-io failed msg=\"%s\"", status.ToString().c_str());
      return -1;
    }
  }

  return -1;
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
data::datax::release_pread()
/* -------------------------------------------------------------------------- */
{
  eos_info("");
  sBufferManager.put_buffer(buffer);
  buffer.reset();
  mLock.UnLock();
  return;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
data::datax::truncate(fuse_req_t req, off_t offset)
/* -------------------------------------------------------------------------- */
{
  XrdSysMutexHelper lLock(mLock);
  eos_info("offset=%llu size=%llu", offset, mSize);

  int dt = 0;

  if (mFile->file()) {
    dt = mFile->file()->truncate(0);
  }

  // if we have a journal it tracks the truncation size
  int jt = 0;

  if (mFile->journal()) {
    jt = mFile->journal() ? mFile->journal()->truncate(offset) : 0;
  }

  eos_info("dt=%d jt=%d", dt, jt);

  if (!mFile->journal()) {
    if (mFile->has_xrdiorw(req)) {
      if (mFile->xrdiorw(req)->IsOpening()) {
        mFile->xrdiorw(req)->WaitOpen();
      }

      mFile->xrdiorw(req)->WaitWrite();

      // the journal keeps track of truncation, otherwise or for O_SYNC we do it here
      XrdCl::XRootDStatus status = mFile->xrdiorw(req)->Truncate( offset );

      errno = XrdCl::Proxy::status2errno (status);

      if ( !status.IsOK() )
      {
        return -1;
      }
    }
    else
    {
      errno = EFAULT;
      return -1;
    }
  }

  if (! (dt | jt) )
    mSize = offset;

  return dt | jt;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
data::datax::sync()
/* -------------------------------------------------------------------------- */
{
  eos_info("");
  int ds = 0;

  if (mFile->file()) {
    ds = mFile->file()->sync();
  }

  int js = 0;

  if (mFile->journal()) {
    js = mFile->journal() ? mFile->journal()->sync() : 0;
  }

  bool journal_recovery = false;

  for (auto it = mFile->get_xrdiorw().begin();
       it != mFile->get_xrdiorw().end(); ++it) {
    if (it->second->IsOpening()) {
      it->second->WaitOpen();
    }

    XrdCl::XRootDStatus status = it->second->WaitWrite();
    if (!status.IsOK())
    {
      errno = XrdCl::Proxy::status2errno (status);
      journal_recovery = true;
    } else {
      status = it->second->Sync();
      if (!status.IsOK())
      {
	errno = XrdCl::Proxy::status2errno (status);
        journal_recovery = true;
      }
    }
  }

  if (journal_recovery) {
    eos_err("syncing failed");
    return -1;
  }

  return ds | js;
}

/* -------------------------------------------------------------------------- */
size_t
/* -------------------------------------------------------------------------- */
data::datax::size()
/* -------------------------------------------------------------------------- */
{
  eos_info("");
  off_t dsize = mFile->file() ? mFile->file()->size() : 0 ;

  if (mSize > dsize) {
    return mSize;
  }

  return dsize;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
data::datax::cache_invalidate()
/* -------------------------------------------------------------------------- */
{
  eos_info("");
  XrdSysMutexHelper lLock(mLock);
  // truncate the block cache
  int dt = mFile->file()->truncate(0);
  int jt = mFile->journal() ? mFile->journal()->truncate(0, true) : 0;

  for (auto fit = mFile->get_xrdioro().begin();
       fit != mFile->get_xrdioro().end(); ++fit)
  {
    if (fit->second->IsOpen())
    {
      fit->second->DropReadAhead();
    }
  }

  return dt | jt;
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
data::datax::set_remote(const std::string& hostport,
                        const std::string& basename,
                        const uint64_t md_ino,
                        const uint64_t md_pino,
                        fuse_req_t req,
                        bool isRW)
/* -------------------------------------------------------------------------- */
{
  eos_info("");
  std::string remoteurl;
  remoteurl = "root://";
  remoteurl += hostport;
  remoteurl += "//fusex-open";
  remoteurl += "?eos.lfn=";

  if (md_ino) {
    remoteurl += "ino:";
    char sino[128];
    snprintf(sino, sizeof(sino), "%lx", md_ino);
    remoteurl += sino;
  } else {
    remoteurl += "pino:";
    char pino[128];
    snprintf(pino, sizeof(pino), "%lx", md_pino);
    remoteurl += pino;
    remoteurl += "/";
    remoteurl += basename;
  }

  remoteurl += "&eos.app=fuse&mgm.mtime=0&mgm.fusex=1&eos.bookingsize=0";
  XrdCl::URL url(remoteurl);
  XrdCl::URL::ParamsMap query = url.GetParams();
  fusexrdlogin::loginurl(url, query, req, md_ino);
  url.SetParams(query);
  remoteurl = url.GetURL();

  if (isRW) {
    mRemoteUrlRW = remoteurl;
  } else {
    mRemoteUrlRO = remoteurl;
  }
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
data::dmap::ioflush(ThreadAssistant& assistant)
/* -------------------------------------------------------------------------- */
{
  while (!assistant.terminationRequested()) {
    {
      //eos_static_debug("");
      std::vector<shared_data> data;
      {
        // avoid mutex contention
        XrdSysMutexHelper mLock(this);

        for (auto it = this->begin(); it != this->end(); ++it) {
          data.push_back(it->second);
        }
      }

      for (auto it = data.begin(); it != data.end(); ++it)
      {
        eos_static_info("dbmap-in %08lx => %lx", (*it)->id(), &(*it));
      }

      for (auto it = data.begin(); it != data.end(); ++it) {
        {
          XrdSysMutexHelper lLock((*it)->Locker());
          eos_static_info("dbmap-in => ino:%16lx %lx attached=%d", (*it)->id(), &(*it),
                          (*it)->attached_nolock());

          if (!(*it)->attached_nolock()) {
            // files which are detached might need an upstream sync
            bool repeat = true;

            while (repeat) {
              std::map<std::string, XrdCl::Proxy*>& map = (*it)->file()->get_xrdiorw();

              for (auto fit = map.begin();
                   fit != map.end(); ++fit) {
                if (!fit->second) {
                  continue;
                }

                if (fit->second->IsOpening() || fit->second->IsClosing()) {
                  eos_static_info("skipping xrdclproxyrw state=%d %d", fit->second->stateTS(),
                                  fit->second->IsClosed());
                  // skip files which are opening or closing
                  break;
                }

                if (fit->second->IsOpen()) {
                  eos_static_info("flushing journal for req=%s id=%08lx", fit->first.c_str(),
                                  (*it)->id());
                  // flush the journal using an asynchronous thread pool
                  (*it)->journalflush_async(fit->first);
                  fit->second->set_state_TS(XrdCl::Proxy::WAITWRITE);
                  eos_static_info("changing to wait write state");
                }

                if (fit->second->IsWaitWrite()) {
                  if (!fit->second->OutstandingWrites()) {
                    if (( fit->second->state_age() > 1.0)) {
		      std::string msg;
		      // check if we need to run a recovery action
		      if ( (fit->second->HadFailures(msg) || ((*it)->simulate_write_error_in_flusher())) && !(*it)->TryRecovery(0, true)) {
			if ((*it)->journalflush(fit->first))
			{
			  eos_static_err("ino:%16lx recovery failed", (*it)->id());
			}
		      }
		      
                      eos_static_info("changing to close async state - age = %f ino:%16lx", fit->second->state_age(), (*it)->id());
                      fit->second->CloseAsync();
		      break;
                    } else {
                      eos_static_info("waiting for right age before async close - age = %f", fit->second->state_age());
		      break;
                    }
                  }
                }

		if (!fit->second->IsClosed())
		{
		  break;
		}

                {
                  std::string msg;
                  if (fit->second->HadFailures(msg)) {
		    // ---------------------------------------------------------
		    // we really have to avoid this to happen, but
		    // we can put everything we have cached in a save place for
		    // manual recovery and tag the error message
		    // ---------------------------------------------------------
		    std::string file_rescue_location;
		    std::string journal_rescue_location;
		    int dt = (*it)->file()->file() ? (*it)->file()->file()->rescue(
										   file_rescue_location) : 0 ;
		    int jt = (*it)->file()->journal() ? (*it)->file()->journal()->rescue(
											 journal_rescue_location) : 0;
		    
		    if (!dt || !jt) {
		      eos_static_crit("ino:%16lx msg=%s file-recovery=%s journal-recovery=%s",
				      (*it)->id(),
				      msg.c_str(),
				      (!dt) ? file_rescue_location.c_str() : "<none>",
				      (!jt) ? journal_rescue_location.c_str() : "<none>");
		    }
		  }

                  eos_static_info("deleting xrdclproxyrw state=%d %d", fit->second->stateTS(),
                                  fit->second->IsClosed());
                  delete fit->second;
                  (*it)->file()->get_xrdiorw().erase(fit);
                  break;
                }
              }

              repeat = false;
            }
          }
        }
        XrdSysMutexHelper mLock(this);
        XrdSysMutexHelper lLock((*it)->Locker());

        // re-check that nobody is attached
        if (!(*it)->attached_nolock() && !(*it)->file()->get_xrdiorw().size()) {
          // here we make the data object unreachable for new clients
          (*it)->detach_nolock();
          cachehandler::instance().rm((*it)->id());
          this->erase((*it)->id());
          this->erase((*it)->id() + 0xffffffff);
        }
      }

      assistant.wait_for(std::chrono::milliseconds(128));
    }
  }
}
