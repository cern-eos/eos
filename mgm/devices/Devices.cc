// ----------------------------------------------------------------------
// File: Devices.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2023 CERN/Switzerland                                  *
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

#include "common/Constants.hh"
#include "common/Logging.hh"
#include "common/RWMutex.hh"
#include "common/Path.hh"
#include "common/utils/BackOffInvoker.hh"
#include "mgm/devices/Devices.hh"
#include "mgm/ofs/XrdMgmOfs.hh"
#include "mgm/fsview/FsView.hh"
#include "mgm/stat/Stat.hh"
#include "namespace/Prefetcher.hh"
#include "namespace/interface/IView.hh"

EOSMGMNAMESPACE_BEGIN


//------------------------------------------------------------------------------
// Run asynchronous devices thread
//------------------------------------------------------------------------------
bool
Devices::Start()
{
  mThread.reset(&Devices::Recorder, this);
  return true;
}

//------------------------------------------------------------------------------
// Cancel the asynchronous devices thread
//------------------------------------------------------------------------------
void
Devices::Stop()
{
  mThread.join();
}

//------------------------------------------------------------------------------
// Eternal thread registering device information, which allows to detect
// devices which have been removed
//------------------------------------------------------------------------------
void
Devices::Recorder(ThreadAssistant& assistant) noexcept
{
  ThreadAssistant::setSelfThreadName("Devices");
  time_t snoozetime = 900;
  if (getenv("EOS_MGM_DEVICES_PUBLISHING_INTERVAL")) {
    auto rtime = std::atoi(getenv("EOS_MGM_DEVICES_PUBLISHING_INTERVAL"));
    if (rtime==0 || rtime > 86400) {
      rtime = 900;
    }
  }
  gOFS->WaitUntilNamespaceIsBooted(assistant);

  if (assistant.terminationRequested()) {
    return;
  }

  assistant.wait_for(std::chrono::seconds(15));
  eos::common::BackOffInvoker backoff_logger;

  while (!assistant.terminationRequested()) {
    // Every now and then we wake up
    backoff_logger.invoke([&snoozetime]() {
      eos_static_info("msg=\"devices thread\" snooze-time=%llu",
                      snoozetime);
    });

    if (!gOFS->mMaster->IsMaster()) {
      assistant.wait_for(std::chrono::seconds(snoozetime));
      continue;
    }

    // get the latest info
    Extract();
    // store in the namespace
    Store();
    for (int i = 0; i < snoozetime / 1; i++) {
      if (assistant.terminationRequested()) {
        eos_static_info("%s", "msg=\"devices thread exiting\"");
        return;
      }
      
      assistant.wait_for(std::chrono::seconds(1));
    }
  }

  eos_static_info("%s", "msg=\"devices thread exiting\"");
}

// Function extracting device information either on request or by the background thread
void
Devices::Extract()
{
  gOFS->MgmStats.Add("Devices::Extract", 0,0 , 1);
  json_map_t jm = std::make_shared<json_map>();
  space_map_t sp = std::make_shared<space_map>();
  smart_map_t sm = std::make_shared<smart_map>();
  
  std::set<uint64_t> fsids;
  {
    // get all the filesystem which are currently visible quickly
    eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
    for ( auto it = FsView::gFsView.mSpaceView.begin(); it != FsView::gFsView.mSpaceView.end(); ++it ) {
      // loop over all filesystems
      for (auto fsit = FsView::gFsView.mIdView.begin(); fsit != FsView::gFsView.mIdView.end(); ++fsit) {
	FileSystem* fs = fsit->second;
	if (fs->GetSpace() != it->first) {
	  // only look at the current space
	  continue;
	}
	fsids.insert(fs->GetId());
	(*sp)[fs->GetId()]=fs->GetSpace();
      }
    }
  }
  
  // loop over the filesystems and take short locks to extract
  for ( auto it= fsids.begin(); it != fsids.end(); ++it ) {
    uint64_t id = *it;
    {
      eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
      FileSystem* fs = FsView::gFsView.mIdView.lookupByID(id);
      if (!fs) {
	// skip this disappeared
	continue;
      }
      // store the compressed maps
      (*jm)[id] = fs->GetString("stat.health.z64smart");
      (*sm)[id] = fs->GetString("stat.health");
    }
    // avoid tight locking loops
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  // decompress without any lock
  for ( auto it = (*jm).begin(); it != (*jm).end(); ++it) {
    std::string compressedjson = it->second;
    std::string ojson;
    bool done = eos::common::SymKey::ZDeBase64(compressedjson, ojson);
    if (!done) {
      eos_static_err("msg=\"failed to decompress JSON smart info from fsid=%lu\"",
		     it->first);
      it->second = "";
    } else {
      it->second = ojson;
    }
  }

  lastExtraction = time(NULL);
  
  // swap the new map with the current one
  setJson(jm);
  setSpaceMap(sp);
  setSmartMap(sm);
}

void
Devices::Store()
{
  gOFS->MgmStats.Add("Devices::Store", 0,0 , 1);
  auto jinfo = getJson();
  auto sminfo = getSmartMap();
  for (auto it=jinfo->begin(); it != jinfo->end(); ++it) {
    std::string storagepath = mDevicesPath;
    storagepath += "/";
    std::string smartstatus="unknown";
    if (sminfo->count(it->first)) {
      smartstatus = (*sminfo)[it->first];
    }

    std::string serial;
    
    {
      Json::Value root;
      std::string errs;
      Json::CharReaderBuilder jsonReaderBuilder;
      std::unique_ptr<Json::CharReader> const reader(jsonReaderBuilder.newCharReader());
      const std::string& ojson = it->second;

      if (reader->parse(ojson.c_str(), ojson.c_str() + ojson.size(), &root, &errs)) {
        try {
	  serial     = root.isMember("serial_number")?root["serial_number"].asString():"";
	} catch (Json::Exception const&) {
        }
      }
    }

    if (serial.empty()) {
      continue;
    }

    storagepath += serial; // serial number
    storagepath += ".";
    storagepath += std::to_string(it->first); // fsid

    eos::Prefetcher::prefetchFileMDAndWait(gOFS->eosView, storagepath.c_str());

    eos::MDLocking::FileWriteLockPtr fmdLock;
    eos::IFileMDPtr fmd = nullptr;
    try {
      fmd = gOFS->eosView->getFile(storagepath.c_str());
      fmdLock = eos::MDLocking::writeLock(fmd.get());
      errno = 0;
    } catch (eos::MDException& e) {
      errno = e.getErrno();
      eos_static_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
		e.getErrno(), e.getMessage().str().c_str());
    }
    if (!fmd) {
      // if it does not exist, create it
      try {
	fmd = gOFS->eosView->createFile(storagepath.c_str(), 0, 0);
	fmdLock = eos::MDLocking::writeLock(fmd.get());
	fmd->setMTimeNow();
	fmd->setCTimeNow();
	eos::IFileMD::ctime_t mtime;
	fmd->getMTime(mtime);
	char btime[256];
	snprintf(btime, sizeof(btime), "%lu.%lu", mtime.tv_sec, mtime.tv_nsec);
	fmd->setAttribute("sys.eos.btime", btime);
	errno = 0;
      } catch (eos::MDException& e) {
	errno = e.getErrno();
	eos_static_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
		  e.getErrno(), e.getMessage().str().c_str());
      }
    }
    // if it exists now, store the latest json and update the mtime
    if (fmd) {
      fmd->setAttribute("sys.smart.json", it->second);
      fmd->setAttribute("sys.smart.status", smartstatus);
      
      fmd->setMTimeNow();
      fmdLock.reset(nullptr);
      gOFS->eosView->updateFileStore(fmd.get());
    } 
  }
}


  
EOSMGMNAMESPACE_END
