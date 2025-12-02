// ----------------------------------------------------------------------
// File: Communicator.cc
// Author: Andreas-Joachim Peters - CERN
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

#include "fst/storage/Storage.hh"
#include "fst/XrdFstOfs.hh"
#include "fst/Config.hh"
#include "fst/storage/FileSystem.hh"
#include "common/SymKeys.hh"
#include "common/Assert.hh"
#include "common/Constants.hh"
#include "common/StringTokenizer.hh"
#include "mq/SharedHashWrapper.hh"
#include "qclient/structures/QScanner.hh"
#include "qclient/shared/SharedHashSubscription.hh"

EOSFSTNAMESPACE_BEGIN

// Set of keys updates to be tracked at the node level
std::set<std::string> Storage::sNodeUpdateKeys {
  "stat.refresh_fs", "manager", "symkey", "publish.interval",
  "debug.level", "error.simulation", "stripexs", "stat.monitor",
  "stat.scaler.xyz"};

//------------------------------------------------------------------------------
// Get configuration value from global FST config
//------------------------------------------------------------------------------
bool
Storage::GetFstConfigValue(const std::string& key, std::string& value) const
{
  common::SharedHashLocator locator =
    gConfig.getNodeHashLocator("getConfigValue", false);

  if (locator.empty()) {
    return false;
  }

  mq::SharedHashWrapper hash(gOFS.mMessagingRealm.get(), locator, true, false);
  return hash.get(key, value);
}

//------------------------------------------------------------------------------
// Get configuration value from global FST config
//------------------------------------------------------------------------------
bool
Storage::GetFstConfigValue(const std::string& key,
                           unsigned long long& value) const
{
  std::string strVal;

  if (!GetFstConfigValue(key, strVal)) {
    return false;
  }

  value = atoi(strVal.c_str());
  return true;
}

//------------------------------------------------------------------------------
// Unregister file system given a queue path
//------------------------------------------------------------------------------
void
Storage::UnregisterFileSystem(const std::string& queuepath)
{
  while (mFsMutex.TryLockWrite() != 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  auto it = std::find_if(mFsVect.begin(), mFsVect.end(), [&](FileSystem * fs) {
    return (fs->GetQueuePath() == queuepath);
  });

  if (it == mFsVect.end()) {
    eos_static_warning("msg=\"file system is already removed\" qpath=%s",
                       queuepath.c_str());
    mFsMutex.UnLockWrite();
    return;
  }

  auto fs = *it;
  mFsVect.erase(it);
  auto it_map = std::find_if(mFsMap.begin(),
  mFsMap.end(), [&](const auto & pair) {
    return (pair.second->GetQueuePath() == queuepath);
  });

  if (it_map == mFsMap.end()) {
    eos_static_warning("msg=\"file system missing from map\" qpath=%s",
                       queuepath.c_str());
  } else {
    mFsMap.erase(it_map);
  }

  mFsMutex.UnLockWrite();
  eos_static_info("msg=\"deleting file system\" qpath=%s",
                  fs->GetQueuePath().c_str());
  delete fs;
}

//------------------------------------------------------------------------------
// Register file system
//------------------------------------------------------------------------------
Storage::FsRegisterStatus
Storage::RegisterFileSystem(const std::string& queuepath)
{
  while (mFsMutex.TryLockWrite() != 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  auto it = std::find_if(mFsVect.begin(), mFsVect.end(),
  [&](FileSystem * fs) {
    return (fs->GetQueuePath() == queuepath);
  });

  if (it != mFsVect.end()) {
    eos_static_warning("msg=\"file system is already registered\" qpath=%s",
                       queuepath.c_str());
    mFsMutex.UnLockWrite();
    return FsRegisterStatus::kNoAction;
  }

  common::FileSystemLocator locator;

  if (!common::FileSystemLocator::fromQueuePath(queuepath, locator)) {
    eos_static_crit("msg=\"failed to parse locator\" qpath=%s",
                    queuepath.c_str());
    mFsMutex.UnLockWrite();
    return FsRegisterStatus::kNoAction;
  }

  fst::FileSystem* fs = new fst::FileSystem(locator, gOFS.mMessagingRealm.get());
  fs->SetLocalId();
  fs->SetLocalUuid();
  mFsVect.push_back(fs);
  eos_static_info("msg=\"attempt file system registration\" qpath=\"%s\" "
                  "fsid=%u uuid=\"%s\"", queuepath.c_str(),
                  fs->GetLocalId(), fs->GetLocalUuid().c_str());

  if ((fs->GetLocalId() == 0ul) || fs->GetLocalUuid().empty()) {
    eos_static_info("msg=\"partially register file system\" qpath=\"%s\"",
                    queuepath.c_str());
    mFsMutex.UnLockWrite();
    return FsRegisterStatus::kPartial;
  }

  if (mFsMap.find(fs->GetLocalId()) != mFsMap.end()) {
    eos_static_crit("msg=\"trying to register an already existing file system\" "
                    "fsid=%u uuid=\"%s\"", fs->GetLocalId(),
                    fs->GetLocalUuid().c_str());
    std::abort();
  }

  mFsMap[fs->GetLocalId()] = fs;

  if (gConfig.autoBoot &&
      (fs->GetConfigStatus() > eos::common::ConfigStatus::kOff)) {
    RunBootThread(fs, "");
  }

  mFsMutex.UnLockWrite();
  return FsRegisterStatus::kRegistered;
}

static int setTrack(IoAggregateMap &map, std::stringstream &stream){
	int code = 0;
	size_t winTime = 0;
	size_t uid = 0;
	size_t gid = 0;
	std::string cmd;

	if (stream >> winTime){
		while (stream >> cmd){
			if (cmd == "uid"){
				if (stream >> uid)
					code = map.setTrack(winTime, io::TYPE::UID, uid);
				else{
					std::cerr << "Monitor: Error: bad uid number" << std::endl;
					return -1;
				}
			}
			else if (cmd == "gid"){
				if (stream >> gid)
					code = map.setTrack(winTime, io::TYPE::GID, gid);
				else{
					std::cerr << "Monitor: Error: bad gid number" << std::endl;
					return -1;
				}
			}
			else
				code = map.setTrack(winTime, cmd);
			if (code != 0)
				return code;
		}
		if (cmd.empty())
			return -1;
	}
	else
		return -1;
	return 0;
}

static int addWindow(IoAggregateMap &map, std::stringstream &stream){
	char *tmp = NULL;
	long winTime = 0;
	std::string cmd;

	while (stream >> cmd){
		winTime = std::strtol(cmd.c_str(), &tmp, 10);
		if (!*tmp){
			if (winTime < 0 || map.addWindow(winTime))
				return -1;
		}
		else
			return -1;
	}

	return 0;
}

template <typename T>
static int printSummary(std::stringstream &os, IoAggregateMap &map, size_t winTime, const T index){
	if (!map.containe(winTime, index))
		return -1;
	os << C_GREEN << "[" << C_CYAN << "Summary winTime: "
		<< winTime << C_GREEN << "][" << C_CYAN << "summary of appName: " << std::string(index)
		<< C_GREEN << "]" << C_RESET << std::endl;
	os << C_CYAN << map.getSummary(winTime, index) << C_RESET << std::endl;
	return 0;
}

template <typename T>
static int printSummary(std::stringstream &os, IoAggregateMap &map, size_t winTime, io::TYPE type, const T index){
	if (!map.containe(winTime, type, index))
		return -1;

	if (type == io::TYPE::UID)
		os << C_GREEN << "[" << C_CYAN << "Summary winTime: "
			<< winTime << C_GREEN << "][" << C_CYAN << "summary of uid: " << index
			<< C_GREEN << "]" << C_RESET << std::endl;
	else if (type == io::TYPE::GID)
		os << C_GREEN << "[" << C_CYAN << "Summary winTime: "
			<< winTime << C_GREEN << "][" << C_CYAN << "summary of gid: " << index
			<< C_GREEN << "]" << C_RESET << std::endl;
	else{
		os << "printSummay failed" << std::endl;
		return -1;
	}
	os << C_CYAN <<  map.getSummary(winTime, type, index) << C_RESET << std::endl;
	return 0;
}

static int printSums(IoAggregateMap &map, std::stringstream &stream, std::stringstream &os){
	size_t winTime = 0;
	std::string cmd;
	int code = 0;
	size_t uid = 0;
	size_t gid = 0;

	if (stream >> winTime){
		while (true){
			if (stream >> cmd){
				if (cmd == "uid" && stream >> uid)
					code = printSummary(os, map, winTime, io::TYPE::UID, uid);
				else if (cmd == "gid" && stream >> gid)
					code = printSummary(os, map, winTime, io::TYPE::GID, gid);
				else
					code = printSummary(os, map, winTime, cmd);
				if (code)
					return -1;
				if (stream.eof())
					break;
			}
			else
				return -1;
		}
	}
	else
		return -1;

	return 0;
}

static int printProto(IoAggregateMap &map, std::stringstream &stream, std::stringstream &os){
	size_t winTime = 0;
	std::string cmd;
	size_t uid = 0;
	size_t gid = 0;
	IoBuffer::Summary sum;
	google::protobuf::util::JsonPrintOptions options;
	options.add_whitespace = true;
	options.always_print_primitive_fields = true;
	options.preserve_proto_field_names = true;

	if (stream >> winTime){
		while (true){
			if (stream >> cmd){
				if (cmd == "uid" && stream >> uid){
					auto summary(map.getSummary(winTime, io::TYPE::UID, uid));
					if (summary.has_value()){
						summary->winTime = winTime;
						summary->Serialize(sum);
					}
				}
				else if (cmd == "gid" && stream >> gid){
					auto summary(map.getSummary(winTime, io::TYPE::GID, gid));
					if (summary.has_value()){
						summary->winTime = winTime;
						summary->Serialize(sum);
					}
				}
				else{
					auto summary(map.getSummary(winTime, cmd));
					if (summary.has_value()){
						summary->winTime = winTime;
						summary->Serialize(sum);
					}
				}
				auto it = google::protobuf::util::MessageToJsonString(sum, &cmd, options);
				if (!it.ok())
					return -1;
				os << "Protobuf JSON:" << std::endl << cmd << std::endl;
				sum.Clear();
			}
			else if (!stream.eof())
				return -1;
			else
				break;
		}
	} else
		return -1;

	return 0;
}

static void fillThread(IoAggregateMap &map, std::mutex &mutex,
			   size_t nbrOfLoop,
			   size_t fileId,
			   std::string appName,
			   size_t maxInteraction,
			   size_t maxByte,
			   size_t uid,
			   size_t gid,
			   bool rw){
	for (size_t i = 0; i < nbrOfLoop; i++){
		std::this_thread::sleep_for(std::chrono::seconds(1));
		std::lock_guard<std::mutex> lock(mutex);
		for (size_t j = (std::abs(rand()) % maxInteraction); j < maxInteraction; j++){
			if (!rw)
				map.addRead(fileId, appName, uid, gid, std::abs(rand()) % maxByte);
			else
				map.addWrite(fileId, appName, uid, gid, std::abs(rand()) % maxByte);
		}
	}
}

static int fillData(IoAggregateMap &map, std::mutex &mutex,std::stringstream &stream, std::stringstream &os){
	std::string input;
	std::string appName;
	size_t fileId = 0;
	size_t uid = 0;
	size_t gid = 0;
	size_t nbrOfLoop = 0;
	size_t maxInteraction = 0;
	size_t maxByte = 0;
	bool rw = true;

	try {
		if (!(stream >> fileId >> appName >> uid >> gid))
			return -1;;
		nbrOfLoop = std::abs(rand()) % 10;
		maxInteraction = std::abs(rand()) % 200;
		maxByte = std::abs(rand()) % 10000;

		std::thread RbgThread = std::thread([&, nbrOfLoop, fileId, appName, maxInteraction, maxByte, uid, gid, rw]() {
			fillThread(map,
				   mutex,
				   nbrOfLoop,
				   fileId,
				   appName,
				   maxInteraction,
				   maxByte,
				   uid,
				   gid,
				   rw);
		});
		nbrOfLoop = std::abs(rand()) % 10;
		maxInteraction = std::abs(rand()) % 200;
		maxByte = std::abs(rand()) % 10000;
		rw = false;
		std::thread WbgThread = std::thread([&, nbrOfLoop, fileId, appName, maxInteraction, maxByte, uid, gid, rw]() {
			fillThread(map,
				   mutex,
				   nbrOfLoop,
				   fileId,
				   appName,
				   maxInteraction,
				   maxByte,
				   uid,
				   gid,
				   rw);
		});
		RbgThread.detach();
		WbgThread.detach();
	} catch(std::exception &e){
		os << "\033[F\033[K";
		os << "Monitor: Error: " << e.what() << ": Bad input" << std::endl;
		return -1;
	}

	return 0;
}

static int rm(IoAggregateMap &map, std::stringstream &os){
	std::string cmd;
	uid_t uid = 0;
	uid_t gid = 0;
	size_t winTime = 0;

	if (os >> winTime){
		if (os >> cmd){
			if (cmd == "uid" && os >> uid && os.eof())
				return map.rm(winTime, io::TYPE::UID, uid);
			if (cmd == "gid" && os >> gid && os.eof())
				return map.rm(winTime, io::TYPE::GID, gid);
			else if (os.eof())
				return map.rm(winTime, cmd);
		}
		else if (os.eof())
			return map.rm(winTime);
	}

	return -1;
}

std::string Storage::MonitorCmd(const std::string &input){
	std::stringstream stream(input);
	std::mutex mutex;
	std::stringstream os;
	std::string cmd;

	if (stream >> cmd){
		int winTime = 0;
		uid_t uid = 0;
		gid_t gid = 0;
		size_t bytes = 0;
		if (cmd == "set"){
			if (!setTrack(gOFS.ioMap, stream))
				os << "track successfully set" << std::endl;
			else
				os << "track set failed" << std::endl;
		}
		else if (cmd == "ls"){
			size_t len = 1;
			if (stream >> len){
				if (stream >> cmd){
					os << "print map failed" << std::endl;
				} else{
					for (size_t i = 0; i < len; i++){
						os << gOFS.ioMap << std::endl;
						if (i + 1 < len)
							std::this_thread::sleep_for(std::chrono::seconds(1));
					}
				}
			} else
				os << gOFS.ioMap << std::endl;
		}
		else if (cmd == "add"){
			if (!addWindow(gOFS.ioMap, stream))
				os << "window successfully set" << std::endl;
			else
				os << "window set failed" << std::endl;
		}
		else if (cmd == "read"){
			int fileId = 0;
			std::string appName;
			if (stream >> fileId >> appName >> uid >> gid >> bytes){
				gOFS.ioMap.addRead(fileId, appName, uid, gid, bytes);
				os << "add read succeed" << std::endl;
			}
			else
				os << "add read failed" << std::endl;
		}
		else if (cmd == "write"){
			int fileId = 0;
			std::string appName;
			if (stream >> fileId >> appName >> uid >> gid >> bytes){
				gOFS.ioMap.addWrite(fileId, appName, uid, gid, bytes);
				os << "add write succeed" << std::endl;
			}
			else
				os << "add write failed" << std::endl;
		}
		else if (cmd == "sum"){
			if (printSums(gOFS.ioMap, stream, os))
				os << "print Summary failed" << std::endl;
		}
		else if (cmd == "shift"){
			long index = 0;
			if (stream >> winTime){
				if (stream >> index){
					index = gOFS.ioMap.shiftWindow(winTime, index);
					if (index == -1)
						os << "shift window " << winTime << " failed" << std::endl;
					else
						os << "shift window " << winTime << " at " << index << std::endl;
				}
				else{
					index = gOFS.ioMap.shiftWindow(winTime);
					if (index == -1)
						os << "shift window " << winTime << " failed" << std::endl;
					else
						os << "shift window " << winTime << " at " << index << std::endl;
				}
			}
		}
		else if (cmd == "fill"){
			if (!fillData(gOFS.ioMap, mutex, stream, os))
				os << "fill map succeed" << std::endl;
			else
				os << "fill map failed" << std::endl;
		}
		else if (cmd == "proto"){
			if (printProto(gOFS.ioMap, stream, os) < 0)
				os << "protobuf conversion failed" << std::endl;
		}
		else if (cmd == "rm")
			os << "rm : " << rm(gOFS.ioMap, stream) << std::endl;
		else
			os << "Monitor: command not found: " << input << std::endl;
	}

	return (os.str());
}

//------------------------------------------------------------------------------
// Process incoming configuration change
//------------------------------------------------------------------------------
void
Storage::ProcessFstConfigChange(const std::string& key,
                                const std::string& value)
{
  static std::string last_refresh_ts = "";
  eos_static_info("msg=\"FST node configuration change\" key=\"%s\" "
                  "value=\"%s\"", key.c_str(), value.c_str());

  // Refresh the list of FS'es registered from QDB shared hashes
  if (key == "stat.refresh_fs") {
    if (last_refresh_ts != value) {
      last_refresh_ts = value;
      SignalRegisterThread();
    }

    return;
  }

  if (key == "manager") {
    XrdSysMutexHelper lock(gConfig.Mutex);
    gConfig.Manager = value.c_str();
    return;
  }

  if (key == "symkey") {
    eos::common::gSymKeyStore.SetKey64(value.c_str(), 0);
    return;
  }

  if (key == "publish.interval") {
    eos_static_info("publish.interval=%s", value.c_str());
    XrdSysMutexHelper lock(gConfig.Mutex);
    gConfig.PublishInterval = atoi(value.c_str());
    return;
  }

  if (key == "debug.level") {
    std::string debuglevel = value;
    eos::common::Logging& g_logging = eos::common::Logging::GetInstance();
    int debugval = g_logging.GetPriorityByString(debuglevel.c_str());

    if (debugval < 0) {
      eos_static_err("msg=\"unknown debug level\" level=\"%s\"",
                     debuglevel.c_str());
    } else {
      g_logging.SetLogPriority(debugval);
    }

    return;
  }

  if (key == "error.simulation") {
    gOFS.SetSimulationError(value.c_str());
    return;
  }

  if (key == "stripexs") {
    // value can be "on" or "off"
    mComputeStripeChecksum = (value == "on");
    return;
  }

  if (key == "stat.monitor"){
	std::string reply(MonitorCmd(value));
	eos_static_info("msg=\"stat.monitor | %s\"", reply.c_str());
	return;
  }

  if (key == "stat.scaler.xyz"){
	// std::string reply(MonitorCmd(value));
	eos_static_info("msg=\"stat.scaler.xyz\"");
	return;
  }
}

//------------------------------------------------------------------------------
// Process incoming filesystem-level configuration change
//------------------------------------------------------------------------------
void
Storage::ProcessFsConfigChange(fst::FileSystem* fs, const std::string& key,
                               const std::string& value)
{
  if ((key == "id") || (key == "uuid") || (key == "bootsenttime")) {
    RunBootThread(fs, key);
  } else {
    mFsUpdQueue.emplace(fs->GetLocalId(), key, value);
  }
}

//------------------------------------------------------------------------------
// Process incoming filesystem-level configuration change
//------------------------------------------------------------------------------
void
Storage::ProcessFsConfigChange(const std::string& queuepath,
                               const std::string& key)
{
  eos::common::RWMutexReadLock fs_rd_lock(mFsMutex);
  auto it = std::find_if(mFsMap.begin(), mFsMap.end(), [&](const auto & pair) {
    return (pair.second->GetQueuePath() == queuepath);
  });

  if (it == mFsMap.end()) {
    // If file system does not exist in the map and this an "id" info then
    // it could be that we have a partially registered file system
    if ((key == "id") || (key == "uuid")) {
      // Switch to a write lock as we might add the new fs to the map
      fs_rd_lock.Release();

      // Mutex write lock non-blocking
      while (mFsMutex.TryLockWrite() != 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }

      auto itv = std::find_if(mFsVect.begin(), mFsVect.end(),
      [&](fst::FileSystem * fs) {
        return (fs->GetQueuePath() == queuepath);
      });

      if (itv == mFsVect.end()) {
        eos_static_err("msg=\"no file system for id modification\" "
                       "qpath=\"%s\" key=\"%s\"", queuepath.c_str(),
                       key.c_str());
        mFsMutex.UnLockWrite();
        return;
      }

      fst::FileSystem* fs = *itv;
      fs->SetLocalId();
      fs->SetLocalUuid();
      eos_static_info("msg=\"attempt file system registration\" qpath=\"%s\" "
                      "fsid=%u uuid=\"%s\"", queuepath.c_str(), fs->GetLocalId(),
                      fs->GetLocalUuid().c_str());

      if ((fs->GetLocalId() == 0ul) || fs->GetLocalUuid().empty()) {
        eos_static_info("msg=\"defer file system registration\" qpath=\"%s\"",
                        queuepath.c_str());
        mFsMutex.UnLockWrite();
        return;
      }

      eos::common::FileSystem::fsid_t fsid = fs->GetLocalId();
      it = mFsMap.emplace(fsid, fs).first;
      eos_static_info("msg=\"fully register file system\" qpath=%s fsid=%u "
                      "uuid=\"%s\"", queuepath.c_str(), fs->GetLocalId(),
                      fs->GetLocalUuid().c_str());
      // Switch back to read lock and update the iterator
      mFsMutex.UnLockWrite();
      fs_rd_lock.Grab(mFsMutex);
      it = mFsMap.find(fsid);
    } else {
      eos_static_err("msg=\"no file system for modification\" qpath=\"%s\" "
                     "key=\"%s\"", queuepath.c_str(), key.c_str());
      return;
    }
  }

  eos_static_info("msg=\"process modification\" qpath=\"%s\" key=\"%s\"",
                  queuepath.c_str(), key.c_str());
  fst::FileSystem* fs = it->second;
  mq::SharedHashWrapper hash(gOFS.mMessagingRealm.get(), fs->getHashLocator());
  std::string value;

  if (!hash.get(key, value)) {
    eos_static_err("msg=\"no such key in hash\" qpath=\"%s\" key=\"%s\"",
                   queuepath.c_str(), key.c_str());
    return;
  }

  return ProcessFsConfigChange(fs, key, value);
}

//------------------------------------------------------------------------------
// Extract filesystem path from QDB hash key - helper function
//------------------------------------------------------------------------------
static std::string ExtractFsPath(const std::string& key)
{
  std::vector<std::string> parts =
    common::StringTokenizer::split<std::vector<std::string>>(key, '|');
  return parts[parts.size() - 1];
}

//------------------------------------------------------------------------------
// Handle FS configuration updates in a separate thread to avoid deadlocks
// in the QClient callback mechanism.
//------------------------------------------------------------------------------
void
Storage::FsConfigUpdate(ThreadAssistant& assistant) noexcept
{
  eos_static_info("%s", "msg=\"starting fs config update thread\"");
  FsCfgUpdate upd;

  while (!assistant.terminationRequested()) {
    mFsUpdQueue.wait_pop(upd);

    // If sentinel object then exit
    if ((upd.fsid == 0) &&
        (upd.key == "ACTION") &&
        (upd.value == "EXIT")) {
      eos_static_notice("%s", "msg=\"fs config update thread got a "
                        "sentinel object exiting\"");
      break;
    }

    if ((upd.key == eos::common::SCAN_IO_RATE_NAME) ||
        (upd.key == eos::common::SCAN_ENTRY_INTERVAL_NAME) ||
        (upd.key == eos::common::SCAN_RAIN_ENTRY_INTERVAL_NAME) ||
        (upd.key == eos::common::SCAN_DISK_INTERVAL_NAME) ||
        (upd.key == eos::common::SCAN_NS_INTERVAL_NAME) ||
        (upd.key == eos::common::SCAN_NS_RATE_NAME) ||
        (upd.key == eos::common::SCAN_ALTXS_INTERVAL_NAME) ||
        (upd.key == eos::common::ALTXS_SYNC) ||
        (upd.key == eos::common::ALTXS_SYNC_INTERVAL)) {
      try {
        long long val = std::stoll(upd.value);

        if (val >= 0) {
          eos::common::RWMutexReadLock fs_rd_lock(mFsMutex);
          auto it = mFsMap.find(upd.fsid);

          if (it != mFsMap.end()) {
            it->second->ConfigScanner(&mFstLoad, upd.key.c_str(), val);
          }
        }
      } catch (...) {
        eos_static_err("msg=\"failed to convert value\" key=\"%s\" val=\"%s\"",
                       upd.key.c_str(), upd.value.c_str());
      }
    }
  }

  eos_static_info("%s", "msg=\"stopped fs config update thread\"");
}

//------------------------------------------------------------------------------
// Update file system list given the QDB shared hash configuration -this
// update is done in a separate thread handling the trigger event otherwise
// we deadlock in the QClient code.
//------------------------------------------------------------------------------
void Storage::UpdateRegisteredFs(ThreadAssistant& assistant) noexcept
{
  eos_static_info("%s", "msg=\"starting register file system thread\"");

  while (!assistant.terminationRequested()) {
    {
      // Reduce scope of mutex to avoid coupling the the SignalRegisterThread
      // which is called from the QClient event loop with other QClient requests
      // like the QScanner listing below - this will lead to a deadlock!!!
      std::unique_lock lock(mMutexRegisterFs);
      mCvRegisterFs.wait(lock, [&] {return mTriggerRegisterFs;});
      eos_static_info("%s", "msg=\"update registered file systems\"");
      mTriggerRegisterFs = false;
    }
    qclient::QScanner scanner(*gOFS.mMessagingRealm->getQSom()->getQClient(),
                              SSTR("eos-hash||fs||" << gConfig.FstHostPort << "||*"));
    std::set<std::string> new_filesystems;

    for (; scanner.valid(); scanner.next()) {
      std::string queuePath = SSTR("/eos/" << gConfig.FstHostPort <<
                                   "/fst" <<  ExtractFsPath(scanner.getValue()));
      new_filesystems.insert(queuePath);
    }

    // Filesystems added?
    std::set<std::string> partial_filesystems;

    for (auto it = new_filesystems.begin(); it != new_filesystems.end(); ++it) {
      if (mLastRoundFilesystems.find(*it) == mLastRoundFilesystems.end()) {
        if (RegisterFileSystem(*it) == FsRegisterStatus::kPartial) {
          partial_filesystems.insert(*it);
        }
      }
    }

    // Filesystems removed?
    for (auto it = mLastRoundFilesystems.begin();
         it != mLastRoundFilesystems.end(); ++it) {
      if (new_filesystems.find(*it) == new_filesystems.end()) {
        eos_static_info("msg=\"unregister file system\" queuepath=\"%s\"",
                        it->c_str());
        UnregisterFileSystem(*it);
      }
    }

    if (!partial_filesystems.empty()) {
      // Reset register trigger flag and remove partial file systems so
      // that we properly register them in them next loop.
      {
        std::unique_lock lock(mMutexRegisterFs);
        mTriggerRegisterFs = true;
      }

      for (const auto& elem : partial_filesystems) {
        UnregisterFileSystem(elem);
        auto it_del  = new_filesystems.find(elem);
        new_filesystems.erase(it_del);
      }

      eos_static_info("%s", "msg=\"re-trigger file system registration "
                      "in 5 seconds\"");
      assistant.wait_for(std::chrono::seconds(5));
    }

    mLastRoundFilesystems = std::move(new_filesystems);
  }

  eos_static_info("%s", "msg=\"stopped register file system thread\"");
}

//------------------------------------------------------------------------------
// FST node update callback - this is triggered whenever the underlying
// qclient::SharedHash corresponding to the node is modified.
//------------------------------------------------------------------------------
void
Storage::NodeUpdateCb(qclient::SharedHashUpdate&& upd)
{
  if (sNodeUpdateKeys.find(upd.key) != sNodeUpdateKeys.end()) {
    ProcessFstConfigChange(upd.key, upd.value);
  }
}

//------------------------------------------------------------------------------
// QdbCommunicator
//------------------------------------------------------------------------------
void
Storage::QdbCommunicator(ThreadAssistant& assistant) noexcept
{
  using namespace std::placeholders;
  eos_static_info("%s", "msg=\"starting QDB communicator thread\"");
  // Process initial FST configuration ... discover instance name
  std::string instance_name;

  for (size_t i = 0; i < 10; i++) {
    if (gOFS.mMessagingRealm->getInstanceName(instance_name)) {
      break;
    }

    std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  if (instance_name.empty()) {
    eos_static_crit("%s", "msg=\"unable to obtain instance name from QDB\"");
    exit(1);
  }

  std::string cfg_queue = SSTR("/config/" << instance_name << "/node/" <<
                               gConfig.FstHostPort);
  gConfig.setFstNodeConfigQueue(cfg_queue);
  // Discover node-specific configuration
  mq::SharedHashWrapper node_hash(gOFS.mMessagingRealm.get(),
                                  gConfig.getNodeHashLocator(),
                                  false, false);
  // Discover MGM name
  std::string mgm_host;

  for (size_t i = 0; i < 10; i++) {
    if (node_hash.get("manager", mgm_host)) {
      break;
    }

    std::this_thread::sleep_for(std::chrono::seconds(5));
  }

  if (mgm_host.empty()) {
    eos_static_crit("%s", "msg=\"unable to obtain manager info for node\"");
    exit(1);
  }

  ProcessFstConfigChange("manager", mgm_host);

  // Discover the rest of the FST node configuration options
  for (const auto& node_key : sNodeUpdateKeys) {
    std::string value;

    if (node_hash.get(node_key, value)) {
      ProcessFstConfigChange(node_key, value);
    }
  }

  // One-off collect all configured file systems for this node
  SignalRegisterThread();
  // Attach callback for node configuration updates
  std::unique_ptr<qclient::SharedHashSubscription>
  node_subscription = node_hash.subscribe();
  node_subscription->attachCallback(std::bind(&Storage::NodeUpdateCb,
                                    this, _1));

  // Broadcast FST node hearbeat
  while (!assistant.terminationRequested()) {
    node_hash.set(eos::common::FST_HEARTBEAT_KEY, std::to_string(time(0)));
    assistant.wait_for(std::chrono::seconds(1));
  }

  node_subscription->detachCallback();
  node_subscription.reset(nullptr);
  mq::SharedHashWrapper::deleteHash(gOFS.mMessagingRealm.get(),
                                    gConfig.getNodeHashLocator(), false);
  eos_static_info("%s", "msg=\"stopped QDB communicator thread\"");
}

//------------------------------------------------------------------------------
// Signal the thread responsible with registered file systems
//------------------------------------------------------------------------------
void
Storage::SignalRegisterThread()
{
  {
    std::unique_lock lock(mMutexRegisterFs);
    mTriggerRegisterFs = true;
  }
  mCvRegisterFs.notify_one();
}

EOSFSTNAMESPACE_END
