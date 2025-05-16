//------------------------------------------------------------------------------
// File: LRU.cc
// Author: Andreas-Joachim Peters - CERN
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

#include "common/Logging.hh"
#include "common/LayoutId.hh"
#include "common/Mapping.hh"
#include "common/RWMutex.hh"
#include "common/ParseUtils.hh"
#include "common/Path.hh"
#include "common/IntervalStopwatch.hh"
#include "mgm/Quota.hh"
#include "mgm/LRU.hh"
#include "mgm/Stat.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/convert/ConverterDriver.hh"
#include "mgm/convert/ConversionTag.hh"
#include "namespace/interface/IView.hh"
#include "namespace/interface/ContainerIterators.hh"
#include "namespace/Prefetcher.hh"
#include "namespace/ns_quarkdb/explorer/NamespaceExplorer.hh"
#include "namespace/ns_quarkdb/NamespaceGroup.hh"
#include <qclient/QClient.hh>

//! Attribute name defining any LRU policy
const char* LRU::gLRUPolicyPrefix = "sys.lru.*";

EOSMGMNAMESPACE_BEGIN

using namespace eos::common;

//------------------------------------------------------------------------------
// Start the LRU thread
//------------------------------------------------------------------------------
void LRU::Start()
{
  mThread.reset(&LRU::backgroundThread, this);
}

//------------------------------------------------------------------------------
// Stop the LRU thread
//------------------------------------------------------------------------------
void LRU::Stop()
{
  mThread.join();
}

//------------------------------------------------------------------------------
// Retrieve "lru.interval" configuration option as string, or empty if
// cannot be found. Assumes gFsView.ViewMutex is at-least readlocked.
//------------------------------------------------------------------------------
std::string LRU::getLRUIntervalConfig() const
{
  if (FsView::gFsView.mSpaceView.count("default") == 0) {
    return "";
  }

  return FsView::gFsView.mSpaceView["default"]->GetConfigMember("lru.interval");
}

//------------------------------------------------------------------------------
// Retrieve current LRU configuration options
//------------------------------------------------------------------------------
LRU::Options LRU::getOptions()
{
  LRU::Options opts;
  // Default options
  opts.enabled = false;
  opts.interval = std::chrono::minutes(30);
  eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);

  if (FsView::gFsView.mSpaceView.count("default") &&
      (FsView::gFsView.mSpaceView["default"]->GetConfigMember("lru") == "on")) {
    opts.enabled = true;
  }

  std::string interval = getLRUIntervalConfig();
  int64_t intv = 0;

  if (opts.enabled && (interval.empty() || !common::ParseInt64(interval, intv))) {
    eos_static_crit("%s", "msg=\"unable to parse space config lru.interval "
                    "option, disabling LRU!\"");
    opts.enabled = false;
  } else {
    opts.interval = std::chrono::seconds(intv);
  }

  if (opts.enabled) {
    eos_static_info("msg=\"lru is enabled\" interval=%ds", opts.interval.count());
  }

  // Set long interval in case LRU is de-activated, prevent the background
  // thread from spinning
  if (!opts.enabled || opts.interval == std::chrono::seconds(0)) {
    opts.interval = std::chrono::minutes(30);
  }

  return opts;
}

//------------------------------------------------------------------------------
// Constructor. To run the LRU thread, call Start
//------------------------------------------------------------------------------
LRU::LRU() :
  mQcl(nullptr), mRootVid(eos::common::VirtualIdentity::Root()),
  mRefresh(false)
{
}

//------------------------------------------------------------------------------
// Destructor - stop the background thread, if running
//------------------------------------------------------------------------------
LRU::~LRU()
{
  Stop();
}

//------------------------------------------------------------------------------
// Parse an "sys.lru.expire.match" policy
//------------------------------------------------------------------------------
bool LRU::parseExpireMatchPolicy(const std::string& policy,
                                 std::map<std::string, time_t>& matchAgeMap)
{
  matchAgeMap.clear();
  std::map<std::string, std::string> tmpMap;

  if (!StringConversion::GetKeyValueMap(policy.c_str(), tmpMap, ":")) {
    // Failed splitting on ":", cannot parse further
    return false;
  }

  for (auto it = tmpMap.begin(); it != tmpMap.end(); it++) {
    uint64_t out;

    if (!StringConversion::GetSizeFromString(it->second, out)) {
      eos_static_err("msg=\"LRU match attribute has illegal age\" "
                     "match=\"%s\", age=\"%s\"",
                     it->first.c_str(),
                     it->second.c_str());
    } else {
      matchAgeMap[it->first] = out;
      eos_static_info("msg=\"add expire policy\" rule=\"%s %llu\"",
                      it->first.c_str(), out);
    }
  }

  return true;
}

//------------------------------------------------------------------------------
// Perform a single LRU cycle, QDB namespace
//------------------------------------------------------------------------------
void LRU::performCycleQDB(ThreadAssistant& assistant) noexcept
{
  ThreadAssistant::setSelfThreadName("LRUQDBCycle");
  eos_static_info("%s", "msg=\"start LRU scan on QDB\"");
  // Build exploration options..
  ExplorationOptions opts;
  opts.populateLinkedAttributes = true;
  opts.view = gOFS->eosView;
  opts.ignoreFiles = true;
  opts.depthLimit = eos::common::Path::MAX_LEVELS;

  // Initialize qclient..
  if (!mQcl) {
    mQcl.reset(new qclient::QClient(gOFS->mQdbContactDetails.members,
                                    gOFS->mQdbContactDetails.constructOptions()));
  }

  // Start exploring
  NamespaceExplorer
  explorer("/", opts, *(mQcl.get()),
           static_cast<QuarkNamespaceGroup*>(gOFS->namespaceGroup.get())->getExecutor());
  NamespaceItem item;
  int64_t processed = 0;

  while (explorer.fetch(item)) {
    eos_static_debug("lru-dir-qdb=\"%s\" attrs=%d", item.fullPath.c_str(),
                     item.attrs.size());
    processDirectory(item.fullPath, item.attrs);
    processed++;

    if (processed % 1000 == 0) {
      eos_static_info("msg=\"LRU scan in progress\" num_scanned_dirs=%lli",
                      processed);

      if (assistant.terminationRequested() || !gOFS->mMaster->IsMaster()) {
        eos_static_info("%s", "msg=\"quit LRU due termination request "
                        "or MGM running in slave mode\"");
        break;
      }
    }
  }

  eos_static_info("msg=\"LRU scan done\" num_scanned_dirs=%lli", processed);
}

//------------------------------------------------------------------------------
// LRU method doing the actual policy scrubbing
//
// This thread loops in regular intervals over all directories which have
// a LRU policy attribute set (sys.lru.*) and applies the defined policy.
//------------------------------------------------------------------------------
void LRU::backgroundThread(ThreadAssistant& assistant) noexcept
{
  ThreadAssistant::setSelfThreadName("LRUBackground");
  // Eternal thread doing LRU scans
  eos_static_notice("%s", "msg=\"starting LRU thread\"");
  gOFS->WaitUntilNamespaceIsBooted(assistant);

  // Wait that current MGM becomes a master
  do {
    eos_static_debug("%s", "msg=\"LRU waiting for master MGM\"");
    assistant.wait_for(std::chrono::seconds(10));
  } while (!assistant.terminationRequested() && !gOFS->mMaster->IsMaster());

  while (!assistant.terminationRequested()) {
    // every now and then we wake up
    Options opts = getOptions();
    common::IntervalStopwatch stopwatch(opts.interval);

    // Only a master needs to run LRU
    if (opts.enabled && gOFS->mMaster->IsMaster()) {
      performCycleQDB(assistant);
    }

    while (stopwatch.timeRemainingInCycle() >= std::chrono::seconds(5)) {
      assistant.wait_for(std::chrono::seconds(5));

      if (assistant.terminationRequested() || mRefresh) {
        mRefresh = false;
        break;
      }
    }
  }

  eos_static_notice("%s", "msg=\"stopped LRU thread\"");
}

//------------------------------------------------------------------------------
// Process the given directory, apply all policies
//------------------------------------------------------------------------------
void LRU::processDirectory(const std::string& dir,
                           eos::IContainerMD::XAttrMap& map)
{
  // No LRU on "/"
  if (dir == "/" || dir == "") {
    return;
  }

  // Don't walk into the proc directory
  if (dir.substr(0, gOFS->MgmProcPath.length()) == gOFS->MgmProcPath.c_str()) {
    eos_static_debug("skipping proc tree %s\n", dir.c_str());
    return;
  }

  // Sort out the individual LRU policies
  if (map.count("sys.lru.expire.empty")) {
    // Remove empty directories older than <age>
    AgeExpireEmpty(dir.c_str(), map["sys.lru.expire.empty"]);
  }

  if (map.count("sys.lru.expire.match")) {
    // Files with a given match will be removed after expiration time
    AgeExpire(dir.c_str(), map["sys.lru.expire.match"]);
  }

  if (map.count("sys.lru.lowwatermark") && map.count("sys.lru.highwatermark")) {
    // If the space in this directory reaches highwatermark, files are
    // cleaned up according to the LRU policy
    CacheExpire(dir.c_str(), map["sys.lru.lowwatermark"],
                map["sys.lru.highwatermark"]);
  }

  if (map.count("sys.lru.convert.match")) {
    // Files with a given match/age will be automatically converted
    ConvertMatch(dir.c_str(), map);
  }
}

//------------------------------------------------------------------------------
// Remove empty directories if they are older than age given in policy
//------------------------------------------------------------------------------
void
LRU::AgeExpireEmpty(const char* dir, const std::string& policy)

{
  struct stat buf;
  eos_static_debug("dir=%s", dir);

  if (!gOFS->_stat(dir, &buf, mError, mRootVid, "")) {
    // check if there is any child in that directory
    if (buf.st_blksize) {
      eos_static_debug("dir=%s children=%d", dir, buf.st_blksize);
      return;
    } else {
      time_t now = time(NULL);
      XrdOucString sage = policy.c_str();
      time_t age = StringConversion::GetSizeFromString(sage);
      eos_static_debug("ctime=%u age=%u now=%u", buf.st_ctime, age, now);

      if ((buf.st_ctime + age) < now) {
        eos_static_notice("msg=\"delete empty directory\" path=\"%s\"", dir);

        if (gOFS->_remdir(dir, mError, mRootVid, "")) {
          eos_static_err("msg=\"failed to delete empty directory\" "
                         "path=\"%s\"", dir);
        }
      }
    }
  }
}

//------------------------------------------------------------------------------
// Remove all files in the directory older than the policy defines
//------------------------------------------------------------------------------
void
LRU::AgeExpire(const char* dir, const std::string& policy)
{
  eos_static_info("msg=\"applying age deletion policy\" dir=\"%s\" age=\"%s\"",
                  dir, policy.c_str());
  std::map<std::string, time_t> lMatchAgeMap;

  if (!parseExpireMatchPolicy(policy, lMatchAgeMap)) {
    eos_static_err("msg=\"LRU match attribute is illegal\" val=\"%s\"",
                   policy.c_str());
    return;
  }

  time_t now = time(NULL);
  std::vector<std::string> lDeleteList;
  {
    // Check the directory contents
    std::shared_ptr<eos::IContainerMD> cmd;
    eos::Prefetcher::prefetchContainerMDWithChildrenAndWait(gOFS->eosView, dir);

    try {
      cmd = gOFS->eosView->getContainer(dir);
      std::string fullpath;
      std::shared_ptr<eos::IFileMD> fmd;
      XrdOucString fname;
      eos::IFileMD::ctime_t fctime;

      // Loop through all file names
      for (auto it = eos::FileMapIterator(cmd); it.valid(); it.next()) {
        // no need to lock the cmd
        fmd = cmd->findFile(it.key());

        if (fmd == nullptr) {
          eos_static_err("msg=\"file is null\" fxid=%08llx", it.key().c_str());
          continue;
        }

        {
          eos::MDLocking::FileReadLock fmdLock(fmd.get());
          fname = fmd->getName().c_str();
          fmd->getCTime(fctime);
        }

        fullpath = dir;
        fullpath += fname.c_str();
        eos_static_debug("check_file=\"%s\"", fullpath.c_str());

        // Loop over the match map
        for (auto mit = lMatchAgeMap.begin(); mit != lMatchAgeMap.end(); mit++) {
          eos_static_debug("check_rule=\"%s\" maches=%d", mit->first.c_str(),
                           fname.matches(mit->first.c_str()));

          if (fname.matches(mit->first.c_str())) {
            // Full match check the age policy
            time_t age = mit->second;

            if ((fctime.tv_sec + age) < now) {
              // This entry can be deleted
              eos_static_notice("msg=\"delete expired file\" path=\"%s\" "
                                "ctime=%u policy-age=%u age=%u",
                                fullpath.c_str(), fctime.tv_sec, age,
                                now - fctime.tv_sec);
              lDeleteList.push_back(fullpath);
              break;
            }
          }
        }
      }
    } catch (eos::MDException& e) {
      errno = e.getErrno();
      cmd = std::shared_ptr<eos::IContainerMD>((eos::IContainerMD*)0);
      eos_static_err("msg=\"exception\" ec=%d emsg=\"%s\"",
                     e.getErrno(), e.getMessage().str().c_str());
    }
  }

  for (auto it = lDeleteList.begin(); it != lDeleteList.end(); it++) {
    if (gOFS->_rem(it->c_str(), mError, mRootVid, "")) {
      eos_static_err("msg=\"failed to expire file\" path=\"%s\"", it->c_str());
    }
  }
}

//------------------------------------------------------------------------------
// Expire the oldest files to go under the low watermark
//------------------------------------------------------------------------------
void
LRU::CacheExpire(const char* dir, std::string& lowmark, std::string& highmark)

{
  eos_static_info("msg=\"applying volume deletion policy\" "
                  "dir=\"%s\" low-mark=\"%s\" high-mark=\"%s\"",
                  dir, lowmark.c_str(), highmark.c_str());

  // Update space quota, return if this is not a ns quota node
  if (!Quota::UpdateFromNsQuota(dir, 0, 0)) {
    return;
  }

  // Check for project quota
  auto map_quotas = Quota::GetGroupStatistics(dir, Quota::gProjectId);
  long long target_volume = map_quotas[SpaceQuota::kGroupLogicalBytesTarget];
  long long is_volume = map_quotas[SpaceQuota::kGroupLogicalBytesIs];

  if (target_volume <= 0) {
    return;
  }

  errno = 0;
  double lwm = strtod(lowmark.c_str(), 0);

  if (!lwm || errno || (lwm >= 100)) {
    eos_static_err("msg=\"low watermark value is illegal - "
                   "must be 0 < lw < 100\" low-watermark=\"%s\"",
                   lowmark.c_str());
    return;
  }

  errno = 0;
  double hwm = strtod(highmark.c_str(), 0);

  if (!hwm || errno || (hwm < lwm) || (hwm >= 100)) {
    eos_static_err("msg = \"high watermark value is illegal - "
                   "must be 0 < lw < hw < 100\" "
                   "low_watermark=\"%s\" high-watermark=\"%s\"",
                   lowmark.c_str(), highmark.c_str());
    return;
  }

  double cwm = 100.0 * is_volume / target_volume;
  eos_static_debug("cwm=%.02f hwm=%.02f", cwm, hwm);

  // check if we have to do cache cleanup e.g. current is over high water mark
  if (cwm < hwm) {
    return;
  }

  unsigned long long bytes_to_free = is_volume - (lwm * target_volume / 100.0);
  XrdOucString sizestring;
  eos_static_notice("low-mark=%.02f high-mark=%.02f current-mark=%.02f "
                    "deletion-bytes=%s", lwm, hwm,  cwm,
                    StringConversion::GetReadableSizeString(sizestring, bytes_to_free, "B"));
  // Build the LRU list
  std::map<std::string, std::set<std::string> > cachedirs;
  XrdOucString stdErr;
  time_t ms = 0;
  // map with path/mtime pairs
  std::set<lru_entry_t> lru_map;
  unsigned long long lru_size = 0;

  if (!gOFS->_find(dir, mError, stdErr, mRootVid, cachedirs, "", "", false, ms)) {
    // Loop through the result and build an LRU list
    // We just keep as many entries in the LRU list to have the required
    // number of bytes to free available.
    for (auto dit = cachedirs.begin(); dit != cachedirs.end(); dit++) {
      eos_static_debug("path=%s", dit->first.c_str());

      for (auto fit = dit->second.begin(); fit != dit->second.end(); fit++) {
        // build the full path name
        std::string fpath = dit->first;
        fpath += *fit;
        struct stat buf;
        eos_static_debug("path=%s", fpath.c_str());

        // get the current ctime & size information
        if (!gOFS->_stat(fpath.c_str(), &buf, mError, mRootVid, "")) {
          if (lru_map.size())
            if ((lru_size > bytes_to_free) &&
                lru_map.size() &&
                ((--lru_map.end())->ctime < buf.st_ctime)) {
              // this entry is newer than all the rest
              continue;
            }

          // add LRU entry in front
          lru_entry_t lru;
          lru.path = fpath;
          lru.ctime = buf.st_ctime;
          lru.size = buf.st_blocks * buf.st_blksize;
          lru_map.insert(lru);
          lru_size += lru.size;
          eos_static_debug("msg=\"adding\" file=\"%s\" "
                           "bytes-free=\"%llu\" lru-size=\"%llu\"",
                           fpath.c_str(),
                           bytes_to_free,
                           lru_size);

          // check if we can shrink the LRU map
          if (lru_map.size() && (lru_size > bytes_to_free)) {
            while (lru_map.size() &&
                   ((lru_size - (--lru_map.end())->size) > bytes_to_free)) {
              // remove the last element  of the map
              auto it = lru_map.end();
              it--;
              // subtract the size
              lru_size -= it->size;
              eos_static_info("msg=\"clean-up\" path=\"%s\"", it->path.c_str());
              lru_map.erase(it);
            }
          }
        }
      }
    }
  } else {
    eos_static_err("msg=\"%s\"", stdErr.c_str());
  }

  eos_static_notice("msg=\"cleaning LRU cache\" files-to-delete=%llu",
                    lru_map.size());

  // Delete starting with the 'oldest' entry until we have freed enough space
  // to go under the low watermark
  for (auto it = lru_map.begin(); it != lru_map.end(); it++) {
    eos_static_notice("msg=\"delete LRU file\" path=\"%s\" ctime=%lu size=%llu",
                      it->path.c_str(),
                      it->ctime,
                      it->size);

    if (gOFS->_rem(it->path.c_str(), mError, mRootVid, "")) {
      eos_static_err("msg=\"failed to expire file\" "
                     "path=\"%s\"", it->path.c_str());
    }
  }
}

//------------------------------------------------------------------------------
//! Convert all files matching
//------------------------------------------------------------------------------
void
LRU::ConvertMatch(const char* dir,
                  eos::IContainerMD::XAttrMap& map)
{
  eos_static_info("msg=\"applying match policy\" dir=\"%s\" match=\"%s\"",
                  dir, map["sys.lru.convert.match"].c_str());
  std::map < std::string, std::string> lMatchMap;
  std::map < std::string, time_t> lMatchAgeMap;
  std::map < std::string, ssize_t> lMatchSizeMap;
  time_t now = time(NULL);

  if (!StringConversion::GetKeyValueMap(map["sys.lru.convert.match"].c_str(),
                                        lMatchMap,
                                        ":")
     ) {
    eos_static_err("msg=\"LRU match attribute is illegal\" val=\"%s\"",
                   map["sys.lru.convert.match"].c_str());
    return;
  }

  for (auto it = lMatchMap.begin(); it != lMatchMap.end(); it++) {
    std::string time_tag;
    std::string size_tag;
    eos::common::StringConversion::SplitKeyValue(it->second, time_tag, size_tag);

    if (time_tag.empty()) {
      time_tag = it->second;
    }

    bool size_smaller = false;
    bool size_larger  = false;
    size_t size_limit = 0;

    if (size_tag.length()) {
      if (size_tag.substr(0, 1) == "<") {
        size_smaller = true;
      }

      if (size_tag.substr(0, 1) == ">") {
        size_larger = true;
      }

      size_tag.erase(0, 1);

      if (!size_smaller && !size_larger) {
        eos_static_err("msg=\"LRU match attribute has illegal size\" "
                       " match=\"%s\", size=\"%s\"",
                       it->first.c_str(),
                       size_tag.c_str());
      } else {
        size_limit = eos::common::StringConversion::GetSizeFromString(size_tag.c_str());
      }
    }

    eos_static_info("time-tag=%s size-tag=%s <%d >%d limit=%lu", time_tag.c_str(),
                    size_tag.c_str(), size_smaller, size_larger, size_limit);
    time_t t = eos::common::StringConversion::GetSizeFromString(time_tag.c_str());

    if (errno) {
      eos_static_err("msg=\"LRU match attribute has illegal age\" "
                     "match=\"%s\", age=\"%s\"", it->first.c_str(),
                     time_tag.c_str());
    } else {
      std::string conv_attr = "sys.conversion.";
      conv_attr += it->first;

      if (map.count(conv_attr)) {
        lMatchAgeMap[it->first] = t;

        if (size_smaller) {
          lMatchSizeMap[it->first] = -size_limit;
        }

        if (size_larger) {
          lMatchSizeMap[it->first] = +size_limit;
        }

        eos_static_info("rule=\"%s %u\"", it->first.c_str(), t);
      } else {
        eos_static_err("msg=\"LRU match attribute has no conversion "
                       "attribute defined\" attr-missing=\"%s\"",
                       conv_attr.c_str());
      }
    }
  }

  std::vector < std::pair<FileId::fileid_t, std::string> > lConversionList;
  {
    // Check the directory contents
    std::shared_ptr<eos::IContainerMD> cmd;
    eos::Prefetcher::prefetchContainerMDWithChildrenAndWait(gOFS->eosView, dir);

    try {
      cmd = gOFS->eosView->getContainer(dir);
      std::shared_ptr<eos::IFileMD> fmd;
      std::string fullpath;
      XrdOucString fname;
      eos::IFileMD::ctime_t fctime;
      uint64_t fsize;
      eos::IFileMD::layoutId_t flayoutId;
      eos::IFileMD::id_t fid;

      for (auto fit = eos::FileMapIterator(cmd); fit.valid(); fit.next()) {
        fmd = cmd->findFile(fit.key());

        if (fmd == nullptr) {
          eos_static_err("msg=\"file is null\" fxid=%08llx", fit.key().c_str());
          continue;
        }

        {
          eos::MDLocking::FileReadLock fmdLock(fmd.get());
          fname = fmd->getName().c_str();
          fmd->getCTime(fctime);
          fsize = fmd->getSize();
          flayoutId = fmd->getLayoutId();
          fid = fmd->getId();
        }

        fullpath = dir;
        fullpath += fname.c_str();
        eos_static_debug("check_file=\"%s\"", fullpath.c_str());

        // Loop over the match map
        for (auto mit = lMatchAgeMap.begin(); mit != lMatchAgeMap.end(); mit++) {
          eos_static_debug("check_rule=\"%s\" matched=%d", mit->first.c_str(),
                           fname.matches(mit->first.c_str()));

          if (fname.matches(mit->first.c_str())) {
            // Full match check the age policy
            time_t age = mit->second;

            if ((fctime.tv_sec + age) < now) {
              std::string conv_attr = "sys.conversion.";
              conv_attr += mit->first;
              // Check if this file has already the proper layout
              std::string conversion = map[conv_attr];
              std::string plctplcy;

              if (((int)conversion.find("|")) != STR_NPOS) {
                eos::common::StringConversion::SplitKeyValue(conversion, conversion, plctplcy,
                    "|");
              }

              unsigned long long lid = strtoll(map[conv_attr].c_str(), 0, 16);

              if (flayoutId == lid) {
                eos_static_debug("msg=\"skipping conversion - file has already "
                                 "the desired target layout\" fxid=%08llx", fid);
                continue;
              }

              if (lMatchSizeMap.count(mit->first)) {
                if (lMatchSizeMap[mit->first] < 0) {
                  // check that this file is smaller as the required size
                  if ((ssize_t)fsize >= (-lMatchSizeMap[mit->first])) {
                    eos_static_debug("msg=\"skipping conversion - file is larger "
                                     "than required\" fxid=%08llx", fid);
                    continue;
                  } else {
                    eos_static_info("msg=\"converting according to age+size specification\" "
                                    "path='%s' fxid=%08llx required-size < %ld size=%ld layout:%08x :=> %08x",
                                    fullpath.c_str(), fid, -lMatchSizeMap[mit->first],
                                    (ssize_t)fsize, lid, flayoutId);
                  }
                }

                if (lMatchSizeMap[mit->first] > 0) {
                  // check that this file is larger than the required size
                  if ((ssize_t)fsize <= lMatchSizeMap[mit->first]) {
                    eos_static_debug("msg=\"skipping conversion - file is smaller "
                                     "than required\" fxid=%08llx", fid);
                    continue;
                  } else {
                    eos_static_info("msg=\"converting according to age+size specification\" "
                                    "path='%s' fxid=%08llx required-size > %ld size=%ld layout:%08x "
                                    ":=> %08x", fullpath.c_str(), fid, lMatchSizeMap[mit->first],
                                    (ssize_t)fsize, lid, flayoutId);
                  }
                }
              } else {
                eos_static_info("msg=\"converting according to age specification\" path='%s' "
                                "fxid=%08llx layout:%08x :=> %08x", fullpath.c_str(),
                                fid, lid, flayoutId);
              }

              // This entry can be converted
              eos_static_notice("msg=\"convert expired file\" path=\"%s\" "
                                "ctime=%u policy-age=%u age=%u fxid=%08llx "
                                "layout=\"%s\"", fullpath.c_str(), fctime.tv_sec,
                                age, now - fctime.tv_sec, (unsigned long long) fid,
                                map[conv_attr].c_str());
              lConversionList.push_back(std::make_pair(fid, map[conv_attr]));
              break;
            }
          }
        }
      }
    } catch (eos::MDException& e) {
      errno = e.getErrno();
      cmd.reset();
      eos_static_err("msg=\"exception\" ec=%d emsg=\"%s\"",
                     e.getErrno(), e.getMessage().str().c_str());
    }
  }

  for (auto it = lConversionList.begin(); it != lConversionList.end(); it++) {
    const eos::common::FileId::fileid_t fid = it->first;
    std::string conversion = it->second;
    std::string plctplcy;

    if (((int)conversion.find("|")) != STR_NPOS) {
      eos::common::StringConversion::SplitKeyValue(conversion, conversion, plctplcy,
          "|");
      plctplcy = "~" + plctplcy;
    }

    std::string space;

    if (map.count("user.forced.space")) {
      space = map["user.forced.space"];
    }

    if (map.count("sys.forced.space")) {
      space = map["sys.forced.space"];
    }

    if (map.count("sys.lru.conversion.space")) {
      space = map["sys.lru.conversion.space"];
    }

    // the conversion value can be directory an layout env representation like
    // "eos.space=...&eos.layout ..."
    XrdOucEnv cenv(conversion.c_str());

    if (cenv.Get("eos.space")) {
      space = cenv.Get("eos.space");
    }

    std::string conv_tag = ConversionTag::Get(it->first, space.c_str(), conversion,
                           plctplcy);

    if (gOFS->mConverterDriver->ScheduleJob(fid, conv_tag)) {
      eos_static_info("msg=\"LRU scheduled conversion job\" tag=\"%s\"",
                      conv_tag.c_str());
    } else {
      eos_static_err("msg=\"LRU failed to schedule conversion job\" "
                     "tag=\"%s\"", conv_tag.c_str());
    }
  }
}


EOSMGMNAMESPACE_END
