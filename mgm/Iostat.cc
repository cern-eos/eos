//------------------------------------------------------------------------------
// File: Iostat.cc
// Authors: Andreas-Joachim Peters - CERN
//          Elvin Alin Sindrilaru  - CERN
//          Jaroslav Guenther      - CERN
//
// Implementation follows presentation from EOS Workshop in 2022
// https://indico.cern.ch/event/1103358/contributions/4758312/attachments/2402845/4109660/EOS_IO_stat_monitoring.pdf
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2021 CERN/Switzerland                                  *
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

#include "common/table_formatter/TableFormatterBase.hh"
#include "common/Report.hh"
#include "common/Path.hh"
#include "common/JeMallocHandler.hh"
#include "common/Logging.hh"
#include "common/Timing.hh"
#include "common/StringTokenizer.hh"
#include "common/StringUtils.hh"
#include "mgm/Iostat.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/IMaster.hh"
#include "mq/MessagingRealm.hh"
#include "namespace/interface/IView.hh"
#include "namespace/ns_quarkdb/QdbContactDetails.hh"
#include "namespace/ns_quarkdb/flusher/MetadataFlusher.hh"
#include "namespace/ns_quarkdb/qclient/include/qclient/ResponseParsing.hh"
#include "namespace/Prefetcher.hh"
#include "mq/ReportListener.hh"
#include <XrdNet/XrdNetUtils.hh>
#include <XrdNet/XrdNetAddr.hh>
#include <curl/curl.h>

EOSMGMNAMESPACE_BEGIN

const char* Iostat::gIostatCollect = "iostat::collect";
const char* Iostat::gIostatReportSave = "iostat::report";
const char* Iostat::gIostatReportNamespace = "iostat::reportnamespace";
const char* Iostat::gIostatPopularity = "iostat::popularity";
const char* Iostat::gIostatUdpTargetList = "iostat::udptargets";
FILE* Iostat::gOpenReportFD = 0;
Period LAST_DAY = Period::DAY;
Period LAST_HOUR = Period::HOUR;
Period LAST_5MIN = Period::FIVEMIN;
Period LAST_1MIN = Period::ONEMIN;
PercentComplete P90 = PercentComplete::p90;
PercentComplete P95 = PercentComplete::p95;
PercentComplete P99 = PercentComplete::p99;
PercentComplete ALL = PercentComplete::p100;


//------------------------------------------------------------------------------
// IostatPeriods implementation
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Adds transfer data on a 24h timeline of [mDataBuffer]
// Provides:
// - [mLongestTransferTime] in last 24h
// - populates [mIntegralBuffer] and every 5 min extracts the
//   time of transfer for > 90/95/100 percent of data
// - [mDataBuffer] circular buffer for all transfers in the last 24h
//------------------------------------------------------------------------------
void
IostatPeriods::Add(unsigned long long val, time_t start, time_t stop,
                   time_t now)
{
  mTotal += val;
  double value = (double)val;

  // Window start/end times are "|", bin start [ and end ] times
  // period window = |-----------[--]----------|
  if (stop > now) {
    eos_static_err("%s", "msg=\"failed report digest, transfer "
                   "stop time in the future\"");
    return;
  }

  time_t t_window_start = now - (sBins * sBinWidth);

  if (stop <= t_window_start) {
    eos_static_warning("%s", "msg=\"failed report digest, transfer stopped "
                       "outside of collection time window\"");
    return;
  }

  time_t tdiff = stop - start + 1;

  if (tdiff < 1) {
    eos_static_err("%s", "msg=\"transfer start time after stop time\"");
    return;
  }

  StampBufferZero(now);
  mTfCount += 1;

  if (stop > mLastAddTime) {
    mLastAddTime = stop;
  }

  if (mLongestTransferTime < (unsigned int)tdiff) {
    mLongestTransferTime = tdiff;
  }

  time_t trep = now - stop;

  if (mLongestReportTime < (unsigned int)(trep)) {
    mLongestReportTime = trep;
  }

  // cutting off data out of time window
  if (start < t_window_start) {
    // re-calculate data portion to save into our time window
    value = ((stop - t_window_start) * value) / tdiff;
    start = t_window_start;
    tdiff = stop - start;
  }

  // Number of bins the measurement hits
  size_t mbins = tdiff / sBinWidth;
  double val_per_bin = value / mbins;
  int index_start = (start / sBinWidth) % sBins;

  for (size_t ibin = 0; ibin < mbins; ++ibin) {
    int bin_index = (index_start + ibin) % sBins;
    // Code block to be added and tested in case sBinWidth !=1
    // double ival = val_per_bin;
    // if (ibin == 0 and mbins > 1):
    //   time_t t_start_bin_duration = (sBins * sBinWidth) - (start - t_window_start) - sBinWidth * (mbins - 1)
    //   ival = (t_start_bin_duration * value) / tdiff;
    // if (ibin == mbins - 1 and mbins > 1):
    //   time_t t_stop_bin_duration = (sBins * sBinWidth) - (now - stop) - sBinWidth * (mbins - 1)
    //   ival = (t_stop_bin_duration * value) / tdiff;
    // mDataBuffer[bin_index] += ival;
    // mIntegralBuffer[ibin] += ival;
    mDataBuffer[bin_index] += val_per_bin;
    mIntegralBuffer[ibin] += val_per_bin;
  }
}

//------------------------------------------------------------------------------
// Update Transfer Buffer to iterate over and calculate how long does it take
// to transfer [mPercComplete] % of the data within sample rate of 5 min
// [mLastTfSampleUpdateInterval]
//------------------------------------------------------------------------------
void
IostatPeriods::UpdateTransferSampleInfo(time_t now)
{
  // Sum data of all transfers
  double sumTx = 0.;

  // Update rating (% of transfers)
  for (size_t i = 0; i < sBins; ++i) {
    if (mIntegralBuffer[i]) {
      sumTx += mIntegralBuffer[i];
    } else {
      break;
    }
  }

  // Reset counters for current sample
  mTfCountInSample = 0ull;
  mAvgTfSize = 0ull;

  //std::cout << "sumTx" << sumTx << std::endl;
  if (sumTx > 0) {
    mTfCountInSample = mTfCount;
    mAvgTfSize = std::ceil((double)sumTx / mTfCountInSample);
    const double multiplier = std::pow(10.0, 6);

    // integrate up to [mPercComplete] and record
    // the time the transfers took in [mDurationToPercComplete]
    for (size_t iperc = 0; iperc < std::size(mPercComplete); ++iperc) {
      double sum_percent = 0;

      for (size_t ibin = 0; ibin < sBins; ibin++) {
        sum_percent += mIntegralBuffer[ibin] / sumTx;

        if ((unsigned int)std::ceil(sum_percent * multiplier) >=
            (unsigned int)std::ceil(mPercComplete[iperc] * multiplier)) {
          mDurationToPercComplete[iperc] = ((ibin + 1) * sBinWidth);
          break;
        }
      }
    }
  } else {
    for (size_t iperc = 0; iperc < std::size(mPercComplete); ++iperc) {
      mDurationToPercComplete[iperc] = 0;
    }
  }

  mTfCount = 0;
  mLongestTransferTimeInSample = mLongestTransferTime;
  mLongestReportTimeInSample = mLongestReportTime;
  mLongestTransferTime = 0;
  mLongestReportTime = 0;
  memset(mIntegralBuffer, 0, sizeof(mIntegralBuffer));
  mLastTfMaxLenUpdateTime = now;
}

//------------------------------------------------------------------------------
// Reset bin content of the buffer w.r.t. given timstamp
//------------------------------------------------------------------------------
void
IostatPeriods::StampBufferZero(time_t& now)
{
  // Clean-up all bins which are older than sPeriod (24h)
  // last_end_index is the index of the timestamp corresponding
  // to the last transfer stop time recorded
  if ((now - mLastTfMaxLenUpdateTime) > mLastTfSampleUpdateInterval) {
    UpdateTransferSampleInfo(now);
  }

  time_t last_upd_time = std::max(mLastAddTime, mLastStampZeroTime);
  int zero_bins = 0;

  if (now - last_upd_time >= sPeriod) {
    zero_bins = sBins;
  } else {
    if (last_upd_time < now) {
      zero_bins = (now - last_upd_time) / sBinWidth;
    } else {
      if ((last_upd_time == mLastStampZeroTime) &&
          (last_upd_time != mLastAddTime)) {
        zero_bins = 1;
      }
    }
  }

  int start_index = (last_upd_time / sBinWidth) % sBins;

  if (last_upd_time != now) {
    start_index = (start_index + 1) % sBins;
  }

  for (int i = 0; i < zero_bins; ++i) {
    int index = (start_index + i) % sBins;
    mDataBuffer[index] = 0.;
  }

  mLastStampZeroTime = now;
}

//------------------------------------------------------------------------------
// Getting the timestamp of the last time the transfer sample was taken
//------------------------------------------------------------------------------
std::string IostatPeriods::GetLastSampleUpdateTimestamp(bool date_format) const
{
  std::string ts;

  if (date_format) {
    ts = common::Timing::ltime(mLastTfMaxLenUpdateTime);
  } else {
    ts = std::to_string(mLastTfMaxLenUpdateTime);
  }

  eos::common::trim(ts);
  return ts;
}


//------------------------------------------------------------------------------
// Get the sum of values for the given buffer period
//------------------------------------------------------------------------------
unsigned long long
IostatPeriods::GetDataInPeriod(size_t period, unsigned long long time_offset,
                               time_t now) const
{
  double sum = 0.;

  if (time_offset > sPeriod) {
    time_offset = sPeriod;
  }

  if (time_offset + period > sPeriod) {
    period = sPeriod - time_offset;
  }

  size_t start_index = ((now - time_offset - period) / sBinWidth) % sBins;
  size_t stop_index = ((now - time_offset) / sBinWidth) % sBins;
  int range = stop_index - start_index ;

  if (period >= sPeriod) {
    range = sBins;
    start_index = 0;
  }

  if (range < 0) {
    range = sBins + range;
  }

  for (int pidx_cnt = 0; pidx_cnt < range; ++pidx_cnt) {
    int idx = (start_index + pidx_cnt) % sBins;
    sum += mDataBuffer[idx];
  }

  return std::ceil(sum);
}

//------------------------------------------------------------------------------
// Iostat implementation
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Iostat constructor
//------------------------------------------------------------------------------
Iostat::Iostat():
  mDoneInit(false), mFlusher(nullptr), mLegacyMode(false), mRunning(false),
  mQcl(nullptr), mReportSave(true), mReportNamespace(false),
  mReportPopularity(true), mHashKeyBase("")
{
  for (size_t i = 0; i < IOSTAT_POPULARITY_HISTORY_DAYS; i++) {
    IostatPopularity[i].set_deleted_key("");
    IostatPopularity[i].resize(100000);
  }

  mLastPopularityBin = 9999999;
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
Iostat::~Iostat()
{
  (void) StopCollection();
  mCirculateThread.join();
}

//------------------------------------------------------------------------------
// Get hash key under which info is stored in QDB. This also included the
// current year and it's cached for ~5 minutes.
//------------------------------------------------------------------------------
std::string
Iostat::GetHashKey() const
{
  using namespace std::chrono;
  static std::string key;
  static seconds cache_interval {300};
  static auto ts = steady_clock::now();

  if (key.empty() ||
      (duration_cast<seconds>(steady_clock::now() - ts) > cache_interval)) {
    key = mHashKeyBase + eos::common::Timing::GetCurrentYear();
    ts = steady_clock::now();
  }

  return key;
}

//------------------------------------------------------------------------------
// Perform object initialization
//------------------------------------------------------------------------------
bool
Iostat::Init(const std::string& instance_name, int port,
             const std::string& legacy_file)
{
  mHashKeyBase = SSTR("eos-iostat:" << instance_name << ":");
  mFlusherPath = SSTR(gOFS->mQClientDir << instance_name << ":" << port
                      << "_iostat");

  if (gOFS) {
    // QDB namespace, initialize qclient
    if (!gOFS->namespaceGroup->isInMemory()) {
      const eos::QdbContactDetails& qdb_details = gOFS->mQdbContactDetails;
      mQcl.reset(new qclient::QClient(qdb_details.members,
                                      qdb_details.constructOptions()));

      if (!OneOffQdbMigration(legacy_file)) {
        eos_static_err("%s", "msg=\"failed while attempting migration to QDB\"");
        return false;
      }

      if (!LoadFromQdb()) {
        eos_static_err("%s", "msg=\"LoadFromQdb failed\"");
        return false;
      }

      if (mFlusher == nullptr) {
        mFlusher.reset(new eos::MetadataFlusher(mFlusherPath,
                                                gOFS->mQdbContactDetails));
      }
    } else {
      // In-memory namespace forces the stats to be saved in the file
      mLegacyMode = true;
      mLegacyFilePath = legacy_file;

      if (!LegacyRestoreFromFile()) {
        eos_static_err("msg=\"failed to restore info from file\" path=%s",
                       mLegacyFilePath.c_str());
        return false;
      }
    }
  }

  mCirculateThread.reset(&Iostat::Circulate, this);
  mDoneInit = true;
  return true;
}

//------------------------------------------------------------------------------
// One off migration from file based to QDB of IoStat information
//------------------------------------------------------------------------------
bool
Iostat::OneOffQdbMigration(const std::string& legacy_file)
{
  struct stat info;

  if (stat(legacy_file.c_str(), &info)) {
    // File does not exist, migration was probably already done
    return true;
  }

  FILE* fin = fopen(legacy_file.c_str(), "r");

  if (!fin) {
    eos_static_err("msg=\"failed to open iostat file\" path=\"%s\"",
                   legacy_file.c_str());
    return false;
  }

  int item = 0;
  char line[16384];
  std::string tag;
  std::list<std::string> entries;

  while ((item = fscanf(fin, "%16383s\n", line)) == 1) {
    XrdOucEnv env(line);

    if (env.Get("tag") && env.Get("uid") && env.Get("val")) {
      entries.push_back(EncodeKey(USER_ID_TYPE, env.Get("uid"), env.Get("tag")));
      entries.push_back(env.Get("val"));
    }

    if (env.Get("tag") && env.Get("gid") && env.Get("val")) {
      entries.push_back(EncodeKey(GROUP_ID_TYPE, env.Get("gid"), env.Get("tag")));
      entries.push_back(env.Get("val"));
    }
  }

  fclose(fin);
  // Push all the collected info to QDB
  qclient::QHash qhash(*mQcl, GetHashKey());

  try {
    bool done = qhash.hmset(entries);

    if (!done) {
      eos_static_err("%s", "msg=\"failed while inserting entries in QDB\"");
      return false;
    }
  } catch (const std::exception& e) {
    eos_static_err("msg=\"got exception while inserting entrines in QDB\" "
                   "emsg=\"%s\"", e.what());
    return false;
  }

  // Save file based iostat as a backup
  const std::string bkp_path = legacy_file + ".bkp";

  if (rename(legacy_file.c_str(), bkp_path.c_str())) {
    eos_static_err("msg=\"failed file rename\" old_path=\"%s\" new_path=\"%s\"",
                   legacy_file.c_str(), bkp_path.c_str());
    return false;
  }

  eos_static_info("msg=\"saved iostat backup successfully\" old_path=\"%s\" "
                  " new_path=\"%s\"", legacy_file.c_str(), bkp_path.c_str());
  return true;
}

//------------------------------------------------------------------------------
// Apply instance level configuration concerning IoStats
//------------------------------------------------------------------------------
void
Iostat::ApplyIostatConfig(FsView* fsview)
{
  std::string iocollect = fsview->GetGlobalConfig(gIostatCollect);

  if ((iocollect == "true") || (iocollect.empty())) {
    StartCollection(); // enable by default
  }

  std::string iopopularity = fsview->GetGlobalConfig(gIostatPopularity);
  mReportPopularity = (iopopularity == "true") || (iopopularity.empty());
  mReportSave = fsview->GetBoolGlobalConfig(gIostatReportSave);
  mReportNamespace = fsview->GetBoolGlobalConfig(gIostatReportNamespace);
  std::string udplist = fsview->GetGlobalConfig(gIostatUdpTargetList);
  std::string delimiter = "|";
  std::vector<std::string> hostlist;
  eos::common::StringConversion::Tokenize(udplist, hostlist, delimiter);
  std::unique_lock<std::mutex> scope_lock(mBcastMutex);
  mUdpPopularityTarget.clear();

  for (size_t i = 0; i < hostlist.size(); ++i) {
    AddUdpTarget(hostlist[i], false);
  }
}

//------------------------------------------------------------------------------
// Store IoStat config in the instance level configuration
//------------------------------------------------------------------------------
bool
Iostat::StoreIostatConfig(FsView* fsview) const
{
  bool ok = fsview->SetGlobalConfig(gIostatPopularity, mReportPopularity) &
            fsview->SetGlobalConfig(gIostatReportSave, mReportSave) &
            fsview->SetGlobalConfig(gIostatReportNamespace, mReportNamespace) &
            fsview->SetGlobalConfig(gIostatCollect, mRunning);
  std::string udp_popularity_targets = EncodeUdpPopularityTargets();

  if (!udp_popularity_targets.empty()) {
    ok &= fsview->SetGlobalConfig(gIostatUdpTargetList, udp_popularity_targets);
  }

  return ok;
}

//------------------------------------------------------------------------------
// Start collection thread
//------------------------------------------------------------------------------
bool
Iostat::StartCollection()
{
  std::unique_lock<std::mutex> scope_lock(mThreadSyncMutex);

  if (!mRunning) {
    mRunning = true;
    mReceivingThread.reset(&Iostat::Receive, this);
    StoreIostatConfig(&FsView::gFsView);
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Stop collection thread
//------------------------------------------------------------------------------
bool
Iostat::StopCollection()
{
  std::unique_lock<std::mutex> scope_lock(mThreadSyncMutex);

  if (mRunning) {
    mReceivingThread.join();
    mRunning = false;
    StoreIostatConfig(&FsView::gFsView);
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Start popularity thread
//------------------------------------------------------------------------------
bool
Iostat::StartPopularity()
{
  std::unique_lock<std::mutex> scope_lock(mThreadSyncMutex);

  if (!mReportPopularity) {
    mReportPopularity = true;
    StoreIostatConfig(&FsView::gFsView);
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Start popularity thread
//------------------------------------------------------------------------------
bool
Iostat::StopPopularity()
{
  std::unique_lock<std::mutex> scope_lock(mThreadSyncMutex);

  if (mReportPopularity) {
    mReportPopularity = false;
    StoreIostatConfig(&FsView::gFsView);
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Start daily report save thread
//------------------------------------------------------------------------------
bool
Iostat::StartReport()
{
  std::unique_lock<std::mutex> scope_lock(mThreadSyncMutex);

  if (!mReportSave) {
    mReportSave = true;
    StoreIostatConfig(&FsView::gFsView);
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Stop daily report save thread
//------------------------------------------------------------------------------
bool
Iostat::StopReport()
{
  std::unique_lock<std::mutex> scope_lock(mThreadSyncMutex);

  if (mReportSave) {
    mReportSave = false;
    StoreIostatConfig(&FsView::gFsView);
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Start namespace report thread
//------------------------------------------------------------------------------
bool
Iostat::StartReportNamespace()
{
  std::unique_lock<std::mutex> scope_lock(mThreadSyncMutex);

  if (!mReportNamespace) {
    mReportNamespace = true;
    StoreIostatConfig(&FsView::gFsView);
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Stop namespace report thread
//------------------------------------------------------------------------------
bool
Iostat::StopReportNamespace()
{
  std::unique_lock<std::mutex> scope_lock(mThreadSyncMutex);

  if (mReportNamespace) {
    mReportNamespace = false;
    StoreIostatConfig(&FsView::gFsView);
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Record measurement to the various periods it overlaps with
//------------------------------------------------------------------------------
void
Iostat::Add(const std::string& tag, uid_t uid, gid_t gid,
            unsigned long long val,
            time_t start, time_t stop, time_t now)
{
  // Flush to QDB if not in testing mode - this can be called without a lock
  // as this is only called from the thread digesting the report messages one
  // by one
  if (gOFS && !mLegacyMode) {
    AddToQdb(tag, uid, gid, val);
  }

  std::unique_lock<std::mutex> scope_lock(mDataMutex);
  IostatTag[tag] += val;
  IostatUid[tag][uid] += val;
  IostatGid[tag][gid] += val;
  IostatPeriodsUid[tag][uid].Add(val, start, stop, now);
  IostatPeriodsGid[tag][gid].Add(val, start, stop, now);
  IostatPeriodsTag[tag].Add(val, start, stop, now);
}

//------------------------------------------------------------------------------
// Low level implementation for Add method also sending data to QDB
//------------------------------------------------------------------------------
void
Iostat::AddToQdb(const std::string& tag, uid_t uid, gid_t gid,
                 unsigned long long val)
{
  if (mFlusher) {
    CacheUpdate(EncodeKey(USER_ID_TYPE, std::to_string(uid), tag),
                EncodeKey(GROUP_ID_TYPE, std::to_string(gid), tag), val);

    if (ShouldFlushCache()) {
      FlushCache();
    }
  }
}

//------------------------------------------------------------------------------
// Save given update in the in-memory cache
//------------------------------------------------------------------------------
void
Iostat::CacheUpdate(const std::string& uid_key, const std::string& gid_key,
                    unsigned long long val)
{
  mMapCacheUpdates[uid_key] += val;
  mMapCacheUpdates[gid_key] += val;
}

//------------------------------------------------------------------------------
// Check if the cache needs to be flushed
//------------------------------------------------------------------------------
bool
Iostat::ShouldFlushCache()
{
  using namespace std::chrono;
  static auto timestamp = steady_clock::now();

  if ((mMapCacheUpdates.size() >= mMapMaxSize) ||
      (duration_cast<seconds>(steady_clock::now() - timestamp) > mCacheFlushDelay)) {
    timestamp = steady_clock::now();
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Flush all cached entries to the QDB backed
//------------------------------------------------------------------------------
void
Iostat::FlushCache()
{
  using namespace std::chrono;
  using eos::common::Timing;
  static const hours timeout {1};
  static auto timestamp = steady_clock::now();
  static std::string hash_key = mHashKeyBase + Timing::GetCurrentYear();

  // Check for change of hash key when a new year starts
  if (duration_cast<minutes>(steady_clock::now() - timestamp) > timeout) {
    timestamp = steady_clock::now();
    std::string new_hash_key = mHashKeyBase + Timing::GetCurrentYear();

    if (new_hash_key != hash_key) {
      hash_key = new_hash_key;
    }
  }

  static std::vector<std::string> request;
  request.reserve(3 * mMapMaxSize + 1);
  request.push_back("HINCRBYMULTI");

  for (const auto& elem : mMapCacheUpdates) {
    const std::string svalue = std::to_string(elem.second);
    request.push_back(hash_key);
    request.push_back(elem.first);
    request.push_back(svalue);
  }

  mMapCacheUpdates.clear();
  mFlusher->exec(request);
  request.clear();
}

//------------------------------------------------------------------------------
// Get sum of measurements for the given tag (looping all uids per tag)
//------------------------------------------------------------------------------
unsigned long long
Iostat::GetTotalStatForTag(const char* tag) const
{
  unsigned long long val = 0ull;

  if (!IostatTag.count(tag)) {
    return val;
  }

  val = IostatTag.find(tag)->second;
  return val;
}

//------------------------------------------------------------------------------
// Get sum of measurements for the given tag an period (looping all uids per tag)
//------------------------------------------------------------------------------
unsigned long long
Iostat::GetPeriodStatForTag(const char* tag, size_t period, time_t secago) const
{
  auto it = IostatPeriodsTag.find(tag);

  if (it == IostatPeriodsTag.end()) {
    return 0ull;
  }

  return it->second.GetDataInPeriod(period, secago, time(0ull));
}

//------------------------------------------------------------------------------
// Method executed by the thread receiving reports
//------------------------------------------------------------------------------
void
Iostat::Receive(ThreadAssistant& assistant) noexcept
{
  eos_static_info("%s", "msg=\"starting iostat receive thread\"");

  if (gOFS == nullptr) {
    return;
  }

  while (!mDoneInit) {
    assistant.wait_for(std::chrono::seconds(5));

    if (assistant.terminationRequested()) {
      break;
    }
  }

  const std::string qdb_channel = "/eos/*/report";
  mq::ReportListener listener(gOFS->MgmOfsBroker.c_str(), gOFS->HostName,
                              gOFS->mMessagingRealm->haveQDB(),
                              gOFS->mQdbContactDetails, qdb_channel);

  while (!assistant.terminationRequested()) {
    std::string newmessage;

    while (listener.fetch(newmessage, &assistant)) {
      if (assistant.terminationRequested()) {
        break;
      }

      XrdOucString body = newmessage.c_str();

      while (body.replace("&&", "&")) {
      }

      XrdOucEnv ioreport(body.c_str());
      time_t now = time(0);
      std::unique_ptr<eos::common::Report> report(new eos::common::Report(ioreport));
      Add("bytes_read", report->uid, report->gid, report->rb, report->ots,
          report->cts, now);
      Add("bytes_read", report->uid, report->gid, report->rvb_sum, report->ots,
          report->cts, now);
      Add("bytes_written", report->uid, report->gid, report->wb, report->ots,
          report->cts, now);
      Add("read_calls", report->uid, report->gid, report->nrc, report->ots,
          report->cts, now);
      Add("readv_calls", report->uid, report->gid, report->rv_op, report->ots,
          report->cts, now);
      Add("write_calls", report->uid, report->gid, report->nwc, report->ots,
          report->cts, now);
      Add("fwd_seeks", report->uid, report->gid, report->nfwds, report->ots,
          report->cts, now);
      Add("bwd_seeks", report->uid, report->gid, report->nbwds, report->ots,
          report->cts, now);
      Add("xl_fwd_seeks", report->uid, report->gid, report->nxlfwds, report->ots,
          report->cts, now);
      Add("xl_bwd_seeks", report->uid, report->gid, report->nxlbwds, report->ots,
          report->cts, now);
      Add("bytes_fwd_seek", report->uid, report->gid, report->sfwdb, report->ots,
          report->cts, now);
      Add("bytes_bwd_wseek", report->uid, report->gid, report->sbwdb, report->ots,
          report->cts, now);
      Add("bytes_xl_fwd_seek", report->uid, report->gid, report->sxlfwdb, report->ots,
          report->cts, now);
      Add("bytes_xl_bwd_wseek", report->uid, report->gid, report->sxlbwdb,
          report->ots, report->cts, now);
      Add("disk_time_read", report->uid, report->gid, report->rt,
          report->ots, report->cts, now);
      Add("disk_time_write", report->uid, report->gid,
          report->wt, report->ots, report->cts, now);

      if (report->dsize) {
        Add("bytes_deleted", 0, 0, report->dsize, now - 30, now, now);
        Add("files_deleted", 0, 0, 1, now - 30, now, now);
      }

      // Do the UDP broadcasting
      UdpBroadCast(report.get());

      // Do the domain accounting
      if (report->path.substr(0, 11) == "/replicate:") {
        // check if this is a replication path
        // push into the 'eos' domain
        std::unique_lock<std::mutex> scope_lock(mDataMutex);

        if (report->rb) {
          IostatPeriodsDomainIOrb["eos"].Add(report->rb, report->ots, report->cts, now);
        }

        if (report->wb) {
          IostatPeriodsDomainIOwb["eos"].Add(report->wb, report->ots, report->cts, now);
        }
      } else {
        if (mReportPopularity) {
          // do the popularity accounting here for everything which is not replication!
          AddToPopularity(report->path, report->rb, report->ots, report->cts);
        }

        std::string sdomain = report->sec_domain;
        {
          std::unique_lock<std::mutex> scope_lock(mDataMutex);

          if (report->rb) {
            IostatPeriodsDomainIOrb[sdomain].Add(report->rb, report->ots, report->cts, now);
          }

          if (report->wb) {
            IostatPeriodsDomainIOwb[sdomain].Add(report->wb, report->ots, report->cts, now);
          }
        }
      }

      // do the application accounting here
      std::string apptag = "other";

      if (report->sec_app.length()) {
        apptag = report->sec_app;
      }

      // Push into app accounting
      {
        std::unique_lock<std::mutex> scope_lock(mDataMutex);

        if (report->rb) {
          IostatPeriodsAppIOrb[apptag].Add(report->rb, report->ots, report->cts, now);
        }

        if (report->wb) {
          IostatPeriodsAppIOwb[apptag].Add(report->wb, report->ots, report->cts, now);
        }
      }

      if (mReportSave && gOFS->mMaster->IsMaster()) {
        WriteRecord(body.c_str());
      }

      if (mReportNamespace) {
        // add the record into the report namespace file
        char path[4096];
        snprintf(path, sizeof(path) - 1, "%s/%s", gOFS->IoReportStorePath.c_str(),
                 report->path.c_str());
        eos::common::Path cPath(path);

        if (cPath.MakeParentPath(S_IRWXU | S_IRGRP | S_IXGRP)) {
          FILE* freport = fopen(path, "a+");

          if (freport) {
            fprintf(freport, "%s\n", body.c_str());
            fclose(freport);
          }
        }
      }
    }

    assistant.wait_for(std::chrono::seconds(1));
  }

  eos_static_info("%s", "msg=\"stopping iostat receiver thread\"");
}


//------------------------------------------------------------------------------
// Write record to the stream - used by the MGM/FUSEX to push entries
//------------------------------------------------------------------------------
void
Iostat::WriteRecord(const std::string& record)
{
  static uint32_t sec_per_day = 24 * 3600;
  static std::mutex s_mutex;
  static std::string s_report_fn = "";
  static time_t s_last_ts = 0ull;
  time_t now_ts = time(NULL);
  std::unique_lock<std::mutex> scope_lock(s_mutex);

  if (now_ts / sec_per_day != s_last_ts / sec_per_day) {
    struct tm nowtm;

    if (localtime_r(&now_ts, &nowtm)) {
      static char logfile[4096];
      snprintf(logfile, sizeof(logfile) - 1, "%s/%04u/%02u/%04u%02u%02u.eosreport",
               gOFS->IoReportStorePath.c_str(),
               1900 + nowtm.tm_year,
               nowtm.tm_mon + 1,
               1900 + nowtm.tm_year,
               nowtm.tm_mon + 1,
               nowtm.tm_mday);
      std::string report_fn = logfile;

      if (report_fn != s_report_fn) {
        if (gOpenReportFD) {
          fclose(gOpenReportFD);
          gOpenReportFD = nullptr;
        }

        eos::common::Path cPath(report_fn.c_str());

        if (cPath.MakeParentPath(S_IRWXU | S_IRGRP | S_IXGRP)) {
          gOpenReportFD = fopen(report_fn.c_str(), "a+");

          if (!gOpenReportFD) {
            eos_static_err("msg=\"failed to open report file\" path=%s",
                           report_fn.c_str());
            return;
          }
        } else {
          eos_static_err("msg=\"failed to create report parent path\" path=%s",
                         report_fn.c_str());
          return;
        }

        s_report_fn = report_fn;
        s_last_ts = now_ts;
      }
    }
  }

  if (gOpenReportFD) {
    fprintf(gOpenReportFD, "%s\n", record.c_str());
    fflush(gOpenReportFD);
  }
}

//------------------------------------------------------------------------------
// Print IO statistics
//------------------------------------------------------------------------------
void
Iostat::PrintOut(XrdOucString& out, bool summary, bool details,
                 bool monitoring, bool numerical, bool top,
                 bool domain, bool apps, bool sample_stat, time_t time_ago,
                 time_t time_interval, XrdOucString option)
{
  std::string format_s = (!monitoring ? "s" : "os");
  std::string format_ss = (!monitoring ? "-s" : "os");
  std::string format_l = (!monitoring ? "+l" : "ol");
  std::string format_ll = (!monitoring ? "l." : "ol");
  std::unique_lock<std::mutex> scope_lock(mDataMutex);
  time_t now = time(NULL);
  bool interval = false;
  time_ago = time_ago % 86400;
  time_interval = time_interval % 86400;

  if (time_interval != 0) {
    interval = true;
  }

  std::vector<std::string> tags;

  if (summary || top) {
    for (auto tit = IostatTag.begin(); tit != IostatTag.end(); ++tit) {
      tags.push_back(tit->first);
    }

    std::sort(tags.begin(), tags.end());
  }

  if (summary) {
    TableFormatterBase table;
    TableData table_data;

    if (interval) {
      if (!monitoring) {
        table.SetHeader({
          std::make_tuple("who", 3, format_ss),
          std::make_tuple("io value", 24, format_s),
          std::make_tuple("data in interval", 8, format_l),
          std::make_tuple("avg rate [B/s]", 8, format_l),
        });
      } else {
        table.SetHeader({
          std::make_tuple("uid", 0, format_ss),
          std::make_tuple("gid", 0, format_s),
          std::make_tuple("measurement", 0, format_s),
          std::make_tuple("intervaldata", 8, format_l),
          std::make_tuple("intervalrate", 8, format_l),
        });
      }
    } else {
      if (!monitoring) {
        table.SetHeader({
          std::make_tuple("who", 3, format_ss),
          std::make_tuple("io value", 24, format_s),
          std::make_tuple("1min", 8, format_l),
          std::make_tuple("5min", 8, format_l),
          std::make_tuple("1h", 8, format_l),
          std::make_tuple("24h", 8, format_l),
          std::make_tuple("sum", 8, format_l),
        });
      } else {
        table.SetHeader({
          std::make_tuple("uid", 0, format_ss),
          std::make_tuple("gid", 0, format_s),
          std::make_tuple("measurement", 0, format_s),
          std::make_tuple("60s", 0, format_l),
          std::make_tuple("300s", 0, format_l),
          std::make_tuple("3600s", 0, format_l),
          std::make_tuple("86400s", 0, format_l),
          std::make_tuple("total", 0, format_l),
        });
      }
    }

    for (const auto& elem : tags) {
      const char* tag = elem.c_str();
      table_data.emplace_back();
      TableRow& row = table_data.back();
      row.emplace_back("all", format_ss);

      if (monitoring) {
        row.emplace_back("all", format_s);
      }

      row.emplace_back(tag, format_s);

      if (interval) {
        row.emplace_back(GetPeriodStatForTag(tag, time_interval, time_ago), format_ll);
        row.emplace_back(GetPeriodStatForTag(tag, time_interval,
                                             time_ago) / (float)time_interval, format_ll);
      } else {
        // getting tag stat sums for 1day (idx=0), 1h (idx=1), 5m (idx=2), 1min (idx=3)
        row.emplace_back(GetPeriodStatForTag(tag, 60), format_ll);
        row.emplace_back(GetPeriodStatForTag(tag, 300), format_ll);
        row.emplace_back(GetPeriodStatForTag(tag, 3600), format_ll);
        row.emplace_back(GetPeriodStatForTag(tag, 86400), format_ll);
        row.emplace_back(GetTotalStatForTag(tag), format_ll);
      }
    }

    table.AddRows(table_data);
    out += table.GenerateTable(HEADER).c_str();
    table_data.clear();

    if (!interval) {
      //! UDP Popularity Broadcast Target
      std::unique_lock<std::mutex> mLock(mBcastMutex);

      if (!mUdpPopularityTarget.empty()) {
        TableFormatterBase table_udp;

        if (!monitoring) {
          table_udp.SetHeader({
            std::make_tuple("UDP Popularity Broadcast Target", 32, format_ss)
          });
        } else {
          table_udp.SetHeader({ std::make_tuple("udptarget", 0, format_ss) });
        }

        for (const auto& elem : mUdpPopularityTarget) {
          table_data.emplace_back();
          table_data.back().emplace_back(elem.c_str(), format_ss);
        }

        table_udp.AddRows(table_data);
        out += table_udp.GenerateTable(HEADER).c_str();
      }
    }
  }

  if (details) {
    if (interval) {
      std::vector<std::tuple<std::string, std::string, unsigned long long, unsigned long long>>
          uidout, gidout;
      TableFormatterBase table_user;
      TableData table_data;

      //! User statistic
      if (!monitoring) {
        table_user.SetHeader({
          std::make_tuple("user", 5, format_ss),
          std::make_tuple("io value", 24, format_s),
          std::make_tuple("data in interval", 8, format_l),
          std::make_tuple("avg rate [B/s]", 8, format_l),
        });
      } else {
        table_user.SetHeader({
          std::make_tuple("uid", 0, format_ss),
          std::make_tuple("measurement", 0, format_s),
          std::make_tuple("intervaldata", 8, format_l),
          std::make_tuple("intervalrate", 8, format_l),
        });
      }

      for (auto tuit = IostatPeriodsUid.begin(); tuit != IostatPeriodsUid.end();
           tuit++) {
        for (auto it = tuit->second.begin(); it != tuit->second.end(); ++it) {
          std::string username;

          if (numerical) {
            username = std::to_string(it->first);
          } else {
            int terrc = 0;
            username = eos::common::Mapping::UidToUserName(it->first, terrc);
          }

          // getting tag stat sums for 1day (idx=0), 1h (idx=1), 5m (idx=2), 1min (idx=3)
          uidout.emplace_back(std::make_tuple(username, tuit->first.c_str(),
                                              it->second.GetDataInPeriod(time_interval, time_ago, now),
                                              it->second.GetDataInPeriod(time_interval, time_ago, now) / (float)time_interval
                                             ));
        }
      }

      std::sort(uidout.begin(), uidout.end());

      for (auto& tup : uidout) {
        table_data.emplace_back();
        TableRow& row = table_data.back();
        row.emplace_back(std::get<0>(tup), format_ss);
        row.emplace_back(std::get<1>(tup), format_s);
        row.emplace_back(std::get<2>(tup), format_l);
        row.emplace_back(std::get<3>(tup), format_l);
      }

      table_user.AddRows(table_data);
      out += table_user.GenerateTable(HEADER).c_str();
      table_data.clear();
      // Group statistics
      TableFormatterBase table_group;

      if (!monitoring) {
        table_group.SetHeader({
          std::make_tuple("group", 5, format_ss),
          std::make_tuple("io value", 24, format_s),
          std::make_tuple("data in interval", 8, format_l),
          std::make_tuple("avg rate [B/s]", 8, format_l),
        });
      } else {
        table_group.SetHeader({
          std::make_tuple("gid", 0, format_ss),
          std::make_tuple("measurement", 0, format_s),
          std::make_tuple("intervaldata", 8, format_l),
          std::make_tuple("intervalrate", 8, format_l),
        });
      }

      for (auto tgit = IostatPeriodsGid.begin(); tgit != IostatPeriodsGid.end();
           tgit++) {
        for (auto it = tgit->second.begin(); it != tgit->second.end(); ++it) {
          std::string groupname;

          if (numerical) {
            groupname = std::to_string(it->first);
          } else {
            int terrc = 0;
            groupname = eos::common::Mapping::GidToGroupName(it->first, terrc);
          }

          // getting stat sums for 1day (idx=0), 1h (idx=1), 5m (idx=2), 1min (idx=3)
          gidout.emplace_back(std::make_tuple(groupname, tgit->first.c_str(),
                                              it->second.GetDataInPeriod(time_interval, time_ago, now),
                                              it->second.GetDataInPeriod(time_interval, time_ago, now) / (float)time_interval
                                             ));
        }
      }

      std::sort(gidout.begin(), gidout.end());

      for (auto& tup : gidout) {
        table_data.emplace_back();
        TableRow& row = table_data.back();
        row.emplace_back(std::get<0>(tup), format_ss);
        row.emplace_back(std::get<1>(tup), format_s);
        row.emplace_back(std::get<2>(tup), format_l);
        row.emplace_back(std::get<3>(tup), format_l);
      }

      table_group.AddRows(table_data);
      out += table_group.GenerateTable(HEADER).c_str();
      table_data.clear();
    } else {
      std::vector<std::tuple<std::string, std::string, unsigned long long,
          unsigned long long, unsigned long long, unsigned long long,
          unsigned long long, unsigned long long, unsigned long long, std::string>>
          uidout_sec, gidout_sec;
      std::vector<std::tuple<std::string, std::string, unsigned long long,
          unsigned long long, unsigned long long, unsigned long long,
          unsigned long long>>
          uidout_b, gidout_b;
      //std::vector<std::tuple<std::string, std::string, unsigned long long,
      //    unsigned long long, unsigned long long, unsigned long long, unsigned long long>>
      //    uidout, gidout;
      TableData table_data;
      XrdOucString marker_b =
        "\n┏━> Sum of bytes transferred in last 1m/5m/1h/24h and total sum: \n";
      //! User statistic
      TableFormatterBase table_user_b;

      if (!monitoring) {
        table_user_b.SetHeader({
          std::make_tuple("user", 5, format_ss),
          std::make_tuple("io value", 24, format_s),
          std::make_tuple("1min", 8, format_l),
          std::make_tuple("5min", 8, format_l),
          std::make_tuple("1h", 8, format_l),
          std::make_tuple("24h", 8, format_l),
          std::make_tuple("sum", 8, format_l),
        });
      } else {
        table_user_b.SetHeader({
          std::make_tuple("uid", 0, format_ss),
          std::make_tuple("measurement", 0, format_s),
          std::make_tuple("60s", 0, format_l),
          std::make_tuple("300s", 0, format_l),
          std::make_tuple("3600s", 0, format_l),
          std::make_tuple("86400s", 0, format_l),
          std::make_tuple("total", 0, format_l),
        });
      }

      XrdOucString marker_sec =
        "\n┏━> Transfer (tf) sample info every 5 min: tf time for 90/95/99% of data, max tf and report times, average tf size, tf count.\n";
      TableFormatterBase table_user_sec;

      if (sample_stat) {
        if (!monitoring) {
          table_user_sec.SetHeader({
            std::make_tuple("user", 5, format_ss),
            std::make_tuple("io value", 24, format_s),
            std::make_tuple("90% [s]", 8, format_l),
            std::make_tuple("95% [s]", 8, format_l),
            std::make_tuple("99% [s]", 8, format_l),
            std::make_tuple("max [s]", 8, format_l),
            std::make_tuple("max report [s]", 8, format_l),
            std::make_tuple("avg tf size", 8, format_l),
            std::make_tuple("tf #", 8, format_l),
            std::make_tuple("sample end time", 24, format_s)
          });
        } else {
          table_user_sec.SetHeader({
            std::make_tuple("uid", 0, format_ss),
            std::make_tuple("measurement", 0, format_s),
            std::make_tuple("tfsecto90p", 0, format_l),
            std::make_tuple("tfsecto95p", 0, format_l),
            std::make_tuple("tfsecto99p", 0, format_l),
            std::make_tuple("maxtransfersec", 0, format_l),
            std::make_tuple("maxreportsec", 0, format_l),
            std::make_tuple("avgtfsize5min", 0, format_l),
            std::make_tuple("tfcount", 0, format_l),
            std::make_tuple("sampletimestamp", 0, format_s)
          });
        }
      }

      for (auto tuit = IostatPeriodsUid.begin(); tuit != IostatPeriodsUid.end();
           tuit++) {
        for (auto it = tuit->second.begin(); it != tuit->second.end(); ++it) {
          std::string username;

          if (numerical) {
            username = std::to_string(it->first);
          } else {
            int terrc = 0;
            username = eos::common::Mapping::UidToUserName(it->first, terrc);
          }

          // getting tag stat sums for 1day (idx=0), 1h (idx=1), 5m (idx=2), 1min (idx=3)
          uidout_b.emplace_back(std::make_tuple(username, tuit->first.c_str(),
                                                it->second.GetDataInPeriod(60, 0, now),
                                                it->second.GetDataInPeriod(300, 0, now),
                                                it->second.GetDataInPeriod(3600, 0, now),
                                                it->second.GetDataInPeriod(86400, 0, now),
                                                IostatUid[tuit->first][it->first]
                                               ));

          if (sample_stat) {
            std::string sample_time = "";

            if (!monitoring) {
              sample_time = it->second.GetLastSampleUpdateTimestamp(true);
            } else {
              sample_time = it->second.GetLastSampleUpdateTimestamp(false);
            }

            uidout_sec.emplace_back(std::make_tuple(username, tuit->first.c_str(),
                                                    it->second.GetTimeToPercComplete(P90),
                                                    it->second.GetTimeToPercComplete(P95),
                                                    it->second.GetTimeToPercComplete(P99),
                                                    it->second.GetLongestTransferTime(),
                                                    it->second.GetLongestReportTime(),
                                                    it->second.GetAvgTransferSize(),
                                                    it->second.GetTfCountInSample(),
                                                    sample_time
                                                   ));
          }
        }
      }

      std::sort(uidout_b.begin(), uidout_b.end());
      std::sort(uidout_sec.begin(), uidout_sec.end());

      for (auto& tup : uidout_b) {
        table_data.emplace_back();
        TableRow& row = table_data.back();
        row.emplace_back(std::get<0>(tup), format_ss);
        row.emplace_back(std::get<1>(tup), format_s);
        row.emplace_back(std::get<2>(tup), format_l);
        row.emplace_back(std::get<3>(tup), format_l);
        row.emplace_back(std::get<4>(tup), format_l);
        row.emplace_back(std::get<5>(tup), format_l);
        row.emplace_back(std::get<6>(tup), format_l);
      }

      table_user_b.AddRows(table_data);
      out += !monitoring ? marker_b : "";
      out += table_user_b.GenerateTable(HEADER).c_str();
      table_data.clear();

      if (sample_stat) {
        for (auto& tup : uidout_sec) {
          table_data.emplace_back();
          TableRow& row = table_data.back();
          row.emplace_back(std::get<0>(tup), format_ss);
          row.emplace_back(std::get<1>(tup), format_s);
          row.emplace_back(std::get<2>(tup), format_l);
          row.emplace_back(std::get<3>(tup), format_l);
          row.emplace_back(std::get<4>(tup), format_l);
          row.emplace_back(std::get<5>(tup), format_l);
          row.emplace_back(std::get<6>(tup), format_l);
          row.emplace_back(std::get<7>(tup), format_l);
          row.emplace_back(std::get<8>(tup), format_l);
          row.emplace_back(std::get<9>(tup), format_s);
        }

        table_user_sec.AddRows(table_data);
        out += !monitoring ? marker_sec : "";
        out += table_user_sec.GenerateTable(HEADER).c_str();
        table_data.clear();
      }

      //! Group statistic
      TableFormatterBase table_group_b;

      if (!monitoring) {
        table_group_b.SetHeader({
          std::make_tuple("group", 5, format_ss),
          std::make_tuple("io value", 24, format_s),
          std::make_tuple("1min", 8, format_l),
          std::make_tuple("5min", 8, format_l),
          std::make_tuple("1h", 8, format_l),
          std::make_tuple("24h", 8, format_l),
          std::make_tuple("sum", 8, format_l),
        });
      } else {
        table_group_b.SetHeader({
          std::make_tuple("gid", 0, format_ss),
          std::make_tuple("measurement", 0, format_s),
          std::make_tuple("60s", 0, format_l),
          std::make_tuple("300s", 0, format_l),
          std::make_tuple("3600s", 0, format_l),
          std::make_tuple("86400s", 0, format_l),
          std::make_tuple("total", 0, format_l),
        });
      }

      TableFormatterBase table_group_sec;

      if (sample_stat) {
        if (!monitoring) {
          table_group_sec.SetHeader({
            std::make_tuple("group", 5, format_ss),
            std::make_tuple("io value", 24, format_s),
            std::make_tuple("90% [s]", 8, format_l),
            std::make_tuple("95% [s]", 8, format_l),
            std::make_tuple("99% [s]", 8, format_l),
            std::make_tuple("max [s]", 8, format_l),
            std::make_tuple("max report [s]", 8, format_l),
            std::make_tuple("avg tf size", 8, format_l),
            std::make_tuple("tf #", 8, format_l),
            std::make_tuple("sample end time", 24, format_s)
          });
        } else {
          table_group_sec.SetHeader({
            std::make_tuple("gid", 0, format_ss),
            std::make_tuple("measurement", 0, format_s),
            std::make_tuple("tfsecto90p", 0, format_l),
            std::make_tuple("tfsecto95p", 0, format_l),
            std::make_tuple("tfsecto99p", 0, format_l),
            std::make_tuple("maxtransfersec", 0, format_l),
            std::make_tuple("maxreportsec", 0, format_l),
            std::make_tuple("avgtfsize5min", 0, format_l),
            std::make_tuple("tfcount", 0, format_l),
            std::make_tuple("sampletimestamp", 0, format_s)

          });
        }
      }

      for (auto tgit = IostatPeriodsGid.begin(); tgit != IostatPeriodsGid.end();
           tgit++) {
        for (auto it = tgit->second.begin(); it != tgit->second.end(); ++it) {
          std::string groupname;

          if (numerical) {
            groupname = std::to_string(it->first);
          } else {
            int terrc = 0;
            groupname = eos::common::Mapping::GidToGroupName(it->first, terrc);
          }

          // getting stat sums for 1day (idx=0), 1h (idx=1), 5m (idx=2), 1min (idx=3)
          gidout_b.emplace_back(std::make_tuple(groupname, tgit->first.c_str(),
                                                it->second.GetDataInPeriod(60, 0, now), it->second.GetDataInPeriod(300, 0, now),
                                                it->second.GetDataInPeriod(3600, 0, now), it->second.GetDataInPeriod(86400, 0,
                                                    now),
                                                IostatGid[tgit->first][it->first]
                                               ));

          if (sample_stat) {
            std::string sample_time = "";

            if (!monitoring) {
              sample_time = it->second.GetLastSampleUpdateTimestamp(true);
            } else {
              sample_time = it->second.GetLastSampleUpdateTimestamp(false);
            }

            gidout_sec.emplace_back(std::make_tuple(groupname, tgit->first.c_str(),
                                                    it->second.GetTimeToPercComplete(P90),
                                                    it->second.GetTimeToPercComplete(P95),
                                                    it->second.GetTimeToPercComplete(P99),
                                                    it->second.GetLongestTransferTime(),
                                                    it->second.GetLongestReportTime(),
                                                    it->second.GetAvgTransferSize(),
                                                    it->second.GetTfCountInSample(),
                                                    sample_time
                                                   ));
          }
        }
      }

      std::sort(gidout_b.begin(), gidout_b.end());
      std::sort(gidout_sec.begin(), gidout_sec.end());

      for (auto& tup : gidout_b) {
        table_data.emplace_back();
        TableRow& row = table_data.back();
        row.emplace_back(std::get<0>(tup), format_ss);
        row.emplace_back(std::get<1>(tup), format_s);
        row.emplace_back(std::get<2>(tup), format_l);
        row.emplace_back(std::get<3>(tup), format_l);
        row.emplace_back(std::get<4>(tup), format_l);
        row.emplace_back(std::get<5>(tup), format_l);
        row.emplace_back(std::get<6>(tup), format_l);
      }

      table_group_b.AddRows(table_data);
      out += !monitoring ? marker_b : "";
      out += table_group_b.GenerateTable(HEADER).c_str();
      table_data.clear();

      if (sample_stat) {
        for (auto& tup : gidout_sec) {
          table_data.emplace_back();
          TableRow& row = table_data.back();
          row.emplace_back(std::get<0>(tup), format_ss);
          row.emplace_back(std::get<1>(tup), format_s);
          row.emplace_back(std::get<2>(tup), format_l);
          row.emplace_back(std::get<3>(tup), format_l);
          row.emplace_back(std::get<4>(tup), format_l);
          row.emplace_back(std::get<5>(tup), format_l);
          row.emplace_back(std::get<6>(tup), format_l);
          row.emplace_back(std::get<7>(tup), format_l);
          row.emplace_back(std::get<8>(tup), format_l);
          row.emplace_back(std::get<9>(tup), format_s);
        }

        table_group_sec.AddRows(table_data);
        out += !monitoring ? marker_sec : "";
        out += table_group_sec.GenerateTable(HEADER).c_str();
        table_data.clear();
      }
    }
  }

  if (top) {
    TableFormatterBase table;
    TableData table_data;

    if (!monitoring) {
      table.SetHeader({
        std::make_tuple("io value", 18, format_ss),
        std::make_tuple("ranking by", 10, format_s),
        std::make_tuple("rank", 8, format_ll),
        std::make_tuple("who", 4, format_s),
        std::make_tuple("sum", 8, format_l)
      });
    } else {
      table.SetHeader({
        std::make_tuple("measurement", 0, format_ss),
        std::make_tuple("rank", 0, format_ll),
        std::make_tuple("uid", 0, format_s),
        std::make_tuple("gid", 0, format_s),
        std::make_tuple("counter", 0, format_l)
      });
    }

    for (auto it = tags.begin(); it != tags.end(); ++it) {
      std::vector <std::tuple<unsigned long long, uid_t>> uidout, gidout;
      table.AddSeparator();

      // by uid name
      for (auto sit : IostatUid[*it]) {
        uidout.push_back(std::make_tuple(sit.second, sit.first));
      }

      std::sort(uidout.begin(), uidout.end());
      std::reverse(uidout.begin(), uidout.end());
      int topplace = 0;

      for (auto sit : uidout) {
        topplace++;
        uid_t uid = std::get<1>(sit);
        unsigned long long counter = std::get<0>(sit);
        std::string username;

        if (numerical) {
          username = std::to_string(uid);
        } else {
          int terrc = 0;
          username = eos::common::Mapping::UidToUserName(uid, terrc);
        }

        table_data.emplace_back();
        TableRow& row = table_data.back();
        row.emplace_back(it->c_str(), format_ss);

        if (!monitoring) {
          row.emplace_back("user", format_s);
        }

        row.emplace_back(topplace, format_ll);
        row.emplace_back(username, format_s);

        if (monitoring) {
          row.emplace_back("", "", "", true);
        }

        row.emplace_back(counter, format_l);
      }

      // by gid name
      for (auto sit : IostatGid[*it]) {
        gidout.push_back(std::make_tuple(sit.second, sit.first));
      }

      std::sort(gidout.begin(), gidout.end());
      std::reverse(gidout.begin(), gidout.end());
      topplace = 0;

      for (auto sit : gidout) {
        topplace++;
        uid_t gid = std::get<1>(sit);
        unsigned long long counter = std::get<0>(sit);
        std::string groupname;

        if (numerical) {
          groupname = std::to_string(gid);
        } else {
          int terrc = 0;
          groupname = eos::common::Mapping::GidToGroupName(gid, terrc);
        }

        table_data.emplace_back();
        TableRow& row = table_data.back();
        row.emplace_back(it->c_str(), format_ss);

        if (!monitoring) {
          row.emplace_back("group", format_s);
        }

        row.emplace_back(topplace, format_ll);

        if (monitoring) {
          row.emplace_back("", "", "", true);
        }

        row.emplace_back(groupname, format_s);
        row.emplace_back(counter, format_l);
      }
    }

    table.AddRows(table_data);
    out += table.GenerateTable(HEADER).c_str();
  }

  if (domain) {
    TableData table_data;

    if (interval) {
      TableFormatterBase table;

      //! User statistic
      if (!monitoring) {
        table.SetHeader({
          std::make_tuple("io", 5, format_ss),
          std::make_tuple("domain", 24, format_s),
          std::make_tuple("data in interval", 8, format_l),
          std::make_tuple("avg rate [B/s]", 8, format_l),
        });
      } else {
        table.SetHeader({
          std::make_tuple("measurement", 0, format_ss),
          std::make_tuple("domain", 0, format_s),
          std::make_tuple("intervaldata", 8, format_l),
          std::make_tuple("intervalrate", 8, format_l),
        });
      }

      // IO out bytes
      for (auto it = IostatPeriodsDomainIOrb.begin();
           it != IostatPeriodsDomainIOrb.end();
           ++it) {
        table_data.emplace_back();
        TableRow& row = table_data.back();
        std::string name = !monitoring ? "out" : "domain_io_out";
        row.emplace_back(name, format_ss);
        row.emplace_back(it->first.c_str(), format_s);
        row.emplace_back(it->second.GetDataInPeriod(time_interval, time_ago, now),
                         format_ll);
        row.emplace_back(it->second.GetDataInPeriod(time_interval, time_ago,
                         now) / (float)time_interval, format_ll);
      }

      // IO in bytes
      for (auto it = IostatPeriodsDomainIOwb.begin();
           it != IostatPeriodsDomainIOwb.end();
           ++it) {
        table_data.emplace_back();
        TableRow& row = table_data.back();
        std::string name = !monitoring ? "in" : "domain_io_in";
        row.emplace_back(name, format_ss);
        row.emplace_back(it->first.c_str(), format_s);
        // getting stat sums for 1day (idx=0), 1h (idx=1), 5m (idx=2), 1min (idx=3)
        row.emplace_back(it->second.GetDataInPeriod(time_interval, time_ago, now),
                         format_ll);
        row.emplace_back(it->second.GetDataInPeriod(time_interval, time_ago,
                         now) / (float)time_interval, format_ll);
      }

      table.AddRows(table_data);
      out += table.GenerateTable(HEADER).c_str();
      table_data.clear();
    } else {
      XrdOucString marker_b =
        "\n┏━> Sum of bytes transferred in last 1m/5m/1h/24h and total sum: \n";
      //! User statistic
      TableFormatterBase table_domain_b;

      if (!monitoring) {
        table_domain_b.SetHeader({
          std::make_tuple("io", 5, format_ss),
          std::make_tuple("domain", 24, format_s),
          std::make_tuple("1min", 8, format_l),
          std::make_tuple("5min", 8, format_l),
          std::make_tuple("1h", 8, format_l),
          std::make_tuple("24h", 8, format_l),
          std::make_tuple("sum", 8, format_l),
        });
      } else {
        table_domain_b.SetHeader({
          std::make_tuple("measurement", 0, format_ss),
          std::make_tuple("domain", 0, format_s),
          std::make_tuple("60s", 0, format_l),
          std::make_tuple("300s", 0, format_l),
          std::make_tuple("3600s", 0, format_l),
          std::make_tuple("86400s", 0, format_l),
          std::make_tuple("total", 0, format_l),
        });
      }

      // IO out bytes
      for (auto it = IostatPeriodsDomainIOrb.begin();
           it != IostatPeriodsDomainIOrb.end();
           ++it) {
        table_data.emplace_back();
        TableRow& row = table_data.back();
        std::string name = !monitoring ? "out" : "domain_io_out";
        row.emplace_back(name, format_ss);
        row.emplace_back(it->first.c_str(), format_s);
        // getting stat sums for 1day (idx=0), 1h (idx=1), 5m (idx=2), 1min (idx=3)
        row.emplace_back(it->second.GetDataInPeriod(60, 0, now), format_l);
        row.emplace_back(it->second.GetDataInPeriod(300, 0, now), format_l);
        row.emplace_back(it->second.GetDataInPeriod(3600, 0, now), format_l);
        row.emplace_back(it->second.GetDataInPeriod(86400, 0, now), format_l);
        row.emplace_back(it->second.GetTotalSum(), format_l);
      }

      // IO in bytes
      for (auto it = IostatPeriodsDomainIOwb.begin();
           it != IostatPeriodsDomainIOwb.end();
           ++it) {
        table_data.emplace_back();
        TableRow& row = table_data.back();
        std::string name = !monitoring ? "in" : "domain_io_in";
        row.emplace_back(name, format_ss);
        row.emplace_back(it->first.c_str(), format_s);
        // getting stat sums for 1day (idx=0), 1h (idx=1), 5m (idx=2), 1min (idx=3)
        row.emplace_back(it->second.GetDataInPeriod(60, 0, now), format_l);
        row.emplace_back(it->second.GetDataInPeriod(300, 0, now), format_l);
        row.emplace_back(it->second.GetDataInPeriod(3600, 0, now), format_l);
        row.emplace_back(it->second.GetDataInPeriod(86400, 0, now), format_l);
        row.emplace_back(it->second.GetTotalSum(), format_l);
      }

      table_domain_b.AddRows(table_data);
      out += !monitoring ? marker_b : "";
      out += table_domain_b.GenerateTable(HEADER).c_str();
      table_data.clear();
    }
  }

  if (apps) {
    TableData table_data;

    if (interval) {
      TableFormatterBase table;

      //! User statistic
      if (!monitoring) {
        table.SetHeader({
          std::make_tuple("io", 5, format_ss),
          std::make_tuple("application", 24, format_s),
          std::make_tuple("data in interval", 8, format_l),
          std::make_tuple("avg rate [B/s]", 8, format_l),
        });
      } else {
        table.SetHeader({
          std::make_tuple("measurement", 0, format_ss),
          std::make_tuple("application", 0, format_s),
          std::make_tuple("intervaldata", 8, format_l),
          std::make_tuple("intervalrate", 8, format_l),
        });
      }

      // IO out bytes
      for (auto it = IostatPeriodsAppIOrb.begin(); it != IostatPeriodsAppIOrb.end();
           ++it) {
        table_data.emplace_back();
        TableRow& row = table_data.back();
        std::string name = (!monitoring ? "out" : "app_io_out");
        row.emplace_back(name, format_ss);
        row.emplace_back(it->first.c_str(), format_s);
        // getting stat sums for 1day (idx=0), 1h (idx=1), 5m (idx=2), 1min (idx=3)
        row.emplace_back(it->second.GetDataInPeriod(time_interval, time_ago, now),
                         format_ll);
        row.emplace_back(it->second.GetDataInPeriod(time_interval, time_ago,
                         now) / (float)time_interval, format_ll);
      }

      // IO in bytes
      for (auto it = IostatPeriodsAppIOwb.begin(); it != IostatPeriodsAppIOwb.end();
           ++it) {
        table_data.emplace_back();
        TableRow& row = table_data.back();
        std::string name = (!monitoring ? "in" : "app_io_in");
        row.emplace_back(name, format_ss);
        row.emplace_back(it->first.c_str(), format_s);
        // getting stat sums for 1day (idx=0), 1h (idx=1), 5m (idx=2), 1min (idx=3)
        row.emplace_back(it->second.GetDataInPeriod(time_interval, time_ago, now),
                         format_ll);
        row.emplace_back(it->second.GetDataInPeriod(time_interval, time_ago,
                         now) / (float)time_interval, format_ll);
      }

      table.AddRows(table_data);
      out += table.GenerateTable(HEADER).c_str();
      table_data.clear();
    } else {
      XrdOucString marker_b =
        "\n┏━> Sum of bytes transferred in last 1m/5m/1h/24h and total sum: \n";
      //! User statistic
      TableFormatterBase table_app_b;

      if (!monitoring) {
        table_app_b.SetHeader({
          std::make_tuple("io", 5, format_ss),
          std::make_tuple("application", 24, format_s),
          std::make_tuple("1min", 8, format_l),
          std::make_tuple("5min", 8, format_l),
          std::make_tuple("1h", 8, format_l),
          std::make_tuple("24h", 8, format_l),
          std::make_tuple("sum", 8, format_l),
        });
      } else {
        table_app_b.SetHeader({
          std::make_tuple("measurement", 0, format_ss),
          std::make_tuple("application", 0, format_s),
          std::make_tuple("60s", 0, format_l),
          std::make_tuple("300s", 0, format_l),
          std::make_tuple("3600s", 0, format_l),
          std::make_tuple("86400s", 0, format_l),
          std::make_tuple("total", 0, format_l),
        });
      }

      XrdOucString marker_sec =
        "\n┏━> Transfer (tf) sample info every 5 min: tf time for 90/95/99% of data, max tf and report times, average tf size, tf count.\n";
      TableFormatterBase table_app_sec;

      if (sample_stat) {
        if (!monitoring) {
          table_app_sec.SetHeader({
            std::make_tuple("io", 5, format_ss),
            std::make_tuple("application", 24, format_s),
            std::make_tuple("90% [s]", 8, format_l),
            std::make_tuple("95% [s]", 8, format_l),
            std::make_tuple("99% [s]", 8, format_l),
            std::make_tuple("max [s]", 8, format_l),
            std::make_tuple("max report [s]", 8, format_l),
            std::make_tuple("avg tf size", 8, format_l),
            std::make_tuple("tf #", 8, format_l),
            std::make_tuple("sample end time", 24, format_s),
          });
        } else {
          table_app_sec.SetHeader({
            std::make_tuple("measurement", 0, format_ss),
            std::make_tuple("application", 0, format_s),
            std::make_tuple("tfsecto90p", 0, format_l),
            std::make_tuple("tfsecto95p", 0, format_l),
            std::make_tuple("tfsecto99p", 0, format_l),
            std::make_tuple("maxtransfersec", 0, format_l),
            std::make_tuple("maxreportsec", 0, format_l),
            std::make_tuple("avgtfsize5min", 0, format_l),
            std::make_tuple("tfcount", 0, format_l),
            std::make_tuple("sampletimestamp", 0, format_s)
          });
        }
      }

      // IO out bytes
      for (auto it = IostatPeriodsAppIOrb.begin(); it != IostatPeriodsAppIOrb.end();
           ++it) {
        table_data.emplace_back();
        TableRow& row = table_data.back();
        std::string name = (!monitoring ? "out" : "app_io_out");
        row.emplace_back(name, format_ss);
        row.emplace_back(it->first.c_str(), format_s);
        // getting stat sums for 1day (idx=0), 1h (idx=1), 5m (idx=2), 1min (idx=3)
        row.emplace_back(it->second.GetDataInPeriod(60, 0, now), format_l);
        row.emplace_back(it->second.GetDataInPeriod(300, 0, now), format_l);
        row.emplace_back(it->second.GetDataInPeriod(3600, 0, now), format_l);
        row.emplace_back(it->second.GetDataInPeriod(86400, 0, now), format_l);
        row.emplace_back(it->second.GetTotalSum(), format_l);
      }

      // IO in bytes
      for (auto it = IostatPeriodsAppIOwb.begin(); it != IostatPeriodsAppIOwb.end();
           ++it) {
        table_data.emplace_back();
        TableRow& row = table_data.back();
        std::string name = (!monitoring ? "in" : "app_io_in");
        row.emplace_back(name, format_ss);
        row.emplace_back(it->first.c_str(), format_s);
        // getting stat sums for 1day (idx=0), 1h (idx=1), 5m (idx=2), 1min (idx=3)
        row.emplace_back(it->second.GetDataInPeriod(60, 0, now), format_l);
        row.emplace_back(it->second.GetDataInPeriod(300, 0, now), format_l);
        row.emplace_back(it->second.GetDataInPeriod(3600, 0, now), format_l);
        row.emplace_back(it->second.GetDataInPeriod(86400, 0, now), format_l);
        row.emplace_back(it->second.GetTotalSum(), format_l);
      }

      out += !monitoring ? marker_b : "";
      table_app_b.AddRows(table_data);
      out += table_app_b.GenerateTable(HEADER).c_str();
      table_data.clear();

      if (sample_stat) {
        // IO out bytes
        for (auto it = IostatPeriodsAppIOrb.begin(); it != IostatPeriodsAppIOrb.end();
             ++it) {
          table_data.emplace_back();
          TableRow& row = table_data.back();
          std::string name = (!monitoring ? "out" : "app_io_out");
          std::string sample_time = "";

          if (!monitoring) {
            sample_time = it->second.GetLastSampleUpdateTimestamp(true);
          } else {
            sample_time = it->second.GetLastSampleUpdateTimestamp(false);
          }

          row.emplace_back(name, format_ss);
          row.emplace_back(it->first.c_str(), format_s);
          // getting stat sums for 1day (idx=0), 1h (idx=1), 5m (idx=2), 1min (idx=3)
          row.emplace_back(it->second.GetTimeToPercComplete(P90), format_l);
          row.emplace_back(it->second.GetTimeToPercComplete(P95), format_l);
          row.emplace_back(it->second.GetTimeToPercComplete(P99), format_l);
          row.emplace_back(it->second.GetLongestTransferTime(), format_l);
          row.emplace_back(it->second.GetLongestReportTime(), format_l);
          row.emplace_back(it->second.GetAvgTransferSize(), format_l);
          row.emplace_back(it->second.GetTfCountInSample(), format_l);
          row.emplace_back(sample_time, format_s);
        }

        // IO in bytes
        for (auto it = IostatPeriodsAppIOwb.begin(); it != IostatPeriodsAppIOwb.end();
             ++it) {
          table_data.emplace_back();
          TableRow& row = table_data.back();
          std::string sample_time = "";

          if (!monitoring) {
            sample_time = it->second.GetLastSampleUpdateTimestamp(true);
          } else {
            sample_time = it->second.GetLastSampleUpdateTimestamp(false);
          }

          std::string name = (!monitoring ? "in" : "app_io_in");
          row.emplace_back(name, format_ss);
          row.emplace_back(it->first.c_str(), format_s);
          // getting stat sums for 1day (idx=0), 1h (idx=1), 5m (idx=2), 1min (idx=3)
          row.emplace_back(it->second.GetTimeToPercComplete(P90), format_l);
          row.emplace_back(it->second.GetTimeToPercComplete(P95), format_l);
          row.emplace_back(it->second.GetTimeToPercComplete(P99), format_l);
          row.emplace_back(it->second.GetLongestTransferTime(), format_l);
          row.emplace_back(it->second.GetLongestReportTime(), format_l);
          row.emplace_back(it->second.GetAvgTransferSize(), format_l);
          row.emplace_back(it->second.GetTfCountInSample(), format_l);
          row.emplace_back(sample_time, format_s);
        }

        out += !monitoring ? marker_sec : "";
        table_app_sec.AddRows(table_data);
        out += table_app_sec.GenerateTable(HEADER).c_str();
        table_data.clear();
      }
    }
  }
}

//------------------------------------------------------------------------------
// Compute and print out the namespace popularity ranking
//------------------------------------------------------------------------------
void
Iostat::PrintNsPopularity(XrdOucString& out, XrdOucString option) const
{
  size_t limit = 10;
  size_t popularitybin = (((time(NULL))) % (IOSTAT_POPULARITY_DAY *
                          IOSTAT_POPULARITY_HISTORY_DAYS)) / IOSTAT_POPULARITY_DAY;
  size_t days = 1;
  time_t tmarker = time(NULL) / IOSTAT_POPULARITY_DAY * IOSTAT_POPULARITY_DAY;
  bool monitoring = false;
  bool bycount = false;
  bool bybytes = false;
  bool hotfiles = false;

  if ((option.find("-m")) != STR_NPOS) {
    monitoring = true;
  }

  if ((option.find("-a")) != STR_NPOS) {
    limit = 999999999;
  }

  if ((option.find("-100")) != STR_NPOS) {
    limit = 100;
  }

  if ((option.find("-1000")) != STR_NPOS) {
    limit = 1000;
  }

  if ((option.find("-10000")) != STR_NPOS) {
    limit = 10000;
  }

  if ((option.find("-n") != STR_NPOS)) {
    bycount = true;
  }

  if ((option.find("-b") != STR_NPOS)) {
    bybytes = true;
  }

  if ((option.find("-w") != STR_NPOS)) {
    days = IOSTAT_POPULARITY_HISTORY_DAYS;
  }

  if (!(bycount || bybytes)) {
    bybytes = bycount = true;
  }

  if ((option.find("-f") != STR_NPOS)) {
    hotfiles = true;
  }

  std::string format_s = !monitoring ? "s" : "os";
  std::string format_ss = !monitoring ? "-s" : "os";
  std::string format_l = !monitoring ? "l" : "ol";
  std::string format_ll = !monitoring ? "-l." : "ol";
  std::string format_lll = !monitoring ? "+l" : "ol";
  std::string unit = !monitoring ? "B" : "";

  // The 'hotfiles' are the files with highest number of present file opens
  if (hotfiles) {
    eos::common::RWMutexReadLock rLock(FsView::gFsView.ViewMutex);
    // print the hotfiles report
    std::set<eos::common::FileSystem::fsid_t>::const_iterator it;
    std::vector<std::string> r_open_vector;
    std::vector<std::string> w_open_vector;
    std::string key;
    std::string val;
    TableFormatterBase table;
    TableData table_data;

    if (!monitoring) {
      table.SetHeader({
        std::make_tuple("type", 5, format_ss),
        std::make_tuple("heat", 5, format_s),
        std::make_tuple("fs", 5, format_s),
        std::make_tuple("host", 24, format_s),
        std::make_tuple("path", 24, format_ss)
      });
    } else {
      table.SetHeader({
        std::make_tuple("measurement", 0, format_ss),
        std::make_tuple("access", 0, format_s),
        std::make_tuple("heat", 0, format_s),
        std::make_tuple("fsid", 0, format_l),
        std::make_tuple("path", 0, format_ss),
        std::make_tuple("fxid", 0, format_s)
      });
    }

    for (auto it = FsView::gFsView.mIdView.begin();
         it != FsView::gFsView.mIdView.end(); it++) {
      r_open_vector.clear();
      w_open_vector.clear();
      FileSystem* fs = it->second;

      if (!fs) {
        continue;
      }

      std::string r_open_hotfiles = fs->GetString("stat.ropen.hotfiles");
      std::string w_open_hotfiles = fs->GetString("stat.wopen.hotfiles");
      std::string node_queue = fs->GetString("queue");
      auto it_node = FsView::gFsView.mNodeView.find(node_queue);

      if (it_node == FsView::gFsView.mNodeView.end()) {
        continue;
      }

      // Check if the corresponding node has a heartbeat
      bool hasHeartbeat = it_node->second->HasHeartbeat();

      // we only show the reports from the last minute, there could be pending values
      if (!hasHeartbeat) {
        r_open_hotfiles = "";
        w_open_hotfiles = "";
      }

      if (r_open_hotfiles == " ") {
        r_open_hotfiles = "";
      }

      if (w_open_hotfiles == " ") {
        w_open_hotfiles = "";
      }

      eos::common::StringConversion::Tokenize(r_open_hotfiles, r_open_vector);
      eos::common::StringConversion::Tokenize(w_open_hotfiles, w_open_vector);
      std::string host = fs->GetString("host");
      std::string path;
      std::string id = fs->GetString("id");
      std::vector<std::tuple<std::string, std::string, std::string,
          std::string, std::string>> data;
      std::vector<std::tuple<std::string, std::string, std::string,
          long long unsigned, std::string, std::string>> data_monitoring;

      // Get information for read
      for (size_t i = 0; i < r_open_vector.size(); i++) {
        eos::common::StringConversion::SplitKeyValue(r_open_vector[i], key, val);
        int rank = 0;

        if (key.c_str()) {
          rank = atoi(key.c_str());
        }

        {
          unsigned long long fid = eos::common::FileId::Hex2Fid(val.c_str());
          eos::Prefetcher::prefetchFileMDWithParentsAndWait(gOFS->eosView, fid);

          try {
            auto fmdLock = gOFS->eosFileService->getFileMDReadLocked(fid);
            path = gOFS->eosView->getUri(fmdLock->getUnderlyingPtr().get());
          } catch (eos::MDException& e) {
            path = "<undef>";
          }
        }

        if (rank > 1) {
          data.emplace_back(std::make_tuple(
                              "read", key.c_str(), id.c_str(), host.c_str(), path.c_str()));
        }

        data_monitoring.emplace_back(std::make_tuple(
                                       "hotfile", "read", key.c_str(), it->first, path.c_str(), val.c_str()));
      }

      // Get information for write
      for (size_t i = 0; i < w_open_vector.size(); i++) {
        eos::common::StringConversion::SplitKeyValue(w_open_vector[i], key, val);
        int rank = 0;

        if (key.c_str()) {
          rank = atoi(key.c_str());
        }

        {
          unsigned long long fid = eos::common::FileId::Hex2Fid(val.c_str());
          eos::Prefetcher::prefetchFileMDWithParentsAndWait(gOFS->eosView, fid);

          try {
            auto fmdLock = gOFS->eosFileService->getFileMDReadLocked(fid);
            path = gOFS->eosView->getUri(fmdLock->getUnderlyingPtr().get());
          } catch (eos::MDException& e) {
            path = "<undef>";
          }
        }

        if (rank > 1) {
          data.emplace_back(std::make_tuple(
                              "write", key.c_str(), id.c_str(), host.c_str(), path.c_str()));
        }

        data_monitoring.emplace_back(std::make_tuple(
                                       "hotfile", "write", key.c_str(), it->first, path.c_str(), val.c_str()));
      }

      // Sort and output
      if (!monitoring) {
        std::sort(data.begin(), data.end());

        for (auto it : data) {
          table_data.emplace_back();
          TableRow& row = table_data.back();
          row.emplace_back(std::get<0>(it), format_ss);
          row.emplace_back(std::get<1>(it), format_s);
          row.emplace_back(std::get<2>(it), format_s);
          row.emplace_back(std::get<3>(it), format_s);
          row.emplace_back(std::get<4>(it), format_ss);
        }
      } else {
        std::sort(data_monitoring.begin(), data_monitoring.end());

        for (auto mdata : data_monitoring) {
          table_data.emplace_back();
          TableRow& row = table_data.back();
          row.emplace_back(std::get<0>(mdata), format_ss);
          row.emplace_back(std::get<1>(mdata), format_s);
          row.emplace_back(std::get<2>(mdata), format_s);
          row.emplace_back(std::get<3>(mdata), format_l);
          row.emplace_back(std::get<4>(mdata), format_ss);
          row.emplace_back(std::get<5>(mdata), format_s);
        }
      }
    }

    table.AddRows(table_data);
    out += table.GenerateTable(HEADER).c_str();
    return;
  }

  //! Namespace IO ranking (popularity)
  for (size_t pbin = 0; pbin < days; pbin++) {
    std::unique_lock<std::mutex> scope_lock(mPopularityMutex);
    size_t sbin = (IOSTAT_POPULARITY_HISTORY_DAYS + popularitybin - pbin) %
                  IOSTAT_POPULARITY_HISTORY_DAYS;
    google::sparse_hash_map<std::string, struct Popularity>::const_iterator it;
    std::vector<popularity_t> popularity_nread(IostatPopularity[sbin].begin(),
        IostatPopularity[sbin].end());
    std::vector<popularity_t> popularity_rb(IostatPopularity[sbin].begin(),
                                            IostatPopularity[sbin].end());
    // sort them (backwards) by rb or nread
    std::sort(popularity_nread.begin(), popularity_nread.end(),
              PopularityCmp_nread());
    std::sort(popularity_rb.begin(), popularity_rb.end(), PopularityCmp_rb());
    XrdOucString marker = "\n┏━> Today\n";

    switch (pbin) {
    case 1:
      marker = "\n┏━> Yesterday\n";
      break;

    case 2:
      marker = "\n┏━> 2 days ago\n";
      break;

    case 3:
      marker = "\n┏━> 3 days ago\n";
      break;

    case 4:
      marker = "\n┏━> 4 days ago\n";
      break;

    case 5:
      marker = "\n┏━> 5 days ago\n";
      break;

    case 6:
      marker = "\n┏━> 6 days ago\n";
    }

    if (bycount) {
      TableFormatterBase table;
      TableData table_data;

      if (!monitoring) {
        table.SetHeader({
          std::make_tuple("rank", 5, format_ll),
          std::make_tuple("by(read count)", 12, format_s),
          std::make_tuple("read bytes", 10, format_lll),
          std::make_tuple("path", 24, format_ss),
        });
      } else {
        table.SetHeader({
          std::make_tuple("measurement", 0, format_ss),
          std::make_tuple("time", 0, format_lll),
          std::make_tuple("rank", 0, format_ll),
          std::make_tuple("nread", 0, format_lll),
          std::make_tuple("rb", 0, format_lll),
          std::make_tuple("path", 0, format_ss)
        });
      }

      size_t cnt = 0;

      for (auto it : popularity_nread) {
        cnt++;

        if (cnt > limit) {
          break;
        }

        table_data.emplace_back();
        TableRow& row = table_data.back();

        if (monitoring) {
          row.emplace_back("popularitybyaccess", format_ss);
          row.emplace_back((unsigned) tmarker, format_lll);
        }

        row.emplace_back((int) cnt, format_ll);
        row.emplace_back(it.second.nread, format_lll);
        row.emplace_back(it.second.rb, format_lll, unit);
        row.emplace_back(it.first.c_str(), format_s);
      }

      if (cnt > 0) {
        out += !monitoring ? marker : "";
        table.AddRows(table_data);
        out += table.GenerateTable(HEADER).c_str();
      }
    }

    if (bybytes) {
      TableFormatterBase table;
      TableData table_data;

      if (!monitoring) {
        table.SetHeader({
          std::make_tuple("rank", 5, format_ll),
          std::make_tuple("by(read bytes)", 12, format_s),
          std::make_tuple("read count", 10, format_lll),
          std::make_tuple("path", 24, format_ss),
        });
      } else {
        table.SetHeader({
          std::make_tuple("measurement", 0, format_ss),
          std::make_tuple("time", 0, format_lll),
          std::make_tuple("rank", 0, format_ll),
          std::make_tuple("nread", 0, format_lll),
          std::make_tuple("rb", 0, format_lll),
          std::make_tuple("path", 0, format_ss)
        });
      }

      size_t cnt = 0;

      for (auto it : popularity_rb) {
        cnt++;

        if (cnt > limit) {
          break;
        }

        table_data.emplace_back();
        TableRow& row = table_data.back();

        if (monitoring) {
          row.emplace_back("popularitybyvolume", format_ss);
          row.emplace_back((unsigned) tmarker, format_lll);
        }

        row.emplace_back((int) cnt, format_ll);

        if (!monitoring) {
          row.emplace_back(it.second.rb, format_lll, unit);
          row.emplace_back(it.second.nread, format_lll);
        } else {
          row.emplace_back(it.second.nread, format_lll);
          row.emplace_back(it.second.rb, format_lll, unit);
        }

        row.emplace_back(it.first.c_str(), format_s);
      }

      table.AddRows(table_data);
      out += table.GenerateTable(HEADER2).c_str();
    }
  }
}

//------------------------------------------------------------------------------
// Print namespace activity report for given path
//------------------------------------------------------------------------------
void
Iostat::PrintNsReport(const char* path, XrdOucString& out) const
{
  XrdOucString reportFile;
  reportFile = gOFS->IoReportStorePath.c_str();
  reportFile += "/";
  reportFile += path;
  std::ifstream inFile(reportFile.c_str());
  std::string reportLine;
  unsigned long long totalreadbytes = 0;
  unsigned long long totalwritebytes = 0;
  double totalreadtime = 0;
  double totalwritetime = 0;
  unsigned long long rcount = 0;
  unsigned long long wcount = 0;

  while (std::getline(inFile, reportLine)) {
    XrdOucEnv ioreport(reportLine.c_str());
    auto report = std::make_unique<eos::common::Report>(ioreport);
    report->Dump(out);

    if (!report->wb) {
      rcount++;
      totalreadtime += ((report->cts - report->ots) + (1.0 * (report->ctms -
                        report->otms) / 1000000));
      totalreadbytes += report->rb;
    } else {
      wcount++;
      totalwritetime += ((report->cts - report->ots) + (1.0 *
                         (report->ctms - report->otms) / 1000000));
      totalwritebytes += report->wb;
    }
  }

  out += "----------------------- SUMMARY -------------------\n";
  char summaryline[4096];
  XrdOucString sizestring1, sizestring2;
  snprintf(summaryline, sizeof(summaryline) - 1,
           "| avg. readd: %.02f MB/s | avg. write: %.02f  MB/s | "
           "total read: %s | total write: %s | times read: %llu | "
           "times written: %llu |\n",
           totalreadtime ? (totalreadbytes / totalreadtime / 1000000.0) : 0,
           totalwritetime ? (totalwritebytes / totalwritetime / 1000000.0) : 0,
           eos::common::StringConversion::GetReadableSizeString
           (sizestring1, totalreadbytes, "B"),
           eos::common::StringConversion::GetReadableSizeString
           (sizestring2, totalwritebytes, "B"), (unsigned long long)rcount,
           (unsigned long long)wcount);
  out += summaryline;
}

//------------------------------------------------------------------------------
// Circulate the entries to get stats collected over last sec, min, hour and day
//------------------------------------------------------------------------------
void
Iostat::Circulate(ThreadAssistant& assistant) noexcept
{
  while (!assistant.terminationRequested()) {
    if (mLegacyMode) {
      static unsigned long long sc = 0ull;

      // Store once per minute the current statistics
      if (sc % 117 == 0) {
        sc = 0ull;

        // save the current state ~ every minute
        if (!LegacyStoreInFile()) {
          eos_static_err("msg=\"failed store io stat dump\" path=\"%s\"",
                         mLegacyFilePath.c_str());
        }
      }

      sc++;
    }

    assistant.wait_for(std::chrono::milliseconds(512));
    google::sparse_hash_map<std::string, google::sparse_hash_map<uid_t, IostatPeriods> >::iterator
    tit;
    google::sparse_hash_map<std::string, IostatPeriods >::iterator dit;
    time_t now = time(NULL);
    std::unique_lock<std::mutex> scope_lock(mDataMutex);

    // loop over tags
    for (tit = IostatPeriodsUid.begin(); tit != IostatPeriodsUid.end(); ++tit) {
      // loop over vids
      google::sparse_hash_map<uid_t, IostatPeriods>::iterator it;

      for (it = tit->second.begin(); it != tit->second.end(); ++it) {
        it->second.StampBufferZero(now);
      }
    }

    for (tit = IostatPeriodsGid.begin(); tit != IostatPeriodsGid.end(); ++tit) {
      // loop over vids
      google::sparse_hash_map<uid_t, IostatPeriods>::iterator it;

      for (it = tit->second.begin(); it != tit->second.end(); ++it) {
        it->second.StampBufferZero(now);
      }
    }

    // loop over domain accounting
    for (dit = IostatPeriodsDomainIOrb.begin();
         dit != IostatPeriodsDomainIOrb.end();
         dit++) {
      dit->second.StampBufferZero(now);
    }

    for (dit = IostatPeriodsDomainIOwb.begin();
         dit != IostatPeriodsDomainIOwb.end();
         dit++) {
      dit->second.StampBufferZero(now);
    }

    // loop over app accounting
    for (dit = IostatPeriodsAppIOrb.begin(); dit != IostatPeriodsAppIOrb.end();
         dit++) {
      dit->second.StampBufferZero(now);
    }

    for (dit = IostatPeriodsAppIOwb.begin(); dit != IostatPeriodsAppIOwb.end();
         dit++) {
      dit->second.StampBufferZero(now);
    }

    size_t popularitybin = (((time(NULL))) % (IOSTAT_POPULARITY_DAY *
                            IOSTAT_POPULARITY_HISTORY_DAYS)) / IOSTAT_POPULARITY_DAY;

    if (mLastPopularityBin != popularitybin) {
      // only if we enter a new bin we erase it
      std::unique_lock<std::mutex> scope_lock(mPopularityMutex);
      IostatPopularity[popularitybin].clear();
      IostatPopularity[popularitybin].resize(10000);
      mLastPopularityBin = popularitybin;
    }
  }

  eos_static_info("%s", "msg=\"stopping iostat circulate thread\"");
}

//------------------------------------------------------------------------------
// Encode the UDP popularity targets to a string using the provided separator
//------------------------------------------------------------------------------
std::string
Iostat::EncodeUdpPopularityTargets() const
{
  std::string out;
  std::unique_lock<std::mutex> scope_lock(mBcastMutex);

  if (mUdpPopularityTarget.empty()) {
    return out;
  }

  for (const auto& elem : mUdpPopularityTarget) {
    out += elem;
    out += "|";
  }

  out.pop_back();
  return out;
}

//------------------------------------------------------------------------------
// Add UDP target
//------------------------------------------------------------------------------
bool
Iostat::AddUdpTarget(const std::string& target, bool store_and_lock)
{
  std::unique_lock<std::mutex> scope_lock(mBcastMutex, std::defer_lock);

  if (store_and_lock) {
    scope_lock.lock();
  }

  if (mUdpPopularityTarget.insert(target).second == false) {
    // Target already exists
    return false;
  }

  // Create an UDP socket for the specified target
  int udpsocket = -1;
  udpsocket = socket(AF_INET, SOCK_DGRAM, 0);

  if (udpsocket >= 0) {
    XrdOucString a_host, a_port, hp;
    int port = 0;
    hp = target.c_str();

    if (!eos::common::StringConversion::SplitKeyValue(hp, a_host, a_port)) {
      a_host = hp;
      a_port = "31000";
    }

    port = atoi(a_port.c_str());
    mUdpSocket[target] = udpsocket;
    XrdNetAddr* addrs  = 0;
    int         nAddrs = 0;
    const char* err    = XrdNetUtils::GetAddrs(a_host.c_str(), &addrs, nAddrs,
                         XrdNetUtils::allIPv64,
                         XrdNetUtils::NoPortRaw);

    if (err || nAddrs == 0) {
      return false;
    }

    memcpy((struct sockaddr*) &mUdpSockAddr[target], addrs[0].SockAddr(),
           sizeof(sockaddr));
    delete [] addrs;
    mUdpSockAddr[target].sin_family = AF_INET;
    mUdpSockAddr[target].sin_port = htons(port);
  }

  // Store configuration if required
  if (store_and_lock) {
    scope_lock.unlock();
    return StoreIostatConfig(&FsView::gFsView);
  }

  return true;
}

//------------------------------------------------------------------------------
// Remove UDP target
//------------------------------------------------------------------------------
bool
Iostat::RemoveUdpTarget(const std::string& target)
{
  bool store = false;
  bool retc = false;
  {
    std::unique_lock<std::mutex> scop_lock(mBcastMutex);

    if (mUdpPopularityTarget.count(target)) {
      mUdpPopularityTarget.erase(target);

      if (mUdpSocket.count(target)) {
        if (mUdpSocket[target] > 0) {
          // close the UDP socket
          close(mUdpSocket[target]);
        }

        mUdpSocket.erase(target);
        mUdpSockAddr.erase(target);
      }

      retc = true;
      store = true;
    }
  }

  if (store) {
    retc &= StoreIostatConfig(&FsView::gFsView);
  }

  return retc;
}

//------------------------------------------------------------------------------
// Do the UDP broadcast
//------------------------------------------------------------------------------
void
Iostat::UdpBroadCast(eos::common::Report* report) const
{
  std::string u = "";
  char fs[1024];
  std::unique_lock<std::mutex> scope_lock(mBcastMutex);

  for (auto it = mUdpPopularityTarget.cbegin();
       it != mUdpPopularityTarget.cend(); ++it) {
    u = "";
    XrdOucString tg = it->c_str();
    XrdOucString sizestring;

    if (tg.endswith("/json")) {
      // do json format broadcast
      tg.replace("/json", "");
      u += "{\"app_info\": \"";
      u += report->sec_app;
      u += "\",\n";
      u += " \"client_domain\": \"";
      u += report->sec_domain;
      u += "\",\n";
      u += " \"client_host\": \"";
      u += report->sec_host;
      u += "\",\n";
      u += " \"end_time\": ";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->cts);
      u += ",\n";
      u += " \"file_lfn\": \"";
      u += report->path;
      u += "\",\n";
      u += " \"file_size\": ";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->csize);
      u += ",\n";
      u += " \"read_average\": ";
      u += eos::common::StringConversion::GetSizeString(sizestring,
           report->rb / ((report->nrc) ? report->nrc : 999999999));
      u += ",\n";
      u += " \"read_bytes_at_close\": ";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->rb);
      u += ",\n";
      u += " \"read_bytes\": ";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->rb);
      u += ",\n";
      u += " \"read_max\": ";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->rb_max);
      u += ",\n";
      u += " \"read_min\": ";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->rb_min);
      u += ",\n";
      u += " \"read_operations\": ";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->nrc);
      u += ",\n";
      snprintf(fs, sizeof(fs) - 1, "%.02f", report->rb_sigma);
      u += " \"read_sigma\": ";
      u += fs;
      u += ",\n";
      /* -- we have currently no access to this information */
      /*
      u += " \"read_single_average\": ";  u += "0"; u += ",\n";
      u += " \"read_single_bytes\": ";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->rb); u += ",\n";
      u += " \"read_single_max\": ";      u += "0"; u += ",\n";
      u += " \"read_single_min\": ";      u += "0"; u += ",\n";
      u += " \"read_single_operations\": ";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->nrc); u += ",\n";
      u += " \"read_single_sigma\": ";    u += "0"; u += ",\n";
      u += " \"read_vector_average\": ";  u += "0"; u += ",\n";
      u += " \"read_vector_bytes\": ";    u += "0"; u += ",\n";
      u += " \"read_vector_count_average\": "; u += "0"; u += ",\n";
      u += " \"read_vector_count_max\": ";u += "0"; u += ",\n";
      u += " \"read_vector_count_min\": ";u += "0"; u += ",\n";
      u += " \"read_vector_count_sigma\": ";   u += "0"; u += ",\n";
      u += " \"read_vector_max\": ";      u += "0"; u += ",\n";
      u += " \"read_vector_min\": ";      u += "0"; u += ",\n";
      u += " \"read_vector_operations\": "; u += "0"; u += ",\n";
      u += " \"read_vector_sigma\": ";    u += "0"; u += ",\n"; */
      u += " \"server_domain\": \"";
      u += report->server_domain;
      u += "\",\n";
      u += " \"server_host\": \"";
      u += report->server_name;
      u += "\",\n";
      u += " \"server_username\": \"";
      u += report->sec_name;
      u += "\",\n";
      u += " \"start_time\": ";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->ots);
      u += ",\n";
      XrdOucString stime; // stores the current time in <s>.<ns>
      u += " \"unique_id\": \"";
      u += gOFS->MgmOfsInstanceName.c_str();
      u += "-";
      u += eos::common::StringConversion::TimeNowAsString(stime);
      u += "\",\n";
      u += " \"user_dn\": \"";
      u += report->sec_info;
      u += "\",\n";
      u += " \"user_fqan\": \"";
      u += report->sec_grps;
      u += "\",\n";
      u += " \"user_role\": \"";
      u += report->sec_role;
      u += "\",\n";
      u += " \"user_vo\": \"";
      u += report->sec_vorg;
      u += "\",\n";
      u += " \"write_average\": ";
      u += eos::common::StringConversion::GetSizeString(sizestring,
           report->wb / ((report->nwc) ? report->nwc : 999999999));
      u += ",\n";
      u += " \"write_bytes_at_close\": ";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->wb);
      u += ",\n";
      u += " \"write_bytes\": ";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->wb);
      u += ",\n";
      u += " \"write_max\": ";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->wb_max);
      u += ",\n";
      u += " \"write_min\": ";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->wb_min);
      u += ",\n";
      u += " \"write_operations\": ";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->nwc);
      u += ",\n";
      snprintf(fs, sizeof(fs) - 1, "%.02f", report->wb_sigma);
      u += " \"write_sigma\": ";
      u += fs;
      u += "}\n";
    } else {
      // do default format broadcast
      u += "#begin\n";
      u += "app_info=";
      u += report->sec_app;
      u += "\n";
      u += "client_domain=";
      u += report->sec_domain;
      u += "\n";
      u += "client_host=";
      u += report->sec_host;
      u += "\n";
      u += "end_time=";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->cts);
      u += "\n";
      u += "file_lfn = ";
      u += report->path;
      u += "\n";
      u += "file_size = ";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->csize);
      u += "\n";
      u += "read_average=";
      u += eos::common::StringConversion::GetSizeString(sizestring,
           report->rb / ((report->nrc) ? report->nrc : 999999999));
      u += "\n";
      u += "read_bytes_at_close=";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->rb);
      u += "\n";
      u += "read_bytes=";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->rb);
      u += "\n";
      u += "read_min=";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->rb_min);
      u += "\n";
      u += "read_max=";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->rb_max);
      u += "\n";
      u += "read_operations=";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->nrc);
      u += "\n";
      u += "read_sigma=";
      u += "0";
      u += "\n";
      snprintf(fs, sizeof(fs) - 1, "%.02f", report->rb_sigma);
      u += "read_sigma=";
      u += fs;
      u += "\n";
      /* -- we have currently no access to this information */
      /* u += "read_single_average=";  u += "0"; u += "\n";
      u += "read_single_bytes=";    u += eos::common::StringConversion::GetSizeString(sizestring, report->rb); u += "\n";
      u += "read_single_max=";      u += "0"; u += "\n";
      u += "read_single_min=";      u += "0"; u += "\n";
      u += "read_single_operations=";    u += eos::common::StringConversion::GetSizeString(sizestring, report->nrc); u += "\n";
      u += "read_single_sigma=";    u += "0"; u += "\n";
      u += "read_vector_average=";  u += "0"; u += "\n";
      u += "read_vector_bytes=";    u += "0"; u += "\n";
      u += "read_vector_count_average="; u += "0"; u += "\n";
      u += "read_vector_count_max=";u += "0"; u += "\n";
      u += "read_vector_count_min=";u += "0"; u += "\n";
      u += "read_vector_count_sigma=";   u += "0"; u += "\n";
      u += "read_vector_max=";      u += "0"; u += "\n";
      u += "read_vector_min=";      u += "0"; u += "\n";
      u += "read_vector_operations="; u += "0"; u += "\n";
      u += "read_vector_sigma=";    u += "0"; u += "\n";*/
      u += "server_domain=";
      u += report->server_domain;
      u += "\n";
      u += "server_host=";
      u += report->server_name;
      u += "\n";
      u += "server_username=";
      u += report->sec_name;
      u += "\n";
      u += "start_time=";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->ots);
      u += "\n";
      XrdOucString stime; // stores the current time in <s>.<ns>
      u += "unique_id=";
      u += gOFS->MgmOfsInstanceName.c_str();
      u += "-";
      u += eos::common::StringConversion::TimeNowAsString(stime);
      u += "\n";
      u += "user_dn = ";
      u += report->sec_info;
      u += "\n";
      u += "user_fqan=";
      u += report->sec_grps;
      u += "\n";
      u += "user_role=";
      u += report->sec_role;
      u += "\n";
      u += "user_vo=";
      u += report->sec_vorg;
      u += "\n";
      u += "write_average=";
      u += eos::common::StringConversion::GetSizeString(sizestring,
           report->wb / ((report->nwc) ? report->nwc : 999999999));
      u += "\n";
      u += "write_bytes_at_close=";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->wb);
      u += "\n";
      u += "write_bytes=";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->wb);
      u += "\n";
      u += "write_min=";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->wb_min);
      u += "\n";
      u += "write_max=";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->wb_max);
      u += "\n";
      u += "write_operations=";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->nwc);
      u += "\n";
      snprintf(fs, sizeof(fs) - 1, "%.02f", report->rb_sigma);
      u += "write_sigma=";
      u += fs;
      u += "\n";
      u += "#end\n";
    }

    int sendretc = sendto(mUdpSocket.at(*it), u.c_str(), u.length(), 0,
                          (struct sockaddr*) &mUdpSockAddr.at(*it),
                          sizeof(struct sockaddr_in));

    if (sendretc < 0) {
      eos_static_err("msg=\"failed to send udp message to %s\"", it->c_str());
    }

    eos_static_debug("sendto_retc=%d", sendretc);
  }
}

//------------------------------------------------------------------------------
// Add entry to popularity statistics
//------------------------------------------------------------------------------
void
Iostat::AddToPopularity(const std::string& path, unsigned long long rb,
                        time_t start, time_t stop)
{
  size_t popularitybin = (((start + stop) / 2) % (IOSTAT_POPULARITY_DAY *
                          IOSTAT_POPULARITY_HISTORY_DAYS)) / IOSTAT_POPULARITY_DAY;
  eos::common::Path cPath(path.c_str());
  std::unique_lock<std::mutex> scope_lock(mPopularityMutex);

  for (size_t k = 0; k < cPath.GetSubPathSize(); ++k) {
    std::string sp = cPath.GetSubPath(k);
    IostatPopularity[popularitybin][sp].rb += rb;
    IostatPopularity[popularitybin][sp].nread++;
  }
}

//------------------------------------------------------------------------------
// Create hash map key string from the given information
//------------------------------------------------------------------------------
std::string
Iostat::EncodeKey(const std::string& id_type, const std::string& id_val,
                  const std::string& tag)
{
  return SSTR("idt=" << id_type << "&id=" << id_val << "&tag=" << tag);
}

//------------------------------------------------------------------------------
// Decode/parse hash map key to extract entry information
//------------------------------------------------------------------------------
bool
Iostat::DecodeKey(const std::string& key, std::string& id_type,
                  std::string& id_val, std::string& tag)
{
  std::vector<std::string> tokens {};
  eos::common::StringConversion::Tokenize(key, tokens, "&");

  for (const auto& token : tokens) {
    std::vector<std::string> kv {};
    eos::common::StringConversion::Tokenize(token, kv, "=");

    if (kv.size() != 2) {
      eos_static_err("msg=\"unexpected token format\" token=\"%s\"",
                     token.c_str());
      return false;
    }

    if (kv[0] == "idt") {
      id_type = kv[1];
    } else if (kv[0] == "id") {
      id_val = kv[1];
    } else if (kv[0] == "tag") {
      tag = kv[1];
    } else {
      eos_static_err("msg=\"unexpected key format\" key=\"%s\"", kv[0].c_str());
      return false;
    }
  }

  return true;
}

//------------------------------------------------------------------------------
// Load Iostat information from Qdb backend
//------------------------------------------------------------------------------
bool
Iostat::LoadFromQdb()
{
  std::string key = GetHashKey();
  eos_static_info("msg=\"loading iostat info from Qdb\" hash_map=\"%s\"",
                  key.c_str());
  qclient::redisReplyPtr reply;

  try {
    reply = mQcl->exec("HGETALL", key).get();
  } catch (const std::exception& e) {
    eos_static_err("msg=\"failed getting entries from Qdb\", emsg=\"%s\"",
                   e.what());
    return true;
  }

  qclient::HgetallParser mQdbRespParser(reply);

  if (!mQdbRespParser.ok()) {
    eos_static_err("%s", "msg=\"failed parsing reply from Qdb\n");
    return false;
  }

  int id = 0;
  unsigned long long val = 0ull;
  std::string id_type, id_val, tag;
  std::map<std::string, std::string> stored_iostat = mQdbRespParser.value();
  std::unique_lock<std::mutex> scope_lock(mDataMutex);
  // Clean up the memory data structures
  IostatUid.clear();
  IostatUid.resize(0);
  IostatTag.clear();
  IostatTag.resize(0);
  IostatGid.clear();
  IostatGid.resize(0);

  for (const auto& pair : stored_iostat) {
    if (!DecodeKey(pair.first, id_type, id_val, tag)) {
      continue;
    }

    // Convert entries from string to numeric
    try {
      id = std::stoi(id_val);
      val = std::stoull(pair.second);
    } catch (...) {
      eos_static_err("msg=\"failed converting to numeric format\" key=\"%s\" "
                     "val=\"%s\"", pair.first.c_str(), pair.second.c_str());
      continue;
    }

    if (id_type == USER_ID_TYPE) {
      IostatUid[tag][id] = val;

      if (!IostatTag.count(tag)) {
        IostatTag[tag] = val;
      } else {
        IostatTag[tag] += val;
      }
    } else if (id_type == GROUP_ID_TYPE) {
      IostatGid[tag][id] = val;
    }
  }

  return true;
}

//------------------------------------------------------------------------------
// Store statistics in legacy file format
//------------------------------------------------------------------------------
bool
Iostat::LegacyStoreInFile()
{
  if (mLegacyFilePath.empty()) {
    return false;
  }

  XrdOucString tmpname = mLegacyFilePath.c_str();
  tmpname += ".tmp";
  FILE* fout = fopen(tmpname.c_str(), "w+");

  if (!fout) {
    return false;
  }

  if (chmod(tmpname.c_str(), S_IRWXU | S_IRGRP | S_IROTH)) {
    fclose(fout);
    return false;
  }

  std::unique_lock<std::mutex> scope_lock(mDataMutex);

  // Store user counters
  for (auto tuit = IostatUid.begin(); tuit != IostatUid.end(); tuit++) {
    for (auto it = tuit->second.begin(); it != tuit->second.end(); ++it) {
      fprintf(fout, "tag=%s&uid=%u&val=%llu\n", tuit->first.c_str(), it->first,
              (unsigned long long)it->second);
    }
  }

  // Store group counter
  for (auto tgit = IostatGid.begin(); tgit != IostatGid.end(); tgit++) {
    for (auto it = tgit->second.begin(); it != tgit->second.end(); ++it) {
      fprintf(fout, "tag=%s&gid=%u&val=%llu\n", tgit->first.c_str(), it->first,
              (unsigned long long)it->second);
    }
  }

  fclose(fout);
  return (rename(tmpname.c_str(), mLegacyFilePath.c_str()) == 0);
}

//------------------------------------------------------------------------------
// Restore statistics from legacy file format
//------------------------------------------------------------------------------
bool
Iostat::LegacyRestoreFromFile()
{
  if (mLegacyFilePath.empty()) {
    return false;
  }

  FILE* fin = fopen(mLegacyFilePath.c_str(), "r");

  if (!fin) {
    return true;
  }

  int item = 0;
  char line[16384];
  std::unique_lock<std::mutex> scope_lock(mDataMutex);

  while ((item = fscanf(fin, "%16383s\n", line)) == 1) {
    XrdOucEnv env(line);

    if (env.Get("tag") && env.Get("uid") && env.Get("val")) {
      std::string tag = env.Get("tag");
      uid_t uid = atoi(env.Get("uid"));
      unsigned long long val = strtoull(env.Get("val"), 0, 10);
      IostatUid[tag][uid] = val;

      if (!IostatTag.count(tag)) {
        IostatTag[tag] = val;
      } else {
        IostatTag[tag] += val;
      }
    }

    if (env.Get("tag") && env.Get("gid") && env.Get("val")) {
      std::string tag = env.Get("tag");
      gid_t gid = atoi(env.Get("gid"));
      unsigned long long val = strtoull(env.Get("val"), 0, 10);
      IostatGid[tag][gid] = val;
    }
  }

  fclose(fin);
  return true;
}

EOSMGMNAMESPACE_END
