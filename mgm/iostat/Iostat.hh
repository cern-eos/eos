//------------------------------------------------------------------------------
//! @file Iostat.hh
//! @authors Andreas-Joachim Peters/Jaroslav Guenther - CERN
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

#pragma once
#include "common/AssistedThread.hh"
#include "common/StringConversion.hh"
#include "mgm/fsview/FsView.hh"
#include "mgm/Namespace.hh"
#include "namespace/ns_quarkdb/qclient/include/qclient/QClient.hh"
#include "namespace/ns_quarkdb/qclient/include/qclient/structures/QHash.hh"
#include <arpa/inet.h>
#include <atomic>
#include <google/sparse_hash_map>
#include <netinet/in.h>
#include <set>
#include <string>
#include <sys/socket.h>
#include <sys/types.h>

namespace eos
{
class MetadataFlusher;
namespace common
{
class Report;
}
} // namespace eos

EOSMGMNAMESPACE_BEGIN

//! Define the history in days we want to do popularity tracking
#define IOSTAT_POPULARITY_HISTORY_DAYS 7
#define IOSTAT_POPULARITY_DAY 86400

//! Enumeration class for the 4 periods for which stats are collected
enum class Period {DAY, HOUR, FIVEMIN, ONEMIN};
enum class PercentComplete {p90, p95, p99, p100};

//------------------------------------------------------------------------------
//! Class IostatPeriods holds read/write stats for the past 24h
//------------------------------------------------------------------------------
class IostatPeriods
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  IostatPeriods()
  {
    memset(mDataBuffer, 0, sizeof(mDataBuffer));
    memset(mIntegralBuffer, 0, sizeof(mIntegralBuffer));
  }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~IostatPeriods() = default;

  //----------------------------------------------------------------------------
  //! Add measurement to the various periods it overlaps with
  //!
  //! @param val measured value
  //! @param start start time of the measurement
  //! @param stop stop time of the measurement
  //! @param now current timestamp
  //----------------------------------------------------------------------------
  void Add(unsigned long long val, time_t start, time_t stop,
           time_t now);

  //------------------------------------------------------------------------------
  //! Reset bin content of the buffer w.r.t. given timstamp
  //------------------------------------------------------------------------------
  void StampBufferZero(time_t& now);

  //------------------------------------------------------------------------------
  //! Get the sum of values for the given buffer period
  //------------------------------------------------------------------------------
  unsigned long long GetDataInPeriod(size_t period,
                                     unsigned long long time_offset,
                                     time_t now) const;

  //------------------------------------------------------------------------------
  //! Get longest transfer time in past 24h
  //------------------------------------------------------------------------------
  inline unsigned long long GetLongestTransferTime() const
  {
    return mLongestTransferTimeInSample;
  }

  //------------------------------------------------------------------------------
  //! Get longest transfer report time (time it took to FST report to arrive at
  //! MGM) in past 24h
  //------------------------------------------------------------------------------
  unsigned long long GetLongestReportTime() const
  {
    return mLongestReportTimeInSample;
  }

  //----------------------------------------------------------------------------
  //! Return time to completion of transfer of 90/95/99/100% of data for
  //! transfers seen during sample time [mLastTfSampleUpdateInterval]
  //------------------------------------------------------------------------------
  inline unsigned long long GetTimeToPercComplete(PercentComplete perc) const
  {
    return (unsigned long long)mDurationToPercComplete[(int)perc];
  }

  //------------------------------------------------------------------------------
  //! Return average transfer size seen during sample time
  //! [mLastTfSampleUpdateInterval]
  //------------------------------------------------------------------------------
  inline unsigned long long GetAvgTransferSize() const
  {
    return mAvgTfSize;
  }

  //------------------------------------------------------------------------------
  //! Return number of transfers seen during sample time
  //! [mLastTfSampleUpdateInterval]
  //------------------------------------------------------------------------------
  inline unsigned long long GetTfCountInSample() const
  {
    return mTfCountInSample;
  }

  //------------------------------------------------------------------------------
  //! Return total IostatPeriod sum
  //------------------------------------------------------------------------------
  inline unsigned long long GetTotalSum() const
  {
    return mTotal;
  }

  //------------------------------------------------------------------------------
  //! Getting the timestamp of the last time the transfer sample was taken
  //------------------------------------------------------------------------------
  std::string GetLastSampleUpdateTimestamp(bool date_format = false) const;

private:
#ifdef IN_TEST_HARNESS
public:
#endif
  unsigned long long mTotal = 0ull;
  // If sBinWidth !=1 please beware of the trannsfer start and stop bins getting
  // the right transfer volume and add code block currently commented out starting
  // from line 199
  static constexpr size_t sBinWidth = 1;
  static constexpr int sBins = 86400;
  //! Number of seconds the sBins correspond to
  static constexpr int sPeriod = sBins * sBinWidth;
  time_t mLastAddTime = 0;
  time_t mLastStampZeroTime = 0;
  // even if you wait for longest transfer time - you still do not know if the longest
  // How much data was transferred during ibin = mDataBuffer[ibin]
  double mDataBuffer[sBins];
  // what we can measure is choosing a period of time [sLastTfMaxLenUpdateRate] `
  // for collecting newly finished transfers, distribute these tf into bins,
  // --> calculate bin/sumall per bin --> integrate bins until reaching
  // e.g. 99% of the data transferred --> the number of bins give us duration it too to get all data
  // through the network in the last e.g. 5 min [sLastTfMaxLenUpdateRate] `
  const double mPercComplete[4] {0.90, 0.95, 0.99, 1.0};
  double mIntegralBuffer[sBins];
  // Udate rate every 5 minutes
  const time_t mLastTfSampleUpdateInterval = 300;
  time_t mLastTfMaxLenUpdateTime = 0;
  // Average transfer size in last 5 min [sLastTfMaxLenUpdateRate]
  unsigned long long mAvgTfSize = 0;
  unsigned long long mDurationToPercComplete[4] {0, 0, 0, 0};
// Transfer count
  unsigned long long mTfCount = 0;
  // Transfer length is not longer because there is longer transfer in the pipe !
  unsigned long long mLongestTransferTime = 0;
  // Monitor how long it took to the transfer report to get to the MGM
  unsigned long long mLongestReportTime = 0;
  // The next 3 variables mean the same as the last 3 above, but these are
  // to be exposed to the user, evaluated every [mLastTfSampleUpdateInterval]
  unsigned long long mTfCountInSample = 0;
  unsigned long long mLongestTransferTimeInSample = 0;
  unsigned long long mLongestReportTimeInSample = 0;

  //------------------------------------------------------------------------------
  //! Update Transfer Buffer to iterate over and calculate how long does it take
  //! to transfer [mPercComplete] % of the data
  //!
  //! @param now current timestamp
  //------------------------------------------------------------------------------
  void UpdateTransferSampleInfo(time_t now);

};

//------------------------------------------------------------------------------
//! Iostat subscribes to MQ, collects and digests report messages
//------------------------------------------------------------------------------
class Iostat: public eos::common::LogId
{
public:
  //! Configuration keys used in config key-val store
  static const char* gIostatCollect;
  static const char* gIostatReportSave;
  static const char* gIostatReportNamespace;
  static const char* gIostatPopularity;
  static const char* gIostatUdpTargetList;
  static FILE* gOpenReportFD;

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  Iostat();

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~Iostat();

  //----------------------------------------------------------------------------
  //! Perform object initialization
  //!
  //! @param instance_name used to build the hash map key to be stored in QDB
  //! @param port instance port
  //! @param legacy_file path legacy iostat file path location
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool Init(const std::string& instance_name, int port,
            const std::string& legacy_file);

  //----------------------------------------------------------------------------
  //! Apply instance level configuration concerning IoStats
  //!
  //! @param fsview pointer to FsView object
  //----------------------------------------------------------------------------
  void ApplyConfig(FsView* fsview);

  //----------------------------------------------------------------------------
  //! Store IoStat config in the instance level configuration
  //!
  //! @param fsview pointer to FsView object
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool StoreIostatConfig(FsView* fsview) const;

  //----------------------------------------------------------------------------
  //! Method executed by the thread receiving reports
  //!
  //! @param assistant reference to thread object
  //----------------------------------------------------------------------------
  void Receive(ThreadAssistant& assistant) noexcept;

  //----------------------------------------------------------------------------
  //! Method executed by the thread ciruclating the entires
  //!
  //! @param assistant reference to thread object
  //----------------------------------------------------------------------------
  void Circulate(ThreadAssistant& assistant) noexcept;

  //----------------------------------------------------------------------------
  //! Start collection thread
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool StartCollection();

  //----------------------------------------------------------------------------
  //! Stop collection thread
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool StopCollection();

  //----------------------------------------------------------------------------
  //! Start popularity thread
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool StartPopularity();

  //----------------------------------------------------------------------------
  //! Stop popularity thread
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool StopPopularity();

  //----------------------------------------------------------------------------
  //! Start daily report thread
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool StartReport();

  //----------------------------------------------------------------------------
  //! Stop daily report thread
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool StopReport();

  //----------------------------------------------------------------------------
  //! Start namespace report thread
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool StartReportNamespace();

  //----------------------------------------------------------------------------
  //! Stop namespace report thread
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool StopReportNamespace();

  //----------------------------------------------------------------------------
  //! Add UDP target
  //!
  //! @param target new UDP target
  //! @param store_and_lock if true store new target in config
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool AddUdpTarget(const std::string& target, bool store_and_lock = true);

  //----------------------------------------------------------------------------
  //! Remove UDP target
  //!
  //! @param target UDP target to be removed
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool RemoveUdpTarget(const std::string& target);

  //----------------------------------------------------------------------------
  //! Write record to the stream - used by the MGM to push entries
  //!
  //! @param report report entry
  //----------------------------------------------------------------------------
  void WriteRecord(const std::string& record);

  //------------------------------------------------------------------------------
  //! Print IO statistics
  //------------------------------------------------------------------------------
  void PrintOut(XrdOucString& out, bool summary, bool details, bool monitoring,
                bool numerical = false, bool top = false, bool domain = false,
                bool apps = false, bool sample_stat = false, time_t time_ago = 0,
                time_t time_interval = 0, XrdOucString option = "");

  //----------------------------------------------------------------------------
  //! Compute and print out the namespace popularity ranking
  //!
  //! @param out output string
  //! @param option fileter options
  //----------------------------------------------------------------------------
  void PrintNsPopularity(XrdOucString& out, XrdOucString option = "") const;

  //----------------------------------------------------------------------------
  //! Print namespace activity report for given path
  //!
  //! @param path namespace path
  //! @param out output string
  //----------------------------------------------------------------------------
  void PrintNsReport(const char* path, XrdOucString& out) const;

  //----------------------------------------------------------------------------
  //! Record measurement to the various periods it overlaps with
  //!
  //! @param tag measurement info tag
  //! @param uid user id
  //! @param gid group id
  //! @param val measurement value
  //! @param start start timestamp of measurement
  //! @param stop stop timestamp of measurement
  //! @param now current timestamp
  //----------------------------------------------------------------------------
  void Add(const std::string& tag, uid_t uid, gid_t gid, unsigned long long val,
           time_t start, time_t stop, time_t now);

  //----------------------------------------------------------------------------
  //! Get sum of measurements for the given tag (looping all uids per tag)
  //! @note: needs a lock on the mDataMutex
  //!
  //! @param tag measurement info tag
  //!
  //! @return total value
  //----------------------------------------------------------------------------
  unsigned long long GetTotalStatForTag(const char* tag) const;

  //----------------------------------------------------------------------------
  //! Get sum of measurements for the given tag (looping all uids per tag) and period
  //! @note: needs a lock on the mDataMutex
  //!
  //! @param tag measurement info tag
  //! @parma period time interval of interest
  //!
  //! @return total value
  //----------------------------------------------------------------------------
  unsigned long long GetPeriodStatForTag(const char* tag, size_t period,
                                         time_t secago = 0) const;

private:
#ifdef IN_TEST_HARNESS
public:
#endif
  inline static const std::string USER_ID_TYPE = "u";
  inline static const std::string GROUP_ID_TYPE = "g";
  ///< Max delay for cache in front of QDB
  static constexpr std::chrono::seconds mCacheFlushDelay {30};
  //! Max cache size before flush - 30 entries per uid/gid pair times 100 users
  static constexpr unsigned int mMapMaxSize {3000};

  google::sparse_hash_map<std::string, unsigned long long> IostatTag;
  google::sparse_hash_map<std::string, IostatPeriods> IostatPeriodsTag;

  google::sparse_hash_map<std::string,
         google::sparse_hash_map<uid_t, unsigned long long>> IostatUid;
  google::sparse_hash_map<std::string,
         google::sparse_hash_map<gid_t, unsigned long long>> IostatGid;
  google::sparse_hash_map<std::string,
         google::sparse_hash_map<uid_t, IostatPeriods>> IostatPeriodsUid;
  google::sparse_hash_map<std::string,
         google::sparse_hash_map<gid_t, IostatPeriods>> IostatPeriodsGid;
  google::sparse_hash_map<std::string, IostatPeriods> IostatPeriodsDomainIOrb;
  google::sparse_hash_map<std::string, IostatPeriods> IostatPeriodsDomainIOwb;
  google::sparse_hash_map<std::string, IostatPeriods> IostatPeriodsAppIOrb;
  google::sparse_hash_map<std::string, IostatPeriods> IostatPeriodsAppIOwb;
  std::atomic<bool> mDoneInit;
  //! Flusher to QDB backend
  std::unique_ptr<eos::MetadataFlusher> mFlusher;
  std::string mFlusherPath;
  //! Mutex protecting the above data structures
  std::mutex mDataMutex;
  //! If true then use the file based approach otherwise store info in QDB
  std::atomic<bool> mLegacyMode;
  //! File path where statistics are stored on disk
  std::string mLegacyFilePath;
  std::atomic<bool> mRunning;
  //! Internal QClient object
  std::unique_ptr<qclient::QClient> mQcl;
  //! Flag to store reports in the local report store
  std::atomic<bool> mReportSave;
  //! Flag if we should fill the report namespace
  std::atomic<bool> mReportNamespace;
  //! Flag if we fill the popularity maps (protected by this::Mutex)
  std::atomic<bool> mReportPopularity;
  //! QuarkDB hash map key name where info is saved
  std::string mHashKeyBase;
  //! Map of cached IoStat updates
  std::map<std::string, unsigned long long> mMapCacheUpdates;
  std::mutex mThreadSyncMutex; ///< Mutex serializing thread(s) start/stop
  AssistedThread mReceivingThread; ///< Looping thread receiving reports
  AssistedThread mCirculateThread; ///< Looping thread circulating report
  //! Mutex protecting the UDP broadcast data structures that follow
  mutable std::mutex mBcastMutex;
  //! Destinations for udp popularity packets
  std::set<std::string> mUdpPopularityTarget;
  //! Socket to the udp destination(s)
  std::map<std::string, int> mUdpSocket;
  //! Socket address structure to be reused for messages
  std::map<std::string, struct sockaddr_in> mUdpSockAddr;
  //! Mutex protecting the popularity data structures
  mutable std::mutex mPopularityMutex;
  //! Popularity data structure
  struct Popularity {
    unsigned int nread;
    unsigned long long rb;
  };

  //! Points to the bin which was last used in IostatPopularity
  std::atomic<size_t> mLastPopularityBin;
  google::sparse_hash_map<std::string, struct Popularity>
    IostatPopularity[IOSTAT_POPULARITY_HISTORY_DAYS];
  typedef std::pair<std::string, struct Popularity> popularity_t;

  //----------------------------------------------------------------------------
  //! Value comparator for number of reads
  //----------------------------------------------------------------------------
  struct PopularityCmp_nread {
    bool operator()(popularity_t const& l, popularity_t const& r)
    {
      if (l.second.nread == r.second.nread) {
        return (l.first < r.first);
      }

      return l.second.nread > r.second.nread;
    }
  };

  //---------------------------------------------------------------------------
  //! Value comparator for read bytes
  //----------------------------------------------------------------------------
  struct PopularityCmp_rb {
    bool operator()(popularity_t const& l, popularity_t const& r)
    {
      if (l.second.rb == r.second.rb) {
        return (l.first < r.first);
      }

      return l.second.rb > r.second.rb;
    }
  };

  //----------------------------------------------------------------------------
  //! Record measurements directly in QDB
  //!
  //! @param tag measurement info tag
  //! @param uid user id
  //! @param gid group id
  //! @param val measurement value
  //----------------------------------------------------------------------------
  void AddToQdb(const std::string& tag, uid_t uid, gid_t gid,
                unsigned long long val);

  //----------------------------------------------------------------------------
  //! Do the UDP broadcast
  //!
  //! @param report pointer to report object
  //----------------------------------------------------------------------------
  void UdpBroadCast(eos::common::Report* report) const;

  //----------------------------------------------------------------------------
  //! Encode the UDP popularity targets to a string using the provided separator
  //!
  //! @param separator separator for the encoding
  //!
  //! @return encoded list of UDP popularity targets
  //----------------------------------------------------------------------------
  std::string EncodeUdpPopularityTargets() const;

  //----------------------------------------------------------------------------
  //! Add entry to popularity statistics
  //!
  //! @param path entry path
  //! @param rb read bytes
  //! @param start start timestamp of the operation
  //! @param stop stop timstamp of the operation
  //----------------------------------------------------------------------------
  void AddToPopularity(const std::string& path, unsigned long long rb,
                       time_t start, time_t stop);

  //----------------------------------------------------------------------------
  //! One off migration from file based to QDB of IoStat information
  //!
  //! @param legacy_file file path for IoStat information
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool OneOffQdbMigration(const std::string& legacy_file);

  //----------------------------------------------------------------------------
  //! Create/encode hash map key string from the given information
  //!
  //! @param id_type type of id, can be either user USER_ID_TYPE
  //!        or group GROUP_ID_TYPE
  //! @param id_val numeric value of the id
  //! @param tag type of tag eg. bytes_read, bytes_write etc.
  //!
  //! @param return string representing the key to be used for storing this
  //!        info in the hash map
  //----------------------------------------------------------------------------
  static std::string EncodeKey(const std::string& id_type,
                               const std::string& id_val,
                               const std::string& tag);

  //----------------------------------------------------------------------------
  //! Decode/parse hash map key to extract entry information
  //!
  //! @param key hash map key obtained by calling EncodeKey
  //! @param id_type type of id, can be either user USER_ID_TYPE
  //!        or group GROUP_ID_TYPE
  //! @param id_val numeric value of the id
  //! @param tag type of tag eg. bytes_read, bytes_write etc.
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  static bool DecodeKey(const std::string& key, std::string& id_type,
                        std::string& id_val, std::string& tag);

  //----------------------------------------------------------------------------
  //! Load info from Qdb backend clearing up any memory data structures
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool LoadFromQdb();

  //----------------------------------------------------------------------------
  //! Get hash key under which info is stored in QDB. This also included the
  //! current year and it's cached for 5 minutes.
  //----------------------------------------------------------------------------
  std::string GetHashKey() const;

  //----------------------------------------------------------------------------
  //! Store statistics in legacy file format
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool LegacyStoreInFile();

  //----------------------------------------------------------------------------
  //! Restore statistics from legacy file format
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool LegacyRestoreFromFile();

  //----------------------------------------------------------------------------
  //! Save given update in the in-memory cache
  //!
  //! @param uid_key uid encoded key
  //! @param gid_key gid encoded key
  //! @param val value update
  //----------------------------------------------------------------------------
  void CacheUpdate(const std::string& uid_key, const std::string& gid_key,
                   unsigned long long val);

  //----------------------------------------------------------------------------
  //! Check if the cache needs to be flushed
  //!
  //! @return true if cache must be flushed, otherwise false
  //----------------------------------------------------------------------------
  bool ShouldFlushCache();

  //----------------------------------------------------------------------------
  //! Flush all cached entries to the QDB backed
  //----------------------------------------------------------------------------
  void FlushCache();
};

EOSMGMNAMESPACE_END
