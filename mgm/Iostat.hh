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
#include "mgm/FsView.hh"
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

//------------------------------------------------------------------------------
//! Class IostatPeriods holds read/write stats in 60 bins per each period of
//! last 1day, 1h, 5min, 1min
//------------------------------------------------------------------------------
class IostatPeriods
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  IostatPeriods()
  {
    for (size_t i = 0; i < std::size(mPeriodBins); ++i) {
      memset(mPeriodBins[i], 0, sizeof(mPeriodBins[i]));
    }
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

  //----------------------------------------------------------------------------
  //! Reset the bin affected by the given timestamp
  //----------------------------------------------------------------------------
  void StampZero(time_t& timestamp);

  //----------------------------------------------------------------------------
  //! Get the sum of values for the given period
  //!
  //! @param period type of period
  //!
  //! @return sum for the given period
  //----------------------------------------------------------------------------
  unsigned long long GetSumForPeriod(Period period) const;

private:
#ifdef IN_TEST_HARNESS
public:
#endif
  static constexpr size_t sNumberOfPeriods = 4;
  static constexpr size_t sBinsPerPeriod = 60;
  //! 3 arrays to collect stat per 1day, 1h, 5m and 60sec each within 60 bins
  unsigned long long mPeriodBins[sNumberOfPeriods][sBinsPerPeriod];
  //! Setting the width of the (60) time bins of the arrays in mPeriodBins to
  //! 1440sec, 60sec, 5sec and 1 sec respectively
  const size_t mPeriodBinWidth[sNumberOfPeriods] {1440, 60, 5, 1};

  //----------------------------------------------------------------------------
  //! Measurement stop time and duration determine which of the bins of the
  //! corresponding period will get populated with the new values
  //!
  //! @param period_indx index of the period to be updated [0, sNumberOfPeriods)
  //! @param val measured value
  //! @param tdiff duration of the measurement
  //! @param stop end time of the measurement
  //----------------------------------------------------------------------------
  void AddToPeriod(size_t period_indx, unsigned long long val,
                   size_t tdiff, time_t stop);
};

//------------------------------------------------------------------------------
//! Iostat subscribes to MQ, collects and digests report messages
//------------------------------------------------------------------------------
class Iostat
{
public:
  //! Configuration keys used in config key-val store
  static const char* gIostatCollect;
  static const char* gIostatReport;
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
  void ApplyIostatConfig(FsView* fsview);

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
                bool apps = false, XrdOucString option = "");

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
  //! Get sum of measurements for the given tag
  //! @note: needs a lock on the mDataMutex
  //!
  //! @param tag measurement info tag
  //!
  //! @return total value
  //----------------------------------------------------------------------------
  unsigned long long GetTotalStatForTag(const char* tag) const;

  //----------------------------------------------------------------------------
  //! Get sum of measurements for the given tag an period
  //! @note: needs a lock on the mDataMutex
  //!
  //! @param tag measurement info tag
  //! @parma period time interval of interest
  //!
  //! @return total value
  //----------------------------------------------------------------------------
  unsigned long long GetPeriodStatForTag(const char* tag, Period period) const;

private:
#ifdef IN_TEST_HARNESS
public:
#endif
  inline static const std::string USER_ID_TYPE = "u";
  inline static const std::string GROUP_ID_TYPE = "g";
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
  std::set<std::string> IoDomains;
  std::set<std::string> IoNodes;
  //! Flusher to QDB backend
  std::unique_ptr<eos::MetadataFlusher> mFlusher;
  std::string mFlusherPath;
  //! Mutex protecting the above data structures
  std::mutex mDataMutex;
  std::atomic<bool> mRunning;
  //! Internal QClient object
  std::unique_ptr<qclient::QClient> mQcl;
  //! Flag to store reports in the local report store
  std::atomic<bool> mReport;
  //! Flag if we should fill the report namespace
  std::atomic<bool> mReportNamespace;
  //! Flag if we fill the popularity maps (protected by this::Mutex)
  std::atomic<bool> mReportPopularity;
  //! QuarkDB hash map key name where info is saved
  std::string mHashKeyBase;
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
};

EOSMGMNAMESPACE_END
