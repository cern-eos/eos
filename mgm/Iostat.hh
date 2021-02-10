// ----------------------------------------------------------------------
// File: Iostat.hh
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

#ifndef __EOSMGM_IOSTAT__HH__
#define __EOSMGM_IOSTAT__HH__

#include "mgm/Namespace.hh"
#include "mgm/FsView.hh"
#include "common/AssistedThread.hh"
#include "common/StringConversion.hh"
#include <google/sparse_hash_map>
#include <sys/types.h>
#include <string>
#include <set>
#include <atomic>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

namespace eos
{
namespace common
{
class Report;
}
}

EOSMGMNAMESPACE_BEGIN

// define the history in days we want to do popularity tracking
#define IOSTAT_POPULARITY_HISTORY_DAYS 7
#define IOSTAT_POPULARITY_DAY 86400

class IostatAvg
{
public:
  unsigned long long avg86400[60];
  unsigned long long avg3600[60];
  unsigned long long avg300[60];
  unsigned long long avg60[60];

  IostatAvg()
  {
    memset(avg86400, 0, sizeof(avg86400));
    memset(avg3600, 0, sizeof(avg3600));
    memset(avg300, 0, sizeof(avg300));
    memset(avg60, 0, sizeof(avg60));
  }

  ~IostatAvg() { };

  void
  Add(unsigned long long val, time_t starttime, time_t stoptime);

  void
  StampZero(time_t& now);

  double
  GetAvg86400();

  double
  GetAvg3600();

  double
  GetAvg300();

  double
  GetAvg60();
};

class Iostat
{
  // -------------------------------------------------------------
  // ! subscribes to our MQ, collects and digestes report messages
  // -------------------------------------------------------------
private:

  XrdSysMutex Mutex;
  google::sparse_hash_map<std::string, google::sparse_hash_map<uid_t, unsigned long long> >
  IostatUid;
  google::sparse_hash_map<std::string, google::sparse_hash_map<gid_t, unsigned long long> >
  IostatGid;
  google::sparse_hash_map<std::string, google::sparse_hash_map<uid_t, IostatAvg> >
  IostatAvgUid;
  google::sparse_hash_map<std::string, google::sparse_hash_map<gid_t, IostatAvg> >
  IostatAvgGid;

  google::sparse_hash_map<std::string, IostatAvg> IostatAvgDomainIOrb;
  google::sparse_hash_map<std::string, IostatAvg> IostatAvgDomainIOwb;

  google::sparse_hash_map<std::string, IostatAvg> IostatAvgAppIOrb;
  google::sparse_hash_map<std::string, IostatAvg> IostatAvgAppIOwb;

  std::set<std::string> IoDomains;
  std::set<std::string> IoNodes;

  // -----------------------------------------------------------
  // here we handle the popularity history for the last 7+1 days
  // -----------------------------------------------------------

  XrdSysMutex PopularityMutex;

  struct Popularity {
    unsigned int nread;
    unsigned long long rb;
  };

  // Points to the bin which was last used in IostatPopularity
  std::atomic<size_t> IostatLastPopularityBin;

  google::sparse_hash_map<std::string, struct Popularity>
    IostatPopularity[ IOSTAT_POPULARITY_HISTORY_DAYS ];

  typedef std::pair<std::string, struct Popularity> popularity_t;

  struct PopularityCmp_nread {

    bool operator()(popularity_t const& l, popularity_t const& r)
    {
      if (l.second.nread == r.second.nread) {
        return (l.first < r.first);
      }

      return l.second.nread > r.second.nread;
    }
  };

  struct PopularityCmp_rb {

    bool operator()(popularity_t const& l, popularity_t const& r)
    {
      if (l.second.rb == r.second.rb) {
        return (l.first < r.first);
      }

      return l.second.rb > r.second.rb;
    }
  };


  //! Flag to store reports in the local report store
  std::atomic<bool> mReport;
  //! Flag if we should fill the report namespace
  std::atomic<bool> mReportNamespace;
  //! Flag if we fill the popularity maps (protected by this::Mutex)
  std::atomic<bool> mReportPopularity;
  mutable XrdSysMutex mBcastMutex; ///< Mutex protecting the following sets
  // Destinations for udp popularity packets
  std::set<std::string> mUdpPopularityTarget;
  //! Socket to the udp destination(s)
  std::map<std::string, int> mUdpSocket;
  //! Socket address structure to be reused for messages
  std::map<std::string, struct sockaddr_in> mUdpSockAddr;
  //! File name where a dump is loaded/saved in Restore/Store
  XrdOucString mStoreFileName;


public:
  // configuration keys used in config key-val store
  static const char* gIostatCollect;
  static const char* gIostatReport;
  static const char* gIostatReportNamespace;
  static const char* gIostatPopularity;
  static const char* gIostatUdpTargetList;

  static FILE* gOpenReportFD;
  bool mRunning;

  Iostat();
  ~Iostat();

  void
  ApplyIostatConfig(FsView* fsview)
  {
    std::string iocollect = fsview->GetGlobalConfig(Iostat::gIostatCollect);
    std::string ioreport = fsview->GetGlobalConfig(Iostat::gIostatReport);
    std::string ioreportns = fsview->GetGlobalConfig(
                               Iostat::gIostatReportNamespace);
    std::string iopopularity = fsview->GetGlobalConfig(
                                 Iostat::gIostatPopularity);
    std::string udplist = fsview->GetGlobalConfig(
                            Iostat::gIostatUdpTargetList);

    if ((iocollect == "true") || (iocollect.empty())) {
      // by default enable
      StartCollection();
    }

    mReport = (ioreport == "true");
    mReportNamespace = (ioreportns == "true");
    mReportPopularity = (iopopularity == "true") || (iopopularity.empty());
    std::string delimiter = "|";
    std::vector<std::string> hostlist;
    eos::common::StringConversion::Tokenize(udplist, hostlist, delimiter);
    XrdSysMutexHelper scope_lock(mBcastMutex);
    mUdpPopularityTarget.clear();

    for (size_t i = 0; i < hostlist.size(); i++) {
      AddUdpTarget(hostlist[i].c_str(), false);
    }
  }

  /* ------------------------------------------------------------------------- */
  bool
  StoreIostatConfig(FsView* fsview)
  {
    bool ok = true;
    ok &= fsview->SetGlobalConfig(Iostat::gIostatPopularity,
                                  mReportPopularity ? "true" : "false");
    ok &= fsview->SetGlobalConfig(Iostat::gIostatReport,
                                  mReport ? "true" : "false");
    ok &= fsview->SetGlobalConfig(Iostat::gIostatReportNamespace,
                                  mReportNamespace ? "true" : "false");
    ok &= fsview->SetGlobalConfig(Iostat::gIostatCollect,
                                  mRunning ? "true" : "false");
    std::string udp_popularity_targets = EncodeUdpPopularityTargets();

    if (!udp_popularity_targets.empty()) {
      ok &= fsview->SetGlobalConfig(Iostat::gIostatUdpTargetList,
                                    udp_popularity_targets);
    }

    return ok;
  }

  bool
  SetStoreFileName(const char* storefilename)
  {
    mStoreFileName = storefilename;
    return Restore();
  }

  bool Store();
  bool Restore();

  void StartCirculate();
  bool Start();
  bool Stop();
  bool StartCollection();
  bool StopCollection();
  bool StartPopularity();
  bool StopPopularity();
  bool StartReport();
  bool StopReport();
  bool StartReportNamespace();
  bool StopReportNamespace();
  bool AddUdpTarget(const char* target, bool storeitandlock = true);
  bool RemoveUdpTarget(const char* target);

  void PrintOut(XrdOucString& out, bool summary, bool details, bool monitoring,
                bool numerical = false, bool top = false, bool domain = false,
                bool apps = false, XrdOucString option = "");

  void PrintNs(XrdOucString& out, XrdOucString option = "");

  void UdpBroadCast(eos::common::Report*);

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
  //! Encode the UDP popularity targets to a string using the provided separator
  //!
  //! @param separator separator for the encoding
  //!
  //! @return encoded list of UDP popularity targets
  //----------------------------------------------------------------------------
  std::string EncodeUdpPopularityTargets() const;


  void WriteRecord(std::string&
                   record); // let's the MGM add some record into the stream

  static bool NamespaceReport(const char* path, XrdOucString& stdOut,
                              XrdOucString& stdErr);

  void
  AddToPopularity(std::string path, unsigned long long rb, time_t starttime,
                  time_t stoptime);




  // Stats collection
  void
  Add(const char* tag, uid_t uid, gid_t gid, unsigned long long val,
      time_t starttime,
      time_t stoptime)
  {
    Mutex.Lock();
    IostatUid[tag][uid] += val;
    IostatGid[tag][gid] += val;
    IostatAvgUid[tag][uid].Add(val, starttime, stoptime);
    IostatAvgGid[tag][gid].Add(val, starttime, stoptime);
    Mutex.UnLock();
  }

  unsigned long long
  GetTotal(const char* tag)
  {
    google::sparse_hash_map<uid_t, unsigned long long>::const_iterator it;
    unsigned long long val = 0;

    if (!IostatUid.count(tag)) {
      return 0;
    }

    for (it = IostatUid[tag].begin(); it != IostatUid[tag].end(); ++it) {
      val += it->second;
    }

    return val;
  }

  // warning: you have to lock the mutex if directly used

  double
  GetTotalAvg86400(const char* tag)
  {
    google::sparse_hash_map<uid_t, IostatAvg>::iterator it;
    double val = 0;

    if (!IostatAvgUid.count(tag)) {
      return 0;
    }

    for (it = IostatAvgUid[tag].begin(); it != IostatAvgUid[tag].end(); ++it) {
      val += it->second.GetAvg86400();
    }

    return val;
  }

  // warning: you have to lock the mutex if directly used

  double
  GetTotalAvg3600(const char* tag)
  {
    google::sparse_hash_map<uid_t, IostatAvg>::iterator it;
    double val = 0;

    if (!IostatAvgUid.count(tag)) {
      return 0;
    }

    for (it = IostatAvgUid[tag].begin(); it != IostatAvgUid[tag].end(); ++it) {
      val += it->second.GetAvg3600();
    }

    return val;
  }

  // warning: you have to lock the mutex if directly used

  double
  GetTotalAvg300(const char* tag)
  {
    google::sparse_hash_map<uid_t, IostatAvg>::iterator it;
    double val = 0;

    if (!IostatAvgUid.count(tag)) {
      return 0;
    }

    for (it = IostatAvgUid[tag].begin(); it != IostatAvgUid[tag].end(); ++it) {
      val += it->second.GetAvg300();
    }

    return val;
  }

  // warning: you have to lock the mutex if directly used

  double
  GetTotalAvg60(const char* tag)
  {
    google::sparse_hash_map<uid_t, IostatAvg>::iterator it;
    double val = 0;

    if (!IostatAvgUid.count(tag)) {
      return 0;
    }

    for (it = IostatAvgUid[tag].begin(); it != IostatAvgUid[tag].end(); ++it) {
      val += it->second.GetAvg60();
    }

    return val;
  }

private:
  AssistedThread mReceivingThread; ///< Looping thread receiving reports
  AssistedThread mCirculateThread; ///< Looping thread circulating reports
};

EOSMGMNAMESPACE_END

#endif
