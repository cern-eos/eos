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

/*----------------------------------------------------------------------------*/
#include "mgm/Namespace.hh"
#include "mq/XrdMqClient.hh"
#include "common/Logging.hh"
#include "common/FileId.hh"
#include "common/Path.hh"
#include "common/Report.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/
#include <google/sparse_hash_map>
#include <sys/types.h>
#include <string>
#include <set>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN


// define the history in days we want to do popularity tracking
#define IOSTAT_POPULARITY_HISTORY_DAYS 7 
#define IOSTAT_POPULARITY_DAY 86400

class IostatAvg {
public:
  unsigned long avg86400[60];
  unsigned long avg3600[60];
  unsigned long avg300[60];
  unsigned long avg60[60];

  IostatAvg() {
    memset(avg86400,0, sizeof(avg86400));
    memset(avg3600,0, sizeof(avg3600));
    memset(avg300,0,sizeof(avg300));
    memset(avg60,0,sizeof(avg60));
  }

  ~IostatAvg(){};

 void Add(unsigned long val, time_t starttime, time_t stoptime) {
    time_t now = time(0);
    
    size_t tdiff = stoptime-starttime;

    size_t toff  = now - stoptime;

    if (toff < 86400) {
      // if the measurements was done in the last 86400 seconds
      unsigned int mbins = tdiff / 1440; // number of bins the measurement was hitting
      if (mbins==0)
        mbins=1;
      unsigned long norm_val = (1.0*val/mbins);

      for (size_t bins = 0; bins < mbins; bins++) {
        unsigned int bin86400 = ( ((stoptime-(bins*1440))/1440)% 60);
        avg86400[bin86400] += norm_val;
      }
    }

    if (toff < 3600) {
      // if the measurements was done in the last 3600 seconds
      unsigned int mbins = tdiff / 60; // number of bins the measurement was hitting
      if (mbins==0)
        mbins=1;
      unsigned long norm_val = mbins?(1.0*val/mbins):val;

      for (size_t bins = 0; bins < mbins; bins++) {
        unsigned int bin3600 = ( ((stoptime-(bins*60))/60)% 60);
        avg3600[bin3600] += norm_val;
      }
    }

    if (toff < 300) {
      // if the measurements was done in the last 300 seconds
      unsigned int mbins = tdiff / 5; // number of bins the measurement was hitting
      if (mbins==0)
        mbins=1;
      unsigned long norm_val = mbins?(1.0*val/mbins):val;

      for (size_t bins = 0; bins < mbins; bins++) {
        unsigned int bin300 = ( ((stoptime-(bins*5))/5)% 60);
        avg300[bin300] += norm_val;
      }
    }

    if (toff < 60) {
      // if the measurements was done in the last 60 seconds
      unsigned int mbins = tdiff / 1 ; // number of bins the measurement was hitting
      if (mbins==0)
        mbins=1;
      unsigned long norm_val = mbins?(1.0*val/mbins):val;
      for (size_t bins = 0; bins < mbins; bins++) {
        unsigned int bin60 = ( ((stoptime-(bins*1))/1)% 60);
        avg60[bin60] += norm_val;
      }
    }
  }

  void StampZero() {
    unsigned int bin86400 = (time(0) / 1440);
    unsigned int bin3600 = (time(0) / 60);
    unsigned int bin300  = (time(0) / 5);
    unsigned int bin60   = (time(0) / 1);
    
    avg86400[(bin86400+1)%60] = 0;
    avg3600[(bin3600+1)%60] = 0;
    avg300[(bin300+1)%60] = 0;
    avg60[(bin60+1)%60] = 0;
  }

  double GetAvg86400() {
    double sum=0;
    for (int i=0; i< 60; i++) 
      sum += avg86400[i];
    return sum;
  }

  double GetAvg3600() {
    double sum=0;
    for (int i=0; i< 60; i++) 
      sum += avg3600[i];
    return sum;
  }

  double GetAvg300() {
    double sum=0;
    for (int i=0; i< 60; i++) 
      sum += avg300[i];
    return sum;
  }

  double GetAvg60() {
    double sum=0;
    for (int i=0; i< 60; i++) 
      sum += avg60[i];
    return sum;
  }
};

class Iostat {
  // -------------------------------------------------------------
  // ! subscribes to our MQ, collects and digestes report messages
  // -------------------------------------------------------------
private:

  XrdSysMutex Mutex;
  google::sparse_hash_map<std::string, google::sparse_hash_map<uid_t, unsigned long long> > IostatUid;
  google::sparse_hash_map<std::string, google::sparse_hash_map<gid_t, unsigned long long> > IostatGid;
  google::sparse_hash_map<std::string, google::sparse_hash_map<uid_t, IostatAvg> > IostatAvgUid;
  google::sparse_hash_map<std::string, google::sparse_hash_map<gid_t, IostatAvg> > IostatAvgGid;

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

  size_t IostatLastPopularityBin; // this points to the bin which was last used in IostatPopularity

  google::sparse_hash_map<std::string, struct Popularity> IostatPopularity[ IOSTAT_POPULARITY_HISTORY_DAYS ];
  
  typedef std::pair<std::string, struct Popularity> popularity_t;
  
  struct PopularityCmp_nread {
    bool operator() (popularity_t const &l, popularity_t const &r) {
      if (l.second.nread == r.second.nread) return (l.first <r.first);
      return l.second.nread > r.second.nread;
    }
  };
  
  struct PopularityCmp_rb {
    bool operator() (popularity_t const &l, popularity_t const &r) {
      if (l.second.rb == r.second.rb) return (l.first <r.first);
      return l.second.rb > r.second.rb;
    }
  };


  bool mReport;           // indicates if we store reports to the local report store

  bool mReportNamespace;  // indicates if we fill the report namespace 

  bool mReportPopularity; // indicates if we fill the popularity maps (protected by this::Mutex)


  XrdSysMutex BroadcastMutex;  // protecting the following set
  std::set<std::string> mUdpPopularityTarget; // contains all destinations for udp popularity packets
  std::map<std::string, int> mUdpSocket;      // contains a socket to the udp destination
  std::map<std::string, struct sockaddr_in> mUdpSockAddr;  // contains the socket address structure to be reused for messages
  XrdOucString mUdpPopularityTargetList;      // contains the string describing the set above for the configuration store
  XrdOucString mStoreFileName; // file name where a dump is loaded/saved in Restore/Store


public:
  // configuration keys used in config key-val store
  static const char* gIostatCollect;
  static const char* gIostatReport;
  static const char* gIostatReportNamespace;
  static const char* gIostatPopularity;
  static const char* gIostatUdpTargetList;

  pthread_t thread;
  pthread_t cthread;
  bool mRunning;
  bool mInit;

  XrdMqClient mClient;

  Iostat();
  ~Iostat();

  void ApplyIostatConfig(); // apply the configuration settings to the iostat class
  bool StoreIostatConfig(); // store the currently running settions of the iostat class to the configuration

  bool SetStoreFileName(const char* storefilename) {
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
  bool AddUdpTarget(const char* target, bool storeitandlock=true);
  bool RemoveUdpTarget(const char* target);

  void PrintOut(XrdOucString &out, bool summary, bool details, bool monitoring, bool numerical=false, bool top=false, bool domain=false, bool apps=false, XrdOucString option="");

  void PrintNs(XrdOucString &out, XrdOucString option="");

  void UdpBroadCast(eos::common::Report*);

  static void* StaticReceive(void*);
  static void* StaticCirculate(void*);
  void* Receive();

  static bool NamespaceReport(const char* path, XrdOucString &stdOut, XrdOucString &stdErr);

  void AddToPopularity(std::string path, unsigned long long rb, time_t starttime, time_t stoptime) {
    size_t popularitybin = ( ((starttime + stoptime)/2)% (IOSTAT_POPULARITY_DAY * IOSTAT_POPULARITY_HISTORY_DAYS))/ IOSTAT_POPULARITY_DAY;
    PopularityMutex.Lock();
    eos::common::Path cPath(path.c_str());
    for (size_t k=0; k< cPath.GetSubPathSize(); k++) {
      std::string sp = cPath.GetSubPath(k);
      IostatPopularity[popularitybin][sp].rb += rb;
      IostatPopularity[popularitybin][sp].nread++;
    }
    IostatLastPopularityBin = popularitybin;
    PopularityMutex.UnLock();
  }

  // stats collection
  void Add(const char* tag, uid_t uid, gid_t gid, unsigned long val, time_t starttime, time_t stoptime) {
    Mutex.Lock();
    IostatUid[tag][uid] += val;
    IostatGid[tag][gid] += val;
    IostatAvgUid[tag][uid].Add(val, starttime, stoptime);
    IostatAvgGid[tag][gid].Add(val, starttime, stoptime);
    Mutex.UnLock();
  }
  
  unsigned long long GetTotal(const char* tag) {
    google::sparse_hash_map<uid_t, unsigned long long>::const_iterator it;
    unsigned long long val=0;
    if (!IostatUid.count(tag))
      return 0;
    for (it=IostatUid[tag].begin(); it!= IostatUid[tag].end(); ++it) {
      val += it->second;
    }
    return val;
  }

  // warning: you have to lock the mutex if directly used
  double GetTotalAvg86400(const char* tag) {
    google::sparse_hash_map<uid_t, IostatAvg>::iterator it;
    double val = 0;
    if (!IostatAvgUid.count(tag))
      return 0;
    for (it=IostatAvgUid[tag].begin(); it!= IostatAvgUid[tag].end(); ++it) {
      val += it->second.GetAvg86400();
    }
    return val;
  }

  // warning: you have to lock the mutex if directly used
  double GetTotalAvg3600(const char* tag) {
    google::sparse_hash_map<uid_t, IostatAvg>::iterator it;
    double val = 0;
    if (!IostatAvgUid.count(tag))
      return 0;
    for (it=IostatAvgUid[tag].begin(); it!= IostatAvgUid[tag].end(); ++it) {
      val += it->second.GetAvg3600();
    }
    return val;
  }

  // warning: you have to lock the mutex if directly used
  double GetTotalAvg300(const char* tag)  {
    google::sparse_hash_map<uid_t, IostatAvg>::iterator it;
    double val = 0;
    if (!IostatAvgUid.count(tag))
      return 0;
    for (it=IostatAvgUid[tag].begin(); it!= IostatAvgUid[tag].end(); ++it) {
      val += it->second.GetAvg300();
    }
    return val;
  }

  // warning: you have to lock the mutex if directly used
  double GetTotalAvg60(const char* tag)   {
    google::sparse_hash_map<uid_t, IostatAvg>::iterator it;

    double val = 0;
    if (!IostatAvgUid.count(tag))
      return 0;
    for (it=IostatAvgUid[tag].begin(); it!= IostatAvgUid[tag].end(); ++it) {
      val += it->second.GetAvg60();
    }
    return val;
  }

  void* Circulate();
};

EOSMGMNAMESPACE_END

#endif
