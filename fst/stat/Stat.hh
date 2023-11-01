// ----------------------------------------------------------------------
// File: Stat.hh
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

#ifndef FST_STAT_HH_
#define FST_STAT_HH_

/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucHash.hh"
#include "XrdSys/XrdSysPthread.hh"
#include "common/AssistedThread.hh"
#include "fst/Namespace.hh"
/*----------------------------------------------------------------------------*/
#include <vector>
#include <map>
#include <string>
#include <deque>
#include <math.h>
#include <thread>
#include <json/json.h>

EOSFSTNAMESPACE_BEGIN

class StatAvg
{
public:
  unsigned long avg3600[3600];
  unsigned long avg300[300];
  unsigned long avg60[60];
  unsigned long avg10[10];

  StatAvg()
  {
    memset(avg3600, 0, sizeof(avg3600));
    memset(avg300, 0, sizeof(avg300));
    memset(avg60, 0, sizeof(avg60));
    memset(avg10, 0, sizeof(avg10));
  }

  ~StatAvg() { };

  void
  Add(unsigned long val)
  {
    time_t now = time(0);

    if (now == -1) {
      now = 0;
    }

    unsigned int bin3600 = (now % 3600);
    unsigned int bin300 = (now % 300);
    unsigned int bin60 = (now % 60);
    unsigned int bin10 = (now % 10);
    avg3600[(bin3600 + 1) % 3600] = 0;
    avg3600[bin3600] += val;
    avg300[(bin300 + 1) % 300] = 0;
    avg300[bin300] += val;
    avg60[(bin60 + 1) % 60] = 0;
    avg60[bin60] += val;
    avg10[(bin10 + 1) % 5] = 0;
    avg10[bin10] += val;
  }

  void
  StampZero()
  {
    time_t now = time(0);

    if (now == -1) {
      now = 0;
    }

    unsigned int bin3600 = (now % 3600);
    unsigned int bin300 = (now % 300);
    unsigned int bin60 = (now % 60);
    unsigned int bin10 = (now % 5);
    avg3600[(bin3600 + 1) % 3600] = 0;
    avg300[(bin300 + 1) % 300] = 0;
    avg60[(bin60 + 1) % 60] = 0;
    avg10[(bin10 + 1) % 5] = 0;
  }

  double
  GetAvg3600()
  {
    double sum = 0;

    for (int i = 0; i < 3600; i++) {
      sum += avg3600[i];
    }

    return (sum / 3599);
  }

  double
  GetAvg300()
  {
    double sum = 0;

    for (int i = 0; i < 300; i++) {
      sum += avg300[i];
    }

    return (sum / 299);
  }

  double
  GetAvg60()
  {
    double sum = 0;

    for (int i = 0; i < 60; i++) {
      sum += avg60[i];
    }

    return (sum / 59);
  }

  double
  GetAvg10()
  {
    double sum = 0;

    for (int i = 0; i < 5; i++) {
      sum += avg10[i];
    }

    return (sum / 4);
  }
};

#define __SUM__TOTAL__ ":sum"

#define ADD_IO_STAT(__ID__, __TAG__, __VID__, __VALUE__)	\
  gOFS.mStreamStats.Add(__ID__,   \
			 __VID__.uid,   \
    			 __VID__.gid,   \
			 __VID__.app,   \
                         __TAG__,       \
			 __VALUE__);

#define ADD_IO_EXEC(__ID__, __TAG__, __VID__, __VALUE__)	\
  gOFS.mStreamStats.AddExec(__ID__,   \
			     __VID__.uid,	\
			     __VID__.gid,	\
			     __VID__.app,	\
                             __TAG__,           \
			     __VALUE__);


class Stat
{
public:

  size_t GetOps()
  {
    return sum_ops;
  }

  size_t GetOpsTS()
  {
    XrdSysMutexHelper sLock(Mutex);
    return GetOps();
  }

  void Add(const char* id, uid_t uid, gid_t gid, const std::string& app, const std::string& tag, unsigned long val);
  void AddExec(const char* id, uid_t, gid_t gid, const std::string& app, const std::string& tag, float exectime);
  void Remove(const char* id, uid_t uid, gid_t gid, const std::string& app, const std::string& tag);
  
  unsigned long long GetTotal(const char* tag);

  // warning: you have to lock the mutex if directly used
  double GetTotalAvg3600(const char* tag);
  // warning: you have to lock the mutex if directly used
  double GetTotalAvg300(const char* tag);

  // warning: you have to lock the mutex if directly used
  double GetTotalAvg60(const char* tag);

  // warning: you have to lock the mutex if directly used
  double GetTotalAvg10(const char* tag);

  // warning: you have to lock the mutex if directly used
  double GetExec(const char* tag, double& deviation);

  // warning: you have to lock the mutex if directly used
  double GetTotalExec(double& deviation, size_t& cnt);

  void Clear();

  void PrintOutTotal(std::string& out,
                     bool monitoring = false);

  void PrintOutTotalJson(Json::Value& out);

  std::string PrintOutTotalJson();
  
  void Circulate(ThreadAssistant& assistant);
  void Dump(ThreadAssistant& assistant);

  Stat() { TotalExec = 0.0; sum_ops = 0; mDumpPath="/var/eos/md/"; mPort = 1094;}

  void SetDumpPath(const char* path) {mDumpPath = path;}
  void SetPort(int port) {mPort = port;}

  std::string GetJson() {
    XrdSysMutexHelper sLock(JsonMutex);
    return json;
  }
  
  std::string GetJsonZBase64() {
    XrdSysMutexHelper sLock(JsonMutex);
    return jsonzbase64;
  }
  
  virtual ~Stat() {}

  bool Start();
  void Stop();

private:
  XrdSysMutex Mutex;

  XrdSysMutex JsonMutex; ///< mutex protection the JSON string
  std::string json; ///< Last json output dumped
  std::string jsonzbase64; ///< Last json output dumped in compressed base64 encoded format

  
  // first is name of value, then the map
  std::map<std::string, std::map<std::string, unsigned long long> >
  StatsId;
  std::map<std::string, std::map<std::string, StatAvg> >
  StatAvgId;
  std::map<std::string, std::deque<float> > StatExec;
  std::map<std::string, double > StatTotal;
  std::map<std::string, time_t> StatTime;
  
  double TotalExec;
  size_t sum_ops;
  AssistedThread mThread; ///< Thread doing the circulation
  AssistedThread mDumpThread; ///< Thread doing the regular dump
  std::string mDumpPath; ///< directory where to dump current state into a file
  int mPort; ///< port of this FST
};

EOSFSTNAMESPACE_END
#endif
