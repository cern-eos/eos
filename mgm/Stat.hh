// ----------------------------------------------------------------------
// File: Stat.hh
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

#ifndef __EOSMGM_MGMOFSSTAT__HH__
#define __EOSMGM_MGMOFSSTAT__HH__

/*----------------------------------------------------------------------------*/
#include "mgm/Namespace.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucHash.hh"
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/
#include <google/sparse_hash_map>
/*----------------------------------------------------------------------------*/
#include <vector>
#include <map>
#include <string>
#include <deque>
#include <math.h>

EOSMGMNAMESPACE_BEGIN

class StatAvg {
public:
  unsigned long avg3600[3600];
  unsigned long avg300[300];
  unsigned long avg60[60];
  unsigned long avg5[5];

  StatAvg() {
    memset(avg3600,0, sizeof(avg3600));
    memset(avg300,0,sizeof(avg300));
    memset(avg60,0,sizeof(avg60));
    memset(avg5,0,sizeof(avg5));
  }

  ~StatAvg(){};

  void Add(unsigned long val) {
    unsigned int bin3600 = (time(0) % 3600);
    unsigned int bin300  = (time(0) % 300);
    unsigned int bin60   = (time(0) % 60);
    unsigned int bin5    = (time(0) % 5);

    avg3600[(bin3600+1)%3600] = 0;
    avg3600[bin3600] += val;

    avg300[(bin300+1)%300] = 0;
    avg300[bin300] += val;

    avg60[(bin60+1)%60] = 0;
    avg60[bin60] += val;

    avg5[(bin5+1)%5] = 0;
    avg5[bin5] += val;
  }

  void StampZero() {
    unsigned int bin3600 = (time(0) % 3600);
    unsigned int bin300  = (time(0) % 300);
    unsigned int bin60   = (time(0) % 60);
    unsigned int bin5    = (time(0) % 5);

    avg3600[(bin3600+1)%3600] = 0;
    avg300[(bin300+1)%300] = 0;
    avg60[(bin60+1)%60] = 0;
    avg5[(bin5+1)%5] = 0;
  }


  double GetAvg3600() {
    double sum=0;
    for (int i=0; i< 3600; i++) 
      sum += avg3600[i];
    return (sum / 3599);
  }

  double GetAvg300() {
    double sum=0;
    for (int i=0; i< 300; i++) 
      sum += avg300[i];
    return (sum / 299);
  }

  double GetAvg60() {
    double sum=0;
    for (int i=0; i< 60; i++) 
      sum += avg60[i];
    return (sum / 59);
  }

  double GetAvg5() {
    double sum=0;
    for (int i=0; i< 5; i++) 
      sum += avg5[i];
    return (sum / 4);
  }
};

class StatExt {
public:
  unsigned long n3600[3600];
  unsigned long n300[300];
  unsigned long n60[60];
  unsigned long n5[5];
  double sum3600[3600];
  double sum300[300];
  double sum60[60];
  double sum5[5];
  double min3600[3600];
  double min300[300];
  double min60[60];
  double min5[5];
  double max3600[3600];
  double max300[300];
  double max60[60];
  double max5[5];

  StatExt() {
    memset(n3600,0, sizeof(n3600));
    memset(n300,0,sizeof(n300));
    memset(n60,0,sizeof(n60));
    memset(n5,0,sizeof(n5));
    for(int k=0;k<3600;k++)  {
        min3600[k]=std::numeric_limits<long long>::max();
        max3600[k]=std::numeric_limits<size_t>::min();
        sum3600[k]=0;
    }
    for(int k=0;k<300;k++)  {
        min300[k]=std::numeric_limits<long long>::max();
        max300[k]=std::numeric_limits<size_t>::min();
        sum300[k]=0;
    }
    for(int k=0;k<60;k++)  {
        min60[k]=std::numeric_limits<long long>::max();
        max60[k]=std::numeric_limits<size_t>::min();
        sum60[k]=0;
    }
    for(int k=0;k<5;k++)  {
        min5[k]=std::numeric_limits<long long>::max();
        max5[k]=std::numeric_limits<size_t>::min();
        sum5[k]=0;
    }
  }

  ~StatExt(){};

  void Insert(unsigned long nsample, const double &avgv, const double &minv, const double &maxv) {
    unsigned int bin3600 = (time(0) % 3600);
    unsigned int bin300  = (time(0) % 300);
    unsigned int bin60   = (time(0) % 60);
    unsigned int bin5    = (time(0) % 5);

    n3600[(bin3600+1)%3600] = 0;
    n3600[bin3600] += nsample;
    sum3600[(bin3600+1)%3600] = 0;
    sum3600[bin3600] += avgv*nsample;
    min3600[(bin3600+1)%3600] = std::numeric_limits<long long>::max();
    min3600[bin3600] = std::min(min3600[bin3600],minv);
    max3600[(bin3600+1)%3600] = std::numeric_limits<size_t>::min();
    max3600[bin3600] = std::max(max3600[bin3600],maxv);

    n300[(bin300+1)%300] = 0;
    n300[bin300] += nsample;
    sum300[(bin300+1)%300] = 0;
    sum300[bin300] += avgv*nsample;
    min300[(bin300+1)%300] = std::numeric_limits<long long>::max();
    min300[bin300] = std::min(min300[bin300],minv);
    max300[(bin300+1)%300] = std::numeric_limits<size_t>::min();
    max300[bin300] = std::max(max300[bin300],maxv);

    n60[(bin60+1)%60] = 0;
    n60[bin60] += nsample;
    sum60[(bin60+1)%60] = 0;
    sum60[bin60] += avgv*nsample;
    min60[(bin60+1)%60] = std::numeric_limits<long long>::max();
    min60[bin60] = std::min(min60[bin60],minv);
    max60[(bin60+1)%60] = std::numeric_limits<size_t>::min();
    max60[bin60] = std::max(max60[bin60],maxv);

    n5[(bin5+1)%5] = 0;
    n5[bin5] += nsample;
    sum5[(bin5+1)%5] = 0;
    sum5[bin5] += avgv*nsample;
    min5[(bin5+1)%5] = std::numeric_limits<long long>::max();
    min5[bin5] = std::min(min5[bin5],minv);
    max5[(bin5+1)%5] = std::numeric_limits<size_t>::min();
    max5[bin5] = std::max(max5[bin5],maxv);
  }

  void StampZero() {
    unsigned int bin3600 = (time(0) % 3600);
    unsigned int bin300  = (time(0) % 300);
    unsigned int bin60   = (time(0) % 60);
    unsigned int bin5    = (time(0) % 5);

    n3600[(bin3600+1)%3600] = 0;
    n300[(bin300+1)%300] = 0;
    n60[(bin60+1)%60] = 0;
    n5[(bin5+1)%5] = 0;
    sum3600[(bin3600+1)%3600] = 0;
    sum300[(bin300+1)%300] = 0;
    sum60[(bin60+1)%60] = 0;
    sum5[(bin5+1)%5] = 0;
    min3600[(bin3600+1)%3600] = std::numeric_limits<long long>::max();
    min300[(bin300+1)%300] = std::numeric_limits<long long>::max();
    min60[(bin60+1)%60] = std::numeric_limits<long long>::max();
    min5[(bin5+1)%5] = std::numeric_limits<long long>::max();
    max3600[(bin3600+1)%3600] = std::numeric_limits<size_t>::min();
    max300[(bin300+1)%300] = std::numeric_limits<size_t>::min();
    max60[(bin60+1)%60] = std::numeric_limits<size_t>::min();
    max5[(bin5+1)%5] = std::numeric_limits<size_t>::min();
  }

  double GetN3600() {
    unsigned long sum=0;
    for (int i=0; i< 3600; i++) {
        sum += n3600[i];
    }
    return (double)sum;
  }

  double GetAvg3600() {
    double sum=0; double n=0;
    for (int i=0; i< 3600; i++) {
        n += n3600[i];
      sum += sum3600[i];
    }
    return (sum / n);
  }

  double GetMin3600() {
    double minval=std::numeric_limits<long long>::max();
    for (int i=0; i< 3600; i++)
      minval = std::min( min3600[i] , minval );
    return double(minval);
  }

  double GetMax3600() {
    double maxval=std::numeric_limits<size_t>::min();
    for (int i=0; i< 3600; i++)
      maxval = std::max( max3600[i] , maxval );
    return double(maxval);
  }

  double GetN300() {
    unsigned long sum=0;
    for (int i=0; i< 300; i++) {
        sum += n300[i];
    }
    return (double)sum;
  }

  double GetAvg300() {
    double sum=0; double n=0;
    for (int i=0; i< 300; i++) {
        n += n300[i];
      sum += sum300[i];
    }
    return (sum / n);
  }

  double GetMin300() {
    double minval=std::numeric_limits<long long>::max();
    for (int i=0; i< 300; i++)
      minval = std::min( min300[i] , minval );
    return double(minval);
  }

  double GetMax300() {
    double maxval=std::numeric_limits<size_t>::min();
    for (int i=0; i< 300; i++)
      maxval = std::max( max300[i] , maxval );
    return double(maxval);
  }

  double GetN60() {
    unsigned long sum=0;
    for (int i=0; i< 60; i++) {
        sum += n60[i];
    }
    return (double)sum;
  }

  double GetAvg60() {
    double sum=0; double n=0;
    for (int i=0; i< 60; i++) {
        n += n60[i];
      sum += sum60[i];
    }
    return (sum / n);
  }

  double GetMin60() {
    double minval=std::numeric_limits<long long>::max();
    for (int i=0; i< 60; i++)
      minval = std::min( min60[i] , minval );
    return double(minval);
  }

  double GetMax60() {
    double maxval=std::numeric_limits<size_t>::min();
    for (int i=0; i< 60; i++)
      maxval = std::max( max60[i] , maxval );
    return double(maxval);
  }

  double GetN5() {
    unsigned long sum=0;
    for (int i=0; i< 5; i++) {
        sum += n5[i];
    }
    return (double)sum;
  }

  double GetAvg5() {
    double sum=0; double n=0;
    for (int i=0; i< 5; i++) {
        n += n5[i];
      sum += sum5[i];
    }
    return (sum / n);
  }

  double GetMin5() {
    double minval=std::numeric_limits<long long>::max();
    for (int i=0; i< 5; i++)
      minval = std::min( min5[i] , minval );
    return double(minval);
  }

  double GetMax5() {
    double maxval=std::numeric_limits<size_t>::min();
    for (int i=0; i< 5; i++)
      maxval = std::max( max5[i] , maxval );
    return double(maxval);
  }

};


#define EXEC_TIMING_BEGIN(__ID__)               \
  struct timeval start__ID__;                   \
  struct timeval stop__ID__;                    \
  struct timezone tz__ID__;                     \
  gettimeofday(&start__ID__, &tz__ID__);                                        

#define EXEC_TIMING_END(__ID__)                                         \
  gettimeofday(&stop__ID__, &tz__ID__);                                 \
  gOFS->MgmStats.AddExec(__ID__, ((stop__ID__.tv_sec-start__ID__.tv_sec)*1000.0) + ((stop__ID__.tv_usec-start__ID__.tv_usec)/1000.0) );

class Stat {
public:
  XrdSysMutex Mutex;

  // first is name of value, then the map
  google::sparse_hash_map<std::string, google::sparse_hash_map<uid_t, unsigned long long> > StatsUid;
  google::sparse_hash_map<std::string, google::sparse_hash_map<gid_t, unsigned long long> > StatsGid;
  google::sparse_hash_map<std::string, google::sparse_hash_map<uid_t, StatAvg> > StatAvgUid;
  google::sparse_hash_map<std::string, google::sparse_hash_map<gid_t, StatAvg> > StatAvgGid;
  google::sparse_hash_map<std::string, google::sparse_hash_map<uid_t, StatExt> > StatExtUid;
  google::sparse_hash_map<std::string, google::sparse_hash_map<gid_t, StatExt> > StatExtGid;
  google::sparse_hash_map<std::string, std::deque<float> > StatExec;

  void Add(const char* tag, uid_t uid, gid_t gid, unsigned long val);

  void AddExt(const char* tag, uid_t uid, gid_t gid, unsigned long nsample, const double &avgv, const double &minv, const double &maxv);

  void AddExec(const char* tag, float exectime);

  unsigned long long GetTotal(const char* tag);

  // warning: you have to lock the mutex if directly used
  double GetTotalAvg3600(const char* tag);
  double GetTotalNExt3600(const char* tag);
  double GetTotalAvgExt3600(const char* tag);
  double GetTotalMinExt3600(const char* tag);
  double GetTotalMaxExt3600(const char* tag);

  // warning: you have to lock the mutex if directly used
  double GetTotalAvg300(const char* tag);
  double GetTotalNExt300(const char* tag);
  double GetTotalAvgExt300(const char* tag);
  double GetTotalMinExt300(const char* tag);
  double GetTotalMaxExt300(const char* tag);

  // warning: you have to lock the mutex if directly used
  double GetTotalAvg60(const char* tag);
  double GetTotalNExt60(const char* tag);
  double GetTotalAvgExt60(const char* tag);
  double GetTotalMinExt60(const char* tag);
  double GetTotalMaxExt60(const char* tag);

  // warning: you have to lock the mutex if directly used
  double GetTotalAvg5(const char* tag);
  double GetTotalNExt5(const char* tag);
  double GetTotalAvgExt5(const char* tag);
  double GetTotalMinExt5(const char* tag);
  double GetTotalMaxExt5(const char* tag);
  
  // warning: you have to lock the mutex if directly used
  double GetExec(const char* tag, double &deviation);

  // warning: you have to lock the mutex if directly used
  double GetTotalExec(double &deviation);

  void Clear();

  void PrintOutTotal(XrdOucString &out, bool details=false, bool monitoring=false, bool numerical=false);

  void Circulate();
};

EOSMGMNAMESPACE_END

#endif
