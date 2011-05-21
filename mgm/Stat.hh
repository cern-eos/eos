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


#define EXEC_TIMING_BEGIN(__ID__)					\
  struct timeval start__ID__;						\
  struct timeval stop__ID__;						\
  struct timezone tz__ID__;						\
  gettimeofday(&start__ID__, &tz__ID__);					

#define EXEC_TIMING_END(__ID__)			\
  gettimeofday(&stop__ID__, &tz__ID__);		\
  gOFS->MgmStats.AddExec(__ID__, ((stop__ID__.tv_sec-start__ID__.tv_sec)*1000.0) + ((stop__ID__.tv_usec-start__ID__.tv_usec)/1000.0) );

class Stat {
public:
  XrdSysMutex Mutex;

  // first is name of value, then the map
  google::sparse_hash_map<std::string, google::sparse_hash_map<uid_t, unsigned long long> > StatsUid;
  google::sparse_hash_map<std::string, google::sparse_hash_map<gid_t, unsigned long long> > StatsGid;
  google::sparse_hash_map<std::string, google::sparse_hash_map<uid_t, StatAvg> > StatAvgUid;
  google::sparse_hash_map<std::string, google::sparse_hash_map<gid_t, StatAvg> > StatAvgGid;
  google::sparse_hash_map<std::string, std::deque<float> > StatExec;

  void Add(const char* tag, uid_t uid, gid_t gid, unsigned long val);

  void AddExec(const char* tag, float exectime);

  unsigned long long GetTotal(const char* tag);

  // warning: you have to lock the mutex if directly used
  double GetTotalAvg3600(const char* tag);

  // warning: you have to lock the mutex if directly used
  double GetTotalAvg300(const char* tag);

  // warning: you have to lock the mutex if directly used
  double GetTotalAvg60(const char* tag);

  // warning: you have to lock the mutex if directly used
  double GetTotalAvg5(const char* tag);
  
  // warning: you have to lock the mutex if directly used
  double GetExec(const char* tag, double &deviation);

  // warning: you have to lock the mutex if directly used
  double GetTotalExec(double &deviation);

  void PrintOutTotal(XrdOucString &out, bool details=false, bool monitoring=false, bool numerical=false);

  void Circulate();
};

EOSMGMNAMESPACE_END

#endif
