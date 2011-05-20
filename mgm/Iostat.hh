#ifndef __EOSMGM_EGROUP__HH__
#define __EOSMGM_EGROUP__HH__

/*----------------------------------------------------------------------------*/
#include "mgm/Namespace.hh"
#include "mq/XrdMqClient.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/
#include <google/sparse_hash_map>
#include <sys/types.h>
#include <string>
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN
class IostatAvg {
public:
  unsigned long avg3600[60];
  unsigned long avg300[60];
  unsigned long avg60[60];

  IostatAvg() {
    memset(avg3600,0, sizeof(avg3600));
    memset(avg300,0,sizeof(avg300));
    memset(avg60,0,sizeof(avg60));
  }

  ~IostatAvg(){};

  void Add(unsigned long val, time_t starttime, time_t stoptime) {
    time_t now = time(0);
    
    size_t tdiff = stoptime-starttime;

    size_t toff  = now - stoptime;

    if (toff < 3600) {
      // if the measurements was done in the last 3600 seconds
      float mbins = tdiff / 60; // number of bins the measurement was hitting
      unsigned long norm_val = mbins?(1.0*val/mbins):val;

      for (size_t bins = 0; bins < mbins; bins++) {
        unsigned int bin3600 = ( ((stoptime-(bins*60))/60)% 60);
        avg3600[bin3600] += norm_val;
      }
      avg3600[((stoptime/60)+1)%60] = 0;
    }

    if (toff < 300) {
      // if the measurements was done in the last 3600 seconds
      float mbins = tdiff / 5; // number of bins the measurement was hitting
      unsigned long norm_val = mbins?(1.0*val/mbins):val;

      for (size_t bins = 0; bins < mbins; bins++) {
        unsigned int bin300 = ( ((stoptime-(bins*5))/5)% 60);
        avg300[bin300] += norm_val;
      }
      avg300[((stoptime/5)+1)%60] = 0;
    }

    if (toff < 60) {
      // if the measurements was done in the last 3600 seconds
      float mbins = tdiff / 1 ; // number of bins the measurement was hitting
      unsigned long norm_val = mbins?(1.0*val/mbins):val;

      for (size_t bins = 0; bins < mbins; bins++) {
        unsigned int bin60 = ( ((stoptime-(bins*1))/1)% 60);
        avg60[bin60] += norm_val;
      }
      avg60[((stoptime/1)+1)%60] = 0;
    }
  }

  void StampZero() {
    unsigned int bin3600 = (time(0) / 60);
    unsigned int bin300  = (time(0) / 5);
    unsigned int bin60   = (time(0) / 1);

    avg3600[(bin3600+1)%60] = 0;
    avg300[(bin300+1)%60] = 0;
    avg60[(bin60+1)%60] = 0;
  }


  double GetAvg3600() {
    double sum=0;
    for (int i=0; i< 60; i++) 
      sum += avg3600[i];
    //    return (sum / 59);
    return sum;
  }

  double GetAvg300() {
    double sum=0;
    for (int i=0; i< 60; i++) 
      sum += avg300[i];
    //    return (sum / 59);
    return sum;
  }

  double GetAvg60() {
    double sum=0;
    for (int i=0; i< 60; i++) 
      sum += avg60[i];
    // return (sum / 59);
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

public:
  pthread_t thread;
  bool mRunning;
  bool mInit;
  XrdMqClient mClient;

  Iostat();
  ~Iostat();

  bool Start();
  bool Stop();

  void PrintOut(XrdOucString &out, bool details, bool monitoring);

  static void* StaticReceive(void*);
  void* Receive();

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

  void Circulate() {
    // empty the circular buffer 
    Mutex.Lock();
    google::sparse_hash_map<std::string, google::sparse_hash_map<uid_t, IostatAvg> >::iterator tit;
    // loop over tags
    for (tit = IostatAvgUid.begin(); tit != IostatAvgUid.end(); ++tit) {
      // loop over vids
      google::sparse_hash_map<uid_t, IostatAvg>::iterator it;
      for (it = tit->second.begin(); it != tit->second.end(); ++it) {
        it->second.StampZero();
      }
    }
    
    for (tit = IostatAvgGid.begin(); tit != IostatAvgGid.end(); ++tit) {
      // loop over vids
      google::sparse_hash_map<uid_t, IostatAvg>::iterator it;
      for (it = tit->second.begin(); it != tit->second.end(); ++it) {
        it->second.StampZero();
      }
      }
    Mutex.UnLock();
  }
};

EOSMGMNAMESPACE_END

#endif
