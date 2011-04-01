#ifndef __EOSMGM_MGMOFSSTAT__HH__
#define __EOSMGM_MGMOFSSTAT__HH__

/*----------------------------------------------------------------------------*/
#include "mgm/Namespace.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucHash.hh"
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/
#include <google/sparse_hash_map>
/*----------------------------------------------------------------------------*/
#include <vector>
#include <map>
#include <string>

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

class Stat {
public:
  XrdSysMutex Mutex;

  // first is name of value, then the map
  google::sparse_hash_map<std::string, google::sparse_hash_map<uid_t, unsigned long long> > StatsUid;
  google::sparse_hash_map<std::string, google::sparse_hash_map<gid_t, unsigned long long> > StatsGid;
  google::sparse_hash_map<std::string, google::sparse_hash_map<uid_t, StatAvg> > StatAvgUid;
  google::sparse_hash_map<std::string, google::sparse_hash_map<gid_t, StatAvg> > StatAvgGid;

  void Add(const char* tag, uid_t uid, gid_t gid, unsigned long val) {
    Mutex.Lock();
    StatsUid[tag][uid] += val;
    StatsGid[tag][gid] += val;
    StatAvgUid[tag][uid].Add(val);
    StatAvgGid[tag][gid].Add(val);
    Mutex.UnLock();
  }

  unsigned long long GetTotal(const char* tag) {
    google::sparse_hash_map<uid_t, unsigned long long>::const_iterator it;
    unsigned long long val=0;
    if (!StatsUid.count(tag))
      return 0;
    for (it=StatsUid[tag].begin(); it!= StatsUid[tag].end(); ++it) {
      val += it->second;
    }
    return val;
  }

  // warning: you have to lock the mutex if directly used
  double GetTotalAvg3600(const char* tag) {
    google::sparse_hash_map<uid_t, StatAvg>::iterator it;
    double val = 0;
    if (!StatAvgUid.count(tag))
      return 0;
    for (it=StatAvgUid[tag].begin(); it!= StatAvgUid[tag].end(); ++it) {
      val += it->second.GetAvg3600();
    }
    return val;
  }

  // warning: you have to lock the mutex if directly used
  double GetTotalAvg300(const char* tag)  {
    google::sparse_hash_map<uid_t, StatAvg>::iterator it;
    double val = 0;
    if (!StatAvgUid.count(tag))
      return 0;
    for (it=StatAvgUid[tag].begin(); it!= StatAvgUid[tag].end(); ++it) {
      val += it->second.GetAvg300();
    }
    return val;
  }

  // warning: you have to lock the mutex if directly used
  double GetTotalAvg60(const char* tag)   {
    google::sparse_hash_map<uid_t, StatAvg>::iterator it;

    double val = 0;
    if (!StatAvgUid.count(tag))
      return 0;
    for (it=StatAvgUid[tag].begin(); it!= StatAvgUid[tag].end(); ++it) {
      val += it->second.GetAvg60();
    }
    return val;
  }
  
  // warning: you have to lock the mutex if directly used
  double GetTotalAvg5(const char* tag)    {
    google::sparse_hash_map<uid_t, StatAvg>::iterator it;
    double val = 0;
    if (!StatAvgUid.count(tag))
      return 0;
    for (it=StatAvgUid[tag].begin(); it!= StatAvgUid[tag].end(); ++it) {
      val += it->second.GetAvg5();
    }
    return val;
  }

  void PrintOutTotal(XrdOucString &out, bool details=false, bool monitoring=false) {
    Mutex.Lock();
    std::vector<std::string> tags;
    std::vector<std::string>::iterator it;

    google::sparse_hash_map<std::string, google::sparse_hash_map<uid_t, unsigned long long> >::iterator tit;

    for (tit = StatsUid.begin(); tit != StatsUid.end(); tit++) {
      tags.push_back(tit->first);
    }

    std::sort(tags.begin(),tags.end());
    
    char outline[1024];

    if (!monitoring) {
      sprintf(outline,"%-8s %-32s %-7s %8s %8s %8s %8s","who", "command","sum","5s","1min","5min","1h");
      out += outline;
      out += "\n";
      out +="# ------------------------------------------------------------------------------------\n";
    }
    for (it = tags.begin(); it!= tags.end(); ++it) {

      const char* tag = it->c_str();

      char a5[1024];
      char a60[1024];
      char a300[1024];
      char a3600[1024];
      sprintf(a5,"%3.02f", GetTotalAvg5(tag));
      sprintf(a60,"%3.02f", GetTotalAvg60(tag));
      sprintf(a300,"%3.02f", GetTotalAvg300(tag));
      sprintf(a3600,"%3.02f", GetTotalAvg3600(tag));
      if (!monitoring) {
	sprintf(outline,"ALL      %-32s %07llu %8s %8s %8s %8s\n",tag, GetTotal(tag),a5,a60,a300,a3600);
      } else {
	sprintf(outline,"all cmd=%s total=%llu 5s=%s 60s=%s 300s=%s 3600s=%s\n",tag, GetTotal(tag),a5,a60,a300,a3600);
      }
      out += outline;
    }
    if (details) {
      if (!monitoring) {
	out +="# ------------------------------------------------------------------------------------\n";
      }
      google::sparse_hash_map<std::string, google::sparse_hash_map<uid_t, StatAvg > >::iterator tuit;
      google::sparse_hash_map<std::string, google::sparse_hash_map<gid_t, StatAvg > >::iterator tgit;

      std::vector <std::string> uidout;
      std::vector <std::string> gidout;

      for (tuit = StatAvgUid.begin(); tuit != StatAvgUid.end(); tuit++) {
	google::sparse_hash_map<uid_t, StatAvg>::iterator it;
	for (it = tuit->second.begin(); it != tuit->second.end(); ++it) {
	  char a5[1024];
	  char a60[1024];
	  char a300[1024];
	  char a3600[1024];
	  sprintf(a5,"%3.02f", it->second.GetAvg5());
	  sprintf(a60,"%3.02f", it->second.GetAvg60());
	  sprintf(a300,"%3.02f", it->second.GetAvg300());
	  sprintf(a3600,"%3.02f", it->second.GetAvg3600());
	  if (!monitoring) {
	    sprintf(outline,"uid=%04d %-32s %07llu %8s %8s %8s %8s\n",it->first, tuit->first.c_str(),StatsUid[tuit->first.c_str()][it->first],a5,a60,a300,a3600);
	  } else {
	    sprintf(outline,"uid=%04d cmd=%s total=%llu 5s=%s 60s=%s 300s=%s 3600s=%s\n",it->first, tuit->first.c_str(),StatsUid[tuit->first.c_str()][it->first],a5,a60,a300,a3600);
	  }

	  uidout.push_back(outline);
	}
      }
      std::sort(uidout.begin(),uidout.end());
      std::vector<std::string>::iterator sit;
      for (sit = uidout.begin(); sit != uidout.end(); sit++) 
	out += sit->c_str();
      
      if (!monitoring) {
	out +="# ------------------------------------------------------------------------------------\n";
      }
      for (tgit = StatAvgGid.begin(); tgit != StatAvgGid.end(); tgit++) {
	google::sparse_hash_map<gid_t, StatAvg>::iterator it;
	for (it = tgit->second.begin(); it != tgit->second.end(); ++it) {
	  char a5[1024];
	  char a60[1024];
	  char a300[1024];
	  char a3600[1024];
	  sprintf(a5,"%3.02f", it->second.GetAvg5());
	  sprintf(a60,"%3.02f", it->second.GetAvg60());
	  sprintf(a300,"%3.02f", it->second.GetAvg300());
	  sprintf(a3600,"%3.02f", it->second.GetAvg3600());
	  if (!monitoring) {
	    sprintf(outline,"gid=%04d %-32s %07llu %8s %8s %8s %8s\n",it->first, tgit->first.c_str(),StatsGid[tgit->first.c_str()][it->first],a5,a60,a300,a3600);
	  } else {
	    sprintf(outline,"gid=%04d cmd=%s total=%llu 5s=%s 60s=%s 300s=%s 3600s=%s\n",it->first, tgit->first.c_str(),StatsUid[tgit->first.c_str()][it->first],a5,a60,a300,a3600);
		    
	  }
	  gidout.push_back(outline);
	}
      }
      std::sort(gidout.begin(),gidout.end());
      for (sit = gidout.begin(); sit != gidout.end(); sit++) 
	out += sit->c_str();
      if (!monitoring) {
	out +="# ------------------------------------------------------------------------------------\n";
      }
    }
    Mutex.UnLock();
  }

  void Circulate() {
    // empty the circular buffer 
    while(1) {
      usleep(512345);
      Mutex.Lock();
      google::sparse_hash_map<std::string, google::sparse_hash_map<uid_t, StatAvg> >::iterator tit;
      // loop over tags
      for (tit = StatAvgUid.begin(); tit != StatAvgUid.end(); ++tit) {
	// loop over vids
	google::sparse_hash_map<uid_t, StatAvg>::iterator it;
	for (it = tit->second.begin(); it != tit->second.end(); ++it) {
	  it->second.StampZero();
	}
      }

      for (tit = StatAvgGid.begin(); tit != StatAvgGid.end(); ++tit) {
	// loop over vids
	google::sparse_hash_map<uid_t, StatAvg>::iterator it;
	for (it = tit->second.begin(); it != tit->second.end(); ++it) {
	  it->second.StampZero();
	}
      }
      Mutex.UnLock();
    }
  }

};

EOSMGMNAMESPACE_END

#endif
