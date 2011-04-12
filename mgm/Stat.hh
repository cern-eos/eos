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

  void Add(const char* tag, uid_t uid, gid_t gid, unsigned long val) {
    Mutex.Lock();
    StatsUid[tag][uid] += val;
    StatsGid[tag][gid] += val;
    StatAvgUid[tag][uid].Add(val);
    StatAvgGid[tag][gid].Add(val);
    Mutex.UnLock();
  }

  void AddExec(const char* tag, float exectime) {
    Mutex.Lock();
    StatExec[tag].push_back(exectime);
    // we average over 100 entries
    if (StatExec[tag].size() > 100) {
      StatExec[tag].pop_front();
    } 
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

  // warning: you have to lock the mutex if directly used
  double GetExec(const char* tag, double &deviation) {
    // calculates average execution time for 'tag'
    if (StatExec.count(tag)) {
      std::deque<float>::const_iterator it;
      double sum=0;
      double avg=0;
      deviation=0;
      int cnt=0;
      for (it = StatExec[tag].begin(); it != StatExec[tag].end(); it++) {
	cnt++;
	sum += *it;
      }

      avg = sum /cnt;

      for (it = StatExec[tag].begin(); it != StatExec[tag].end(); it++) {
	deviation += pow( (*it-avg),2);
      }
      deviation = sqrt(deviation/cnt);
      return avg;
    }
    return 0;
  }

  // warning: you have to lock the mutex if directly used
  double GetTotalExec(double &deviation) {
    // calculates average execution time for all commands
    google::sparse_hash_map<std::string, std::deque<float> >::const_iterator ittag;

    double sum=0;
    double avg=0;
    deviation=0;
    int cnt=0;    

    for (ittag = StatExec.begin(); ittag != StatExec.end(); ittag ++) {
      std::deque<float>::const_iterator it;
      
      for (it = ittag->second.begin(); it != ittag->second.end(); it++) {
	cnt++;
	sum += *it;
      }
    }
    
    if (cnt)
      avg = sum /cnt;
    
    for (ittag = StatExec.begin(); ittag != StatExec.end(); ittag ++) {
      std::deque<float>::const_iterator it;
      for (it = ittag->second.begin(); it != ittag->second.end(); it++) {
	deviation += pow( (*it-avg),2);
      }
    }
    
    if (cnt)
      deviation = sqrt(deviation/cnt);
    return avg;
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

    double avg=0;
    double sig=0;
    avg = GetTotalExec(sig);
    
    if (!monitoring) {
      sprintf(outline,"%-8s %-32s %3.02f +- %3.02f\n","ALL", "Execution Time", avg,sig);
      out += outline;
      out +="# ---------------------------------------------------------------------------------------------------------\n";
      sprintf(outline,"%-8s %-32s %-7s %8s %8s %8s %8s %-8s +- %-10s","who", "command","sum","5s","1min","5min","1h","exec(ms)","sigma(ms)");
      out += outline;
      out += "\n";
      out +="# ---------------------------------------------------------------------------------------------------------\n";
    } else {
      sprintf(outline,"total.exec.avg=%.02f total.exec.sigma=%.02f\n", avg,sig);
      out += outline;
    }
    for (it = tags.begin(); it!= tags.end(); ++it) {

      const char* tag = it->c_str();

      char a5[1024];
      char a60[1024];
      char a300[1024];
      char a3600[1024];
      char aexec[1024];
      char aexecsig[1024];
      double avg=0;
      double sig=0;
      avg = GetExec(tag, sig);
      sprintf(a5,"%3.02f", GetTotalAvg5(tag));
      sprintf(a60,"%3.02f", GetTotalAvg60(tag));
      sprintf(a300,"%3.02f", GetTotalAvg300(tag));
      sprintf(a3600,"%3.02f", GetTotalAvg3600(tag));
      if (avg) 
	sprintf(aexec,"%3.02f", avg); 
      else 
	sprintf(aexec,"-NA-");
      if (sig)
	sprintf(aexecsig,"%3.02f", sig);
      else
	sprintf(aexecsig,"-NA-");

      if (!monitoring) {
	sprintf(outline,"ALL      %-32s %07llu %8s %8s %8s %8s %8s +- %-10s\n",tag, GetTotal(tag),a5,a60,a300,a3600, aexec, aexecsig);
      } else {
	sprintf(outline,"uid=all gid=all cmd=%s total=%llu 5s=%s 60s=%s 300s=%s 3600s=%s exec=%f execsig=%f\n",tag, GetTotal(tag),a5,a60,a300,a3600,avg,sig);
      }
      out += outline;
    }
    if (details) {
      if (!monitoring) {
	out +="# ---------------------------------------------------------------------------------------------------------\n";
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
