/*----------------------------------------------------------------------------*/
#include "common/Mapping.hh"
#include "mgm/Stat.hh"
#include "mq/XrdMqSharedObject.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN
/*----------------------------------------------------------------------------*/
void 
Stat::Add(const char* tag, uid_t uid, gid_t gid, unsigned long val) 
{
  Mutex.Lock();
  StatsUid[tag][uid] += val;
  StatsGid[tag][gid] += val;
  StatAvgUid[tag][uid].Add(val);
  StatAvgGid[tag][gid].Add(val);
  Mutex.UnLock();
}

/*----------------------------------------------------------------------------*/
void 
Stat::AddExec(const char* tag, float exectime) 
{
  Mutex.Lock();
  StatExec[tag].push_back(exectime);
  // we average over 100 entries
  if (StatExec[tag].size() > 100) {
    StatExec[tag].pop_front();
  } 
  Mutex.UnLock();
}

/*----------------------------------------------------------------------------*/
unsigned long long 
Stat::GetTotal(const char* tag) 
{
  google::sparse_hash_map<uid_t, unsigned long long>::const_iterator it;
  unsigned long long val=0;
  if (!StatsUid.count(tag))
    return 0;
  for (it=StatsUid[tag].begin(); it!= StatsUid[tag].end(); ++it) {
    val += it->second;
  }
  return val;
}

/*----------------------------------------------------------------------------*/
// warning: you have to lock the mutex if directly used
double 
Stat::GetTotalAvg3600(const char* tag) 
{
  google::sparse_hash_map<uid_t, StatAvg>::iterator it;
  double val = 0;
  if (!StatAvgUid.count(tag))
    return 0;
  for (it=StatAvgUid[tag].begin(); it!= StatAvgUid[tag].end(); ++it) {
    val += it->second.GetAvg3600();
  }
  return val;
}

/*----------------------------------------------------------------------------*/
// warning: you have to lock the mutex if directly used
double 
Stat::GetTotalAvg300(const char* tag)  
{
  google::sparse_hash_map<uid_t, StatAvg>::iterator it;
  double val = 0;
  if (!StatAvgUid.count(tag))
    return 0;
  for (it=StatAvgUid[tag].begin(); it!= StatAvgUid[tag].end(); ++it) {
    val += it->second.GetAvg300();
  }
  return val;
}

/*----------------------------------------------------------------------------*/
// warning: you have to lock the mutex if directly used
double 
Stat::GetTotalAvg60(const char* tag)   
{
  google::sparse_hash_map<uid_t, StatAvg>::iterator it;
  
  double val = 0;
  if (!StatAvgUid.count(tag))
    return 0;
  for (it=StatAvgUid[tag].begin(); it!= StatAvgUid[tag].end(); ++it) {
    val += it->second.GetAvg60();
  }
  return val;
}

/*----------------------------------------------------------------------------*/
// warning: you have to lock the mutex if directly used
double 
Stat::GetTotalAvg5(const char* tag)    
{
  google::sparse_hash_map<uid_t, StatAvg>::iterator it;
  double val = 0;
  if (!StatAvgUid.count(tag))
    return 0;
  for (it=StatAvgUid[tag].begin(); it!= StatAvgUid[tag].end(); ++it) {
    val += it->second.GetAvg5();
  }
  return val;
}

/*----------------------------------------------------------------------------*/
// warning: you have to lock the mutex if directly used
double 
Stat::GetExec(const char* tag, double &deviation) 
{
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

/*----------------------------------------------------------------------------*/
// warning: you have to lock the mutex if directly used
double 
Stat::GetTotalExec(double &deviation) 
{
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

/*----------------------------------------------------------------------------*/
void 
Stat::PrintOutTotal(XrdOucString &out, bool details, bool monitoring, bool numerical)
{
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
    out +="# -----------------------------------------------------------------------------------------------------------\n";
    sprintf(outline,"%-8s %-32s %-9s %8s %8s %8s %8s %-8s +- %-10s","who", "command","sum","5s","1min","5min","1h","exec(ms)","sigma(ms)");
    out += outline;
    out += "\n";
    out +="# -----------------------------------------------------------------------------------------------------------\n";
  } else {
    sprintf(outline,"uid=all gid=all total.exec.avg=%.02f total.exec.sigma=%.02f\n", avg,sig);
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
      sprintf(outline,"ALL        %-32s %12llu %8s %8s %8s %8s %8s +- %-10s\n",tag, GetTotal(tag),a5,a60,a300,a3600, aexec, aexecsig);
    } else {
      sprintf(outline,"uid=all gid=all cmd=%s total=%llu 5s=%s 60s=%s 300s=%s 3600s=%s exec=%f execsig=%f\n",tag, GetTotal(tag),a5,a60,a300,a3600,avg,sig);
    }
    out += outline;
  }
  if (details) {
    if (!monitoring) {
      out +="# -----------------------------------------------------------------------------------------------------------\n";
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
        
        char identifier[1024];
        if (numerical) {
          snprintf(identifier, 1023, "uid=%d", it->first);
        } else {
          int terrc=0;
          std::string username = eos::common::Mapping::UidToUserName(it->first, terrc);
          if (monitoring)
            snprintf(identifier, 1023, "uid=%s", username.c_str());
          else
            snprintf(identifier, 1023, "%s", username.c_str());
        }
        
        if (!monitoring) {
          sprintf(outline,"%-10s %-32s %12llu %8s %8s %8s %8s\n",identifier, tuit->first.c_str(),StatsUid[tuit->first.c_str()][it->first],a5,a60,a300,a3600);
        } else {
          sprintf(outline,"%s cmd=%s total=%llu 5s=%s 60s=%s 300s=%s 3600s=%s\n",identifier, tuit->first.c_str(),StatsUid[tuit->first.c_str()][it->first],a5,a60,a300,a3600);
        }
        
        uidout.push_back(outline);
      }
    }
    std::sort(uidout.begin(),uidout.end());
    std::vector<std::string>::iterator sit;
    for (sit = uidout.begin(); sit != uidout.end(); sit++) 
      out += sit->c_str();
    
    if (!monitoring) {
      out +="# --------------------------------------------------------------------------------------\n";
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
        
        char identifier[1024];
        if (numerical) {
          snprintf(identifier, 1023, "gid=%d", it->first);
        } else {
          int terrc=0;
          std::string groupname = eos::common::Mapping::GidToGroupName(it->first, terrc);
          if (monitoring)
            snprintf(identifier, 1023, "gid=%s", groupname.c_str());
          else
            snprintf(identifier, 1023, "%s", groupname.c_str());
        }

        if (!monitoring) {
          sprintf(outline,"%-10s %-32s %12llu %8s %8s %8s %8s\n",identifier, tgit->first.c_str(),StatsGid[tgit->first.c_str()][it->first],a5,a60,a300,a3600);
        } else {
          sprintf(outline,"%s cmd=%s total=%llu 5s=%s 60s=%s 300s=%s 3600s=%s\n",identifier, tgit->first.c_str(),StatsUid[tgit->first.c_str()][it->first],a5,a60,a300,a3600);
          
        }
        gidout.push_back(outline);
      }
    }
    std::sort(gidout.begin(),gidout.end());
    for (sit = gidout.begin(); sit != gidout.end(); sit++) 
      out += sit->c_str();
    if (!monitoring) {
      out +="# --------------------------------------------------------------------------------------\n";
    }
  }
  Mutex.UnLock();
}

/*----------------------------------------------------------------------------*/
void 
Stat::Circulate() 
{
  unsigned long long l1=0;
  unsigned long long l2=0;
  unsigned long long l3=0;
  unsigned long long l1tmp,l2tmp,l3tmp;
  // empty the circular buffer and extract some Mq statistic values
  while(1) {
    usleep(512345);
    // --------------------------------------------
    // mq statistics extraction
    l1tmp = XrdMqSharedHash::SetCounter;
    l2tmp = XrdMqSharedHash::SetNLCounter;
    l3tmp = XrdMqSharedHash::GetCounter;

    Add("HashSet"  ,0,0,l1tmp-l1);
    Add("HashSetNoLock",0,0,l2tmp-l2);
    Add("HashGet"  ,0,0,l3tmp-l3);

    l1 = l1tmp;
    l2 = l2tmp;
    l3 = l3tmp;
    // --------------------------------------------

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

EOSMGMNAMESPACE_END
