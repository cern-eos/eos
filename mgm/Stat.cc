// ----------------------------------------------------------------------
// File: Stat.cc
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

/*----------------------------------------------------------------------------*/
#include "common/Mapping.hh"
#include "mgm/Stat.hh"
#include "mgm/FsView.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mq/XrdMqSharedObject.hh"
#include "mgm/Quota.hh"
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
Stat::AddExt(const char* tag, uid_t uid, gid_t gid, unsigned long nsample, const double &avgv, const double &minv, const double &maxv)
{
  Mutex.Lock();
  StatExtUid[tag][uid].Insert(nsample,avgv,minv,maxv);
  StatExtGid[tag][gid].Insert(nsample,avgv,minv,maxv);
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
Stat::GetTotalNExt3600(const char* tag)
{
  google::sparse_hash_map<uid_t, StatExt>::iterator it;
  unsigned long n = 0;
  if ( !StatExtUid.count(tag))
    return 0;

  for (it=StatExtUid[tag].begin(); it!= StatExtUid[tag].end(); ++it) {
    for(int i=0;i<3600;i++) n += it->second.n3600[i];
  }
  return (double)n;
}

/*----------------------------------------------------------------------------*/
// warning: you have to lock the mutex if directly used
double
Stat::GetTotalAvgExt3600(const char* tag)
{
  google::sparse_hash_map<uid_t, StatExt>::iterator it;
  double val = 0; double totw = 0;
  if ( !StatExtUid.count(tag))
    return 0;

  for (it=StatExtUid[tag].begin(); it!= StatExtUid[tag].end(); ++it) {
    double w = 0;
    for(int i=0;i<3600;i++) w += it->second.n3600[i];
    totw += w;
    val  += it->second.GetAvg3600()*w;
  }
  return val/totw;
}

/*----------------------------------------------------------------------------*/
// warning: you have to lock the mutex if directly used
double
Stat::GetTotalMinExt3600(const char* tag)
{
  google::sparse_hash_map<uid_t, StatExt>::iterator it;
  double minval = std::numeric_limits<unsigned long>::max();
  if ( !StatExtUid.count(tag))
    return 0;

  for (it=StatExtUid[tag].begin(); it!= StatExtUid[tag].end(); ++it) {
    minval = std::min( minval , it->second.GetMin3600() );
  }

  return minval;
}

/*----------------------------------------------------------------------------*/
// warning: you have to lock the mutex if directly used
double
Stat::GetTotalMaxExt3600(const char* tag)
{
  google::sparse_hash_map<uid_t, StatExt>::iterator it;
  double maxval = std::numeric_limits<unsigned long>::min();
  if ( !StatExtUid.count(tag))
    return 0;

  for (it=StatExtUid[tag].begin(); it!= StatExtUid[tag].end(); ++it) {
    maxval = std::max( maxval , it->second.GetMax3600() );
  }

  return maxval;
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
Stat::GetTotalNExt300(const char* tag)
{
  google::sparse_hash_map<uid_t, StatExt>::iterator it;
  unsigned long n = 0;
  if ( !StatExtUid.count(tag))
    return 0;

  for (it=StatExtUid[tag].begin(); it!= StatExtUid[tag].end(); ++it) {
    for(int i=0;i<300;i++) n += it->second.n300[i];
  }
  return (double)n;
}

/*----------------------------------------------------------------------------*/
// warning: you have to lock the mutex if directly used
double 
Stat::GetTotalAvgExt300(const char* tag)
{
  google::sparse_hash_map<uid_t, StatExt>::iterator it;
  double val = 0; double totw = 0;
  if ( !StatExtUid.count(tag))
    return 0;

  for (it=StatExtUid[tag].begin(); it!= StatExtUid[tag].end(); ++it) {
    double w = 0;
    for(int i=0;i<300;i++) w += it->second.n300[i];
    totw += w;
    val  += it->second.GetAvg300()*w;
  }
  return val/totw;
}

/*----------------------------------------------------------------------------*/
// warning: you have to lock the mutex if directly used
double
Stat::GetTotalMinExt300(const char* tag)
{
  google::sparse_hash_map<uid_t, StatExt>::iterator it;
  double minval = std::numeric_limits<unsigned long>::max();
  if ( !StatExtUid.count(tag))
    return 0;

  for (it=StatExtUid[tag].begin(); it!= StatExtUid[tag].end(); ++it) {
    minval = std::min( minval , it->second.GetMin300() );
  }

  return minval;
}

/*----------------------------------------------------------------------------*/
// warning: you have to lock the mutex if directly used
double
Stat::GetTotalMaxExt300(const char* tag)
{
  google::sparse_hash_map<uid_t, StatExt>::iterator it;
  double maxval = std::numeric_limits<unsigned long>::min();
  if ( !StatExtUid.count(tag))
    return 0;

  for (it=StatExtUid[tag].begin(); it!= StatExtUid[tag].end(); ++it) {
    maxval = std::max( maxval , it->second.GetMax300() );
  }

  return maxval;
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
Stat::GetTotalNExt60(const char* tag)
{
  google::sparse_hash_map<uid_t, StatExt>::iterator it;
  unsigned long n = 0;
  if ( !StatExtUid.count(tag))
    return 0;

  for (it=StatExtUid[tag].begin(); it!= StatExtUid[tag].end(); ++it) {
    for(int i=0;i<60;i++) n += it->second.n60[i];
  }
  return (double)n;
}

/*----------------------------------------------------------------------------*/
// warning: you have to lock the mutex if directly used
double 
Stat::GetTotalAvgExt60(const char* tag)
{
  google::sparse_hash_map<uid_t, StatExt>::iterator it;
  double val = 0; double totw = 0;
  if ( !StatExtUid.count(tag))
    return 0;

  for (it=StatExtUid[tag].begin(); it!= StatExtUid[tag].end(); ++it) {
    double w = 0;
    for(int i=0;i<60;i++) w += it->second.n60[i];
    totw += w;
    val  += it->second.GetAvg60()*w;
  }
  return val/totw;
}

/*----------------------------------------------------------------------------*/
// warning: you have to lock the mutex if directly used
double
Stat::GetTotalMinExt60(const char* tag)
{
  google::sparse_hash_map<uid_t, StatExt>::iterator it;
  double minval = std::numeric_limits<unsigned long>::max();
  if ( !StatExtUid.count(tag))
    return 0;

  for (it=StatExtUid[tag].begin(); it!= StatExtUid[tag].end(); ++it) {
    minval = std::min( minval , it->second.GetMin60() );
  }

  return minval;
}

/*----------------------------------------------------------------------------*/
// warning: you have to lock the mutex if directly used
double
Stat::GetTotalMaxExt60(const char* tag)
{
  google::sparse_hash_map<uid_t, StatExt>::iterator it;
  double maxval = std::numeric_limits<unsigned long>::min();
  if ( !StatExtUid.count(tag))
    return 0;

  for (it=StatExtUid[tag].begin(); it!= StatExtUid[tag].end(); ++it) {
    maxval = std::max( maxval , it->second.GetMax60() );
  }

  return maxval;
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
Stat::GetTotalNExt5(const char* tag)
{
  google::sparse_hash_map<uid_t, StatExt>::iterator it;
  unsigned long n = 0;
  if ( !StatExtUid.count(tag))
    return 0;

  for (it=StatExtUid[tag].begin(); it!= StatExtUid[tag].end(); ++it) {
    for(int i=0;i<5;i++) n += it->second.n5[i];
  }
  return (double)n;
}

/*----------------------------------------------------------------------------*/
// warning: you have to lock the mutex if directly used
double 
Stat::GetTotalAvgExt5(const char* tag)
{
  google::sparse_hash_map<uid_t, StatExt>::iterator it;
  double val = 0; double totw = 0;
  if ( !StatExtUid.count(tag))
    return 0;

  for (it=StatExtUid[tag].begin(); it!= StatExtUid[tag].end(); ++it) {
    double w = 0;
    for(int i=0;i<5;i++) w += it->second.n5[i];
    totw += w;
    val  += it->second.GetAvg5()*w;
  }
  return val/totw;
}

/*----------------------------------------------------------------------------*/
// warning: you have to lock the mutex if directly used
double
Stat::GetTotalMinExt5(const char* tag)
{
  google::sparse_hash_map<uid_t, StatExt>::iterator it;
  double minval = std::numeric_limits<unsigned long>::max();
  if ( !StatExtUid.count(tag))
    return 0;

  for (it=StatExtUid[tag].begin(); it!= StatExtUid[tag].end(); ++it) {
    minval = std::min( minval , it->second.GetMin5() );
  }

  return minval;
}

/*----------------------------------------------------------------------------*/
// warning: you have to lock the mutex if directly used
double
Stat::GetTotalMaxExt5(const char* tag)
{
  google::sparse_hash_map<uid_t, StatExt>::iterator it;
  double maxval = std::numeric_limits<unsigned long>::min();
  if ( !StatExtUid.count(tag))
    return 0;

  for (it=StatExtUid[tag].begin(); it!= StatExtUid[tag].end(); ++it) {
    maxval = std::max( maxval , it->second.GetMax5() );
  }

  return maxval;
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
Stat::Clear()
{
  Mutex.Lock();
  google::sparse_hash_map<std::string, google::sparse_hash_map<uid_t, unsigned long long> >::iterator ittag;
  for (ittag = StatsUid.begin(); ittag != StatsUid.end(); ittag ++) {
    StatsUid[ittag->first].clear();
    StatsUid[ittag->first].resize(1000);
    StatsGid[ittag->first].clear();
    StatsGid[ittag->first].resize(1000);
    StatAvgUid[ittag->first].clear();
    StatAvgUid[ittag->first].resize(1000);
    StatAvgGid[ittag->first].clear();
    StatAvgGid[ittag->first].resize(1000);
    StatExec[ittag->first].resize(1000);
  }
  Mutex.UnLock();
}

/*----------------------------------------------------------------------------*/
void 
Stat::PrintOutTotal(XrdOucString &out, bool details, bool monitoring, bool numerical)
{
  Mutex.Lock();
  std::vector<std::string> tags,tags_ext;
  std::vector<std::string>::iterator it;
  
  google::sparse_hash_map<std::string, google::sparse_hash_map<uid_t, unsigned long long> >::iterator tit;
  google::sparse_hash_map<std::string, google::sparse_hash_map<uid_t, StatExt > >::iterator tit_ext;
  
  for (tit = StatsUid.begin(); tit != StatsUid.end(); tit++) {
    tags.push_back(tit->first);
  }
  for (tit_ext = StatExtUid.begin(); tit_ext != StatExtUid.end(); tit_ext++) {
    tags_ext.push_back(tit_ext->first);
  }

  std::sort(tags.begin(),tags.end());
  std::sort(tags_ext.begin(),tags_ext.end());
  
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
  for (it = tags_ext.begin(); it!= tags_ext.end(); ++it) {

    const char* tag = it->c_str();
    double nsample;
    const char na[9]="NA";
    char n5[1024],a5[1024],m5[1024],M5[1024];
    char n60[1024],a60[1024],m60[1024],M60[1024];
    char n300[1024],a300[1024],m300[1024],M300[1024];
    char n3600[1024],a3600[1024],m3600[1024],M3600[1024];
    if((nsample=GetTotalNExt5(tag))<1) {
        strcpy(a5,na); strcpy(m5,na); strcpy(M5,na); sprintf(n5,"%6.01e", nsample);
    }
    else {
      sprintf(n5,"%6.01e", nsample);
      sprintf(a5,"%6.01e", GetTotalAvgExt5(tag));
      sprintf(m5,"%6.01e", GetTotalMinExt5(tag));
      sprintf(M5,"%6.01e", GetTotalMaxExt5(tag));
    }
    if((nsample=GetTotalNExt60(tag))<1) {
      strcpy(a60,na); strcpy(m60,na); strcpy(M60,na); sprintf(n60,"%6.01e", nsample);
    }
    else {
      sprintf(n60,"%6.01e", nsample);
      sprintf(a60,"%6.01e", GetTotalAvgExt60(tag));
      sprintf(m60,"%6.01e", GetTotalMinExt60(tag));
      sprintf(M60,"%6.01e", GetTotalMaxExt60(tag));
    }
    if((nsample=GetTotalNExt300(tag))<1) {
      strcpy(a300,na); strcpy(m300,na); strcpy(M300,na); sprintf(n300,"%6.01e", nsample);
    }
    else {
      sprintf(n300,"%6.01e", nsample);
      sprintf(a300,"%6.01e", GetTotalAvgExt300(tag));
      sprintf(m300,"%6.01e", GetTotalMinExt300(tag));
      sprintf(M300,"%6.01e", GetTotalMaxExt300(tag));
    }
    if((nsample=GetTotalNExt3600(tag))<1) {
      strcpy(a3600,na); strcpy(m3600,na); strcpy(M3600,na); sprintf(n3600,"%6.01e", nsample);
    }
    else {
      sprintf(n3600,"%6.01e", GetTotalNExt3600(tag));
      sprintf(a3600,"%6.01e", GetTotalAvgExt3600(tag));
      sprintf(m3600,"%6.01e", GetTotalMinExt3600(tag));
      sprintf(M3600,"%6.01e", GetTotalMaxExt3600(tag));
    }

    if (details) {
      if (!monitoring) {
	sprintf(outline,"ALL        %-32s %12s %8s %8s %8s %8s\n",tag,"spl", n5,n60,n300,n3600); out+=outline;
	sprintf(outline,"ALL        %-32s %12s %8s %8s %8s %8s\n",tag,"min", m5,m60,m300,m3600); out+=outline;
	sprintf(outline,"ALL        %-32s %12s %8s %8s %8s %8s\n",tag,"avg", a5,a60,a300,a3600); out+=outline;
	sprintf(outline,"ALL        %-32s %12s %8s %8s %8s %8s\n",tag,"max", M5,M60,M300,M3600); out+=outline;
      } else {
	sprintf(outline,"uid=all gid=all cmd=%s:spl 5s=%s 60s=%s 300s=%s 3600s=%s\n",tag, n5,n60,n300,n3600); out+=outline;
	sprintf(outline,"uid=all gid=all cmd=%s:min 5s=%s 60s=%s 300s=%s 3600s=%s\n",tag, m5,m60,m300,m3600); out+=outline;
	sprintf(outline,"uid=all gid=all cmd=%s:avg 5s=%s 60s=%s 300s=%s 3600s=%s\n",tag, a5,a60,a300,a3600); out+=outline;
	sprintf(outline,"uid=all gid=all cmd=%s:max 5s=%s 60s=%s 300s=%s 3600s=%s\n",tag, M5,M60,M300,M3600); out+=outline;
      }
    }
  }
  if (details) {
    if (!monitoring) {
      out +="# -----------------------------------------------------------------------------------------------------------\n";
    }
    google::sparse_hash_map<std::string, google::sparse_hash_map<uid_t, StatAvg > >::iterator tuit;
    google::sparse_hash_map<std::string, google::sparse_hash_map<gid_t, StatAvg > >::iterator tgit;
    google::sparse_hash_map<std::string, google::sparse_hash_map<uid_t, StatExt > >::iterator tuit_ext;
    google::sparse_hash_map<std::string, google::sparse_hash_map<gid_t, StatExt > >::iterator tgit_ext;
    
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

    uidout.clear();
    for (tuit_ext = StatExtUid.begin(); tuit_ext != StatExtUid.end(); tuit_ext++) {
      google::sparse_hash_map<uid_t, StatExt>::iterator it;
      for (it = tuit_ext->second.begin(); it != tuit_ext->second.end(); ++it) {
          const char* tag = tuit_ext->first.c_str();
          double nsample;
          const char na[9]="NA";
          char n5[1024],a5[1024],m5[1024],M5[1024];
          char n60[1024],a60[1024],m60[1024],M60[1024];
          char n300[1024],a300[1024],m300[1024],M300[1024];
          char n3600[1024],a3600[1024],m3600[1024],M3600[1024];
          if((nsample=it->second.GetN5())<1) {
              strcpy(a5,na); strcpy(m5,na); strcpy(M5,na); sprintf(n5,"%6.01e", nsample);
          }
          else {
            sprintf(n5,"%6.01e", nsample);
            sprintf(a5,"%6.01e", it->second.GetAvg5());
            sprintf(m5,"%6.01e", it->second.GetMin5());
            sprintf(M5,"%6.01e", it->second.GetMax5());
          }
          if((nsample=it->second.GetN60())<1) {
              strcpy(a60,na); strcpy(m60,na); strcpy(M60,na); sprintf(n60,"%6.01e", nsample);
          }
          else {
            sprintf(n60,"%6.01e", nsample);
            sprintf(a60,"%6.01e", it->second.GetAvg60());
            sprintf(m60,"%6.01e", it->second.GetMin60());
            sprintf(M60,"%6.01e", it->second.GetMax60());
          }
          if((nsample=it->second.GetN300())<1) {
              strcpy(a300,na); strcpy(m300,na); strcpy(M300,na); sprintf(n300,"%6.01e", nsample);
          }
          else {
            sprintf(n300,"%6.01e", nsample);
            sprintf(a300,"%6.01e", it->second.GetAvg300());
            sprintf(m300,"%6.01e", it->second.GetMin300());
            sprintf(M300,"%6.01e", it->second.GetMax300());
          }
          if((nsample=it->second.GetN3600())<1) {
              strcpy(a3600,na); strcpy(m3600,na); strcpy(M3600,na); sprintf(n3600,"%6.01e", nsample);
          }
          else {
            sprintf(n3600,"%6.01e", nsample);
            sprintf(a3600,"%6.01e", it->second.GetAvg3600());
            sprintf(m3600,"%6.01e", it->second.GetMin3600());
            sprintf(M3600,"%6.01e", it->second.GetMax3600());
          }

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
            sprintf(outline,"%-10s %-32s %12s %8s %8s %8s %8s\n",identifier,tag,"spl", n5,n60,n300,n3600); out+=outline;
            sprintf(outline,"%-10s %-32s %12s %8s %8s %8s %8s\n",identifier,tag,"min", m5,m60,m300,m3600); out+=outline;
            sprintf(outline,"%-10s %-32s %12s %8s %8s %8s %8s\n",identifier,tag,"avg", a5,a60,a300,a3600); out+=outline;
            sprintf(outline,"%-10s %-32s %12s %8s %8s %8s %8s\n",identifier,tag,"max", M5,M60,M300,M3600); out+=outline;
          } else {
            sprintf(outline,"%s cmd=%s:spl 5s=%s 60s=%s 300s=%s 3600s=%s\n",identifier,tag, n5,n60,n300,n3600); out+=outline;
            sprintf(outline,"%s cmd=%s:min 5s=%s 60s=%s 300s=%s 3600s=%s\n",identifier,tag, m5,m60,m300,m3600); out+=outline;
            sprintf(outline,"%s cmd=%s:avg 5s=%s 60s=%s 300s=%s 3600s=%s\n",identifier,tag, a5,a60,a300,a3600); out+=outline;
            sprintf(outline,"%s cmd=%s:max 5s=%s 60s=%s 300s=%s 3600s=%s\n",identifier,tag, M5,M60,M300,M3600); out+=outline;
          }
      }
    }
    std::sort(uidout.begin(),uidout.end());
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

    gidout.clear();
    for (tgit_ext = StatExtGid.begin(); tgit_ext != StatExtGid.end(); tgit_ext++) {
      google::sparse_hash_map<gid_t, StatExt>::iterator it;
      for (it = tgit_ext->second.begin(); it != tgit_ext->second.end(); ++it) {
          double nsample;
          const char na[9]="NA";
          char n5[1024],a5[1024],m5[1024],M5[1024];
          char n60[1024],a60[1024],m60[1024],M60[1024];
          char n300[1024],a300[1024],m300[1024],M300[1024];
          char n3600[1024],a3600[1024],m3600[1024],M3600[1024];
          if((nsample=it->second.GetN5())<1) {
              strcpy(a5,na); strcpy(m5,na); strcpy(M5,na); sprintf(n5,"%6.01e", nsample);
          }
          else {
            sprintf(n5,"%6.01e", nsample);
            sprintf(a5,"%6.01e", it->second.GetAvg5());
            sprintf(m5,"%6.01e", it->second.GetMin5());
            sprintf(M5,"%6.01e", it->second.GetMax5());
          }
          if((nsample=it->second.GetN60())<1) {
              strcpy(a60,na); strcpy(m60,na); strcpy(M60,na); sprintf(n60,"%6.01e", nsample);
          }
          else {
            sprintf(n60,"%6.01e", nsample);
            sprintf(a60,"%6.01e", it->second.GetAvg60());
            sprintf(m60,"%6.01e", it->second.GetMin60());
            sprintf(M60,"%6.01e", it->second.GetMax60());
          }
          if((nsample=it->second.GetN300())<1) {
              strcpy(a300,na); strcpy(m300,na); strcpy(M300,na); sprintf(n300,"%6.01e", nsample);
          }
          else {
            sprintf(n300,"%6.01e", nsample);
            sprintf(a300,"%6.01e", it->second.GetAvg300());
            sprintf(m300,"%6.01e", it->second.GetMin300());
            sprintf(M300,"%6.01e", it->second.GetMax300());
          }
          if((nsample=it->second.GetN3600())<1) {
              strcpy(a3600,na); strcpy(m3600,na); strcpy(M3600,na); sprintf(n3600,"%6.01e", nsample);
          }
          else {
            sprintf(n3600,"%6.01e", nsample);
            sprintf(a3600,"%6.01e", it->second.GetAvg3600());
            sprintf(m3600,"%6.01e", it->second.GetMin3600());
            sprintf(M3600,"%6.01e", it->second.GetMax3600());
          }

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

#ifdef EOS_INSTRUMENTED_RWMUTEX
  unsigned long long qu1=0;
  unsigned long long qu2=0;
  unsigned long long ns1=0;
  unsigned long long ns2=0;
  unsigned long long view1=0;
  unsigned long long view2=0;
  eos::common::RWMutexTimingStats qu12stmp,ns12stmp,view12stmp;
  unsigned long long ns1tmp,ns2tmp,view1tmp,view2tmp,qu1tmp,qu2tmp;
#endif

  // empty the circular buffer and extract some Mq statistic values
  while(1) {
    XrdSysTimer sleeper;
    sleeper.Wait(512);

    // --------------------------------------------
    // mq statistics extraction
    l1tmp = AtomicGet(XrdMqSharedHash::SetCounter);
    l2tmp = AtomicGet(XrdMqSharedHash::SetNLCounter);
    l3tmp = AtomicGet(XrdMqSharedHash::GetCounter);
#ifdef EOS_INSTRUMENTED_RWMUTEX
    // fsview statistics extraction
    view1tmp = FsView::gFsView.ViewMutex.GetReadLockCounter();
    view2tmp = FsView::gFsView.ViewMutex.GetWriteLockCounter();
    FsView::gFsView.ViewMutex.GetTimingStatistics( view12stmp );
    FsView::gFsView.ViewMutex.ResetTimingStatistics();

    // namespace lock statistics extraction
    ns1tmp = gOFS->eosViewRWMutex.GetReadLockCounter();
    ns2tmp = gOFS->eosViewRWMutex.GetWriteLockCounter();
    gOFS->eosViewRWMutex.GetTimingStatistics( ns12stmp );
    gOFS->eosViewRWMutex.ResetTimingStatistics();
    
    // quota lock statistics extraction
    qu1tmp = Quota::gQuotaMutex.GetReadLockCounter();
    qu2tmp = Quota::gQuotaMutex.GetWriteLockCounter();
    Quota::gQuotaMutex.GetTimingStatistics( qu12stmp );
    Quota::gQuotaMutex.ResetTimingStatistics();
#endif
    Add("HashSet"  ,0,0,l1tmp-l1);
    Add("HashSetNoLock",0,0,l2tmp-l2);
    Add("HashGet"  ,0,0,l3tmp-l3);
#ifdef EOS_INSTRUMENTED_RWMUTEX
    Add("ViewLockR",0,0,view1tmp-view1);
    Add("ViewLockW",0,0,view2tmp-view2);
    Add("NsLockR",0,0,ns1tmp-ns1);
    Add("NsLockW",0,0,ns2tmp-ns2);
    Add("QuotaLockR",0,0,qu1tmp-qu1);
    Add("QuotaLockW",0,0,qu2tmp-qu2);
    AddExt("ViewLockRWait",0,0,(unsigned long)view12stmp.readLockCounterSample, view12stmp.averagewaitread, view12stmp.minwaitread,view12stmp.maxwaitread);
    AddExt("ViewLockWWait",0,0,(unsigned long)view12stmp.writeLockCounterSample,view12stmp.averagewaitwrite,view12stmp.minwaitwrite,view12stmp.maxwaitwrite);
    AddExt("NsLockRWait",0,0,(unsigned long)ns12stmp.readLockCounterSample,ns12stmp.averagewaitread,ns12stmp.minwaitread,ns12stmp.maxwaitread);
    AddExt("NsLockWWait",0,0,(unsigned long)ns12stmp.writeLockCounterSample,ns12stmp.averagewaitwrite,ns12stmp.minwaitwrite,ns12stmp.maxwaitwrite);
    AddExt("QuotaLockRWait",0,0,(unsigned long)qu12stmp.readLockCounterSample,qu12stmp.averagewaitread,ns12stmp.minwaitread,ns12stmp.maxwaitread);
    AddExt("QuotaLockWWait",0,0,(unsigned long)qu12stmp.writeLockCounterSample,qu12stmp.averagewaitwrite,ns12stmp.minwaitwrite,ns12stmp.maxwaitwrite);
    view1 = view1tmp;
    view2 = view2tmp;
    ns1 = ns1tmp;
    ns2 = ns2tmp;
    qu1 = qu1tmp;
    qu2 = qu2tmp;
#endif
    l1 = l1tmp;
    l2 = l2tmp;
    l3 = l3tmp;

    // --------------------------------------------

    Mutex.Lock();

    google::sparse_hash_map<std::string, google::sparse_hash_map<uid_t, StatAvg> >::iterator tit;
    google::sparse_hash_map<std::string, google::sparse_hash_map<uid_t, StatExt> >::iterator tit_ext;
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

    for (tit_ext = StatExtGid.begin(); tit_ext != StatExtGid.end(); ++tit_ext) {
      // loop over vids
      google::sparse_hash_map<uid_t, StatExt>::iterator it;
      for (it = tit_ext->second.begin(); it != tit_ext->second.end(); ++it) {
        it->second.StampZero();
      }
    }

    for (tit_ext = StatExtGid.begin(); tit_ext != StatExtGid.end(); ++tit_ext) {
      // loop over vids
      google::sparse_hash_map<uid_t, StatExt>::iterator it;
      for (it = tit_ext->second.begin(); it != tit_ext->second.end(); ++it) {
        it->second.StampZero();
      }
    }

    Mutex.UnLock();
  }
}

EOSMGMNAMESPACE_END
