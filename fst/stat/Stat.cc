// ----------------------------------------------------------------------
// File: Stat.cc
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

/*----------------------------------------------------------------------------*/
#include "fst/Namespace.hh"
#include "common/Mapping.hh"
#include "common/Logging.hh"
#include "common/Timing.hh"
#include "common/StringConversion.hh"
#include "common/SymKeys.hh"
#include "fst/stat/Stat.hh"
#include "fst/Config.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "fmt/printf.h"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

static constexpr std::string_view na = "0.00";

void
Stat::Add(const char* id, uid_t uid, gid_t gid, const std::string& app, const std::string& tag, unsigned long val)
{
  std::string su = "uid:" + std::to_string(uid) + std::string(":") + tag;
  std::string sg = "gid:" + std::to_string(gid) + std::string(":") + tag;
  std::string sa = "app:" + app + std::string(":") + tag;
  time_t now = time(NULL);
  
  XrdSysMutexHelper lLock(Mutex);
  StatsId[su][id] += val;
  StatsId[sg][id] += val;
  StatsId[sa][id] += val;
  StatsId[su][__SUM__TOTAL__] += val;
  StatsId[sg][__SUM__TOTAL__] += val;
  StatsId[sa][__SUM__TOTAL__] += val;
  StatAvgId[su][id].Add(val);
  StatAvgId[sg][id].Add(val);
  StatAvgId[sa][id].Add(val);
  StatAvgId[su][__SUM__TOTAL__].Add(val);
  StatAvgId[sg][__SUM__TOTAL__].Add(val);
  StatAvgId[sa][__SUM__TOTAL__].Add(val);
  StatTime[su]=now;
  StatTime[sg]=now;
  StatTime[sa]=now;
}

void
Stat::Remove(const char* id, uid_t uid, gid_t gid, const std::string& app, const std::string& tag)
{
  std::string su = "uid:" + std::to_string(uid) + std::string(":") + tag;
  std::string sg = "gid:" + std::to_string(gid) + std::string(":") + tag;
  std::string sa = "app:" + app + std::string(":") + tag;

  XrdSysMutexHelper lLock(Mutex);
  StatsId[su].erase(id);
  StatsId[sg].erase(id);
  StatsId[sa].erase(id);
  StatAvgId[su].erase(id);
  StatAvgId[sg].erase(id);
  StatAvgId[sa].erase(id);
}

/*----------------------------------------------------------------------------*/
void
Stat::AddExec(const char* id, uid_t uid, gid_t gid, const std::string& app, const std::string& tag, float exectime)
{
  std::string su = "uid:" + std::to_string(uid) + std::string(":") + tag;
  std::string sg = "gid:" + std::to_string(gid) + std::string(":") + tag;
  std::string sa = "app:" + app + std::string(":") + tag;
  time_t now = time(NULL);
  
  XrdSysMutexHelper lLock(Mutex);
  
  StatExec[su].push_back(exectime);
  StatExec[sg].push_back(exectime);
  StatExec[sa].push_back(exectime);

  TotalExec += exectime;

  // we average over 1000 entries
  if (StatExec[su].size() > 1000) {
    StatExec[su].pop_front();
  }
  if (StatExec[sg].size() > 1000) {
    StatExec[sg].pop_front();
  }
  if (StatExec[sa].size() > 1000) {
    StatExec[sa].pop_front();
  }
  StatTime[su]=now;
  StatTime[sg]=now;
  StatTime[sa]=now;
}

/*----------------------------------------------------------------------------*/
unsigned long long
Stat::GetTotal(const char* tag)
{
  std::map<std::string, unsigned long long>::const_iterator it;

  if (!StatsId.count(tag)) {
    return 0;
  }

  return StatsId[tag][__SUM__TOTAL__];
}

/*----------------------------------------------------------------------------*/
// warning: you have to lock the mutex if directly used

double
Stat::GetTotalAvg3600(const char* tag)
{
  std::map<std::string, StatAvg>::iterator it;

  if (!StatAvgId.count(tag)) {
    return 0;
  }

  return StatAvgId[tag][__SUM__TOTAL__].GetAvg3600();
}

/*----------------------------------------------------------------------------*/
// warning: you have to lock the mutex if directly used

double
Stat::GetTotalAvg300(const char* tag)
{
  std::map<std::string, StatAvg>::iterator it;

  if (!StatAvgId.count(tag)) {
    return 0;
  }

  return StatAvgId[tag][__SUM__TOTAL__].GetAvg300();
}

/*----------------------------------------------------------------------------*/
// warning: you have to lock the mutex if directly used

double
Stat::GetTotalAvg60(const char* tag)
{
  std::map<std::string, StatAvg>::iterator it;

  if (!StatAvgId.count(tag)) {
    return 0;
  }
  
  return StatAvgId[tag][__SUM__TOTAL__].GetAvg60();
}

/*----------------------------------------------------------------------------*/
// warning: you have to lock the mutex if directly used

double
Stat::GetTotalAvg10(const char* tag)
{
  std::map<std::string, StatAvg>::iterator it;
  
  if (!StatAvgId.count(tag)) {
    return 0;
  }
  
  return StatAvgId[tag][__SUM__TOTAL__].GetAvg10();
}

/*----------------------------------------------------------------------------*/
// warning: you have to lock the mutex if directly used

double
Stat::GetExec(const char* tag, double& deviation)
{
  // calculates average execution time for 'tag'
  if (StatExec.count(tag)) {
    std::deque<float>::const_iterator it;
    double sum = 0;
    double avg = 0;
    deviation = 0;
    int cnt = 0;

    for (it = StatExec[tag].begin(); it != StatExec[tag].end(); it++) {
      cnt++;
      sum += *it;
    }

    avg = sum / (cnt ? cnt : 999999999);

    for (it = StatExec[tag].begin(); it != StatExec[tag].end(); it++) {
      deviation += pow((*it - avg), 2);
    }

    deviation = sqrt(deviation / (cnt ? cnt : 99999999));
    return avg;
  }

  return 0;
}

/*----------------------------------------------------------------------------*/
// warning: you have to lock the mutex if directly used
/*----------------------------------------------------------------------------*/

double
Stat::GetTotalExec(double& deviation, size_t& ops)
{
  // calculates average execution time for all commands
  std::map<std::string, std::deque<float> >::const_iterator ittag;
  double sum = 0;
  double avg = 0;
  size_t cnt = 0;
  deviation = 0;

  for (ittag = StatExec.begin(); ittag != StatExec.end(); ittag++) {
    std::deque<float>::const_iterator it;

    for (it = ittag->second.begin(); it != ittag->second.end(); it++) {
      cnt++;
      sum += *it;
    }

    ops += GetTotal(ittag->first.c_str());
  }

  if (cnt) {
    avg = sum / cnt;
  }

  for (ittag = StatExec.begin(); ittag != StatExec.end(); ittag++) {
    std::deque<float>::const_iterator it;

    for (it = ittag->second.begin(); it != ittag->second.end(); it++) {
      deviation += pow((*it - avg), 2);
    }
  }

  if (cnt) {
    deviation = sqrt(deviation / cnt);
  }

  return avg;
}


/*----------------------------------------------------------------------------*/
void
Stat::Clear()
{
  XrdSysMutexHelper lLock(Mutex);

  for (auto ittag = StatsId.begin(); ittag != StatsId.end(); ittag++) {
    StatsId[ittag->first].clear();
  }

  for (auto ittag = StatAvgId.begin(); ittag != StatAvgId.end(); ittag++) {
    StatAvgId[ittag->first].clear();
  }

  for (auto ittag = StatExec.begin(); ittag != StatExec.end(); ittag++) {
    StatExec[ittag->first].clear();
  }

  TotalExec = 0;
}


/*----------------------------------------------------------------------------*/
void
Stat::PrintOutTotal(std::string& out, bool monitoring)
{

  std::vector<std::string> tags, tags_ext;
  std::vector<std::string>::iterator it;
  std::map<std::string, std::map<std::string, unsigned long long> >::iterator tit;
  char outline[8192];
  double avg = 0;
  double sig = 0;
  size_t ops = 0;
  
  {
    XrdSysMutexHelper lLock(Mutex);
    for (tit = StatsId.begin(); tit != StatsId.end(); tit++) {
      tags.push_back(tit->first);
    }
    avg = GetTotalExec(sig, ops);
  }
  
  std::sort(tags.begin(), tags.end());
  std::sort(tags_ext.begin(), tags_ext.end());



  if (!monitoring) {
    snprintf(outline, sizeof(outline),
             "%-7s %-32s %3.02f +- %3.02f = %.02fs\n", "ALL",
             "Execution Time", avg,
             sig, TotalExec / 1000.0);
    out += outline;
    out += "# ---------------------------------------------------------------------------------------------------------------------------------------------------------------\n";
    snprintf(outline, sizeof(outline),
             "%-7s %-32s %-12s %16s %16s %16s %16s %10s +- %-10s", "who",
             "stream", "sum", "10s", "1min", "5min", "1h", "exec(ms)", "sigma(ms)");
    out += outline;
    out += "\n";
    out += "# ---------------------------------------------------------------------------------------------------------------------------------------------------------------\n";
  } else {
    snprintf(outline, sizeof(outline),
             "uid=all gid=all total.exec.avg=%.02f total.exec.sigma=%.02f total.exec.sum=%.02f\n",
             avg, sig, TotalExec);
    out += outline;
  }

  for (it = tags.begin(); it != tags.end(); ++it) {
    const char* tag = it->c_str();
    std::string aexec = "-NA-";
    std::string aexecsig = "-NA-";
    double avg = 0;
    double sig = 0;
    unsigned long long av10,av60,av300,av3600 = 0;
    unsigned  long atotal = 0;
    
    {
      XrdSysMutexHelper lLock(Mutex);
      avg    = GetExec(tag, sig);
      av10    = GetTotalAvg10(tag);
      av60   = GetTotalAvg60(tag);
      av300  = GetTotalAvg300(tag);
      av3600 = GetTotalAvg3600(tag);
      atotal = GetTotal(tag);
    }
    
    std::string a10 = fmt::sprintf("%llu", av10);
    std::string a60 = fmt::sprintf("%llu", av60);
    std::string a300 = fmt::sprintf("%llu", av300);
    std::string a3600 = fmt::sprintf("%llu", av3600);
    

    if (avg) {
      aexec = fmt::sprintf("%3.02f", avg);
    }

    if (sig) {
      aexecsig = fmt::sprintf("%3.02f", sig);
    }

    // TODO: make the template a constexpr sv so that it is easier to validate
    if (!monitoring) {
      std::string st = std::to_string(GetTotal(tag));
      out += fmt::sprintf("ALL     %-32s %-12s %16s %16s %16s %16s %10s +- %-10s\n",
                          tag, st, a10, a60, a300, a3600, aexec, aexecsig);
    } else {
      out += fmt::sprintf("uid=all gid=all cmd=%s total=%llu 10s=%s 60s=%s 300s=%s 3600s=%s exec=%f execsig=%f\n",
                          tag, atotal, a10, a60, a300, a3600, avg, sig);
    }
  }
}

void
Stat::PrintOutTotalJson(Json::Value& out)
{
  std::vector<std::string> tags, tags_ext;
  std::vector<std::string>::iterator it;
  std::map<std::string, std::map<std::string, unsigned long long> >::iterator
  tit;

  double avg = 0;
  double sig = 0;
  size_t ops = 0;

  eos::common::Timing tm("Test");
  COMMONTIMING("START",&tm);
  {
    XrdSysMutexHelper lLock(Mutex);
    for (tit = StatsId.begin(); tit != StatsId.end(); tit++) {
      tags.push_back(tit->first);
    }
    avg = GetTotalExec(sig, ops);
  }


  std::sort(tags.begin(), tags.end());
  std::sort(tags_ext.begin(), tags_ext.end());


  sum_ops = ops;
  out["time"]=Json::Value{};
  out["time"]["avg(ms)"] = Json::Value(avg);
  out["time"]["sigma(ms)"] = Json::Value(sig);
  out["time"]["total(s)"] = Json::Value((double) (TotalExec / 1000.0));
  out["time"]["ops"] = Json::LargestUInt(sum_ops);

  for (it = tags.begin(); it != tags.end(); ++it) {
    Json::Value entry{};
    const char* tag = it->c_str();
    avg = 0;
    sig = 0;
    double total = 0;
    XrdSysMutexHelper lLock(Mutex);
    avg = GetExec(tag, sig);
    entry["sum"]        = (Json::LargestUInt) GetTotal(tag);
    entry["10s"]         = GetTotalAvg10(tag);
    entry["1min"]       = GetTotalAvg60(tag);
    entry["5min"]       = GetTotalAvg300(tag);
    entry["1h"]         = GetTotalAvg3600(tag);
    entry["exec_ms"]   = avg;
    entry["sigma_ms"]  = sig;
    out["activity"][tag]=entry;
  }
  COMMONTIMING("STOP",&tm);
  out["publishing"]["ms"] = tm.RealTime();
  out["publishing"]["unixtime"] =(Json::LargestUInt) time(NULL);
}

std::string
Stat::PrintOutTotalJson()
{
  Json::Value json;
  PrintOutTotalJson(json);
  return SSTR(json);
}

//------------------------------------------------------------------------------
// Run asynchronous circulation thread
//------------------------------------------------------------------------------
bool
Stat::Start()
{
  mThread.reset(&Stat::Circulate, this);
  mDumpThread.reset(&Stat::Dump, this);
  return true;
}

//------------------------------------------------------------------------------
// Cancel the asynchronous circuluation thread
//------------------------------------------------------------------------------
void
Stat::Stop()
{
  mThread.join();
  mDumpThread.join();
}


/*----------------------------------------------------------------------------*/
void
Stat::Circulate(ThreadAssistant& assistant)
{
  size_t cnt=0;
  
  // empty the circular buffer and extract some Mq statistic values
  while (true) {
    cnt++;
    assistant.wait_for(std::chrono::milliseconds(512));

    if (assistant.terminationRequested()) {
      break;
    }
    
    {
      std::set<std::string> tags;
      std::set<std::string> deletetags;
      {
	time_t now = time(NULL);
	
	XrdSysMutexHelper lLock(Mutex);
	for (auto tit = StatAvgId.begin(); tit != StatAvgId.end(); ++tit) {
	  tags.insert(tit->first);
	  auto t = StatTime.find(tit->first);
	  if (t!=StatTime.end()) {
	    // remove everything older than 1 hour
	    if (now > (t->second+3600)) {
	      deletetags.insert(tit->first);
	      tags.erase(tit->first);
	    }
	  } 
	}
      }

      // do deletion of old streams
      for (auto tit = deletetags.begin(); tit != deletetags.end(); ++tit) {
	XrdSysMutexHelper lLock(Mutex);
	StatsId.erase(*tit);
	StatAvgId.erase(*tit);
	StatTime.erase(*tit);
      }
	
      // loop over active tags
      for (auto tit = tags.begin(); tit != tags.end(); ++tit) {
	XrdSysMutexHelper lLock(Mutex);
	auto entry = StatAvgId.find(*tit);
	if (entry!=StatAvgId.end()) {
	  // loop over vids
	  std::map<std::string, StatAvg>::iterator it;
	  for (it = entry->second.begin(); it != entry->second.end(); ++it) {
	    it->second.StampZero();
	  }
	}
      }
    }
  }
}

void
Stat::Dump(ThreadAssistant& assistant)
{
  size_t cnt=0;
  
  // empty the circular buffer and extract some Mq statistic values
  while (true) {
    cnt++;
    assistant.wait_for(std::chrono::milliseconds(500));

    if (assistant.terminationRequested()) {
      break;
    }

    if (!(cnt%10)) {
      {
	XrdSysMutexHelper lLock(JsonMutex);
	json = PrintOutTotalJson();
	eos::common::SymKey::ZBase64(json, jsonzbase64);
      }
      std::string dfile1 = mDumpPath;
      std::string dfile2 = mDumpPath;
      dfile1 += "/.iotop";
      dfile2 += "/iotop";
      dfile1 += ".";
      dfile2 += ".";
      dfile1 += std::to_string(mPort);
      dfile2 += std::to_string(mPort);
      ofstream outdata; // outdata is like cin
      outdata.open(dfile1.c_str()); // opens the file
      if(outdata ) {
	outdata << json << std::endl;
	outdata.close();
      }
      ::rename(dfile1.c_str(), dfile2.c_str());
    }
  }
}

EOSFSTNAMESPACE_END
