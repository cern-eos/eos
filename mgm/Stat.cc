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

#include "common/Mapping.hh"
#include "common/table_formatter/TableFormatterBase.hh"
#include "mgm/Stat.hh"
#include "mgm/FsView.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mq/XrdMqSharedObject.hh"
#include "mgm/Quota.hh"
#include "XrdOuc/XrdOucString.hh"

EOSMGMNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
void
Stat::Add(const char* tag, uid_t uid, gid_t gid, unsigned long val)
{
  XrdSysMutexHelper lock(mMutex);
  StatsUid[tag][uid] += val;
  StatsGid[tag][gid] += val;
  StatAvgUid[tag][uid].Add(val);
  StatAvgGid[tag][gid].Add(val);
}

/*----------------------------------------------------------------------------*/
void
Stat::AddExt(const char* tag, uid_t uid, gid_t gid, unsigned long nsample,
             const double& avgv, const double& minv, const double& maxv)
{
  XrdSysMutexHelper lock(mMutex);
  StatExtUid[tag][uid].Insert(nsample, avgv, minv, maxv);
  StatExtGid[tag][gid].Insert(nsample, avgv, minv, maxv);
}

/*----------------------------------------------------------------------------*/
void
Stat::AddExec(const char* tag, float exectime)
{
  XrdSysMutexHelper lock(mMutex);
  StatExec[tag].push_back(exectime);

  // we average over 100 entries
  if (StatExec[tag].size() > 100) {
    StatExec[tag].pop_front();
  }
}

/*----------------------------------------------------------------------------*/
unsigned long long
Stat::GetTotal(const char* tag)
{
  google::sparse_hash_map<uid_t, unsigned long long>::const_iterator it;
  unsigned long long val = 0;

  if (!StatsUid.count(tag)) {
    return 0;
  }

  for (it = StatsUid[tag].begin(); it != StatsUid[tag].end(); ++it) {
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

  if (!StatAvgUid.count(tag)) {
    return 0;
  }

  for (it = StatAvgUid[tag].begin(); it != StatAvgUid[tag].end(); ++it) {
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

  if (!StatExtUid.count(tag)) {
    return 0;
  }

  for (it = StatExtUid[tag].begin(); it != StatExtUid[tag].end(); ++it) {
    for (int i = 0; i < 3600; i++) {
      n += it->second.n3600[i];
    }
  }

  return (double) n;
}

/*----------------------------------------------------------------------------*/
// warning: you have to lock the mutex if directly used

double
Stat::GetTotalAvgExt3600(const char* tag)
{
  google::sparse_hash_map<uid_t, StatExt>::iterator it;
  double val = 0;
  double totw = 0;

  if (!StatExtUid.count(tag)) {
    return 0;
  }

  for (it = StatExtUid[tag].begin(); it != StatExtUid[tag].end(); ++it) {
    double w = 0;

    for (int i = 0; i < 3600; i++) {
      w += it->second.n3600[i];
    }

    totw += w;
    val += it->second.GetAvg3600() * w;
  }

  return val / totw;
}

/*----------------------------------------------------------------------------*/
// warning: you have to lock the mutex if directly used

double
Stat::GetTotalMinExt3600(const char* tag)
{
  google::sparse_hash_map<uid_t, StatExt>::iterator it;
  double minval = std::numeric_limits<unsigned long>::max();

  if (!StatExtUid.count(tag)) {
    return 0;
  }

  for (it = StatExtUid[tag].begin(); it != StatExtUid[tag].end(); ++it) {
    minval = std::min(minval, it->second.GetMin3600());
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

  if (!StatExtUid.count(tag)) {
    return 0;
  }

  for (it = StatExtUid[tag].begin(); it != StatExtUid[tag].end(); ++it) {
    maxval = std::max(maxval, it->second.GetMax3600());
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

  if (!StatAvgUid.count(tag)) {
    return 0;
  }

  for (it = StatAvgUid[tag].begin(); it != StatAvgUid[tag].end(); ++it) {
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

  if (!StatExtUid.count(tag)) {
    return 0;
  }

  for (it = StatExtUid[tag].begin(); it != StatExtUid[tag].end(); ++it) {
    for (int i = 0; i < 300; i++) {
      n += it->second.n300[i];
    }
  }

  return (double) n;
}

/*----------------------------------------------------------------------------*/
// warning: you have to lock the mutex if directly used

double
Stat::GetTotalAvgExt300(const char* tag)
{
  google::sparse_hash_map<uid_t, StatExt>::iterator it;
  double val = 0;
  double totw = 0;

  if (!StatExtUid.count(tag)) {
    return 0;
  }

  for (it = StatExtUid[tag].begin(); it != StatExtUid[tag].end(); ++it) {
    double w = 0;

    for (int i = 0; i < 300; i++) {
      w += it->second.n300[i];
    }

    totw += w;
    val += it->second.GetAvg300() * w;
  }

  return val / totw;
}

/*----------------------------------------------------------------------------*/
// warning: you have to lock the mutex if directly used

double
Stat::GetTotalMinExt300(const char* tag)
{
  google::sparse_hash_map<uid_t, StatExt>::iterator it;
  double minval = std::numeric_limits<unsigned long>::max();

  if (!StatExtUid.count(tag)) {
    return 0;
  }

  for (it = StatExtUid[tag].begin(); it != StatExtUid[tag].end(); ++it) {
    minval = std::min(minval, it->second.GetMin300());
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

  if (!StatExtUid.count(tag)) {
    return 0;
  }

  for (it = StatExtUid[tag].begin(); it != StatExtUid[tag].end(); ++it) {
    maxval = std::max(maxval, it->second.GetMax300());
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

  if (!StatAvgUid.count(tag)) {
    return 0;
  }

  for (it = StatAvgUid[tag].begin(); it != StatAvgUid[tag].end(); ++it) {
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

  if (!StatExtUid.count(tag)) {
    return 0;
  }

  for (it = StatExtUid[tag].begin(); it != StatExtUid[tag].end(); ++it) {
    for (int i = 0; i < 60; i++) {
      n += it->second.n60[i];
    }
  }

  return (double) n;
}

/*----------------------------------------------------------------------------*/
// warning: you have to lock the mutex if directly used

double
Stat::GetTotalAvgExt60(const char* tag)
{
  google::sparse_hash_map<uid_t, StatExt>::iterator it;
  double val = 0;
  double totw = 0;

  if (!StatExtUid.count(tag)) {
    return 0;
  }

  for (it = StatExtUid[tag].begin(); it != StatExtUid[tag].end(); ++it) {
    double w = 0;

    for (int i = 0; i < 60; i++) {
      w += it->second.n60[i];
    }

    totw += w;
    val += it->second.GetAvg60() * w;
  }

  return val / totw;
}

/*----------------------------------------------------------------------------*/
// warning: you have to lock the mutex if directly used

double
Stat::GetTotalMinExt60(const char* tag)
{
  google::sparse_hash_map<uid_t, StatExt>::iterator it;
  double minval = std::numeric_limits<unsigned long>::max();

  if (!StatExtUid.count(tag)) {
    return 0;
  }

  for (it = StatExtUid[tag].begin(); it != StatExtUid[tag].end(); ++it) {
    minval = std::min(minval, it->second.GetMin60());
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

  if (!StatExtUid.count(tag)) {
    return 0;
  }

  for (it = StatExtUid[tag].begin(); it != StatExtUid[tag].end(); ++it) {
    maxval = std::max(maxval, it->second.GetMax60());
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

  if (!StatAvgUid.count(tag)) {
    return 0;
  }

  for (it = StatAvgUid[tag].begin(); it != StatAvgUid[tag].end(); ++it) {
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

  if (!StatExtUid.count(tag)) {
    return 0;
  }

  for (it = StatExtUid[tag].begin(); it != StatExtUid[tag].end(); ++it) {
    for (int i = 0; i < 5; i++) {
      n += it->second.n5[i];
    }
  }

  return (double) n;
}

/*----------------------------------------------------------------------------*/
// warning: you have to lock the mutex if directly used

double
Stat::GetTotalAvgExt5(const char* tag)
{
  google::sparse_hash_map<uid_t, StatExt>::iterator it;
  double val = 0;
  double totw = 0;

  if (!StatExtUid.count(tag)) {
    return 0;
  }

  for (it = StatExtUid[tag].begin(); it != StatExtUid[tag].end(); ++it) {
    double w = 0;

    for (int i = 0; i < 5; i++) {
      w += it->second.n5[i];
    }

    totw += w;
    val += it->second.GetAvg5() * w;
  }

  return val / totw;
}

/*----------------------------------------------------------------------------*/
// warning: you have to lock the mutex if directly used

double
Stat::GetTotalMinExt5(const char* tag)
{
  google::sparse_hash_map<uid_t, StatExt>::iterator it;
  double minval = std::numeric_limits<unsigned long>::max();

  if (!StatExtUid.count(tag)) {
    return 0;
  }

  for (it = StatExtUid[tag].begin(); it != StatExtUid[tag].end(); ++it) {
    minval = std::min(minval, it->second.GetMin5());
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

  if (!StatExtUid.count(tag)) {
    return 0;
  }

  for (it = StatExtUid[tag].begin(); it != StatExtUid[tag].end(); ++it) {
    maxval = std::max(maxval, it->second.GetMax5());
  }

  return maxval;
}


//------------------------------------------------------------------------------
// Calculate the average execution time for 'tag'
// warning: you have to lock the mutex if directly used
//------------------------------------------------------------------------------
double
Stat::GetExec(const char* tag, double& deviation)
{
  double avg = 0;
  deviation = 0;

  if (StatExec.count(tag)) {
    std::deque<float>::const_iterator it;
    double sum = 0;
    int cnt = 0;

    for (it = StatExec[tag].begin(); it != StatExec[tag].end(); ++it) {
      cnt++;
      sum += *it;
    }

    if (!cnt) {
      return avg;
    }

    avg = sum / cnt;

    for (it = StatExec[tag].begin(); it != StatExec[tag].end(); ++it) {
      deviation += pow((*it - avg), 2);
    }

    deviation = sqrt(deviation / cnt);
  }

  return avg;
}

/*----------------------------------------------------------------------------*/
// warning: you have to lock the mutex if directly used

double
Stat::GetTotalExec(double& deviation)
{
  // calculates average execution time for all commands
  google::sparse_hash_map<std::string, std::deque<float> >::const_iterator ittag;
  double sum = 0;
  double avg = 0;
  deviation = 0;
  int cnt = 0;

  for (ittag = StatExec.begin(); ittag != StatExec.end(); ittag++) {
    std::deque<float>::const_iterator it;

    for (it = ittag->second.begin(); it != ittag->second.end(); it++) {
      cnt++;
      sum += *it;
    }
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
  XrdSysMutexHelper lock(mMutex);

  for (auto ittag = StatsUid.begin(); ittag != StatsUid.end(); ittag++) {
    StatsUid[ittag->first].clear();
    StatsUid[ittag->first].resize(1000);
    StatsGid[ittag->first].clear();
    StatsGid[ittag->first].resize(1000);
    StatAvgUid[ittag->first].clear();
    StatAvgUid[ittag->first].resize(1000);
    StatAvgGid[ittag->first].clear();
    StatAvgGid[ittag->first].resize(1000);
    StatExec[ittag->first].clear();
    StatExec[ittag->first].resize(1000);
  }
}

/*----------------------------------------------------------------------------*/
void
Stat::PrintOutTotal(XrdOucString& out, bool details, bool monitoring,
                    bool numerical)
{
  mMutex.Lock();
  std::vector<std::string> tags, tags_ext;
  std::vector<std::string>::iterator it;
  google::sparse_hash_map < std::string,
         google::sparse_hash_map<uid_t, unsigned long long> >::iterator tit;
  google::sparse_hash_map < std::string,
         google::sparse_hash_map<uid_t, StatExt > >::iterator tit_ext;

  for (tit = StatsUid.begin(); tit != StatsUid.end(); ++tit) {
    tags.push_back(tit->first);
  }

  for (tit_ext = StatExtUid.begin(); tit_ext != StatExtUid.end(); ++tit_ext) {
    tags_ext.push_back(tit_ext->first);
  }

  std::sort(tags.begin(), tags.end());
  std::sort(tags_ext.begin(), tags_ext.end());
  char outline[1024];
  double avg = 0;
  double sig = 0;
  avg = GetTotalExec(sig);

  if (!monitoring) {
    sprintf(outline, "%-8s %-32s %3.02f ± %3.02f\n", "ALL", "Execution Time", avg,
            sig);
  } else {
    sprintf(outline,
            "uid=all gid=all total.exec.avg=%.02f total.exec.sigma=%.02f\n", avg, sig);
  }

  out += outline;
  std::string na = !monitoring ? "-NA-" : "NA";
  std::string format_cmd = !monitoring ? "-s" : "os";
  std::string format_s = !monitoring ? "s" : "os";
  std::string format_ss = !monitoring ? "-s" : "os";
  std::string format_l = !monitoring ? "+l" : "ol";
  std::string format_f = !monitoring ? "f" : "of";
  std::string format_ff = !monitoring ? "±f" : "of";
  // Specification for all users and groups
  TableFormatterBase table_all;

  if (!monitoring) {
    table_all.SetHeader({
      std::make_tuple("who", 3, format_ss),
      std::make_tuple("command", 24, format_cmd),
      std::make_tuple("sum", 8, format_l),
      std::make_tuple("5s", 8, format_f),
      std::make_tuple("1min", 8, format_f),
      std::make_tuple("5min", 8, format_f),
      std::make_tuple("1h", 8, format_f),
      std::make_tuple("exec(ms)", 8, format_f),
      std::make_tuple("sigma(ms)", 8, format_ff)
    });
  } else {
    table_all.SetHeader({
      std::make_tuple("uid", 0, format_ss),
      std::make_tuple("gid", 0, format_s),
      std::make_tuple("cmd", 0, format_s),
      std::make_tuple("total", 0, format_l),
      std::make_tuple("5s", 0, format_f),
      std::make_tuple("60s", 0, format_f),
      std::make_tuple("300s", 0, format_f),
      std::make_tuple("3600s", 0, format_f),
      std::make_tuple("exec", 0, format_f),
      std::make_tuple("execsig", 0, format_ff)
    });
  }

  for (it = tags.begin(); it != tags.end(); ++it) {
    const char* tag = it->c_str();
    double avg = 0, sig = 0;
    avg = GetExec(tag, sig);
    TableData table_data;
    table_data.emplace_back();
    table_data.back().push_back(TableCell("all", format_ss));

    if (monitoring) {
      table_data.back().push_back(TableCell("all", format_s));
    }

    table_data.back().push_back(TableCell(tag, format_cmd));
    table_data.back().push_back(TableCell(GetTotal(tag), format_l));
    table_data.back().push_back(TableCell(GetTotalAvg5(tag), format_f));
    table_data.back().push_back(TableCell(GetTotalAvg60(tag), format_f));
    table_data.back().push_back(TableCell(GetTotalAvg300(tag), format_f));
    table_data.back().push_back(TableCell(GetTotalAvg3600(tag), format_f));

    if (avg || monitoring) {
      table_data.back().push_back(TableCell(avg, format_f));
    } else {
      table_data.back().push_back(TableCell(na, format_s));
    }

    if (sig || monitoring) {
      table_data.back().push_back(TableCell(sig, format_ff));
    } else {
      table_data.back().push_back(TableCell(na, format_s));
    }

    table_all.AddRows(table_data);
  }

  if (details) {
    for (it = tags_ext.begin(); it != tags_ext.end(); ++it) {
      const char* tag = it->c_str();
      TableData table_data_spl, table_data_min, table_data_avg, table_data_max;
      table_data_spl.emplace_back();
      table_data_min.emplace_back();
      table_data_avg.emplace_back();
      table_data_max.emplace_back();
      table_data_spl.back().push_back(TableCell("all", format_ss));
      table_data_min.back().push_back(TableCell("all", format_ss));
      table_data_avg.back().push_back(TableCell("all", format_ss));
      table_data_max.back().push_back(TableCell("all", format_ss));

      if (monitoring) {
        table_data_spl.back().push_back(TableCell("all", format_s));
        table_data_min.back().push_back(TableCell("all", format_s));
        table_data_avg.back().push_back(TableCell("all", format_s));
        table_data_max.back().push_back(TableCell("all", format_s));
      }

      std::string tag_spl = tag, tag_min = tag, tag_avg = tag, tag_max = tag;
      tag_spl += ":spl";
      tag_min += ":min";
      tag_avg += ":avg";
      tag_max += ":max";
      table_data_spl.back().push_back(TableCell(tag_spl, format_s));
      table_data_min.back().push_back(TableCell(tag_min, format_s));
      table_data_avg.back().push_back(TableCell(tag_avg, format_s));
      table_data_max.back().push_back(TableCell(tag_max, format_s));
      table_data_spl.back().push_back(TableCell("", "", "", true));
      table_data_min.back().push_back(TableCell("", "", "", true));
      table_data_avg.back().push_back(TableCell("", "", "", true));
      table_data_max.back().push_back(TableCell("", "", "", true));
      table_data_spl.back().push_back(TableCell(GetTotalNExt5(tag), format_f));

      if (GetTotalNExt5(tag) < 1) {
        table_data_min.back().push_back(TableCell(na, format_s));
        table_data_avg.back().push_back(TableCell(na, format_s));
        table_data_max.back().push_back(TableCell(na, format_s));
      } else {
        table_data_min.back().push_back(TableCell(GetTotalMinExt5(tag), format_f));
        table_data_avg.back().push_back(TableCell(GetTotalAvgExt5(tag), format_f));
        table_data_max.back().push_back(TableCell(GetTotalMaxExt5(tag), format_f));
      }

      table_data_spl.back().push_back(TableCell(GetTotalNExt60(tag), format_f));

      if (GetTotalNExt60(tag) < 1) {
        table_data_min.back().push_back(TableCell(na, format_s));
        table_data_avg.back().push_back(TableCell(na, format_s));
        table_data_max.back().push_back(TableCell(na, format_s));
      } else {
        table_data_min.back().push_back(TableCell(GetTotalMinExt60(tag), format_f));
        table_data_avg.back().push_back(TableCell(GetTotalAvgExt60(tag), format_f));
        table_data_max.back().push_back(TableCell(GetTotalMaxExt60(tag), format_f));
      }

      table_data_spl.back().push_back(TableCell(GetTotalNExt300(tag), format_f));

      if (GetTotalNExt300(tag) < 1) {
        table_data_min.back().push_back(TableCell(na, format_s));
        table_data_avg.back().push_back(TableCell(na, format_s));
        table_data_max.back().push_back(TableCell(na, format_s));
      } else {
        table_data_min.back().push_back(TableCell(GetTotalMinExt300(tag), format_f));
        table_data_avg.back().push_back(TableCell(GetTotalAvgExt300(tag), format_f));
        table_data_max.back().push_back(TableCell(GetTotalMaxExt300(tag), format_f));
      }

      table_data_spl.back().push_back(TableCell(GetTotalNExt3600(tag), format_f));

      if (GetTotalNExt3600(tag) < 1) {
        table_data_min.back().push_back(TableCell(na, format_s));
        table_data_avg.back().push_back(TableCell(na, format_s));
        table_data_max.back().push_back(TableCell(na, format_s));
      } else {
        table_data_min.back().push_back(TableCell(GetTotalMinExt3600(tag), format_f));
        table_data_avg.back().push_back(TableCell(GetTotalAvgExt3600(tag), format_f));
        table_data_max.back().push_back(TableCell(GetTotalMaxExt3600(tag), format_f));
      }

      table_all.AddRows(table_data_spl);
      table_all.AddRows(table_data_min);
      table_all.AddRows(table_data_avg);
      table_all.AddRows(table_data_max);
    }
  }

  out += table_all.GenerateTable(HEADER).c_str();

  if (details) {
    // Collect uids and gids inside the lock and the do the translation outside
    // the lock
    std::set<uid_t> set_uids;
    std::set<gid_t> set_gids;

    for (auto tuit = StatAvgUid.begin(); tuit != StatAvgUid.end(); tuit++) {
      for (auto it = tuit->second.begin(); it != tuit->second.end(); ++it) {
        set_uids.insert(it->first);
      }
    }

    for (auto tuit_ext = StatExtUid.begin(); tuit_ext != StatExtUid.end();
         tuit_ext++) {
      for (auto it = tuit_ext->second.begin(); it != tuit_ext->second.end(); ++it) {
        set_uids.insert(it->first);
      }
    }

    for (auto tgit = StatAvgGid.begin(); tgit != StatAvgGid.end(); tgit++) {
      for (auto it = tgit->second.begin(); it != tgit->second.end(); ++it) {
        set_gids.insert(it->first);
      }
    }

    for (auto tgit_ext = StatExtGid.begin(); tgit_ext != StatExtGid.end();
         tgit_ext++) {
      for (auto it = tgit_ext->second.begin(); it != tgit_ext->second.end(); ++it) {
        set_gids.insert(it->first);
      }
    }

    mMutex.UnLock();
    std::map<uid_t, std::string> umap;
    std::map<gid_t, std::string> gmap;

    for (const auto numeric_uid : set_uids) {
      int terrc = 0;
      std::string username = eos::common::Mapping::UidToUserName(numeric_uid, terrc);
      umap[numeric_uid] = username;
    }

    for (const auto numeric_gid : set_gids) {
      int terrc = 0;
      std::string groupname = eos::common::Mapping::GidToGroupName(numeric_gid,
                              terrc);
      gmap[numeric_gid] = groupname;
    }

    mMutex.Lock();
    //! User statistic
    TableFormatterBase table_user;

    if (!monitoring) {
      table_user.SetHeader({
        std::make_tuple("user", 5, format_ss),
        std::make_tuple("command", 24, format_cmd),
        std::make_tuple("sum", 8, format_l),
        std::make_tuple("5s", 8, format_f),
        std::make_tuple("1min", 8, format_f),
        std::make_tuple("5min", 8, format_f),
        std::make_tuple("1h", 8, format_f)
      });
    } else {
      table_user.SetHeader({
        std::make_tuple("uid", 0, format_ss),
        std::make_tuple("cmd", 0, format_s),
        std::make_tuple("total", 0, format_l),
        std::make_tuple("5s", 0, format_f),
        std::make_tuple("60s", 0, format_f),
        std::make_tuple("300s", 0, format_f),
        std::make_tuple("3600s", 0, format_f)
      });
    }

    std::vector<std::tuple<int, std::string, std::string, unsigned long long,
        double, double, double, double>> table_data;
    std::vector<std::tuple<int, std::string, std::string, double, double,
        double, double, double, double, double, double, double, double,
        double, double, double, double, double, double>> table_data_ext;

    for (auto tuit = StatAvgUid.begin(); tuit != StatAvgUid.end(); tuit++) {
      for (auto it = tuit->second.begin(); it != tuit->second.end(); ++it) {
        std::string username;

        if (numerical) {
          username = std::to_string(it->first);
        } else {
          username = umap.count(it->first) ? umap[it->first] :
                     eos::common::StringConversion::GetSizeString(username,
                         (unsigned long long)it->first);
        }

        table_data.push_back(std::make_tuple(0, username, tuit->first.c_str(),
                                             StatsUid[tuit->first.c_str()][it->first],
                                             it->second.GetAvg5(), it->second.GetAvg60(),
                                             it->second.GetAvg300(), it->second.GetAvg3600()));
      }
    }

    for (auto tuit_ext = StatExtUid.begin(); tuit_ext != StatExtUid.end();
         tuit_ext++) {
      for (auto it = tuit_ext->second.begin(); it != tuit_ext->second.end(); ++it) {
        std::string username;

        if (numerical) {
          username = std::to_string(it->first);
        } else {
          username = umap.count(it->first) ? umap[it->first] :
                     eos::common::StringConversion::GetSizeString(username,
                         (unsigned long long)it->first);
        }

        table_data_ext.push_back(std::make_tuple(
                                   0, username, tuit_ext->first.c_str(),
                                   it->second.GetN5(), it->second.GetAvg5(),
                                   it->second.GetMin5(), it->second.GetMax5(),
                                   it->second.GetN60(), it->second.GetAvg60(),
                                   it->second.GetMin60(), it->second.GetMax60(),
                                   it->second.GetN300(), it->second.GetAvg300(),
                                   it->second.GetMin300(), it->second.GetMax300(),
                                   it->second.GetN3600(), it->second.GetAvg3600(),
                                   it->second.GetMin3600(), it->second.GetMax3600()));
      }
    }

    //! Group statistic
    TableFormatterBase table_group;

    if (!monitoring) {
      table_group.SetHeader({
        std::make_tuple("group", 5, format_ss),
        std::make_tuple("command", 24, format_cmd),
        std::make_tuple("sum", 8, format_l),
        std::make_tuple("5s", 8, format_f),
        std::make_tuple("1min", 8, format_f),
        std::make_tuple("5min", 8, format_f),
        std::make_tuple("1h", 8, format_f)
      });
    } else {
      table_group.SetHeader({
        std::make_tuple("gid", 0, format_ss),
        std::make_tuple("cmd", 0, format_s),
        std::make_tuple("total", 0, format_l),
        std::make_tuple("5s", 0, format_f),
        std::make_tuple("60s", 0, format_f),
        std::make_tuple("300s", 0, format_f),
        std::make_tuple("3600s", 0, format_f)
      });
    }

    for (auto tgit = StatAvgGid.begin(); tgit != StatAvgGid.end(); tgit++) {
      for (auto it = tgit->second.begin(); it != tgit->second.end(); ++it) {
        std::string groupname;

        if (numerical) {
          groupname = std::to_string(it->first);
        } else {
          groupname = gmap.count(it->first) ? gmap[it->first] :
                      eos::common::StringConversion::GetSizeString(groupname,
                          (unsigned long long)it->first);
        }

        table_data.push_back(std::make_tuple(1, groupname, tgit->first.c_str(),
                                             StatsGid[tgit->first.c_str()][it->first],
                                             it->second.GetAvg5(), it->second.GetAvg60(),
                                             it->second.GetAvg300(), it->second.GetAvg3600()));
      }
    }

    for (auto tgit_ext = StatExtGid.begin(); tgit_ext != StatExtGid.end();
         tgit_ext++) {
      for (auto it = tgit_ext->second.begin(); it != tgit_ext->second.end(); ++it) {
        std::string groupname;

        if (numerical) {
          groupname = std::to_string(it->first);
        } else {
          groupname = gmap.count(it->first) ? gmap[it->first] :
                      eos::common::StringConversion::GetSizeString(groupname,
                          (unsigned long long)it->first);
        }

        table_data_ext.push_back(std::make_tuple(
                                   1, groupname, tgit_ext->first.c_str(),
                                   it->second.GetN5(), it->second.GetAvg5(),
                                   it->second.GetMin5(), it->second.GetMax5(),
                                   it->second.GetN60(), it->second.GetAvg60(),
                                   it->second.GetMin60(), it->second.GetMax60(),
                                   it->second.GetN300(), it->second.GetAvg300(),
                                   it->second.GetMin300(), it->second.GetMax300(),
                                   it->second.GetN3600(), it->second.GetAvg3600(),
                                   it->second.GetMin3600(), it->second.GetMax3600()));
      }
    }

    // Data sorting
    std::sort(table_data.begin(), table_data.end());
    std::sort(table_data_ext.begin(), table_data_ext.end());

    // Output user and group statistic
    for (int i = 0; i <= 1; i++) {
      for (auto it : table_data) {
        if (std::get<0>(it) == i) {
          TableData table_data_sorted;
          table_data_sorted.emplace_back();
          table_data_sorted.back().push_back(TableCell(std::get<1>(it), format_ss));
          table_data_sorted.back().push_back(TableCell(std::get<2>(it), format_s));
          table_data_sorted.back().push_back(TableCell(std::get<3>(it), format_l));
          table_data_sorted.back().push_back(TableCell(std::get<4>(it), format_f));
          table_data_sorted.back().push_back(TableCell(std::get<5>(it), format_f));
          table_data_sorted.back().push_back(TableCell(std::get<6>(it), format_f));
          table_data_sorted.back().push_back(TableCell(std::get<7>(it), format_f));

          if (i == 0) {
            table_user.AddRows(table_data_sorted);
          } else if (i == 1) {
            table_group.AddRows(table_data_sorted);
          }
        }
      }

      for (auto it : table_data_ext) {
        if (std::get<0>(it) == i) {
          TableData table_data_spl, table_data_min, table_data_avg, table_data_max;
          table_data_spl.emplace_back();
          table_data_min.emplace_back();
          table_data_avg.emplace_back();
          table_data_max.emplace_back();
          table_data_spl.back().push_back(TableCell(std::get<1>(it), format_ss));
          table_data_min.back().push_back(TableCell(std::get<1>(it), format_ss));
          table_data_avg.back().push_back(TableCell(std::get<1>(it), format_ss));
          table_data_max.back().push_back(TableCell(std::get<1>(it), format_ss));
          std::string tag = std::get<2>(it);
          std::string tag_spl = tag, tag_min = tag, tag_avg = tag, tag_max = tag;
          tag_spl += ":spl";
          tag_min += ":min";
          tag_avg += ":avg";
          tag_max += ":max";
          table_data_spl.back().push_back(TableCell(tag_spl, format_s));
          table_data_min.back().push_back(TableCell(tag_min, format_s));
          table_data_avg.back().push_back(TableCell(tag_avg, format_s));
          table_data_max.back().push_back(TableCell(tag_max, format_s));
          table_data_spl.back().push_back(TableCell("", "", "", true));
          table_data_min.back().push_back(TableCell("", "", "", true));
          table_data_avg.back().push_back(TableCell("", "", "", true));
          table_data_max.back().push_back(TableCell("", "", "", true));
          table_data_spl.back().push_back(TableCell(std::get<3>(it), format_f));

          if (std::get<3>(it) < 1) {
            table_data_min.back().push_back(TableCell(na, format_s));
            table_data_avg.back().push_back(TableCell(na, format_s));
            table_data_max.back().push_back(TableCell(na, format_s));
          } else {
            table_data_min.back().push_back(TableCell(std::get<4>(it), format_f));
            table_data_avg.back().push_back(TableCell(std::get<5>(it), format_f));
            table_data_max.back().push_back(TableCell(std::get<6>(it), format_f));
          }

          table_data_spl.back().push_back(TableCell(std::get<7>(it), format_f));

          if (std::get<7>(it) < 1) {
            table_data_min.back().push_back(TableCell(na, format_s));
            table_data_avg.back().push_back(TableCell(na, format_s));
            table_data_max.back().push_back(TableCell(na, format_s));
          } else {
            table_data_min.back().push_back(TableCell(std::get<8>(it), format_f));
            table_data_avg.back().push_back(TableCell(std::get<9>(it), format_f));
            table_data_max.back().push_back(TableCell(std::get<10>(it), format_f));
          }

          table_data_spl.back().push_back(TableCell(std::get<11>(it), format_f));

          if (std::get<11>(it) < 1) {
            table_data_min.back().push_back(TableCell(na, format_s));
            table_data_avg.back().push_back(TableCell(na, format_s));
            table_data_max.back().push_back(TableCell(na, format_s));
          } else {
            table_data_min.back().push_back(TableCell(std::get<12>(it), format_f));
            table_data_avg.back().push_back(TableCell(std::get<13>(it), format_f));
            table_data_max.back().push_back(TableCell(std::get<14>(it), format_f));
          }

          table_data_spl.back().push_back(TableCell(std::get<15>(it), format_f));

          if (std::get<15>(it) < 1) {
            table_data_min.back().push_back(TableCell(na, format_s));
            table_data_avg.back().push_back(TableCell(na, format_s));
            table_data_max.back().push_back(TableCell(na, format_s));
          } else {
            table_data_min.back().push_back(TableCell(std::get<16>(it), format_f));
            table_data_avg.back().push_back(TableCell(std::get<17>(it), format_f));
            table_data_max.back().push_back(TableCell(std::get<18>(it), format_f));
          }

          if (i == 0) {
            table_user.AddRows(table_data_spl);
            table_user.AddRows(table_data_min);
            table_user.AddRows(table_data_avg);
            table_user.AddRows(table_data_max);
          } else if (i == 1) {
            table_group.AddRows(table_data_spl);
            table_group.AddRows(table_data_min);
            table_group.AddRows(table_data_avg);
            table_group.AddRows(table_data_max);
          }
        }
      }
    }

    out += table_user.GenerateTable(HEADER).c_str();
    out += table_group.GenerateTable(HEADER).c_str();
  }

  mMutex.UnLock();
}

/*----------------------------------------------------------------------------*/
void
Stat::Circulate(ThreadAssistant& assistant) noexcept
{
  unsigned long long l1 = 0;
  unsigned long long l2 = 0;
  unsigned long long l3 = 0;
  unsigned long long l1tmp, l2tmp, l3tmp;
#ifdef EOS_INSTRUMENTED_RWMUTEX
  unsigned long long qu1 = 0;
  unsigned long long qu2 = 0;
  unsigned long long ns1 = 0;
  unsigned long long ns2 = 0;
  unsigned long long view1 = 0;
  unsigned long long view2 = 0;
  eos::common::RWMutex::TimingStats qu12stmp, ns12stmp, view12stmp;
  unsigned long long ns1tmp = 0ull, ns2tmp = 0ull, view1tmp = 0ull,
                     view2tmp = 0ull, qu1tmp = 0ull, qu2tmp = 0ull;
#endif

  // Empty the circular buffer and extract some Mq statistic values
  while (!assistant.terminationRequested()) {
    assistant.wait_for(std::chrono::milliseconds(512));
    // --------------------------------------------
    // mq statistics extraction
    l1tmp = XrdMqSharedHash::sSetCounter.load();
    l2tmp = XrdMqSharedHash::sSetNLCounter.load();
    l3tmp = XrdMqSharedHash::sGetCounter.load();
#ifdef EOS_INSTRUMENTED_RWMUTEX
    eos::common::RWMutex* fs_mtx = &FsView::gFsView.ViewMutex;
    eos::common::RWMutex* quota_mtx = &Quota::pMapMutex;
    eos::common::RWMutex* ns_mtx = &gOFS->eosViewRWMutex;
    // fsview statistics extraction
    view1tmp = fs_mtx->GetReadLockCounter();
    view2tmp = fs_mtx->GetWriteLockCounter();
    fs_mtx->GetTimingStatistics(view12stmp);
    fs_mtx->ResetTimingStatistics();
    // namespace lock statistics extraction
    ns1tmp = ns_mtx->GetReadLockCounter();
    ns2tmp = ns_mtx->GetWriteLockCounter();
    ns_mtx->GetTimingStatistics(ns12stmp);
    ns_mtx->ResetTimingStatistics();
    // quota lock statistics extraction
    qu1tmp = quota_mtx->GetReadLockCounter();
    qu2tmp = quota_mtx->GetWriteLockCounter();
    quota_mtx->GetTimingStatistics(qu12stmp);
    quota_mtx->ResetTimingStatistics();
#endif
    Add("HashSet", 0, 0, l1tmp - l1);
    Add("HashSetNoLock", 0, 0, l2tmp - l2);
    Add("HashGet", 0, 0, l3tmp - l3);
#ifdef EOS_INSTRUMENTED_RWMUTEX
    Add("ViewLockR", 0, 0, view1tmp - view1);
    Add("ViewLockW", 0, 0, view2tmp - view2);
    Add("NsLockR", 0, 0, ns1tmp - ns1);
    Add("NsLockW", 0, 0, ns2tmp - ns2);
    Add("QuotaLockR", 0, 0, qu1tmp - qu1);
    Add("QuotaLockW", 0, 0, qu2tmp - qu2);
    AddExt("ViewLockRWait", 0, 0, (unsigned long) view12stmp.readLockCounterSample,
           view12stmp.averagewaitread, view12stmp.minwaitread, view12stmp.maxwaitread);
    AddExt("ViewLockWWait", 0, 0, (unsigned long) view12stmp.writeLockCounterSample,
           view12stmp.averagewaitwrite, view12stmp.minwaitwrite, view12stmp.maxwaitwrite);
    AddExt("NsLockRWait", 0, 0, (unsigned long) ns12stmp.readLockCounterSample,
           ns12stmp.averagewaitread, ns12stmp.minwaitread, ns12stmp.maxwaitread);
    AddExt("NsLockWWait", 0, 0, (unsigned long) ns12stmp.writeLockCounterSample,
           ns12stmp.averagewaitwrite, ns12stmp.minwaitwrite, ns12stmp.maxwaitwrite);
    AddExt("QuotaLockRWait", 0, 0, (unsigned long) qu12stmp.readLockCounterSample,
           qu12stmp.averagewaitread, ns12stmp.minwaitread, ns12stmp.maxwaitread);
    AddExt("QuotaLockWWait", 0, 0, (unsigned long) qu12stmp.writeLockCounterSample,
           qu12stmp.averagewaitwrite, ns12stmp.minwaitwrite, ns12stmp.maxwaitwrite);
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
    XrdSysMutexHelper lock(mMutex);
    time_t now = time(NULL);

    // loop over tags
    for (auto tit = StatAvgUid.begin(); tit != StatAvgUid.end(); ++tit) {
      // loop over vids
      for (auto it = tit->second.begin(); it != tit->second.end(); ++it) {
        it->second.StampZero(now);
      }
    }

    for (auto tit = StatAvgGid.begin(); tit != StatAvgGid.end(); ++tit) {
      // loop over vids
      for (auto it = tit->second.begin(); it != tit->second.end(); ++it) {
        it->second.StampZero(now);
      }
    }

    for (auto tit_ext = StatExtGid.begin(); tit_ext != StatExtGid.end();
         ++tit_ext) {
      // loop over vids
      for (auto it = tit_ext->second.begin(); it != tit_ext->second.end(); ++it) {
        it->second.StampZero(now);
      }
    }

    for (auto tit_ext = StatExtGid.begin(); tit_ext != StatExtGid.end();
         ++tit_ext) {
      // loop over vids
      for (auto it = tit_ext->second.begin(); it != tit_ext->second.end(); ++it) {
        it->second.StampZero(now);
      }
    }
  }
}

EOSMGMNAMESPACE_END
