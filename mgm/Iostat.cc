// ----------------------------------------------------------------------
// File: Iostat.cc
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


#include "common/table_formatter/TableFormatterBase.hh"
#include "common/Report.hh"
#include "common/Path.hh"
#include "common/JeMallocHandler.hh"
#include "common/Logging.hh"
#include "mgm/Iostat.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/IMaster.hh"
#include "namespace/interface/IView.hh"
#include "namespace/Prefetcher.hh"
#include "mq/ReportListener.hh"
#include "XrdNet/XrdNetUtils.hh"
#include "XrdNet/XrdNetAddr.hh"

EOSMGMNAMESPACE_BEGIN

const char* Iostat::gIostatCollect = "iostat::collect";
const char* Iostat::gIostatReport = "iostat::report";
const char* Iostat::gIostatReportNamespace = "iostat::reportnamespace";
const char* Iostat::gIostatPopularity = "iostat::popularity";
const char* Iostat::gIostatUdpTargetList = "iostat::udptargets";
FILE* Iostat::gOpenReportFD = 0;

/* ------------------------------------------------------------------------- */
Iostat::Iostat():
  mReport(true), mReportNamespace(false), mReportPopularity(true)
{
  mRunning = false;
  mStoreFileName = "";
  // push default domains to watch TODO: make generic
  IoDomains.insert(".ch");
  IoDomains.insert(".it");
  IoDomains.insert(".ru");
  IoDomains.insert(".de");
  IoDomains.insert(".nl");
  IoDomains.insert(".fr");
  IoDomains.insert(".se");
  IoDomains.insert(".ro");
  IoDomains.insert(".su");
  IoDomains.insert(".no");
  IoDomains.insert(".dk");
  IoDomains.insert(".cz");
  IoDomains.insert(".uk");
  IoDomains.insert(".se");
  IoDomains.insert(".org");
  IoDomains.insert(".edu");
  // push default nodes to watch TODO: make generic
  IoNodes.insert("lxplus"); // CERN interactive cluster
  IoNodes.insert("lxb"); // CERN batch cluster
  IoNodes.insert("pb-d-128-141"); // CERN DHCP
  IoNodes.insert("aldaq"); // ALICE DAQ
  IoNodes.insert("cms-cdr"); // CMS DAQ
  IoNodes.insert("pc-tdq"); // ATLAS DAQ

  for (size_t i = 0; i < IOSTAT_POPULARITY_HISTORY_DAYS; i++) {
    IostatPopularity[i].set_deleted_key("");
    IostatPopularity[i].resize(100000);
  }

  IostatLastPopularityBin = 9999999;
}


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
void
Iostat::StartCirculate()
{
  // We have to do after the name of the dump file was set, therefore the
  // StartCirculate is an extra call
  mCirculateThread.reset(&Iostat::Circulate, this);
}


/* ------------------------------------------------------------------------- */
bool
Iostat::Start()
{
  if (!mRunning) {
    mReceivingThread.reset(&Iostat::Receive, this);
    mRunning = true;
    return true;
  } else {
    return false;
  }
}

/* ------------------------------------------------------------------------- */
bool
Iostat::Stop()
{
  if (mRunning) {
    mRunning = false;
    mReceivingThread.join();
    return true;
  } else {
    return false;
  }
}

/* ------------------------------------------------------------------------- */
Iostat::~Iostat()
{
  (void) Stop();
  mCirculateThread.join();
}

/* ------------------------------------------------------------------------- */
void
Iostat::Receive(ThreadAssistant& assistant) noexcept
{ 
  if (gOFS != NULL) {
    
    mq::ReportListener listener(gOFS->MgmOfsBroker.c_str(), gOFS->HostName);
  
    while (!assistant.terminationRequested()) {
      std::string newmessage;
  
      while (listener.fetch(newmessage, assistant)) {
        if (assistant.terminationRequested()) {
          break;
        }
  
        XrdOucString body = newmessage.c_str();
  
        while (body.replace("&&", "&")) {
        }
  
        XrdOucEnv ioreport(body.c_str());
        std::unique_ptr<eos::common::Report> report(new eos::common::Report(ioreport));
        Add("bytes_read", report->uid, report->gid, report->rb, report->ots,
            report->cts);
        Add("bytes_read", report->uid, report->gid, report->rvb_sum, report->ots,
            report->cts);
        Add("bytes_written", report->uid, report->gid, report->wb, report->ots,
            report->cts);
        Add("read_calls", report->uid, report->gid, report->nrc, report->ots,
            report->cts);
        Add("readv_calls", report->uid, report->gid, report->rv_op, report->ots,
            report->cts);
        Add("write_calls", report->uid, report->gid, report->nwc, report->ots,
            report->cts);
        Add("fwd_seeks", report->uid, report->gid, report->nfwds, report->ots,
            report->cts);
        Add("bwd_seeks", report->uid, report->gid, report->nbwds, report->ots,
            report->cts);
        Add("xl_fwd_seeks", report->uid, report->gid, report->nxlfwds, report->ots,
            report->cts);
        Add("xl_bwd_seeks", report->uid, report->gid, report->nxlbwds, report->ots,
            report->cts);
        Add("bytes_fwd_seek", report->uid, report->gid, report->sfwdb, report->ots,
            report->cts);
        Add("bytes_bwd_wseek", report->uid, report->gid, report->sbwdb, report->ots,
            report->cts);
        Add("bytes_xl_fwd_seek", report->uid, report->gid, report->sxlfwdb, report->ots,
            report->cts);
        Add("bytes_xl_bwd_wseek", report->uid, report->gid, report->sxlbwdb,
            report->ots, report->cts);
        Add("disk_time_read", report->uid, report->gid, report->rt,
            report->ots, report->cts);
        Add("disk_time_write", report->uid, report->gid,
            report->wt, report->ots, report->cts);
        {
          // track deletions
          time_t now = time(NULL);
          Add("bytes_deleted", 0, 0, report->dsize, now - 30, now);
          Add("files_deleted", 0, 0, 1, now - 30, now);
        }
        {
          // Do the UDP broadcasting here
          XrdSysMutexHelper mLock(mBcastMutex);
  
          if (mUdpPopularityTarget.size()) {
            UdpBroadCast(report.get());
          }
        }
  
        // do the domain accounting here
        if (report->path.substr(0, 11) == "/replicate:") {
          // check if this is a replication path
          // push into the 'eos' domain
          Mutex.Lock();
  
          if (report->rb) {
            IostatAvgDomainIOrb["eos"].Add(report->rb, report->ots, report->cts);
          }
  
          if (report->wb) {
            IostatAvgDomainIOwb["eos"].Add(report->wb, report->ots, report->cts);
          }
  
          Mutex.UnLock();
        } else {
          bool dfound = false;
  
          if (mReportPopularity) {
            // do the popularity accounting here for everything which is not replication!
            AddToPopularity(report->path, report->rb, report->ots, report->cts);
          }
  
          size_t pos = 0;
  
          if ((pos = report->sec_domain.rfind(".")) != std::string::npos) {
            // we can sort in by domain
            std::string sdomain = report->sec_domain.substr(pos);
  
            if (IoDomains.find(sdomain) != IoDomains.end()) {
              Mutex.Lock();
  
              if (report->rb) {
                IostatAvgDomainIOrb[sdomain].Add(report->rb, report->ots, report->cts);
              }
  
              if (report->wb) {
                IostatAvgDomainIOwb[sdomain].Add(report->wb, report->ots, report->cts);
              }
  
              Mutex.UnLock();
              dfound = true;
            }
          }
  
          // do the node accounting here - keep the node list small !!!
          std::set<std::string>::const_iterator nit;
  
          for (nit = IoNodes.begin(); nit != IoNodes.end(); nit++) {
            if (*nit == report->sec_host.substr(0, nit->length())) {
              Mutex.Lock();
  
              if (report->rb) {
                IostatAvgDomainIOrb[*nit].Add(report->rb, report->ots, report->cts);
              }
  
              if (report->wb) {
                IostatAvgDomainIOwb[*nit].Add(report->wb, report->ots, report->cts);
              }
  
              Mutex.UnLock();
              dfound = true;
            }
          }
          if (!dfound) {
            // push into the 'other' domain
            Mutex.Lock();
  
            if (report->rb) {
              IostatAvgDomainIOrb["other"].Add(report->rb, report->ots, report->cts);
            }
  
            if (report->wb) {
              IostatAvgDomainIOwb["other"].Add(report->wb, report->ots, report->cts);
            }
  
            Mutex.UnLock();
          }
        }
  
        // do the application accounting here
        std::string apptag = "other";
  
        if (report->sec_app.length()) {
          apptag = report->sec_app;
        }
  
        // Push into app accounting
        Mutex.Lock();
  
        if (report->rb) {
          IostatAvgAppIOrb[apptag].Add(report->rb, report->ots, report->cts);
        }
  
        if (report->wb) {
          IostatAvgAppIOwb[apptag].Add(report->wb, report->ots, report->cts);
        }
  
        Mutex.UnLock();
  
        if (mReport) {
          // add the record to a daily report log file
          static XrdOucString openreportfile = "";
          time_t now = time(NULL);
          struct tm nowtm;
          XrdOucString reportfile = "";

          if (localtime_r(&now, &nowtm)) {
            static char logfile[4096];
            snprintf(logfile, sizeof(logfile) - 1, "%s/%04u/%02u/%04u%02u%02u.eosreport",
                     gOFS->IoReportStorePath.c_str(),
                     1900 + nowtm.tm_year,
                     nowtm.tm_mon + 1,
                     1900 + nowtm.tm_year,
                     nowtm.tm_mon + 1,
                     nowtm.tm_mday);
            reportfile = logfile;

            if (reportfile == openreportfile) {
              // just add it here;
              if (gOpenReportFD) {
                fprintf(gOpenReportFD, "%s\n", body.c_str());
                fflush(gOpenReportFD);
              }
            } else {
              Mutex.Lock();
  
              if (gOpenReportFD) {
                fclose(gOpenReportFD);
              }
  
              eos::common::Path cPath(reportfile.c_str());
  
              if (cPath.MakeParentPath(S_IRWXU | S_IRGRP | S_IXGRP)) {
                gOpenReportFD = fopen(reportfile.c_str(), "a+");
  
                if (gOpenReportFD) {
                  fprintf(gOpenReportFD, "%s\n", body.c_str());
                  fflush(gOpenReportFD);
                }
  
                openreportfile = reportfile;
              }
  
              Mutex.UnLock();
            }
          }
        }

        if (mReportNamespace) {
          // add the record into the report namespace file
          char path[4096];
          snprintf(path, sizeof(path) - 1, "%s/%s", gOFS->IoReportStorePath.c_str(),
                   report->path.c_str());
          eos::common::Path cPath(path);
  
          if (cPath.MakeParentPath(S_IRWXU | S_IRGRP | S_IXGRP)) {
            FILE* freport = fopen(path, "a+");
  
            if (freport) {
              fprintf(freport, "%s\n", body.c_str());
              fclose(freport);
            }
          }
        }
      }

      assistant.wait_for(std::chrono::seconds(1));
    }
  }
  eos_static_info("%s", "msg=\"stopping iostat receiver thread\"");
}


/* ------------------------------------------------------------------------- */
void
Iostat::WriteRecord(std::string& record)
{
  Mutex.Lock();

  if (gOpenReportFD) {
    fprintf(gOpenReportFD, "%s\n", record.c_str());
    fflush(gOpenReportFD);
  }

  Mutex.UnLock();
}

/* ------------------------------------------------------------------------- */
void
Iostat::PrintOut(XrdOucString& out, bool summary, bool details,
                 bool monitoring, bool numerical, bool top,
                 bool domain, bool apps, XrdOucString option)
{
  Mutex.Lock();
  std::string format_s = (!monitoring ? "s" : "os");
  std::string format_ss = (!monitoring ? "-s" : "os");
  std::string format_l = (!monitoring ? "+l" : "ol");
  std::string format_ll = (!monitoring ? "l." : "ol");
  std::vector<std::string> tags;

  for (auto tit = IostatUid.begin(); tit != IostatUid.end(); ++tit) {
    tags.push_back(tit->first);
  }

  std::sort(tags.begin(), tags.end());

  if (summary) {
    TableFormatterBase table;
    TableData table_data;

    if (!monitoring) {
      table.SetHeader({
        std::make_tuple("who", 3, format_ss),
        std::make_tuple("io value", 24, format_s),
        std::make_tuple("sum", 8, format_l),
        std::make_tuple("1min", 8, format_l),
        std::make_tuple("5min", 8, format_l),
        std::make_tuple("1h", 8, format_l),
        std::make_tuple("24h", 8, format_l)
      });
    } else {
      table.SetHeader({
        std::make_tuple("uid", 0, format_ss),
        std::make_tuple("gid", 0, format_s),
        std::make_tuple("measurement", 0, format_s),
        std::make_tuple("total", 0, format_l),
        std::make_tuple("60s", 0, format_l),
        std::make_tuple("300s", 0, format_l),
        std::make_tuple("3600s", 0, format_l),
        std::make_tuple("86400s", 0, format_l)
      });
    }

    for (const auto& elem : tags) {
      const char* tag = elem.c_str();
      table_data.emplace_back();
      TableRow& row = table_data.back();
      row.emplace_back("all", format_ss);

      if (monitoring) {
        row.emplace_back("all", format_s);
      }

      row.emplace_back(tag, format_s);
      row.emplace_back(GetTotal(tag), format_ll);
      row.emplace_back(GetTotalAvg60(tag), format_ll);
      row.emplace_back(GetTotalAvg300(tag), format_ll);
      row.emplace_back(GetTotalAvg3600(tag), format_ll);
      row.emplace_back(GetTotalAvg86400(tag), format_ll);
    }

    table.AddRows(table_data);
    out += table.GenerateTable(HEADER).c_str();
    table_data.clear();
    //! UDP Popularity Broadcast Target
    {
      XrdSysMutexHelper mLock(mBcastMutex);

      if (!mUdpPopularityTarget.empty()) {
        TableFormatterBase table_udp;

        if (!monitoring) {
          table_udp.SetHeader({
            std::make_tuple("UDP Popularity Broadcast Target", 32, format_ss)
          });
        } else {
          table_udp.SetHeader({ std::make_tuple("udptarget", 0, format_ss) });
        }

        for (const auto& elem : mUdpPopularityTarget) {
          table_data.emplace_back();
          table_data.back().emplace_back(elem.c_str(), format_ss);
        }

        table_udp.AddRows(table_data);
        out += table_udp.GenerateTable(HEADER).c_str();
      }
    }
  }

  if (details) {
    std::vector<std::tuple<std::string, std::string, unsigned long long,
        double, double, double, double>> uidout, gidout;
    //! User statistic
    TableFormatterBase table_user;
    TableData table_data;

    if (!monitoring) {
      table_user.SetHeader({
        std::make_tuple("user", 4, format_ss),
        std::make_tuple("io value", 24, format_s),
        std::make_tuple("sum", 8, format_l),
        std::make_tuple("1min", 8, format_l),
        std::make_tuple("5min", 8, format_l),
        std::make_tuple("1h", 8, format_l),
        std::make_tuple("24h", 8, format_l)
      });
    } else {
      table_user.SetHeader({
        std::make_tuple("uid", 0, format_ss),
        std::make_tuple("measurement", 0, format_s),
        std::make_tuple("total", 0, format_l),
        std::make_tuple("60s", 0, format_l),
        std::make_tuple("300s", 0, format_l),
        std::make_tuple("3600s", 0, format_l),
        std::make_tuple("86400s", 0, format_l)
      });
    }

    for (auto tuit = IostatAvgUid.begin(); tuit != IostatAvgUid.end(); tuit++) {
      for (auto it = tuit->second.begin(); it != tuit->second.end(); ++it) {
        std::string username;

        if (numerical) {
          username = std::to_string(it->first);
        } else {
          int terrc = 0;
          username = eos::common::Mapping::UidToUserName(it->first, terrc);
        }

        uidout.emplace_back(std::make_tuple(username, tuit->first.c_str(),
                                            IostatUid[tuit->first][it->first],
                                            it->second.GetAvg60(), it->second.GetAvg300(),
                                            it->second.GetAvg3600(), it->second.GetAvg86400()));
      }
    }

    std::sort(uidout.begin(), uidout.end());

    for (auto& tup : uidout) {
      table_data.emplace_back();
      TableRow& row = table_data.back();
      row.emplace_back(std::get<0>(tup), format_ss);
      row.emplace_back(std::get<1>(tup), format_s);
      row.emplace_back(std::get<2>(tup), format_l);
      row.emplace_back(std::get<3>(tup), format_l);
      row.emplace_back(std::get<4>(tup), format_l);
      row.emplace_back(std::get<5>(tup), format_l);
      row.emplace_back(std::get<6>(tup), format_l);
    }

    table_user.AddRows(table_data);
    out += table_user.GenerateTable(HEADER).c_str();
    table_data.clear();
    //! Group statistic
    TableFormatterBase table_group;

    if (!monitoring) {
      table_group.SetHeader({
        std::make_tuple("group", 5, format_ss),
        std::make_tuple("io value", 24, format_s),
        std::make_tuple("sum", 8, format_l),
        std::make_tuple("1min", 8, format_l),
        std::make_tuple("5min", 8, format_l),
        std::make_tuple("1h", 8, format_l),
        std::make_tuple("24h", 8, format_l)
      });
    } else {
      table_group.SetHeader({
        std::make_tuple("gid", 0, format_ss),
        std::make_tuple("measurement", 0, format_s),
        std::make_tuple("total", 0, format_l),
        std::make_tuple("60s", 0, format_l),
        std::make_tuple("300s", 0, format_l),
        std::make_tuple("3600s", 0, format_l),
        std::make_tuple("86400s", 0, format_l)
      });
    }

    for (auto tgit = IostatAvgGid.begin(); tgit != IostatAvgGid.end(); tgit++) {
      for (auto it = tgit->second.begin(); it != tgit->second.end(); ++it) {
        std::string groupname;

        if (numerical) {
          groupname = std::to_string(it->first);
        } else {
          int terrc = 0;
          groupname = eos::common::Mapping::GidToGroupName(it->first, terrc);
        }

        gidout.emplace_back(std::make_tuple(groupname, tgit->first.c_str(),
                                            IostatGid[tgit->first][it->first],
                                            it->second.GetAvg60(), it->second.GetAvg300(),
                                            it->second.GetAvg3600(), it->second.GetAvg86400()));
      }
    }

    std::sort(gidout.begin(), gidout.end());

    for (auto& tup : gidout) {
      table_data.emplace_back();
      TableRow& row = table_data.back();
      row.emplace_back(std::get<0>(tup), format_ss);
      row.emplace_back(std::get<1>(tup), format_s);
      row.emplace_back(std::get<2>(tup), format_l);
      row.emplace_back(std::get<3>(tup), format_l);
      row.emplace_back(std::get<4>(tup), format_l);
      row.emplace_back(std::get<5>(tup), format_l);
      row.emplace_back(std::get<6>(tup), format_l);
    }

    table_group.AddRows(table_data);
    out += table_group.GenerateTable(HEADER).c_str();
  }

  if (top) {
    TableFormatterBase table;
    TableData table_data;

    if (!monitoring) {
      table.SetHeader({
        std::make_tuple("io value", 18, format_ss),
        std::make_tuple("ranking by", 10, format_s),
        std::make_tuple("rank", 8, format_ll),
        std::make_tuple("who", 4, format_s),
        std::make_tuple("sum", 8, format_l)
      });
    } else {
      table.SetHeader({
        std::make_tuple("measurement", 0, format_ss),
        std::make_tuple("rank", 0, format_ll),
        std::make_tuple("uid", 0, format_s),
        std::make_tuple("gid", 0, format_s),
        std::make_tuple("counter", 0, format_l)
      });
    }

    for (auto it = tags.begin(); it != tags.end(); ++it) {
      std::vector <std::tuple<unsigned long long, uid_t>> uidout, gidout;
      table.AddSeparator();

      // by uid name
      for (auto sit : IostatUid[*it]) {
        uidout.push_back(std::make_tuple(sit.second, sit.first));
      }

      std::sort(uidout.begin(), uidout.end());
      std::reverse(uidout.begin(), uidout.end());
      int topplace = 0;

      for (auto sit : uidout) {
        topplace++;
        uid_t uid = std::get<1>(sit);
        unsigned long long counter = std::get<0>(sit);
        std::string username;

        if (numerical) {
          username = std::to_string(uid);
        } else {
          int terrc = 0;
          username = eos::common::Mapping::UidToUserName(uid, terrc);
        }

        table_data.emplace_back();
        TableRow& row = table_data.back();
        row.emplace_back(it->c_str(), format_ss);

        if (!monitoring) {
          row.emplace_back("user", format_s);
        }

        row.emplace_back(topplace, format_ll);
        row.emplace_back(username, format_s);

        if (monitoring) {
          row.emplace_back("", "", "", true);
        }

        row.emplace_back(counter, format_l);
      }

      // by gid name
      for (auto sit : IostatGid[*it]) {
        gidout.push_back(std::make_tuple(sit.second, sit.first));
      }

      std::sort(gidout.begin(), gidout.end());
      std::reverse(gidout.begin(), gidout.end());
      topplace = 0;

      for (auto sit : gidout) {
        topplace++;
        uid_t gid = std::get<1>(sit);
        unsigned long long counter = std::get<0>(sit);
        std::string groupname;

        if (numerical) {
          groupname = std::to_string(gid);
        } else {
          int terrc = 0;
          groupname = eos::common::Mapping::GidToGroupName(gid, terrc);
        }

        table_data.emplace_back();
        TableRow& row = table_data.back();
        row.emplace_back(it->c_str(), format_ss);

        if (!monitoring) {
          row.emplace_back("group", format_s);
        }

        row.emplace_back(topplace, format_ll);

        if (monitoring) {
          row.emplace_back("", "", "", true);
        }

        row.emplace_back(groupname, format_s);
        row.emplace_back(counter, format_l);
      }
    }

    table.AddRows(table_data);
    out += table.GenerateTable(HEADER).c_str();
  }

  if (domain) {
    TableFormatterBase table;
    TableData table_data;

    if (!monitoring) {
      table.SetHeader({
        std::make_tuple("io", 3, format_ss),
        std::make_tuple("domain", 24, format_s),
        std::make_tuple("1min", 8, format_l),
        std::make_tuple("5min", 8, format_l),
        std::make_tuple("1h", 8, format_l),
        std::make_tuple("24h", 8, format_l)
      });
    } else {
      table.SetHeader({
        std::make_tuple("measurement", 0, format_ss),
        std::make_tuple("domain", 0, format_s),
        std::make_tuple("60s", 0, format_l),
        std::make_tuple("300s", 0, format_l),
        std::make_tuple("3600s", 0, format_l),
        std::make_tuple("86400s", 0, format_l)
      });
    }

    // IO out bytes
    for (auto it = IostatAvgDomainIOrb.begin(); it != IostatAvgDomainIOrb.end();
         ++it) {
      table_data.emplace_back();
      TableRow& row = table_data.back();
      std::string name = !monitoring ? "out" : "domain_io_out";
      row.emplace_back(name, format_ss);
      row.emplace_back(it->first.c_str(), format_s);
      row.emplace_back(it->second.GetAvg60(), format_l);
      row.emplace_back(it->second.GetAvg300(), format_l);
      row.emplace_back(it->second.GetAvg3600(), format_l);
      row.emplace_back(it->second.GetAvg86400(), format_l);
    }

    // IO in bytes
    for (auto it = IostatAvgDomainIOwb.begin(); it != IostatAvgDomainIOwb.end();
         ++it) {
      table_data.emplace_back();
      TableRow& row = table_data.back();
      std::string name = !monitoring ? "in" : "domain_io_in";
      row.emplace_back(name, format_ss);
      row.emplace_back(it->first.c_str(), format_s);
      row.emplace_back(it->second.GetAvg60(), format_l);
      row.emplace_back(it->second.GetAvg300(), format_l);
      row.emplace_back(it->second.GetAvg3600(), format_l);
      row.emplace_back(it->second.GetAvg86400(), format_l);
    }

    table.AddRows(table_data);
    out += table.GenerateTable(HEADER).c_str();
  }

  if (apps) {
    TableFormatterBase table;
    TableData table_data;

    if (!monitoring) {
      table.SetHeader({
        std::make_tuple("io", 3, format_ss),
        std::make_tuple("application", 24, format_s),
        std::make_tuple("1min", 8, format_l),
        std::make_tuple("5min", 8, format_l),
        std::make_tuple("1h", 8, format_l),
        std::make_tuple("24h", 8, format_l)
      });
    } else {
      table.SetHeader({
        std::make_tuple("measurement", 0, format_ss),
        std::make_tuple("application", 0, format_s),
        std::make_tuple("60s", 0, format_l),
        std::make_tuple("300s", 0, format_l),
        std::make_tuple("3600s", 0, format_l),
        std::make_tuple("86400s", 0, format_l)
      });
    }

    // IO out bytes
    for (auto it = IostatAvgAppIOrb.begin(); it != IostatAvgAppIOrb.end(); ++it) {
      table_data.emplace_back();
      TableRow& row = table_data.back();
      std::string name = (!monitoring ? "out" : "app_io_out");
      row.emplace_back(name, format_ss);
      row.emplace_back(it->first.c_str(), format_s);
      row.emplace_back(it->second.GetAvg60(), format_l);
      row.emplace_back(it->second.GetAvg300(), format_l);
      row.emplace_back(it->second.GetAvg3600(), format_l);
      row.emplace_back(it->second.GetAvg86400(), format_l);
    }

    // IO in bytes
    for (auto it = IostatAvgAppIOwb.begin(); it != IostatAvgAppIOwb.end(); ++it) {
      table_data.emplace_back();
      TableRow& row = table_data.back();
      std::string name = (!monitoring ? "in" : "app_io_in");
      row.emplace_back(name, format_ss);
      row.emplace_back(it->first.c_str(), format_s);
      row.emplace_back(it->second.GetAvg60(), format_l);
      row.emplace_back(it->second.GetAvg300(), format_l);
      row.emplace_back(it->second.GetAvg3600(), format_l);
      row.emplace_back(it->second.GetAvg86400(), format_l);
    }

    table.AddRows(table_data);
    out += table.GenerateTable(HEADER).c_str();
  }

  Mutex.UnLock();
}

/* ------------------------------------------------------------------------- */
void
Iostat::PrintNs(XrdOucString& out, XrdOucString option)
{
  // ---------------------------------------------------------------------------
  // ! compute and printout the namespace popularity ranking
  // ---------------------------------------------------------------------------
  size_t limit = 10;
  size_t popularitybin = (((time(NULL))) % (IOSTAT_POPULARITY_DAY *
                          IOSTAT_POPULARITY_HISTORY_DAYS)) / IOSTAT_POPULARITY_DAY;
  size_t days = 1;
  time_t tmarker = time(NULL) / IOSTAT_POPULARITY_DAY * IOSTAT_POPULARITY_DAY;
  bool monitoring = false;
  bool bycount = false;
  bool bybytes = false;
  bool hotfiles = false;

  if ((option.find("-m")) != STR_NPOS) {
    monitoring = true;
  }

  if ((option.find("-a")) != STR_NPOS) {
    limit = 999999999;
  }

  if ((option.find("-100")) != STR_NPOS) {
    limit = 100;
  }

  if ((option.find("-1000")) != STR_NPOS) {
    limit = 1000;
  }

  if ((option.find("-10000")) != STR_NPOS) {
    limit = 10000;
  }

  if ((option.find("-n") != STR_NPOS)) {
    bycount = true;
  }

  if ((option.find("-b") != STR_NPOS)) {
    bybytes = true;
  }

  if ((option.find("-w") != STR_NPOS)) {
    days = IOSTAT_POPULARITY_HISTORY_DAYS;
  }

  if (!(bycount || bybytes)) {
    bybytes = bycount = true;
  }

  if ((option.find("-f") != STR_NPOS)) {
    hotfiles = true;
  }

  std::string format_s = !monitoring ? "s" : "os";
  std::string format_ss = !monitoring ? "-s" : "os";
  std::string format_l = !monitoring ? "l" : "ol";
  std::string format_ll = !monitoring ? "-l." : "ol";
  std::string format_lll = !monitoring ? "+l" : "ol";
  std::string unit = !monitoring ? "B" : "";

  //! The 'hotfiles' which are the files with highest number of present file opens
  if (hotfiles) {
    eos::common::RWMutexReadLock rLock(FsView::gFsView.ViewMutex);
    // print the hotfiles report
    std::set<eos::common::FileSystem::fsid_t>::const_iterator it;
    std::vector<std::string> r_open_vector;
    std::vector<std::string> w_open_vector;
    std::string key;
    std::string val;
    TableFormatterBase table;
    TableData table_data;

    if (!monitoring) {
      table.SetHeader({
        std::make_tuple("type", 5, format_ss),
        std::make_tuple("heat", 5, format_s),
        std::make_tuple("fs", 5, format_s),
        std::make_tuple("host", 24, format_s),
        std::make_tuple("path", 24, format_ss)
      });
    } else {
      table.SetHeader({
        std::make_tuple("measurement", 0, format_ss),
        std::make_tuple("access", 0, format_s),
        std::make_tuple("heat", 0, format_s),
        std::make_tuple("fsid", 0, format_l),
        std::make_tuple("path", 0, format_ss),
        std::make_tuple("fxid", 0, format_s)
      });
    }

    for (auto it = FsView::gFsView.mIdView.begin();
         it != FsView::gFsView.mIdView.end(); it++) {
      r_open_vector.clear();
      w_open_vector.clear();
      FileSystem* fs = it->second;

      if (!fs) {
        continue;
      }

      std::string r_open_hotfiles = fs->GetString("stat.ropen.hotfiles");
      std::string w_open_hotfiles = fs->GetString("stat.wopen.hotfiles");
      std::string node_queue = fs->GetString("queue");
      auto it_node = FsView::gFsView.mNodeView.find(node_queue);

      if (it_node == FsView::gFsView.mNodeView.end()) {
        continue;
      }

      // Check if the corresponding node has a heartbeat
      bool hasHeartbeat = it_node->second->HasHeartbeat();

      // we only show the reports from the last minute, there could be pending values
      if (!hasHeartbeat) {
        r_open_hotfiles = "";
        w_open_hotfiles = "";
      }

      if (r_open_hotfiles == " ") {
        r_open_hotfiles = "";
      }

      if (w_open_hotfiles == " ") {
        w_open_hotfiles = "";
      }

      eos::common::StringConversion::Tokenize(r_open_hotfiles, r_open_vector);
      eos::common::StringConversion::Tokenize(w_open_hotfiles, w_open_vector);
      std::string host = fs->GetString("host");
      std::string path;
      std::string id = fs->GetString("id");
      std::vector<std::tuple<std::string, std::string, std::string,
          std::string, std::string>> data;
      std::vector<std::tuple<std::string, std::string, std::string,
          long long unsigned, std::string, std::string>> data_monitoring;

      // Get information for read
      for (size_t i = 0; i < r_open_vector.size(); i++) {
        eos::common::StringConversion::SplitKeyValue(r_open_vector[i], key, val);
        int rank = 0;

        if (key.c_str()) {
          rank = atoi(key.c_str());
        }

        {
          unsigned long long fid = eos::common::FileId::Hex2Fid(val.c_str());
          eos::Prefetcher::prefetchFileMDWithParentsAndWait(gOFS->eosView, fid);
          eos::common::RWMutexReadLock viewLock(gOFS->eosViewRWMutex, __FUNCTION__,
                                                __LINE__, __FILE__);

          try {
            path = gOFS->eosView->getUri(gOFS->eosFileService->getFileMD(fid).get());
          } catch (eos::MDException& e) {
            path = "<undef>";
          }
        }

        if (rank > 1) {
          data.emplace_back(std::make_tuple(
                              "read", key.c_str(), id.c_str(), host.c_str(), path.c_str()));
        }

        data_monitoring.emplace_back(std::make_tuple(
                                       "hotfile", "read", key.c_str(), it->first, path.c_str(), val.c_str()));
      }

      // Get information for write
      for (size_t i = 0; i < w_open_vector.size(); i++) {
        eos::common::StringConversion::SplitKeyValue(w_open_vector[i], key, val);
        int rank = 0;

        if (key.c_str()) {
          rank = atoi(key.c_str());
        }

        {
          unsigned long long fid = eos::common::FileId::Hex2Fid(val.c_str());
          eos::Prefetcher::prefetchFileMDWithParentsAndWait(gOFS->eosView, fid);
          eos::common::RWMutexReadLock viewLock(gOFS->eosViewRWMutex, __FUNCTION__,
                                                __LINE__, __FILE__);

          try {
            path = gOFS->eosView->getUri(gOFS->eosFileService->getFileMD(fid).get());
          } catch (eos::MDException& e) {
            path = "<undef>";
          }
        }

        if (rank > 1) {
          data.emplace_back(std::make_tuple(
                              "write", key.c_str(), id.c_str(), host.c_str(), path.c_str()));
        }

        data_monitoring.emplace_back(std::make_tuple(
                                       "hotfile", "write", key.c_str(), it->first, path.c_str(), val.c_str()));
      }

      // Sort and output
      if (!monitoring) {
        std::sort(data.begin(), data.end());

        for (auto it : data) {
          table_data.emplace_back();
          TableRow& row = table_data.back();
          row.emplace_back(std::get<0>(it), format_ss);
          row.emplace_back(std::get<1>(it), format_s);
          row.emplace_back(std::get<2>(it), format_s);
          row.emplace_back(std::get<3>(it), format_s);
          row.emplace_back(std::get<4>(it), format_ss);
        }
      } else {
        std::sort(data_monitoring.begin(), data_monitoring.end());

        for (auto mdata : data_monitoring) {
          table_data.emplace_back();
          TableRow& row = table_data.back();
          row.emplace_back(std::get<0>(mdata), format_ss);
          row.emplace_back(std::get<1>(mdata), format_s);
          row.emplace_back(std::get<2>(mdata), format_s);
          row.emplace_back(std::get<3>(mdata), format_l);
          row.emplace_back(std::get<4>(mdata), format_ss);
          row.emplace_back(std::get<5>(mdata), format_s);
        }
      }
    }

    table.AddRows(table_data);
    out += table.GenerateTable(HEADER).c_str();
    return;
  }

  //! Namespace IO ranking (popularity)
  for (size_t pbin = 0; pbin < days; pbin++) {
    PopularityMutex.Lock();
    size_t sbin = (IOSTAT_POPULARITY_HISTORY_DAYS + popularitybin - pbin) %
                  IOSTAT_POPULARITY_HISTORY_DAYS;
    google::sparse_hash_map<std::string, struct Popularity>::const_iterator it;
    std::vector<popularity_t> popularity_nread(IostatPopularity[sbin].begin(),
        IostatPopularity[sbin].end());
    std::vector<popularity_t> popularity_rb(IostatPopularity[sbin].begin(),
                                            IostatPopularity[sbin].end());
    // sort them (backwards) by rb or nread
    std::sort(popularity_nread.begin(), popularity_nread.end(),
              PopularityCmp_nread());
    std::sort(popularity_rb.begin(), popularity_rb.end(), PopularityCmp_rb());
    XrdOucString marker = "\n┏━> Today\n";

    switch (pbin) {
    case 1:
      marker = "\n┏━> Yesterday\n";
      break;

    case 2:
      marker = "\n┏━> 2 days ago\n";
      break;

    case 3:
      marker = "\n┏━> 3 days ago\n";
      break;

    case 4:
      marker = "\n┏━> 4 days ago\n";
      break;

    case 5:
      marker = "\n┏━> 5 days ago\n";
      break;

    case 6:
      marker = "\n┏━> 6 days ago\n";
    }

    if (bycount) {
      TableFormatterBase table;
      TableData table_data;

      if (!monitoring) {
        table.SetHeader({
          std::make_tuple("rank", 5, format_ll),
          std::make_tuple("by(read count)", 12, format_s),
          std::make_tuple("read bytes", 10, format_lll),
          std::make_tuple("path", 24, format_ss),
        });
      } else {
        table.SetHeader({
          std::make_tuple("measurement", 0, format_ss),
          std::make_tuple("time", 0, format_lll),
          std::make_tuple("rank", 0, format_ll),
          std::make_tuple("nread", 0, format_lll),
          std::make_tuple("rb", 0, format_lll),
          std::make_tuple("path", 0, format_ss)
        });
      }

      size_t cnt = 0;

      for (auto it : popularity_nread) {
        cnt++;

        if (cnt > limit) {
          break;
        }

        table_data.emplace_back();
        TableRow& row = table_data.back();

        if (monitoring) {
          row.emplace_back("popularitybyaccess", format_ss);
          row.emplace_back((unsigned) tmarker, format_lll);
        }

        row.emplace_back((int) cnt, format_ll);
        row.emplace_back(it.second.nread, format_lll);
        row.emplace_back(it.second.rb, format_lll, unit);
        row.emplace_back(it.first.c_str(), format_s);
      }

      if (cnt > 0) {
        out += !monitoring ? marker : "";
        table.AddRows(table_data);
        out += table.GenerateTable(HEADER).c_str();
      }
    }

    if (bybytes) {
      TableFormatterBase table;
      TableData table_data;

      if (!monitoring) {
        table.SetHeader({
          std::make_tuple("rank", 5, format_ll),
          std::make_tuple("by(read bytes)", 12, format_s),
          std::make_tuple("read count", 10, format_lll),
          std::make_tuple("path", 24, format_ss),
        });
      } else {
        table.SetHeader({
          std::make_tuple("measurement", 0, format_ss),
          std::make_tuple("time", 0, format_lll),
          std::make_tuple("rank", 0, format_ll),
          std::make_tuple("nread", 0, format_lll),
          std::make_tuple("rb", 0, format_lll),
          std::make_tuple("path", 0, format_ss)
        });
      }

      size_t cnt = 0;

      for (auto it : popularity_rb) {
        cnt++;

        if (cnt > limit) {
          break;
        }

        table_data.emplace_back();
        TableRow& row = table_data.back();

        if (monitoring) {
          row.emplace_back("popularitybyvolume", format_ss);
          row.emplace_back((unsigned) tmarker, format_lll);
        }

        row.emplace_back((int) cnt, format_ll);

        if (!monitoring) {
          row.emplace_back(it.second.rb, format_lll, unit);
          row.emplace_back(it.second.nread, format_lll);
        } else {
          row.emplace_back(it.second.nread, format_lll);
          row.emplace_back(it.second.rb, format_lll, unit);
        }

        row.emplace_back(it.first.c_str(), format_s);
      }

      table.AddRows(table_data);
      out += table.GenerateTable(HEADER2).c_str();
    }

    PopularityMutex.UnLock();
  }
}

//------------------------------------------------------------------------------
// Save current uid/gid counters to a dump file
//------------------------------------------------------------------------------
bool
Iostat::Store()
{
  XrdOucString tmpname = mStoreFileName;

  if (!mStoreFileName.length()) {
    return false;
  }

  tmpname += ".tmp";
  FILE* fout = fopen(tmpname.c_str(), "w+");

  if (!fout) {
    return false;
  }

  if (chmod(tmpname.c_str(), S_IRWXU | S_IRGRP | S_IROTH)) {
    fclose(fout);
    return false;
  }

  google::sparse_hash_map<std::string, google::sparse_hash_map<uid_t, unsigned long long> >::iterator
  tuit;
  google::sparse_hash_map<std::string, google::sparse_hash_map<uid_t, unsigned long long> >::iterator
  tgit;
  Mutex.Lock();

  // store user counters
  for (tuit = IostatUid.begin(); tuit != IostatUid.end(); tuit++) {
    for (auto it = tuit->second.begin(); it != tuit->second.end(); ++it) {
      fprintf(fout, "tag=%s&uid=%u&val=%llu\n", tuit->first.c_str(), it->first,
              (unsigned long long)it->second);
    }
  }

  // store group counter
  for (tgit = IostatGid.begin(); tgit != IostatGid.end(); tgit++) {
    for (auto it = tgit->second.begin(); it != tgit->second.end(); ++it) {
      fprintf(fout, "tag=%s&gid=%u&val=%llu\n", tgit->first.c_str(), it->first,
              (unsigned long long)it->second);
    }
  }

  Mutex.UnLock();
  fclose(fout);
  return rename(tmpname.c_str(), mStoreFileName.c_str()) == 0;
}

/* ------------------------------------------------------------------------- */
bool
Iostat::Restore()
{
  // ---------------------------------------------------------------------------
  // ! load current uid/gid counters from a dump file
  // ---------------------------------------------------------------------------
  if (!mStoreFileName.length()) {
    return false;
  }

  FILE* fin = fopen(mStoreFileName.c_str(), "r");

  if (!fin) {
    return false;
  }

  Mutex.Lock();
  int item = 0;
  char line[16384];

  while ((item = fscanf(fin, "%16383s\n", line)) == 1) {
    XrdOucEnv env(line);

    if (env.Get("tag") && env.Get("uid") && env.Get("val")) {
      std::string tag = env.Get("tag");
      uid_t uid = atoi(env.Get("uid"));
      unsigned long long val = strtoull(env.Get("val"), 0, 10);
      IostatUid[tag][uid] = val;
    }

    if (env.Get("tag") && env.Get("gid") && env.Get("val")) {
      std::string tag = env.Get("tag");
      gid_t gid = atoi(env.Get("gid"));
      unsigned long long val = strtoull(env.Get("val"), 0, 10);
      IostatGid[tag][gid] = val;
    }
  }

  Mutex.UnLock();
  fclose(fin);
  return true;
}

/* ------------------------------------------------------------------------- */
bool
Iostat::NamespaceReport(const char* path, XrdOucString& stdOut,
                        XrdOucString& stdErr)
{
  // ---------------------------------------------------------------------------
  // ! print a report on the activity recorded in the namespace on the given path
  // ---------------------------------------------------------------------------
  XrdOucString reportFile;
  reportFile = gOFS->IoReportStorePath.c_str();
  reportFile += "/";
  reportFile += path;
  std::ifstream inFile(reportFile.c_str());
  std::string reportLine;
  unsigned long long totalreadbytes = 0;
  unsigned long long totalwritebytes = 0;
  double totalreadtime = 0;
  double totalwritetime = 0;
  unsigned long long rcount = 0;
  unsigned long long wcount = 0;

  while (std::getline(inFile, reportLine)) {
    XrdOucEnv ioreport(reportLine.c_str());
    auto* report = new eos::common::Report(ioreport);
    report->Dump(stdOut);

    if (!report->wb) {
      rcount++;
      totalreadtime += ((report->cts - report->ots) + (1.0 * (report->ctms -
                        report->otms) / 1000000));
      totalreadbytes += report->rb;
    } else {
      wcount++;
      totalwritetime += ((report->cts - report->ots) + (1.0 *
                         (report->ctms - report->otms) / 1000000));
      totalwritebytes += report->wb;
    }

    delete report;
  }

  stdOut += "----------------------- SUMMARY -------------------\n";
  char summaryline[4096];
  XrdOucString sizestring1, sizestring2;
  snprintf(summaryline, sizeof(summaryline) - 1,
           "| avg. readd: %.02f MB/s | avg. write: %.02f  MB/s | total read: %s | total write: %s | times read: %llu | times written: %llu |\n",
           totalreadtime ? (totalreadbytes / totalreadtime / 1000000.0) : 0,
           totalwritetime ? (totalwritebytes / totalwritetime / 1000000.0) : 0,
           eos::common::StringConversion::GetReadableSizeString(sizestring1,
               totalreadbytes, "B"), eos::common::StringConversion::GetReadableSizeString(
             sizestring2, totalwritebytes, "B"), (unsigned long long)rcount,
           (unsigned long long)wcount);
  stdOut += summaryline;
  return true;
}

//------------------------------------------------------------------------------
//! Circulate the entries to get averages over sec.min.hour and day
//------------------------------------------------------------------------------
void
Iostat::Circulate(ThreadAssistant& assistant) noexcept
{
  unsigned long long sc = 0;

  // empty the circular buffer
  while (!assistant.terminationRequested()) {
    // we store once per minute the current statistics
    if (!(sc % 117)) {
      // save the current state ~ every minute
      if (!Store()) {
        eos_static_err("failed store io stat dump file <%s>", mStoreFileName.c_str());
      }
    }

    sc++;
    assistant.wait_for(std::chrono::milliseconds(512));
    Mutex.Lock();
    google::sparse_hash_map<std::string, google::sparse_hash_map<uid_t, IostatAvg> >::iterator
    tit;
    google::sparse_hash_map<std::string, IostatAvg >::iterator dit;
    time_t now = time(NULL);

    // loop over tags
    for (tit = IostatAvgUid.begin(); tit != IostatAvgUid.end(); ++tit) {
      // loop over vids
      google::sparse_hash_map<uid_t, IostatAvg>::iterator it;

      for (it = tit->second.begin(); it != tit->second.end(); ++it) {
        it->second.StampZero(now);
      }
    }

    for (tit = IostatAvgGid.begin(); tit != IostatAvgGid.end(); ++tit) {
      // loop over vids
      google::sparse_hash_map<uid_t, IostatAvg>::iterator it;

      for (it = tit->second.begin(); it != tit->second.end(); ++it) {
        it->second.StampZero(now);
      }
    }

    // loop over domain accounting
    for (dit = IostatAvgDomainIOrb.begin(); dit != IostatAvgDomainIOrb.end();
         dit++) {
      dit->second.StampZero(now);
    }

    for (dit = IostatAvgDomainIOwb.begin(); dit != IostatAvgDomainIOwb.end();
         dit++) {
      dit->second.StampZero(now);
    }

    // loop over app accounting
    for (dit = IostatAvgAppIOrb.begin(); dit != IostatAvgAppIOrb.end(); dit++) {
      dit->second.StampZero(now);
    }

    for (dit = IostatAvgAppIOwb.begin(); dit != IostatAvgAppIOwb.end(); dit++) {
      dit->second.StampZero(now);
    }

    Mutex.UnLock();
    size_t popularitybin = (((time(NULL))) % (IOSTAT_POPULARITY_DAY *
                            IOSTAT_POPULARITY_HISTORY_DAYS)) / IOSTAT_POPULARITY_DAY;

    if (IostatLastPopularityBin != popularitybin) {
      // only if we enter a new bin we erase it
      PopularityMutex.Lock();
      IostatPopularity[popularitybin].clear();
      IostatPopularity[popularitybin].resize(10000);
      IostatLastPopularityBin = popularitybin;
      PopularityMutex.UnLock();
    }
  }

  eos_static_info("%s", "msg=\"stopping iostat circulate thread\"");
}

/* ------------------------------------------------------------------------- */
bool
Iostat::StartPopularity()
{
  if (mReportPopularity) {
    return false;
  }

  mReportPopularity = true;
  StoreIostatConfig<FsView>(&FsView::gFsView);
  return true;
}

/* ------------------------------------------------------------------------- */
bool
Iostat::StopPopularity()
{
  if (!mReportPopularity) {
    return false;
  }

  mReportPopularity = false;
  StoreIostatConfig<FsView>(&FsView::gFsView);
  return true;
}

/* ------------------------------------------------------------------------- */
bool
Iostat::StartReport()
{
  if (mReport) {
    return false;
  }

  mReport = true;
  StoreIostatConfig<FsView>(&FsView::gFsView);
  return true;
}

/* ------------------------------------------------------------------------- */
bool
Iostat::StopReport()
{
  if (!mReport) {
    return false;
  }

  mReport = false;
  StoreIostatConfig<FsView>(&FsView::gFsView);
  return true;
}

/* ------------------------------------------------------------------------- */
bool
Iostat::StartCollection()
{
  bool retc = false;
  {
    XrdSysMutexHelper mLock(Mutex);
    retc = Start();
  }

  if (retc) {
    StoreIostatConfig<FsView>(&FsView::gFsView);
  }

  return retc;
}

/* ------------------------------------------------------------------------- */
bool
Iostat::StopCollection()
{
  bool retc = false;
  {
    XrdSysMutexHelper mLock(Mutex);
    retc = Stop();
  }

  if (retc) {
    StoreIostatConfig<FsView>(&FsView::gFsView);
  }

  return retc;
}

/* ------------------------------------------------------------------------- */
bool
Iostat::StartReportNamespace()
{
  if (mReportNamespace) {
    return false;
  }

  mReportNamespace = true;
  StoreIostatConfig<FsView>(&FsView::gFsView);
  return true;
}

/* ------------------------------------------------------------------------- */
bool
Iostat::StopReportNamespace()
{
  if (!mReportNamespace) {
    return false;
  }

  mReportNamespace = false;
  StoreIostatConfig<FsView>(&FsView::gFsView);
  return true;
}

/* ------------------------------------------------------------------------- */
bool
Iostat::AddUdpTarget(const char* target, bool storeitandlock)
{
  {
    if (storeitandlock) {
      mBcastMutex.Lock();
    }

    std::string starget = target;

    if (mUdpPopularityTarget.count(starget)) {
      if (storeitandlock) {
        mBcastMutex.UnLock();
      }

      return false;
    }

    mUdpPopularityTarget.insert(starget);
    // create an UDP socket for the specified target
    int udpsocket = -1;
    udpsocket = socket(AF_INET, SOCK_DGRAM, 0);

    if (udpsocket >= 0) {
      XrdOucString a_host, a_port, hp;
      int port = 0;
      hp = starget.c_str();

      if (!eos::common::StringConversion::SplitKeyValue(hp, a_host, a_port)) {
        a_host = hp;
        a_port = "31000";
      }

      port = atoi(a_port.c_str());
      mUdpSocket[starget] = udpsocket;
      XrdNetAddr* addrs  = 0;
      int         nAddrs = 0;
      const char* err    = XrdNetUtils::GetAddrs(a_host.c_str(), &addrs, nAddrs,
                           XrdNetUtils::allIPv64,
                           XrdNetUtils::NoPortRaw);

      if (err || nAddrs == 0) {
        if (storeitandlock) {
          mBcastMutex.UnLock();
        }

        return false;
      }

      memcpy((struct sockaddr*) &mUdpSockAddr[starget], addrs[0].SockAddr(),
             sizeof(sockaddr));
      delete [] addrs;
      mUdpSockAddr[starget].sin_family = AF_INET;
      mUdpSockAddr[starget].sin_port = htons(port);
    }
  }

  // store the configuration
  if (storeitandlock) {
    mBcastMutex.UnLock();
    return StoreIostatConfig<FsView>(&FsView::gFsView);
  }

  if (storeitandlock) {
    mBcastMutex.UnLock();
  }

  return true;
}

/* ------------------------------------------------------------------------- */
bool
Iostat::RemoveUdpTarget(const char* target)
{
  bool store = false;
  bool retc = false;
  {
    std::string starget = target;
    XrdSysMutexHelper mLock(mBcastMutex);
    
    if (mUdpPopularityTarget.count(starget)) {
      mUdpPopularityTarget.erase(starget);

      if (mUdpSocket.count(starget)) {
        if (mUdpSocket[starget] > 0) {
          // close the UDP socket
          close(mUdpSocket[starget]);
        }

        mUdpSocket.erase(starget);
        mUdpSockAddr.erase(starget);
      }

      retc = true;
      store = true;
    }
  }
  if (store) {
    retc &= StoreIostatConfig<FsView>(&FsView::gFsView);
  }  

  return retc;
}

/* ------------------------------------------------------------------------- */
void
Iostat::UdpBroadCast(eos::common::Report* report)
{
  std::set<std::string>::const_iterator it;
  std::string u = "";
  char fs[1024];

  for (it = mUdpPopularityTarget.begin(); it != mUdpPopularityTarget.end();
       it++) {
    u = "";
    XrdOucString tg = it->c_str();
    XrdOucString sizestring;

    if (tg.endswith("/json")) {
      // do json format broadcast
      tg.replace("/json", "");
      u += "{\"app_info\": \"";
      u += report->sec_app;
      u += "\",\n";
      u += " \"client_domain\": \"";
      u += report->sec_domain;
      u += "\",\n";
      u += " \"client_host\": \"";
      u += report->sec_host;
      u += "\",\n";
      u += " \"end_time\": ";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->cts);
      u += ",\n";
      u += " \"file_lfn\": \"";
      u += report->path;
      u += "\",\n";
      u += " \"file_size\": ";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->csize);
      u += ",\n";
      u += " \"read_average\": ";
      u += eos::common::StringConversion::GetSizeString(sizestring,
           report->rb / ((report->nrc) ? report->nrc : 999999999));
      u += ",\n";
      u += " \"read_bytes_at_close\": ";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->rb);
      u += ",\n";
      u += " \"read_bytes\": ";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->rb);
      u += ",\n";
      u += " \"read_max\": ";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->rb_max);
      u += ",\n";
      u += " \"read_min\": ";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->rb_min);
      u += ",\n";
      u += " \"read_operations\": ";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->nrc);
      u += ",\n";
      snprintf(fs, sizeof(fs) - 1, "%.02f", report->rb_sigma);
      u += " \"read_sigma\": ";
      u += fs;
      u += ",\n";
      /* -- we have currently no access to this information */
      /*
      u += " \"read_single_average\": ";  u += "0"; u += ",\n";
      u += " \"read_single_bytes\": ";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->rb); u += ",\n";
      u += " \"read_single_max\": ";      u += "0"; u += ",\n";
      u += " \"read_single_min\": ";      u += "0"; u += ",\n";
      u += " \"read_single_operations\": ";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->nrc); u += ",\n";
      u += " \"read_single_sigma\": ";    u += "0"; u += ",\n";
      u += " \"read_vector_average\": ";  u += "0"; u += ",\n";
      u += " \"read_vector_bytes\": ";    u += "0"; u += ",\n";
      u += " \"read_vector_count_average\": "; u += "0"; u += ",\n";
      u += " \"read_vector_count_max\": ";u += "0"; u += ",\n";
      u += " \"read_vector_count_min\": ";u += "0"; u += ",\n";
      u += " \"read_vector_count_sigma\": ";   u += "0"; u += ",\n";
      u += " \"read_vector_max\": ";      u += "0"; u += ",\n";
      u += " \"read_vector_min\": ";      u += "0"; u += ",\n";
      u += " \"read_vector_operations\": "; u += "0"; u += ",\n";
      u += " \"read_vector_sigma\": ";    u += "0"; u += ",\n"; */
      u += " \"server_domain\": \"";
      u += report->server_domain;
      u += "\",\n";
      u += " \"server_host\": \"";
      u += report->server_name;
      u += "\",\n";
      u += " \"server_username\": \"";
      u += report->sec_name;
      u += "\",\n";
      u += " \"start_time\": ";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->ots);
      u += ",\n";
      XrdOucString stime; // stores the current time in <s>.<ns>
      u += " \"unique_id\": \"";
      u += gOFS->MgmOfsInstanceName.c_str();
      u += "-";
      u += eos::common::StringConversion::TimeNowAsString(stime);
      u += "\",\n";
      u += " \"user_dn\": \"";
      u += report->sec_info;
      u += "\",\n";
      u += " \"user_fqan\": \"";
      u += report->sec_grps;
      u += "\",\n";
      u += " \"user_role\": \"";
      u += report->sec_role;
      u += "\",\n";
      u += " \"user_vo\": \"";
      u += report->sec_vorg;
      u += "\",\n";
      u += " \"write_average\": ";
      u += eos::common::StringConversion::GetSizeString(sizestring,
           report->wb / ((report->nwc) ? report->nwc : 999999999));
      u += ",\n";
      u += " \"write_bytes_at_close\": ";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->wb);
      u += ",\n";
      u += " \"write_bytes\": ";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->wb);
      u += ",\n";
      u += " \"write_max\": ";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->wb_max);
      u += ",\n";
      u += " \"write_min\": ";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->wb_min);
      u += ",\n";
      u += " \"write_operations\": ";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->nwc);
      u += ",\n";
      snprintf(fs, sizeof(fs) - 1, "%.02f", report->wb_sigma);
      u += " \"write_sigma\": ";
      u += fs;
      u += "}\n";
    } else {
      // do default format broadcast
      u += "#begin\n";
      u += "app_info=";
      u += report->sec_app;
      u += "\n";
      u += "client_domain=";
      u += report->sec_domain;
      u += "\n";
      u += "client_host=";
      u += report->sec_host;
      u += "\n";
      u += "end_time=";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->cts);
      u += "\n";
      u += "file_lfn = ";
      u += report->path;
      u += "\n";
      u += "file_size = ";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->csize);
      u += "\n";
      u += "read_average=";
      u += eos::common::StringConversion::GetSizeString(sizestring,
           report->rb / ((report->nrc) ? report->nrc : 999999999));
      u += "\n";
      u += "read_bytes_at_close=";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->rb);
      u += "\n";
      u += "read_bytes=";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->rb);
      u += "\n";
      u += "read_min=";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->rb_min);
      u += "\n";
      u += "read_max=";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->rb_max);
      u += "\n";
      u += "read_operations=";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->nrc);
      u += "\n";
      u += "read_sigma=";
      u += "0";
      u += "\n";
      snprintf(fs, sizeof(fs) - 1, "%.02f", report->rb_sigma);
      u += "read_sigma=";
      u += fs;
      u += "\n";
      /* -- we have currently no access to this information */
      /* u += "read_single_average=";  u += "0"; u += "\n";
      u += "read_single_bytes=";    u += eos::common::StringConversion::GetSizeString(sizestring, report->rb); u += "\n";
      u += "read_single_max=";      u += "0"; u += "\n";
      u += "read_single_min=";      u += "0"; u += "\n";
      u += "read_single_operations=";    u += eos::common::StringConversion::GetSizeString(sizestring, report->nrc); u += "\n";
      u += "read_single_sigma=";    u += "0"; u += "\n";
      u += "read_vector_average=";  u += "0"; u += "\n";
      u += "read_vector_bytes=";    u += "0"; u += "\n";
      u += "read_vector_count_average="; u += "0"; u += "\n";
      u += "read_vector_count_max=";u += "0"; u += "\n";
      u += "read_vector_count_min=";u += "0"; u += "\n";
      u += "read_vector_count_sigma=";   u += "0"; u += "\n";
      u += "read_vector_max=";      u += "0"; u += "\n";
      u += "read_vector_min=";      u += "0"; u += "\n";
      u += "read_vector_operations="; u += "0"; u += "\n";
      u += "read_vector_sigma=";    u += "0"; u += "\n";*/
      u += "server_domain=";
      u += report->server_domain;
      u += "\n";
      u += "server_host=";
      u += report->server_name;
      u += "\n";
      u += "server_username=";
      u += report->sec_name;
      u += "\n";
      u += "start_time=";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->ots);
      u += "\n";
      XrdOucString stime; // stores the current time in <s>.<ns>
      u += "unique_id=";
      u += gOFS->MgmOfsInstanceName.c_str();
      u += "-";
      u += eos::common::StringConversion::TimeNowAsString(stime);
      u += "\n";
      u += "user_dn = ";
      u += report->sec_info;
      u += "\n";
      u += "user_fqan=";
      u += report->sec_grps;
      u += "\n";
      u += "user_role=";
      u += report->sec_role;
      u += "\n";
      u += "user_vo=";
      u += report->sec_vorg;
      u += "\n";
      u += "write_average=";
      u += eos::common::StringConversion::GetSizeString(sizestring,
           report->wb / ((report->nwc) ? report->nwc : 999999999));
      u += "\n";
      u += "write_bytes_at_close=";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->wb);
      u += "\n";
      u += "write_bytes=";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->wb);
      u += "\n";
      u += "write_min=";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->wb_min);
      u += "\n";
      u += "write_max=";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->wb_max);
      u += "\n";
      u += "write_operations=";
      u += eos::common::StringConversion::GetSizeString(sizestring, report->nwc);
      u += "\n";
      snprintf(fs, sizeof(fs) - 1, "%.02f", report->rb_sigma);
      u += "write_sigma=";
      u += fs;
      u += "\n";
      u += "#end\n";
    }

    int sendretc = sendto(mUdpSocket[*it], u.c_str(), u.length(), 0,
                          (struct sockaddr*) &mUdpSockAddr[*it], sizeof(struct sockaddr_in));

    if (sendretc < 0) {
      eos_static_err("failed to send udp message to %s\n", it->c_str());
    }

    if (EOS_LOGS_DEBUG) {
      fprintf(stderr, "===>UDP\n%s<===UDP\n", u.c_str());
      eos_static_debug("retc(sendto)=%d", sendretc);
    }
  }
}

void
Iostat::AddToPopularity(std::string path, unsigned long long rb,
                        time_t starttime,
                        time_t stoptime)
{
  size_t popularitybin = (((starttime + stoptime) / 2) % (IOSTAT_POPULARITY_DAY *
                          IOSTAT_POPULARITY_HISTORY_DAYS)) / IOSTAT_POPULARITY_DAY;
  eos::common::Path cPath(path.c_str());
  XrdSysMutexHelper scope_lock(PopularityMutex);

  for (size_t k = 0; k < cPath.GetSubPathSize(); k++) {
    std::string sp = cPath.GetSubPath(k);
    IostatPopularity[popularitybin][sp].rb += rb;
    IostatPopularity[popularitybin][sp].nread++;
  }
}

/* ------------------------------------------------------------------------- */
void
IostatAvg::Add(unsigned long long val, time_t starttime, time_t stoptime)
{
  time_t now = time(0);
  size_t tdiff = stoptime - starttime;
  size_t toff = now - stoptime;
  
  if (toff < 86400) {
    // if the measurements was done in the last 86400 seconds
    unsigned int mbins = tdiff / 1440; // number of bins the measurement was hitting

    if (mbins == 0) {
      mbins = 1;
    }

    unsigned long long norm_val = (1.0 * val / mbins);

    for (size_t bins = 0; bins < mbins; bins++) {
      unsigned int bin86400 = (((stoptime - (bins * 1440)) / 1440) % 60);

      if (bins < remainder) {
        avg86400[bin86400] += (norm_val + 1);
      } else {
        avg86400[bin86400] += norm_val;
      }
    }
    
  }

  if (toff < 3600) {
    // if the measurements was done in the last 3600 seconds
    unsigned int mbins = tdiff / 60; // number of bins the measurement was hitting

    if (mbins == 0) {
      mbins = 1;
    }

    unsigned long long norm_val = 1.0 * val / mbins;
    
    for (size_t bins = 0; bins < mbins; bins++) {
      unsigned int bin3600 = (((stoptime - (bins * 60)) / 60) % 60);

      if (bins < remainder) {
        avg3600[bin3600] += (norm_val + 1);
      } else {
        avg3600[bin3600] += norm_val;
      }
    }
  }

  if (toff < 300) {
    // if the measurements was done in the last 300 seconds
    unsigned int mbins = tdiff / 5; // number of bins the measurement was hitting

    if (mbins == 0) {
      mbins = 1;
    }

    unsigned long long norm_val = 1.0 * val / mbins;
    
    for (size_t bins = 0; bins < mbins; bins++) {
      unsigned int bin300 = (((stoptime - (bins * 5)) / 5) % 60);

      if (bins < remainder) {
        avg300[bin300] += (norm_val + 1);
      } else {
        avg300[bin300] += norm_val;
      }
    }
  }

  if (toff < 60) {
    // if the measurements was done in the last 60 seconds
    unsigned int mbins = tdiff / 1; // number of bins the measurement was hitting

    if (mbins == 0) {
      mbins = 1;
    }

    unsigned long long norm_val = 1.0 * val / mbins;
    
    for (size_t bins = 0; bins < mbins; ++bins) {
      unsigned int bin60 = (((stoptime - (bins * 1)) / 1) % 60);

      if (bins < remainder) {
        avg60[bin60] += (norm_val + 1);
      } else {
        avg60[bin60] += norm_val;
      }
    }
  }
}

//------------------------------------------------------------------------------
// Encode the UDP popularity targets to a string using the provided separator
//------------------------------------------------------------------------------
std::string
Iostat::EncodeUdpPopularityTargets() const
{
  std::string out;
  XrdSysMutexHelper scope_lock(mBcastMutex);

  if (mUdpPopularityTarget.empty()) {
    return out;
  }

  for (const auto& elem : mUdpPopularityTarget) {
    out += elem;
    out += "|";
  }

  out.pop_back();
  return out;
}

//------------------------------------------------------------------------------
// Reset all the bins
//------------------------------------------------------------------------------
void
IostatAvg::StampZero(time_t& now)
{
  unsigned int bin86400 = (now / 1440);
  unsigned int bin3600 = (now / 60);
  unsigned int bin300 = (now / 5);
  unsigned int bin60 = (now / 1);
  avg86400[(bin86400 + 1) % 60] = 0;
  avg3600[(bin3600 + 1) % 60] = 0;
  avg300[(bin300 + 1) % 60] = 0;
  avg60[(bin60 + 1) % 60] = 0;
}

double
IostatAvg::GetAvg86400()
{
  double sum = 0;

  for (int i = 0; i < 60; i++) {
    sum += avg86400[i];
  }

  return sum;
}

double
IostatAvg::GetAvg3600()
{
  double sum = 0;

  for (int i = 0; i < 60; i++) {
    sum += avg3600[i];
  }

  return sum;
}

double
IostatAvg::GetAvg300()
{
  double sum = 0;

  for (int i = 0; i < 60; i++) {
    sum += avg300[i];
  }

  return sum;
}

double
IostatAvg::GetAvg60()
{
  double sum = 0;

  for (int i = 0; i < 60; i++) {
    sum += avg60[i];
  }

  return sum;
}

EOSMGMNAMESPACE_END
