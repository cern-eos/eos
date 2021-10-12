//------------------------------------------------------------------------------
// File: Iostat.cc
// Authors: Andreas-Joachim Peters/Jaroslav Guenther      - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2021 CERN/Switzerland                                  *
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
#include "common/Timing.hh"
#include "common/StringTokenizer.hh"
#include "mgm/Iostat.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/IMaster.hh"
#include "namespace/interface/IView.hh"
#include "namespace/ns_quarkdb/QdbContactDetails.hh"
#include "namespace/ns_quarkdb/qclient/include/qclient/ResponseParsing.hh"
#include "namespace/Prefetcher.hh"
#include "mq/ReportListener.hh"
#include "XrdNet/XrdNetUtils.hh"
#include "XrdNet/XrdNetAddr.hh"
#include <curl/curl.h>

EOSMGMNAMESPACE_BEGIN

const char* Iostat::gIostatCollect = "iostat::collect";
const char* Iostat::gIostatReport = "iostat::report";
const char* Iostat::gIostatReportNamespace = "iostat::reportnamespace";
const char* Iostat::gIostatPopularity = "iostat::popularity";
const char* Iostat::gIostatUdpTargetList = "iostat::udptargets";
FILE* Iostat::gOpenReportFD = 0;
Period LAST_DAY = Period::DAY;
Period LAST_HOUR = Period::HOUR;
Period LAST_5MIN = Period::FIVEMIN;
Period LAST_1MIN = Period::ONEMIN;

namespace
{
//------------------------------------------------------------------------------
// Write callback used by CURL
//------------------------------------------------------------------------------
static size_t
CurlWriteCallback(void* contents, size_t size, size_t nmemb, void* userp)
{
  ((std::string*)userp)->append((char*) contents, size * nmemb);
  return size * nmemb;
}


//------------------------------------------------------------------------------
// Get list of top level domains
//------------------------------------------------------------------------------
void GetTopLevelDomains(std::set<std::string>& domains)
{
  const std::string url = "http://data.iana.org/TLD/tlds-alpha-by-domain.txt";
  CURL* curl = curl_easy_init();
  std::string data;

  if (curl) {
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, CurlWriteCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &data);
    CURLcode res = curl_easy_perform(curl);
    curl_easy_cleanup(curl);

    if ((res != CURLE_OK) || data.empty()) {
      domains.clear();
    } else {
      // Parse line by line each top level domain
      eos::common::StringTokenizer tokenizer(data);
      const char* line;
      std::string tld;

      while ((line = tokenizer.GetLine())) {
        // Skip commented lines
        if (*line == '#') {
          continue;
        }

        tld = ".";
        tld += line;
        domains.insert(tld);
      }
    }
  }

  // If there were any issues then we add some default values
  if (domains.empty()) {
    domains = {".ch", ".it", ".ru", ".de", ".nl", ".fr", ".se", ".ro",
               ".su", ".no", ".dk", ".cz", ".uk", ".se", ".org", ".edu"
              };
  }
}
}

//------------------------------------------------------------------------------
// IostatPeriods implementation
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Add measurement to the various periods it overlaps with
//------------------------------------------------------------------------------
void
IostatPeriods::Add(unsigned long long val, time_t start, time_t stop,
                   time_t now)
{
  size_t tdiff = stop - start;
  size_t toff = now - stop;

  for (size_t pidx = 0; pidx < std::size(mPeriodBinWidth); ++pidx) {
    // Chech if stop time falls into the last day/hour/5min/1min interval
    if (toff < mPeriodBinWidth[pidx] * sBinsPerPeriod) {
      AddToPeriod(pidx, val, tdiff, stop);
    }
  }
}

//------------------------------------------------------------------------------
// Measurement stop time and duration determine which of the bins of the
// corresponding period (period_indx) will get populated with the new stat values
//------------------------------------------------------------------------------
void
IostatPeriods::AddToPeriod(size_t period_indx, unsigned long long val,
                           size_t tdiff, time_t stop)
{
  // Get bin width for the given period
  size_t bin_width = mPeriodBinWidth[period_indx];
  // Number of bins the measurement hits
  size_t mbins = tdiff / bin_width;

  if (mbins == 0) {
    mbins = 1;
  }

  // We partially mitigate the precision loss in integer division
  // when getting norm_val below by redistribution of reminder into bins
  unsigned long long remainder = val % mbins;
  unsigned long long norm_val = val / mbins;

  for (size_t bins = 0; bins < mbins; bins++) {
    size_t bin_indx = (((stop - (bins * bin_width)) / bin_width) % sBinsPerPeriod);

    if (bins < remainder) {
      mPeriodBins[period_indx][bin_indx] += (norm_val + 1);
    } else {
      mPeriodBins[period_indx][bin_indx] += norm_val;
    }
  }
}

//------------------------------------------------------------------------------
// Reset the bin affected by the given timestamp
//------------------------------------------------------------------------------
void
IostatPeriods::StampZero(time_t& timestamp)
{
  for (size_t pidx = 0; pidx < std::size(mPeriodBinWidth); ++pidx) {
    size_t binT = (timestamp / mPeriodBinWidth[pidx]);
    mPeriodBins[pidx][(binT + 1) % sBinsPerPeriod] = 0;
  }
}

//------------------------------------------------------------------------------
// Get the sum of values for the given period
//------------------------------------------------------------------------------
unsigned long long
IostatPeriods::GetSumForPeriod(Period period) const
{
  unsigned long long sum = 0ull;

  for (size_t i = 0; i < sBinsPerPeriod; ++i) {
    sum += mPeriodBins[(int)period][i];
  }

  return sum;
}

//------------------------------------------------------------------------------
// Iostat implementation
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Iostat constructor
//------------------------------------------------------------------------------
Iostat::Iostat():
  mQcl(nullptr), mReport(true), mReportNamespace(false), mReportPopularity(true)
{
  mRunning = false;
  mReportHashKeyBase = "";
  StoreKeyStruct["id"] = "";
  StoreKeyStruct["idt"] = "";
  StoreKeyStruct["tag"] = "";
  GetTopLevelDomains(IoDomains);
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
// Destructor
//------------------------------------------------------------------------------
Iostat::~Iostat()
{
  (void) StopCollection();
  mCirculateThread.join();
}

//------------------------------------------------------------------------------
// Perform object initialization
//------------------------------------------------------------------------------
bool
Iostat::Init(const std::string& instance_name)
{
  mReportHashKeyBase = SSTR("eos-iostat-report:" << instance_name << ":");

  // Initialize qclient
  if (gOFS != NULL && !mQcl) {
    const eos::QdbContactDetails& qdb_details = gOFS->mQdbContactDetails;
    mQcl.reset(new qclient::QClient(qdb_details.members,
                                    qdb_details.constructOptions()));
  }

  if (!mQcl) {
    eos_static_err("%s", "msg=\"failed to configure qclient\"");
    return false;
  }

  if (Restore()) {
    mCirculateThread.reset(&Iostat::Circulate, this);
    return true;
  } else {
    return false;
  }
}

//------------------------------------------------------------------------------
// Apply instance level configuration concerning IoStats
//------------------------------------------------------------------------------
void
Iostat::ApplyIostatConfig(FsView* fsview)
{
  std::string iocollect = fsview->GetGlobalConfig(gIostatCollect);

  if ((iocollect == "true") || (iocollect.empty())) {
    StartCollection(); // enable by default
  }

  std::string iopopularity = fsview->GetGlobalConfig(gIostatPopularity);
  mReportPopularity = (iopopularity == "true") || (iopopularity.empty());
  mReport = fsview->GetBoolGlobalConfig(gIostatReport);
  mReportNamespace = fsview->GetBoolGlobalConfig(gIostatReportNamespace);
  std::string udplist = fsview->GetGlobalConfig(gIostatUdpTargetList);
  std::string delimiter = "|";
  std::vector<std::string> hostlist;
  eos::common::StringConversion::Tokenize(udplist, hostlist, delimiter);
  std::unique_lock<std::mutex> scope_lock(mBcastMutex);
  mUdpPopularityTarget.clear();

  for (size_t i = 0; i < hostlist.size(); ++i) {
    AddUdpTarget(hostlist[i], false);
  }
}

//------------------------------------------------------------------------------
// Store IoStat config in the instance level configuration
//------------------------------------------------------------------------------
bool
Iostat::StoreIostatConfig(FsView* fsview) const
{
  bool ok = fsview->SetGlobalConfig(gIostatPopularity, mReportPopularity) &
            fsview->SetGlobalConfig(gIostatReport, mReport) &
            fsview->SetGlobalConfig(gIostatReportNamespace, mReportNamespace) &
            fsview->SetGlobalConfig(gIostatCollect, mRunning);
  std::string udp_popularity_targets = EncodeUdpPopularityTargets();

  if (!udp_popularity_targets.empty()) {
    ok &= fsview->SetGlobalConfig(gIostatUdpTargetList, udp_popularity_targets);
  }

  return ok;
}

//------------------------------------------------------------------------------
// Start collection thread
//------------------------------------------------------------------------------
bool
Iostat::StartCollection()
{
  std::unique_lock<std::mutex> scope_lock(mThreadSyncMutex);

  if (!mRunning) {
    mRunning = true;
    mReceivingThread.reset(&Iostat::Receive, this);
    StoreIostatConfig(&FsView::gFsView);
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Stop collection thread
//------------------------------------------------------------------------------
bool
Iostat::StopCollection()
{
  std::unique_lock<std::mutex> scope_lock(mThreadSyncMutex);

  if (mRunning) {
    mReceivingThread.join();
    mRunning = false;
    StoreIostatConfig(&FsView::gFsView);
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Start popularity thread
//------------------------------------------------------------------------------
bool
Iostat::StartPopularity()
{
  std::unique_lock<std::mutex> scope_lock(mThreadSyncMutex);

  if (!mReportPopularity) {
    mReportPopularity = true;
    StoreIostatConfig(&FsView::gFsView);
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Start popularity thread
//------------------------------------------------------------------------------
bool
Iostat::StopPopularity()
{
  std::unique_lock<std::mutex> scope_lock(mThreadSyncMutex);

  if (mReportPopularity) {
    mReportPopularity = false;
    StoreIostatConfig(&FsView::gFsView);
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Start daily report thread
//------------------------------------------------------------------------------
bool
Iostat::StartReport()
{
  std::unique_lock<std::mutex> scope_lock(mThreadSyncMutex);

  if (!mReport) {
    mReport = true;
    StoreIostatConfig(&FsView::gFsView);
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Stop daily report thread
//------------------------------------------------------------------------------
bool
Iostat::StopReport()
{
  std::unique_lock<std::mutex> scope_lock(mThreadSyncMutex);

  if (mReport) {
    mReport = false;
    StoreIostatConfig(&FsView::gFsView);
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Start namespace report thread
//------------------------------------------------------------------------------
bool
Iostat::StartReportNamespace()
{
  std::unique_lock<std::mutex> scope_lock(mThreadSyncMutex);

  if (!mReportNamespace) {
    mReportNamespace = true;
    StoreIostatConfig(&FsView::gFsView);
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Stop namespace report thread
//------------------------------------------------------------------------------
bool
Iostat::StopReportNamespace()
{
  std::unique_lock<std::mutex> scope_lock(mThreadSyncMutex);

  if (mReportNamespace) {
    mReportNamespace = false;
    StoreIostatConfig(&FsView::gFsView);
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Record measurement to the various periods it overlaps with
//------------------------------------------------------------------------------
void
Iostat::Add(const char* tag, uid_t uid, gid_t gid, unsigned long long val,
            time_t start, time_t stop, time_t now)
{
  std::unique_lock<std::mutex> scope_lock(Mutex);
  IostatUid[tag][uid] += val;
  IostatGid[tag][gid] += val;
  IostatPeriodsUid[tag][uid].Add(val, start, stop, now);
  IostatPeriodsGid[tag][gid].Add(val, start, stop, now);
}

//------------------------------------------------------------------------------
// Get sum of measurements for the given tag
//------------------------------------------------------------------------------
unsigned long long
Iostat::GetTotalStatForTag(const char* tag) const
{
  unsigned long long val = 0ull;

  if (!IostatUid.count(tag)) {
    return val;
  }

  const auto map_uid = IostatUid.find(tag)->second;

  for (auto it = map_uid.begin(); it != map_uid.end(); ++it) {
    val += it->second;
  }

  return val;
}

//------------------------------------------------------------------------------
// Get sum of measurements for the given tag an period
//------------------------------------------------------------------------------
unsigned long long
Iostat::GetPeriodStatForTag(const char* tag, Period period) const
{
  unsigned long long val = 0ull;

  if (!IostatPeriodsUid.count(tag)) {
    return val;
  }

  const auto map_uid = IostatPeriodsUid.find(tag)->second;

  for (auto it = map_uid.begin(); it != map_uid.end(); ++it) {
    val += it->second.GetSumForPeriod(period);
  }

  return val;
}

//------------------------------------------------------------------------------
// Method executed by the thread receiving reports
//------------------------------------------------------------------------------
void
Iostat::Receive(ThreadAssistant& assistant) noexcept
{
  if (gOFS == nullptr) {
    return;
  }

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
      time_t now = time(0);
      std::unique_ptr<eos::common::Report> report(new eos::common::Report(ioreport));
      Add("bytes_read", report->uid, report->gid, report->rb, report->ots,
          report->cts, now);
      Add("bytes_read", report->uid, report->gid, report->rvb_sum, report->ots,
          report->cts, now);
      Add("bytes_written", report->uid, report->gid, report->wb, report->ots,
          report->cts, now);
      Add("read_calls", report->uid, report->gid, report->nrc, report->ots,
          report->cts, now);
      Add("readv_calls", report->uid, report->gid, report->rv_op, report->ots,
          report->cts, now);
      Add("write_calls", report->uid, report->gid, report->nwc, report->ots,
          report->cts, now);
      Add("fwd_seeks", report->uid, report->gid, report->nfwds, report->ots,
          report->cts, now);
      Add("bwd_seeks", report->uid, report->gid, report->nbwds, report->ots,
          report->cts, now);
      Add("xl_fwd_seeks", report->uid, report->gid, report->nxlfwds, report->ots,
          report->cts, now);
      Add("xl_bwd_seeks", report->uid, report->gid, report->nxlbwds, report->ots,
          report->cts, now);
      Add("bytes_fwd_seek", report->uid, report->gid, report->sfwdb, report->ots,
          report->cts, now);
      Add("bytes_bwd_wseek", report->uid, report->gid, report->sbwdb, report->ots,
          report->cts, now);
      Add("bytes_xl_fwd_seek", report->uid, report->gid, report->sxlfwdb, report->ots,
          report->cts, now);
      Add("bytes_xl_bwd_wseek", report->uid, report->gid, report->sxlbwdb,
          report->ots, report->cts, now);
      Add("disk_time_read", report->uid, report->gid, report->rt,
          report->ots, report->cts, now);
      Add("disk_time_write", report->uid, report->gid,
          report->wt, report->ots, report->cts, now);
      Add("bytes_deleted", 0, 0, report->dsize, now - 30, now, now);
      Add("files_deleted", 0, 0, 1, now - 30, now, now);
      // Do the UDP broadcasting
      UdpBroadCast(report.get());

      // Do the domain accounting
      if (report->path.substr(0, 11) == "/replicate:") {
        // check if this is a replication path
        // push into the 'eos' domain
        std::unique_lock<std::mutex> scope_lock(Mutex);

        if (report->rb) {
          IostatPeriodsDomainIOrb["eos"].Add(report->rb, report->ots, report->cts, now);
        }

        if (report->wb) {
          IostatPeriodsDomainIOwb["eos"].Add(report->wb, report->ots, report->cts, now);
        }
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
            std::unique_lock<std::mutex> scope_lock(Mutex);

            if (report->rb) {
              IostatPeriodsDomainIOrb[sdomain].Add(report->rb, report->ots, report->cts, now);
            }

            if (report->wb) {
              IostatPeriodsDomainIOwb[sdomain].Add(report->wb, report->ots, report->cts, now);
            }

            dfound = true;
          }
        }

        // do the node accounting here - keep the node list small !!!
        std::set<std::string>::const_iterator nit;

        for (nit = IoNodes.begin(); nit != IoNodes.end(); nit++) {
          if (*nit == report->sec_host.substr(0, nit->length())) {
            std::unique_lock<std::mutex> scope_lock(Mutex);

            if (report->rb) {
              IostatPeriodsDomainIOrb[*nit].Add(report->rb, report->ots, report->cts, now);
            }

            if (report->wb) {
              IostatPeriodsDomainIOwb[*nit].Add(report->wb, report->ots, report->cts, now);
            }

            dfound = true;
          }
        }

        if (!dfound) {
          // push into the 'other' domain
          std::unique_lock<std::mutex> scope_lock(Mutex);

          if (report->rb) {
            IostatPeriodsDomainIOrb["other"].Add(report->rb, report->ots, report->cts, now);
          }

          if (report->wb) {
            IostatPeriodsDomainIOwb["other"].Add(report->wb, report->ots, report->cts, now);
          }
        }
      }

      // do the application accounting here
      std::string apptag = "other";

      if (report->sec_app.length()) {
        apptag = report->sec_app;
      }

      // Push into app accounting
      {
        std::unique_lock<std::mutex> scope_lock(Mutex);

        if (report->rb) {
          IostatPeriodsAppIOrb[apptag].Add(report->rb, report->ots, report->cts, now);
        }

        if (report->wb) {
          IostatPeriodsAppIOwb[apptag].Add(report->wb, report->ots, report->cts, now);
        }
      }

      if (mReport && gOFS->mMaster->IsMaster()) {
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
            std::unique_lock<std::mutex> scope_lock(Mutex);

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

  eos_static_info("%s", "msg=\"stopping iostat receiver thread\"");
}


//------------------------------------------------------------------------------
// Write record to the stream - used by the MGM to push entries
//------------------------------------------------------------------------------
void
Iostat::WriteRecord(const std::string& record)
{
  std::unique_lock<std::mutex> scope_lock(Mutex);

  if (gOpenReportFD) {
    fprintf(gOpenReportFD, "%s\n", record.c_str());
    fflush(gOpenReportFD);
  }
}

//------------------------------------------------------------------------------
// Print IO statistics
//------------------------------------------------------------------------------
void
Iostat::PrintOut(XrdOucString& out, bool summary, bool details,
                 bool monitoring, bool numerical, bool top,
                 bool domain, bool apps, XrdOucString option)
{
  std::string format_s = (!monitoring ? "s" : "os");
  std::string format_ss = (!monitoring ? "-s" : "os");
  std::string format_l = (!monitoring ? "+l" : "ol");
  std::string format_ll = (!monitoring ? "l." : "ol");
  std::vector<std::string> tags;
  std::unique_lock<std::mutex> scope_lock(Mutex);

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
      row.emplace_back(GetTotalStatForTag(tag), format_ll);
      // getting tag stat sums for 1day (idx=0), 1h (idx=1), 5m (idx=2), 1min (idx=3)
      row.emplace_back(GetPeriodStatForTag(tag, LAST_1MIN), format_ll);
      row.emplace_back(GetPeriodStatForTag(tag, LAST_5MIN), format_ll);
      row.emplace_back(GetPeriodStatForTag(tag, LAST_HOUR), format_ll);
      row.emplace_back(GetPeriodStatForTag(tag, LAST_DAY), format_ll);
    }

    table.AddRows(table_data);
    out += table.GenerateTable(HEADER).c_str();
    table_data.clear();
    //! UDP Popularity Broadcast Target
    {
      std::unique_lock<std::mutex> mLock(mBcastMutex);

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
        unsigned long long, unsigned long long, unsigned long long, unsigned long long>>
        uidout, gidout;
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

    for (auto tuit = IostatPeriodsUid.begin(); tuit != IostatPeriodsUid.end();
         tuit++) {
      for (auto it = tuit->second.begin(); it != tuit->second.end(); ++it) {
        std::string username;

        if (numerical) {
          username = std::to_string(it->first);
        } else {
          int terrc = 0;
          username = eos::common::Mapping::UidToUserName(it->first, terrc);
        }

        // getting tag stat sums for 1day (idx=0), 1h (idx=1), 5m (idx=2), 1min (idx=3)
        uidout.emplace_back(std::make_tuple(username, tuit->first.c_str(),
                                            IostatUid[tuit->first][it->first],
                                            it->second.GetSumForPeriod(LAST_1MIN), it->second.GetSumForPeriod(LAST_5MIN),
                                            it->second.GetSumForPeriod(LAST_HOUR), it->second.GetSumForPeriod(LAST_DAY)));
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

    for (auto tgit = IostatPeriodsGid.begin(); tgit != IostatPeriodsGid.end();
         tgit++) {
      for (auto it = tgit->second.begin(); it != tgit->second.end(); ++it) {
        std::string groupname;

        if (numerical) {
          groupname = std::to_string(it->first);
        } else {
          int terrc = 0;
          groupname = eos::common::Mapping::GidToGroupName(it->first, terrc);
        }

        // getting stat sums for 1day (idx=0), 1h (idx=1), 5m (idx=2), 1min (idx=3)
        gidout.emplace_back(std::make_tuple(groupname, tgit->first.c_str(),
                                            IostatGid[tgit->first][it->first],
                                            it->second.GetSumForPeriod(LAST_1MIN), it->second.GetSumForPeriod(LAST_5MIN),
                                            it->second.GetSumForPeriod(LAST_HOUR), it->second.GetSumForPeriod(LAST_DAY)));
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
    for (auto it = IostatPeriodsDomainIOrb.begin();
         it != IostatPeriodsDomainIOrb.end();
         ++it) {
      table_data.emplace_back();
      TableRow& row = table_data.back();
      std::string name = !monitoring ? "out" : "domain_io_out";
      row.emplace_back(name, format_ss);
      row.emplace_back(it->first.c_str(), format_s);
      // getting stat sums for 1day (idx=0), 1h (idx=1), 5m (idx=2), 1min (idx=3)
      row.emplace_back(it->second.GetSumForPeriod(LAST_1MIN), format_l);
      row.emplace_back(it->second.GetSumForPeriod(LAST_5MIN), format_l);
      row.emplace_back(it->second.GetSumForPeriod(LAST_HOUR), format_l);
      row.emplace_back(it->second.GetSumForPeriod(LAST_DAY), format_l);
    }

    // IO in bytes
    for (auto it = IostatPeriodsDomainIOwb.begin();
         it != IostatPeriodsDomainIOwb.end();
         ++it) {
      table_data.emplace_back();
      TableRow& row = table_data.back();
      std::string name = !monitoring ? "in" : "domain_io_in";
      row.emplace_back(name, format_ss);
      row.emplace_back(it->first.c_str(), format_s);
      // getting stat sums for 1day (idx=0), 1h (idx=1), 5m (idx=2), 1min (idx=3)
      row.emplace_back(it->second.GetSumForPeriod(LAST_1MIN), format_l);
      row.emplace_back(it->second.GetSumForPeriod(LAST_5MIN), format_l);
      row.emplace_back(it->second.GetSumForPeriod(LAST_HOUR), format_l);
      row.emplace_back(it->second.GetSumForPeriod(LAST_DAY), format_l);
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
    for (auto it = IostatPeriodsAppIOrb.begin(); it != IostatPeriodsAppIOrb.end();
         ++it) {
      table_data.emplace_back();
      TableRow& row = table_data.back();
      std::string name = (!monitoring ? "out" : "app_io_out");
      row.emplace_back(name, format_ss);
      row.emplace_back(it->first.c_str(), format_s);
      // getting stat sums for 1day (idx=0), 1h (idx=1), 5m (idx=2), 1min (idx=3)
      row.emplace_back(it->second.GetSumForPeriod(LAST_1MIN), format_l);
      row.emplace_back(it->second.GetSumForPeriod(LAST_5MIN), format_l);
      row.emplace_back(it->second.GetSumForPeriod(LAST_HOUR), format_l);
      row.emplace_back(it->second.GetSumForPeriod(LAST_DAY), format_l);
    }

    // IO in bytes
    for (auto it = IostatPeriodsAppIOwb.begin(); it != IostatPeriodsAppIOwb.end();
         ++it) {
      table_data.emplace_back();
      TableRow& row = table_data.back();
      std::string name = (!monitoring ? "in" : "app_io_in");
      row.emplace_back(name, format_ss);
      row.emplace_back(it->first.c_str(), format_s);
      // getting stat sums for 1day (idx=0), 1h (idx=1), 5m (idx=2), 1min (idx=3)
      row.emplace_back(it->second.GetSumForPeriod(LAST_1MIN), format_l);
      row.emplace_back(it->second.GetSumForPeriod(LAST_5MIN), format_l);
      row.emplace_back(it->second.GetSumForPeriod(LAST_HOUR), format_l);
      row.emplace_back(it->second.GetSumForPeriod(LAST_DAY), format_l);
    }

    table.AddRows(table_data);
    out += table.GenerateTable(HEADER).c_str();
  }
}

//------------------------------------------------------------------------------
// Compute and print out the namespace popularity ranking
//------------------------------------------------------------------------------
void
Iostat::PrintNsPopularity(XrdOucString& out, XrdOucString option) const
{
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

  // The 'hotfiles' are the files with highest number of present file opens
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
    std::unique_lock<std::mutex> scope_lock(mPopularityMutex);
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
  }
}

//------------------------------------------------------------------------------
// Print namespace activity report for given path
//------------------------------------------------------------------------------
void
Iostat::PrintNsReport(const char* path, XrdOucString& out) const
{
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
    auto report = std::make_unique<eos::common::Report>(ioreport);
    report->Dump(out);

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
  }

  out += "----------------------- SUMMARY -------------------\n";
  char summaryline[4096];
  XrdOucString sizestring1, sizestring2;
  snprintf(summaryline, sizeof(summaryline) - 1,
           "| avg. readd: %.02f MB/s | avg. write: %.02f  MB/s | "
           "total read: %s | total write: %s | times read: %llu | "
           "times written: %llu |\n",
           totalreadtime ? (totalreadbytes / totalreadtime / 1000000.0) : 0,
           totalwritetime ? (totalwritebytes / totalwritetime / 1000000.0) : 0,
           eos::common::StringConversion::GetReadableSizeString
           (sizestring1, totalreadbytes, "B"),
           eos::common::StringConversion::GetReadableSizeString
           (sizestring2, totalwritebytes, "B"), (unsigned long long)rcount,
           (unsigned long long)wcount);
  out += summaryline;
}


// creates key under which we will store an Iostat value in QuarkDB hash map report
std::string Iostat::BuildStatKey()
{
  std::string iostat_id = std::string("idt=") + StoreKeyStruct["idt"];
  iostat_id += std::string("&id=") + StoreKeyStruct["id"];
  iostat_id += std::string("&tag=") + StoreKeyStruct["tag"];
  return iostat_id;
}

//------------------------------------------------------------------------------
// Save current uid/gid counters to Quark DB
//------------------------------------------------------------------------------
bool
Iostat::Store()
{
  std::string year = eos::common::Timing::GetCurrentYear();
  // check if current year map exists in the QuarkDB, if not (new year) reset all counters to 0
  std::string mReportHashKeyBaseWithYear = mReportHashKeyBase + year;

  if (!mQcl->exists(mReportHashKeyBaseWithYear)) {
    eos_static_info("msg=\"HashMap=\"%s\" does not exists, cleaning up\"",
                    mReportHashKeyBaseWithYear.c_str());
    Iostat::ResetIostatMaps();
    // In case MGM starts for first time, no stat key initialised yet
    // making sure here that there is at least one kv entry in IostatUid and IostatGid,
    // so that the new hash map gets inserted to QDB
    time_t now = time(0);
    Add("files_deleted", 0, 0, 0, now - 30, now, now);
  };

  std::unique_lock<std::mutex> scope_lock(Mutex);

  // store user counters to the vectors
  return InsertToQDBStore(IostatUid, "u") && InsertToQDBStore(IostatGid, "g");
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
template <typename T>
bool Iostat::InsertToQDBStore(const T& gmap, const char* id_type)
{
  std::list<std::string> iostat_id_val_list;
  std::list<std::string> iostat_tot_val_list;

  // collecting the info and constructing key value lists
  for (auto tit : gmap) {
    unsigned long long tagtotalval = 0;

    for (auto it : tit.second) {
      if (std::strcmp(id_type, "u") == 0) {
        StoreKeyStruct["idt"] = "u";
      } else {
        StoreKeyStruct["idt"] = "g";
      }

      StoreKeyStruct["id"] = std::to_string(it.first);
      StoreKeyStruct["tag"] = tit.first;
      std::string itemkey = BuildStatKey();
      iostat_id_val_list.push_back(itemkey);
      iostat_id_val_list.push_back(std::to_string((unsigned long long)it.second));
      tagtotalval += (unsigned long long)it.second;
    }

    StoreKeyStruct["id"] = "ALL";
    std::string totitemkey = BuildStatKey();
    iostat_tot_val_list.push_back(totitemkey);
    iostat_tot_val_list.push_back(std::to_string(tagtotalval));
  }

  // QDB insert
  bool allok = true;
  std::string year = eos::common::Timing::GetCurrentYear();
  eos_static_info("msg=\"storing QDB hash map %s%s\"",
                  mReportHashKeyBase.c_str(), year.c_str());
  mQHashIostat = qclient::QHash(*mQcl, mReportHashKeyBase + year);

  try {
    allok += mQHashIostat.hmset(iostat_id_val_list);
  } catch (const std::exception& e) {
    eos_static_err("msg=\"error trying to add iostat entry (sum per tag)\" "
                   "emsg=\"%s\"", e.what());
    return false;
  }

  eos_static_info("msg=\"storing QDB totals hash map %s%s%s\"",
                  mReportHashKeyBase.c_str(), "sum-per-tag:", year.c_str());
  mQHashIostat = qclient::QHash(*mQcl,
                                mReportHashKeyBase + "sum-per-tag:" + year);

  try {
    allok += mQHashIostat.hmset(iostat_tot_val_list);
  } catch (const std::exception& e) {
    eos_static_err("msg=\"error trying to add iostat entry (sum per tag)\""
                   " emsg=\"%s\"", e.what());
    return false;
  }

  return allok;
}

// parsing the key-values from key "idt=u&id=xxx&tag=blabla"
// of the QuarkDB report hash map entry to be able to repopulate IostatUid/IostatGid objects
bool Iostat::UnfoldStatKey(const char* storedqdbkey_kv)
{
  std::vector<std::string> entry{};
  eos::common::StringConversion::Tokenize(storedqdbkey_kv, entry, "&");

  for (auto itkv = entry.begin(); itkv != entry.end(); itkv++) {
    std::vector<std::string> entry_kv{};
    eos::common::StringConversion::Tokenize(*itkv, entry_kv, "=");

    if (entry_kv.size() != 2) {
      // wrong/unexpected QDB entry format
      eos_static_err("[Iostat] bad entry, clean the QDB %s", entry_kv[0].c_str());
      return false;
    } else {
      if ("idt" == entry_kv[0]) {
        StoreKeyStruct["idt"] = entry_kv[1];
      }

      if ("id" == entry_kv[0]) {
        StoreKeyStruct["id"] = entry_kv[1];
      }

      if ("tag" == entry_kv[0]) {
        StoreKeyStruct["tag"] = entry_kv[1];
      }
    }

    entry_kv.clear();
  }

  entry.clear();
  return true;
}

// ---------------------------------------------------------------------------
// ! load current uid/gid counters from a QuarkDB hash map
// ---------------------------------------------------------------------------
bool
Iostat::Restore()
{
  std::string year = eos::common::Timing::GetCurrentYear();
  std::string mReportHashKeyBaseWithYear = mReportHashKeyBase + year;
  eos_static_info("[Iostat] Restoring from hash map %s",
                  mReportHashKeyBaseWithYear.c_str());
  qclient::redisReplyPtr reply;

  try {
    reply = mQcl->exec("HGETALL", mReportHashKeyBaseWithYear).get();
  } catch (const std::exception& e) {
    eos_static_err("msg=\"[Iostat] Error encountered while trying to get iostat entries from QuarkDB, emsg=\"%s\"",
                   e.what());
    return false;
  }

  qclient::HgetallParser mQdbRespParser(reply);

  if (!mQdbRespParser.ok() || mQdbRespParser.value().empty()) {
    return false;
  }

  std::map<std::string, std::string> stored_iostat = mQdbRespParser.value();
  std::vector<std::string> entry{};
  std::unique_lock<std::mutex> scope_lock(Mutex);
  unsigned long long val = 0;
  int id = 0;

  for (auto it = stored_iostat.begin(); it != stored_iostat.end(); it++) {
    // parsing the key-values from key "idt=u&id=xxx&tag=blabla"
    // of the hash map entry
    val = strtoull(it->second.c_str(), 0, 10);

    // populating StoreKeyStruct with values via UnfoldStatKey method
    if (!UnfoldStatKey(it->first.c_str())) {
      continue;
    }

    id = atoi(StoreKeyStruct["id"].c_str());

    if (StoreKeyStruct["id"] == "ALL") {
      continue;
    }

    if (StoreKeyStruct["idt"] == "u") {
      IostatUid[StoreKeyStruct["tag"]][(uid_t)id] = val;
    }

    if (StoreKeyStruct["idt"] == "g") {
      IostatGid[StoreKeyStruct["tag"]][(gid_t)id] = val;
    }
  }

  return true;
}

//------------------------------------------------------------------------------
// Circulate the entries to get stats collected over last sec, min, hour and day
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
        eos_static_err("msg=\"IoStat store failed\" hash_key=\"%s\"",
                       mReportHashKeyBase.c_str());
      }
    }

    sc++;
    assistant.wait_for(std::chrono::milliseconds(512));
    std::unique_lock<std::mutex> scope_lock(Mutex);
    google::sparse_hash_map<std::string, google::sparse_hash_map<uid_t, IostatPeriods> >::iterator
    tit;
    google::sparse_hash_map<std::string, IostatPeriods >::iterator dit;
    time_t now = time(NULL);

    // loop over tags
    for (tit = IostatPeriodsUid.begin(); tit != IostatPeriodsUid.end(); ++tit) {
      // loop over vids
      google::sparse_hash_map<uid_t, IostatPeriods>::iterator it;

      for (it = tit->second.begin(); it != tit->second.end(); ++it) {
        it->second.StampZero(now);
      }
    }

    for (tit = IostatPeriodsGid.begin(); tit != IostatPeriodsGid.end(); ++tit) {
      // loop over vids
      google::sparse_hash_map<uid_t, IostatPeriods>::iterator it;

      for (it = tit->second.begin(); it != tit->second.end(); ++it) {
        it->second.StampZero(now);
      }
    }

    // loop over domain accounting
    for (dit = IostatPeriodsDomainIOrb.begin();
         dit != IostatPeriodsDomainIOrb.end();
         dit++) {
      dit->second.StampZero(now);
    }

    for (dit = IostatPeriodsDomainIOwb.begin();
         dit != IostatPeriodsDomainIOwb.end();
         dit++) {
      dit->second.StampZero(now);
    }

    // loop over app accounting
    for (dit = IostatPeriodsAppIOrb.begin(); dit != IostatPeriodsAppIOrb.end();
         dit++) {
      dit->second.StampZero(now);
    }

    for (dit = IostatPeriodsAppIOwb.begin(); dit != IostatPeriodsAppIOwb.end();
         dit++) {
      dit->second.StampZero(now);
    }

    size_t popularitybin = (((time(NULL))) % (IOSTAT_POPULARITY_DAY *
                            IOSTAT_POPULARITY_HISTORY_DAYS)) / IOSTAT_POPULARITY_DAY;

    if (IostatLastPopularityBin != popularitybin) {
      // only if we enter a new bin we erase it
      std::unique_lock<std::mutex> scope_lock(mPopularityMutex);
      IostatPopularity[popularitybin].clear();
      IostatPopularity[popularitybin].resize(10000);
      IostatLastPopularityBin = popularitybin;
    }
  }

  eos_static_info("%s", "msg=\"stopping iostat circulate thread\"");
}


// resets the IostatUid and IostatGid counters to 0
// useful when new year starts
void
Iostat::ResetIostatMaps()
{
  std::unique_lock<std::mutex> reset_scope_lock(Mutex);

  // store user counters to the vectors
  for (auto tit : IostatUid) {
    for (auto it : tit.second) {
      IostatUid[tit.first][it.first] = 0;
    }
  }

  for (auto tit : IostatGid) {
    for (auto it : tit.second) {
      IostatGid[tit.first][it.first] = 0;
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
  std::unique_lock<std::mutex> scope_lock(mBcastMutex);

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
// Add UDP target
//------------------------------------------------------------------------------
bool
Iostat::AddUdpTarget(const std::string& target, bool store_and_lock)
{
  std::unique_lock<std::mutex> scope_lock(mBcastMutex, std::defer_lock);

  if (store_and_lock) {
    scope_lock.lock();
  }

  if (mUdpPopularityTarget.insert(target).second == false) {
    // Target already exists
    return false;
  }

  // Create an UDP socket for the specified target
  int udpsocket = -1;
  udpsocket = socket(AF_INET, SOCK_DGRAM, 0);

  if (udpsocket >= 0) {
    XrdOucString a_host, a_port, hp;
    int port = 0;
    hp = target.c_str();

    if (!eos::common::StringConversion::SplitKeyValue(hp, a_host, a_port)) {
      a_host = hp;
      a_port = "31000";
    }

    port = atoi(a_port.c_str());
    mUdpSocket[target] = udpsocket;
    XrdNetAddr* addrs  = 0;
    int         nAddrs = 0;
    const char* err    = XrdNetUtils::GetAddrs(a_host.c_str(), &addrs, nAddrs,
                         XrdNetUtils::allIPv64,
                         XrdNetUtils::NoPortRaw);

    if (err || nAddrs == 0) {
      return false;
    }

    memcpy((struct sockaddr*) &mUdpSockAddr[target], addrs[0].SockAddr(),
           sizeof(sockaddr));
    delete [] addrs;
    mUdpSockAddr[target].sin_family = AF_INET;
    mUdpSockAddr[target].sin_port = htons(port);
  }

  // Store configuration if required
  if (store_and_lock) {
    scope_lock.unlock();
    return StoreIostatConfig(&FsView::gFsView);
  }

  return true;
}

//------------------------------------------------------------------------------
// Remove UDP target
//------------------------------------------------------------------------------
bool
Iostat::RemoveUdpTarget(const std::string& target)
{
  bool store = false;
  bool retc = false;
  {
    std::unique_lock<std::mutex> scop_lock(mBcastMutex);

    if (mUdpPopularityTarget.count(target)) {
      mUdpPopularityTarget.erase(target);

      if (mUdpSocket.count(target)) {
        if (mUdpSocket[target] > 0) {
          // close the UDP socket
          close(mUdpSocket[target]);
        }

        mUdpSocket.erase(target);
        mUdpSockAddr.erase(target);
      }

      retc = true;
      store = true;
    }
  }

  if (store) {
    retc &= StoreIostatConfig(&FsView::gFsView);
  }

  return retc;
}

//------------------------------------------------------------------------------
// Do the UDP broadcast
//------------------------------------------------------------------------------
void
Iostat::UdpBroadCast(eos::common::Report* report) const
{
  std::string u = "";
  char fs[1024];
  std::unique_lock<std::mutex> scope_lock(mBcastMutex);

  for (auto it = mUdpPopularityTarget.cbegin();
       it != mUdpPopularityTarget.cend(); ++it) {
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

    int sendretc = sendto(mUdpSocket.at(*it), u.c_str(), u.length(), 0,
                          (struct sockaddr*) &mUdpSockAddr.at(*it),
                          sizeof(struct sockaddr_in));

    if (sendretc < 0) {
      eos_static_err("msg=\"failed to send udp message to %s\"", it->c_str());
    }

    eos_static_debug("sendto_retc=%d", sendretc);
  }
}

//------------------------------------------------------------------------------
// Add entry to popularity statistics
//------------------------------------------------------------------------------
void
Iostat::AddToPopularity(const std::string& path, unsigned long long rb,
                        time_t start, time_t stop)
{
  size_t popularitybin = (((start + stop) / 2) % (IOSTAT_POPULARITY_DAY *
                          IOSTAT_POPULARITY_HISTORY_DAYS)) / IOSTAT_POPULARITY_DAY;
  eos::common::Path cPath(path.c_str());
  std::unique_lock<std::mutex> scope_lock(mPopularityMutex);

  for (size_t k = 0; k < cPath.GetSubPathSize(); ++k) {
    std::string sp = cPath.GetSubPath(k);
    IostatPopularity[popularitybin][sp].rb += rb;
    IostatPopularity[popularitybin][sp].nread++;
  }
}

EOSMGMNAMESPACE_END
