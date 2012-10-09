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

/*----------------------------------------------------------------------------*/
#include "common/Report.hh"
#include "common/Path.hh"
#include "mgm/Iostat.hh"
#include "mgm/XrdMgmOfs.hh"
/*----------------------------------------------------------------------------*/

#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>
/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysDNS.hh"
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

const char* Iostat::gIostatCollect       = "iostat::collect";
const char* Iostat::gIostatReport        = "iostat::report";
const char* Iostat::gIostatReportNamespace = "iostat::reportnamespace";
const char* Iostat::gIostatPopularity    = "iostat::popularity";
const char* Iostat::gIostatUdpTargetList = "iostat::udptargets";

/* ------------------------------------------------------------------------- */
Iostat::Iostat() 
{
  mRunning = false;
  mInit = false;
  mStoreFileName="";
  cthread = 0;
  thread = 0;

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
  IoNodes.insert("lxb");    // CERN batch cluster
  IoNodes.insert("pb-d-128-141"); // CERN DHCP
  IoNodes.insert("aldaq");  // ALICE DAQ
  IoNodes.insert("cms-cdr");// CMS DAQ
  IoNodes.insert("pc-tdq"); // ATLAS DAQ

  for (size_t i=0; i< IOSTAT_POPULARITY_HISTORY_DAYS; i++) {
    IostatPopularity[i].set_deleted_key("");
    IostatPopularity[i].resize(100000);
  }

  IostatLastPopularityBin = 0;
  mReportPopularity = true;
  mReportNamespace = false;
  mReport = true;
}

/* ------------------------------------------------------------------------- */
void
Iostat::StartCirculate()
{
  // we have to do after the name of the dump file was set, therefore the StartCirculate is an extra call
  XrdSysThread::Run(&cthread, Iostat::StaticCirculate, static_cast<void *>(this),XRDSYSTHREAD_HOLD, "Report Circulation Thread");
}

/* ------------------------------------------------------------------------- */
void
Iostat::ApplyIostatConfig() 
{
  std::string enabled="";
  
  std::string iocollect    = FsView::gFsView.GetGlobalConfig(Iostat::gIostatCollect);
  std::string ioreport     = FsView::gFsView.GetGlobalConfig(Iostat::gIostatReport);
  std::string ioreportns   = FsView::gFsView.GetGlobalConfig(Iostat::gIostatReportNamespace);
  std::string iopopularity = FsView::gFsView.GetGlobalConfig(Iostat::gIostatPopularity);
  std::string udplist      = FsView::gFsView.GetGlobalConfig(Iostat::gIostatUdpTargetList);

  if ( (iocollect == "true") || (iocollect == "") ) {
    // by default enable
    StartCollection();
  }

  {
    XrdSysMutexHelper mLock(Mutex);

    if (ioreport == "true") {
      mReport = true;
    } else {
      // by default is disabled
      mReport = false;
    }


    if (ioreportns == "true") {
      mReportNamespace = true;
    } else {
      // by default is disabled
      mReportNamespace = false;
    }
    
    if ( (iopopularity == "true") || (iopopularity == "")) {
      // by default enabled
      mReportPopularity = true;
    } else {
      mReportPopularity = false;
    }
  }

  {
    XrdSysMutexHelper mLock(BroadcastMutex);
    std::string delimiter="|";
    std::vector<std::string> hostlist;
    eos::common::StringConversion::Tokenize(udplist,hostlist,delimiter);
    mUdpPopularityTarget.clear();
    for (size_t i=0; i< hostlist.size(); i++) {
      AddUdpTarget(hostlist[i].c_str(),false);
    }
  }
}

/* ------------------------------------------------------------------------- */
bool
Iostat::StoreIostatConfig() 
{
  bool ok=1;

  ok &= FsView::gFsView.SetGlobalConfig(Iostat::gIostatPopularity,mReportPopularity?"true":"false");
  ok &= FsView::gFsView.SetGlobalConfig(Iostat::gIostatReport,mReport?"true":"false");
  ok &= FsView::gFsView.SetGlobalConfig(Iostat::gIostatReportNamespace,mReportNamespace?"true":"false");
  ok &= FsView::gFsView.SetGlobalConfig(Iostat::gIostatCollect, mRunning?"true":"false");
  ok &= FsView::gFsView.SetGlobalConfig(Iostat::gIostatUdpTargetList, mUdpPopularityTargetList.c_str()?mUdpPopularityTargetList.c_str():"");

  return ok;
}



/* ------------------------------------------------------------------------- */
bool
Iostat::Start()
{
  if (!mInit) {
    XrdOucString queue = gOFS->MgmOfsBroker;
    queue += gOFS->ManagerId;
    queue += "/report";
    queue.replace("root://","root://daemon@");
    mClient.AddBroker(queue.c_str());
    mInit = true;
  }

  if (!mRunning) {
    mClient.Subscribe();
    XrdSysThread::Run(&thread, Iostat::StaticReceive, static_cast<void *>(this),XRDSYSTHREAD_HOLD, "Report Receiver Thread");
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
    XrdSysThread::Cancel(thread);
    XrdSysThread::Join(thread,NULL);
    mRunning = false;
    mClient.Unsubscribe();
    return true;
  } else {
    return false;
  }
}

/* ------------------------------------------------------------------------- */
Iostat::~Iostat() 
{
  if (mRunning)
    Stop();
  XrdSysThread::Cancel(cthread);
  XrdSysThread::Join(cthread,NULL);
}

/* ------------------------------------------------------------------------- */
void* 
Iostat::StaticReceive(void* arg){
  return reinterpret_cast<Iostat*>(arg)->Receive();
}

/* ------------------------------------------------------------------------- */
void* 
Iostat::StaticCirculate(void* arg){
  return reinterpret_cast<Iostat*>(arg)->Circulate();
}

/* ------------------------------------------------------------------------- */
void* 
Iostat::Receive(void)
{
  while (1) {
    XrdMqMessage* newmessage = 0;
    while ( (newmessage = mClient.RecvMessage()) ) {
      XrdOucString body = newmessage->GetBody();
      while (body.replace("&&","&")) {}
      XrdOucEnv ioreport(body.c_str());
      eos::common::Report* report = new eos::common::Report(ioreport);
      Add("bytes_read",    report->uid, report->gid, report->rb,report->ots, report->cts);
      Add("bytes_written", report->uid, report->gid, report->wb,report->ots, report->cts);
      Add("read_calls", report->uid, report->gid, report->nrc,report->ots, report->cts);
      Add("write_calls", report->uid, report->gid, report->nwc,report->ots, report->cts);
      Add("bytes_rseek",  report->uid, report->gid, report->srb,report->ots, report->cts);
      Add("bytes_wseek",    report->uid, report->gid, report->swb,report->ots, report->cts);
      Add("disk_time_read",  report->uid, report->gid, (unsigned long long)report->rt,report->ots, report->cts);
      Add("disk_time_write",  report->uid, report->gid, (unsigned long long)report->wt,report->ots, report->cts);

      // do the UDP broadcasting here
      {
	XrdSysMutexHelper mLock(BroadcastMutex);
	if (mUdpPopularityTarget.size()) {
	  UdpBroadCast(report);
	}
      }

      // do the domain accounting here      
      if (report->path.substr(0, 11) == "/replicate:") {
	// check if this is a replication path
	// push into the 'eos' domain
	Mutex.Lock();
	if (report->rb) 
	  IostatAvgDomainIOrb["eos"].Add(report->rb, report->ots, report->cts);
	if (report->wb) 
	  IostatAvgDomainIOwb["eos"].Add(report->wb, report->ots, report->cts);
	Mutex.UnLock();
      } else {
	bool dfound=false;

	if (mReportPopularity) {
	  // do the popularity accounting here for everything which is not replication!
	  AddToPopularity(report->path, report->rb, report->ots, report->cts);
	}

	size_t pos=0;
	if ( (pos = report->sec_domain.rfind(".")) != std::string::npos) {
	  // we can sort in by domain
	  std::string sdomain = report->sec_domain.substr(pos);
	  if (IoDomains.find(sdomain) != IoDomains.end()) {
	    Mutex.Lock();
	    if (report->rb) 
	      IostatAvgDomainIOrb[sdomain].Add(report->rb, report->ots, report->cts);
	    if (report->wb)
	      IostatAvgDomainIOwb[sdomain].Add(report->wb, report->ots, report->cts);
	    Mutex.UnLock();
	    dfound=true;
	  }
	}
	
	// do the node accounting here - keep the node list small !!!
	std::set<std::string>::const_iterator nit;
	for (nit = IoNodes.begin(); nit != IoNodes.end(); nit++) {
	  if (*nit == report->sec_host.substr(0, nit->length())) {
	    Mutex.Lock();
	    if (report->rb) 
	      IostatAvgDomainIOrb[*nit].Add(report->rb, report->ots, report->cts);
	    if (report->wb) 
	      IostatAvgDomainIOwb[*nit].Add(report->wb, report->ots, report->cts);
	    Mutex.UnLock();
	    dfound=true;
	  }
	}
	
	if (!dfound) {
	  // push into the 'other' domain
	  Mutex.Lock();
	  if (report->rb) 
	    IostatAvgDomainIOrb["other"].Add(report->rb, report->ots, report->cts);
	  if (report->wb) 
	    IostatAvgDomainIOwb["other"].Add(report->wb, report->ots, report->cts);
	  Mutex.UnLock();
	}
      }

      // do the application accounting here      
      std::string apptag="other";
      if (report->sec_app.length()) {
	apptag = report->sec_app;
      }
      
      // push into the app accounting
      Mutex.Lock();
      if (report->rb) 
	IostatAvgAppIOrb[apptag].Add(report->rb, report->ots, report->cts);
      if (report->wb) 
	IostatAvgAppIOwb[apptag].Add(report->wb, report->ots, report->cts);
      Mutex.UnLock();

      if (mReport) {
        // add the record to a daily report log file

        static XrdOucString openreportfile="";
        static FILE* openreportfd=0;
        time_t now = time(NULL);
        struct tm nowtm;
        XrdOucString reportfile="";

        if (localtime_r(&now, &nowtm)) {
          static char logfile[4096];
          snprintf(logfile,sizeof(logfile) -1, "%s/%04u/%02u/%04u%02u%02u.eosreport",gOFS->IoReportStorePath.c_str(), 
                   1900+nowtm.tm_year,
                   nowtm.tm_mon+1,
                   1900+nowtm.tm_year,
                   nowtm.tm_mon+1,
                   nowtm.tm_mday);

          reportfile = logfile;
          
          if (reportfile == openreportfile) {
            // just add it here;
            if (openreportfd) {
              fprintf(openreportfd,"%s\n", body.c_str());
              fflush(openreportfd);
            }
          } else {
            if (openreportfd) 
              fclose(openreportfd);

            eos::common::Path cPath(reportfile.c_str());
            if (cPath.MakeParentPath(S_IRWXU)) {
              openreportfd = fopen(reportfile.c_str(),"a+");
              if (openreportfd) {
                fprintf(openreportfd,"%s\n", body.c_str());
                fflush(openreportfd);
              }
              openreportfile = reportfile;
            }
          }
        }
      }

      if (mReportNamespace) {
        // add the record into the report namespace file
        char path[4096];
        snprintf(path,sizeof(path)-1,"%s/%s", gOFS->IoReportStorePath.c_str(), report->path.c_str());
        eos::common::Path cPath(path);
        
        if (cPath.MakeParentPath(S_IRWXU)) {
          FILE* freport = fopen(path,"a+");
          if (freport) {
            fprintf(freport,"%s\n", body.c_str());
            fclose(freport);
          }
        }
      }

      delete report;
      delete newmessage;
    }
    XrdSysThread::SetCancelOn();
    XrdSysTimer sleeper;
    sleeper.Snooze(1);   
    XrdSysThread::CancelPoint();
    XrdSysThread::SetCancelOff();
    
  }
  return 0;
}

/* ------------------------------------------------------------------------- */
void 
Iostat::PrintOut(XrdOucString &out, bool summary, bool details, bool monitoring, bool numerical, bool top, bool domain, bool apps, XrdOucString option)
{
  Mutex.Lock();
  std::vector<std::string> tags;
  std::vector<std::string>::iterator it;
   
  google::sparse_hash_map<std::string, google::sparse_hash_map<uid_t, unsigned long long> >::iterator tit;
   
  for (tit = IostatUid.begin(); tit != IostatUid.end(); tit++) {
    tags.push_back(tit->first);
  }
   
  std::sort(tags.begin(),tags.end());

  char outline[1024];

  if (summary) {
    if (!monitoring) {
      out +="# -----------------------------------------------------------------------------------------------------------\n";
      sprintf(outline,"%-10s %-32s %-9s %8s %8s %8s %8s","who", "io value","sum","1min","5min","1h", "24h");
      out += outline;
      out += "\n";
      out +="# -----------------------------------------------------------------------------------------------------------\n";
    }
    
    for (it = tags.begin(); it!= tags.end(); ++it) {
      
      const char* tag = it->c_str();
      
      char a60[1024];
      char a300[1024];
      char a3600[1024];
      char a86400[1024];
      
      snprintf(a60,1023,"%3.02f", GetTotalAvg60(tag));
      snprintf(a300,1023,"%3.02f", GetTotalAvg300(tag));
      snprintf(a3600,1023,"%3.02f", GetTotalAvg3600(tag));
      snprintf(a86400,1023,"%3.02f", GetTotalAvg86400(tag));
      
      if (!monitoring) {
	XrdOucString sizestring;
	XrdOucString sa1;
	XrdOucString sa2;
	XrdOucString sa3;
	XrdOucString sa4;
	
	sprintf(outline,"ALL        %-32s %10s %8s %8s %8s %8s\n",tag, eos::common::StringConversion::GetReadableSizeString(sizestring,GetTotal(tag),""),eos::common::StringConversion::GetReadableSizeString(sa1,GetTotalAvg60(tag),""),eos::common::StringConversion::GetReadableSizeString(sa2,GetTotalAvg300(tag),""),eos::common::StringConversion::GetReadableSizeString(sa3,GetTotalAvg3600(tag),""),eos::common::StringConversion::GetReadableSizeString(sa4,GetTotalAvg86400(tag),""));
      } else {
	sprintf(outline,"uid=all gid=all measurement=%s total=%llu 60s=%s 300s=%s 3600s=%s 86400s=%s\n",tag, GetTotal(tag),a60,a300,a3600,a86400);
      }
      out += outline;
    }

    {
      XrdSysMutexHelper mLock(BroadcastMutex);
      std::set<std::string>::const_iterator it;
      if (mUdpPopularityTarget.size()) {
	if (!monitoring) {
	  out +="# -----------------------------------------------------------------------------------------------------------\n";
	  out +="# UDP Popularity Broadcast Target\n";
	}

	for (it = mUdpPopularityTarget.begin(); it != mUdpPopularityTarget.end(); it++) {
	  if (!monitoring) {      
	    out += it->c_str(); out +="\n";
	  } else {
	    out += "udptarget="; out += it->c_str(); out +="\n";
	  }
	}
      }
    }
  }

  if (details) {
    if (!monitoring) {
      out +="# -----------------------------------------------------------------------------------------------------------\n";
    }
    google::sparse_hash_map<std::string, google::sparse_hash_map<uid_t, IostatAvg > >::iterator tuit;
    google::sparse_hash_map<std::string, google::sparse_hash_map<gid_t, IostatAvg > >::iterator tgit;

    std::vector <std::string> uidout;
    std::vector <std::string> gidout;

    for (tuit = IostatAvgUid.begin(); tuit != IostatAvgUid.end(); tuit++) {
      google::sparse_hash_map<uid_t, IostatAvg>::iterator it;
      for (it = tuit->second.begin(); it != tuit->second.end(); ++it) {
        char a60[1024];
        char a300[1024];
        char a3600[1024];
        char a86400[1024];
         
        snprintf(a60,1023,"%3.02f", it->second.GetAvg60());
        snprintf(a300,1023,"%3.02f", it->second.GetAvg300());
        snprintf(a3600,1023,"%3.02f", it->second.GetAvg3600());
        snprintf(a86400,1023,"%3.02f", it->second.GetAvg86400());

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
          XrdOucString sizestring;
          XrdOucString sa1;
          XrdOucString sa2;
          XrdOucString sa3;
          XrdOucString sa4;

          sprintf(outline,"%-10s  %-32s %8s %8s %8s %8s %8s\n",identifier, tuit->first.c_str(),eos::common::StringConversion::GetReadableSizeString(sizestring,IostatUid[tuit->first.c_str()][it->first],""),eos::common::StringConversion::GetReadableSizeString(sa1,it->second.GetAvg60(),""),eos::common::StringConversion::GetReadableSizeString(sa2,it->second.GetAvg300(),""),eos::common::StringConversion::GetReadableSizeString(sa3,it->second.GetAvg3600(),""),eos::common::StringConversion::GetReadableSizeString(sa4,it->second.GetAvg86400(),""));
        } else {
          sprintf(outline,"%s gid=all measurement=%s total=%llu 60s=%s 300s=%s 3600s=%s 86400s=%s\n",identifier, tuit->first.c_str(), IostatUid[tuit->first.c_str()][it->first],a60,a300,a3600,a86400);
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
    if (!monitoring) {
      out +="# --------------------------------------------------------------------------------------\n";
    }
    for (tgit = IostatAvgGid.begin(); tgit != IostatAvgGid.end(); tgit++) {
      google::sparse_hash_map<gid_t, IostatAvg>::iterator it;
      for (it = tgit->second.begin(); it != tgit->second.end(); ++it) {
        char a60[1024];
        char a300[1024];
        char a3600[1024];
        char a86400[1024];
         
        snprintf(a60,1023,"%3.02f", it->second.GetAvg60());
        snprintf(a300,1023,"%3.02f", it->second.GetAvg300());
        snprintf(a3600,1023,"%3.02f", it->second.GetAvg3600());
        snprintf(a86400,1023,"%3.02f", it->second.GetAvg86400());

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
          XrdOucString sizestring;
          XrdOucString sa1;
          XrdOucString sa2;
          XrdOucString sa3;
          XrdOucString sa4;
           
          sprintf(outline,"%-10s  %-32s %8s %8s %8s %8s %8s\n",identifier, tgit->first.c_str(),eos::common::StringConversion::GetReadableSizeString(sizestring,IostatGid[tgit->first.c_str()][it->first],""),eos::common::StringConversion::GetReadableSizeString(sa1,it->second.GetAvg60(),""),eos::common::StringConversion::GetReadableSizeString(sa2,it->second.GetAvg300(),""),eos::common::StringConversion::GetReadableSizeString(sa3,it->second.GetAvg3600(),""),eos::common::StringConversion::GetReadableSizeString(sa4,it->second.GetAvg86400(),""));
        } else {
          sprintf(outline,"%s gid=all measurement=%s total=%llu 60s=%s 300s=%s 3600s=%s 86400s=%s\n",identifier, tgit->first.c_str(), IostatGid[tgit->first.c_str()][it->first],a60,a300,a3600,a86400);
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

  if (top) {
    for (it = tags.begin(); it!= tags.end(); ++it) {

      if (!monitoring) {
        out +="# --------------------------------------------------------------------------------------\n";
        out +="# top IO list by user name: "; out += it->c_str();out+= "\n";
        out +="# --------------------------------------------------------------------------------------\n";
      }

         
      std::vector <std::string> uidout;
      std::vector <std::string> gidout;
      std::vector<std::string>::reverse_iterator sit;
      google::sparse_hash_map<uid_t, unsigned long long>::iterator tuit;         
      for (tuit = IostatUid[*it].begin(); tuit != IostatUid[*it].end(); tuit++) {
        sprintf(outline,"%020llu|%u\n", tuit->second, tuit->first);
        uidout.push_back(outline);
      }
      std::sort(uidout.begin(),uidout.end());
      int topplace=0;
      for (sit = uidout.rbegin(); sit != uidout.rend(); sit++) {
        topplace++;
        std::string counter=sit->c_str();
        std::string suid = sit->c_str();
         
         
        XrdOucString stopplace="";
        XrdOucString sizestring="";
        stopplace += (int)topplace;
        counter.erase(sit->find("|"));
        suid.erase(0, sit->find("|")+1);
         
        uid_t uid = atoi(suid.c_str());
         
        char identifier[1024];
        if (numerical) {
          if (!monitoring)
            snprintf(identifier, 1023, "uid=%d", uid);
          else
            snprintf(identifier, 1023, "%d", uid);
        } else {
          int terrc=0;
          std::string username = eos::common::Mapping::UidToUserName(uid, terrc);
          snprintf(identifier, 1023, "%s", username.c_str());
        }
         
        if (!monitoring) {
          sprintf(outline,"[ %-16s ] %4s. %-10s %s\n", it->c_str(),stopplace.c_str(), identifier, eos::common::StringConversion::GetReadableSizeString(sizestring, strtoull(counter.c_str(),0,10),""));
        } else {
          sprintf(outline,"measurement=%s rank=%d uid=%s counter=%s\n",it->c_str(), topplace, identifier, counter.c_str());
        }

        out += outline;
      }
       
      // by gid name
       
      if (!monitoring) {
        out +="# --------------------------------------------------------------------------------------\n";
        out +="# top IO list by group name: "; out += it->c_str();out+= "\n";
        out +="# --------------------------------------------------------------------------------------\n";
      }

      google::sparse_hash_map<gid_t, unsigned long long>::iterator tgit;         
      for (tgit = IostatGid[*it].begin(); tgit != IostatGid[*it].end(); tgit++) {
        sprintf(outline,"%020llu|%u\n", tgit->second, tgit->first);
        gidout.push_back(outline);
      }
      std::sort(gidout.begin(),gidout.end());
      topplace=0;
      for (sit = gidout.rbegin(); sit != gidout.rend(); sit++) {
        topplace++;
        std::string counter=sit->c_str();
        std::string suid = sit->c_str();
         
         
        XrdOucString stopplace="";
        XrdOucString sizestring="";
        stopplace += (int)topplace;
        counter.erase(sit->find("|"));
        suid.erase(0, sit->find("|")+1);
         
        uid_t uid = atoi(suid.c_str());
         
        char identifier[1024];
        if (numerical) {
          if (!monitoring)
            snprintf(identifier, 1023, "gid=%d", uid);
          else
            snprintf(identifier, 1023, "%d", uid);
        } else {
          int terrc=0;
          std::string groupname = eos::common::Mapping::GidToGroupName(uid, terrc);
          snprintf(identifier, 1023, "%s", groupname.c_str());
        }
         
        if (!monitoring) {
          sprintf(outline,"[ %-16s ] %4s. %-10s %s\n", it->c_str(),stopplace.c_str(), identifier, eos::common::StringConversion::GetReadableSizeString(sizestring, strtoull(counter.c_str(),0,10),""));
        } else {
          sprintf(outline,"measurement=%s rank=%d gid=%s counter=%s\n",it->c_str(), topplace, identifier, counter.c_str());
        }
        out += outline;
      }
    }
  }

  if (domain) {
    XrdOucString sizestring;
    XrdOucString sa1;
    XrdOucString sa2;
    XrdOucString sa3;
    XrdOucString sa4;

    if (!monitoring) {
      out +="# --------------------------------------------------------------------------------------\n";
      out +="# IO by domain/node name: \n";
      out +="# --------------------------------------------------------------------------------------\n";
      sprintf(outline,"%-10s %-32s %9s %8s %8s %8s %8s\n", "io", "domain", "", "1min", "5min", "1h", "24h");
      out += outline;
      out +="# --------------------------------------------------------------------------------------\n";
    }

    // IO out bytes
    google::sparse_hash_map<std::string, IostatAvg>::iterator it;
    for (it=IostatAvgDomainIOrb.begin(); it!=IostatAvgDomainIOrb.end(); it++) {
      if (!monitoring) {
	sprintf(outline,"%-10s %-32s %9s %8s %8s %8s %8s\n", "OUT", it->first.c_str(),""
		,eos::common::StringConversion::GetReadableSizeString(sa1, (unsigned long long) it->second.GetAvg60(),"")
		,eos::common::StringConversion::GetReadableSizeString(sa2, (unsigned long long) it->second.GetAvg300(),"")
		,eos::common::StringConversion::GetReadableSizeString(sa3, (unsigned long long) it->second.GetAvg3600(),"")
		,eos::common::StringConversion::GetReadableSizeString(sa4, (unsigned long long) it->second.GetAvg86400(),""));
      } else {
	sprintf(outline,"measurement=%s domain=\"%s\" 60s=%llu 300s=%llu 3600s=%llu 86400s=%llu\n", "domain_io_out", it->first.c_str(), (unsigned long long) it->second.GetAvg60(),(unsigned long long) it->second.GetAvg300(),(unsigned long long) it->second.GetAvg3600(),(unsigned long long) it->second.GetAvg86400());
        }      
      out += outline;      
    }
    if (!monitoring)
      out += "\n";

    // IO in bytes
    for (it=IostatAvgDomainIOwb.begin(); it!=IostatAvgDomainIOwb.end(); it++) {
      if (!monitoring) {
	sprintf(outline,"%-10s %-32s %9s %8s %8s %8s %8s\n", "IN", it->first.c_str(),""
		,eos::common::StringConversion::GetReadableSizeString(sa1, (unsigned long long) it->second.GetAvg60(),"")
		,eos::common::StringConversion::GetReadableSizeString(sa2, (unsigned long long) it->second.GetAvg300(),"")
		,eos::common::StringConversion::GetReadableSizeString(sa3, (unsigned long long) it->second.GetAvg3600(),"")
		,eos::common::StringConversion::GetReadableSizeString(sa4, (unsigned long long) it->second.GetAvg86400(),""));
      } else {
	sprintf(outline,"measurement=%s domain=\"%s\" 60s=%llu 300s=%llu 3600s=%llu 86400s=%llu\n", "domain_io_in", it->first.c_str(), (unsigned long long) it->second.GetAvg60(),(unsigned long long) it->second.GetAvg300(),(unsigned long long) it->second.GetAvg3600(),(unsigned long long) it->second.GetAvg86400());
        }      
      out += outline;      
    }
  }

  if (apps) {
    XrdOucString sizestring;
    XrdOucString sa1;
    XrdOucString sa2;
    XrdOucString sa3;
    XrdOucString sa4;

    if (!monitoring) {
      out +="# --------------------------------------------------------------------------------------\n";
      out +="# IO by application name: \n";
      out +="# --------------------------------------------------------------------------------------\n";
      sprintf(outline,"%-10s %-32s %9s %8s %8s %8s %8s\n", "io", "application", "", "1min", "5min", "1h", "24h");
      out += outline;
      out +="# --------------------------------------------------------------------------------------\n";
    }
    
    // IO out bytes
    google::sparse_hash_map<std::string, IostatAvg>::iterator it;
    for (it=IostatAvgAppIOrb.begin(); it!=IostatAvgAppIOrb.end(); it++) {
      if (!monitoring) {
	sprintf(outline,"%-10s %-32s %9s %8s %8s %8s %8s\n", "OUT", it->first.c_str(),""
		,eos::common::StringConversion::GetReadableSizeString(sa1, (unsigned long long) it->second.GetAvg60(),"")
		,eos::common::StringConversion::GetReadableSizeString(sa2, (unsigned long long) it->second.GetAvg300(),"")
		,eos::common::StringConversion::GetReadableSizeString(sa3, (unsigned long long) it->second.GetAvg3600(),"")
		,eos::common::StringConversion::GetReadableSizeString(sa4, (unsigned long long) it->second.GetAvg86400(),""));
      } else {
	sprintf(outline,"measurement=%s app=\"%s\" 60s=%llu 300s=%llu 3600s=%llu 86400s=%llu\n", "app_io_out", it->first.c_str(), (unsigned long long) it->second.GetAvg60(),(unsigned long long) it->second.GetAvg300(),(unsigned long long) it->second.GetAvg3600(),(unsigned long long) it->second.GetAvg86400());
        }      
      out += outline;      
    }
    if (!monitoring) 
      out += "\n";

    // IO in bytes
    for (it=IostatAvgAppIOwb.begin(); it!=IostatAvgAppIOwb.end(); it++) {
      if (!monitoring) {
	sprintf(outline,"%-10s %-32s %9s %8s %8s %8s %8s\n", "IN", it->first.c_str(),""
		,eos::common::StringConversion::GetReadableSizeString(sa1, (unsigned long long) it->second.GetAvg60(),"")
		,eos::common::StringConversion::GetReadableSizeString(sa2, (unsigned long long) it->second.GetAvg300(),"")
		,eos::common::StringConversion::GetReadableSizeString(sa3, (unsigned long long) it->second.GetAvg3600(),"")
		,eos::common::StringConversion::GetReadableSizeString(sa4, (unsigned long long) it->second.GetAvg86400(),""));
      } else {
	sprintf(outline,"measurement=%s app=\"%s\" 60s=%llu 300s=%llu 3600s=%llu 86400s=%llu\n", "app_io_in", it->first.c_str(), (unsigned long long) it->second.GetAvg60(),(unsigned long long) it->second.GetAvg300(),(unsigned long long) it->second.GetAvg3600(),(unsigned long long) it->second.GetAvg86400());
        }      
      out += outline;      
    }
  }

  Mutex.UnLock();
}

/* ------------------------------------------------------------------------- */
void 
Iostat::PrintNs(XrdOucString &out, XrdOucString option)
{
  // ---------------------------------------------------------------------------
  // ! compute and printout the namespace popularity ranking
  // ---------------------------------------------------------------------------
  size_t limit=10;
  size_t popularitybin = ( ((time(NULL)))% (IOSTAT_POPULARITY_DAY * IOSTAT_POPULARITY_HISTORY_DAYS))/ IOSTAT_POPULARITY_DAY;
  size_t days = 1;
  time_t tmarker = time(NULL)/IOSTAT_POPULARITY_DAY*IOSTAT_POPULARITY_DAY;

  bool monitoring = false;
  bool bycount=false;
  bool bybytes=false;
  
  if ( (option.find("-m")) != STR_NPOS ) {
    monitoring = true;
  }

  if ( (option.find("-a")) != STR_NPOS ) {
    limit=999999999;
  }

  if ( (option.find("-100")) != STR_NPOS) {
    limit=100;
  } 

  if ( (option.find("-1000")) != STR_NPOS) {
    limit=1000;
  }

  if ( (option.find("-10000")) != STR_NPOS) {
    limit=10000;
  }

  if ( (option.find("-n") != STR_NPOS) ) {
    bycount=true;
  }

  if ( (option.find("-b") != STR_NPOS) ) {
    bybytes=true;
  }

  if ( (option.find("-w") != STR_NPOS) ) {
    days = IOSTAT_POPULARITY_HISTORY_DAYS;
  }

  if (!(bycount || bybytes)) {
    bybytes=bycount=true;
  }

  for (size_t pbin = 0; pbin < days; pbin++) {
    PopularityMutex.Lock();
    size_t sbin = (IOSTAT_POPULARITY_HISTORY_DAYS+popularitybin-pbin)%IOSTAT_POPULARITY_HISTORY_DAYS;
    google::sparse_hash_map<std::string, struct Popularity>::const_iterator it;
    
    std::vector<popularity_t> popularity_nread(IostatPopularity[sbin].begin(), IostatPopularity[sbin].end());
    std::vector<popularity_t> popularity_rb   (IostatPopularity[sbin].begin(), IostatPopularity[sbin].end());

    // sort them (backwards) by rb or nread
    std::sort(popularity_nread.begin(), popularity_nread.end(), PopularityCmp_nread());
    std::sort(popularity_rb.begin(),    popularity_rb.end(),    PopularityCmp_rb() );
    
    XrdOucString marker = "<today>";
    if (pbin ==1) {
      marker = "<yesterday>";
    } 
    if (pbin ==2) {
      marker = "<2 days ago>";
    } 
    if (pbin ==3) {
      marker = "<3 days ago>";
    }
    if (pbin ==4) {
      marker = "<4 days ago>";
    }
    if (pbin ==5) {
      marker = "<5 days ago>";
    }
    if (pbin ==6) {
      marker = "<6 days ago>";
    }

    std::vector<popularity_t>::const_iterator popit;

    if (bycount) {
      if (!monitoring) {
	char outline[4096];
	out +="# --------------------------------------------------------------------------------------\n";
	snprintf(outline,sizeof(outline)-1, "%-6s %-14s %-14s %-64s\n", "rank", "by(read count)", "read bytes","path");
	out += outline;
	out +="# --------------------------------------------------------------------------------------\n";
      }

      size_t cnt=0;

      for (popit = popularity_nread.begin(); popit != popularity_nread.end(); popit++) {
	cnt++;
	if (cnt > limit)
	  break;
	char line[4096];
	char nr[256];
	snprintf(nr,sizeof(nr)-1, "%u", popit->second.nread);
	if (monitoring) {
	  snprintf(line,sizeof(line)-1,"measurement=popularitybyaccess time=%u rank=%d nread=%u rb=%llu path=%s\n",(unsigned int)tmarker, (int) cnt, popit->second.nread, popit->second.rb, popit->first.c_str());
	} else {
	  XrdOucString sizestring;
	  snprintf(line,sizeof(line)-1,"%06d nread=%-7s rb=%-10s %-64s\n", (int)cnt, nr, eos::common::StringConversion::GetReadableSizeString(sizestring, popit->second.rb,"B"), popit->first.c_str());
	}
	out += line;
      }
    }

    if (bybytes) {
      if (!monitoring) {
	char outline[4096];
	out +="# --------------------------------------------------------------------------------------\n";
	snprintf(outline,sizeof(outline)-1, "%-6s %-14s %-14s %-64s\n", "rank", "by(read bytes)", "read count","path");
	out += outline;
	out +="# --------------------------------------------------------------------------------------\n";
      }
      size_t cnt=0;
      for (popit = popularity_rb.begin(); popit != popularity_rb.end(); popit++) {
	cnt++;
	if (cnt>limit)
	  break;
	char line[4096];
	char nr[256];
	snprintf(nr,sizeof(nr)-1, "%u", popit->second.nread);
	if (monitoring) {
	  snprintf(line,sizeof(line)-1,"measurement=popularitybyvolume time=%u rank=%d nread=%u rb=%llu path=%s\n", (unsigned int)tmarker, (int) cnt, popit->second.nread, popit->second.rb, popit->first.c_str());
	} else {
	  XrdOucString sizestring;
	  snprintf(line,sizeof(line)-1,"%06d rb=%-10s nread=%-7s %-64s\n", (int)cnt, eos::common::StringConversion::GetReadableSizeString(sizestring, popit->second.rb,"B"), nr, popit->first.c_str());
	}
	out += line;
      }
    }

    
    /*    for (it = IostatPopularity[sbin].begin(); it != IostatPopularity[sbin].end(); it++) {

      }*/
    PopularityMutex.UnLock();
  }
}

/* ------------------------------------------------------------------------- */
bool 
Iostat::Store() 
{
  // ---------------------------------------------------------------------------
  // ! save current uid/gid counters to a dump file
  // ---------------------------------------------------------------------------
  XrdOucString tmpname = mStoreFileName;
  if (!mStoreFileName.length())
    return false;

  tmpname += ".tmp";
  FILE* fout = fopen(tmpname.c_str(),"w+");
  if (!fout)
    return false;

  if (chmod(tmpname.c_str(),S_IRWXU| S_IRGRP | S_IROTH)) {
    return false;
  }

  google::sparse_hash_map<std::string, google::sparse_hash_map<uid_t, unsigned long long> >::iterator tuit;
  google::sparse_hash_map<std::string, google::sparse_hash_map<uid_t, unsigned long long> >::iterator tgit;
  Mutex.Lock();
  // store user counters
  for (tuit = IostatUid.begin(); tuit != IostatUid.end(); tuit++) {
    google::sparse_hash_map<uid_t, unsigned long long>::iterator it;
    for (it = tuit->second.begin(); it != tuit->second.end(); ++it) {
      fprintf(fout,"tag=%s&uid=%u&val=%llu\n", tuit->first.c_str(), it->first, it->second);
    }
  }
  // store group counter
  for (tgit = IostatGid.begin(); tgit != IostatGid.end(); tgit++) {
    google::sparse_hash_map<uid_t, unsigned long long>::iterator it;
    for (it = tgit->second.begin(); it != tgit->second.end(); ++it) {
      fprintf(fout,"tag=%s&gid=%u&val=%llu\n", tgit->first.c_str(), it->first, it->second);
    }
  }
  Mutex.UnLock();
  
  fclose(fout);
  return (rename(tmpname.c_str(), mStoreFileName.c_str())?false:true);
}

/* ------------------------------------------------------------------------- */
bool 
Iostat::Restore() 
{
  // ---------------------------------------------------------------------------
  // ! load current uid/gid counters from a dump file
  // ---------------------------------------------------------------------------
  if (!mStoreFileName.length())
    return false;
  
  FILE* fin = fopen(mStoreFileName.c_str(),"r");
  if (!fin)
    return false;

  Mutex.Lock();
  int item =0;
  char line[16384];
  while ((item = fscanf(fin, "%s\n", line))==1) {
    XrdOucEnv env(line);
    if (env.Get("tag") && env.Get("uid") && env.Get("val")) {
      std::string tag = env.Get("tag");
      uid_t uid = atoi(env.Get("uid"));
      unsigned long long val = strtoull(env.Get("val"),0,10);
      IostatUid[tag][uid] = val;
    }
    if (env.Get("tag") && env.Get("gid") && env.Get("val")) {
      std::string tag = env.Get("tag");
      gid_t gid = atoi(env.Get("gid"));
      unsigned long long val = strtoull(env.Get("val"),0,10);
      IostatGid[tag][gid] = val;
    }
  }
  Mutex.UnLock();
  fclose(fin);
  return true;
}

/* ------------------------------------------------------------------------- */
bool 
Iostat::NamespaceReport(const char* path, XrdOucString &stdOut, XrdOucString &stdErr)
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
  
  unsigned long long totalreadbytes  = 0;
  unsigned long long totalwritebytes = 0;
  double totalreadtime  = 0;
  double totalwritetime = 0;
  unsigned long long rcount  = 0;
  unsigned long long wcount  = 0;

  while(std::getline(inFile, reportLine)) {
    XrdOucEnv ioreport(reportLine.c_str());
    eos::common::Report* report = new eos::common::Report(ioreport);
    report->Dump(stdOut);
    if(!report->wb) {
      rcount++;
      totalreadtime   += ((report->cts - report->ots) + (1.0 * (report->ctms - report->otms) / 1000000 ));    
      totalreadbytes  += report->rb;
    } else {
      wcount++;
      totalwritetime   += ((report->cts - report->ots) + (1.0 * (report->ctms - report->otms) / 1000000 ));
      totalwritebytes += report->wb;
    }
    delete report;
  }
  stdOut += "----------------------- SUMMARY -------------------\n";
  char summaryline[4096];
  XrdOucString sizestring1, sizestring2;

  snprintf(summaryline, sizeof(summaryline) -1 ,"| avg. readd: %.02f MB/s | avg. write: %.02f  MB/s | total read: %s | total write: %s | times read: %llu | times written: %llu |\n", totalreadtime?(totalreadbytes/totalreadtime / 1000000.0):0, totalwritetime?(totalwritebytes/totalwritetime / 1000000.0):0, eos::common::StringConversion::GetReadableSizeString(sizestring1, totalreadbytes,"B"),eos::common::StringConversion::GetReadableSizeString(sizestring2, totalwritebytes,"B"), rcount, wcount);
  stdOut += summaryline;

  return true;
}

/* ------------------------------------------------------------------------- */
void* 
Iostat::Circulate() {
  // ---------------------------------------------------------------------------
  // ! circulate the entries to get averages over sec.min.hour and day
  // ---------------------------------------------------------------------------

  unsigned long long sc = 0 ;
  XrdSysThread::SetCancelOn();
  // empty the circular buffer 
  while(1) {
    // we store once per minute the current statistics 
    if (!(sc%117)) {
      // save the current state ~ every minute
      if (!Store()) {
	eos_static_err("failed store io stat dump file <%s>",mStoreFileName.c_str());
      }
    }
    sc++;
    XrdSysTimer sleeper;
    sleeper.Wait(512);
    Mutex.Lock();
    google::sparse_hash_map<std::string, google::sparse_hash_map<uid_t, IostatAvg> >::iterator tit;
    google::sparse_hash_map<std::string, IostatAvg >::iterator dit;
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
    
    // loop over domain accounting
    for (dit = IostatAvgDomainIOrb.begin(); dit != IostatAvgDomainIOrb.end(); dit++) {
      dit->second.StampZero();
      }
    for (dit = IostatAvgDomainIOwb.begin(); dit != IostatAvgDomainIOwb.end(); dit++) {
      dit->second.StampZero();
    }
    // loop over app accounting
    for (dit = IostatAvgAppIOrb.begin(); dit != IostatAvgAppIOrb.end(); dit++) {
      dit->second.StampZero();
      }
    for (dit = IostatAvgAppIOwb.begin(); dit != IostatAvgAppIOwb.end(); dit++) {
      dit->second.StampZero();
    }
    
    Mutex.UnLock();

    size_t popularitybin = ( ((time(NULL)))% (IOSTAT_POPULARITY_DAY * IOSTAT_POPULARITY_HISTORY_DAYS))/ IOSTAT_POPULARITY_DAY;
    if (IostatLastPopularityBin != popularitybin) {
      // only if we enter a new bin we erase it 
      PopularityMutex.Lock();
      IostatPopularity[popularitybin].clear();
      IostatPopularity[popularitybin].resize(10000);
      IostatLastPopularityBin = popularitybin;
      PopularityMutex.UnLock();
    }
    XrdSysThread::CancelPoint();
  }
  return 0;
}

/* ------------------------------------------------------------------------- */
bool
Iostat::StartPopularity()
{
  {
    XrdSysMutexHelper mLock(Mutex);
    if (mReportPopularity) 
      return false;
    mReportPopularity = true;
  }
  StoreIostatConfig();
  return true;
}

/* ------------------------------------------------------------------------- */
bool
Iostat::StopPopularity()
{
  {
    XrdSysMutexHelper mLock(Mutex);
    if (!mReportPopularity) 
      return false;
    mReportPopularity = false;
  }
  StoreIostatConfig();
  return true;
}

/* ------------------------------------------------------------------------- */
bool
Iostat::StartReport()
{
  {
    XrdSysMutexHelper mLock(Mutex);
    if (mReport) 
      return false;
    mReport = true;
  }
  StoreIostatConfig();
  return true;
}

/* ------------------------------------------------------------------------- */
bool
Iostat::StopReport()
{
  {
    XrdSysMutexHelper mLock(Mutex);
    if (!mReport) 
      return false;
    mReport = false;
  }
  StoreIostatConfig();
  return true;
}

/* ------------------------------------------------------------------------- */
bool
Iostat::StartCollection()
{
  bool retc=false;
  {
    XrdSysMutexHelper mLock(Mutex);
    retc = Start();
  }
  if (retc) {
    StoreIostatConfig();
  }
  return retc;
}

/* ------------------------------------------------------------------------- */
bool
Iostat::StopCollection()
{
  bool retc=false;
  {
    XrdSysMutexHelper mLock(Mutex);
    retc = Stop();
  }
  if (retc) {
    StoreIostatConfig();
  }
  return retc;
}

/* ------------------------------------------------------------------------- */
bool
Iostat::StartReportNamespace()
{
  {
    XrdSysMutexHelper mLock(Mutex);
    if (mReportNamespace) 
      return false;
    mReportNamespace = true;
  }
  StoreIostatConfig();
  return true;
}

/* ------------------------------------------------------------------------- */
bool
Iostat::StopReportNamespace()
{
  {
    XrdSysMutexHelper mLock(Mutex);
    if (!mReportNamespace) 
      return false;
    mReportNamespace = false;
  }
  StoreIostatConfig();
  return true;
}

/* ------------------------------------------------------------------------- */
bool
Iostat::AddUdpTarget(const char* target, bool storeitandlock)
{
  {
    if (storeitandlock) BroadcastMutex.Lock();
    std::string starget = target;
    if (mUdpPopularityTarget.count(starget)) {
      if (storeitandlock) BroadcastMutex.UnLock();
      return false;
    }
    
    mUdpPopularityTarget.insert(starget);
    
    // create an UDP socket for the specified target
    int udpsocket=-1;
    udpsocket=socket(AF_INET,SOCK_DGRAM,0);

    if (udpsocket>0) {
      XrdOucString a_host, a_port, hp;
      int port=0;
      hp = starget.c_str();
      if (!eos::common::StringConversion::SplitKeyValue(hp, a_host, a_port)) {
	a_host=hp;
	a_port="31000";
      }
      port = atoi(a_port.c_str());

      mUdpSocket[starget] = udpsocket;		

      XrdSysDNS::getHostAddr(a_host.c_str(), (struct sockaddr*)&mUdpSockAddr[starget]);
      mUdpSockAddr[starget].sin_family = AF_INET;
      mUdpSockAddr[starget].sin_port=htons(port);
    }

    // rebuild the list for the configuration
    mUdpPopularityTargetList = "";
    std::set<std::string>::const_iterator it;
    for (it = mUdpPopularityTarget.begin(); it != mUdpPopularityTarget.end(); it++) {
      mUdpPopularityTargetList += it->c_str();
      mUdpPopularityTargetList += "|";
    }
    if (mUdpPopularityTargetList.length()) {
      mUdpPopularityTargetList.erase(mUdpPopularityTargetList.length()-1);
    }
  }
  // store the configuration
  if ((storeitandlock) && (!StoreIostatConfig())) {
    return false;
  }
  if (storeitandlock) BroadcastMutex.UnLock();
  return true;
}

/* ------------------------------------------------------------------------- */
bool
Iostat::RemoveUdpTarget(const char* target)
{
  bool store=false;
  bool retc=false;
  {
    XrdSysMutexHelper mLock(BroadcastMutex);
    std::string starget = target;
    if (mUdpPopularityTarget.count(starget)) {
      mUdpPopularityTarget.erase(starget);
      if (mUdpSocket.count(starget)) {
	if (mUdpSocket[starget]>0) {
	  // close the UDP socket
	  close(mUdpSocket[starget]);
	}
	mUdpSocket.erase(starget);
	mUdpSockAddr.erase(starget);
      }
      // rebuild the list for the configuration
      mUdpPopularityTargetList = "";
      std::set<std::string>::const_iterator it;
      for (it = mUdpPopularityTarget.begin(); it != mUdpPopularityTarget.end(); it++) {
	mUdpPopularityTargetList += it->c_str();
	mUdpPopularityTargetList += "|";
      }
      if (mUdpPopularityTargetList.length()) {
	mUdpPopularityTargetList.erase(mUdpPopularityTargetList.length()-1);
      }
      retc = true;
      store = true;
    }
  }

  if (store) {
    retc &= StoreIostatConfig();
  }
  return retc;
}

/* ------------------------------------------------------------------------- */
void
Iostat::UdpBroadCast(eos::common::Report* report) 
{

  std::set<std::string>::const_iterator it;
  std::string u="";
  char fs[1024];
  for (it = mUdpPopularityTarget.begin(); it != mUdpPopularityTarget.end(); it++) {
    u="";
    XrdOucString tg = it->c_str();
    XrdOucString sizestring;

    if (tg.endswith("/json")) {
      // do json format broadcast
      tg.replace("/json","");
      u += "{\"app_info\": \"";             u += report->sec_app; u += "\",\n";
      u += " \"client_domain\": \"";        u += report->sec_domain; u += "\",\n";
      u += " \"client_host\": \"";          u += report->sec_host; u += "\",\n";
      u += " \"end_time\": \"";             u += eos::common::StringConversion::GetSizeString(sizestring, report->cts); u += "\",\n";
      u += " \"file_lfn\": \"";             u += report->path; u += "\"\n";
      u += " \"file_size\": \"";            u += eos::common::StringConversion::GetSizeString(sizestring, report->csize); u+= "\",\n";
      u += " \"read_average\": \"";         u += eos::common::StringConversion::GetSizeString(sizestring, report->rb/((report->nrc)?report->nrc:999999999)); u += "\",\n";
      u += " \"read_bytes_at_close\": \"";  u += eos::common::StringConversion::GetSizeString(sizestring, report->rb); u += "\",\n";
      u += " \"read_bytes\": \"";           u += eos::common::StringConversion::GetSizeString(sizestring, report->rb); u += "\",\n";
      u += " \"read_max\": \"";             u += eos::common::StringConversion::GetSizeString(sizestring, report->rb_max); u += "\",\n";
      u += " \"read_min\": \"";             u += eos::common::StringConversion::GetSizeString(sizestring, report->rb_min); u += "\",\n";
      u += " \"read_operations\": \"";      u += eos::common::StringConversion::GetSizeString(sizestring, report->nrc); u += "\",\n";
      snprintf(fs,sizeof(fs)-1, "%.02f",report->rb_sigma);
      u += " \"read_sigma\": \"";          u += fs; u += "\",\n";
      /* -- we have currently no access to this information */
      /*      u += " \"read_single_average\": \"";  u += "0"; u += "\",\n";
      u += " \"read_single_bytes\": \"";    u += eos::common::StringConversion::GetSizeString(sizestring, report->rb); u += "\",\n";
      u += " \"read_single_max\": \"";      u += "0"; u += "\",\n";
      u += " \"read_single_min\": \"";      u += "0"; u += "\",\n";
      u += " \"read_single_operations\": \"";    u += eos::common::StringConversion::GetSizeString(sizestring, report->nrc); u += "\",\n";
      u += " \"read_single_sigma\": \"";    u += "0"; u += "\",\n";
      u += " \"read_vector_average\": \"";  u += "0"; u += "\",\n";
      u += " \"read_vector_bytes\": \"";    u += "0"; u += "\",\n";
      u += " \"read_vector_count_average\": \""; u += "0"; u += "\",\n";
      u += " \"read_vector_count_max\": \"";u += "0"; u += "\",\n";
      u += " \"read_vector_count_min\": \"";u += "0"; u += "\",\n";
      u += " \"read_vector_count_sigma\": \"";   u += "0"; u += "\",\n";
      u += " \"read_vector_max\": \"";      u += "0"; u += "\",\n";
      u += " \"read_vector_min\": \"";      u += "0"; u += "\",\n";
      u += " \"read_vector_operations\": \""; u += "0"; u += "\",\n";
      u += " \"read_vector_sigma\": \"";    u += "0"; u += "\",\n"; */
      u += " \"server_domain\": \"";        u += report->server_domain; u += "\",\n";
      u += " \"server_host\": \"";          u += report->server_name; u += "\",\n";
      u += " \"server_username\": \"";      u += report->sec_name; u += "\",\n";
      u += " \"start_time\": \"";           u += eos::common::StringConversion::GetSizeString(sizestring, report->ots); u += "\",\n";
      XrdOucString stime; // stores the current time in <s>.<ns> 
      u += " \"unique_id\": \"";            u += gOFS->MgmOfsInstanceName.c_str(); u += "-"; u += eos::common::StringConversion::TimeNowAsString(stime); u += "\",\n";
      u += " \"user_dn\": \"";               u += report->sec_info;  u += "\",\n";
      u += " \"user_fqan\": \"";            u += report->sec_grps; u += "\",\n";
      u += " \"user_role\": \"";            u += report->sec_role; u += "\",\n";
      u += " \"user_vo\": \"";              u += report->sec_vorg; u += "\",\n";
      u += " \"write_average\": \"";        u += eos::common::StringConversion::GetSizeString(sizestring, report->wb/((report->nwc)?report->nwc:999999999)); u += "\",\n";
      u += " \"write_bytes_at_close\": \""; u += eos::common::StringConversion::GetSizeString(sizestring, report->wb); u += "\",\n";
      u += " \"write_bytes\": \"";          u += eos::common::StringConversion::GetSizeString(sizestring, report->wb); u += "\",\n";
      u += " \"write_max\": \"";            u += eos::common::StringConversion::GetSizeString(sizestring, report->wb_max); u += "\",\n";
      u += " \"write_min\": \"";            u += eos::common::StringConversion::GetSizeString(sizestring, report->wb_min); u += "\",\n";
      u += " \"write_operations\": \"";     u += eos::common::StringConversion::GetSizeString(sizestring, report->nwc); u += "\",\n";
      snprintf(fs,sizeof(fs)-1, "%.02f",report->wb_sigma);
      u += " \"write_sigma\": \"";            u += fs; u += "\"}\n";
    } else { 
      // do default format broadcast
      u += "#begin\n";
      u += "app_info=";             u += report->sec_app; u += "\n";
      u += "client_domain=";        u += report->sec_domain; u += "\n";
      u += "client_host=";          u += report->sec_host; u += "\n";
      u += "end_time=";             u += eos::common::StringConversion::GetSizeString(sizestring, report->cts); u += "\n";
      u += "file_lfn = ";              u += report->path; u += "\n";
      u += "file_size = ";             u += eos::common::StringConversion::GetSizeString(sizestring, report->csize); u+= "\n";
      u += "read_average=";         u += eos::common::StringConversion::GetSizeString(sizestring, report->rb/((report->nrc)?report->nrc:999999999)); u += "\n";
      u += "read_bytes_at_close=";  u += eos::common::StringConversion::GetSizeString(sizestring, report->rb); u += "\n";
      u += "read_bytes=";           u += eos::common::StringConversion::GetSizeString(sizestring, report->rb); u += "\n";
      u += "read_min=";             u += eos::common::StringConversion::GetSizeString(sizestring, report->rb_min); u += "\n";
      u += "read_max=";             u += eos::common::StringConversion::GetSizeString(sizestring, report->rb_max); u += "\n";
      u += "read_operations=";      u += eos::common::StringConversion::GetSizeString(sizestring, report->nrc); u += "\n";
      u += "read_sigma=";           u += "0"; u += "\n";
      snprintf(fs,sizeof(fs)-1, "%.02f",report->rb_sigma);
      u += "read_sigma=";           u += fs;  u += "\n";
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
      u += "server_domain=";        u += report->server_domain; u += "\n";
      u += "server_host=";          u += report->server_name; u += "\n";
      u += "server_username=";      u += report->sec_name; u += "\n";
      u += "start_time=";           u += eos::common::StringConversion::GetSizeString(sizestring, report->ots); u += "\n";
      XrdOucString stime; // stores the current time in <s>.<ns> 
      u += "unique_id=";            u += gOFS->MgmOfsInstanceName.c_str(); u += "-"; u += eos::common::StringConversion::TimeNowAsString(stime); u += "\n";
      u += "user_dn = ";             u += report->sec_info; u += "\n";
      u += "user_fqan=";            u += report->sec_grps; u += "\n";
      u += "user_role=";            u += report->sec_role; u += "\n";
      u += "user_vo=";              u += report->sec_vorg; u += "\n";
      u += "write_average=";        u += eos::common::StringConversion::GetSizeString(sizestring, report->wb/((report->nwc)?report->nwc:999999999)); u += "\n";
      u += "write_bytes_at_close="; u += eos::common::StringConversion::GetSizeString(sizestring, report->wb); u += "\n";
      u += "write_bytes=";          u += eos::common::StringConversion::GetSizeString(sizestring, report->wb); u += "\n";
      u += "write_min=";            u += eos::common::StringConversion::GetSizeString(sizestring, report->wb_min); u += "\n";
      u += "write_max=";            u += eos::common::StringConversion::GetSizeString(sizestring, report->wb_max); u += "\n";
      u += "write_operations=";     u += eos::common::StringConversion::GetSizeString(sizestring, report->nwc); u += "\n";
      snprintf(fs,sizeof(fs)-1, "%.02f",report->rb_sigma);
      u += "write_sigma=";          u += fs;  u += "\n";
      u += "#end\n"; 
    }
    int sendretc=sendto(mUdpSocket[*it],u.c_str(),u.length(),0,(struct sockaddr *)&mUdpSockAddr[*it],sizeof(struct sockaddr_in));
    if (sendretc<0) {
      eos_static_err("failed to send udp message to %s\n", it->c_str());
    }
    if (EOS_LOGS_DEBUG) {
      fprintf(stderr,"===>UDP\n%s<===UDP\n",u.c_str());
      eos_static_debug("retc(sendto)=%d", sendretc);
    }
  }
}
EOSMGMNAMESPACE_END
