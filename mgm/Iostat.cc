/*----------------------------------------------------------------------------*/
#include "common/Report.hh"
#include "mgm/Iostat.hh"
#include "mgm/XrdMgmOfs.hh"
/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

/* ------------------------------------------------------------------------- */
Iostat::Iostat() 
{
  mRunning = false;
  mInit = false;
  XrdSysThread::Run(&cthread, Iostat::StaticCirculate, static_cast<void *>(this),0, "Report Circulation Thread");
}

/* ------------------------------------------------------------------------- */
bool
Iostat::Start()
{
  if (!mInit) {
    XrdOucString queue = gOFS->MgmOfsBroker;
    queue += gOFS->ManagerId;
    queue += "/report";
    mClient.AddBroker(queue.c_str());
    mInit = true;
  }

  if (!mRunning) {
    mClient.Subscribe();
    XrdSysThread::Run(&thread, Iostat::StaticReceive, static_cast<void *>(this),0, "Report Receiver Thread");
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
  XrdSysThread::SetCancelOn();
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

      delete newmessage;
    }
    usleep(1000000);
    XrdSysThread::CancelPoint();
  }
  return 0;
}

/* ------------------------------------------------------------------------- */
void 
Iostat::PrintOut(XrdOucString &out, bool details, bool monitoring, bool numerical)
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
     snprintf(a86400,1023,"%3.02f", GetTotalAvg3600(tag));

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
         snprintf(a86400,1023,"%3.02f", it->second.GetAvg3600());

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
         snprintf(a86400,1023,"%3.02f", it->second.GetAvg3600());

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
   Mutex.UnLock();
}

EOSMGMNAMESPACE_END
