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
      Add("bytes read",    report->uid, report->gid, report->rb,report->ots, report->cts);
      Add("bytes written", report->uid, report->gid, report->wb,report->ots, report->cts);
      Add("read  calls", report->uid, report->gid, report->nrc,report->ots, report->cts);
      Add("write calls", report->uid, report->gid, report->nwc,report->ots, report->cts);
      Add("bytes rseek",  report->uid, report->gid, report->srb,report->ots, report->cts);
      Add("bytes wseek",    report->uid, report->gid, report->swb,report->ots, report->cts);
      Add("disk time read",  report->uid, report->gid, (unsigned long long)report->rt,report->ots, report->cts);
      Add("disk time write",  report->uid, report->gid, (unsigned long long)report->wt,report->ots, report->cts);

      delete newmessage;
    }
    usleep(1000000);
    XrdSysThread::CancelPoint();
  }
  return 0;
}

/* ------------------------------------------------------------------------- */
void 
Iostat::PrintOut(XrdOucString &out, bool details, bool monitoring)
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
     sprintf(outline,"%-8s %-32s %-9s %8s %8s %8s","who", "command","sum","1min","5min","1h");
     out += outline;
     out += "\n";
     out +="# -----------------------------------------------------------------------------------------------------------\n";
   }

   for (it = tags.begin(); it!= tags.end(); ++it) {
     
     const char* tag = it->c_str();
     
     char a60[1024];
     char a300[1024];
     char a3600[1024];

     sprintf(a60,"%3.02f", GetTotalAvg60(tag));
     sprintf(a300,"%3.02f", GetTotalAvg300(tag));
     sprintf(a3600,"%3.02f", GetTotalAvg3600(tag));

     if (!monitoring) {
       XrdOucString sizestring;
       XrdOucString sa1;
       XrdOucString sa2;
       XrdOucString sa3;
       sprintf(outline,"ALL      %-32s %10s %8s %8s %8s\n",tag, eos::common::StringConversion::GetReadableSizeString(sizestring,GetTotal(tag),""),eos::common::StringConversion::GetReadableSizeString(sa1,GetTotalAvg60(tag),""),eos::common::StringConversion::GetReadableSizeString(sa2,GetTotalAvg300(tag),""),eos::common::StringConversion::GetReadableSizeString(sa3,GetTotalAvg3600(tag),""));
     } else {
       sprintf(outline,"uid=all gid=all cmd=%s total=%llu 60s=%s 300s=%s 3600s=%s\n",tag, GetTotal(tag),a60,a300,a3600);
     }
     out += outline;
   }
   Mutex.UnLock();
}

EOSMGMNAMESPACE_END
