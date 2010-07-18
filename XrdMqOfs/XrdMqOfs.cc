//          $Id: XrdMqOfs.cc,v 1.00 2007/10/04 01:34:19 ajp Exp $

const char *XrdMqOfsCVSID = "$Id: XrdMqOfs.cc,v 1.0.0 2007/10/04 01:34:19 ajp Exp $";


#include "XrdVersion.hh"
#include "XrdClient/XrdClientAdmin.hh"
#include "XrdOss/XrdOss.hh"
#include "XrdOss/XrdOssApi.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucTokenizer.hh"
#include "XrdOuc/XrdOucTrace.hh"
#include "XrdOuc/XrdOucTList.hh"
#include "XrdSys/XrdSysError.hh"
#include "XrdSys/XrdSysLogger.hh"
#include "XrdSys/XrdSysPthread.hh"
#include "XrdSys/XrdSysTimer.hh"
#include "XrdSec/XrdSecInterface.hh"
#include "XrdSfs/XrdSfsAio.hh"
#include "XrdNet/XrdNetDNS.hh"
#include "XrdMqOfs/XrdMqOfs.hh"
#include "XrdMqOfs/XrdMqMessage.hh"
#include "XrdOfs/XrdOfsTrace.hh"

#include <pwd.h>
#include <grp.h>

/******************************************************************************/
/*                        G l o b a l   O b j e c t s                         */
/******************************************************************************/

XrdSysError     OfsEroute(0);  
extern XrdOssSys *XrdOfsOss;
XrdSysError    *XrdMqOfs::eDest;
extern XrdOucTrace OfsTrace;
extern XrdOss    *XrdOssGetSS(XrdSysLogger *, const char *, const char *);

XrdOucHash<XrdOucString>*     XrdMqOfs::stringstore;

XrdMqOfs   XrdOfsFS;



/******************************************************************************/
/*                        C o n v i n i e n c e                               */
/******************************************************************************/

/*----------------------------------------------------------------------------*/
/* this helps to avoid memory leaks by strdup                                 */
/* we maintain a string hash to keep all used user ids/group ids etc.         */

char* 
STRINGSTORE(const char* __charptr__) {
  XrdOucString* yourstring;
  if (!__charptr__ ) return (char*)"";

  if ((yourstring = XrdOfsFS.stringstore->Find(__charptr__))) {
    return (char*)yourstring->c_str();
  } else {
    XrdOucString* newstring = new XrdOucString(__charptr__);
    XrdOfsFS.StoreMutex.Lock();
    XrdOfsFS.stringstore->Add(__charptr__,newstring);
    XrdOfsFS.StoreMutex.UnLock();
    return (char*)newstring->c_str();
  } 
}


XrdMqOfsOutMutex::XrdMqOfsOutMutex() {
  XrdOfsFS.QueueOutMutex.Lock();
}

XrdMqOfsOutMutex::~XrdMqOfsOutMutex() 
{
  XrdOfsFS.QueueOutMutex.UnLock();
}
  
/******************************************************************************/
/*                           C o n s t r u c t o r                            */
/******************************************************************************/

XrdMqOfs::XrdMqOfs(XrdSysError *ep)
{
  eDest = ep;
  ConfigFN  = 0;  
  StartupTime = time(0);
  LastOutputTime = time(0);
  ReceivedMessages = 0;
  FanOutMessages = 0;
  DeliveredMessages = 0;
  AdvisoryMessages = 0;
  UndeliverableMessages = 0;
}

/******************************************************************************/
/*                           I n i t i a l i z a t i o n                      */
/******************************************************************************/
bool
XrdMqOfs::Init (XrdSysError &ep)
{
  stringstore = new XrdOucHash<XrdOucString> ();

  return true;
}


/******************************************************************************/
/*                         G e t F i l e S y s t e m                          */
/******************************************************************************/
  
XrdSfsFileSystem *XrdSfsGetFileSystem(XrdSfsFileSystem *native_fs, 
                                      XrdSysLogger     *lp,
				      const char       *configfn)
{
  // Do the herald thing
  //
  OfsEroute.SetPrefix("mqofs_");
  OfsEroute.logger(lp);
  OfsEroute.Say("++++++ (c) 2010 CERN/IT-DSS ",
		"v 1.0");

   XrdOfsFS.ConfigFN = (configfn && *configfn ? strdup(configfn) : 0);

   if ( XrdOfsFS.Configure(OfsEroute) ) return 0;
   
   // All done, we can return the callout vector to these routines.
   //
   return &XrdOfsFS;
}
  
/******************************************************************************/
/*                            g e t V e r s i o n                             */
/******************************************************************************/

const char *XrdMqOfs::getVersion() {return XrdVERSION;}

/******************************************************************************/
/*                                 S t a l l                                  */
/******************************************************************************/

int XrdMqOfs::Stall(XrdOucErrInfo   &error, // Error text & code
                  int              stime, // Seconds to stall
                  const char      *msg)   // Message to give
{
  XrdOucString smessage = msg;
  smessage += "; come back in ";
  smessage += stime;
  smessage += " seconds!";
  
  EPNAME("Stall");
  const char *tident = error.getErrUser();
  
  ZTRACE(delay, "Stall " <<stime <<": " << smessage.c_str());

  // Place the error message in the error object and return
  //
  error.setErrInfo(0, smessage.c_str());
  
  // All done
  //
  return stime;
}

int 
XrdMqOfs::stat(const char                *queuename,
	       struct stat               *buf,
	       XrdOucErrInfo             &error,
	       const XrdSecEntity        *client,
	       const char                *opaque) {

  EPNAME("stat");
  const char *tident = error.getErrUser();

  XrdMqMessageOut* Out = 0;

  Statistics();

  ZTRACE(open,"stat by buf: "<< queuename);

  {
    XrdMqOfsOutMutex qm;
    if (!(Out = XrdOfsFS.QueueOut.Find(queuename))) {
	return XrdMqOfs::Emsg(epname, error, EINVAL,"check queue - no such queue");
    }
    Out->DeletionSem.Wait();
  }

  {
    XrdOfsFS.AdvisoryMessages++;
    // submit an advisory message
    XrdAdvisoryMqMessage amg("AdvisoryQuery", queuename,true, XrdMqMessageHeader::kQueryMessage);
    XrdMqMessageHeader::GetTime(amg.kMessageHeader.kSenderTime_sec,amg.kMessageHeader.kSenderTime_nsec);
    XrdMqMessageHeader::GetTime(amg.kMessageHeader.kBrokerTime_sec,amg.kMessageHeader.kBrokerTime_nsec);
    amg.kMessageHeader.kSenderId = XrdOfsFS.BrokerId;
    amg.Encode();
    //    amg.Print();
    XrdSmartOucEnv* env = new XrdSmartOucEnv(amg.GetMessageBuffer());
    XrdMqOfsMatches matches(XrdOfsFS.QueueAdvisory.c_str(), env, tident, XrdMqMessageHeader::kQueryMessage, queuename);
    XrdMqOfsOutMutex qm;
    env->procmutex.Lock();
    XrdOfsFS.QueueOut.Apply(XrdOfsFS.AddToMatch,&matches);
    env->procmutex.UnLock();
  }


  // this should be the case always ...
  ZTRACE(open, "Waiting for message");
  //  Out->MessageSem.Wait(1);
  Out->Lock();
  ZTRACE(open, "Grabbing message");
  
  memset(buf,sizeof(struct stat),0);
  buf->st_blksize= 1024;
  buf->st_dev    = 0;
  buf->st_rdev   = 0;
  buf->st_nlink  = 1;
  buf->st_uid    = 0;
  buf->st_gid    = 0;
  buf->st_size   = Out->RetrieveMessages();
  buf->st_atime  = 0;
  buf->st_mtime  = 0;
  buf->st_ctime  = 0;
  buf->st_blocks = 1024;
  buf->st_ino    = 0;
  buf->st_mode   = S_IXUSR|S_IRUSR|S_IWUSR |S_IFREG;
  Out->UnLock();
  Out->DeletionSem.Post();
  if (buf->st_size == 0) {
    XrdOfsFS.NoMessages++;
  }
  return SFS_OK;
}


int 
XrdMqOfs::stat(const char                *Name,
	       mode_t                    &mode,
	       XrdOucErrInfo             &error,
	       const XrdSecEntity        *client,
	       const char                *opaque) {

  EPNAME("stat");
  const char *tident = error.getErrUser();

  ZTRACE(open,"stat by mode");
  return SFS_ERROR;
}




int
XrdMqOfsFile::open(const char                *queuename,
		   XrdSfsFileOpenMode   openMode,
		   mode_t               createMode,
		   const XrdSecEntity        *client,
		   const char                *opaque)
{
  EPNAME("open");

  ZTRACE(open,"Connecting Queue: " << queuename);
  
  XrdMqOfsOutMutex qm;
  QueueName = queuename;

  //  printf("%s %s %s\n",QueueName.c_str(),XrdOfsFS.QueuePrefix.c_str(),opaque);
  // check if this queue is accepted by the broker
  if (!QueueName.beginswith(XrdOfsFS.QueuePrefix)) {
    // this queue is not supported by us
    return XrdMqOfs::Emsg(epname, error, EINVAL,"connect queue - the broker does not serve the requested queue");
  }
  

  if (XrdOfsFS.QueueOut.Find(queuename)) {
    // this is already open by 'someone'
    return XrdMqOfs::Emsg(epname, error, EBUSY, "connect queue - already connected",queuename);
  }
    
  Out = new XrdMqMessageOut(queuename);

  // check if advisory messages are requested
  XrdOucEnv queueenv((opaque)?opaque:"");

  bool advisorystatus=false;
  bool advisoryquery=false;
  const char* val;
  if ( (val = queueenv.Get(XMQCADVISORYSTATUS))) {
    advisorystatus = atoi(val);
  } 
  if ( (val = queueenv.Get(XMQCADVISORYQUERY))) {
    advisoryquery = atoi(val);
  }

  Out->AdvisoryStatus = advisorystatus;
  Out->AdvisoryQuery  = advisoryquery;

  XrdOfsFS.QueueOut.Add(QueueName.c_str(), Out);

  ZTRACE(open,"Connected Queue: " << queuename);
  return SFS_OK;
}

int
XrdMqOfsFile::close() {
  EPNAME("close");

  ZTRACE(close,"Disconnecting Queue: " << QueueName.c_str());
	 

  {
    XrdMqOfsOutMutex qm; 
    if (Out) {
      Out->DeletionSem.Wait();
      Out->Lock();
      XrdOfsFS.QueueOut.Del(QueueName.c_str());
    }
    Out = 0;
  }

  {
    XrdOfsFS.AdvisoryMessages++;
    // submit an advisory message
    XrdAdvisoryMqMessage amg("AdvisoryStatus", QueueName.c_str(),false, XrdMqMessageHeader::kStatusMessage);
    XrdMqMessageHeader::GetTime(amg.kMessageHeader.kSenderTime_sec,amg.kMessageHeader.kSenderTime_nsec);
    XrdMqMessageHeader::GetTime(amg.kMessageHeader.kBrokerTime_sec,amg.kMessageHeader.kBrokerTime_nsec);
    amg.kMessageHeader.kSenderId = XrdOfsFS.BrokerId;
    amg.Encode();
    //    amg.Print();
    XrdSmartOucEnv* env =new XrdSmartOucEnv(amg.GetMessageBuffer());
    XrdMqOfsMatches matches(XrdOfsFS.QueueAdvisory.c_str(), env, tident, XrdMqMessageHeader::kStatusMessage, QueueName.c_str());
    XrdMqOfsOutMutex qm;
    env->procmutex.Lock();
    XrdOfsFS.QueueOut.Apply(XrdOfsFS.AddToMatch,&matches);
    env->procmutex.UnLock();
  }

  return SFS_OK;
}


XrdSfsXferSize 
XrdMqOfsFile::read(XrdSfsFileOffset  fileOffset, 
		    char            *buffer,
		    XrdSfsXferSize   buffer_size) {
  EPNAME("read");
  ZTRACE(open,"read");
  if (Out) {
    unsigned int mlen = Out->MessageBuffer.length();
    ZTRACE(open,"reading size:" << buffer_size);
    if ((unsigned long) buffer_size != mlen) {
      memcpy(buffer,Out->MessageBuffer.c_str(),buffer_size);
      Out->MessageBuffer.erase(0,buffer_size);
      return buffer_size;
    } else {
      memcpy(buffer,Out->MessageBuffer.c_str(),mlen);
      Out->MessageBuffer="";
      return mlen;
    }
  }
  error.setErrInfo(-1, "");
  return SFS_ERROR;
}



int
XrdMqOfsFile::stat(struct stat *buf) {
  EPNAME("stat");
  ZTRACE(open,"fstat");

  if (Out) {
    Out->DeletionSem.Wait();
    // this should be the case always ...
    ZTRACE(open, "Waiting for message");

    {
      XrdOfsFS.AdvisoryMessages++;
      // submit an advisory message
      XrdAdvisoryMqMessage amg("AdvisoryQuery", QueueName.c_str(),true, XrdMqMessageHeader::kQueryMessage);
      XrdMqMessageHeader::GetTime(amg.kMessageHeader.kSenderTime_sec,amg.kMessageHeader.kSenderTime_nsec);
      XrdMqMessageHeader::GetTime(amg.kMessageHeader.kBrokerTime_sec,amg.kMessageHeader.kBrokerTime_nsec);
      amg.kMessageHeader.kSenderId = XrdOfsFS.BrokerId;
      amg.Encode();
      //      amg.Print();
      XrdSmartOucEnv* env = new XrdSmartOucEnv(amg.GetMessageBuffer());
      XrdMqOfsMatches matches(XrdOfsFS.QueueAdvisory.c_str(), env, tident, XrdMqMessageHeader::kQueryMessage, QueueName.c_str());
      XrdMqOfsOutMutex qm;
      env->procmutex.Lock();
      XrdOfsFS.QueueOut.Apply(XrdOfsFS.AddToMatch,&matches);
      env->procmutex.UnLock();
    }


    //    Out->MessageSem.Wait(1);
    Out->Lock();
    ZTRACE(open, "Grabbing message");

    memset(buf,sizeof(struct stat),0);
    buf->st_blksize= 1024;
    buf->st_dev    = 0;
    buf->st_rdev   = 0;
    buf->st_nlink  = 1;
    buf->st_uid    = 0;
    buf->st_gid    = 0;
    buf->st_size   = Out->RetrieveMessages();
    buf->st_atime  = 0;
    buf->st_mtime  = 0;
    buf->st_ctime  = 0;
    buf->st_blocks = 1024;
    buf->st_ino    = 0;
    buf->st_mode   = S_IXUSR|S_IRUSR|S_IWUSR |S_IFREG;
    Out->UnLock();
    Out->DeletionSem.Post();

    if (buf->st_size == 0) {
      XrdOfsFS.NoMessages++;
    }
    return SFS_OK;
  }
  ZTRACE(open, "No message queue");
  return SFS_ERROR;
}

/******************************************************************************/
/*                         C o n f i g u r e                                  */
/******************************************************************************/
int XrdMqOfs::Configure(XrdSysError& Eroute)
{
  char *var;
  const char *val;
  int  cfgFD;

  StatisticsFile = "/var/log/xroot/mq/proc/stats";


  QueuePrefix = "/xmessage/";
  QueueAdvisory = "/xmessage/*";

  // extract the manager from the config file
  XrdOucStream Config(&Eroute, getenv("XRDINSTANCE"));

  {
    // borrowed from XrdOfs
    unsigned int myIPaddr = 0;

    char buff[256], *bp;
    int i;

    // Obtain port number we will be using
    //
    myPort = (bp = getenv("XRDPORT")) ? strtol(bp, (char **)0, 10) : 0;

    // Establish our hostname and IPV4 address
    //
    HostName      = XrdNetDNS::getHostName();

    if (!XrdNetDNS::Host2IP(HostName, &myIPaddr)) myIPaddr = 0x7f000001;
    strcpy(buff, "[::"); bp = buff+3;
    bp += XrdNetDNS::IP2String(myIPaddr, 0, bp, 128);
    *bp++ = ']'; *bp++ = ':';
    sprintf(bp, "%d", myPort);
    for (i = 0; HostName[i] && HostName[i] != '.'; i++);
    HostName[i] = '\0';
    HostPref = strdup(HostName);
    HostName[i] = '.';
    Eroute.Say("=====> mq.hostname: ", HostName,"");
    Eroute.Say("=====> mq.hostpref: ", HostPref,"");
    ManagerId=HostName;
    ManagerId+=":";
    ManagerId+=(int)myPort;
    Eroute.Say("=====> mq.managerid: ",ManagerId.c_str(),"");
  }



  if( !ConfigFN || !*ConfigFN) {
    // this error will be reported by XrdOfsFS.Configure
  } else {
    // Try to open the configuration file.
    //
    if ( (cfgFD = open(ConfigFN, O_RDONLY, 0)) < 0)
      return Eroute.Emsg("Config", errno, "open config file fn=", ConfigFN);
    
    Config.Attach(cfgFD);
    // Now start reading records until eof.
    //
    
    while((var = Config.GetMyFirstWord())) {
      if (!strncmp(var, "mq.",3)) {
	var += 3;

	if (!strcmp("queue",var)) {
	  if (( val = Config.GetWord())) {
	    QueuePrefix = val;
	    QueueAdvisory = QueuePrefix;
	    QueueAdvisory += "*";
	  }
	}

	if (!strcmp("statfile",var)) {
	  if (( val = Config.GetWord())) {
	    StatisticsFile = val;
	  }
	}
      }
    }
    
    Config.Close();
  }
 
  XrdOucString basestats = StatisticsFile;
  basestats.erase(basestats.rfind("/"));
  XrdOucString mkdirbasestats="mkdir -p "; mkdirbasestats += basestats; mkdirbasestats += " 2>/dev/null";
  system(mkdirbasestats.c_str());
  
  BrokerId = "root://";
  BrokerId += ManagerId;
  BrokerId += "/";
  BrokerId += QueuePrefix;

  Eroute.Say("=====> mq.queue: ", QueuePrefix.c_str());
  Eroute.Say("=====> mq.brokerid: ", BrokerId.c_str());
  int rc = XrdOfs::Configure(Eroute);
  return rc;
}

void
XrdMqOfs::Statistics() {
  EPNAME("Statistics");
  StatLock.Lock();
  static bool startup=true;
  static struct timeval tstart;
  static struct timeval tstop;
  static struct timezone tz;
  if (startup) {
    tstart.tv_sec=0;
    tstart.tv_usec=0;
    startup = false;
  }

  gettimeofday(&tstop,&tz);

  if (!tstart.tv_sec) {
    gettimeofday(&tstart,&tz);
    StatLock.UnLock();
    return;
  }

  const char* tident="";
  time_t now = time(0);
  float tdiff = ((tstop.tv_sec - tstart.tv_sec)*1000) + (tstop.tv_usec - tstart.tv_usec)/1000;
  if (tdiff > (60 * 1000) ) {
    // every minute
    XrdOucString tmpfile = StatisticsFile; tmpfile += ".tmp";
    int fd = open(tmpfile.c_str(),O_CREAT|O_RDWR|O_TRUNC, S_IROTH | S_IRGRP | S_IRUSR);
    if (fd >=0) {
      char line[4096];
      sprintf(line,"mq.received               %lld\n",ReceivedMessages); write(fd,line,strlen(line));
      sprintf(line,"mq.delivered              %lld\n",DeliveredMessages); write(fd,line,strlen(line));
      sprintf(line,"mq.fanout                 %lld\n",FanOutMessages); write(fd,line,strlen(line));
      sprintf(line,"mq.advisory               %lld\n",AdvisoryMessages); write(fd,line,strlen(line));
      sprintf(line,"mq.undeliverable          %lld\n",UndeliverableMessages); write(fd,line,strlen(line));
      sprintf(line,"mq.total                  %lld\n",NoMessages); write(fd,line,strlen(line));
      sprintf(line,"mq.queued                 %d\n",Messages.Num()); write(fd,line,strlen(line));
      sprintf(line,"mq.nqueues                %d\n",QueueOut.Num()); write(fd,line,strlen(line));
      sprintf(line,"mq.backloghits            %lld\n",QueueBacklogHits); write(fd,line,strlen(line));
      sprintf(line,"mq.in_rate                %f\n",(1000.0*ReceivedMessages/(tdiff))); write(fd,line,strlen(line));
      sprintf(line,"mq.out_rate               %f\n",(1000.0*DeliveredMessages/(tdiff))); write(fd,line,strlen(line));
      sprintf(line,"mq.fan_rate               %f\n",(1000.0*FanOutMessages/(tdiff))); write(fd,line,strlen(line));
      sprintf(line,"mq.advisory_rate          %f\n",(1000.0*AdvisoryMessages/(tdiff))); write(fd,line,strlen(line));
      sprintf(line,"mq.undeliverable_rate     %f\n",(1000.0*UndeliverableMessages/(tdiff))); write(fd,line,strlen(line));
      sprintf(line,"mq.total_rate             %f\n",(1000.0*NoMessages/(tdiff))); write(fd,line,strlen(line));
      close(fd);
      ::rename(tmpfile.c_str(),StatisticsFile.c_str());
    }
    gettimeofday(&tstart,&tz);
  }

  if ((now-LastOutputTime) > 2) {
    ZTRACE(getstats,"*****************************************************");
    ZTRACE(getstats,"Received  Messages            : " << ReceivedMessages);
    ZTRACE(getstats,"Delivered Messages            : " << DeliveredMessages);
    ZTRACE(getstats,"FanOut    Messages            : " << FanOutMessages);
    ZTRACE(getstats,"Advisory  Messages            : " << AdvisoryMessages);
    ZTRACE(getstats,"Undeliverable Messages        : " << UndeliverableMessages);
    ZTRACE(getstats,"No        Messages            : " << NoMessages);
    ZTRACE(getstats,"Queue     Messages            : " << Messages.Num());
    ZTRACE(getstats,"#Queues                       : " << QueueOut.Num());
    ZTRACE(getstats,"Deferred  Messages (backlog)  : " << BacklogDeferred);
    ZTRACE(getstats,"Backlog   Messages Hits       : " << QueueBacklogHits);
    char rates[4096];
    sprintf(rates, "Rates: IN: %d OUT: %d FAN: %d ADV: %d: UNDEV: %d NOMSG: %d" 
	    ,(int)(1.0*ReceivedMessages/(now-StartupTime))
	    ,(int)(1.0*DeliveredMessages/(now-StartupTime))
	    ,(int)(1.0*FanOutMessages/(now-StartupTime))
	    ,(int)(1.0*AdvisoryMessages/(now-StartupTime))
	    ,(int)(1.0*UndeliverableMessages/(now-StartupTime))
	    ,(int)(1.0*NoMessages/(now-StartupTime)));
    ZTRACE(getstats, rates);
    ZTRACE(getstats,"*****************************************************");
    LastOutputTime = now;
  }

  StatLock.UnLock();
}
