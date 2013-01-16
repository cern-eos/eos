// ----------------------------------------------------------------------
// File: XrdMqOfs.cc
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
#include "XrdSys/XrdSysDNS.hh"
#include "mq/XrdMqOfs.hh"
#include "mq/XrdMqMessage.hh"
#include "mq/XrdMqOfsTrace.hh"

#include <pwd.h>
#include <grp.h>
#include <signal.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>


/******************************************************************************/
/*                        G l o b a l   O b j e c t s                         */
/******************************************************************************/

XrdSysError     gMqOfsEroute(0);
XrdOucTrace     gMqOfsTrace(&gMqOfsEroute);

XrdOucHash<XrdOucString>*     XrdMqOfs::stringstore;

XrdMqOfs*   gMqFS=0;

void
xrdmqofs_shutdown(int sig) {
  
  exit(0);
}

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

  if ((yourstring = gMqFS->stringstore->Find(__charptr__))) {
    return (char*)yourstring->c_str();
  } else {
    XrdOucString* newstring = new XrdOucString(__charptr__);
    gMqFS->StoreMutex.Lock();
    gMqFS->stringstore->Add(__charptr__,newstring);
    gMqFS->StoreMutex.UnLock();
    return (char*)newstring->c_str();
  } 
}

XrdMqOfsOutMutex::XrdMqOfsOutMutex() {
  gMqFS->QueueOutMutex.Lock();
}

XrdMqOfsOutMutex::~XrdMqOfsOutMutex() 
{
  gMqFS->QueueOutMutex.UnLock();
}
  
/******************************************************************************/
/*                           C o n s t r u c t o r                            */
/******************************************************************************/

XrdMqOfs::XrdMqOfs(XrdSysError *ep)
{
  ConfigFN  = 0;  
  StartupTime = time(0);
  LastOutputTime = time(0);
  ReceivedMessages = 0;
  FanOutMessages = 0;
  DeliveredMessages = 0;
  AdvisoryMessages = 0;
  UndeliverableMessages = 0;
  DiscardedMonitoringMessages = 0;
  BacklogDeferred = NoMessages = QueueBacklogHits = 0;

  (void) signal(SIGINT,xrdmqofs_shutdown);
  HostName=0;
  HostPref=0;
  fprintf(stderr,"Addr::QueueOutMutex        0x%llx\n",(unsigned long long) &gMqFS->QueueOutMutex);
  fprintf(stderr,"Addr::MessageMutex         0x%llx\n",(unsigned long long) &gMqFS->MessagesMutex);
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
  
extern "C" 
XrdSfsFileSystem *XrdSfsGetFileSystem(XrdSfsFileSystem *native_fs, 
                                      XrdSysLogger     *lp,
                                      const char       *configfn)
{
  // Do the herald thing
  //
  gMqOfsEroute.SetPrefix("mqofs_");
  gMqOfsEroute.logger(lp);
  gMqOfsEroute.Say("++++++ (c) 2012 CERN/IT-DSS ",
                VERSION);

  static XrdMqOfs myFS(&gMqOfsEroute);

  gMqFS = &myFS;

  gMqFS->ConfigFN = (configfn && *configfn ? strdup(configfn) : 0);

  if ( gMqFS->Configure(gMqOfsEroute) ) return 0;
   
  // All done, we can return the callout vector to these routines.
  //
  return gMqFS;
}
  
/******************************************************************************/
/*                            g e t V e r s i o n                             */
/******************************************************************************/

const char *XrdMqOfs::getVersion() {return XrdVERSION;}


/******************************************************************************/
/*                                 S t a l l                                  */
/******************************************************************************/

int XrdMqOfs::Emsg(const char    *pfx,    // Message prefix value
		   XrdOucErrInfo &einfo,  // Place to put text & error code
		   int            ecode,  // The error code
		   const char    *op,     // Operation being performed
		   const char    *target) // The target (e.g., fname)
{
  char *etext, buffer[4096], unkbuff[64];
  
  // Get the reason for the error
  //
  if (ecode < 0) ecode = -ecode;
  if (!(etext = strerror(ecode)))
    {sprintf(unkbuff, "reason unknown (%d)", ecode); etext = unkbuff;}

  // Format the error message
  //
  
  
  snprintf(buffer,sizeof(buffer),"Unable to %s %s; %s", op, target, etext);

  gMqOfsEroute.Emsg(pfx, buffer);

  // Place the error message in the error object and return
  //
  einfo.setErrInfo(ecode, buffer);
  
  return SFS_ERROR;
}
  
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
  
  if (!strcmp(queuename,"/eos/")) {
    // this is just a ping test if we are alive
    memset(buf,0,sizeof(struct stat));
    buf->st_blksize= 1024;
    buf->st_dev    = 0;
    buf->st_rdev   = 0;
    buf->st_nlink  = 1;
    buf->st_uid    = 0;
    buf->st_gid    = 0;
    buf->st_size   = 0;
    buf->st_atime  = 0;
    buf->st_mtime  = 0;
    buf->st_ctime  = 0;
    buf->st_blocks = 1024;
    buf->st_ino    = 0;
    buf->st_mode   = S_IXUSR|S_IRUSR|S_IWUSR |S_IFREG;
    return SFS_OK;
  }
  
  MAYREDIRECT;

  XrdMqMessageOut* Out = 0;

  Statistics();

  ZTRACE(stat,"stat by buf: "<< queuename);
  std::string squeue = queuename;

  {
    XrdMqOfsOutMutex qm;
    if ((!gMqFS->QueueOut.count(squeue)) || (!(Out = gMqFS->QueueOut[squeue]))) {
      return gMqFS->Emsg(epname, error, EINVAL,"check queue - no such queue");
    }
    Out->DeletionSem.Wait();
  }

  {
    gMqFS->AdvisoryMessages++;
    // submit an advisory message
    XrdAdvisoryMqMessage amg("AdvisoryQuery", queuename,true, XrdMqMessageHeader::kQueryMessage);
    XrdMqMessageHeader::GetTime(amg.kMessageHeader.kSenderTime_sec,amg.kMessageHeader.kSenderTime_nsec);
    XrdMqMessageHeader::GetTime(amg.kMessageHeader.kBrokerTime_sec,amg.kMessageHeader.kBrokerTime_nsec);
    amg.kMessageHeader.kSenderId = gMqFS->BrokerId;
    amg.Encode();
    //    amg.Print();
    XrdSmartOucEnv* env = new XrdSmartOucEnv(amg.GetMessageBuffer());
    XrdMqOfsMatches matches(gMqFS->QueueAdvisory.c_str(), env, tident, XrdMqMessageHeader::kQueryMessage, queuename);
    XrdMqOfsOutMutex qm;
    if (!gMqFS->Deliver(matches))
      delete env;
  }


  // this should be the case always ...
  ZTRACE(stat, "Waiting for message");
  //  Out->MessageSem.Wait(1);
  Out->Lock();
  ZTRACE(stat, "Grabbing message");
  
  memset(buf,0,sizeof(struct stat));
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
    gMqFS->NoMessages++;
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

  ZTRACE(stat,"stat by mode");
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
  tident = error.getErrUser();

  MAYREDIRECT;

  ZTRACE(open,"Connecting Queue: " << queuename);
  
  XrdMqOfsOutMutex qm;
  QueueName = queuename;
  std::string squeue = queuename;

  //  printf("%s %s %s\n",QueueName.c_str(),gMqFS->QueuePrefix.c_str(),opaque);
  // check if this queue is accepted by the broker
  if (!QueueName.beginswith(gMqFS->QueuePrefix)) {
    // this queue is not supported by us
    return gMqFS->Emsg(epname, error, EINVAL,"connect queue - the broker does not serve the requested queue");
  }
  

  if (gMqFS->QueueOut.count(squeue)) {
    fprintf(stderr,"EBUSY: Queue %s is busy\n", QueueName.c_str());
    // this is already open by 'someone'
    return gMqFS->Emsg(epname, error, EBUSY, "connect queue - already connected",queuename);
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

  gMqFS->QueueOut.insert(std::pair<std::string, XrdMqMessageOut*>(squeue, Out));

  ZTRACE(open,"Connected Queue: " << queuename);
  IsOpen = true;

  return SFS_OK;
}

int
XrdMqOfsFile::close() {
  EPNAME("close");

  if (!IsOpen) 
    return SFS_OK;

  ZTRACE(close,"Disconnecting Queue: " << QueueName.c_str());
         
  std::string squeue = QueueName.c_str();

  {
    XrdMqOfsOutMutex qm; 
    if ((gMqFS->QueueOut.count(squeue)) && (Out = gMqFS->QueueOut[squeue])) {
      // hmm this could create a dead lock
      //      Out->DeletionSem.Wait();
      Out->Lock();
      // we have to take away all pending messages
      Out->RetrieveMessages();
      gMqFS->QueueOut.erase(squeue);
      delete Out;
    }
    Out = 0;
  }

  {
    gMqFS->AdvisoryMessages++;
    // submit an advisory message
    XrdAdvisoryMqMessage amg("AdvisoryStatus", QueueName.c_str(),false, XrdMqMessageHeader::kStatusMessage);
    XrdMqMessageHeader::GetTime(amg.kMessageHeader.kSenderTime_sec,amg.kMessageHeader.kSenderTime_nsec);
    XrdMqMessageHeader::GetTime(amg.kMessageHeader.kBrokerTime_sec,amg.kMessageHeader.kBrokerTime_nsec);
    amg.kMessageHeader.kSenderId = gMqFS->BrokerId;
    amg.Encode();
    //    amg.Print();
    XrdSmartOucEnv* env =new XrdSmartOucEnv(amg.GetMessageBuffer());
    XrdMqOfsMatches matches(gMqFS->QueueAdvisory.c_str(), env, tident, XrdMqMessageHeader::kStatusMessage, QueueName.c_str());
    XrdMqOfsOutMutex qm;
    if (!gMqFS->Deliver(matches))
      delete env;
  }

  ZTRACE(close,"Disconnected Queue: " << QueueName.c_str());
  return SFS_OK;
}


XrdSfsXferSize 
XrdMqOfsFile::read(XrdSfsFileOffset  fileOffset, 
                   char            *buffer,
                   XrdSfsXferSize   buffer_size) {
  EPNAME("read");
  ZTRACE(read,"read");
  if (Out) {
    unsigned int mlen = Out->MessageBuffer.length();
    ZTRACE(read,"reading size:" << buffer_size);
    if ((unsigned long) buffer_size < mlen) {
      memcpy(buffer,Out->MessageBuffer.c_str(),buffer_size);
      Out->MessageBuffer.erase(0,buffer_size);
      return buffer_size;
    } else {
      memcpy(buffer,Out->MessageBuffer.c_str(),mlen);
      Out->MessageBuffer.clear();
      Out->MessageBuffer.reserve(0);
      return mlen;
    }
  }
  error.setErrInfo(-1, "");
  return SFS_ERROR;
}



int
XrdMqOfsFile::stat(struct stat *buf) {
  EPNAME("stat");
  ZTRACE(read,"fstat");

  int port=0;                                              
  XrdOucString host="";                                    
  if (gMqFS->ShouldRedirect(host,port)) {
    // we have to close this object to make the client reopen it to be redirected
    this->close();
    return gMqFS->Emsg(epname, error, EINVAL,"stat - forced close - you should be redirected");
  }


  MAYREDIRECT;

  if (Out) {
    Out->DeletionSem.Wait();
    // this should be the case always ...
    ZTRACE(stat, "Waiting for message");

    {
      gMqFS->AdvisoryMessages++;
      // submit an advisory message
      XrdAdvisoryMqMessage amg("AdvisoryQuery", QueueName.c_str(),true, XrdMqMessageHeader::kQueryMessage);
      XrdMqMessageHeader::GetTime(amg.kMessageHeader.kSenderTime_sec,amg.kMessageHeader.kSenderTime_nsec);
      XrdMqMessageHeader::GetTime(amg.kMessageHeader.kBrokerTime_sec,amg.kMessageHeader.kBrokerTime_nsec);
      amg.kMessageHeader.kSenderId = gMqFS->BrokerId;
      amg.Encode();
      //      amg.Print();
      XrdSmartOucEnv* env = new XrdSmartOucEnv(amg.GetMessageBuffer());
      XrdMqOfsMatches matches(gMqFS->QueueAdvisory.c_str(), env, tident, XrdMqMessageHeader::kQueryMessage, QueueName.c_str());
      XrdMqOfsOutMutex qm;
      if (!gMqFS->Deliver(matches))
        delete env;
    }


    //    Out->MessageSem.Wait(1);
    Out->Lock();
    ZTRACE(stat, "Grabbing message");

    memset(buf,0,sizeof(struct stat));
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
      gMqFS->NoMessages++;
    }
    return SFS_OK;
  }
  ZTRACE(stat, "No message queue");
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

  StatisticsFile = "/var/log/eos/mq/proc/stats";


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
    HostName      = XrdSysDNS::getHostName();

    if (!XrdSysDNS::Host2IP(HostName, &myIPaddr)) myIPaddr = 0x7f000001;
    strcpy(buff, "[::"); bp = buff+3;
    bp += XrdSysDNS::IP2String(myIPaddr, 0, bp, 128);
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


  gMqOfsTrace.What= TRACE_getstats | TRACE_close| TRACE_open;

  if( !ConfigFN || !*ConfigFN) {
    // this error will be reported by gMqFS->Configure
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

	if (!strcmp("trace",var)) {
	  if (( val = Config.GetWord())) {
	    XrdOucString tracelevel = val;
	    if ( tracelevel == "low") {
	      gMqOfsTrace.What= TRACE_close| TRACE_open;
	    }
	    if ( tracelevel == "medium") {
	      gMqOfsTrace.What = TRACE_getstats | TRACE_open| TRACE_close;
	    }
	    if ( tracelevel == "high") {
	      gMqOfsTrace.What = TRACE_ALL;
	    }
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
  int rc = system(mkdirbasestats.c_str());
  if (rc) { fprintf(stderr,"error {%s/%s/%d}: system command failed;retc=%d", __FUNCTION__,__FILE__, __LINE__,WEXITSTATUS(rc)); }

  BrokerId = "root://";
  BrokerId += ManagerId;
  BrokerId += "/";
  BrokerId += QueuePrefix;

  Eroute.Say("=====> mq.queue: ", QueuePrefix.c_str());
  Eroute.Say("=====> mq.brokerid: ", BrokerId.c_str());
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
  static long long LastReceivedMessages, LastDeliveredMessages, LastFanOutMessages,LastAdvisoryMessages,LastUndeliverableMessages,LastNoMessages, LastDiscardedMonitoringMessages;
  if (startup) {
    tstart.tv_sec=0;
    tstart.tv_usec=0;
    LastReceivedMessages = LastDeliveredMessages = LastFanOutMessages = LastAdvisoryMessages = LastUndeliverableMessages = LastNoMessages = LastDiscardedMonitoringMessages = 0;
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
  if (tdiff > (10 * 1000) ) {
    // every minute
    XrdOucString tmpfile = StatisticsFile; tmpfile += ".tmp";
    int fd = open(tmpfile.c_str(),O_CREAT|O_RDWR|O_TRUNC, S_IROTH | S_IRGRP | S_IRUSR);
    if (fd >=0) {
      char line[4096];
      int rc;
      sprintf(line,"mq.received               %lld\n",ReceivedMessages); rc = write(fd,line,strlen(line));
      sprintf(line,"mq.delivered              %lld\n",DeliveredMessages); rc = write(fd,line,strlen(line));
      sprintf(line,"mq.fanout                 %lld\n",FanOutMessages); rc = write(fd,line,strlen(line));
      sprintf(line,"mq.advisory               %lld\n",AdvisoryMessages); rc = write(fd,line,strlen(line));
      sprintf(line,"mq.undeliverable          %lld\n",UndeliverableMessages); rc = write(fd,line,strlen(line));
      sprintf(line,"mq.droppedmonitoring      %lld\n",DiscardedMonitoringMessages); rc = write(fd,line,strlen(line));
      sprintf(line,"mq.total                  %lld\n",NoMessages); rc = write(fd,line,strlen(line));
      sprintf(line,"mq.queued                 %d\n",(int)Messages.size()); rc = write(fd,line,strlen(line));
      sprintf(line,"mq.nqueues                %d\n",(int)QueueOut.size()); rc = write(fd,line,strlen(line));
      sprintf(line,"mq.backloghits            %lld\n",QueueBacklogHits); rc = write(fd,line,strlen(line));
      sprintf(line,"mq.in_rate                %f\n",(1000.0*(ReceivedMessages-LastReceivedMessages)/(tdiff))); rc = write(fd,line,strlen(line));
      sprintf(line,"mq.out_rate               %f\n",(1000.0*(DeliveredMessages-LastDeliveredMessages)/(tdiff))); rc = write(fd,line,strlen(line));
      sprintf(line,"mq.fan_rate               %f\n",(1000.0*(FanOutMessages-LastFanOutMessages)/(tdiff))); rc = write(fd,line,strlen(line));
      sprintf(line,"mq.advisory_rate          %f\n",(1000.0*(AdvisoryMessages-LastAdvisoryMessages)/(tdiff))); rc = write(fd,line,strlen(line));
      sprintf(line,"mq.undeliverable_rate     %f\n",(1000.0*(UndeliverableMessages-LastUndeliverableMessages)/(tdiff))); rc = write(fd,line,strlen(line));
      sprintf(line,"mq.droppedmonitoring_rate %f\n",(1000.0*(DiscardedMonitoringMessages-LastDiscardedMonitoringMessages)/(tdiff))); rc = write(fd,line,strlen(line));
      sprintf(line,"mq.total_rate             %f\n",(1000.0*(NoMessages-LastNoMessages)/(tdiff))); rc = write(fd,line,strlen(line));
      close(fd);
      rc = ::rename(tmpfile.c_str(),StatisticsFile.c_str());
      if (rc) { fprintf(stderr,"error {%s/%s/%d}: system command failed;retc=%d", __FUNCTION__,__FILE__, __LINE__,WEXITSTATUS(rc)); }
    }
    gettimeofday(&tstart,&tz);

    ZTRACE(getstats,"*****************************************************");
    ZTRACE(getstats,"Received  Messages            : " << ReceivedMessages);
    ZTRACE(getstats,"Delivered Messages            : " << DeliveredMessages);
    ZTRACE(getstats,"FanOut    Messages            : " << FanOutMessages);
    ZTRACE(getstats,"Advisory  Messages            : " << AdvisoryMessages);
    ZTRACE(getstats,"Undeliverable Messages        : " << UndeliverableMessages);
    ZTRACE(getstats,"Discarded Monitoring Messages : " << DiscardedMonitoringMessages);
    ZTRACE(getstats,"No        Messages            : " << NoMessages);
    ZTRACE(getstats,"Queue     Messages            : " << Messages.size());
    ZTRACE(getstats,"#Queues                       : " << QueueOut.size());
    ZTRACE(getstats,"Deferred  Messages (backlog)  : " << BacklogDeferred);
    ZTRACE(getstats,"Backlog   Messages Hits       : " << QueueBacklogHits);
    char rates[4096];
    sprintf(rates, "Rates: IN: %.02f OUT: %.02f FAN: %.02f ADV: %.02f: UNDEV: %.02f DISCMON: %.02f NOMSG: %.02f" 
            ,(1000.0*(ReceivedMessages-LastReceivedMessages)/(tdiff))
            ,(1000.0*(DeliveredMessages-LastDeliveredMessages)/(tdiff))
            ,(1000.0*(FanOutMessages-LastFanOutMessages)/(tdiff))
            ,(1000.0*(AdvisoryMessages-LastAdvisoryMessages)/(tdiff))
            ,(1000.0*(UndeliverableMessages-LastUndeliverableMessages)/(tdiff))
            ,(1000.0*(DiscardedMonitoringMessages-LastDiscardedMonitoringMessages)/(tdiff))
            ,(1000.0*(NoMessages-LastNoMessages)/(tdiff)));
    ZTRACE(getstats, rates);
    ZTRACE(getstats,"*****************************************************");
    LastOutputTime = now;
    LastReceivedMessages = ReceivedMessages;
    LastDeliveredMessages = DeliveredMessages;
    LastFanOutMessages = FanOutMessages;
    LastAdvisoryMessages = AdvisoryMessages;
    LastUndeliverableMessages = UndeliverableMessages;
    LastNoMessages = NoMessages;
    LastDiscardedMonitoringMessages = DiscardedMonitoringMessages;

  }

  StatLock.UnLock();
}

bool XrdMqOfs::ShouldRedirect(XrdOucString &host,
		      int &port)
{
  EPNAME("ShouldRedirect");
  const char *tident = "internal";
  static time_t lastaliascheck=0;
  static bool isSlave=false;
  static XrdOucString remoteMq = "localhost";
  static XrdSysMutex sMutex;

  XrdSysMutexHelper sLock(sMutex);
  time_t now = time(NULL);

  if ( (now - lastaliascheck) > 10) {
    XrdOucString myName = HostName;
    XrdOucString master1Name;
    XrdOucString master2Name;

    bool m1ok;
    bool m2ok;
    m1ok = ResolveName(getenv("EOS_MGM_MASTER1"),master1Name);
    m2ok = ResolveName(getenv("EOS_MGM_MASTER2"),master2Name);
    remoteMq = "localhost";
    isSlave = false;
    if (myName == master1Name) {
      remoteMq = master2Name;
    }
    if (myName == master2Name) {
      remoteMq = master1Name;
    }

    {
      // check if we should be master or slave MQ
      XrdOucString mastertagfile    = "/var/eos/eos.mgm.rw";
      XrdOucString remotemqfile     = "/var/eos/eos.mq.remote.up";
      XrdOucString localmqfile      = "/var/eos/eos.mq.master";

      struct stat buf;
      if (::stat(localmqfile.c_str(), &buf)) {
	isSlave = true;
	if (::stat(remotemqfile.c_str(),&buf)) {
	  // oh no, the remote mq is down, keep the guys around here
	  isSlave = false;
	}
      } else {
	// we should be the master according to configuration
	isSlave = false;
      }
    }

    lastaliascheck = now;

    if (isSlave) {
      host = remoteMq;
      port = myPort;
      
      ZTRACE(redirect, "Redirect (resolv)" <<host.c_str() <<":" << port);
      return true;
    } else {
      host = "localhost";
      port = myPort;
      ZTRACE(redirect, "Stay (resolve)" <<host.c_str() <<":" << port);
      return false;
    }
  } else {
    if (isSlave) {
      host = remoteMq;
      port = myPort;
      ZTRACE(redirect, "Redirect (cached) " <<host.c_str() <<":" <<port);
      return true;
    } else {
      host = "localhost";
      port = myPort;
      ZTRACE(redirect, "Stay (cached) " << host.c_str() <<":" <<port);
    }
  }

  return false;
}


bool XrdMqOfs::ResolveName(const char* inhost, XrdOucString &outhost)
{
  struct hostent *hp;
  struct hostent *rhp;
  if (!inhost)
    return false;
  hp = gethostbyname(inhost);
  outhost = "localhost";
  if (hp) {
    if (hp->h_addrtype == AF_INET ) {
      if (hp->h_addr_list[0]) {
	outhost=inet_ntoa( *(struct in_addr *)hp->h_addr_list[0]);
	rhp = gethostbyaddr(hp->h_addr_list[0], sizeof(int),AF_INET);
	if (rhp) {
	  outhost = rhp->h_name;
	}
	return true;
      }
    }
  }
  return false;
}


int XrdMqOfs::Redirect(XrdOucErrInfo   &error, // Error text & code
		      XrdOucString &host,
		      int &port)
{
  EPNAME("Redirect");
  const char *tident = error.getErrUser();
  
  ZTRACE(delay, "Redirect " <<host.c_str() <<":" << port);
  
  // Place the error message in the error object and return
  //
  error.setErrInfo(port,host.c_str());
  
  // All done
  //
  return SFS_REDIRECT;
}
