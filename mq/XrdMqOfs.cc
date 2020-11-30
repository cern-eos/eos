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

#include "XrdVersion.hh"
#include "XrdOss/XrdOss.hh"
#include "XrdOuc/XrdOucTrace.hh"
#include "XrdSys/XrdSysError.hh"
#include "XrdSys/XrdSysLogger.hh"
#include "XrdSys/XrdSysTimer.hh"
#include "XrdSec/XrdSecInterface.hh"
#include "XrdSfs/XrdSfsAio.hh"
#include "XrdNet/XrdNetUtils.hh"
#include "XrdNet/XrdNetAddr.hh"
#include "mq/XrdMqOfs.hh"
#include "mq/XrdMqMessage.hh"
#include "mq/XrdMqOfsTrace.hh"
#include "common/PasswordHandler.hh"
#include "common/Strerror_r_wrapper.hh"
#include "namespace/ns_quarkdb/BackendClient.hh"
#include <pwd.h>
#include <grp.h>
#include <signal.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>

#define XRDMQOFS_FSCTLPATHLEN 1024

std::string XrdMqOfs::sLeaseKey {"master_lease"};
XrdSysError gMqOfsEroute(0);
XrdOucTrace gMqOfsTrace(&gMqOfsEroute);
XrdMqOfs* gMqFS = 0;

#ifdef COVERAGE_BUILD
// Forward declaration of gcov flush API
extern "C" void __gcov_flush();
#endif

//------------------------------------------------------------------------------
// Shutdown handler
//------------------------------------------------------------------------------
void
xrdmqofs_shutdown(int sig)
{
  exit(0);
}

//------------------------------------------------------------------------------
// Coverage report handler
//------------------------------------------------------------------------------
void
xrdmqofs_coverage(int sig)
{
#ifdef COVERAGE_BUILD
  eos_static_notice("printing coverage data");
  __gcov_flush();
  return;
#endif
  eos_static_notice("compiled without coverage support");
}

//------------------------------------------------------------------------------
// File open
//------------------------------------------------------------------------------
int
XrdMqOfsFile::open(const char* queuename, XrdSfsFileOpenMode openMode,
                   mode_t createMode, const XrdSecEntity* client,
                   const char* opaque)
{
  EPNAME("open");
  tident = error.getErrUser();
  SetLogId(nullptr, tident);
  eos_info_lite("connecting queue: %s", queuename);
  MAYREDIRECT;
  mQueueName = queuename;
  XrdSysMutexHelper scope_lock(gMqFS->mQueueOutMutex);

  //  printf("%s %s %s\n",mQueueName.c_str(),gMqFS->QueuePrefix.c_str(),opaque);
  // check if this queue is accepted by the broker
  if (mQueueName.find(gMqFS->QueuePrefix.c_str()) != 0) {
    // this queue is not supported by us
    return gMqFS->Emsg(epname, error, EINVAL,
                       "connect queue - the broker does not serve the requested queue");
  }

  if (gMqFS->mQueueOut.count(mQueueName)) {
    fprintf(stderr, "EBUSY: Queue %s is busy\n", mQueueName.c_str());
    // this is already open by 'someone'
    return gMqFS->Emsg(epname, error, EBUSY, "connect queue - already connected",
                       queuename);
  }

  mMsgOut = new XrdMqMessageOut(queuename);
  // check if advisory messages are requested
  XrdOucEnv queueenv((opaque) ? opaque : "");
  bool advisorystatus = false;
  bool advisoryquery = false;
  bool advisoryflushbacklog = false;
  const char* val;

  if ((val = queueenv.Get(XMQCADVISORYSTATUS))) {
    advisorystatus = atoi(val);
  }

  if ((val = queueenv.Get(XMQCADVISORYQUERY))) {
    advisoryquery = atoi(val);
  }

  if ((val = queueenv.Get(XMQCADVISORYFLUSHBACKLOG))) {
    advisoryflushbacklog = atoi(val);
  }

  mMsgOut->AdvisoryStatus = advisorystatus;
  mMsgOut->AdvisoryQuery  = advisoryquery;
  mMsgOut->AdvisoryFlushBackLog = advisoryflushbacklog;
  mMsgOut->BrokenByFlush = false;
  gMqFS->mQueueOut.insert(std::make_pair(mQueueName, mMsgOut));
  eos_info_lite("connected queue: %s", mQueueName.c_str());
  mIsOpen = true;
  return SFS_OK;
}

//------------------------------------------------------------------------------
// File stat
//------------------------------------------------------------------------------
int
XrdMqOfsFile::stat(struct stat* buf)
{
  EPNAME("stat");
  int port = 0;
  XrdOucString host = "";

  if (gMqFS->ShouldRedirect(host, port)) {
    // we have to close this object to make the client reopen it to be redirected
    // this->close();
    return gMqFS->Emsg(epname, error, EINVAL,
                       "stat - forced close - you should be redirected");
  }

  MAYREDIRECT;
  gMqFS->Statistics();

  if (mMsgOut) {
    mMsgOut->DeletionSem.Wait();
    // this should be the case always ...
    ZTRACE(stat, "Waiting for message");
    gMqFS->AdvisoryMessages++;
    // Submit an advisory message
    XrdAdvisoryMqMessage amg("AdvisoryQuery", mQueueName.c_str(), true,
                             XrdMqMessageHeader::kQueryMessage);
    XrdMqMessageHeader::GetTime(amg.kMessageHeader.kSenderTime_sec,
                                amg.kMessageHeader.kSenderTime_nsec);
    XrdMqMessageHeader::GetTime(amg.kMessageHeader.kBrokerTime_sec,
                                amg.kMessageHeader.kBrokerTime_nsec);
    amg.kMessageHeader.kSenderId = gMqFS->BrokerId;
    amg.Encode();
    // amg.Print();
    XrdSmartOucEnv* env = new XrdSmartOucEnv(amg.GetMessageBuffer());
    XrdMqOfsMatches matches(gMqFS->QueueAdvisory.c_str(), env, tident,
                            XrdMqMessageHeader::kQueryMessage, mQueueName.c_str());

    if (!gMqFS->Deliver(matches)) {
      delete env;
    }

    ZTRACE(stat, "Grabbing message");
    memset(buf, 0, sizeof(struct stat));
    buf->st_blksize = 1024;
    buf->st_dev    = 0;
    buf->st_rdev   = 0;
    buf->st_nlink  = 1;
    buf->st_uid    = 0;
    buf->st_gid    = 0;
    buf->st_size   = mMsgOut->RetrieveMessages();
    buf->st_atime  = 0;
    buf->st_mtime  = 0;
    buf->st_ctime  = 0;
    buf->st_blocks = 1024;
    buf->st_ino    = 0;
    buf->st_mode   = S_IXUSR | S_IRUSR | S_IWUSR | S_IFREG;
    mMsgOut->DeletionSem.Post();

    if (buf->st_size == 0) {
      gMqFS->NoMessages++;
    }

    return SFS_OK;
  }

  ZTRACE(stat, "No message queue");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// File read
//------------------------------------------------------------------------------
XrdSfsXferSize
XrdMqOfsFile::read(XrdSfsFileOffset fileOffset, char* buffer,
                   XrdSfsXferSize buffer_size)
{
  EPNAME("read");
  ZTRACE(read, "read");

  if (mMsgOut) {
    unsigned int mlen = mMsgOut->mMsgBuffer.length();
    ZTRACE(read, "reading size:" << buffer_size);

    if ((unsigned long) buffer_size < mlen) {
      memcpy(buffer, mMsgOut->mMsgBuffer.c_str(), buffer_size);
      mMsgOut->mMsgBuffer.erase(0, buffer_size);
      return buffer_size;
    } else {
      memcpy(buffer, mMsgOut->mMsgBuffer.c_str(), mlen);
      mMsgOut->mMsgBuffer.clear();
      mMsgOut->mMsgBuffer.reserve(0);
      return mlen;
    }
  }

  error.setErrInfo(-1, "");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// File close
//------------------------------------------------------------------------------
int
XrdMqOfsFile::close()
{
  if (!mIsOpen) {
    return SFS_OK;
  }

  mIsOpen = false;
  eos_info_lite("disconnecting queue: %s", mQueueName.c_str());
  {
    XrdSysMutexHelper scope_lock(gMqFS->mQueueOutMutex);

    if ((gMqFS->mQueueOut.count(mQueueName)) &&
        (mMsgOut = gMqFS->mQueueOut[mQueueName])) {
      // hmm this could create a dead lock
      //      mMsgOut->DeletionSem.Wait();
      // Take away all pending messages
      mMsgOut->RetrieveMessages();
      gMqFS->mQueueOut.erase(mQueueName);
      delete mMsgOut;
    }

    mMsgOut = nullptr;
  }
  {
    gMqFS->AdvisoryMessages++;
    // submit an advisory message
    XrdAdvisoryMqMessage amg("AdvisoryStatus", mQueueName.c_str(), false,
                             XrdMqMessageHeader::kStatusMessage);
    XrdMqMessageHeader::GetTime(amg.kMessageHeader.kSenderTime_sec,
                                amg.kMessageHeader.kSenderTime_nsec);
    XrdMqMessageHeader::GetTime(amg.kMessageHeader.kBrokerTime_sec,
                                amg.kMessageHeader.kBrokerTime_nsec);
    amg.kMessageHeader.kSenderId = gMqFS->BrokerId;
    amg.Encode();
    //    amg.Print();
    XrdSmartOucEnv* env = new XrdSmartOucEnv(amg.GetMessageBuffer());
    XrdMqOfsMatches matches(gMqFS->QueueAdvisory.c_str(), env, tident,
                            XrdMqMessageHeader::kStatusMessage, mQueueName.c_str());

    if (!gMqFS->Deliver(matches)) {
      delete env;
    }
  }
  eos_info_lite("disconnected queue: %s", mQueueName.c_str());
  return SFS_OK;
}

/******************************************************************************/
/*                         G e t F i l e S y s t e m                          */
/******************************************************************************/
// Set the version information
XrdVERSIONINFO(XrdSfsGetFileSystem, MqOfs);

extern "C"
XrdSfsFileSystem* XrdSfsGetFileSystem(XrdSfsFileSystem* native_fs,
                                      XrdSysLogger*     lp,
                                      const char*       configfn)
{
  // Do the herald thing
  gMqOfsEroute.SetPrefix("MqOfs_");
  gMqOfsEroute.logger(lp);
  gMqOfsEroute.Say("++++++ (c) 2018 CERN/IT-DSS ",
                   VERSION);
  static XrdMqOfs myFS(&gMqOfsEroute);
  lp->setRotate(0); // disable XRootD log rotation
  gMqFS = &myFS;
  gMqFS->ConfigFN = (configfn && *configfn ? strdup(configfn) : 0);

  if (gMqFS->Configure(gMqOfsEroute)) {
    return 0;
  }

  // All done, we can return the callout vector to these routines.
  return gMqFS;
}


//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
XrdMqOfs::XrdMqOfs(XrdSysError* ep):
  myPort(1097), mDeliveredMessages(0ull), mFanOutMessages(0ull),
  mMaxQueueBacklog(MQOFSMAXQUEUEBACKLOG),
  mRejectQueueBacklog(MQOFSREJECTQUEUEBACKLOG), mQdbCluster(), mQdbPassword(),
  mQdbContactDetails(), mQcl(nullptr), mMgmId()
{
  ConfigFN  = 0;
  StartupTime = time(0);
  LastOutputTime = time(0);
  ReceivedMessages = 0;
  AdvisoryMessages = 0;
  UndeliverableMessages = 0;
  DiscardedMonitoringMessages = 0;
  BacklogDeferred = NoMessages = QueueBacklogHits = 0;
  MaxMessageBacklog  = MQOFSMAXMESSAGEBACKLOG;
  (void) signal(SIGINT, xrdmqofs_shutdown);

  if (getenv("EOS_COVERAGE_REPORT")) {
    (void) signal(SIGPROF, xrdmqofs_coverage);
  }

  HostName = 0;
  HostPref = 0;
  eos_info_lite("Addr:mQueueOutMutex: 0x%llx", &mQueueOutMutex);
  eos_info_lite("Addr:MessageMutex:   0x%llx", &mMsgsMutex);
}

//------------------------------------------------------------------------------
// OFS plugin configure
//------------------------------------------------------------------------------
int XrdMqOfs::Configure(XrdSysError& Eroute)
{
  int rc = 0;
  char* var;
  const char* val;
  int  cfgFD;
  StatisticsFile = "/var/log/eos/mq/proc/stats";
  QueuePrefix = "/xmessage/";
  QueueAdvisory = "/xmessage/*";
  // extract the manager from the config file
  XrdOucStream Config(&Eroute, getenv("XRDINSTANCE"));
  {
    // borrowed from XrdOfs
    char buff[256], *bp;
    int i;
    // Obtain port number we will be using
    myPort = (bp = getenv("XRDPORT")) ? strtol(bp, (char**)0, 10) : 0;
    // Establish our hostname and IPV4 address
    const char* errtext = 0;
    HostName = XrdNetUtils::MyHostName(0, &errtext);

    if (!HostName) {
      return Eroute.Emsg("Config", errno, "cannot get hostname : %s", errtext);
    }

    XrdNetAddr* addrs  = 0;
    int         nAddrs = 0;
    const char* err    = XrdNetUtils::GetAddrs(HostName, &addrs, nAddrs,
                         XrdNetUtils::allIPv64,
                         XrdNetUtils::NoPortRaw);

    if (err || nAddrs == 0) {
      sprintf(buff, "[::127.0.0.1]:%d", myPort);
    } else {
      int len = XrdNetUtils::IPFormat(addrs[0].SockAddr(), buff, sizeof(buff),
                                      XrdNetUtils::noPort | XrdNetUtils::oldFmt);
      delete [] addrs;

      if (len == 0) {
        sprintf(buff, "[::127.0.0.1]:%d", myPort);
      } else {
        sprintf(buff + len, ":%d", myPort);
      }
    }

    for (i = 0; HostName[i] && HostName[i] != '.'; i++);

    HostName[i] = '\0';
    HostPref = strdup(HostName);
    HostName[i] = '.';
    Eroute.Say("=====> mq.hostname: ", HostName, "");
    Eroute.Say("=====> mq.hostpref: ", HostPref, "");
    ManagerId = HostName;
    ManagerId += ":";
    ManagerId += (int)myPort;
    Eroute.Say("=====> mq.managerid: ", ManagerId.c_str(), "");
    // @todo(esindril): maybe the MGM id should be taken from an env variable
    mMgmId = SSTR(HostName << ":1094").c_str();
  }
  gMqOfsTrace.What = TRACE_getstats | TRACE_close | TRACE_open;

  if (!ConfigFN || !*ConfigFN) {
    // this error will be reported by gMqFS->Configure
  } else {
    // Try to open the configuration file.
    if ((cfgFD = open(ConfigFN, O_RDONLY, 0)) < 0) {
      return Eroute.Emsg("Config", errno, "open config file fn=", ConfigFN);
    }

    Config.Attach(cfgFD);
    // Now start reading records until eof.

    while ((var = Config.GetMyFirstWord())) {
      if (!strncmp(var, "mq.", 3)) {
        var += 3;

        if (!strcmp("queue", var)) {
          if ((val = Config.GetWord())) {
            QueuePrefix = val;
            QueueAdvisory = QueuePrefix;
            QueueAdvisory += "*";
          }
        }

        if (!strcmp("maxmessagebacklog", var)) {
          if ((val = Config.GetWord())) {
            (void) sscanf(val, "%lld", &MaxMessageBacklog);
          }
        }

        if (!strcmp("maxqueuebacklog", var)) {
          if ((val = Config.GetWord())) {
            uint64_t tmp_val {0};
            (void) sscanf(val, "%lu", &tmp_val);
            mMaxQueueBacklog = tmp_val;
          }
        }

        if (!strcmp("rejectqueuebacklog", var)) {
          if ((val = Config.GetWord())) {
            uint64_t tmp_val {0};
            (void) sscanf(val, "%lu", &tmp_val);
            mRejectQueueBacklog = tmp_val;
          }
        }

        if (!strcmp("trace", var)) {
          if ((val = Config.GetWord())) {
            auto& g_logging = eos::common::Logging::GetInstance();
            g_logging.SetLogPriority(LOG_INFO);
            g_logging.SetUnit(SSTR("mq@" << ManagerId).c_str());
            XrdOucString tracelevel = val;

            if (tracelevel == "low") {
              gMqOfsTrace.What = TRACE_close | TRACE_open;
              g_logging.SetLogPriority(LOG_INFO);
            }

            if (tracelevel == "medium") {
              gMqOfsTrace.What = TRACE_getstats | TRACE_open | TRACE_close;
              g_logging.SetLogPriority(LOG_NOTICE);
            }

            if (tracelevel == "high") {
              gMqOfsTrace.What = TRACE_ALL;
              g_logging.SetLogPriority(LOG_DEBUG);
            }
          }
        }

        if (!strcmp("statfile", var)) {
          if ((val = Config.GetWord())) {
            StatisticsFile = val;
          }
        }

        if (!strcmp("qdbcluster", var)) {
          while ((val = Config.GetWord())) {
            mQdbCluster += val;
            mQdbCluster += " ";
          }

          Eroute.Say("=====> mq.qdbcluster : ", mQdbCluster.c_str());
          mQdbContactDetails.members.parse(mQdbCluster);
        }

        if (!strcmp("qdbpassword", var)) {
          while ((val = Config.GetWord())) {
            mQdbPassword += val;
          }

          // Trim whitespace at the end
          mQdbPassword.erase(mQdbPassword.find_last_not_of(" \t\n\r\f\v") + 1);
          std::string pwlen = std::to_string(mQdbPassword.size());
          Eroute.Say("=====> mq.qdbpassword length : ", pwlen.c_str());
          mQdbContactDetails.password = mQdbPassword;
        }

        if (!strcmp("qdbpassword_file", var)) {
          std::string path;

          while ((val = Config.GetWord())) {
            path += val;
          }

          if (!eos::common::PasswordHandler::readPasswordFile(path, mQdbPassword)) {
            Eroute.Emsg("Config", "failed to open path pointed by qdbpassword_file");
            rc = 1;
          }

          std::string pwlen = std::to_string(mQdbPassword.size());
          Eroute.Say("=====> mq.qdbpassword length : ", pwlen.c_str());
          mQdbContactDetails.password = mQdbPassword;
        }
      }
    }

    Config.Close();
  }

  if (rc) {
    eos_err("msg=\"failed while parsing the configuration file\"");
    return rc;
  }

  if (!mQdbContactDetails.members.empty() &&
      mQdbContactDetails.password.empty()) {
    Eroute.Say("=====> Configuration error: Found QDB cluster members, but no password."
               " EOS will only connect to password-protected QDB instances. (mqofs.qdbpassword / mqofs.qdbpassword_file missing)");
    return 1;
  }

  // Create a qclient object if cluster information provided
  if (!mQdbCluster.empty()) {
    mQcl = std::make_unique<qclient::QClient>(mQdbContactDetails.members,
           mQdbContactDetails.constructOptions());
  }

  XrdOucString basestats = StatisticsFile;
  basestats.erase(basestats.rfind("/"));
  XrdOucString mkdirbasestats = "mkdir -p ";
  mkdirbasestats += basestats;
  mkdirbasestats += " 2>/dev/null";
  rc = system(mkdirbasestats.c_str());

  if (rc) {
    fprintf(stderr, "error {%s/%s/%d}: system command failed;retc=%d", __FUNCTION__,
            __FILE__, __LINE__, WEXITSTATUS(rc));
  }

  BrokerId = "root://";
  BrokerId += ManagerId;
  BrokerId += "/";
  BrokerId += QueuePrefix;
  Eroute.Say("=====> mq.queue: ", QueuePrefix.c_str());
  Eroute.Say("=====> mq.brokerid: ", BrokerId.c_str());
  return rc;
}

//------------------------------------------------------------------------------
// File system stat
//------------------------------------------------------------------------------
int
XrdMqOfs::stat(const char* queuename, struct stat* buf, XrdOucErrInfo& error,
               const XrdSecEntity* client, const char* opaque)
{
  EPNAME("stat");
  const char* tident = error.getErrUser();

  if (!strcmp(queuename, "/eos/")) {
    // this is just a ping test if we are alive
    memset(buf, 0, sizeof(struct stat));
    buf->st_blksize = 1024;
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
    buf->st_mode   = S_IXUSR | S_IRUSR | S_IWUSR | S_IFREG;
    return SFS_OK;
  }

  MAYREDIRECT;
  XrdMqMessageOut* msg_out = nullptr;
  Statistics();
  ZTRACE(stat, "stat by buf: " << queuename);
  std::string squeue = queuename;
  {
    XrdSysMutexHelper scope_lock(mQueueOutMutex);

    if ((!gMqFS->mQueueOut.count(squeue)) ||
        (!(msg_out = gMqFS->mQueueOut[squeue]))) {
      return gMqFS->Emsg(epname, error, EINVAL, "check queue - no such queue");
    }

    msg_out->DeletionSem.Wait();
  }
  {
    gMqFS->AdvisoryMessages++;
    // submit an advisory message
    XrdAdvisoryMqMessage amg("AdvisoryQuery", queuename, true,
                             XrdMqMessageHeader::kQueryMessage);
    XrdMqMessageHeader::GetTime(amg.kMessageHeader.kSenderTime_sec,
                                amg.kMessageHeader.kSenderTime_nsec);
    XrdMqMessageHeader::GetTime(amg.kMessageHeader.kBrokerTime_sec,
                                amg.kMessageHeader.kBrokerTime_nsec);
    amg.kMessageHeader.kSenderId = gMqFS->BrokerId;
    amg.Encode();
    //    amg.Print();
    XrdSmartOucEnv* env = new XrdSmartOucEnv(amg.GetMessageBuffer());
    XrdMqOfsMatches matches(gMqFS->QueueAdvisory.c_str(), env, tident,
                            XrdMqMessageHeader::kQueryMessage, queuename);

    if (!gMqFS->Deliver(matches)) {
      delete env;
    }
  }
  // this should be the case always ...
  ZTRACE(stat, "Waiting for message");
  ZTRACE(stat, "Grabbing message");
  memset(buf, 0, sizeof(struct stat));
  buf->st_blksize = 1024;
  buf->st_dev    = 0;
  buf->st_rdev   = 0;
  buf->st_nlink  = 1;
  buf->st_uid    = 0;
  buf->st_gid    = 0;
  buf->st_size   = msg_out->RetrieveMessages();
  buf->st_atime  = 0;
  buf->st_mtime  = 0;
  buf->st_ctime  = 0;
  buf->st_blocks = 1024;
  buf->st_ino    = 0;
  buf->st_mode   = S_IXUSR | S_IRUSR | S_IWUSR | S_IFREG;
  msg_out->DeletionSem.Post();

  if (buf->st_size == 0) {
    gMqFS->NoMessages++;
  }

  return SFS_OK;
}

//------------------------------------------------------------------------------
// Stat by mode
//------------------------------------------------------------------------------
int
XrdMqOfs::stat(const char*                Name,
               mode_t&                    mode,
               XrdOucErrInfo&             error,
               const XrdSecEntity*        client,
               const char*                opaque)
{
  EPNAME("stat");
  const char* tident = error.getErrUser();
  ZTRACE(stat, "stat by mode");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// Statistics
//------------------------------------------------------------------------------
void
XrdMqOfs::Statistics()
{
  EPNAME("Statistics");
  StatLock.Lock();
  static bool startup = true;
  static struct timeval tstart;
  static struct timeval tstop;
  static struct timezone tz;
  static uint64_t LastDeliveredMessages;
  static uint64_t LastFanOutMessages;
  static long long LastReceivedMessages,
         LastAdvisoryMessages, LastUndeliverableMessages,
         LastNoMessages, LastDiscardedMonitoringMessages;

  if (startup) {
    tstart.tv_sec = 0;
    tstart.tv_usec = 0;
    LastDeliveredMessages = LastFanOutMessages = 0ull;
    LastReceivedMessages =  LastAdvisoryMessages =
                              LastUndeliverableMessages = LastNoMessages =
                                    LastDiscardedMonitoringMessages = 0;
    startup = false;
  }

  gettimeofday(&tstop, &tz);

  if (!tstart.tv_sec) {
    gettimeofday(&tstart, &tz);
    StatLock.UnLock();
    return;
  }

  const char* tident = "";
  time_t now = time(0);
  float tdiff = ((tstop.tv_sec - tstart.tv_sec) * 1000) +
                (tstop.tv_usec - tstart.tv_usec) / 1000;

  if (tdiff > (10 * 1000)) {
    // every minute
    XrdOucString tmpfile = StatisticsFile;
    tmpfile += ".tmp";
    int fd = open(tmpfile.c_str(), O_CREAT | O_RDWR | O_TRUNC,
                  S_IROTH | S_IRGRP | S_IRUSR);

    if (fd >= 0) {
      char line[4096];
      int rc;
      sprintf(line, "mq.received               %lld\n", ReceivedMessages);
      rc = write(fd, line, strlen(line));
      sprintf(line, "mq.delivered              %lu\n", mDeliveredMessages.load());
      rc = write(fd, line, strlen(line));
      sprintf(line, "mq.fanout                 %lu\n", mFanOutMessages.load());
      rc = write(fd, line, strlen(line));
      sprintf(line, "mq.advisory               %lld\n", AdvisoryMessages);
      rc = write(fd, line, strlen(line));
      sprintf(line, "mq.undeliverable          %lld\n", UndeliverableMessages);
      rc = write(fd, line, strlen(line));
      sprintf(line, "mq.droppedmonitoring      %lld\n", DiscardedMonitoringMessages);
      rc = write(fd, line, strlen(line));
      sprintf(line, "mq.total                  %lld\n", NoMessages);
      rc = write(fd, line, strlen(line));
      sprintf(line, "mq.queued                 %d\n", (int)Messages.size());
      rc = write(fd, line, strlen(line));
      sprintf(line, "mq.nqueues                %d\n", (int)mQueueOut.size());
      rc = write(fd, line, strlen(line));
      sprintf(line, "mq.backloghits            %lld\n", QueueBacklogHits);
      rc = write(fd, line, strlen(line));
      sprintf(line, "mq.in_rate                %f\n",
              (1000.0 * (ReceivedMessages - LastReceivedMessages) / (tdiff)));
      rc = write(fd, line, strlen(line));
      sprintf(line, "mq.out_rate               %f\n",
              (1000.0 * (mDeliveredMessages - LastDeliveredMessages) / (tdiff)));
      rc = write(fd, line, strlen(line));
      sprintf(line, "mq.fan_rate               %f\n",
              (1000.0 * (mFanOutMessages - LastFanOutMessages) / (tdiff)));
      rc = write(fd, line, strlen(line));
      sprintf(line, "mq.advisory_rate          %f\n",
              (1000.0 * (AdvisoryMessages - LastAdvisoryMessages) / (tdiff)));
      rc = write(fd, line, strlen(line));
      sprintf(line, "mq.undeliverable_rate     %f\n",
              (1000.0 * (UndeliverableMessages - LastUndeliverableMessages) / (tdiff)));
      rc = write(fd, line, strlen(line));
      sprintf(line, "mq.droppedmonitoring_rate %f\n",
              (1000.0 * (DiscardedMonitoringMessages - LastDiscardedMonitoringMessages) /
               (tdiff)));
      rc = write(fd, line, strlen(line));
      sprintf(line, "mq.total_rate             %f\n",
              (1000.0 * (NoMessages - LastNoMessages) / (tdiff)));
      rc = write(fd, line, strlen(line));
      close(fd);
      rc = ::rename(tmpfile.c_str(), StatisticsFile.c_str());

      if (rc) {
        fprintf(stderr, "error {%s/%s/%d}: system command failed;retc=%d", __FUNCTION__,
                __FILE__, __LINE__, WEXITSTATUS(rc));
      }
    }

    gettimeofday(&tstart, &tz);
    ZTRACE(getstats, "*****************************************************");
    ZTRACE(getstats, "Received  Messages            : " << ReceivedMessages);
    ZTRACE(getstats, "Delivered Messages            : " << mDeliveredMessages);
    ZTRACE(getstats, "FanOut    Messages            : " << mFanOutMessages);
    ZTRACE(getstats, "Advisory  Messages            : " << AdvisoryMessages);
    ZTRACE(getstats, "Undeliverable Messages        : " << UndeliverableMessages);
    ZTRACE(getstats, "Discarded Monitoring Messages : " <<
           DiscardedMonitoringMessages);
    ZTRACE(getstats, "No        Messages            : " << NoMessages);
    ZTRACE(getstats, "Queue     Messages            : " << Messages.size());
    ZTRACE(getstats, "#Queues                       : " << mQueueOut.size());
    ZTRACE(getstats, "Deferred  Messages (backlog)  : " << BacklogDeferred);
    ZTRACE(getstats, "Backlog   Messages Hits       : " << QueueBacklogHits);
    char rates[4096];
    sprintf(rates,
            "Rates: IN: %.02f OUT: %.02f FAN: %.02f ADV: %.02f: UNDEV: %.02f DISCMON: %.02f NOMSG: %.02f"
            , (1000.0 * (ReceivedMessages - LastReceivedMessages) / (tdiff))
            , (1000.0 * (mDeliveredMessages - LastDeliveredMessages) / (tdiff))
            , (1000.0 * (mFanOutMessages - LastFanOutMessages) / (tdiff))
            , (1000.0 * (AdvisoryMessages - LastAdvisoryMessages) / (tdiff))
            , (1000.0 * (UndeliverableMessages - LastUndeliverableMessages) / (tdiff))
            , (1000.0 * (DiscardedMonitoringMessages - LastDiscardedMonitoringMessages) /
               (tdiff))
            , (1000.0 * (NoMessages - LastNoMessages) / (tdiff)));
    ZTRACE(getstats, rates);
    ZTRACE(getstats, "*****************************************************");
    LastOutputTime = now;
    LastReceivedMessages = ReceivedMessages;
    LastDeliveredMessages = mDeliveredMessages;
    LastFanOutMessages = mFanOutMessages;
    LastAdvisoryMessages = AdvisoryMessages;
    LastUndeliverableMessages = UndeliverableMessages;
    LastNoMessages = NoMessages;
    LastDiscardedMonitoringMessages = DiscardedMonitoringMessages;
  }

  StatLock.UnLock();
}

//------------------------------------------------------------------------------
// Get the identity of the current lease holder
//------------------------------------------------------------------------------
std::string
XrdMqOfs::GetLeaseHolder()
{
  std::string holder;
  std::future<qclient::redisReplyPtr> f = mQcl->exec("lease-get", sLeaseKey);
  qclient::redisReplyPtr reply = f.get();

  if ((reply == nullptr) || (reply->type == REDIS_REPLY_NIL)) {
    eos_debug("%s", "msg=\"lease-get is NULL\"");
    return holder;
  }

  std::string reply_msg = std::string(reply->element[0]->str,
                                      reply->element[0]->len);
  eos_debug("lease-get reply: %s", reply_msg.c_str());
  std::string tag {"HOLDER: "};
  size_t pos = reply_msg.find(tag);

  if (pos == std::string::npos) {
    return holder;
  }

  pos += tag.length();
  size_t pos_end = reply_msg.find('\n', pos);

  if (pos_end == std::string::npos) {
    holder = reply_msg.substr(pos);
  } else {
    holder = reply_msg.substr(pos, pos_end - pos + 1);
  }

  return holder;
}

//------------------------------------------------------------------------------
// Decide if client should be redirected to a different host based on the
// current master-slave status.
//------------------------------------------------------------------------------
bool XrdMqOfs::ShouldRedirect(XrdOucString& host, int& port)
{
  if (mQcl) {
    return ShouldRedirectQdb(host, port);
  } else {
    return ShouldRedirectInMem(host, port);
  }
}

//------------------------------------------------------------------------------
// Decide if client should be redirected to a different host based on the
// current master-slave status. Used for QuarkDB namespace.
//------------------------------------------------------------------------------
bool XrdMqOfs::ShouldRedirectQdb(XrdOucString& host, int& port)
{
  using namespace std::chrono;
  static time_t last_check = 0;
  time_t now = time(nullptr);
  std::string master_id;

  // The master lease is taken for 10 seconds so we can check every 5 seconds
  if (now - last_check > 5) {
    last_check = now;
    master_id = GetLeaseHolder();
  }

  // If we are the current master or there is no master then don't redirect
  if ((master_id == mMgmId) || master_id.empty()) {
    return false;
  } else {
    size_t pos = master_id.find(':');

    try {
      host = master_id.substr(0, pos).c_str();
      port = myPort; // 1097
    } catch (const std::exception& e) {
      eos_notice("msg=\"unset or unexpected master identity format\" "
                 "master_id=\"%s\"", master_id.c_str());
      return false;
    }

    if (now - last_check > 10) {
      eos_info_lite("msg=\"redirect to new master mq\" id=%s:%i", host.c_str(),
                    port);
    }

    return true;
  }
}

//------------------------------------------------------------------------------
// Decide if client should be redirected to a different host based on the
// current master-slave status. Used for in-memory namespace.
//------------------------------------------------------------------------------
bool XrdMqOfs::ShouldRedirectInMem(XrdOucString& host, int& port)
{
  EPNAME("ShouldRedirect");
  const char* tident = "internal";
  static time_t lastaliascheck = 0;
  static bool isSlave = false;
  static XrdOucString remoteMq = "localhost";
  static XrdSysMutex sMutex;
  XrdSysMutexHelper sLock(sMutex);
  time_t now = time(NULL);

  if ((now - lastaliascheck) > 10) {
    XrdOucString myName = HostName;
    XrdOucString master1Name;
    XrdOucString master2Name;
    bool m1ok;
    bool m2ok;
    m1ok = ResolveName(getenv("EOS_MGM_MASTER1"), master1Name);
    m2ok = ResolveName(getenv("EOS_MGM_MASTER2"), master2Name);

    if (!m1ok) {
      fprintf(stderr, "error: unable to resolve %s\n", getenv("EOS_MGM_MASTER1"));
    }

    if (!m2ok) {
      fprintf(stderr, "error: unable to resolve %s\n", getenv("EOS_MGM_MASTER2"));
    }

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

        if (::stat(remotemqfile.c_str(), &buf)) {
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
      ZTRACE(redirect, "Redirect (resolv)" << host.c_str() << ":" << port);
      return true;
    } else {
      host = "localhost";
      port = myPort;
      ZTRACE(redirect, "Stay (resolve)" << host.c_str() << ":" << port);
      return false;
    }
  } else {
    if (isSlave) {
      host = remoteMq;
      port = myPort;
      ZTRACE(redirect, "Redirect (cached) " << host.c_str() << ":" << port);
      return true;
    } else {
      host = "localhost";
      port = myPort;
      ZTRACE(redirect, "Stay (cached) " << host.c_str() << ":" << port);
    }
  }

  return false;
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
bool XrdMqOfs::ResolveName(const char* inhost, XrdOucString& outhost)
{
  struct hostent* hp;
  struct hostent* rhp;

  if (!inhost) {
    return false;
  }

  hp = gethostbyname(inhost);
  outhost = "localhost";

  if (hp) {
    if (hp->h_addrtype == AF_INET) {
      if (hp->h_addr_list[0]) {
        outhost = inet_ntoa(*(struct in_addr*)hp->h_addr_list[0]);
        rhp = gethostbyaddr(hp->h_addr_list[0], sizeof(int), AF_INET);

        if (rhp) {
          outhost = rhp->h_name;
        }

        return true;
      }
    }
  }

  return false;
}

//------------------------------------------------------------------------------
// Error message formatting
//------------------------------------------------------------------------------
int XrdMqOfs::Emsg(const char*    pfx,    // Message prefix value
                   XrdOucErrInfo& einfo,  // Place to put text & error code
                   int            ecode,  // The error code
                   const char*    op,     // Operation being performed
                   const char*    target) // The target (e.g., fname)
{
  char etext[128], buffer[4096];

  // Get the reason for the error
  if (ecode < 0) {
    ecode = -ecode;
  }

  if (eos::common::strerror_r(ecode, etext, sizeof(etext))) {
    snprintf(etext, sizeof(etext), "reason unknown (%d)", ecode);
  }

  // Format the error message
  snprintf(buffer, sizeof(buffer), "Unable to %s %s; %s", op, target, etext);
  gMqOfsEroute.Emsg(pfx, buffer);
  // Place the error message in the error object and return
  einfo.setErrInfo(ecode, buffer);
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// Stall method
//------------------------------------------------------------------------------
int XrdMqOfs::Stall(XrdOucErrInfo&   error, // Error text & code
                    int              stime, // Seconds to stall
                    const char*      msg)   // Message to give
{
  XrdOucString smessage = msg;
  smessage += "; come back in ";
  smessage += stime;
  smessage += " seconds!";
  EPNAME("Stall");
  const char* tident = error.getErrUser();
  ZTRACE(delay, "Stall " << stime << ": " << smessage.c_str());
  // Place the error message in the error object and return
  error.setErrInfo(0, smessage.c_str());
  return stime;
}

//------------------------------------------------------------------------------
// Build redirect response
//------------------------------------------------------------------------------
int XrdMqOfs::Redirect(XrdOucErrInfo& error, XrdOucString& host, int& port)
{
  EPNAME("Redirect");
  const char* tident = error.getErrUser();
  ZTRACE(delay, "Redirect " << host.c_str() << ":" << port);
  // Place the error message in the error object and return
  error.setErrInfo(port, host.c_str());
  return SFS_REDIRECT;
}

//------------------------------------------------------------------------------
// Get XRootD version
//------------------------------------------------------------------------------
const char*
XrdMqOfs::getVersion()
{
  return XrdVERSION;
}

//------------------------------------------------------------------------------
// FSctl plugin function
//------------------------------------------------------------------------------
int
XrdMqOfs::FSctl(const int cmd, XrdSfsFSctl& args, XrdOucErrInfo& error,
                const XrdSecEntity* client)
{
  char ipath[XRDMQOFS_FSCTLPATHLEN + 1];
  static const char* epname = "FSctl";
  const char* tident = client->tident;
  SetLogId(nullptr, tident);
  eos_static_debug("arg1=\"%s\" arg2=\"%s\"", args.Arg1, args.Arg2);
  MAYREDIRECT;

  if (cmd != SFS_FSCTL_PLUGIN) {
    gMqFS->Emsg(epname, error, EINVAL, "to call FSctl - not supported", "");
    return SFS_ERROR;
  }

  // check for backlog
  if ((long long)Messages.size() > MaxMessageBacklog) {
    // this is not absolutely threadsafe .... better would lock
    BacklogDeferred++;
    gMqFS->Emsg(epname, error, ENOMEM, "accept message - too many pending messages",
                "");
    return SFS_ERROR;
  }

  if (args.Arg1Len) {
    if (args.Arg1Len < XRDMQOFS_FSCTLPATHLEN) {
      strncpy(ipath, args.Arg1, args.Arg1Len);
      ipath[args.Arg1Len] = 0;
    } else {
      gMqFS->Emsg(epname, error, EINVAL, "convert path argument - string too long",
                  "");
      return SFS_ERROR;
    }
  } else {
    ipath[0] = 0;
  }

  // from here on we can deal with XrdOucString which is more 'comfortable'
  XrdOucString path    = ipath;
  XrdOucString result  = "";
  XrdOucString opaque  = "";

  if (args.Arg2Len) {
    opaque.assign(args.Arg2, 0, args.Arg2Len);
  }

  XrdSmartOucEnv* env = new XrdSmartOucEnv(opaque.c_str());

  if (!env) {
    gMqFS->Emsg(epname, error, ENOMEM, "allocate memory", "");
    return SFS_ERROR;
  }

  // look into the header
  XrdMqMessageHeader mh;

  if (!mh.Decode(opaque.c_str())) {
    gMqFS->Emsg(epname, error, EINVAL, "decode message header", "");
    delete env;
    return SFS_ERROR;
  }

  // add the broker ID
  mh.kBrokerId = BrokerId;
  // update broker time
  mh.GetTime(mh.kBrokerTime_sec, mh.kBrokerTime_nsec);
  // dump it
  // mh.Print();
  // encode the new values
  mh.Encode();
  // replace the old header with the new one .... that's ugly :-(
  int envlen;
  XrdOucString envstring = env->Env(envlen);
  int p1 = envstring.find(XMQHEADER);
  int p2 = envstring.find("&", p1 + 1);
  envstring.erase(p1, p2 - 1);
  envstring.insert(mh.GetHeaderBuffer(), p1);
  delete env;
  env = new XrdSmartOucEnv(envstring.c_str());
  XrdMqOfsMatches matches(mh.kReceiverQueue.c_str(), env, tident, mh.kType,
                          mh.kSenderId.c_str());
  Deliver(matches);

  if (matches.backlogrejected) {
    XrdOucString backlogmessage =
      "queue message on all receivers - maximum backlog exceeded on queues: ";
    backlogmessage += matches.backlogqueues;
    gMqFS->Emsg(epname, error, E2BIG, backlogmessage.c_str(), ipath);

    if (backlogmessage.length() > 255) {
      backlogmessage.erase(255);
      backlogmessage += "...";
    }

    TRACES(backlogmessage.c_str());

    if (!matches.message->Refs()) {
      delete env;
    }

    return SFS_ERROR;
  }

  if (matches.backlog) {
    XrdOucString backlogmessage =
      "guarantee quick delivery - backlog exceeded on queues: ";
    backlogmessage += matches.backlogqueues;

    if (backlogmessage.length() > 255) {
      backlogmessage.erase(255);
      backlogmessage += "...";
    }

    gMqFS->Emsg(epname, error, ENFILE, backlogmessage.c_str(), ipath);
    TRACES(backlogmessage.c_str());
    return SFS_ERROR;
  }

  if (matches.matches) {
    const char* result = "OK";
    error.setErrInfo(3, (char*)result);

    if (((matches.messagetype) != XrdMqMessageHeader::kStatusMessage) &&
        ((matches.messagetype) != XrdMqMessageHeader::kQueryMessage)) {
      gMqFS->ReceivedMessages++;
    }

    return SFS_DATA;
  } else {
    bool ismonitor = false;

    if (env->Get(XMQMONITOR)) {
      ismonitor = true;
    }

    int envlen;
    std::string c = env->Env(envlen);
    delete env;

    // This is a new hook for special monitoring message, to just accept them
    // and if nobody listens they just go to nirvana.
    if (!ismonitor) {
      gMqFS->UndeliverableMessages++;
      gMqFS->Emsg(epname, error, EINVAL,
                  "submit message - no listener on requested queue: ", ipath);
      TRACES("no listener on requested queue: ");
      TRACES(ipath);
      return SFS_ERROR;
    } else {
      //      fprintf(stderr,"Dropped Monitor message %s\n",c.c_str());
      ZTRACE(fsctl, "Discarding monitor message without receiver");
      const char* result = "OK";
      error.setErrInfo(3, (char*)result);
      gMqFS->DiscardedMonitoringMessages++;
      return SFS_DATA;
    }
  }
}

//------------------------------------------------------------------------------
// Helper Classes & Functions
//------------------------------------------------------------------------------
bool
XrdMqOfs::Deliver(XrdMqOfsMatches& Matches)
{
  EPNAME("Deliver");
  XrdSysMutexHelper scope_lock(mQueueOutMutex);
  const char* tident = Matches.mTident;
  std::string sendername = Matches.sendername.c_str();
  // Store all the queues where we need to deliver this message
  std::vector<XrdMqMessageOut*> matched_out_queues;
  Matches.message->procmutex.Lock();

  // If we have a status message we have to do a complete loop
  if (((Matches.messagetype) == XrdMqMessageHeader::kStatusMessage) ||
      ((Matches.messagetype) == XrdMqMessageHeader::kQueryMessage)) {
    for (auto it = mQueueOut.begin(); it != mQueueOut.end(); ++it) {
      XrdMqMessageOut* msg_out = it->second;

      // If this is be a loop back message we continue
      if (sendername == it->first) {
        // avoid feedback to the same queue
        continue;
      }

      // If this queue does not take advisory status messages we continue
      if ((Matches.messagetype == XrdMqMessageHeader::kStatusMessage) &&
          (!msg_out->AdvisoryStatus)) {
        continue;
      }

      // if this queue does not take advisory query messages we continue
      if ((Matches.messagetype == XrdMqMessageHeader::kQueryMessage)  &&
          (!msg_out->AdvisoryQuery)) {
        continue;
      } else {
        matched_out_queues.push_back(msg_out);
      }
    }
  } else {
    // If we have a wildcard match we have to do a complete loop
    if ((Matches.queuename.find("*") != STR_NPOS)) {
      for (auto it = mQueueOut.begin(); it != mQueueOut.end(); ++it) {
        XrdMqMessageOut* msg_out = it->second;

        // fprintf(stderr,"current queue name: %s <=> sender :%s\n",
        //        it->first.c_str(), sendername.c_str());
        // If this would be a loop back message we continue
        if (sendername == it->first) {
          // avoid feedback to the same queue
          continue;
        }

        XrdOucString Key = it->first.c_str();
        XrdOucString nowildcard = Matches.queuename;
        nowildcard.replace("*", "");
        int nmatch = Key.matches(Matches.queuename.c_str(), '*');

        if (nmatch == nowildcard.length()) {
          // this is a match
          ZTRACE(fsctl, "Adding Wildcard matched Message to Queuename: "
                 << msg_out->QueueName.c_str());
          matched_out_queues.push_back(msg_out);
        }
      }
    } else {
      // We have just to find one named queue
      std::string queuename = Matches.queuename.c_str();
      XrdMqMessageOut* msg_out = 0;

      if (mQueueOut.count(queuename)) {
        msg_out = mQueueOut[queuename];
      }

      if (msg_out) {
        ZTRACE(fsctl, "Adding full matched Message to Queuename: " <<
               msg_out->QueueName.c_str());
        matched_out_queues.push_back(msg_out);
      }
    }
  }

  // This is a match
  if (matched_out_queues.size()) {
    Matches.backlog = false;
    Matches.backlogrejected = false;

    // Lock all matched queues at once
    for (auto msg_out : matched_out_queues) {
      msg_out->Lock();
    }

    for (auto msg_out : matched_out_queues) {
      // check for backlog on this queue and set a warning flag
      if (msg_out->mMsgQueue.size() > mMaxQueueBacklog) {
        // Only set the backlog flag if the queue has not set the advisory
        // flush back log flag
        if (!msg_out->AdvisoryFlushBackLog) {
          Matches.backlog = true;
        } else {
          if (!msg_out->BrokenByFlush) {
            msg_out->BrokenByFlush = true;
            TRACES("warning: queue " << msg_out->QueueName
                   << " is broken by backlog flush of "
                   << mMaxQueueBacklog  << " message!");
          }
        }

        Matches.backlogqueues += msg_out->QueueName;
        Matches.backlogqueues += ":";
        gMqFS->QueueBacklogHits++;

        if (!msg_out->BrokenByFlush) {
          TRACES("warning: queue " << msg_out->QueueName
                 << " exceeds backlog of " << mMaxQueueBacklog
                 << " message!");
        }
      } else {
        if (msg_out->BrokenByFlush) {
          msg_out->BrokenByFlush = false;
          TRACES("warning: re-enabling queue " << msg_out->QueueName
                 << " backlog is now " << msg_out->mMsgQueue.size()
                 << " messages!");
        }
      }

      if (msg_out->mMsgQueue.size() > mRejectQueueBacklog) {
        // Only set the reject flag if the queue has not set the advisory
        // flush back log flag
        if (!msg_out->AdvisoryFlushBackLog) {
          Matches.backlogrejected = true;
        } else {
          if (!msg_out->BrokenByFlush) {
            msg_out->BrokenByFlush = true;
            TRACES("warning: queue " << msg_out->QueueName
                   << " is broken by backlog flush of " << mRejectQueueBacklog
                   << " message!");
          }
        }

        Matches.backlogqueues += msg_out->QueueName;
        Matches.backlogqueues += ":";
        gMqFS->BacklogDeferred++;

        if (!msg_out->BrokenByFlush)
          TRACES("error: queue " << msg_out->QueueName
                 << " exceeds max. accepted backlog of " << mRejectQueueBacklog
                 << " message!");
      } else {
        if (!msg_out->BrokenByFlush) {
          // We deliver only to not broken clients, they have to reconnect to
          // get out of this situation
          Matches.matches++;

          if (Matches.matches == 1) {
            // add to the message hash
            std::string messageid = Matches.message->Get(XMQHEADER);
            XrdSysMutexHelper scope_lock(gMqFS->mMsgsMutex);
            gMqFS->Messages.insert(std::pair<std::string, XrdSmartOucEnv*> (messageid,
                                   Matches.message));
          }

          ZTRACE(fsctl, "Adding Message to Queuename: " << msg_out->QueueName.c_str());
          msg_out->mMsgQueue.push_back(Matches.message);
          Matches.message->AddRefs(1);
        }
      }
    }

    // Unlock all matched queues at once
    for (auto msg_out : matched_out_queues) {
      msg_out->UnLock();
    }
  }

  Matches.message->procmutex.UnLock();
  return (Matches.matches > 0);
}

//------------------------------------------------------------------------------
// Collect all messages from the queue and append them to the internal
// buffer. Also delete messages if this was the last reference towards them.
//------------------------------------------------------------------------------
size_t
XrdMqMessageOut::RetrieveMessages()
{
  XrdSmartOucEnv* message {nullptr};
  XrdSysMutexHelper scope_lock(mMutex);

  while (mMsgQueue.size()) {
    message = mMsgQueue.front();
    mMsgQueue.pop_front();
    message->procmutex.Lock();
    // fprintf(stderr,"%llu %s Message %llu nref: %d\n", (unsigned long long)
    // &mMsgQueue, QueueName.c_str(), (unsigned long long) message, message->Refs());
    int len;
    mMsgBuffer += message->Env(len);
    ++gMqFS->mDeliveredMessages;
    message->DecRefs();

    if (message->Refs() <= 0) {
      // We can delete this message from the queue!
      const char* ptr = message->Get(XMQHEADER);

      if (ptr) {
        std::string msg_id = message->Get(XMQHEADER);
        XrdSysMutexHelper scope_lock(gMqFS->mMsgsMutex);
        gMqFS->Messages.erase(msg_id.c_str());
      }

      message->procmutex.UnLock();
      // fprintf(stderr,"%s delete %llu \n", QueueName.c_str(),
      // (unsigned long long) message);
      delete message;
      ++gMqFS->mFanOutMessages;
    } else {
      message->procmutex.UnLock();
    }
  }

  return mMsgBuffer.length();
}
