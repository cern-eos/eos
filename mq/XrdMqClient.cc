// ----------------------------------------------------------------------
// File: XrdMqClient.cc
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

#include <common/Logging.hh>
#include <mq/XrdMqClient.hh>
#include <mq/XrdMqTiming.hh>
#include <XrdSys/XrdSysDNS.hh>
#include <XrdCl/XrdClDefaultEnv.hh>
#include <setjmp.h>
#include <signal.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <thread>

/******************************************************************************/
/*                        X r d M q C l i e n t                               */
/******************************************************************************/

XrdSysMutex XrdMqClient::Mutex;
XrdMqClient::DiscardResponseHandler XrdMqClient::gDiscardResponseHandler;

//------------------------------------------------------------------------------
// Signal Handler for SIGBUS
//------------------------------------------------------------------------------
static sigjmp_buf xrdmqclient_sj_env;

static void
xrdmqclient_sigbus_hdl(int sig, siginfo_t* siginfo, void* ptr)
{
  siglongjmp(xrdmqclient_sj_env, 1);
}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
XrdMqClient::XrdMqClient(const char* clientid, const char* brokerurl,
                         const char* defaultreceiverid)
{
  kInitOK = true;
  kBrokerN = 0;
  kMessageBuffer = "";
  kRecvBuffer = nullptr;
  kRecvBufferAlloc = 0;
  // Install sigbus signal handler
  struct sigaction act;
  memset(&act, 0, sizeof(act));
  act.sa_sigaction = xrdmqclient_sigbus_hdl;
  act.sa_flags = SA_SIGINFO;

  if (sigaction(SIGBUS, &act, 0)) {
    fprintf(stderr, "error: [XrdMqClient] cannot install SIGBUS handler\n");
  }

  // Set short timeout resolution, connection window, connection retry and
  // stream error window.
  XrdCl::DefaultEnv::GetEnv()->PutInt("TimeoutResolution", 1);
  XrdCl::DefaultEnv::GetEnv()->PutInt("ConnectionWindow", 5);
  XrdCl::DefaultEnv::GetEnv()->PutInt("ConnectionRetry", 1);
  XrdCl::DefaultEnv::GetEnv()->PutInt("StreamErrorWindow", 0);

  if (brokerurl && !AddBroker(brokerurl)) {
    fprintf(stderr, "error: [XrdMqClient] cannot add broker %s\n", brokerurl);
  }

  if (defaultreceiverid) {
    kDefaultReceiverQueue = defaultreceiverid;
  } else {
    // Default receiver is always a master
    kDefaultReceiverQueue = "/xmessage/*/master/*";
  }

  if (clientid) {
    kClientId = clientid;

    if (kClientId.beginswith("root://")) {
      // Truncate the URL away
      int pos = kClientId.find("//", 7);

      if (pos != STR_NPOS) {
        kClientId.erase(0, pos + 1);
      }
    }
  } else {
    // By default create the client id as /xmesssage/<domain>/<host>/
    int ppos = 0;
    char* cfull_name = XrdSysDNS::getHostName();

    if (!cfull_name || std::string(cfull_name) == "0.0.0.0") {
      kInitOK = false;
    }

    XrdOucString FullName = cfull_name;
    XrdOucString HostName = FullName;
    XrdOucString Domain = FullName;

    if ((ppos = FullName.find(".")) != STR_NPOS) {
      HostName.assign(FullName, 0, ppos - 1);
      Domain.assign(FullName, ppos + 1);
    } else {
      Domain = "unknown";
    }

    kClientId = "/xmessage/";
    kClientId += HostName;
    kClientId += "/";
    kClientId += Domain;
    free(cfull_name);
  }

  kInternalBufferPosition = 0;
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
XrdMqClient::~XrdMqClient()
{
  if (kRecvBuffer) {
    free(kRecvBuffer);
    kRecvBuffer = nullptr;
  }
}

//------------------------------------------------------------------------------
// Subscribe
//------------------------------------------------------------------------------
bool
XrdMqClient::Subscribe(const char* queue)
{
  if (queue) {
    // We support subscrition to a single queue only - queue has to be 0!!!
    XrdMqMessage::Eroute.Emsg("Subscribe", EINVAL,
                              "subscribe to additional user specified queue");
    return false;
  }

  for (int i = 0; i < kBrokerN; i++) {
    XrdCl::OpenFlags::Flags flags_xrdcl = XrdCl::OpenFlags::Read;
    XrdCl::File* file = GetBrokerXrdClientReceiver(i);
    XrdOucString* url = kBrokerUrls.Find(GetBrokerId(i).c_str());

    if (!file || !file->Open(url->c_str(), flags_xrdcl).IsOK()) {
      // Open failed
      continue;
    }
  }

  return true;
}

//------------------------------------------------------------------------------
// Unsubscribe
//------------------------------------------------------------------------------
bool
XrdMqClient::Unsubscribe(const char* queue)
{
  if (queue) {
    XrdMqMessage::Eroute.Emsg("Unubscribe", EINVAL,
                              "unsubscribe from additional user specified queue");
    return false;
  }

  for (int i = 0; i < kBrokerN; i++) {
    XrdCl::File* file = GetBrokerXrdClientReceiver(i);

    if (file && (!file->Close().IsOK())) {
      // Close failed
      continue;
    }
  }

  return true;
}

//------------------------------------------------------------------------------
// Disconnect
//------------------------------------------------------------------------------
void
XrdMqClient::Disconnect()
{
  for (int i = 0; i < kBrokerN; i++) {
    delete GetBrokerXrdClientReceiver(i);
  }

  kBrokerN = 0;
  return;
}

//------------------------------------------------------------------------------
// SendMessage
//------------------------------------------------------------------------------
bool
XrdMqClient::SendMessage(XrdMqMessage& msg, const char* receiverid, bool sign,
                         bool encrypt, bool asynchronous)
{
  bool rc = false;
  XrdSysMutexHelper lock(Mutex);
  // Tag the sender
  msg.kMessageHeader.kSenderId = kClientId;
  // Tag the send time
  XrdMqMessageHeader::GetTime(msg.kMessageHeader.kSenderTime_sec,
                              msg.kMessageHeader.kSenderTime_nsec);

  // Tag the receiver queue
  if (!receiverid) {
    msg.kMessageHeader.kReceiverQueue = kDefaultReceiverQueue;
  } else {
    msg.kMessageHeader.kReceiverQueue = receiverid;
  }

  if (encrypt) {
    msg.Sign(true);
  } else {
    if (sign) {
      msg.Sign(false);
    } else {
      msg.Encode();
    }
  }

  XrdOucString message = msg.kMessageHeader.kReceiverQueue;
  message += "?";
  message += msg.GetMessageBuffer();

  if (message.length() > (2 * 1000 * 1000)) {
    fprintf(stderr, "XrdMqClient::SendMessage: error => trying to send message "
            "with size %d [limit is 2M]\n", message.length());
    XrdMqMessage::Eroute.Emsg("SendMessage", E2BIG,
                              "The message exceeds the maximum size of 2M!");
    return false;
  }

  XrdCl::Buffer arg;
  XrdCl::XRootDStatus status;

  for (int i = 0; i < kBrokerN; i++) {
    XrdOucString rhostport;
    XrdCl::URL url(GetBrokerUrl(i, rhostport)->c_str());

    if (!url.IsValid()) {
      fprintf(stderr, "error=URL is not valid: %s",
              GetBrokerUrl(i, rhostport)->c_str());
      XrdMqMessage::Eroute.Emsg("SendMessage", EINVAL, "URL is not valid");
      continue;
    }

    uint16_t timeout = (getenv("EOS_FST_OP_TIMEOUT") ?
                        atoi(getenv("EOS_FST_OP_TIMEOUT")) : 0);
    XrdCl::Buffer* response_raw {nullptr};
    std::unique_ptr<XrdCl::Buffer> response {nullptr};
    std::unique_ptr<XrdCl::FileSystem> fs {new XrdCl::FileSystem(url)};

    if (!fs) {
      fprintf(stderr, "error=failed to get new FS object");
      XrdMqMessage::Eroute.Emsg("SendMessage", EINVAL, "no broker available");
      continue;
    }

    arg.FromString(message.c_str());

    if (asynchronous) {
      // Don't wait for responses if not required
      status = fs->Query(XrdCl::QueryCode::OpaqueFile, arg,
                         &gDiscardResponseHandler, timeout);
    } else {
      status = fs->Query(XrdCl::QueryCode::OpaqueFile, arg, response_raw,
                         timeout);
      response.reset(response_raw);
      response_raw = nullptr;
    }

    rc = status.IsOK();

    // We continue until any of the brokers accepts the message
    if (!rc) {
      XrdMqMessage::Eroute.Emsg("SendMessage", status.errNo,
                                status.GetErrorMessage().c_str());
    }
  }

  return rc;
}

//----------------------------------------------------------------------------
//! Reply to a particular message
//----------------------------------------------------------------------------
bool
XrdMqClient::ReplyMessage(XrdMqMessage& replymsg, XrdMqMessage& inmsg,
                          bool sign, bool encrypt)
{
  replymsg.SetReply(inmsg);
  return SendMessage(replymsg, inmsg.kMessageHeader.kSenderId.c_str(), sign,
                     encrypt);
}

//------------------------------------------------------------------------------
// RecvMessage
//------------------------------------------------------------------------------
XrdMqMessage*
XrdMqClient::RecvFromInternalBuffer()
{
  if ((kMessageBuffer.length() - kInternalBufferPosition) > 0) {
    // fprintf( stderr,"Message Buffer %ld\n", kMessageBuffer.length());
    //          there is still a message in the buffer
    int nextmessage;
    int firstmessage;
    // fprintf( stderr,"#### %ld Entering at position %ld %ld\n", time(NULL),
    //          kInternalBufferPosition, kMessageBuffer.length() );
    firstmessage = kMessageBuffer.find(XMQHEADER, kInternalBufferPosition);

    if (firstmessage == STR_NPOS) {
      return 0;
    } else {
      if ((firstmessage > 0) && ((size_t) firstmessage > kInternalBufferPosition)) {
        kMessageBuffer.erase(0, firstmessage);
        kInternalBufferPosition = 0;
      }
    }

    if ((kMessageBuffer.length() + kInternalBufferPosition) <
        (int) strlen(XMQHEADER)) {
      return 0;
    }

    nextmessage = kMessageBuffer.find(XMQHEADER,
                                      kInternalBufferPosition + strlen(XMQHEADER));
    char savec = 0;

    if (nextmessage != STR_NPOS) {
      savec = kMessageBuffer.c_str()[nextmessage];
      ((char*) kMessageBuffer.c_str())[nextmessage] = 0;
    }

    XrdMqMessage* message = XrdMqMessage::Create(kMessageBuffer.c_str() +
                            kInternalBufferPosition);

    if (!message) {
      fprintf(stderr, "couldn't get any message\n");
      return 0;
    }

    XrdMqMessageHeader::GetTime(message->kMessageHeader.kReceiverTime_sec,
                                message->kMessageHeader.kReceiverTime_nsec);

    if (nextmessage != STR_NPOS) {
      ((char*) kMessageBuffer.c_str())[nextmessage] = savec;
    }

    if (nextmessage == STR_NPOS) {
      // Last message
      kMessageBuffer = "";
      kInternalBufferPosition = 0;
    } else {
      // Move forward
      //kMessageBuffer.erase(0,nextmessage);
      kInternalBufferPosition = nextmessage;
    }

    return message;
  } else {
    kMessageBuffer = "";
    kInternalBufferPosition = 0;
  }

  return 0;
}

//------------------------------------------------------------------------------
// Receive message
//------------------------------------------------------------------------------
XrdMqMessage*
XrdMqClient::RecvMessage(ThreadAssistant* assistant)
{
  if (kBrokerN == 1) {
    // Single broker case - check if there is still a buffered message
    XrdMqMessage* message;
    message = RecvFromInternalBuffer();

    if (message) {
      return message;
    }

    XrdCl::File* file = GetBrokerXrdClientReceiver(0);

    if (!file) {
      // Fatal no client
      XrdMqMessage::Eroute.Emsg("RecvMessage", EINVAL,
                                "receive message - no client present");
      return nullptr;
    }

    uint16_t timeout = (getenv("EOS_FST_OP_TIMEOUT") ?
                        atoi(getenv("EOS_FST_OP_TIMEOUT")) : 0);
    XrdCl::StatInfo* stinfo = nullptr;

    while (!file->Stat(true, stinfo, timeout).IsOK()) {
      fprintf(stderr, "XrdMqClient::RecvMessage => Stat failed\n");
      ReNewBrokerXrdClientReceiver(0);
      file = GetBrokerXrdClientReceiver(0);

      if (assistant) {
        assistant->wait_for(std::chrono::seconds(2));

        if (assistant->terminationRequested()) {
          return nullptr;
        }
      } else {
        std::this_thread::sleep_for(std::chrono::seconds(2));
      }
    }

    if (stinfo->GetSize() == 0) {
      return 0;
    }

    // mantain a receiver buffer which fits the need
    if (kRecvBufferAlloc < (int) stinfo->GetSize()) {
      uint64_t allocsize = 1024 * 1024;

      if (stinfo->GetSize() > allocsize) {
        allocsize = stinfo->GetSize() + 1;
      }

      kRecvBuffer = static_cast<char*>(realloc(kRecvBuffer, allocsize));

      if (!kRecvBuffer) {
        // Fatal - we exit!
        exit(-1);
      }

      kRecvBufferAlloc = allocsize;
    }

    // Read all messages
    uint32_t nread = 0;
    XrdCl::XRootDStatus status = file->Read(0, stinfo->GetSize(), kRecvBuffer,
                                            nread);

    if (status.IsOK() && (nread > 0)) {
      kRecvBuffer[nread] = 0;
      // Add to the internal message buffer
      kInternalBufferPosition = 0;
      kMessageBuffer = kRecvBuffer;
    }

    delete stinfo;
    return RecvFromInternalBuffer();
  } else {
    // Multiple broker case
    return nullptr;
  }

  return nullptr;
}

//------------------------------------------------------------------------------
// GetBrokerUrl
//------------------------------------------------------------------------------
XrdOucString*
XrdMqClient::GetBrokerUrl(int i, XrdOucString& rhostport)
{
  XrdOucString n = "";
  n += i;
  // Split url
  return kBrokerUrls.Find(n.c_str());
}

//------------------------------------------------------------------------------
// GetBrokerId
//------------------------------------------------------------------------------
XrdOucString
XrdMqClient::GetBrokerId(int i)
{
  XrdOucString brokern;

  if (i == 0) {
    brokern = "0";
  } else {
    brokern += kBrokerN;
  }

  return brokern;
}

//------------------------------------------------------------------------------
// GetBrokerXrdClientReceiver
//------------------------------------------------------------------------------
XrdCl::File*
XrdMqClient::GetBrokerXrdClientReceiver(int i)
{
  return kBrokerXrdClientReceiver.Find(GetBrokerId(i).c_str());
}

//------------------------------------------------------------------------------
// ReNewBrokerXrdClientReceiver
//------------------------------------------------------------------------------
void
XrdMqClient::ReNewBrokerXrdClientReceiver(int i)
{
  auto old_file = kBrokerXrdClientReceiver.Find(GetBrokerId(i).c_str());

  if (old_file) {
    // Close old file with small timeout to avoid any hangs, blow it will be
    // automatically deleted
    (void) old_file->Close(1);
  }

  kBrokerXrdClientReceiver.Del(GetBrokerId(i).c_str());

  do {
    auto file = new XrdCl::File();
    XrdOucString rhostport;
    uint16_t timeout = (getenv("EOS_FST_OP_TIMEOUT") ?
                        atoi(getenv("EOS_FST_OP_TIMEOUT")) : 0);
    std::string url {GetBrokerUrl(i, rhostport)->c_str()};
    XrdCl::XRootDStatus status = file->Open(url, XrdCl::OpenFlags::Read,
                                            XrdCl::Access::None, timeout);

    if (status.IsOK()) {
      kBrokerXrdClientReceiver.Add(GetBrokerId(i).c_str(), file);
      break;
    } else {
      delete file;
      fprintf(stderr, "XrdMqClient::Reopening of new alias failed ...\n");
      std::this_thread::sleep_for(std::chrono::seconds(2));
    }
  } while (true);
}

//------------------------------------------------------------------------------
// AddBroker
//------------------------------------------------------------------------------
bool
XrdMqClient::AddBroker(const char* brokerurl,
                       bool advisorystatus,
                       bool advisoryquery,
                       bool advisoryflushbacklog)
{
  if (!brokerurl) {
    return false;
  }

  bool exists = false;
  XrdOucString newBrokerUrl = brokerurl;

  if ((newBrokerUrl.find("?")) == STR_NPOS) {
    newBrokerUrl += "?";
  }

  newBrokerUrl += "&";
  newBrokerUrl += XMQCADVISORYSTATUS;
  newBrokerUrl += "=";
  newBrokerUrl += advisorystatus;
  newBrokerUrl += "&";
  newBrokerUrl += XMQCADVISORYQUERY;
  newBrokerUrl += "=";
  newBrokerUrl += advisoryquery;
  newBrokerUrl += "&";
  newBrokerUrl += XMQCADVISORYFLUSHBACKLOG;
  newBrokerUrl += "=";
  newBrokerUrl += advisoryflushbacklog;
  printf("==> new Broker %s\n", newBrokerUrl.c_str());

  for (int i = 0; i < kBrokerN; i++) {
    XrdOucString rhostport;
    XrdOucString* brk = GetBrokerUrl(i, rhostport);

    if (brk && ((*brk) == newBrokerUrl)) {
      exists = true;
    }
  }

  if (!exists) {
    XrdOucString brokern = GetBrokerId(kBrokerN);
    kBrokerUrls.Add(brokern.c_str(), new XrdOucString(newBrokerUrl.c_str()));
    XrdCl::URL url(newBrokerUrl.c_str());

    if (!url.IsValid()) {
      fprintf(stderr, "error=URL is not valid: %s", newBrokerUrl.c_str());
      return exists;
    }

    XrdCl::FileSystem* fs = new XrdCl::FileSystem(url);

    if (!fs) {
      fprintf(stderr, "cannot create FS obj to %s\n", newBrokerUrl.c_str());
      kBrokerUrls.Del(brokern.c_str());
      XrdMqMessage::Eroute.Emsg("AddBroker", EPERM, "add and connect to broker:",
                                newBrokerUrl.c_str());
      return false;
    }

    try {
      kBrokerXrdClientSender.Add(GetBrokerId(kBrokerN).c_str(), fs);
      kBrokerXrdClientReceiver.Add(GetBrokerId(kBrokerN).c_str(), new XrdCl::File());
      kBrokerN++;
    } catch (int& err) {
      fprintf(stderr, "error: out of memory while inserting into hash\n");
      return false;
    }
  }

  return (!exists);
}
