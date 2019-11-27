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
// Disconnect
//------------------------------------------------------------------------------
void
XrdMqClient::Disconnect()
{
  eos::common::RWMutexWriteLock wr_lock(mMutexMap);
  mMapBrokerToChannels.clear();
}

//------------------------------------------------------------------------------
// SendMessage
//------------------------------------------------------------------------------
bool
XrdMqClient::SendMessage(XrdMqMessage& msg, const char* receiverid, bool sign,
                         bool encrypt, bool asynchronous)
{
  bool rc = false;
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

  {
    eos::common::RWMutexReadLock rd_lock(mMutexMap);

    for (const auto& broker : mMapBrokerToChannels) {
      XrdCl::Buffer arg;
      XrdCl::XRootDStatus status;
      uint16_t timeout = (getenv("EOS_FST_OP_TIMEOUT") ?
                          atoi(getenv("EOS_FST_OP_TIMEOUT")) : 0);
      XrdCl::Buffer* response_raw {nullptr};
      std::unique_ptr<XrdCl::Buffer> response {nullptr};
      auto send_channel = broker.second.second;
      arg.FromString(message.c_str());

      if (asynchronous) {
        // Don't wait for responses if not required
        status = send_channel->Query(XrdCl::QueryCode::OpaqueFile, arg,
                                     &gDiscardResponseHandler, timeout);
      } else {
        status = send_channel->Query(XrdCl::QueryCode::OpaqueFile, arg,
                                     response_raw, timeout);
        response.reset(response_raw);
        response_raw = nullptr;
      }

      rc = status.IsOK();

      // We continue until any of the brokers accepts the message
      if (!rc) {
        UpdateBrokersEndpoints(true);
        eos_err("msg=\"failed to send message\" dst=\"%s\" msg=\"%s\"",
                broker.first.c_str(), message.c_str());
        XrdMqMessage::Eroute.Emsg("SendMessage", status.errNo,
                                  status.GetErrorMessage().c_str());
      }
    }
  }

  UpdateBrokersEndpoints();
  return rc;
}

//------------------------------------------------------------------------------
// Reply to a particular message
//------------------------------------------------------------------------------
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
  std::shared_ptr<XrdCl::File> recv_channel;
  {
    eos::common::RWMutexReadLock rd_lock(mMutexMap);

    if (mMapBrokerToChannels.size() != 1) {
      eos_static_err("msg=\"no support for multi-broker setup or no broker "
                     "registered\" map_size=%i", mMapBrokerToChannels.size());
      return nullptr;
    }

    // Single broker case - check if there is still a buffered message
    XrdMqMessage* message;
    message = RecvFromInternalBuffer();

    if (message) {
      return message;
    }

    recv_channel = mMapBrokerToChannels.begin()->second.first;
  }
  uint16_t timeout = (getenv("EOS_FST_OP_TIMEOUT") ?
                      atoi(getenv("EOS_FST_OP_TIMEOUT")) : 0);
  XrdCl::StatInfo* stinfo = nullptr;

  while (!recv_channel->Stat(true, stinfo, timeout).IsOK()) {
    UpdateBrokersEndpoints(true);
    recv_channel = mMapBrokerToChannels.begin()->second.first;

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
    delete stinfo;
    return 0;
  }

  // Mantain a receiver buffer which fits the need
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
  XrdCl::XRootDStatus status = recv_channel->Read(0, stinfo->GetSize(),
                               kRecvBuffer, nread);

  if (status.IsOK() && (nread > 0)) {
    kRecvBuffer[nread] = 0;
    // Add to the internal message buffer
    kInternalBufferPosition = 0;
    kMessageBuffer = kRecvBuffer;
  }

  delete stinfo;
  return RecvFromInternalBuffer();
}

//------------------------------------------------------------------------------
// AddBroker
//------------------------------------------------------------------------------
bool
XrdMqClient::AddBroker(const std::string& broker_url, bool advisorystatus,
                       bool advisoryquery, bool advisoryflushbacklog)
{
  if (broker_url.empty()) {
    return false;
  }

  std::ostringstream oss;
  oss << broker_url;

  if (broker_url.find("?") == std::string::npos) {
    oss << "?";
  } else {
    oss << "&";
  }

  oss << XMQCADVISORYSTATUS << "=" << advisorystatus << "&"
      << XMQCADVISORYQUERY << "=" << advisoryquery << "&"
      << XMQCADVISORYFLUSHBACKLOG << "=" << advisoryflushbacklog;
  std::string new_url = oss.str();
  // Check validity of the new broker url
  XrdCl::URL xrd_url(new_url);

  if (!xrd_url.IsValid()) {
    eos_static_err("msg=\"invalid url\" url=\"%s\"", new_url.c_str());
    return false;
  }

  eos_static_info("msg=\"add broker\" url=\"%s\"", new_url.c_str());
  eos::common::RWMutexWriteLock wr_lock(mMutexMap);

  if (mMapBrokerToChannels.find(new_url) != mMapBrokerToChannels.end()) {
    eos_static_err("msg=\"broker already exists\" url=\"%s\"", new_url.c_str());
    return false;
  }

  auto ret = mMapBrokerToChannels.emplace
             (new_url, std::make_pair(std::make_shared<XrdCl::File>(),
                                      std::make_shared<XrdCl::FileSystem>(xrd_url)));

  if (!ret.second) {
    eos_static_err("msg=\"failed to create broker channels\" url=\"%s\"",
                   new_url.c_str());
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Update the broker url if we get a redirect
//------------------------------------------------------------------------------
void
XrdMqClient::UpdateBrokersEndpoints(bool force)
{
  using namespace std::chrono;
  static auto last_ts = system_clock::now();
  auto now = system_clock::now();

  // Attempt endpoint update once every 5 seconds if not forced
  if (!force && (duration_cast<seconds>(now - last_ts) < seconds(5))) {
    return;
  } else {
    last_ts = now;
  }

  std::map<std::string, std::string> endpoint_replacements;
  {
    // Collect broker endpoints that need to be updated
    eos::common::RWMutexReadLock rd_lock(mMutexMap);

    for (const auto& broker : mMapBrokerToChannels) {
      XrdCl::File file;
      std::string new_brokerid;
      XrdCl::URL url(broker.first);
      XrdCl::XRootDStatus st = file.Open(broker.first, XrdCl::OpenFlags::Read);

      // Skip if we can't contact or we couldn't get the propety
      if ((st.IsOK() == false) ||
          (file.GetProperty("DataServer", new_brokerid) == false)) {
        continue;
      }

      if (url.GetHostId() != new_brokerid) {
        // Build the new broker URL
        std::string hostname {new_brokerid};
        int port = 1097;
        size_t pos = new_brokerid.find(':');

        // Extract hostname and port
        if (pos != std::string::npos) {
          try {
            port = std::stoi(new_brokerid.substr(pos + 1));
          } catch (...) {
            // ignore any conversion errors
          }

          hostname = new_brokerid.substr(0, pos);
        }

        url.SetHostPort(hostname, port);
        eos_static_info("msg=\"broker endpoint update\", old_url=\"%s\" "
                        "new_url=\"%s\"", broker.first.c_str(),
                        url.GetURL().c_str());
        endpoint_replacements.emplace(broker.first, url.GetURL());
      }
    }
  }

  if (endpoint_replacements.empty()) {
    return;
  }

  eos::common::RWMutexWriteLock wr_lock(mMutexMap);

  for (const auto& replace : endpoint_replacements) {
    auto it_old = mMapBrokerToChannels.find(replace.first);

    if (it_old == mMapBrokerToChannels.end()) {
      continue;
    }

    // Close old receive channel with small timeout to avoid any hangs
    auto recv_channel = it_old->second.first;
    (void) recv_channel->Close(1);
    mMapBrokerToChannels.erase(it_old);

    if (mMapBrokerToChannels.find(replace.second) != mMapBrokerToChannels.end()) {
      eos_static_err("msg=\"broker already exists\" url=\"%s\"",
                     replace.second.c_str());
      continue;
    }

    XrdCl::URL xrd_url(replace.second);
    auto ret = mMapBrokerToChannels.emplace
               (replace.second, std::make_pair(std::make_shared<XrdCl::File>(),
                   std::make_shared<XrdCl::FileSystem>(xrd_url)));

    if (!ret.second) {
      eos_static_err("msg=\"failed to create broker channels\" url=\"%s\"",
                     replace.second.c_str());
    }
  }
}

//------------------------------------------------------------------------------
// Subscribe
//------------------------------------------------------------------------------
void
XrdMqClient::Subscribe()
{
  eos::common::RWMutexReadLock rd_lock(mMutexMap);

  for (const auto& broker : mMapBrokerToChannels) {
    std::string url = broker.first;
    auto recv_channel = broker.second.first;

    if (recv_channel->Open(url.c_str(), XrdCl::OpenFlags::Read).IsOK()) {
      eos_static_info("msg=\"successfully subscribed to broker\" url=\"%s\"",
                      url.c_str());
    } else {
      eos_static_info("msg=\"failed to subscribe to broker\" url=\"%s\"",
                      url.c_str());
    }
  }
}

//------------------------------------------------------------------------------
// Unsubscribe
//------------------------------------------------------------------------------
void
XrdMqClient::Unsubscribe()
{
  eos::common::RWMutexReadLock rd_lock(mMutexMap);

  for (const auto& broker : mMapBrokerToChannels) {
    auto recv_channel = broker.second.first;
    auto st = recv_channel->Close(1);
    (void) st;
  }
}
