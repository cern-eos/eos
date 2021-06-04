//------------------------------------------------------------------------------
// File: XrdMqClient.cc
// Author: Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

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

#include "mq/XrdMqClient.hh"
#include <XrdNet/XrdNetUtils.hh>
#include <XrdCl/XrdClDefaultEnv.hh>
#include <setjmp.h>
#include <signal.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>

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
  kInternalBufferPosition = 0;
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
    char* cfull_name = XrdNetUtils::MyHostName(0);

    if (!cfull_name) {
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
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
XrdMqClient::~XrdMqClient()
{
  eos::common::RWMutexWriteLock wr_lock(mMutexMap);

  for (const auto& broker : mMapBrokerToChannels) {
    auto st = broker.second.first->Close(1);
    (void) st;
  }

  mMapBrokerToChannels.clear();

  if (kRecvBuffer) {
    free(kRecvBuffer);
    kRecvBuffer = nullptr;
  }
}

//------------------------------------------------------------------------------
// AddBroker
//------------------------------------------------------------------------------
bool
XrdMqClient::AddBroker(const std::string& broker_url, bool advisorystatus,
                       bool advisoryquery, bool advisoryflushbacklog)
{
  if (broker_url.empty()) {
    eos_static_err("%s", "msg=\"cannot add empty broker url\"");
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
  mDefaultBrokerUrl = new_url;
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
// SendMessage
//------------------------------------------------------------------------------
bool
XrdMqClient::SendMessage(XrdMqMessage& msg, const char* receiverid, bool sign,
                         bool encrypt, bool asynchronous)
{
  bool rc = false;
  // Only one send message at a time
  static std::mutex s_mutex_send;
  std::unique_lock lock(s_mutex_send);
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

  int all_ok = true;
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
        all_ok = false;
        eos_err("msg=\"failed to send message\" dst=\"%s\" msg=\"%s\"",
                broker.first.c_str(), message.c_str());
        XrdMqMessage::Eroute.Emsg("SendMessage", status.errNo,
                                  status.GetErrorMessage().c_str());
      }
    }
  }

  if (!all_ok) {
    RefreshBrokersEndpoints();
  }

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
// Receive message
//------------------------------------------------------------------------------
XrdMqMessage*
XrdMqClient::RecvMessage(ThreadAssistant* assistant)
{
  std::shared_ptr<XrdCl::File> recv_channel;
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

  uint16_t timeout = (getenv("EOS_FST_OP_TIMEOUT") ?
                      atoi(getenv("EOS_FST_OP_TIMEOUT")) : 0);
  XrdCl::StatInfo* stinfo = nullptr;
  recv_channel = mMapBrokerToChannels.begin()->second.first;

  while (!recv_channel->Stat(true, stinfo, timeout).IsOK()) {
    // Any error on stat requires a refresh of the broker endpoints
    rd_lock.Release();
    RefreshBrokersEndpoints();
    rd_lock.Grab(mMutexMap);

    if (mMapBrokerToChannels.empty()) {
      eos_static_err("%s", "msg=\"no broker registered\"");
      return nullptr;
    }

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
    return nullptr;
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
// Receive message from internal buffer
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
// Refresh the in/out-bound channels to all the brokers
//------------------------------------------------------------------------------
void
XrdMqClient::RefreshBrokersEndpoints()
{
  // Only one refresh at a time
  static std::mutex s_mutex_refresh;
  std::unique_lock lock(s_mutex_refresh);
  std::map<std::string, std::string> endpoint_replacements;
  {
    // Collect broker endpoints that need to be updated
    eos::common::RWMutexReadLock rd_lock(mMutexMap);

    for (const auto& broker : mMapBrokerToChannels) {
      XrdCl::File file;
      std::string new_hostid;
      // Create a new dummy url since the current one might be actually working
      // and check if we get redirected
      XrdCl::URL tmp_url(broker.first);
      tmp_url.SetPath(tmp_url.GetPath() + "_mq_test");

      if (!tmp_url.IsValid()) {
        eos_static_err("msg=\"invalid url\" url=\"%s\"", tmp_url.GetURL().c_str());
        std::abort();
      }

      XrdCl::XRootDStatus st = file.Open(tmp_url.GetURL(), XrdCl::OpenFlags::Read);

      // Skip if we can't contact or we couldn't get the property
      if (!st.IsOK() || !file.GetProperty("DataServer", new_hostid)) {
        eos_static_err("msg=\"failed to contact broker\" url=\"%s\"",
                       tmp_url.GetURL().c_str());
        st = file.Close(1);

        if (mDefaultBrokerUrl != broker.first) {
          eos_static_info("msg=\"refresh broker endpoint\" old_url=\"%s\" "
                          "default_url=\"%s\"", broker.first.c_str(),
                          mDefaultBrokerUrl.c_str());
          endpoint_replacements.emplace(broker.first, mDefaultBrokerUrl);
        }

        break;
      }

      st = file.Close(1);
      (void) st;
      // Extract hostname and port from new_hostid
      int new_port;
      std::string new_hostname;
      ParseXrdClHostId(new_hostid, new_hostname, new_port);
      XrdCl::URL new_url(broker.first);
      const std::string old_host_id {new_url.GetHostId()};
      // Update the new url endpoint
      new_url.SetHostPort(new_hostname, new_port);

      if (!new_url.IsValid()) {
        eos_static_err("msg=\"skip adding invalid new broker url\", "
                       "new_url=\"%s\"", new_url.GetURL().c_str());
        continue;
      }

      if (new_url.GetHostId() == old_host_id) {
        // The new endpoint is the same as the old one, therefore we trigger
        // a reconnection only if stat of the endpoint fails - this can happen
        // when the MQ is restarted without an MGM restart.
        auto recv_channel = broker.second.first;
        XrdCl::StatInfo* stinfo {nullptr};
        auto st = recv_channel->Stat(true, stinfo);
        delete stinfo;

        if (st.IsOK()) {
          continue;
        }
      }

      eos_static_info("msg=\"refresh broker endpoint\" old_url=\"%s\" "
                      "new_url=\"%s\"", broker.first.c_str(),
                      new_url.GetURL().c_str());
      endpoint_replacements.emplace(broker.first, new_url.GetURL());
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
    auto tmp_stat =  recv_channel->Close(1);
    (void) tmp_stat; // make the compiler happy, we don't care about the resp
    mMapBrokerToChannels.erase(it_old);
    XrdCl::URL xrd_url(replace.second);
    auto ret = mMapBrokerToChannels.emplace
               (replace.second, std::make_pair(std::make_shared<XrdCl::File>(),
                   std::make_shared<XrdCl::FileSystem>(xrd_url)));

    if (!ret.second) {
      eos_static_err("msg=\"failed to create broker channels\" url=\"%s\"",
                     replace.second.c_str());
      continue;
    } else {
      eos_static_info("msg=\"successfully added new broker\" url=\"%s\"",
                      xrd_url.GetURL().c_str());
    }
  }

  // Subscribe receiving channel to the new broker
  Subscribe(false);
  mNewMqBroker = true;
}

//------------------------------------------------------------------------------
// Subscribe
//------------------------------------------------------------------------------
void
XrdMqClient::Subscribe(bool take_lock)
{
  eos::common::RWMutexReadLock rd_lock;

  if (take_lock) {
    rd_lock.Grab(mMutexMap);
  }

  for (const auto& broker : mMapBrokerToChannels) {
    std::string surl = broker.first;
    auto recv_channel = broker.second.first;

    if (recv_channel->Open(surl.c_str(), XrdCl::OpenFlags::Read).IsOK()) {
      eos_static_info("msg=\"successfully subscribed to broker\" url=\"%s\"",
                      surl.c_str());
      // Check if we were redirected to another MQ
      std::string new_hostid;

      if (!recv_channel->GetProperty("DataServer", new_hostid)) {
        eos_static_err("msg=\"failed to get DataServer for file\" url=\"%s\"",
                       surl.c_str());
        continue;
      }

      int new_port;
      std::string new_hostname;
      ParseXrdClHostId(new_hostid, new_hostname, new_port);
      XrdCl::URL old_url(surl);

      // We got redirected to a new mq
      if (old_url.GetHostId() != new_hostid) {
        eos_static_info("msg=\"got redirection to new MQ\" host_id=%s",
                        new_hostid.c_str());
        XrdCl::URL new_url(surl);
        new_url.SetHostPort(new_hostname, new_port);
        recv_channel = std::make_shared<XrdCl::File>();

        if (!recv_channel->Open(new_url.GetURL(), XrdCl::OpenFlags::Read).IsOK()) {
          eos_static_err("msg=\"failed opening file to new MQ\" url=\"%s\"",
                         new_url.GetURL().c_str());
          continue;
        }

        // Delete old broker and add a new one
        mMapBrokerToChannels.erase(surl);
        mMapBrokerToChannels.emplace(new_url.GetURL(),
                                     std::make_pair(recv_channel,
                                         std::make_shared<XrdCl::FileSystem>(new_url)));
        break;
      }
    } else {
      eos_static_err("msg=\"failed to subscribe to broker\" url=\"%s\"",
                     surl.c_str());

      if (mDefaultBrokerUrl != surl) {
        eos_static_info("msg=\"put back default broker url\" url=\%s\"",
                        mDefaultBrokerUrl.c_str());
        mMapBrokerToChannels.erase(surl);
        recv_channel = std::make_shared<XrdCl::File>();
        XrdCl::URL default_url(mDefaultBrokerUrl);
        mMapBrokerToChannels.emplace(mDefaultBrokerUrl,
                                     std::make_pair(recv_channel,
                                         std::make_shared<XrdCl::FileSystem>(default_url)));
      }

      break;
    }
  }
}

//------------------------------------------------------------------------------
// Extract hostname and port from XrdCl hostid info
//------------------------------------------------------------------------------
void
XrdMqClient::ParseXrdClHostId(const std::string& hostid, std::string& hostname,
                              int& port)
{
  port = 1097; // by default
  hostname = hostid;
  size_t pos = hostname.find('@');

  if (pos != std::string::npos) {
    hostname = hostname.substr(pos + 1);
  }

  pos = hostname.find(':');

  // Extract hostname and port
  if (pos != std::string::npos) {
    try {
      port = std::stoi(hostname.substr(pos + 1));
    } catch (...) {
      // ignore any conversion errors
    }

    hostname = hostname.substr(0, pos);
  }
}
