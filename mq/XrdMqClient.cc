//          $Id: XrdMqClient.cc,v 1.00 2007/10/04 01:34:19 ajp Exp $

const char *XrdMqClientCVSID = "$Id: XrdMqClient.cc,v 1.0.0 2007/10/04 01:34:19 ajp Exp $";

#include <mq/XrdMqClient.hh>
#include <XrdNet/XrdNetDNS.hh>

/******************************************************************************/
/*                        X r d M q C l i e n t                               */
/******************************************************************************/

/*----------------------------------------------------------------------------*/
/* Subscribe                                                                  */
/*----------------------------------------------------------------------------*/
bool XrdMqClient::Subscribe(const char* queue) {
  if (queue) {
    // at the moment we support subscrition to a single queue only - queue has to be 0 !!!
    XrdMqMessage::Eroute.Emsg("Subscribe", EINVAL, "subscribe to additional user specified queue");
    return false;
  }
  for (int i=0;i< kBrokerN;i++) { 
    if (!GetBrokerXrdClientReceiver(i)->Open(0,0,false)) {
      // open failed
      continue;
    }
  }
  return true;
}

/*----------------------------------------------------------------------------*/
/* Unsubscribe                                                                */
/*----------------------------------------------------------------------------*/
bool XrdMqClient::Unsubscribe(const char* queue) {
  if (queue) {
    XrdMqMessage::Eroute.Emsg("Unubscribe", EINVAL, "unsubscribe from additional user specified queue");
    return false;
  }

  for (int i=0; i< kBrokerN;i++) {
    if (!GetBrokerXrdClientReceiver(i)->Close()) {    
      // open failed
      continue;
    }
  }
  return true;
}
  
/*----------------------------------------------------------------------------*/
/* SendMessage                                                                */
/*----------------------------------------------------------------------------*/
bool XrdMqClient::SendMessage(XrdMqMessage &msg, const char* receiverid, bool sign, bool encrypt) {
  // tag the sender
  msg.kMessageHeader.kSenderId = kClientId;
  // tag the send time
  XrdMqMessageHeader::GetTime(msg.kMessageHeader.kSenderTime_sec,msg.kMessageHeader.kSenderTime_nsec);
  // tag the receiver queue
  if (!receiverid) {
    msg.kMessageHeader.kReceiverQueue = kDefaultReceiverQueue;
  } else {
    msg.kMessageHeader.kReceiverQueue = receiverid;
  }
  if (encrypt) {
    msg.Sign(true);
  } else {
    if (sign) 
      msg.Sign(false);
    else 
      msg.Encode();
  }

  XrdOucString message = msg.kMessageHeader.kReceiverQueue;
  message += "?";
  message += msg.GetMessageBuffer();

  XrdClientAdmin* admin=0;
  //  msg.Print();
  for (int i=0 ;i< kBrokerN; i++) {
    admin = GetBrokerXrdClientSender(i);
    if (admin) {
      char result[8192]; result[0]=0;
      int  result_size=8192;
      Mutex.Lock();
      admin->Connect();
      admin->GetClientConn()->ClearLastServerError();
      admin->GetClientConn()->SetOpTimeLimit(10);
      admin->Query(kXR_Qopaquf,
                   (kXR_char *) message.c_str(),
                   (kXR_char *) result, result_size);
      if (!admin->LastServerResp()) {
        Mutex.UnLock();
        return false;
      }
      switch (admin->LastServerResp()->status) {
      case kXR_ok:
        Mutex.UnLock();
        return true;
      
      case kXR_error:
        //      admin->GetClientConn()->Disconnect(true);
        break;
        
      default:
        Mutex.UnLock();
        return true;
      }
    }
    // we continue until any of the brokers accepts the message
  }
  Mutex.UnLock();
  //  XrdMqMessage::Eroute.Emsg("SendMessage", EINVAL, "send message to all brokers");  
  if (admin) {
    XrdMqMessage::Eroute.Emsg("SendMessage", admin->LastServerError()->errnum, admin->LastServerError()->errmsg);
  } else {
    XrdMqMessage::Eroute.Emsg("SendMessage", EINVAL, "no broker available");
  }
  return false;
}

/*----------------------------------------------------------------------------*/
/* RecvMessage                                                                */
/*----------------------------------------------------------------------------*/
XrdMqMessage* XrdMqClient::RecvFromInternalBuffer() {
  if (kMessageBuffer.length()) {
    // there is still a message in the buffer
    int nextmessage;
    int firstmessage;

    firstmessage = kMessageBuffer.find(XMQHEADER);

    if (firstmessage == STR_NPOS)
      return 0;
    else {
      if (firstmessage>1)
        kMessageBuffer.erase(0,firstmessage);
    }
    
    if (kMessageBuffer.length() < (int)strlen(XMQHEADER))
      return 0;

    nextmessage = kMessageBuffer.find(XMQHEADER,strlen(XMQHEADER));
    char savec=0;
    if (nextmessage != STR_NPOS) {savec = kMessageBuffer.c_str()[nextmessage]; ((char*)kMessageBuffer.c_str())[nextmessage] = 0;}
    XrdMqMessage* message = XrdMqMessage::Create(kMessageBuffer.c_str());
    if (!message) 
      return 0;
    XrdMqMessageHeader::GetTime(message->kMessageHeader.kReceiverTime_sec,message->kMessageHeader.kReceiverTime_nsec);
    if (nextmessage != STR_NPOS) ((char*)kMessageBuffer.c_str())[nextmessage] = savec;
    if (nextmessage == STR_NPOS) {
      // last message
      kMessageBuffer="";
    } else {
      // remove one 
      kMessageBuffer.erase(0,nextmessage);
    }
    return message;
  }
  return 0;
}


XrdMqMessage* XrdMqClient::RecvMessage() {

  if (kBrokerN == 1) {
    // single broker case

    // try if there is still a buffered message
    XrdMqMessage* message;
    message = RecvFromInternalBuffer();
    if (message) return message;

    XrdClient* client = GetBrokerXrdClientReceiver(0);
    if (!client) {
      // fatal no client 
      XrdMqMessage::Eroute.Emsg("RecvMessage", EINVAL, "receive message - no client present");  
      return 0;
    }
    struct XrdClientStatInfo stinfo;

    if (!client->IsOpen()) {
      // re-open the file
      client->Open(0,0,false);
    }

    if (!client->Stat(&stinfo,true)) {
      return 0;
    }

    if (!stinfo.size) {
      return 0;
    }

    // mantain a receiver buffer which fits the need
    if (kRecvBufferAlloc < stinfo.size) {
      int allocsize = 1024*1024;
      if (stinfo.size > allocsize) {
        allocsize = stinfo.size + 1;
      }
      kRecvBuffer = (char*)realloc(kRecvBuffer,allocsize);
      if (!kRecvBuffer) {
        // this is really fatal - we exit !
        exit(-1);
      }
      kRecvBufferAlloc = allocsize;
    }
    // read all messages
    size_t nread = client->Read(kRecvBuffer, 0, stinfo.size);
    if (nread>0) {
      kRecvBuffer[nread] = 0;
      // add to the internal message buffer
      kMessageBuffer += kRecvBuffer;
    }
    return RecvFromInternalBuffer();
    // ...
  } else {
    // multiple broker case
    return 0;
  }

  return 0;
}

/*----------------------------------------------------------------------------*/
/* RegisterRecvCallback                                                       */
/*----------------------------------------------------------------------------*/
bool XrdMqClient::RegisterRecvCallback(void (*callback_func)(void *arg)) {
  return false;
}

/*----------------------------------------------------------------------------*/
/* GetBrokerUrl                                                              */
/*----------------------------------------------------------------------------*/
XrdOucString*
XrdMqClient::GetBrokerUrl(int i) {
  XrdOucString n = "";
  n+= i;
  return kBrokerUrls.Find(n.c_str());
}

/*----------------------------------------------------------------------------*/
/* GetBrokerId                                                                */
/*----------------------------------------------------------------------------*/
XrdOucString
XrdMqClient::GetBrokerId(int i) {
  XrdOucString brokern;
  if (i==0) 
    brokern = "0";
  else 
    brokern += kBrokerN;
  return brokern;
}

/*----------------------------------------------------------------------------*/
/* GetBrokerXrdClientReceiver                                                 */
/*----------------------------------------------------------------------------*/
XrdClient*
XrdMqClient::GetBrokerXrdClientReceiver(int i) {
  return kBrokerXrdClientReceiver.Find(GetBrokerId(i).c_str());
}

/*----------------------------------------------------------------------------*/
/* GetBrokerXrdClientSender                                                   */
/*----------------------------------------------------------------------------*/
XrdClientAdmin*
XrdMqClient::GetBrokerXrdClientSender(int i) {
  return kBrokerXrdClientSender.Find(GetBrokerId(i).c_str());
}

/*----------------------------------------------------------------------------*/
/* AddBroker                                                                  */
/*----------------------------------------------------------------------------*/
bool XrdMqClient::AddBroker(const char* brokerurl, bool advisorystatus, bool advisoryquery) {
  bool exists=false;
  if (!brokerurl) return false;

  XrdOucString newBrokerUrl = brokerurl;
  if ((newBrokerUrl.find("?"))==STR_NPOS) {
    newBrokerUrl+= "?";
  }

  newBrokerUrl+= "&"; newBrokerUrl += XMQCADVISORYSTATUS; newBrokerUrl+="=";newBrokerUrl += advisorystatus;
  newBrokerUrl+= "&"; newBrokerUrl += XMQCADVISORYQUERY; newBrokerUrl+="=";newBrokerUrl += advisoryquery;

  printf("==> new Broker %s\n",newBrokerUrl.c_str());
  for (int i=0;i< kBrokerN;i++) { 
    XrdOucString* brk = GetBrokerUrl(i);
    if (brk && ((*brk) == newBrokerUrl)) exists = true;
  }
  if (!exists) {
    XrdOucString brokern = GetBrokerId(kBrokerN);
    kBrokerUrls.Add(brokern.c_str(), new XrdOucString(newBrokerUrl.c_str()));
    kBrokerXrdClientSender.Add(GetBrokerId(kBrokerN).c_str(), new XrdClientAdmin(newBrokerUrl.c_str()));
    EnvPutInt(NAME_READCACHESIZE,0);
    EnvPutInt(NAME_MAXREDIRECTCOUNT,30000);
    EnvPutInt(NAME_CONNECTTIMEOUT,10);
    EnvPutInt(NAME_REQUESTTIMEOUT,300);
    kBrokerXrdClientReceiver.Add(GetBrokerId(kBrokerN).c_str(), new XrdClient(newBrokerUrl.c_str()));

    if (!GetBrokerXrdClientSender(kBrokerN)->Connect()) {
      kBrokerUrls.Del(brokern.c_str());
      kBrokerXrdClientSender.Del(GetBrokerId(kBrokerN).c_str());
      kBrokerXrdClientReceiver.Del(GetBrokerId(kBrokerN).c_str());
      XrdMqMessage::Eroute.Emsg("AddBroker", EPERM, "add and connect to broker:", newBrokerUrl.c_str());
      return false;
    }
    kBrokerN++;
  }
  return (!exists);
}

/*----------------------------------------------------------------------------*/
/* Constructor                                                                */
/*----------------------------------------------------------------------------*/
XrdMqClient::XrdMqClient(const char* clientid, const char* brokerurl, const char* defaultreceiverid) {
  kBrokerN=0;
  kMessageBuffer="";
  kRecvBuffer=0;
  kRecvBufferAlloc=0;
  if (brokerurl && (!AddBroker(brokerurl))) {
    fprintf(stderr,"error: cannot add broker %s\n", brokerurl);
  }
  if (defaultreceiverid) {
    kDefaultReceiverQueue = defaultreceiverid;
  } else {
    // default receiver is always a master
    kDefaultReceiverQueue = "/xmessage/*/master/*";
  }

  if (clientid) {
    kClientId = clientid;
    if (kClientId.beginswith("root://")) {
      // truncate the URL away
      int pos = kClientId.find("//",7);
      if (pos!=STR_NPOS) {
        kClientId.erase(0,pos+1);
      }
    }
  } else {
    // the default is to create the client id as /xmesssage/<domain>/<host>/
    XrdOucString FullName      = XrdNetDNS::getHostName();
    int ppos=0;
    XrdOucString HostName = FullName;
    XrdOucString Domain   = FullName;
    if ( (ppos = FullName.find("."))!=STR_NPOS) {
      HostName.assign(FullName, 0,ppos-1);
      Domain.assign(FullName,ppos+1);
    } else {
      Domain = "unknown";
    }

    kClientId = "/xmessage/"; kClientId += HostName; kClientId+="/"; kClientId += Domain;
  }
}

/*----------------------------------------------------------------------------*/
/* Destructor                                                                 */
/*----------------------------------------------------------------------------*/
XrdMqClient::~XrdMqClient() {
}

