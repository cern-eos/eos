//         $Id: XrdMqClient.hh,v 1.00 2007/10/04 01:34:19 abh Exp $
#ifndef __XMQCLIENT_H__
#define __XMQCLIENT_H__

#include <XrdOuc/XrdOucString.hh>
#include <XrdOuc/XrdOucHash.hh>
#include <XrdClient/XrdClient.hh>
#include <XrdClient/XrdClientAdmin.hh>
#include <XrdClient/XrdClientEnv.hh>
#include <XrdMqOfs/XrdMqMessage.hh>

class XrdMqClient {
private:
  XrdOucHash <XrdOucString>   kBrokerUrls;
  XrdOucHash <XrdClient>      kBrokerXrdClientReceiver;
  XrdOucHash <XrdClientAdmin> kBrokerXrdClientSender;
  
  XrdOucString                kMessageBuffer;
  int kBrokerN;
  XrdOucString                kClientId;
  XrdOucString                kDefaultReceiverQueue;

public:

  bool Subscribe(const char* queue = 0);
  bool Unsubscribe(const char* queue = 0);

  bool        SendMessage(XrdMqMessage &msg, const char* receiverid=0, bool sign=false, bool encrypt=false );
  bool        ReplyMessage(XrdMqMessage &replymsg, XrdMqMessage &inmsg, bool sign=false, bool encrypt=false ) {
    replymsg.SetReply(inmsg); return SendMessage(replymsg, inmsg.kMessageHeader.kSenderId.c_str(),sign, encrypt);
  }

  void        SetDefaultReceiverQueue(const char* defqueue) {kDefaultReceiverQueue = defqueue;}
  void        SetClientId(const char* clientid) {kClientId = clientid;}

  XrdMqMessage* RecvFromInternalBuffer();
  XrdMqMessage* RecvMessage();

  bool RegisterRecvCallback(void (*callback_func)(void *arg));
  XrdOucString* GetBrokerUrl(int i);
  XrdOucString  GetBrokerId(int i);
  XrdClient*      GetBrokerXrdClientReceiver(int i);
  XrdClientAdmin* GetBrokerXrdClientSender(int i);
 
  bool AddBroker(const char* brokerurl, bool advisorystatus=false, bool advisoryquery=false);


  XrdMqClient(const char* clientid = 0, const char* brokerurl = 0, const char* defaultreceiverid = 0);

  ~XrdMqClient();


  bool operator << (XrdMqMessage& msg) {
    return (*this).SendMessage(msg);
  }

  XrdMqMessage* operator >> (XrdMqMessage* msg) {
    msg = (*this).RecvMessage();
    return msg;
  }
};


#endif
