// ----------------------------------------------------------------------
// File: XrdMqClient.hh
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

#ifndef __XMQCLIENT_H__
#define __XMQCLIENT_H__

#define ENOTBLK 15

#include <XrdOuc/XrdOucString.hh>
#include <XrdOuc/XrdOucHash.hh>
#include <XrdCl/XrdClFile.hh>
#include <XrdCl/XrdClFileSystem.hh>
#include <XrdClient/XrdClientEnv.hh>
#include <mq/XrdMqMessage.hh>

class XrdMqMessage;

//------------------------------------------------------------------------------
//! Class XrdMqClient
//------------------------------------------------------------------------------
class XrdMqClient
{
 public:

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  XrdMqClient(const char* clientid = 0, const char* brokerurl = 0,
              const char* defaultreceiverid = 0);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~XrdMqClient();

  //----------------------------------------------------------------------------
  //! Subscribe to a particular queue
  //!
  //! @param queue queue name
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool Subscribe(const char* queue = 0);

  //----------------------------------------------------------------------------
  //! Unsubscribe from a particular queue
  //!
  //! @param queue queue name
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool Unsubscribe(const char* queue = 0);

  //----------------------------------------------------------------------------
  //! Send message
  //!
  //! @param msg
  //! @param receiverid
  //! @param sign
  //! @param encrypt
  //! @param asynchronous
  //!
  //! @return true if message sent, otherwise false
  //----------------------------------------------------------------------------
  bool SendMessage(XrdMqMessage& msg, const char* receiverid = 0,
                   bool sign = false, bool encrypt = false,
                   bool asynchronous = false);

  //----------------------------------------------------------------------------
  //! Reply to a particular message
  //!
  //! @param replymsg
  //! @param inmsg
  //! @param sign
  //! @param encrypt
  //!
  //! @return true if message sent, otherwise false
  //----------------------------------------------------------------------------
  bool ReplyMessage(XrdMqMessage& replymsg, XrdMqMessage& inmsg,
                    bool sign = false, bool encrypt = false);


  //----------------------------------------------------------------------------
  //! Set the default receiver queue
  //!
  //! @param defqueue queue name
  //----------------------------------------------------------------------------
  inline void SetDefaultReceiverQueue(const char* defqueue)
  {
    kDefaultReceiverQueue = defqueue;
  }

  //----------------------------------------------------------------------------
  //! Get default receiver queue
  //----------------------------------------------------------------------------
  inline XrdOucString GetDefaultReceiverQueue()
  {
    return kDefaultReceiverQueue;
  }

  //----------------------------------------------------------------------------
  //! Set client id
  //!
  //! @param clientid client id to set
  //----------------------------------------------------------------------------
  inline void SetClientId(const char* clientid)
  {
    kClientId = clientid;
  }

  //----------------------------------------------------------------------------
  //! Get client id
  //----------------------------------------------------------------------------
  inline const char* GetClientId()
  {
    return kClientId.c_str();
  }

  XrdMqMessage* RecvFromInternalBuffer();

  XrdMqMessage* RecvMessage();

  XrdOucString* GetBrokerUrl(int i, XrdOucString& rhostport);

  XrdOucString GetBrokerId(int i);

  XrdCl::File* GetBrokerXrdClientReceiver(int i);

  XrdCl::FileSystem* GetBrokerXrdClientSender(int i);

  bool IsInitOK() const { return kInitOK; }

  void ReNewBrokerXrdClientReceiver(int i);

  void CheckBrokerXrdClientReceiver(int i);

  bool AddBroker(const char* brokerurl, bool advisorystatus = false,
                 bool advisoryquery = false, bool advisoryflushbacklog = false);

  void Disconnect();


  bool operator << (XrdMqMessage& msg)
  {
    return (*this).SendMessage(msg);
  }

  XrdMqMessage* operator >> (XrdMqMessage* msg)
  {
    msg = (*this).RecvMessage();
    return msg;
  }

  //----------------------------------------------------------------------------
  //! Response handler class to clean-up asynchronous callbacks which are
  //! ignored.
  //----------------------------------------------------------------------------
  class DiscardResponseHandler : public XrdCl::ResponseHandler
  {
   public:

    //--------------------------------------------------------------------------
    //! Constructor
    //--------------------------------------------------------------------------
    DiscardResponseHandler() { }

    //--------------------------------------------------------------------------
    //! Destructor
    //--------------------------------------------------------------------------
    virtual ~DiscardResponseHandler() { }

    //--------------------------------------------------------------------------
    //! Handle response method. See XrdClFile.hh class for signature.
    //--------------------------------------------------------------------------
    virtual void HandleResponse(XrdCl::XRootDStatus* status,
                                XrdCl::AnyObject* response)
    {
      XrdSysMutexHelper vLock(Lock);

      if (status)
        delete status;

      if (response)
        delete response;
    }

   private:
    XrdSysMutex Lock;
  };

  static DiscardResponseHandler gDiscardResponseHandler;

 private:
  static XrdSysMutex Mutex;
  XrdOucHash <XrdOucString> kBrokerUrls;
  XrdOucHash <XrdCl::File> kBrokerXrdClientReceiver;
  XrdOucHash <XrdCl::FileSystem> kBrokerXrdClientSender;

  XrdOucString kMessageBuffer;
  int kBrokerN;
  XrdOucString kClientId;
  XrdOucString kDefaultReceiverQueue;
  char* kRecvBuffer;
  int kRecvBufferAlloc;
  size_t kInternalBufferPosition;
  bool kInitOK;
};


#endif
