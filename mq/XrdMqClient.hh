//------------------------------------------------------------------------------
// File: XrdMqClient.hh
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

#pragma once
#define ENOTBLK 15

#include "XrdOuc/XrdOucString.hh"
#include "XrdCl/XrdClFile.hh"
#include "XrdCl/XrdClFileSystem.hh"
#include "common/AssistedThread.hh"
#include "common/Logging.hh"
#include "mq/XrdMqMessage.hh"

//------------------------------------------------------------------------------
//! Class XrdMqClient
//------------------------------------------------------------------------------
class XrdMqClient: public eos::common::LogId
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
  //! Add broker to the list available to the current mq client
  //!
  //! @param broker_url root://host:port//path/?optional_opaque info
  //! @param advisorystatus mark advisory status
  //! @param advisoryquery mark advisory query
  //! @param advisoryflusbacklog mark advisory flush backlog
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool AddBroker(const std::string& broker_url, bool advisorystatus = false,
                 bool advisoryquery = false, bool advisoryflushbacklog = false);

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
  //! Try reading a message from the attached broker
  //!
  //! @param assistant thread assistant
  //!
  //! @return newly read message or nullptr
  //----------------------------------------------------------------------------
  XrdMqMessage* RecvMessage(ThreadAssistant* assistant = nullptr);

  //----------------------------------------------------------------------------
  //! Receive message from internal buffer
  //!
  //! @return message object or nullptr
  //----------------------------------------------------------------------------
  XrdMqMessage* RecvFromInternalBuffer();

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
  inline XrdOucString GetDefaultReceiverQueue() const
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
  inline const char* GetClientId() const
  {
    return kClientId.c_str();
  }

  //----------------------------------------------------------------------------
  //! Check if initialization (construction) was successful
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  inline bool IsInitOK() const
  {
    return kInitOK;
  }

  //----------------------------------------------------------------------------
  //! Get and reset the new mq broker flag
  //!
  //! @return true if the client was redirected to a new broker, otherwise false
  //----------------------------------------------------------------------------
  inline bool GetAndResetNewMqFlag()
  {
    return std::atomic_exchange(&mNewMqBroker, false);
  }

  //----------------------------------------------------------------------------
  //! Convenience operator to send a message
  //----------------------------------------------------------------------------
  bool operator << (XrdMqMessage& msg)
  {
    return (*this).SendMessage(msg);
  }

  //----------------------------------------------------------------------------
  //! Subscribe to all the brokers
  //----------------------------------------------------------------------------
  void Subscribe();

  //----------------------------------------------------------------------------
  //! Unsubscribe from all brokers
  //----------------------------------------------------------------------------
  void Unsubscribe();

  //----------------------------------------------------------------------------
  //! Disconenct from all the brokers by clearing the map and destroying all
  //! the in/ou-bound channels.
  //----------------------------------------------------------------------------
  void Disconnect();

private:
  //! Map of broker urls to channel objects i.e XrdCl::File object for receiving
  //! messages and XrdCl::FileSystem for sending messages
  std::map<std::string, std::pair<std::shared_ptr<XrdCl::File>,
      std::shared_ptr<XrdCl::FileSystem>>>
      mMapBrokerToChannels;
  mutable eos::common::RWMutex mMutexMap;
  static XrdSysMutex mMutexSend;
  XrdOucString kMessageBuffer;
  XrdOucString kClientId;
  XrdOucString kDefaultReceiverQueue;
  char* kRecvBuffer;
  int kRecvBufferAlloc;
  size_t kInternalBufferPosition;
  bool kInitOK;
  std::atomic<bool> mNewMqBroker {true};

  //----------------------------------------------------------------------------
  //! Refresh the in/out-bound channels to all the brokers even if we don't
  //! get any redirect
  //----------------------------------------------------------------------------
  void RefreshBrokersEndpoints();

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
    DiscardResponseHandler() = default;

    //--------------------------------------------------------------------------
    //! Destructor
    //--------------------------------------------------------------------------
    virtual ~DiscardResponseHandler() = default;

    //--------------------------------------------------------------------------
    //! Handle response method. See XrdClFile.hh class for signature.
    //--------------------------------------------------------------------------
    virtual void HandleResponse(XrdCl::XRootDStatus* status,
                                XrdCl::AnyObject* response) override
    {
      XrdSysMutexHelper vLock(Lock);

      if (status) {
        delete status;
      }

      if (response) {
        delete response;
      }
    }

  private:
    XrdSysMutex Lock;
  };

  static DiscardResponseHandler gDiscardResponseHandler;
};
