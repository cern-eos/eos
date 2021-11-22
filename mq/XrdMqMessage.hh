// ----------------------------------------------------------------------
// File: XrdMqMessage.hh
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

#ifndef __XMQMESSAGE_H__
#define __XMQMESSAGE_H__

#include <memory>
#include <XrdOuc/XrdOucString.hh>
#include <XrdOuc/XrdOucHash.hh>
#include <XrdOuc/XrdOucStream.hh>
#include <XrdSys/XrdSysLogger.hh>
#include <openssl/evp.h>

#define XMQHEADER                "xrdmqmessage.header"
#define XMQBODY                  "xrdmqmessage.body"
#define XMQMONITOR               "xrdmqmessage.mon"
#define XMQADVISORYHOST          "xrdmqmessage.advisoryhost"
#define XMQADVISORYSTATE         "xrdmqmessage.advisorystate"
#define XMQCADVISORYSTATUS       "xmqclient.advisory.status"
#define XMQCADVISORYQUERY        "xmqclient.advisory.query"
#define XMQCADVISORYFLUSHBACKLOG "xmqclient.advisory.flushbacklog"

//------------------------------------------------------------------------------
//! Class KeyWrapper
//------------------------------------------------------------------------------
class KeyWrapper
{
public:

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! By constructing this object, you give up ownership of the pointer!
  //----------------------------------------------------------------------------
  KeyWrapper(EVP_PKEY* key) : pkey(key) { }
  KeyWrapper() {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~KeyWrapper()
  {
    if (pkey) {
      EVP_PKEY_free(pkey);
      pkey = nullptr;
    }
  }

  EVP_PKEY* get()
  {
    return pkey;
  }

private:
  EVP_PKEY* pkey = nullptr;
};

//------------------------------------------------------------------------------
//! Class XrdMqMessageHeader
//------------------------------------------------------------------------------
class XrdMqMessageHeader
{
public:

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  XrdMqMessageHeader();

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~XrdMqMessageHeader() {};

  //----------------------------------------------------------------------------
  //! Message type
  //----------------------------------------------------------------------------
  enum {
    kMessage = 0,
    kStatusMessage = 1,
    kQueryMessage = 2
  };

  //----------------------------------------------------------------------------
  //! Get header buffer
  //----------------------------------------------------------------------------
  const char* GetHeaderBuffer() const;

  //----------------------------------------------------------------------------
  //! Encode the message header
  //----------------------------------------------------------------------------
  void Encode();

  //----------------------------------------------------------------------------
  //! Decode
  //!
  //! @param str_header header in string format
  //!
  //! @return true if successfully decoded, otherwise false
  //----------------------------------------------------------------------------
  bool Decode(const char* str_header = 0);

  //----------------------------------------------------------------------------
  //! Print message header
  //----------------------------------------------------------------------------
  void  Print();

  //----------------------------------------------------------------------------
  //! Get the current time and set the two values
  //!
  //! @param sec current time in seconds
  //! @param nsec current time in nanoseconds
  //!
  //! @todo This should be moved in a common place
  //----------------------------------------------------------------------------
  static void GetTime(time_t& sec, long& nsec);

  XrdOucString kMessageId;     ///< filled by sender
  XrdOucString kReplyId;       ///< filled by sender
  XrdOucString kSenderId;      ///< filled by sender
  XrdOucString kBrokerId;      ///< filled by broker
  XrdOucString kReceiverId;    ///< filled by receiver
  XrdOucString kReceiverQueue; ///< filled by sender
  XrdOucString kDescription;   ///< filled by sender
  time_t kSenderTime_sec;      ///< filled by sender
  long   kSenderTime_nsec;     ///< filled by sender
  time_t kBrokerTime_sec;      ///< filled by broker
  long   kBrokerTime_nsec;     ///< filled by broker
  time_t kReceiverTime_sec;    ///< filled by receiver
  long   kReceiverTime_nsec;   ///< filled by receiver

  XrdOucString kMessageSignature; ///< signature of the message body hash
  XrdOucString kMessageDigest; ///< hash of the message body
  bool kEncrypted;///< encrypted with private key or not
  int kType; ///< type of message

private:
  XrdOucString mMsgHdrBuffer; ///< message header buffer
  XrdOucString kCertificateHash; ///< certificate hash used to verify sender
};


//------------------------------------------------------------------------------
//! Class XrdMqMessage
//------------------------------------------------------------------------------
class XrdMqMessage
{
public:

  //----------------------------------------------------------------------------
  //! Constructor for empty message
  //----------------------------------------------------------------------------
  XrdMqMessage(const char* description = "XrdMqMessage",
               int type = XrdMqMessageHeader::kMessage);

  //----------------------------------------------------------------------------
  //! Constructor for message based on raw(wire) format
  //!
  //! @param rawmessage raw messsage format
  //----------------------------------------------------------------------------
  XrdMqMessage(XrdOucString& rawmessage);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~XrdMqMessage() {};

  //----------------------------------------------------------------------------
  //! Factory method to create a message from raw format
  //!
  //! @param rawmessage raw message data
  //!
  //! @return message object or 0
  //----------------------------------------------------------------------------
  static XrdMqMessage* Create(const char* rawmessage);

  //----------------------------------------------------------------------------
  //! Generate a new message id
  //----------------------------------------------------------------------------
  void NewId();

  //----------------------------------------------------------------------------
  //! Read in configuration from file
  //!
  //! @param configfile path to configuration file
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  static bool Configure(const char* configfile = 0);

  //----------------------------------------------------------------------------
  //! Encode full message
  //----------------------------------------------------------------------------
  virtual void Encode();

  //----------------------------------------------------------------------------
  //! Decode
  //!
  //! @return true if successful, othersie false
  //----------------------------------------------------------------------------
  virtual bool Decode();

  //----------------------------------------------------------------------------
  //! RSA encrypt using private key
  //!
  //! @param data input data
  //! @param data_length input data length
  //! @param encrypted_data output encrypted data. It's not necessarily null
  //!        terminated and could contain embedded nulls.
  //! @param encrypted_length output data length
  //!
  //! @return true if encryption successful, otherwise false
  //----------------------------------------------------------------------------
  static bool RSAEncrypt(char* data, ssize_t data_length,
                         char*& encrypted_data, ssize_t& encrypted_length);

  //----------------------------------------------------------------------------
  //! RSA decrypt using public key
  //!
  //! @param encrypted_data input encrypted data
  //! @param encrypted_length input data length
  //! @param data decrypted data pointer for which the caller takes ownership
  //! @param data_length length of the decrypted data
  //! @param key_hash hash of the key to be used
  //!
  //! @return true if decryption successful, otherwise false
  //----------------------------------------------------------------------------
  static bool RSADecrypt(char* encrypted_data, ssize_t encrypted_length,
                         char*& data, ssize_t& data_length, XrdOucString& key_hash);

  //----------------------------------------------------------------------------
  //! Sign message object
  //!
  //! @param encrypt if true
  //!
  //! @return true if signing successful, otherwise false
  //----------------------------------------------------------------------------
  bool Sign(bool encrypt = false);

  //----------------------------------------------------------------------------
  //! Verify message object
  //----------------------------------------------------------------------------
  bool Verify();

  //----------------------------------------------------------------------------
  //! Print contents of the message object
  //----------------------------------------------------------------------------
  virtual void Print();


  //----------------------------------------------------------------------------
  //! Get message buffer
  //----------------------------------------------------------------------------
  inline const char* GetMessageBuffer()
  {
    return kMessageBuffer.c_str();
  }

  //----------------------------------------------------------------------------
  //! Set reply ID in the header of the message
  //!
  //! @param message message holding the reply ID
  //----------------------------------------------------------------------------
  void SetReply(XrdMqMessage& message);

  //----------------------------------------------------------------------------
  //! Seal string by replacing & with the desired seal
  //!
  //! @param s input string
  //! @param seal type of seal to use
  //!
  //! @return pointer to the sealed string
  //! @toto This should be moved in a common place
  //----------------------------------------------------------------------------
  static const char* Seal(XrdOucString& s, const char* seal = "#AND#")
  {
    while (s.replace("&", seal)) {};

    return s.c_str();
  }

  //----------------------------------------------------------------------------
  //! Un-seal string
  //!
  //! @param s input string
  //! @param seal type of seal to use
  //!
  //! @return pointer to the un-sealed string
  //! @toto This should be moved in a common place
  //----------------------------------------------------------------------------
  static const char* UnSeal(XrdOucString& s, const char* seal = "#AND#")
  {
    //! @note: this is to ensure backwards compatibility with versions prior to
    //! 4.8.67 and this should be removed once we move to 4.8.68 everywhere
    const char* old_seal = "#and#";

    if (s.find(old_seal) != STR_NPOS) {
      while (s.replace(old_seal, "&")) {};
    } else {
      while (s.replace(seal, "&")) {};
    }

    return s.c_str();
  }

  //----------------------------------------------------------------------------
  //! Set message body
  //!
  //! @param body raw data
  //----------------------------------------------------------------------------
  void SetBody(const char* body)
  {
    kMessageBody = body;
    Seal(kMessageBody);
  }

  //----------------------------------------------------------------------------
  //! Get message body
  //!
  //! @return raw data
  //----------------------------------------------------------------------------
  const char* GetBody()
  {
    UnSeal(kMessageBody);
    return kMessageBody.c_str();
  }

  //----------------------------------------------------------------------------
  //! Mark as monitor message
  //----------------------------------------------------------------------------
  inline void MarkAsMonitor()
  {
    kMonitor = true;
  }

  //! @todo These two should be review as they are used only for printing info
  static bool kCanSign;
  static bool kCanVerify;

  // Static settings and configuration
  static EVP_PKEY* PrivateKey;             ///< private key for signatures
  static XrdOucString
  PublicKeyDirectory;  ///< containing public keys names with hashval
  static XrdOucString PrivateKeyFile;      ///< name of private key file
  static XrdOucString
  PublicKeyFileHash;   ///< hash value of corresponding public key
  static XrdOucHash<KeyWrapper> PublicKeyHash; ///< hash with public keys
  static XrdSysLogger* Logger; ///< logger object for error/debug info
  static XrdSysError Eroute; ///< error object for error/debug info
  XrdMqMessageHeader kMessageHeader; ///< message header

protected:

  XrdOucString kMessageBuffer;
  XrdOucString kMessageBody;
  bool kMonitor;
  int errc;
};


//------------------------------------------------------------------------------
// Class XrdAdvisoryMqMessage
//------------------------------------------------------------------------------
class XrdAdvisoryMqMessage : public XrdMqMessage
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  XrdAdvisoryMqMessage():
    XrdMqMessage(), kQueue(""), kOnline(false)
  { }

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  XrdAdvisoryMqMessage(const char* description , const char* queue,
                       bool online, int type):
    XrdMqMessage(description, type),
    kQueue(queue), kOnline(online)
  { }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~XrdAdvisoryMqMessage() {};

  //----------------------------------------------------------------------------
  //! Factory method to create advisory object from raw format
  //!
  //! @param rawmessage raw message data
  //!
  //! @return advisory mq message object or 0
  //----------------------------------------------------------------------------
  static XrdAdvisoryMqMessage* Create(const char* rawmessage);

  //----------------------------------------------------------------------------
  //! Encode message - encodes the header only, the message encoding has to be
  //! implemented in the derived class and the derived class has FIRST to call
  //! the base class. Encode function and append to kMessageBuffer the tag
  //! "xrdmqmessage.body=<....>" [ defined as XMQBODY ]
  //----------------------------------------------------------------------------
  void Encode();

  //----------------------------------------------------------------------------
  //! Decode message
  //----------------------------------------------------------------------------
  bool Decode();

  //----------------------------------------------------------------------------
  //! Print contents of the message object
  //----------------------------------------------------------------------------
  void Print();

  XrdOucString kQueue; ///< queue that changed
  bool kOnline; ///< mark online status
};

#endif
