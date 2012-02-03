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

//         $Id: XrdMqMessage.hh,v 1.00 2007/10/04 01:34:19 abh Exp $
#ifndef __XMQMESSAGE_H__
#define __XMQMESSAGE_H__

/* xroot includes                 */
#include <XrdOuc/XrdOucString.hh>
#include <XrdOuc/XrdOucHash.hh>
#include <XrdOuc/XrdOucEnv.hh>
#include <XrdOuc/XrdOucStream.hh>
#include <XrdOuc/XrdOucTokenizer.hh>
#include <XrdSys/XrdSysError.hh>
#include <XrdSys/XrdSysPthread.hh>
#include <XrdSys/XrdSysLogger.hh>

/* openssl includes               */
#include <openssl/rsa.h>
#include <openssl/evp.h>
#include <openssl/objects.h>
#include <openssl/x509.h>
#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/ssl.h>


#define XMQHEADER "xrdmqmessage.header"
#define XMQBODY   "xrdmqmessage.body"
#define XMQMONITOR "xrdmqmessage.mon"

#define XMQADVISORYHOST "xrdmqmessage.advisoryhost"
#define XMQADVISORYSTATE "xrdmqmessage.advisorystate"

#define XMQCADVISORYSTATUS "xmqclient.advisory.status"
#define XMQCADVISORYQUERY  "xmqclient.advisory.query"
#define XMQCIPHER EVP_des_cbc


class XrdMqMessageHeader {
protected:
  XrdOucString kMessageHeaderBuffer;
  
  // we save us all the getter & setter functions
public:
  enum {kMessage=0, kStatusMessage=1, kQueryMessage=2};
  
  const char* GetHeaderBuffer() { return kMessageHeaderBuffer.c_str();}
  static void GetTime(time_t &sec, long &nsec);
  static const char* ToString(XrdOucString &s, long n)   { char tb[1024];sprintf(tb,"%ld", n); s = tb; return s.c_str();}

  XrdOucString kMessageId;     // filled by sender
  XrdOucString kReplyId;       // filled by sender
  XrdOucString kSenderId;      // filled by sender
  XrdOucString kBrokerId;      // filled by broker
  XrdOucString kReceiverId;    // filled by receiver
  XrdOucString kReceiverQueue; // filled by sender
  XrdOucString kDescription;   // filled by sender
  time_t kSenderTime_sec;      // filled by sender
  long   kSenderTime_nsec;     // filled by sender
  time_t kBrokerTime_sec;      // filled by broker
  long   kBrokerTime_nsec;     // filled by broker
  time_t kReceiverTime_sec;    // filled by receiver
  long   kReceiverTime_nsec;   // filled by receiver
  
  XrdOucString kCertificateHash;   // hash of the certificate needed to verify sender
  XrdOucString kMessageSignature;  // signature of the message body hash
  XrdOucString kMessageDigest;     // hash of the message body
  bool   kEncrypted;               // encrypted with private key or not
  int    kType;                    // type of message

  bool  Encode();
  bool  Decode(const char* headerasstring = 0);
  void  Print();


  XrdMqMessageHeader();
  virtual ~XrdMqMessageHeader();
};

class XrdMqMessage {
  
protected:

  XrdOucString kMessageBuffer;
  XrdOucString kMessageBody;
  int errc;
  XrdOucString errmsg;
  bool         kMonitor;

public:

  static const char*   Seal(XrdOucString &s) {  while (s.replace("&","#and#")) {}; return s.c_str();}
  static const char* UnSeal(XrdOucString &s) {  while (s.replace("#and#","&")) {}; return s.c_str();}
  static void Sort(XrdOucString &s, bool dosort=true);


  static bool Base64Encode(char* in, unsigned int inlen, XrdOucString &fout);
  static bool Base64Decode(XrdOucString &in, char* &out, unsigned int &outlen);

  static bool CipherEncrypt(XrdOucString &in, char* &out, unsigned int &outlen, char* key); // key length is SHA_DIGEST_LENGTH
  static bool CipherDecrypt(char* in, unsigned int inlen, XrdOucString &out, char* key);    // key length is SHA_DIGEST_LENGHT

  static bool RSAEncrypt(char* in, unsigned int len, char* &out, unsigned int &outlen);
  static bool RSADecrypt(char* in, unsigned int len, char* &out, unsigned int &outlen, XrdOucString &KeyHash);

  static bool SymmetricStringEncrypt(XrdOucString &in, XrdOucString &out, char* key); // key length is SHA_DIGEST_LENGTH
  static bool SymmetricStringDecrypt(XrdOucString &in, XrdOucString &out, char* key); // key length is SHA_DIGEST_LENGTH

  static bool kCanSign;
  static bool kCanVerify;

  XrdMqMessageHeader kMessageHeader;
 
  const char* GetMessageBuffer() { return kMessageBuffer.c_str();}

  // create a new message ID
  void NewId();

  // set reply ID
  void SetReply(XrdMqMessage &message);

  // constructor for empty message
  XrdMqMessage(const char* description ="XrdMqMessage", int type = XrdMqMessageHeader::kMessage);

  // constructor for message based on raw(wire) format
  XrdMqMessage(XrdOucString &rawmessage);

  // factory function to build a message from raw format
  static XrdMqMessage* Create(const char* rawmessage);

  // add's user message information 
  void SetBody(const char* body) { kMessageBody = body;  Seal(kMessageBody);}
  
  // retrieves user message information 
  const char* GetBody() { UnSeal(kMessageBody); return kMessageBody.c_str();}

  // mark as monitor message
  void MarkAsMonitor() { kMonitor=true;}

  // static settings and configuration
  static EVP_PKEY* PrivateKey;             // private key for signatures
  static XrdOucString PublicKeyDirectory;  // containing public keys names with hashval
  static XrdOucString PrivateKeyFile;      // name of private key file
  static XrdOucString PublicKeyFileHash;   // hash value of corresponding public key
  static XrdOucHash<EVP_PKEY> PublicKeyHash; // hash with public keys 
  static XrdSysLogger* Logger; 
  static XrdSysError  Eroute;

  // load configuration file
  static bool Configure(const char* configfile=0);
  static XrdSysMutex CryptoMutex;
  virtual ~XrdMqMessage();

  virtual bool Encode();
  virtual bool Decode();

  bool Sign(bool encrypt=false);
  bool Verify();
  virtual void Print();
};


class XrdAdvisoryMqMessage : public XrdMqMessage {
public:
  XrdOucString kQueue;
  bool kOnline;

  bool Encode();
  bool Decode();

  void Print();
  XrdAdvisoryMqMessage(const char* description ,const char* queue, bool online, int type) : XrdMqMessage(description, type) { kQueue = queue; kOnline = online;};
  XrdAdvisoryMqMessage() : XrdMqMessage() { kQueue = ""; kOnline = false;}
  static XrdAdvisoryMqMessage* Create(const char* rawmessage);
};

#endif
