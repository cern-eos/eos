// ----------------------------------------------------------------------
// File: XrdMqMessage.cc
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

#include "mq/XrdMqMessage.hh"
#include "common/SymKeys.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include <sys/time.h>
#include <uuid/uuid.h>
#include <fcntl.h>
#include <dirent.h>
#include <stdint.h>
#include <iostream>
#include <sstream>
#include <openssl/rsa.h>
#include <openssl/objects.h>
#include <openssl/x509.h>
#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/ssl.h>
#include <openssl/evp.h>

// Add compatibility methods present in OpenSSL >= 1.1.0 if we use an older
// version of OpenSSL
#if (OPENSSL_VERSION_NUMBER < 0x10100000L) || defined (LIBRESSL_VERSION_NUMBER)
void* EVP_MD_CTX_md_data(const EVP_MD_CTX* ctx)
{
  return ctx->md_data;
}
#endif

EVP_PKEY*    XrdMqMessage::PrivateKey = 0;
XrdOucString XrdMqMessage::PublicKeyDirectory = "";
XrdOucString XrdMqMessage::PrivateKeyFile = "";
XrdOucString XrdMqMessage::PublicKeyFileHash = "";
XrdOucHash<KeyWrapper> XrdMqMessage::PublicKeyHash;
bool         XrdMqMessage::kCanSign = false;
bool         XrdMqMessage::kCanVerify = false;
XrdSysLogger* XrdMqMessage::Logger = 0;
XrdSysError  XrdMqMessage::Eroute(0);

/******************************************************************************/
/*                X r d M q M e s s a g e H e a d e r                         */
/******************************************************************************/

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
XrdMqMessageHeader::XrdMqMessageHeader():
  kMessageId(""), kReplyId(""), kSenderId(""), kBrokerId(""), kReceiverId(""),
  kSenderTime_sec(0), kSenderTime_nsec(0), kBrokerTime_sec(0),
  kBrokerTime_nsec(0), kReceiverTime_sec(0), kReceiverTime_nsec(0),
  kMessageSignature(""), kMessageDigest(""), kEncrypted(false), kType(0),
  mMsgHdrBuffer(""), kCertificateHash("")
{
}

//------------------------------------------------------------------------------
// Get header buffer
//------------------------------------------------------------------------------
const char*
XrdMqMessageHeader::GetHeaderBuffer() const
{
  return mMsgHdrBuffer.c_str();
}

//------------------------------------------------------------------------------
// GetTime
//------------------------------------------------------------------------------
void
XrdMqMessageHeader::GetTime(time_t& sec, long& nsec)
{
  struct timeval tv;
  struct timezone tz;
  gettimeofday(&tv, &tz);
  sec  = tv.tv_sec;
  nsec = tv.tv_usec * 1000;
}

//------------------------------------------------------------------------------
// Encode
//------------------------------------------------------------------------------
void
XrdMqMessageHeader::Encode()
{
  char buff[1024];
  char sep = '^';
  std::ostringstream oss;
  // TODO: check that none of this strings contains a : !
  oss << XMQHEADER << "=" << kMessageId << sep << kReplyId  << sep
      << kSenderId  << sep << kBrokerId << sep << kReceiverId << sep
      << kReceiverQueue << sep << kDescription << sep;
  sprintf(buff, "%ld", kSenderTime_sec);
  oss << buff << sep;
  sprintf(buff, "%ld", kSenderTime_nsec);
  oss << buff << sep;
  sprintf(buff, "%ld", kBrokerTime_sec);
  oss << buff << sep;
  sprintf(buff, "%ld", kBrokerTime_nsec);
  oss << buff << sep;
  sprintf(buff, "%ld", kReceiverTime_sec);
  oss << buff << sep;
  sprintf(buff, "%ld", kReceiverTime_nsec);
  oss << buff << sep;
  oss << kCertificateHash << sep << kMessageSignature << sep
      << kMessageDigest << sep << kEncrypted << sep << kType << sep;
  mMsgHdrBuffer = oss.str().c_str();
}

//------------------------------------------------------------------------------
// Decode
//------------------------------------------------------------------------------
bool
XrdMqMessageHeader::Decode(const char* str_header)
{
  XrdOucEnv decenv(str_header);
  const char* hp = 0;
  hp = decenv.Get(XMQHEADER);
  mMsgHdrBuffer = XMQHEADER;
  mMsgHdrBuffer += "=";

  if (hp) {
    mMsgHdrBuffer += hp;
  } else {
    mMsgHdrBuffer += str_header;
  }

  if (!mMsgHdrBuffer.length()) {
    return false;
  }

  char sep = '^';
  int pos  = strlen(XMQHEADER) + 1;
  int ppos = STR_NPOS;

  if ((ppos = mMsgHdrBuffer.find(sep, pos)) != STR_NPOS) {
    kMessageId.assign(mMsgHdrBuffer, pos, ppos - 1);
    pos = ppos + 1;

    if ((ppos = mMsgHdrBuffer.find(sep, pos)) != STR_NPOS) {
      kReplyId.assign(mMsgHdrBuffer, pos, ppos - 1);
      pos = ppos + 1;

      if ((ppos = mMsgHdrBuffer.find(sep, pos)) != STR_NPOS) {
        kSenderId.assign(mMsgHdrBuffer, pos, ppos - 1);
        pos = ppos + 1;

        if ((ppos = mMsgHdrBuffer.find(sep, pos)) != STR_NPOS) {
          kBrokerId.assign(mMsgHdrBuffer, pos, ppos - 1);
          pos = ppos + 1;

          if ((ppos = mMsgHdrBuffer.find(sep, pos)) != STR_NPOS) {
            kReceiverId.assign(mMsgHdrBuffer, pos, ppos - 1);
            pos = ppos + 1;

            if ((ppos = mMsgHdrBuffer.find(sep, pos)) != STR_NPOS) {
              kReceiverQueue.assign(mMsgHdrBuffer, pos, ppos - 1);
              pos = ppos + 1;

              if ((ppos = mMsgHdrBuffer.find(sep, pos)) != STR_NPOS) {
                kDescription.assign(mMsgHdrBuffer, pos, ppos - 1);
                pos = ppos + 1;
                XrdOucString tmpstring;

                if ((ppos = mMsgHdrBuffer.find(sep, pos)) != STR_NPOS) {
                  tmpstring.assign(mMsgHdrBuffer, pos, ppos - 1);
                  pos = ppos + 1;
                  kSenderTime_sec = (time_t)strtol(tmpstring.c_str(), 0, 10);

                  if ((ppos = mMsgHdrBuffer.find(sep, pos)) != STR_NPOS) {
                    tmpstring.assign(mMsgHdrBuffer, pos, ppos - 1);
                    pos = ppos + 1;
                    kSenderTime_nsec = (long)strtol(tmpstring.c_str(), 0, 10);

                    if ((ppos = mMsgHdrBuffer.find(sep, pos)) != STR_NPOS) {
                      tmpstring.assign(mMsgHdrBuffer, pos, ppos - 1);
                      pos = ppos + 1;
                      kBrokerTime_sec = (time_t)strtol(tmpstring.c_str(), 0, 10);

                      if ((ppos = mMsgHdrBuffer.find(sep, pos)) != STR_NPOS) {
                        tmpstring.assign(mMsgHdrBuffer, pos, ppos - 1);
                        pos = ppos + 1;
                        kBrokerTime_nsec = (long)strtol(tmpstring.c_str(), 0, 10);

                        if ((ppos = mMsgHdrBuffer.find(sep, pos)) != STR_NPOS) {
                          tmpstring.assign(mMsgHdrBuffer, pos, ppos - 1);
                          pos = ppos + 1;
                          kReceiverTime_sec = (time_t)strtol(tmpstring.c_str(), 0, 10);

                          if ((ppos = mMsgHdrBuffer.find(sep, pos)) != STR_NPOS) {
                            tmpstring.assign(mMsgHdrBuffer, pos, ppos - 1);
                            pos = ppos + 1;
                            kReceiverTime_nsec = (long)strtol(tmpstring.c_str(), 0, 10);

                            if ((ppos = mMsgHdrBuffer.find(sep, pos)) != STR_NPOS) {
                              kCertificateHash.assign(mMsgHdrBuffer, pos, ppos - 1);
                              pos = ppos + 1;

                              if ((ppos = mMsgHdrBuffer.find(sep, pos)) != STR_NPOS) {
                                kMessageSignature.assign(mMsgHdrBuffer, pos, ppos - 1);
                                pos = ppos + 1;

                                if ((ppos = mMsgHdrBuffer.find(sep, pos)) != STR_NPOS) {
                                  kMessageDigest.assign(mMsgHdrBuffer, pos, ppos - 1);
                                  pos = ppos + 1;

                                  if ((ppos = mMsgHdrBuffer.find(sep, pos)) != STR_NPOS) {
                                    tmpstring.assign(mMsgHdrBuffer, pos, ppos - 1);
                                    pos = ppos + 1;
                                    kEncrypted = atoi(tmpstring.c_str());

                                    if ((ppos = mMsgHdrBuffer.find(sep, pos)) != STR_NPOS) {
                                      tmpstring.assign(mMsgHdrBuffer, pos, ppos - 1);
                                      pos = ppos + 1;
                                      kType = atoi(tmpstring.c_str());
                                      return true;
                                    }
                                  }
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  return false;
}

//------------------------------------------------------------------------------
// Print
//------------------------------------------------------------------------------
void
XrdMqMessageHeader::Print()
{
  std::cerr << "-------------------------------------------------------------";
  std::cerr << std::endl;
  std::cerr << "kMessageId         : " << kMessageId << std::endl;
  std::cerr << "kReplyId           : " << kReplyId << std::endl;
  std::cerr << "kSenderId          : " << kSenderId << std::endl;
  std::cerr << "kBrokerId          : " << kBrokerId << std::endl;
  std::cerr << "kReceiverId        : " << kReceiverId << std::endl;
  std::cerr << "kReceiverQueue     : " << kReceiverQueue << std::endl;
  std::cerr << "kDescription       : " << kDescription << std::endl;
  std::cerr << "kSenderTime_sec    : " << kSenderTime_sec << std::endl;
  std::cerr << "kSenderTime_nsec   : " << kSenderTime_nsec << std::endl;
  std::cerr << "kBrokerTime_sec    : " << kBrokerTime_sec << std::endl;
  std::cerr << "kBrokerTime_nsec   : " << kBrokerTime_nsec << std::endl;
  std::cerr << "kReceiverTime_sec  : " << kReceiverTime_sec << std::endl;
  std::cerr << "kReceiverTime_nsec : " << kReceiverTime_nsec << std::endl;
  std::cerr << "kCertificateHash   : " << kCertificateHash << std::endl;
  std::cerr << "kMessageSignature  : " << kMessageSignature << std::endl;
  std::cerr << "kMessageDigest     : " << kMessageDigest << std::endl;
  std::cerr << "kEncrypted         : " << kEncrypted << std::endl;
  std::cerr << "kType              : " << kType << std::endl;
  std::cerr << "mMsgHdrBuffer      : " << mMsgHdrBuffer << std::endl;
  std::cerr << "---------------------------------------------------------------";
  std::cerr << std::endl;
}


/******************************************************************************/
/*                        X r d M q M e s s a g e                             */
/******************************************************************************/

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
XrdMqMessage::XrdMqMessage(const char* description, int type):
  kMonitor(false), errc(0)
{
  kMessageHeader.kDescription = description;
  NewId();
  kMessageHeader.kType = type;
}

//------------------------------------------------------------------------------
// Constructor - just fills the raw buffer, the Decode method has to be called
// to unpack it.
//------------------------------------------------------------------------------
XrdMqMessage::XrdMqMessage(XrdOucString& rawmessage):
  kMonitor(false), errc(0)
{
  kMessageBuffer = rawmessage;
}

//------------------------------------------------------------------------------
// Factory method
//------------------------------------------------------------------------------
XrdMqMessage*
XrdMqMessage::Create(const char* messagebuffer)
{
  XrdOucString mbuf = messagebuffer;
  XrdMqMessage* msg = new XrdMqMessage(mbuf);

  if (!msg->Decode()) {
    delete msg;
    return 0;
  } else {
    return msg;
  }
}

//------------------------------------------------------------------------------
// Generate new id
//------------------------------------------------------------------------------
void
XrdMqMessage::NewId()
{
  char uuidstring[40];
  uuid_t uuid;
  uuid_generate_time(uuid);
  uuid_unparse(uuid, uuidstring);
  kMessageHeader.kMessageId = uuidstring;
}

//------------------------------------------------------------------------------
// Configure
//------------------------------------------------------------------------------
bool
XrdMqMessage::Configure(const char* ConfigFN)
{
  char* var;
  const char* val;
  int  cfgFD;
  ERR_load_crypto_strings();

  if (!Logger) {
    // Create a logger, if there was none set before
    Logger = new XrdSysLogger();
  }

  Eroute.logger(Logger);
  XrdOucStream Config(&Eroute, "xmessage");

  if ((! ConfigFN) || (!strlen(ConfigFN))) {
    return false;
  }

  if ((cfgFD = open(ConfigFN, O_RDONLY, 0)) < 0) {
    return Eroute.Emsg("Config", errno, "open config file fn=", ConfigFN);
  }

  Config.Attach(cfgFD);

  while ((var = Config.GetMyFirstWord())) {
    if (!strncmp(var, "mq.", 3)) {
      var += 3;

      if (!strcmp("privatekeyfile", var)) {
        if ((val = Config.GetWord())) {
          PrivateKeyFile = val;
        }
      }

      if (!strcmp("publickeydirectory", var)) {
        if ((val = Config.GetWord())) {
          PublicKeyDirectory = val;
        }
      }

      if (!strcmp("publickeyfilehash", var)) {
        if ((val = Config.GetWord())) {
          PublicKeyFileHash = val;
        }
      }
    }
  }

  Config.Close();
  close(cfgFD);

  if (PrivateKeyFile.length()) {
    // Load the private key
    FILE* fp = fopen(PrivateKeyFile.c_str(), "r");

    if (fp == 0) {
      return Eroute.Emsg("Config", errno, "open private key file fn=",
                         PrivateKeyFile.c_str());
    }

    PrivateKey = PEM_read_PrivateKey(fp, 0, 0, 0);
    fclose(fp);

    if (!PrivateKey) {
      return Eroute.Emsg("Config", EINVAL, "load private key from file fn=",
                         PrivateKeyFile.c_str());
    }

    if (!PublicKeyFileHash.length()) {
      return Eroute.Emsg("Config", EINVAL, "continue - you have to provide the "
                         "hash value of the corresponding public key for your "
                         "private key [ use: openssl x509 -in <cert> -hash ]");
    }

    kCanSign = true;
  }

  if (PublicKeyDirectory.length()) {
    // Read all public keys into the public key hash
    DIR* dp;
    struct dirent* ep;
    dp = opendir(PublicKeyDirectory.c_str());

    if (dp != 0) {
      while ((ep = readdir(dp))) {
        if (!strncmp(ep->d_name, ".", 1)) {
          continue;
        }

        XrdOucString fullcertpath = PublicKeyDirectory;
        fullcertpath += "/";
        fullcertpath += (char*)ep->d_name;
        FILE* fp = fopen(fullcertpath.c_str(), "r");

        if (!fp) {
          closedir(dp);
          return Eroute.Emsg("Config", errno, "open public key file fn=",
                             fullcertpath.c_str());
        }

        X509* x509 = PEM_read_X509(fp, 0, 0, 0);
        fclose(fp);

        if (x509 == 0) {
          ERR_print_errors_fp(stderr);

          if (dp) {
            closedir(dp);
          }

          return Eroute.Emsg("Config", EINVAL, "load public key file fn=",
                             fullcertpath.c_str());
        }

        EVP_PKEY* pkey = X509_extract_key(x509);

        if (pkey == 0) {
          ERR_print_errors_fp(stderr);

          if (dp) {
            closedir(dp);
          }

          return Eroute.Emsg("Config", EINVAL, "extract public key from file fn=",
                             fullcertpath.c_str());
        }

        // add to the public key hash
        try {
          PublicKeyHash.Add(ep->d_name, new KeyWrapper(pkey));
        } catch (int& excp) {
          return Eroute.Emsg("Config", EINVAL, "insert public key in map");
        }

        X509_free(x509);
        x509 = 0;
      }

      (void) closedir(dp);
    } else {
      return Eroute.Emsg("Config", errno, "open public key directory dn=",
                         PublicKeyDirectory.c_str());
    }

    kCanVerify = true;
  }

  if (kCanSign) {
    Eroute.Say("*****> mq-client can sign messages");
    Eroute.Say("=====> mq.privatekeyfile     :     ", PrivateKeyFile.c_str(), "");
    Eroute.Say("=====> mq.publickeyhash      :     ", PublicKeyFileHash.c_str(),
               "");
  }

  if (kCanVerify) {
    Eroute.Say("*****> mq-client can verify messages");
    Eroute.Say("=====> mq.publickeydirectory :     ", PublicKeyDirectory.c_str(),
               "");
    XrdOucString nh = "";
    nh += PublicKeyHash.Num();
    Eroute.Say("=====> public keys <#>   :   :     ", nh.c_str(), "");
  }

  return 0;
}

//------------------------------------------------------------------------------
// Encode the header and the body
//------------------------------------------------------------------------------
void XrdMqMessage::Encode()
{
  kMessageHeader.Encode();
  kMessageBuffer = kMessageHeader.GetHeaderBuffer();
  kMessageBuffer += "&";
  kMessageBuffer += XMQBODY;
  kMessageBuffer += "=";
  kMessageBuffer += kMessageBody;

  if (kMonitor) {
    kMessageBuffer += "&";
    kMessageBuffer += XMQMONITOR;
    kMessageBuffer += "=1";
  }
}

//------------------------------------------------------------------------------
// Decode
//------------------------------------------------------------------------------
bool XrdMqMessage::Decode()
{
  bool decode_hdr = kMessageHeader.Decode(kMessageBuffer.c_str());
  XrdOucEnv decenv(kMessageBuffer.c_str());
  const char* hp = decenv.Get(XMQBODY);
  kMessageBody = (hp ? hp : "");
  kMonitor = (decenv.Get(XMQMONITOR) ? true : false);
  return decode_hdr;
}

//------------------------------------------------------------------------------
// RSA encrypt
//------------------------------------------------------------------------------
bool
XrdMqMessage::RSAEncrypt(char* data, ssize_t data_length, char*& encrypted_data,
                         ssize_t& encrypted_length)
{
  encrypted_data = (char*)malloc(RSA_size(EVP_PKEY_get1_RSA(PrivateKey)));

  if (!encrypted_data) {
    return false;
  }

  encrypted_length = RSA_private_encrypt(data_length, (uint_fast8_t*)data,
                                         (uint_fast8_t*)encrypted_data,
                                         EVP_PKEY_get1_RSA(PrivateKey), RSA_PKCS1_PADDING);

  if (encrypted_length < 0) {
    free(encrypted_data);
    encrypted_data = 0;
    Eroute.Emsg(__FUNCTION__, EINVAL, "encrypt with private key",
                ERR_error_string(ERR_get_error(), 0));
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// RSA key wrapper
//------------------------------------------------------------------------------
class RSAWrapper
{
public:
  RSAWrapper(RSA* r) : rsa(r) {}

  ~RSAWrapper()
  {
    RSA_free(rsa);
  }

  RSA* get()
  {
    return rsa;
  }
private:
  RSA* rsa = nullptr;
};

//------------------------------------------------------------------------------
// RSA Decrypt
//------------------------------------------------------------------------------
bool
XrdMqMessage::RSADecrypt(char* encrypted_data, ssize_t encrypted_length,
                         char*& data, ssize_t& data_length, XrdOucString& key_hash)
{
  KeyWrapper* wrapper = PublicKeyHash.Find(key_hash.c_str());
  EVP_PKEY* pkey = nullptr;

  if (wrapper) {
    pkey = wrapper->get();
  }

  if (!pkey) {
    Eroute.Emsg(__FUNCTION__, EINVAL, "load requested public key:",
                key_hash.c_str());
    return false;
  }

  RSAWrapper rsaKey(EVP_PKEY_get1_RSA(pkey));

  if ((encrypted_length != (unsigned int)RSA_size(rsaKey.get()))) {
    Eroute.Emsg(__FUNCTION__, EINVAL, "decrypt - keylength/encryption buffer"
                " mismatch");
    return false;
  }

  data = (char*)malloc(RSA_size(rsaKey.get()));

  if (!data) {
    return false;
  }

  data_length = RSA_public_decrypt(encrypted_length,
                                   (uint_fast8_t*)encrypted_data,
                                   (uint_fast8_t*)data, rsaKey.get(),
                                   RSA_PKCS1_PADDING);

  if (data_length < 0) {
    free(data);
    data = 0;
    Eroute.Emsg(__FUNCTION__, EINVAL, "decrypt with public key",
                ERR_error_string(ERR_get_error(), 0));
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Sign
//------------------------------------------------------------------------------
bool XrdMqMessage::Sign(bool encrypt)
{
  ssize_t sig_len;
  unsigned char sig_buf[16384];
  EVP_MD_CTX* md_ctx = EVP_MD_CTX_create();
  std::string b64out;
  EVP_MD_CTX_init(md_ctx);
  EVP_SignInit(md_ctx, EVP_sha1());
  EVP_SignUpdate(md_ctx, kMessageBody.c_str(), kMessageBody.length());
  sig_len = sizeof(sig_buf);

  if (!EVP_SignFinal(md_ctx, sig_buf, (unsigned int*)&sig_len, PrivateKey)) {
    EVP_MD_CTX_destroy(md_ctx);
    return false;
  }

  std::string signature;

  if (!eos::common::SymKey::Base64Encode((char*)sig_buf, sig_len, signature)) {
    EVP_MD_CTX_destroy(md_ctx);
    return false;
  }

  kMessageHeader.kMessageSignature = "rsa:";
  kMessageHeader.kMessageSignature += PublicKeyFileHash;
  kMessageHeader.kMessageSignature += ":";
  kMessageHeader.kMessageSignature += signature.c_str();

  if (!encrypt) {
    // Base64 encode the message digest
    if (!eos::common::SymKey::Base64Encode((char*)EVP_MD_CTX_md_data(md_ctx),
                                           (ssize_t)SHA_DIGEST_LENGTH, b64out)) {
      EVP_MD_CTX_destroy(md_ctx);
      return false;
    }

    kMessageHeader.kMessageDigest = b64out.c_str();
    EVP_MD_CTX_destroy(md_ctx);
    Encode();
    return true;
  }

  // RSA encode the message digest
  char* rsadigest = 0;
  ssize_t rsalen;

  if (!RSAEncrypt((char*)EVP_MD_CTX_md_data(md_ctx), SHA_DIGEST_LENGTH, rsadigest,
                  rsalen)) {
    EVP_MD_CTX_destroy(md_ctx);
    free(rsadigest);
    return false;
  }

  // Base64 encode the rsa encoded digest
  if (!eos::common::SymKey::Base64Encode(rsadigest, rsalen, b64out)) {
    EVP_MD_CTX_destroy(md_ctx);
    free(rsadigest);
    return false;
  }

  kMessageHeader.kMessageDigest = b64out.c_str();
  free(rsadigest);
  // Add a prefix with the public key rsa:<pubkey>:<encrypted64>digest
  XrdOucString sdigest = "rsa:";
  sdigest += PublicKeyFileHash;
  sdigest += ":";
  sdigest += kMessageHeader.kMessageDigest;
  kMessageHeader.kMessageDigest = sdigest;
  // Encrypt the message with the plain digest
  char* encryptptr = 0;
  ssize_t encryptlen = 0;

  if ((!eos::common::SymKey::CipherEncrypt(kMessageBody.c_str(),
       kMessageBody.length(),
       encryptptr, encryptlen,
       (char*)EVP_MD_CTX_md_data(md_ctx)))) {
    Eroute.Emsg(__FUNCTION__, EINVAL, "encrypt message");
    EVP_MD_CTX_destroy(md_ctx);
    return false;
  }

  if ((!eos::common::SymKey::Base64Encode(encryptptr, encryptlen, b64out))) {
    Eroute.Emsg(__FUNCTION__, EINVAL, "base64 encode message");
    EVP_MD_CTX_destroy(md_ctx);
    free(encryptptr);
    return false;
  }

  kMessageBody = b64out.c_str();
  kMessageHeader.kEncrypted = true;
  free(encryptptr);
  EVP_MD_CTX_destroy(md_ctx);
  Encode();
  return true;
}

//------------------------------------------------------------------------------
// Verify
//------------------------------------------------------------------------------
bool XrdMqMessage::Verify()
{
  using eos::common::SymKey;

  if (!Decode()) {
    Eroute.Emsg(__FUNCTION__, EINVAL, "decode message");
    return false;
  }

  if (kMessageHeader.kEncrypted) {
    // Decode the digest
    if (!kMessageHeader.kMessageDigest.beginswith("rsa:")) {
      Eroute.Emsg(__FUNCTION__, EINVAL,
                  "decode message digest - is not rsa encrypted");
      return false;
    }

    // Get public key
    XrdOucString PublicKeyName;
    int dpos = kMessageHeader.kMessageDigest.find(":", 4);

    if (dpos != STR_NPOS) {
      PublicKeyName.assign(kMessageHeader.kMessageDigest, 4, dpos - 1);
    } else {
      Eroute.Emsg(__FUNCTION__, EINVAL,
                  "find public key reference in message digest");
      return false;
    }

    // Truncate the key rsa:<publickeyhash> from the digest string
    kMessageHeader.kMessageDigest.erase(0, dpos + 1);
    // Base64 decode the digest string
    char* encrypteddigest = 0;
    ssize_t encrypteddigestlen = 0;
    char* decrypteddigest = 0;
    ssize_t decrypteddigestlen = 0;

    if (!eos::common::SymKey::Base64Decode((char*)
                                           kMessageHeader.kMessageDigest.c_str(),
                                           encrypteddigest, encrypteddigestlen)) {
      Eroute.Emsg(__FUNCTION__, EINVAL, "base64 decode encrypted message digest");
      free(encrypteddigest);
      return false;
    }

    if (!RSADecrypt(encrypteddigest, (unsigned int) encrypteddigestlen,
                    decrypteddigest, decrypteddigestlen, PublicKeyName)) {
      Eroute.Emsg(__FUNCTION__, EINVAL, "RSA decrypt encrypted message digest");
      free(encrypteddigest);
      free(decrypteddigest);
      return false;
    }

    if (decrypteddigestlen != SHA_DIGEST_LENGTH) {
      Eroute.Emsg(__FUNCTION__, EINVAL, "RSA decrypted message digest has illegal "
                  "length");
      free(encrypteddigest);
      free(decrypteddigest);
      return false;
    }

    // Base64 decode message body
    char* encryptedbody = 0;
    ssize_t encryptedbodylen = 0;

    if (!eos::common::SymKey::Base64Decode((char*)kMessageBody.c_str(),
                                           encryptedbody, encryptedbodylen)) {
      Eroute.Emsg(__FUNCTION__, EINVAL, "base64 decode encrypted message body");
      free(encryptedbody);
      free(encrypteddigest);
      free(decrypteddigest);
      return false;
    }

    // CIPHER decrypt message body
    char* data;
    ssize_t data_len;

    if (!eos::common::SymKey::CipherDecrypt(encryptedbody, encryptedbodylen, data,
                                            data_len, decrypteddigest)) {
      Eroute.Emsg(__FUNCTION__, EINVAL, "base64 decode encrypted message body");
      free(encryptedbody);
      free(encrypteddigest);
      free(decrypteddigest);
      return false;
    }

    kMessageBody = data;
    kMessageHeader.kEncrypted = false;
    free(encryptedbody);
    free(encrypteddigest);
    free(decrypteddigest);
  }

  // Decompose the signature
  if (!kMessageHeader.kMessageSignature.beginswith("rsa:")) {
    Eroute.Emsg(__FUNCTION__, EINVAL, "decode message signature - misses rsa: tag");
    return false;
  }

  // Get public key
  XrdOucString PublicKeyName = "";
  int dpos = kMessageHeader.kMessageSignature.find(":", 4);

  if (dpos != STR_NPOS) {
    PublicKeyName.assign(kMessageHeader.kMessageSignature, 4, dpos - 1);
  } else {
    Eroute.Emsg(__FUNCTION__, EINVAL, "find public key reference in signature");
    return false;
  }

  // Truncate the key rsa:<publickeyhash> from the digest string
  kMessageHeader.kMessageSignature.erase(0, dpos + 1);
  // Base64 decode signature
  char* sig = 0;
  ssize_t siglen = 0;

  if (!eos::common::SymKey::Base64Decode((char*)
                                         kMessageHeader.kMessageSignature.c_str(),
                                         sig, siglen)) {
    Eroute.Emsg(__FUNCTION__, EINVAL, "base64 decode message signature");
    free(sig);
    return false;
  }

  KeyWrapper* wrapper = PublicKeyHash.Find(PublicKeyName.c_str());
  EVP_PKEY* PublicKey = nullptr;

  if (wrapper) {
    PublicKey = wrapper->get();
  }

  if (!PublicKey) {
    Eroute.Emsg(__FUNCTION__, EINVAL, "load requested public key:",
                PublicKeyName.c_str());
    free(sig);
    return false;
  }

  // Verify the signature of the body
  EVP_MD_CTX* md_ctx = EVP_MD_CTX_create();
  EVP_VerifyInit(md_ctx, EVP_sha1());
  EVP_VerifyUpdate(md_ctx, kMessageBody.c_str(), kMessageBody.length());
  int retc = EVP_VerifyFinal(md_ctx, (unsigned char*) sig, siglen, PublicKey);
  EVP_MD_CTX_destroy(md_ctx);

  if (!retc) {
    Eroute.Emsg(__FUNCTION__, EPERM, "verify signature of message body",
                ERR_error_string(ERR_get_error(), 0));
    free(sig);
    return false;
  }

  free(sig);
  kMessageBuffer = "";
  kMessageHeader.kMessageSignature = "";
  kMessageHeader.kMessageDigest = "";
  kMessageHeader.kEncrypted = false;
  kMessageHeader.Encode();
  return true;
}

//------------------------------------------------------------------------------
// SetReply
//------------------------------------------------------------------------------
void
XrdMqMessage::SetReply(XrdMqMessage& message)
{
  kMessageHeader.kReplyId = message.kMessageHeader.kMessageId;
}

//------------------------------------------------------------------------------
// Print
//------------------------------------------------------------------------------
void
XrdMqMessage::Print()
{
  kMessageHeader.Print();

  if (kMessageBody.length() > 256) {
    std::cerr << "kMessageBody           : (...) too long" << std::endl;
  } else {
    std::cerr << "kMessageBody           : " << kMessageBody << std::endl;
  }

  std::cerr << "--------------------------------------------------" << std::endl;

  if (kMessageBuffer.length() > 256) {
    std::cerr << "kMessageBuffer         : (...) too long" << std::endl;
    std::cerr << "Length                 : " << kMessageBuffer.length() <<
              std::endl;
  } else {
    std::cerr << "kMessageBuffer         : " << kMessageBuffer << std::endl;
  }

  std::cerr << "--------------------------------------------------" << std::endl;
}


/******************************************************************************/
/*                X r d A d v i s o r y M q M e s s a g e                     */
/******************************************************************************/

//------------------------------------------------------------------------------
// Encode method
//------------------------------------------------------------------------------
void XrdAdvisoryMqMessage::Encode()
{
  kMessageHeader.Encode();
  std::ostringstream oss;
  oss << kMessageHeader.GetHeaderBuffer() << "&"
      << XMQADVISORYHOST << "=" << kQueue << "&"
      << XMQADVISORYSTATE << "=" << kOnline;
  kMessageBuffer = oss.str().c_str();
}

//------------------------------------------------------------------------------
// Decode method
//------------------------------------------------------------------------------
bool XrdAdvisoryMqMessage::Decode()
{
  if (!kMessageHeader.Decode(kMessageBuffer.c_str())) {
    fprintf(stderr, "Failed to decode message header\n");
    return false;
  }

  XrdOucEnv mq(kMessageBuffer.c_str());
  const char* q = mq.Get(XMQADVISORYHOST);
  const char* p = mq.Get(XMQADVISORYSTATE);

  if ((!q) || (!p)) {
    return false;
  }

  // Extract the queue which changed nad the online state
  kQueue = q;
  kOnline = atoi(p);
  return true;
}

//------------------------------------------------------------------------------
// Print
//------------------------------------------------------------------------------
void
XrdAdvisoryMqMessage::Print()
{
  XrdMqMessage::Print();
  std::cerr << "--------------------------------------------------" << std::endl;
  std::cerr << "kQueue             : " << kQueue << std::endl;
  std::cerr << "kOnline            : " << kOnline << std::endl;
}

//----------------------------------------------------------------------------
// Construction factory
//----------------------------------------------------------------------------
XrdAdvisoryMqMessage*
XrdAdvisoryMqMessage::Create(const char* messagebuffer)
{
  XrdAdvisoryMqMessage* msg = new XrdAdvisoryMqMessage();
  msg->kMessageBuffer = messagebuffer;

  if (!msg->Decode()) {
    delete msg;
    return 0;
  } else {
    return msg;
  }
}
