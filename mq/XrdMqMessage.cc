//          $Id: XrdMqClient.cc,v 1.00 2007/10/04 01:34:19 ajp Exp $

const char *XrdMqMessageCVSID = "$Id: XrdMqMessage.cc,v 1.0.0 2007/10/04 01:34:19 ajp Exp $";

/**********************************/
/* xroot includes                 */

#include <mq/XrdMqMessage.hh>

/**********************************/
/* system includes                */

#include <sys/time.h>
#include <uuid/uuid.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <dirent.h>
#include <libgen.h>
#include <stdint.h>
#include <iostream>
#include <algorithm>
#include <vector>

void PRINTBUFFER(const char* x, unsigned int y) {
  printf("----------------------------\n");
  for (unsigned int i=0; i< y; i++) {
    printf("%03d \t",x[i]);
  }
  printf("============================\n");
}



EVP_PKEY    *XrdMqMessage::PrivateKey = 0;
XrdOucString XrdMqMessage::PublicKeyDirectory="";
XrdOucString XrdMqMessage::PrivateKeyFile="";
XrdOucString XrdMqMessage::PublicKeyFileHash="";
XrdOucHash<EVP_PKEY> XrdMqMessage::PublicKeyHash;
bool         XrdMqMessage::kCanSign=false;
bool         XrdMqMessage::kCanVerify=false;
XrdSysMutex  XrdMqMessage::CryptoMutex;
XrdSysLogger* XrdMqMessage::Logger=0;
XrdSysError  XrdMqMessage::Eroute(0);

/******************************************************************************/
/*                X r d M q M e s s a g e H e a d e r                         */
/******************************************************************************/
/*----------------------------------------------------------------------------*/
/* GetTime                                                                     */
/*----------------------------------------------------------------------------*/
void
XrdMqMessageHeader::GetTime(time_t &sec, long &nsec) {
  struct timeval tv;
  struct timezone tz;
  gettimeofday(&tv, &tz);
  sec  = tv.tv_sec;
  nsec = tv.tv_usec * 1000;
}

/*----------------------------------------------------------------------------*/
/* Encode                                                                     */
/*----------------------------------------------------------------------------*/
bool
XrdMqMessageHeader::Encode() {
  kMessageHeaderBuffer = XMQHEADER;
  kMessageHeaderBuffer += "=";
  XrdOucString ts="";

  // TODO: check that none of this strings contains a : !

  kMessageHeaderBuffer += kMessageId;     kMessageHeaderBuffer += "^";
  kMessageHeaderBuffer += kReplyId;     kMessageHeaderBuffer += "^";
  kMessageHeaderBuffer += kSenderId;      kMessageHeaderBuffer += "^";
  kMessageHeaderBuffer += kBrokerId;      kMessageHeaderBuffer += "^";
  kMessageHeaderBuffer += kReceiverId;    kMessageHeaderBuffer += "^";
  kMessageHeaderBuffer += kReceiverQueue; kMessageHeaderBuffer += "^";
  kMessageHeaderBuffer += kDescription;   kMessageHeaderBuffer += "^";
  kMessageHeaderBuffer += ToString(ts,kSenderTime_sec);kMessageHeaderBuffer += "^";
  kMessageHeaderBuffer += ToString(ts,kSenderTime_nsec);kMessageHeaderBuffer += "^";
  kMessageHeaderBuffer += ToString(ts,kBrokerTime_sec);kMessageHeaderBuffer += "^";
  kMessageHeaderBuffer += ToString(ts,kBrokerTime_nsec);kMessageHeaderBuffer += "^";
  kMessageHeaderBuffer += ToString(ts,kReceiverTime_sec);kMessageHeaderBuffer += "^";
  kMessageHeaderBuffer += ToString(ts,kReceiverTime_nsec);kMessageHeaderBuffer += "^";
  kMessageHeaderBuffer += kCertificateHash;kMessageHeaderBuffer += "^";
  kMessageHeaderBuffer += kMessageSignature; kMessageHeaderBuffer += "^";
  kMessageHeaderBuffer += kMessageDigest; kMessageHeaderBuffer += "^";
  kMessageHeaderBuffer += kEncrypted; kMessageHeaderBuffer += "^";
  kMessageHeaderBuffer += kType; kMessageHeaderBuffer += "^";
  return true;
}

/*----------------------------------------------------------------------------*/
/* Decode                                                                     */
/*----------------------------------------------------------------------------*/
bool 
XrdMqMessageHeader::Decode(const char* header) {
  // we can decode a full message extracting the query tag or decode with the query value only

  XrdOucEnv decenv(header);
  const char* hp=0;

  hp = decenv.Get(XMQHEADER);
  kMessageHeaderBuffer = XMQHEADER;
  kMessageHeaderBuffer += "=";
  if (hp)
    kMessageHeaderBuffer += hp;
  else
    kMessageHeaderBuffer += header;

  if (!kMessageHeaderBuffer.length()) {
    return false;
  }

  int pos  = strlen(XMQHEADER) + 1;
  int ppos = STR_NPOS;
  if ((ppos = kMessageHeaderBuffer.find("^",pos))!= STR_NPOS) {
    kMessageId.assign(kMessageHeaderBuffer,pos,ppos-1); pos = ppos+1;
    if ((ppos = kMessageHeaderBuffer.find("^",pos))!= STR_NPOS) {
      kReplyId.assign(kMessageHeaderBuffer,pos,ppos-1); pos = ppos+1;
      if ((ppos = kMessageHeaderBuffer.find("^",pos))!= STR_NPOS) {
        kSenderId.assign(kMessageHeaderBuffer,pos,ppos-1); pos = ppos+1;
        if ((ppos = kMessageHeaderBuffer.find("^",pos))!= STR_NPOS) {
          kBrokerId.assign(kMessageHeaderBuffer,pos,ppos-1); pos = ppos+1;
          if ((ppos = kMessageHeaderBuffer.find("^",pos))!= STR_NPOS) {
            kReceiverId.assign(kMessageHeaderBuffer,pos,ppos-1); pos = ppos+1;
            if ((ppos = kMessageHeaderBuffer.find("^",pos))!= STR_NPOS) {
              kReceiverQueue.assign(kMessageHeaderBuffer,pos,ppos-1); pos = ppos+1;
              if ((ppos = kMessageHeaderBuffer.find("^",pos))!= STR_NPOS) {
                kDescription.assign(kMessageHeaderBuffer,pos,ppos-1); pos = ppos+1;
                XrdOucString tmpstring;
                if ((ppos = kMessageHeaderBuffer.find("^",pos))!= STR_NPOS) {
                  tmpstring.assign(kMessageHeaderBuffer,pos,ppos-1); pos = ppos+1;
                  kSenderTime_sec = (time_t)strtol(tmpstring.c_str(),0,10);
                  if ((ppos = kMessageHeaderBuffer.find("^",pos))!= STR_NPOS) {
                    tmpstring.assign(kMessageHeaderBuffer,pos,ppos-1); pos = ppos+1;
                    kSenderTime_nsec = (time_t)strtol(tmpstring.c_str(),0,10);
                    if ((ppos = kMessageHeaderBuffer.find("^",pos))!= STR_NPOS) {
                      tmpstring.assign(kMessageHeaderBuffer,pos,ppos-1); pos = ppos+1;
                      kBrokerTime_sec = (time_t)strtol(tmpstring.c_str(),0,10);
                      if ((ppos = kMessageHeaderBuffer.find("^",pos))!= STR_NPOS) {
                        tmpstring.assign(kMessageHeaderBuffer,pos,ppos-1); pos = ppos+1;
                        kBrokerTime_nsec = (time_t)strtol(tmpstring.c_str(),0,10);
                        if ((ppos = kMessageHeaderBuffer.find("^",pos))!= STR_NPOS) {
                          tmpstring.assign(kMessageHeaderBuffer,pos,ppos-1); pos = ppos+1;
                          kReceiverTime_sec = (time_t)strtol(tmpstring.c_str(),0,10);
                          if ((ppos = kMessageHeaderBuffer.find("^",pos))!= STR_NPOS) {
                            tmpstring.assign(kMessageHeaderBuffer,pos,ppos-1); pos = ppos+1;
                            kReceiverTime_nsec = (time_t)strtol(tmpstring.c_str(),0,10);
                            if ((ppos = kMessageHeaderBuffer.find("^",pos))!= STR_NPOS) {
                              kCertificateHash.assign(kMessageHeaderBuffer,pos,ppos-1); pos = ppos+1;
                              if ((ppos = kMessageHeaderBuffer.find("^",pos))!= STR_NPOS) {
                                kMessageSignature.assign(kMessageHeaderBuffer,pos,ppos-1); pos = ppos+1;
                                if ((ppos = kMessageHeaderBuffer.find("^",pos))!= STR_NPOS) {
                                  kMessageDigest.assign(kMessageHeaderBuffer,pos,ppos-1); pos = ppos+1;
                                  if ((ppos = kMessageHeaderBuffer.find("^",pos))!= STR_NPOS) {
                                    tmpstring.assign(kMessageHeaderBuffer,pos,ppos-1); pos = ppos+1;
                                    kEncrypted = atoi(tmpstring.c_str());
                                    if ((ppos = kMessageHeaderBuffer.find("^",pos))!= STR_NPOS) {
                                      tmpstring.assign(kMessageHeaderBuffer,pos,ppos-1); pos = ppos+1;
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

/*----------------------------------------------------------------------------*/
/* Print                                                                      */
/*----------------------------------------------------------------------------*/
void
XrdMqMessageHeader::Print() {
  std::cerr << "-----------------------------------------------------" << std::endl;
  std::cerr << "kMessageId             : " << kMessageId << std::endl;
  std::cerr << "kReplyId               : " << kReplyId << std::endl;
  std::cerr << "kSenderId              : " << kSenderId << std::endl;
  std::cerr << "kBrokerId              : " << kBrokerId << std::endl;
  std::cerr << "kReceiverId            : " << kReceiverId << std::endl;
  std::cerr << "kReceiverQueue         : " << kReceiverQueue << std::endl;
  std::cerr << "kDescription           : " << kDescription << std::endl;
  std::cerr << "kSenderTime_sec        : " << kSenderTime_sec << std::endl;
  std::cerr << "kSenderTime_nsec       : " << kSenderTime_nsec << std::endl;
  std::cerr << "kBrokerTime_sec        : " << kBrokerTime_sec << std::endl;
  std::cerr << "kBrokerTime_nsec       : " << kBrokerTime_nsec << std::endl;
  std::cerr << "kReceiverTime_sec      : " << kReceiverTime_sec << std::endl;
  std::cerr << "kReceiverTime_nsec     : " << kReceiverTime_nsec << std::endl;
  std::cerr << "kCertificateHash       : " << kCertificateHash << std::endl;
  std::cerr << "kMessageSignature      : " << kMessageSignature << std::endl;
  std::cerr << "kMessageDigest:        : " << kMessageDigest << std::endl;
  std::cerr << "kEncrypted             : " << kEncrypted << std::endl;
  std::cerr << "kType                  : " << kType << std::endl;
  std::cerr << "kMessageHeaderBuffer   : " << kMessageHeaderBuffer << std::endl;
  std::cerr << "-----------------------------------------------------" << std::endl;
}

/*----------------------------------------------------------------------------*/
/* Constructor                                                                */
/*----------------------------------------------------------------------------*/
XrdMqMessageHeader::XrdMqMessageHeader() {
  kMessageId="";
  kReplyId="";
  kSenderId="";
  kBrokerId="";
  kReceiverId="";
  kSenderTime_sec = kBrokerTime_sec = kReceiverTime_sec = 0;
  kSenderTime_nsec = kBrokerTime_nsec = kReceiverTime_nsec = 0;
  kCertificateHash = "";
  kMessageSignature = "";
  kMessageDigest = "";
  kEncrypted = false;
  kMessageHeaderBuffer ="";
}

/*----------------------------------------------------------------------------*/
/* Destructor                                                                 */
/*----------------------------------------------------------------------------*/
XrdMqMessageHeader::~XrdMqMessageHeader() {
}




/******************************************************************************/
/*                        X r d M q M e s s a g e                             */
/******************************************************************************/

/*----------------------------------------------------------------------------*/
/* Encode                                                                     */
/*----------------------------------------------------------------------------*/
bool XrdMqMessage::Encode() {
  // this function encodes the header and the body

  kMessageHeader.Encode();
  kMessageBuffer= kMessageHeader.GetHeaderBuffer();
  kMessageBuffer+= "&";
  kMessageBuffer+= XMQBODY;
  kMessageBuffer+= "=";
  kMessageBuffer+= kMessageBody;
  if (kMonitor) {
    kMessageBuffer+="&";
    kMessageBuffer+=XMQMONITOR;
    kMessageBuffer+="=1";
  }

  return true;
}

/*----------------------------------------------------------------------------*/
/* Decode                                                                     */
/*----------------------------------------------------------------------------*/
bool XrdMqMessage::Decode() {
  bool headerdec = kMessageHeader.Decode(kMessageBuffer.c_str());

  XrdOucEnv decenv(kMessageBuffer.c_str());
  const char* hp=0;

  hp = decenv.Get(XMQBODY);
  if (hp) {
    kMessageBody = hp;
  } else {
    kMessageBody = "";
  }

  if (decenv.Get(XMQMONITOR)) {
    kMonitor=true;
  } else {
    kMonitor=false;
  }
  return headerdec;
}

/*----------------------------------------------------------------------------*/
/* Base64 Encoding                                                            */
/*----------------------------------------------------------------------------*/
bool
XrdMqMessage::Base64Encode(char* in, unsigned int inlen, XrdOucString &out) {
  BIO *bmem, *b64;
  BUF_MEM *bptr;

  /* base64 encode */
  b64 = BIO_new(BIO_f_base64());
  if (!b64) {
    Eroute.Emsg("Verify", ENOMEM, "get new BIO");
    return false;
  }

  bmem = BIO_new(BIO_s_mem());
  if (!bmem) {
    Eroute.Emsg("Verify", ENOMEM, "get new mem BIO");
    return false;
  }

  b64 = BIO_push(b64, bmem);

  BIO_write(b64, in, inlen);
  int rc = BIO_flush(b64);
  // to make gcc happy
  rc /=1;

  // retrieve size
  char* dummy;
  long size = BIO_get_mem_data(b64, &dummy);

  // retrieve buffer pointer
  BIO_get_mem_ptr(b64, &bptr);

  out.assign((char*)bptr->data,0, size-1);
  BIO_free_all(b64);
  return true;
}

/*----------------------------------------------------------------------------*/
/* Base64 Decoding                                                            */
/*----------------------------------------------------------------------------*/
bool
XrdMqMessage::Base64Decode(XrdOucString &in, char* &out, unsigned int &outlen) {
  BIO *b64, *bmem;
  b64 = BIO_new(BIO_f_base64());
  if (!b64) {
    Eroute.Emsg("Verify", ENOMEM, "get new BIO");
    return false;
  }

  unsigned int body64len = in.length();
  bmem = BIO_new_mem_buf((void*)in.c_str(), body64len);
  if (!bmem) {
    Eroute.Emsg("Verify", ENOMEM, "get new mem BIO");
    return false;
  }

  char* encryptionbuffer = (char*) malloc(body64len);
  
  bmem                = BIO_push(b64  , bmem);
  outlen = BIO_read(bmem , encryptionbuffer, body64len);
  BIO_free_all(b64);
  out = encryptionbuffer;
  return true;
}


/*----------------------------------------------------------------------------*/
/* Cipher Encrypt                                                             */
/*----------------------------------------------------------------------------*/
bool 
XrdMqMessage::CipherEncrypt(XrdOucString &in, char* &out, unsigned int &outlen, char* key) {
  // key length is SHA_DIGEST_LENGTH
  uint_fast8_t iv[EVP_MAX_IV_LENGTH+1];
  // write the initialization vector
  sprintf((char*)iv,"$KJh#(}q");

  int32_t encryptlen1=0;
  int32_t encryptlen2=0;
  

  // that is 'slow', but we really don't care here for small messages
  char* encryptptr = (char*) malloc((int)(4096 + in.length()));
  char* oencryptptr = encryptptr;

  if (!encryptptr) {
    Eroute.Emsg("CipherEncrypt",ENOMEM, "allocate encryption memory");
    return false;
  }
  
  EVP_CIPHER_CTX ctx;
  const EVP_CIPHER* cipher = XMQCIPHER();

  if (!cipher) {
    free(encryptptr);
    Eroute.Emsg("CipherEncrypt", EINVAL, "get cipher");
    return false;
  }

  EVP_CIPHER_CTX_init(&ctx);
  EVP_EncryptInit_ex(&ctx, cipher, 0, 0, 0);
  EVP_CIPHER_CTX_set_key_length(&ctx, SHA_DIGEST_LENGTH);
  EVP_EncryptInit_ex(&ctx, 0, 0, (const unsigned char*) key, iv);
  if (!(EVP_EncryptUpdate(&ctx, (uint_fast8_t*) oencryptptr, &encryptlen1, (uint_fast8_t*) in.c_str(), in.length()+1))) {
    Eroute.Emsg("CipherEncrypt", EINVAL, "update cipher block");
    return false;
  }

  if (encryptlen1>0) {
    oencryptptr += encryptlen1;
  } else {
    free(encryptptr);
    return false;
  }

  if (!(EVP_EncryptFinal(&ctx, (uint_fast8_t*) oencryptptr, &encryptlen2))) {
    Eroute.Emsg("CipherEncrypt", EINVAL, "finalize cipher block");
    return false;
  }

  if (encryptlen2>=0) {
    oencryptptr += encryptlen2;
  }

  out = encryptptr;
  outlen = (encryptlen1 + encryptlen2);
  if (outlen> (unsigned int)(4096 + in.length())) {
    Eroute.Emsg("CipherEncrypt", ENOMEM, "guarantee uncorrupted memory - memory overwrite detected");
    return false;
  }
  return true;
}

/*----------------------------------------------------------------------------*/
/* Cipher Decrypt                                                             */
/*----------------------------------------------------------------------------*/
bool 
XrdMqMessage::CipherDecrypt(char* in, unsigned int inlen, XrdOucString &out, char* key) {
  // key length is SHA_DIGEST_LENGTH
  uint_fast8_t iv[EVP_MAX_IV_LENGTH+1];
  // write the initialization vector
  sprintf((char*)iv,"$KJh#(}q");

  // that is 'slow', but we really don't care here for small messages
  char* decryptptr = (char*) malloc((int)(4096 + inlen));
  char* odecryptptr = decryptptr;

  int decryptlen1=0;
  int decryptlen2=0;

  EVP_CIPHER_CTX ctx;
  const EVP_CIPHER* cipher = XMQCIPHER();

  EVP_CIPHER_CTX_init(&ctx);
  EVP_DecryptInit_ex(&ctx, cipher, 0, 0, 0);
  EVP_CIPHER_CTX_set_key_length(&ctx, SHA_DIGEST_LENGTH);
  EVP_DecryptInit_ex(&ctx, 0, 0, (const unsigned char*) key, iv);
  
  
  if (!(EVP_DecryptUpdate(&ctx, (uint_fast8_t*) decryptptr, &decryptlen1, (uint_fast8_t*) in, inlen))) {
    Eroute.Emsg("CipherDecrypt", EINVAL, "update cipher block");
    if (decryptptr) free(decryptptr);
    return false;
  }

  if (decryptlen1>0) {
    odecryptptr += decryptlen1;
  } else {
    free(decryptptr);
    return false;
  }

  if (decryptlen2>=0) {
    odecryptptr += decryptlen2;
  }

  if ((!EVP_DecryptFinal(&ctx, (uint_fast8_t*) odecryptptr, &decryptlen2))) {
    Eroute.Emsg("CipherDecrypt", EINVAL, "finalize cipher block");
    return false;
  }

  if ((unsigned int)(decryptlen1+decryptlen2)> (4096 + inlen)) {
    Eroute.Emsg("CipherDecrypt", ENOMEM, "guarantee uncorrupted memory - memory overwrite detected");
    return false;
  }
  out.assign(decryptptr,0, decryptlen1+decryptlen2);
  free(decryptptr);
  return true;
}

/*----------------------------------------------------------------------------*/
/* RSA Encrypt                                                                */
/*----------------------------------------------------------------------------*/

bool 
XrdMqMessage::RSAEncrypt(char* in, unsigned int inlen, char* &out, unsigned int &outlen) {
  out = (char*)malloc(RSA_size(PrivateKey->pkey.rsa));
  if (!out) {
    return false;
  }

  int retc = RSA_private_encrypt(inlen, (uint_fast8_t*) in, (uint_fast8_t*) out,  PrivateKey->pkey.rsa,RSA_PKCS1_PADDING);
  if (retc<0) {
    free(out);
    out=0;
    Eroute.Emsg("RSAEncrypt", EINVAL, "encrypt with public key", ERR_error_string(ERR_get_error(),0));
    return false;
  }
  outlen = retc;

  return true;
}

/*----------------------------------------------------------------------------*/
/* RSA Decrypt                                                                */
/*----------------------------------------------------------------------------*/
bool 
XrdMqMessage::RSADecrypt(char* in, unsigned int inlen, char* &out, unsigned int &outlen, XrdOucString &KeyHash) {
  EVP_PKEY* pkey = PublicKeyHash.Find(KeyHash.c_str());
  if (!pkey) {
    Eroute.Emsg("RSADecrypt", EINVAL, "load requested public key:", KeyHash.c_str());
    return false;
  }

  if ((inlen != (unsigned int)RSA_size(pkey->pkey.rsa))) {
    Eroute.Emsg("RSADecrypt", EINVAL, "decrypt - keylength/encryption buffer mismatch");
    return false;
  }
  out = (char*)malloc(RSA_size(pkey->pkey.rsa));
  
  if (!out) {
    return false;
  }
  int retc = RSA_public_decrypt(inlen, (uint_fast8_t*) in, (uint_fast8_t*) out,pkey->pkey.rsa,RSA_PKCS1_PADDING);

  if (retc<0) {

    free(out);
    out=0;
    Eroute.Emsg("RSADecrypt", EINVAL, "decrypt with public key", ERR_error_string(ERR_get_error(),0));
    return false;
  }

  outlen = retc;
  return true;
}


/*----------------------------------------------------------------------------*/
/* SymmetricStringEncrypt                                                     */
/*----------------------------------------------------------------------------*/
bool 
XrdMqMessage::SymmetricStringEncrypt(XrdOucString &in, XrdOucString &out, char* key) {
  // key length is SHA_DIGEST_LENGTH
  unsigned int tmpbuflen;
  char* tmpbuf=0;

  if (!CipherEncrypt(in, tmpbuf, tmpbuflen, key)) {
    return false;
  }
  if (!Base64Encode(tmpbuf,tmpbuflen, out)) {
    free(tmpbuf);

    return false;
  }
  free(tmpbuf);
  return true;
}


/*----------------------------------------------------------------------------*/
/* SymmetricStringDecrypt                                                     */
/*----------------------------------------------------------------------------*/
bool 
XrdMqMessage::SymmetricStringDecrypt(XrdOucString &in, XrdOucString &out, char* key) {
  // key length is SHA_DIGEST_LENGTH
  unsigned int tmpbuflen;
  char* tmpbuf=0;

  if (!Base64Decode(in , tmpbuf,tmpbuflen)) {
    free(tmpbuf);
    return false;
  }
  

  if (!CipherDecrypt(tmpbuf, tmpbuflen,out, key)) {
    return false;
  }
  free(tmpbuf);
  return true;
}

/*----------------------------------------------------------------------------*/
/* Sign                                                                       */
/*----------------------------------------------------------------------------*/
bool XrdMqMessage::Sign(bool encrypt) {
  unsigned int sig_len;
  unsigned char sig_buf[16384];


  EVP_MD_CTX     md_ctx;
  EVP_MD_CTX_init(&md_ctx);

  EVP_SignInit   (&md_ctx, EVP_sha1());
  EVP_SignUpdate (&md_ctx, kMessageBody.c_str(), kMessageBody.length());
  sig_len = sizeof(sig_buf);

  if (!EVP_SignFinal (&md_ctx, sig_buf, &sig_len, PrivateKey)) {
    EVP_MD_CTX_cleanup(&md_ctx);
    return false;
  }

  XrdOucString Signature="";
  if (!Base64Encode((char*)sig_buf, sig_len, Signature)) {
    EVP_MD_CTX_cleanup(&md_ctx);
    return false;
  }

  kMessageHeader.kMessageSignature = "rsa:";
  kMessageHeader.kMessageSignature += PublicKeyFileHash;
  kMessageHeader.kMessageSignature += ":";
  kMessageHeader.kMessageSignature += Signature;

  if (!encrypt) {

    /* base64 encode the message digest */
    if (!Base64Encode((char*)md_ctx.md_data, SHA_DIGEST_LENGTH, kMessageHeader.kMessageDigest)) {
      EVP_MD_CTX_cleanup(&md_ctx);
      return false;
    }
    EVP_MD_CTX_cleanup(&md_ctx);
    return Encode();
  }

  /* RSA encode the message digest */
  char* rsadigest=0;
  unsigned int rsalen;
  if (!RSAEncrypt((char*)md_ctx.md_data, SHA_DIGEST_LENGTH, rsadigest, rsalen)) {
    EVP_MD_CTX_cleanup(&md_ctx);
    if (rsadigest) free (rsadigest);
    return false;
  }


  /* Base64 encode the rsa encoded digest */
  if (!Base64Encode(rsadigest, rsalen, kMessageHeader.kMessageDigest)) {
    EVP_MD_CTX_cleanup(&md_ctx);
    if (rsadigest) free (rsadigest);
    return false;
  }

  if (rsadigest) free (rsadigest);


  /* add a prefix with the public key rsa:<pubkey>:<encrypted64>digest*/
  XrdOucString sdigest = "rsa:"; sdigest += PublicKeyFileHash; sdigest += ":";
  sdigest += kMessageHeader.kMessageDigest;
  kMessageHeader.kMessageDigest = sdigest;

  /* encrypt the message with the plain digest */
  char* encryptptr;
  unsigned int encryptlen;

  if ((!CipherEncrypt(kMessageBody, encryptptr, encryptlen, (char*)md_ctx.md_data))) {
    Eroute.Emsg("Sign",EINVAL, "encrypt message");
    EVP_MD_CTX_cleanup(&md_ctx);
    return false;
  }

  if ((!Base64Encode(encryptptr,encryptlen, kMessageBody))) {
    Eroute.Emsg("Sign",EINVAL, "base64 encode message");
    EVP_MD_CTX_cleanup(&md_ctx);
    return false;
  }

  kMessageHeader.kEncrypted=true;
  

  if (encryptptr) 
    free(encryptptr);

  EVP_MD_CTX_cleanup(&md_ctx);
  return Encode();
}

/*----------------------------------------------------------------------------*/
/* Verify                                                                     */
/*----------------------------------------------------------------------------*/
bool XrdMqMessage::Verify() {
  if (!Decode()) {
    Eroute.Emsg("Verify", EINVAL, "decode message");
    return false;
  }

  if (kMessageHeader.kEncrypted) {
    // decode the digest
    if (!kMessageHeader.kMessageDigest.beginswith("rsa:")) {
      Eroute.Emsg("Verify", EINVAL, "decode message digest - is not rsa encrypted");
      return false;
    }

    // get public key
    XrdOucString PublicKeyName;
      
    int dpos = kMessageHeader.kMessageDigest.find(":",4);
    if (dpos != STR_NPOS) {
      PublicKeyName.assign(kMessageHeader.kMessageDigest, 4, dpos-1);
    } else {
      Eroute.Emsg("Verify", EINVAL, "find public key reference in message digest");
      return false;
    }

    // truncate the key rsa:<publickeyhash> from the digest string
    kMessageHeader.kMessageDigest.erase(0,dpos+1);

    // base64 decode the digest string
    char* encrypteddigest=0;
    unsigned int encrypteddigestlen=0;
    char* decrypteddigest=0;
    unsigned int decrypteddigestlen=0;
    
    if (!Base64Decode(kMessageHeader.kMessageDigest, encrypteddigest, encrypteddigestlen)) {
      Eroute.Emsg("Verify", EINVAL, "base64 decode encrypted message digest");
      if (encrypteddigest) free (encrypteddigest);
      return false;
    }
    
    if (!RSADecrypt(encrypteddigest, encrypteddigestlen, decrypteddigest, decrypteddigestlen, PublicKeyName)) {
      Eroute.Emsg("Verify", EINVAL, "RSA decrypt encrypted message digest");
      if (encrypteddigest) free (encrypteddigest);
      if (decrypteddigest) free (decrypteddigest);
      return false;
    }

    if (decrypteddigestlen != SHA_DIGEST_LENGTH) {
      Eroute.Emsg("Verify", EINVAL ,"RSA decrypted message digest has illegal length");
      if (encrypteddigest) free (encrypteddigest);
      if (decrypteddigest) free (decrypteddigest);
      return false;
    }

    // base64 decode message body
    char* encryptedbody=0;
    unsigned int encryptedbodylen=0;
    
    if (!Base64Decode(kMessageBody, encryptedbody, encryptedbodylen)) {
      Eroute.Emsg("Verify", EINVAL, "base64 decode encrypted message body");
      if (encryptedbody) free (encryptedbody);
      if (encrypteddigest) free (encrypteddigest);
      if (decrypteddigest) free (decrypteddigest);
      return false;
    }
    
    // CIPHER decrypt message body
    if (!CipherDecrypt(encryptedbody, encryptedbodylen, kMessageBody, decrypteddigest)) {
      Eroute.Emsg("Verify", EINVAL, "base64 decode encrypted message body");
      if (encryptedbody) free (encryptedbody);
      if (encrypteddigest) free (encrypteddigest);
      if (decrypteddigest) free (decrypteddigest);
      return false;
    }

    kMessageHeader.kEncrypted = 0;
    if (encryptedbody) free (encryptedbody);
    if (encrypteddigest) free (encrypteddigest);
    if (decrypteddigest) free (decrypteddigest);
  }

  // decompose the signature
  if (!kMessageHeader.kMessageSignature.beginswith("rsa:")) {
    Eroute.Emsg("Verify", EINVAL, "decode message signature - misses rsa: tag");
    return false;
  }

  // get public key
  XrdOucString PublicKeyName="";

  int dpos = kMessageHeader.kMessageSignature.find(":",4);
  if (dpos != STR_NPOS) {
    PublicKeyName.assign(kMessageHeader.kMessageSignature, 4, dpos-1);
  } else {
    Eroute.Emsg("Verify", EINVAL, "find public key reference in signature");
    return false;
  }
  
  // truncate the key rsa:<publickeyhash> from the digest string
  kMessageHeader.kMessageSignature.erase(0,dpos+1);

  // base64 decode signature
  char* sig=0;
  unsigned int siglen=0;

  if (!Base64Decode(kMessageHeader.kMessageSignature, sig, siglen)) {
    Eroute.Emsg("Verify", EINVAL, "base64 decode message signature");
    if (sig) free(sig);
    return false;
  }

  EVP_PKEY* PublicKey = PublicKeyHash.Find(PublicKeyName.c_str());
  if (!PublicKey) {
    Eroute.Emsg("Verify", EINVAL, "load requested public key:", PublicKeyName.c_str());
    if (sig) free(sig);
    return false;
  }

  // verify the signature of the body
  EVP_MD_CTX     md_ctx;
  EVP_VerifyInit   (&md_ctx, EVP_sha1());
  EVP_VerifyUpdate (&md_ctx, kMessageBody.c_str(), kMessageBody.length());
  int retc = EVP_VerifyFinal (&md_ctx, (unsigned char*) sig, siglen, PublicKey);
  EVP_MD_CTX_cleanup(&md_ctx);
  if (!retc) {
    Eroute.Emsg("Verify", EPERM, "verify signature of message body", ERR_error_string(ERR_get_error(),0));
    if (sig) free(sig);
    return false;
  }
  if (sig) free(sig);

  kMessageBuffer="";
  kMessageHeader.kMessageSignature="";
  kMessageHeader.kMessageDigest="";
  kMessageHeader.kEncrypted=false;
  kMessageHeader.Encode();
  return true;
}

void
XrdMqMessage::NewId() {
  // create message ID;
  char uuidstring[40];
  uuid_t uuid;
  uuid_generate_time(uuid);
  uuid_unparse(uuid,uuidstring);
  kMessageHeader.kMessageId = uuidstring;
}

void
XrdMqMessage::SetReply(XrdMqMessage &message) {
  kMessageHeader.kReplyId = message.kMessageHeader.kMessageId;
}

/*----------------------------------------------------------------------------*/
/* Constructor                                                                */
/*----------------------------------------------------------------------------*/
XrdMqMessage::XrdMqMessage(const char* description, int type) {
  kMessageHeader.kDescription = description;
  NewId();
  kMessageHeader.kType = type;
  kMonitor=false;
}


XrdMqMessage::XrdMqMessage(XrdOucString &rawmessage) {
  // this just fills the raw buffer, the Decode function has to be called to unpack it
  kMessageBuffer = rawmessage;
}


/*----------------------------------------------------------------------------*/
/* Construction factory                                                       */
/*----------------------------------------------------------------------------*/
XrdMqMessage*
XrdMqMessage::Create(const char* messagebuffer) {
  XrdOucString mbuf = messagebuffer;
  XrdMqMessage* msg = new XrdMqMessage(mbuf);
  if (!msg->Decode()) {
    delete msg;
    return 0;
  } else {
    return msg;
  }
} 

/*----------------------------------------------------------------------------*/
/* Destructor                                                                 */
/*----------------------------------------------------------------------------*/
XrdMqMessage::~XrdMqMessage() {
}

/*----------------------------------------------------------------------------*/
/* Print                                                                      */
/*----------------------------------------------------------------------------*/
void
XrdMqMessage::Print() {
  kMessageHeader.Print();
  if (kMessageBody.length() > 256) {
    std::cerr << "kMessageBody           : (...) too long" << std::endl;
  } else {
    std::cerr << "kMessageBody           : " << kMessageBody << std::endl;
  }
  std::cerr << "-----------------------------------------------------" << std::endl;
  if (kMessageBuffer.length() > 256) {
    std::cerr << "kMessageBuffer         : (...) too long" << std::endl;
    std::cerr << "Length                 : " << kMessageBuffer.length() << std::endl;
  } else {
    std::cerr << "kMessageBuffer         : " << kMessageBuffer << std::endl;
  }
  std::cerr << "-----------------------------------------------------" << std::endl;
}

/*----------------------------------------------------------------------------*/
/* Configure                                                                  */
/*----------------------------------------------------------------------------*/
bool 
XrdMqMessage::Configure(const char* ConfigFN) {
  char *var;
  const char *val;
  int  cfgFD;
  
  ERR_load_crypto_strings();

  if (!Logger) {
    // create a logger, if there was none set before
    Logger = new XrdSysLogger();
  }

  Eroute.logger(Logger);

  XrdOucStream Config(&Eroute,"xmessage");

  if ( (! ConfigFN) || (!strlen(ConfigFN))) 
    return false;
  
  if ( (cfgFD = open(ConfigFN, O_RDONLY, 0)) < 0)
    return Eroute.Emsg("Config", errno, "open config file fn=", ConfigFN);
  
  Config.Attach(cfgFD);

  while((var = Config.GetMyFirstWord())) {
    if (!strncmp(var, "mq.",3)) {
      var += 3;
      
      if (!strcmp("privatekeyfile",var)) {
        if ((val = Config.GetWord())) {
          PrivateKeyFile = val;
        }
      }

      if (!strcmp("publickeydirectory",var)) {
        if ((val = Config.GetWord())) {
          PublicKeyDirectory = val;
        }
      }

      if (!strcmp("publickeyfilehash",var)) {
        if ((val = Config.GetWord())) {
          PublicKeyFileHash = val;
        }
      }
    }
  }

  Config.Close();
  if (PrivateKeyFile.length()) {
    // load the private key
    FILE* fp = fopen(PrivateKeyFile.c_str(), "r");
    if (fp == 0) {
      return Eroute.Emsg("Config", errno, "open private key file fn=", PrivateKeyFile.c_str());
    }
    
    PrivateKey = PEM_read_PrivateKey(fp, 0, 0, 0);
    fclose (fp);
    
    if (!PrivateKey) {
      return Eroute.Emsg("Config", EINVAL, "load private key from file fn=", PrivateKeyFile.c_str());
    }
    if (!PublicKeyFileHash.length()) {
      return Eroute.Emsg("Config", EINVAL, "continue - you have to provide the hash value of the corresponding public key for your private key [ use: openssl x509 -in <cert> -hash ]");
    }

    kCanSign = true;
  }
  
  if (PublicKeyDirectory.length()) {
    // read all public keys into the public key hash
    DIR *dp;
    struct dirent *ep;
    
    dp = opendir (PublicKeyDirectory.c_str());
    if (dp != 0) {
      while ((ep = readdir (dp))) {
        if ( (!strncmp(ep->d_name,".",1) ) )
          continue;

        XrdOucString fullcertpath = PublicKeyDirectory;
        fullcertpath += "/";
        fullcertpath += (char*)ep->d_name;
        FILE* fp = fopen(fullcertpath.c_str(),"r");
        if (!fp) {
          closedir(dp);
          return Eroute.Emsg("Config", errno, "open public key file fn=", fullcertpath.c_str());
        }
        
        X509* x509 = PEM_read_X509(fp, 0, 0, 0);
        fclose(fp);
        
        if (x509 == 0) {
          ERR_print_errors_fp (stderr);
          if (dp)
            closedir(dp);
          return Eroute.Emsg("Config", EINVAL, "load public key file fn=", fullcertpath.c_str());
        }
        EVP_PKEY* pkey = X509_extract_key(x509);
        if (pkey == 0) {
          ERR_print_errors_fp (stderr);
          if (dp)
            closedir(dp);
          return Eroute.Emsg("Config", EINVAL, "extract public key from file fn=", fullcertpath.c_str());
        }
        // add to the public key hash
        PublicKeyHash.Add(ep->d_name, pkey);
        
        X509_free(x509);
        x509 = 0;
      }
      (void) closedir (dp);
    } else {
      return Eroute.Emsg("Config", errno, "open public key directory dn=",  PublicKeyDirectory.c_str());
    }
    kCanVerify = true;
  }

  if (kCanSign) {
    Eroute.Say("*****> mq-client can sign messages");
    Eroute.Say("=====> mq.privatekeyfile     :     ",PrivateKeyFile.c_str(),"");
    Eroute.Say("=====> mq.publickeyhash      :     ",PublicKeyFileHash.c_str(),"");
  }

  if (kCanVerify) {
    Eroute.Say("*****> mq-client can verify messages");
    Eroute.Say("=====> mq.publickeydirectory :     ",PublicKeyDirectory.c_str(),"");
    XrdOucString nh = ""; nh+= PublicKeyHash.Num();
    Eroute.Say("=====> public keys <#>   :   :     ", nh.c_str(),"");
  }
  return 0;
}


/******************************************************************************/
/*                X r d A d v i s o r y M q M e s s a g e                     */
/******************************************************************************/


/*----------------------------------------------------------------------------*/
/* Encode                                                                     */
/*----------------------------------------------------------------------------*/
bool XrdAdvisoryMqMessage::Encode() {
  // this function encodes the header only, the message encoding has to be implemented in the derived class and the derived class has FIRST to call the base class Encode function and append to kMessageBuffer the tag "xrdmqmessage.body=<....>" [ defined as XMQBODY ] ;

  kMessageHeader.Encode();
  kMessageBuffer= kMessageHeader.GetHeaderBuffer();
  kMessageBuffer+= "&";
  kMessageBuffer+= XMQADVISORYHOST;
  kMessageBuffer+= "=";
  kMessageBuffer+= kQueue;
  kMessageBuffer+= "&";
  kMessageBuffer+= XMQADVISORYSTATE;
  kMessageBuffer+= "=";
  kMessageBuffer+= kOnline;
  return true;
}

/*----------------------------------------------------------------------------*/
/* Decode                                                                     */
/*----------------------------------------------------------------------------*/
bool XrdAdvisoryMqMessage::Decode() {
  if (!kMessageHeader.Decode(kMessageBuffer.c_str())) {
    fprintf(stderr,"Failed to decode message header\n");
    return false;
  }

  XrdOucEnv mq(kMessageBuffer.c_str());
  const char* q = mq.Get(XMQADVISORYHOST);
  const char* p = mq.Get(XMQADVISORYSTATE);
  if ((!q) || (!p)) 
    return false;

  // extract the queue which changed
  kQueue = q;
  // extract the online state
  kOnline = atoi(p);
  return true;
}

/*----------------------------------------------------------------------------*/
/* Print                                                                      */
/*----------------------------------------------------------------------------*/
void
XrdAdvisoryMqMessage::Print() {
  XrdMqMessage::Print();
  std::cerr << "-----------------------------------------------------" << std::endl;
  std::cerr << "kQueue             : " << kQueue << std::endl;
  std::cerr << "kOnline            : " << kOnline << std::endl;
}

/*----------------------------------------------------------------------------*/
/* Construction factory                                                       */
/*----------------------------------------------------------------------------*/
XrdAdvisoryMqMessage*
XrdAdvisoryMqMessage::Create(const char* messagebuffer) {
  XrdOucString mbuf = messagebuffer;
  XrdAdvisoryMqMessage* msg = new XrdAdvisoryMqMessage();
  msg->kMessageBuffer = messagebuffer;
  if (!msg->Decode()) {
    delete msg;
    return 0;
  } else {
    return msg;
  }
} 

/*----------------------------------------------------------------------------*/
/* A Text Sort Function                                                       */
/*----------------------------------------------------------------------------*/

void
XrdMqMessage::Sort(XrdOucString &s, bool dosort) 
{
  if (!dosort) return;
  XrdOucString sorts="";
  std::vector<std::string> vec;
  XrdOucTokenizer linizer((char*)s.c_str());
  char* val=0;
  while ( (val = linizer.GetLine()) ) {
    vec.push_back(val);
  }
  std::sort(vec.begin(), vec.end());
  for (unsigned int i = 0; i < vec.size(); ++i) {
    sorts+=vec[i].c_str();
    sorts+="\n";
  }
  s = sorts;
}
