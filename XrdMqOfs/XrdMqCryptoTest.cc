#define TRACE_debug 0xffff
#include <XrdMqOfs/XrdMqClient.hh>
#include <XrdMqOfs/XrdMqTiming.hh>
#include <XrdSys/XrdSysLogger.hh>
#include <stdio.h>

int main(int argc, char* argv[]) {
  if (!XrdMqMessage::Configure("xrd.mqclient.cf")) {
    fprintf(stderr, "error: cannot open client configuration file xrd.mqclient.cf\n");
    exit(-1);
  }

  XrdMqMessage message("HelloCrypto");
  message.SetBody("mqtest=testmessage12343556124368273468273468273468273468234");
  
  //  message.Print();
  printf("Signature/Encryption gave : %d\n",message.Sign(true));
  //  message.Print();
  printf("Verify/Decryption gave    : %d\n", message.Verify());
  //  message.Print();
  printf("Signature gave            : %d\n",message.Sign(false));
  //  message.Print();
  printf("Verify gave               : %d\n", message.Verify());
  //  message.Print();

  {
    XrdMqTiming mqs("SignatureTiming");
    TIMING("START", &mqs);
    for (int i=0; i< 1000; i++ ) {
      message.Sign(false);
    }
    TIMING("STOP", &mqs);
    mqs.Print();
  }
  {
    XrdMqTiming mqs("Signature/VerifyTiming");
    TIMING("START", &mqs);
    for (int i=0; i< 1000; i++ ) {
      message.Sign(false);
      message.Verify();
    }
    TIMING("STOP", &mqs);
    mqs.Print();
  }
  {
    XrdMqTiming mqs("Encryption/Decryption/Signature/Verify-Timing");
    TIMING("START", &mqs);
    for (int i=0; i< 1000; i++ ) {
      message.Sign(true);
      message.Verify();
    }
    TIMING("STOP", &mqs);
    mqs.Print();
  }

  {
    XrdMqTiming mqs("Symmetric Enc/Dec-Timing");
    TIMING("START", &mqs);
    char* secretkey=(char*) "12345678901234567890";
    XrdOucString textplain = "this is a very secret message";
    XrdOucString textencrypted="";
    XrdOucString textdecrypted="";
    for (int i=0; i< 1000; i++ ) {
      XrdMqMessage::SymmetricStringEncrypt(textplain,textencrypted,secretkey);
      XrdMqMessage::SymmetricStringDecrypt(textencrypted,textdecrypted,secretkey);
      //      printf("a) |%s|\nb) |%s|\nc) |%s|\n\n", textplain.c_str(), textencrypted.c_str(), textdecrypted.c_str());
    }
    TIMING("STOP", &mqs);
    mqs.Print();
  }

  
}
