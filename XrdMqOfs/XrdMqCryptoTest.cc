#define TRACE_debug 0xffff
#include <XrdMqOfs/XrdMqClient.hh>
#include <XrdMqOfs/XrdMqTiming.hh>
#include <XrdSys/XrdSysLogger.hh>
#include <stdio.h>

int main(int argc, char* argv[]) {
  XrdMqMessage message("HelloCrypto");
  message.SetBody("mqtest=testmessage12343556124368273468273468273468273468234");
  
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
      int inlen = strlen(secretkey);
      XrdOucString fout;
      XrdMqMessage::Base64Encode(secretkey, inlen, fout);
      fprintf(stdout,"%s\n", fout.c_str());
      char* binout =0;
      unsigned int outlen;
      XrdMqMessage::Base64Decode(fout, binout, outlen);
      binout[20]=0;
      
      fprintf(stdout,"outlen is %d - %s\n", outlen, binout);
      //      printf("a) |%s|\nb) |%s|\nc) |%s|\n\n", textplain.c_str(), textencrypted.c_str(), textdecrypted.c_str());
    }
    TIMING("STOP", &mqs);
    mqs.Print();
  }

  
}
