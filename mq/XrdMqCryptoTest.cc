// ----------------------------------------------------------------------
// File: XrdMqCryptoTest.cc
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

#define TRACE_debug 0xffff
#include <mq/XrdMqClient.hh>
#include <mq/XrdMqTiming.hh>
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
      if (!XrdMqMessage::Base64Decode(fout, binout, outlen)) {
        fprintf(stderr,"error: cannot base64 decode\n");
        exit(-1);
      }
      binout[20]=0;
      
      fprintf(stdout,"outlen is %d - %s\n", outlen, binout);
      //      printf("a) |%s|\nb) |%s|\nc) |%s|\n\n", textplain.c_str(), textencrypted.c_str(), textdecrypted.c_str());
    }
    TIMING("STOP", &mqs);
    mqs.Print();
  }

  
}
