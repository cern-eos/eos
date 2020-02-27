//------------------------------------------------------------------------------
//! @file EosCryptoTimingTest.cc
//! @author: Elvin Sindrilaru - CERN
//----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2020 CERN/Switzerland                                  *
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

#include "common/SymKeys.hh"
#include "common/Timing.hh"
#include <stdio.h>

int main(int argc, char* argv[])
{
  using eos::common::SymKey;
  eos::common::Timing tm("Symmetric Enc/Dec-Timing");
  COMMONTIMING("START", &tm);
  char* secretkey = (char*) "12345678901234567890";
  XrdOucString textplain = "this is a very secret message";
  XrdOucString textencrypted = "";
  XrdOucString textdecrypted = "";

  for (int i = 0; i < 1000; i++) {
    if (!SymKey::SymmetricStringEncrypt(textplain, textencrypted,
                                        secretkey)) {
      fprintf(stderr, "error: failed symmetric string encrypt\n");
      exit(-1);
    }

    if (!SymKey::SymmetricStringDecrypt(textencrypted, textdecrypted,
                                        secretkey)) {
      fprintf(stderr, "error: failed symmetric key decrypt\n");
      exit(-1);
    }

    int inlen = strlen(secretkey);
    std::string fout;

    if (!SymKey::Base64Encode(secretkey, inlen, fout)) {
      fprintf(stderr, "error: cannot base64 encode\n");
      exit(-1);
    }

    fprintf(stdout, "%s\n", fout.c_str());
    char* binout = 0;
    ssize_t outlen;

    if (!SymKey::Base64Decode((char*)fout.c_str(), binout, outlen)) {
      fprintf(stderr, "error: cannot base64 decode\n");
      exit(-1);
    }

    binout[20] = 0;
    fprintf(stdout, "outlen is %zd - %s\n", outlen, binout);
    // printf("a) |%s|\nb) |%s|\nc) |%s|\n\n", textplain.c_str(),
    // textencrypted.c_str(), textdecrypted.c_str());
  }

  COMMONTIMING("STOP", &tm);
  tm.Print();
}
