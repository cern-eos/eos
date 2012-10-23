//------------------------------------------------------------------------------
// File: TestHmacSha256.cc
// Author: Elvin-Alin Sindrilaru - CERN
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

//------------------------------------------------------------------------------
//! @file TestHmacSha256.cc
//! @author Elvin-Alin Sindrilaru - CERN
//! @brief Test unit for the HMAC SHA 256 implementation
//------------------------------------------------------------------------------

/*----------------------------------------------------------------------------*/
#include "common/SymKeys.hh"
/*----------------------------------------------------------------------------*/

using  eos::common::SymKey;

int main(void)
{
  std::string key = "key-to-encrypt";
  std::string data = "This is just a plain simple example to test the basic "
                     "functionality.";
  std::string expected = "e44f11c53447641d0183ecf1a2ca07d77408176a116685802432f"
                         "0dff74c2ab1";

  std::string result = SymKey::HmacSha256( key, data );  
  unsigned char* ptrResult = ( unsigned char* ) result.c_str();

  XrdOucString expectedBase64;
  XrdOucString resultBase64;
  
  std::string readableStr;
  char str[2];

  for ( unsigned int i = 0; i < result.length(); ++i, ptrResult++ ) {
    sprintf( str, "%02x", *ptrResult );
    readableStr += str;
  }

  if ( !SymKey::Base64Encode( (char*) readableStr.c_str(),
                              readableStr.length(),
                              resultBase64 ) )
  {
    fprintf( stdout, "Error while encoding the result. \n" );
    exit(-1);
  }


  if ( !SymKey::Base64Encode( (char*) expected.c_str(),
                              expected.length(),
                              expectedBase64 ) )
  {
    fprintf( stdout, "Error while expected string. \n" );
    exit(-1);
  }

  //fprintf( stdout, "Expected string is:%s \nResult string is  :%s \n",
  //         expectedBase64.c_str(), resultBase64.c_str() );
  
  if ( strncmp( expectedBase64.c_str(), resultBase64.c_str(), expectedBase64.length() ) != 0 ) {
    fprintf( stdout, "Test FAILED. \n" );
    return -1;
  }
  else {
    fprintf( stdout, "Test SUCCEEDED. \n" );
    return 0;
  }
}
