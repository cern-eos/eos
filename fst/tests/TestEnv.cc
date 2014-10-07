//------------------------------------------------------------------------------
// File: TestEnv.hh
// Author: Elvin Sindrilaru <esindril@cern.ch> 
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2013 CERN/Switzerland                                  *
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

/*----------------------------------------------------------------------------*/
#include "TestEnv.hh"
/*----------------------------------------------------------------------------*/
#include <iostream>
/*----------------------------------------------------------------------------*/

EOSFSTTEST_NAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//
// Notice:
// File file32MB.dat is created as follows:
// dd if=/dev/zero count=32 bs=1M | tr '\000' '\001' > /eos/dev/test/fst/plain/file32MB.dat
//
//------------------------------------------------------------------------------
TestEnv::TestEnv()
{
  mMapParam.insert(std::make_pair("server", "localhost"));
  mMapParam.insert(std::make_pair("dummy_file", "/eos/dev/test/fst/plain/dummy.dat"));
  mMapParam.insert(std::make_pair("plain_file", "/eos/dev/test/fst/plain/file32MB.dat"));
  mMapParam.insert(std::make_pair("raiddp_file", "/eos/dev/test/fst/raiddp/file32MB.dat"));
  mMapParam.insert(std::make_pair("reeds_file", "/eos/dev/test/fst/raid6/file32MB.dat"));
  mMapParam.insert(std::make_pair("file_size", "33554432")); // 32MB

  // ReadV sequences used for testing
  // Test set 1 - 4KB read out of each MB
  mMapParam.insert(std::make_pair("off1", "0 1048576 2097152 3145728 4194304 5242880 "));
  mMapParam.insert(std::make_pair("len1", "4096 4096 4096 4096 4096 4096"));
  // Correct responses for the set 1
  mMapParam.insert(std::make_pair("off1_stripe0", "0 1048576"));
  mMapParam.insert(std::make_pair("len1_stripe0", "4096 4096"));
  mMapParam.insert(std::make_pair("off1_stripe1", "0 1048576"));
  mMapParam.insert(std::make_pair("len1_stripe1", "4096 4096"));
  mMapParam.insert(std::make_pair("off1_stripe2", "0"));
  mMapParam.insert(std::make_pair("len1_stripe2", "4096"));
  mMapParam.insert(std::make_pair("off1_stripe3", "0"));
  mMapParam.insert(std::make_pair("len1_stripe3", "4096"));

  // Test set 2 - 16KB read around each MB
  mMapParam.insert(std::make_pair("off2",
                                  "1040384 2088960 3137536 4186112 5234688 "
                                  "6283264 7331840 8380416 9428992 10477568"));
  mMapParam.insert(std::make_pair("len2",
                                  "16384 16384 16384 16384 16384 16384 16384 "
                                  "16384 16384 16384"));
  // Correct responses for set 2
  mMapParam.insert(std::make_pair("off2_stripe0",
                                  "1040384 1048576 2088960 2097152 3137536"));
  mMapParam.insert(std::make_pair("len2_stripe0", "8192 8192 8192 8192 8192"));
  mMapParam.insert(std::make_pair("off2_stripe1",
                                  "0 1040384 1048576 2088960 2097152 3137536"));
  mMapParam.insert(std::make_pair("len2_stripe1",
                                  "8192 8192 8192 8192 8192 8192"));
  mMapParam.insert(std::make_pair("off2_stripe2",
                                  "0 1040384 1048576 2088960 2097152"));
  mMapParam.insert(std::make_pair("len2_stripe2", "8192 8192 8192 8192 8192"));
  mMapParam.insert(std::make_pair("off2_stripe3", "0 1040384 1048576 2088960"));
  mMapParam.insert(std::make_pair("len2_stripe3", "8192 8192 8192 8192"));

  // Test set 3
  mMapParam.insert(std::make_pair("off3", "1048576"));
  mMapParam.insert(std::make_pair("len3", "2097169"));
  // Correct responses for set 3
  mMapParam.insert(std::make_pair("off3_stripe0", ""));
  mMapParam.insert(std::make_pair("len3_stripe0", ""));
  mMapParam.insert(std::make_pair("off3_stripe1", "0"));
  mMapParam.insert(std::make_pair("len3_stripe1", "1048576"));
  mMapParam.insert(std::make_pair("off3_stripe2", "0"));
  mMapParam.insert(std::make_pair("len3_stripe2", "1048576"));
  mMapParam.insert(std::make_pair("off3_stripe3", "0"));
  mMapParam.insert(std::make_pair("len3_stripe3", "17"));

  // Test sequences for the AlingBuffer method
  // Test set 1
  mMapParam.insert(std::make_pair("align1_off", "4095"));
  mMapParam.insert(std::make_pair("align1_len", "8194"));
  mMapParam.insert(std::make_pair("align1_resp_off", "0, 4096, 12288"));
  mMapParam.insert(std::make_pair("align1_resp_len", "4096, 8192, 4096"));
  // Test set 2
  mMapParam.insert(std::make_pair("align2_off", "4095"));
  mMapParam.insert(std::make_pair("align2_len", "1048576"));
  mMapParam.insert(std::make_pair("align2_resp_off", "0 4096 1048576"));
  mMapParam.insert(std::make_pair("align2_resp_len", "4096 1044480 4096"));
  // Test set 3
  mMapParam.insert(std::make_pair("align3_off", "4096"));
  mMapParam.insert(std::make_pair("align3_len", "1048576"));
  mMapParam.insert(std::make_pair("align3_resp_off", "4096"));
  mMapParam.insert(std::make_pair("align3_resp_len", "1048576"));

  // Test set 4
  mMapParam.insert(std::make_pair("align4_off", "20971520"));
  mMapParam.insert(std::make_pair("align4_len", "2048"));
  mMapParam.insert(std::make_pair("align4_resp_off", "20971520"));
  mMapParam.insert(std::make_pair("align4_resp_len", "4096"));

  // Test set 5
  mMapParam.insert(std::make_pair("align5_off", "20972544"));
  mMapParam.insert(std::make_pair("align5_len", "3072"));
  mMapParam.insert(std::make_pair("align5_resp_off", "20971520"));
  mMapParam.insert(std::make_pair("align5_resp_len", "4096"));

  // Test set 6
  mMapParam.insert(std::make_pair("align6_off", "20972544"));
  mMapParam.insert(std::make_pair("align6_len", "4096"));
  mMapParam.insert(std::make_pair("align6_resp_off", "20971520 20975616"));
  mMapParam.insert(std::make_pair("align6_resp_len", "4096 4096"));

  // Test set 7
  mMapParam.insert(std::make_pair("align7_off", "20972544"));
  mMapParam.insert(std::make_pair("align7_len", "9216"));
  mMapParam.insert(std::make_pair("align7_resp_off", "20971520 20975616 20979712"));
  mMapParam.insert(std::make_pair("align7_resp_len", "4096 4096 4096"));

  
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
TestEnv::~TestEnv()
{
  // empty
}


//------------------------------------------------------------------------------
// Set key value mapping
//------------------------------------------------------------------------------
void
TestEnv::SetMapping(const std::string& key, const std::string& value)
{
  auto pair_res = mMapParam.insert(std::make_pair(key, value));

  if (!pair_res.second)
  {
    std::cerr << "Mapping already exists, key=" << key
              << " value=" << value << std::endl;
  }
}


//------------------------------------------------------------------------------
// Get key value mapping
//------------------------------------------------------------------------------
std::string
TestEnv::GetMapping(const std::string& key) const
{
  auto iter = mMapParam.find(key);

  if (iter != mMapParam.end())
    return iter->second;
  else
    return std::string("");
}

EOSFSTTEST_NAMESPACE_END
