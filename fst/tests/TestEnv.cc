//------------------------------------------------------------------------------
// File: TestEnv.hh
// Author: Elvin Sindrilaru <esindril@cern.ch> CERN
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
// File /eos/dev/test/auth/file1MB.dat is created as follows:
// dd if=/dev/zero count=1024 bs=1024 | tr '\000' '\001' > /eos/dev/file1MB.dat
//
// And the extended attributes on the /eos/dev/test/auth directory are:
// sys.forced.checksum="adler"
// sys.forced.space="default"
//
//------------------------------------------------------------------------------
TestEnv::TestEnv()
{
  mMapParam.insert(std::make_pair("server","localhost"));
  mMapParam.insert(std::make_pair("plain_file", "/eos/dev/test/fst/plain/file32MB.dat"));
  mMapParam.insert(std::make_pair("raiddp_file", "/eos/dev/test/fst/raiddp/file32MB.dat"));
  mMapParam.insert(std::make_pair("reeds_file", "/eos/dev/test/fst/raid6/file32MB.dat"));
  mMapParam.insert(std::make_pair("file_size", "33554432")); // 32MB
  // ReadV sequences used for testing
  // 4KB read out of each MB
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
    
  // 16KB read around each MB
  mMapParam.insert(std::make_pair("off2", "1040384 2088960 3137536 4186112 5234688 "
                                  "6283264 7331840 8380416 9428992 10477568"));
  mMapParam.insert(std::make_pair("len2", "16384 16384 16384 16384 16384 16384 16384 "
                                  "16384 16384 16384"));
  // Correct responses for set 2
  mMapParam.insert(std::make_pair("off2_stripe0", "1040384 1048576 2088960 2097152 3137536"));
  mMapParam.insert(std::make_pair("len2_stripe0", "8192 8192 8192 8192 8192"));
  mMapParam.insert(std::make_pair("off2_stripe1", "0 1040384 1048576 2088960 2097152 3137536"));
  mMapParam.insert(std::make_pair("len2_stripe1", "8192 8192 8192 8192 8192 8192"));
  mMapParam.insert(std::make_pair("off2_stripe2", "0 1040384 1048576 2088960 2097152"));
  mMapParam.insert(std::make_pair("len2_stripe2", "8192 8192 8192 8192 8192"));
  mMapParam.insert(std::make_pair("off2_stripe3", "0 1040384 1048576 2088960"));
  mMapParam.insert(std::make_pair("len2_stripe3", "8192 8192 8192 8192"));

  // Test set 3
  mMapParam.insert(std::make_pair("off3", "1048576"));
  mMapParam.insert(std::make_pair("len3", "2097169"));
  // Correct responses for set 2
  mMapParam.insert(std::make_pair("off3_stripe0", ""));
  mMapParam.insert(std::make_pair("len3_stripe0", ""));
  mMapParam.insert(std::make_pair("off3_stripe1", "0"));
  mMapParam.insert(std::make_pair("len3_stripe1", "1048576"));
  mMapParam.insert(std::make_pair("off3_stripe2", "0"));
  mMapParam.insert(std::make_pair("len3_stripe2", "1048576"));
  mMapParam.insert(std::make_pair("off3_stripe3", "0"));
  mMapParam.insert(std::make_pair("len3_stripe3", "17"));
    
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
