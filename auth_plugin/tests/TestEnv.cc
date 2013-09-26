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

EOSAUTHTEST_NAMESPACE_BEGIN

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
  mMapParam.insert(std::make_pair("server","localhost:1099"));
  mMapParam.insert(std::make_pair("file_path", "/eos/dev/test/auth/file1MB.dat"));
  mMapParam.insert(std::make_pair("file_size", "1048576")); // 1MB
  mMapParam.insert(std::make_pair("file_chksum", "eos 71e800f1")); 
  mMapParam.insert(std::make_pair("file_missing", "/eos/dev/test/auth/file_unknown.dat"));
  mMapParam.insert(std::make_pair("file_rename", "/eos/dev/test/auth/file1MB.dat_rename"));
  mMapParam.insert(std::make_pair("dir_name", "/eos/dev/test/auth/"));
  mMapParam.insert(std::make_pair("dir_new", "/eos/dev/test/auth/dummy"));  
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

EOSAUTHTEST_NAMESPACE_END
