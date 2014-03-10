//------------------------------------------------------------------------------
// File: TestEnv.hh
// Author: Elvin-Alin Sindrilaru <esindril@cern.ch> CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2014 CERN/Switzerland                                  *
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
#include <iostream>
#include <sstream>
#include <fstream>
/*----------------------------------------------------------------------------*/
#include "TestEnv.hh"
/*----------------------------------------------------------------------------*/

EOSFUSETEST_NAMESPACE_BEGIN


//------------------------------------------------------------------------------
// Constructor
//
// Notice:
// File /eos/dev/test/fuse/file1MB.dat is created as follows:
// dd if=/dev/zero count=1024 bs=1024 | tr '\000' '\001' > /eos/dev/file1MB.dat
//
// And the extended attributes on the /eos/dev/test/fuse directory are:
// sys.forced.blockchecksum="crc32c"
// sys.forced.blocksize="4k"
// sys.forced.checksum="adler"
// sys.forced.layout="replica"
// sys.forced.nstripes="2"
// sys.forced.space="default"
// 
// The directory should contain just one file.
//
//------------------------------------------------------------------------------
TestEnv::TestEnv()
{
  mMapParam.insert(std::make_pair("file_path", "/eos/dev/test/fuse/file1MB.dat"));
  mMapParam.insert(std::make_pair("file_size", "1048576")); // 1MB
  mMapParam.insert(std::make_pair("file_chksum", "eos 71e800f1"));
  mMapParam.insert(std::make_pair("file_missing", "/eos/dev/test/fuse/file_unknown.dat"));
  mMapParam.insert(std::make_pair("file_rename", "/eos/dev/test/fuse/file1MB.dat_rename"));
  mMapParam.insert(std::make_pair("dir_path", "/eos/dev/test/fuse/"));
  mMapParam.insert(std::make_pair("dir_dummy", "/eos/dev/test/fuse/dummy"));
  mMapParam.insert(std::make_pair("file_dummy", "/eos/dev/test/fuse/dummy.dat"));
  mMapParam.insert(std::make_pair("file_rename", "/eos/dev/test/fuse/file_rename.dat"));

  // Get fuse write cache size
  off_t sz_buff = 1024;
  char buff[sz_buff];
  std::string cmd = "ps aux | grep \"[e]osd \" | awk \'{print $2}\'";
  FILE* pipe = popen(cmd.c_str(), "r");

  if (!pipe)
  {
    std::cerr << "Error getting fuse cache size" << std::endl;
    exit(1);
  }

  std::string sz_cache;
  std::string result = "";

  while (!feof(pipe))
  {
    if (fgets(buff, sz_buff, pipe) != NULL)
      result += buff;
  }

  pclose(pipe);
  int eosd_pid = atoi(result.c_str());

  // Read fuse cache size value from environ file
  std::string key = "EOS_FUSE_CACHE_SIZE=";
  std::ostringstream oss;
  oss << "/proc/" << eosd_pid << "/environ";
  std::ifstream f(oss.str().c_str(), std::ios::in | std::ios::binary);

  if (!f.is_open())
  {
    std::cerr << "Error opening environ file of fuse proceess" << std::endl;
    exit(1);
  }

  while (!f.eof())
  {
    f.getline(buff, sz_buff, '\0');
    result = buff;

    if (result.find(key) == 0)
    {
      sz_cache = result.substr(key.length()).c_str();
      //std::cout << "Size cache is: " << sz_cache << std::endl;
    }
  }

  f.close();
  mMapParam.insert(std::make_pair("fuse_cache_size", sz_cache));
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

EOSFUSETEST_NAMESPACE_END
