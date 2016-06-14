/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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

#include <fstream>
#include "RamCloudClient.hh"
#include "Context.h"

EOSNSNAMESPACE_BEGIN

using namespace RAMCloud;
static std::map<std::string, std::string> sRamCloudConfigMap;
static std::string sRamCloudConfigFile;
static thread_local Context* sRamCloudContext = nullptr;
static thread_local RamCloud *sRamCloudClient = nullptr;

//------------------------------------------------------------------------------
// Function returning a thread local RamCloud client object
//------------------------------------------------------------------------------
std::map<std::string, std::string>
parseClientConfigFile(const std::string& fn)
{
  std::string line;
  std::map<std::string, std::string> map;
  std::ifstream file(fn.c_str());

  while (std::getline(file, line))
  {
    // Skip comment lines
    if (line.find('#') == 0)
      continue;

    size_t pos = line.find('=');

    // Skip lines which are not in <key>=<value> format
    if (pos == std::string::npos)
      continue;

    map.emplace(line.substr(0, pos), line.substr(pos + 1));
  }

  return map;
}

//------------------------------------------------------------------------------
// Function returning a thread local RamCloud client object
//------------------------------------------------------------------------------
RAMCloud::RamCloud*
getRamCloudClient()
{
  if (sRamCloudClient)
    return sRamCloudClient;

  if (sRamCloudConfigMap.empty())
  {
    if (sRamCloudConfigFile.empty())
    {
      sRamCloudConfigFile =  "/etc/ramcloud.client.config";
      fprintf(stderr, "Using default RAMCloud client config file: %s\n",
	      sRamCloudConfigFile.c_str());
    }

    sRamCloudConfigMap = parseClientConfigFile(sRamCloudConfigFile);

    if (sRamCloudConfigMap.size() != 3)
    {
      fprintf(stderr, "Incomplete RAMCloud configuration map\n");
      return nullptr;
    }
  }

  // Create new RAMCloud client object
  sRamCloudContext = new Context(false);
  sRamCloudContext->configFileExternalStorage =
    sRamCloudConfigMap["configFileExternalStorage"];
  sRamCloudClient = new RamCloud(sRamCloudContext,
				 sRamCloudConfigMap["externalStorage"].c_str(),
				 sRamCloudConfigMap["clusterNamespace"].c_str());
  return sRamCloudClient;
  }

//------------------------------------------------------------------------------
// Test if RAMCloud table is empty
//------------------------------------------------------------------------------
bool isEmptyTable(uint64_t table_id)
{
  RAMCloud::RamCloud* client = getRamCloudClient();
  RAMCloud::TableEnumerator iter(*client, table_id, true);

  if (iter.hasNext())
    return false;
  else
    return true;
}

EOSNSNAMESPACE_END
