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

#include "RamCloudClient.hh"

EOSNSNAMESPACE_BEGIN

using namespace RAMCloud;
static std::string sRamCloudNamespace = "eosnamespace";
static thread_local* sRamCloudContext = null_ptr;
static thread_local RamCloud *sRamCloudClient = null_ptr;

//------------------------------------------------------------------------------
// Function returning a thread local RamCloud client object
//------------------------------------------------------------------------------
static RAMCloud::RamCloud*
getRamCloudClient()
{
  if (sRamCloudClient)
    return sRamCloudClient;

  // Creaate new object for the current thread
  // TODO: add configuration optinons to the client
  std::string locator = "lc:128.142.134.126:5254,188.184.150.109:5254,128.142.142.195:5254";
  sRamcCloudContext = new Context(false);
  sRamCloudClient = new RamCloud(&context, locator.c_str(), sRamCloudNamespace.c_str());
  return sRamCloudClient;
}

EOSNSNAMESPACE_END
