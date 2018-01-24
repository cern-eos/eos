// ----------------------------------------------------------------------
// File: Config.cc
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

/*----------------------------------------------------------------------------*/
#include "fst/Config.hh"
#include "common/Logging.hh"
#include <thread>
#include <chrono>

EOSFSTNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
Config Config::gConfig;
/*----------------------------------------------------------------------------*/

XrdOucString& Config::getFstNodeConfigQueue(const std::string &location) {

  while(!configQueueInitialized) {
    std::this_thread::sleep_for(std::chrono::seconds(5));
    eos_static_info("Waiting for config queue in %s ... ", location.c_str());
  }

  return FstNodeConfigQueue;
}

void Config::setFstNodeConfigQueue(const XrdOucString& value) {
  FstNodeConfigQueue = value;
  configQueueInitialized = true;
}

EOSFSTNAMESPACE_END
