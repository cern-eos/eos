// ----------------------------------------------------------------------
// File: Config.hh
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

#pragma once
#include "fst/Namespace.hh"
#include "common/Locators.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdSys/XrdSysPthread.hh"
#include <atomic>
#include <chrono>
#include <random>
#include <mutex>

EOSFSTNAMESPACE_BEGIN

class Config
{
public:
  bool autoBoot; // -> indicates if the node tries to boot automatically or waits for a boot message from a master
  XrdOucString FstMetaLogDir; //  Directory containing the meta data log files
  XrdOucString FstAuthDir; // Directory needed for file transfers among FSTs
  XrdOucString FstOfsBrokerUrl; // Url of the message broker
  XrdOucString
  FstDefaultReceiverQueue; // Queue where we are sending to by default
  XrdOucString FstQueue; // our queue name
  XrdOucString FstQueueWildcard; // our queue match name
  XrdOucString FstGwQueueWildcard; // our gateway queue match name
  XrdOucString FstConfigQueueWildcard; // our configuration queue match name
  XrdOucString FstHostPort; // <host>:<port>
  XrdOucString FstS3Credentials; // S3 storage credentials <access>:<secret>
  XrdOucString Manager; // <host>:<port>
  XrdOucString KernelVersion; // kernel version of the host
  std::string ProtoWFEndpoint; // proto wf endpoint (typically CTA frontend)
  std::string ProtoWFResource; //  proto wf resource (typically CTA frontend)
  int PublishInterval; // Interval after which filesystem information should be published
  XrdOucString StartDate; // Time when daemon was started
  XrdOucString KeyTabAdler; // adler string of the keytab file
  mutable XrdSysMutex Mutex; // lock for dynamic updates like 'Manager'

  Config() : generator((std::random_device())())
  {
    autoBoot = false;
    PublishInterval = 10;
    Manager = "";
  }

  ~Config() = default;

  //----------------------------------------------------------------------------
  //! Get the current manager hostname and port
  //----------------------------------------------------------------------------
  std::string GetManager() const;

  //----------------------------------------------------------------------------
  //! Wait for the current manager hostname and port
  //----------------------------------------------------------------------------
  std::string WaitManager() const;

  XrdOucString getFstNodeConfigQueue(const std::string& location = "",
                                     bool blocking = true);

  common::SharedHashLocator getNodeHashLocator(const std::string& location = "",
      bool blocking = true);

  void setFstNodeConfigQueue(const std::string& value);
  std::chrono::seconds getPublishInterval();

  // Return a random number, uniformly distributed within
  // [(1/2) publishInterval, (3/2) publishInterval]
  std::chrono::milliseconds getRandomizedPublishInterval();

private:
  //! Queue holding this node's configuration settings
  std::mutex mConfigQueueMtx;
  XrdOucString FstNodeConfigQueue;
  std::atomic<bool> configQueueInitialized {false};
  eos::common::SharedHashLocator mNodeHashLocator;
  // Random number generator
  std::mutex generatorMutex;
  std::mt19937 generator;
};

extern Config gConfig;

EOSFSTNAMESPACE_END
