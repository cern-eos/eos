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

#ifndef __EOSFST_CONFIG_HH__
#define __EOSFST_CONFIG_HH__
/*----------------------------------------------------------------------------*/
#include "fst/Namespace.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdSys/XrdSysPthread.hh"

/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

class Config
{
public:
  bool autoBoot; // -> indicates if the node tries to boot automatically or waits for a boot message from a master
  XrdOucString FstMetaLogDir; //  Directory containing the meta data log files
  XrdOucString FstOfsBrokerUrl; // Url of the message broker
  XrdOucString FstDefaultReceiverQueue; // Queue where we are sending to by default
  XrdOucString FstQueue; // our queue name
  XrdOucString FstQueueWildcard; // our queue match name
  XrdOucString FstGwQueueWildcard; // our gateway queue match name
  XrdOucString FstConfigQueueWildcard; // our configuration queue match name
  XrdOucString FstNodeConfigQueue; // our queue holding this node's configuration settings
  XrdOucString FstHostPort; // <host>:<port>
  XrdOucString Manager; // <host>:<port> 
  XrdOucString KernelVersion; // kernel version of the host
  int PublishInterval; // Interval after which filesystem information should be published
  XrdOucString StartDate; // Time when daemon was started
  XrdOucString KeyTabAdler; // adler string of the keytab file
  XrdSysMutex Mutex; // lock for dynamic updates like 'Manager' 
  static Config gConfig;

  Config ()
  {
    autoBoot = false;
    PublishInterval = 10;
    Manager = "";
  }

  ~Config () { }
};

EOSFSTNAMESPACE_END

#endif
