//------------------------------------------------------------------------------
//! @file BulkRequestProcCleaner.hh
//! @author Cedric Caffy - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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

#ifndef EOS_BULKREQUESTPROCCLEANER_HH
#define EOS_BULKREQUESTPROCCLEANER_HH

#include "common/AssistedThread.hh"
#include "XrdOuc/XrdOucErrInfo.hh"
#include "common/Mapping.hh"
#include "mgm/Namespace.hh"
#include "common/VirtualIdentity.hh"
#include "mgm/bulk-request/dao/proc/cleaner/BulkRequestProcCleanerConfig.hh"

EOSBULKNAMESPACE_BEGIN

class BulkRequestProcCleaner {
public:
  BulkRequestProcCleaner();
  /**
   * Start the cleaner thread
   */
  void Start();
  /**
   * Stop the cleaner thread
   */
  void Stop();

  /**
   * Method that will be ran by the thread
   *
   * This thread will look for the bulk-request directories in /proc/ and check the last time
   * a bulk-request was queried (extended attribute on the bulk-request directory. If a bulk-request has not been queried for more than one week,
   * it will be deleted from the system.
   */
  void backgroundThread(ThreadAssistant & assistant);

  /**
   * Destructor, stop the cleaner thread
   */
  ~BulkRequestProcCleaner();
private:
  AssistedThread mThread; ///< thread of the /proc/ cleaner thread
  BulkRequestProcCleanerConfig mConfig; ///< Configuration of the cleaner (e.g interval of execution)
};

EOSBULKNAMESPACE_END

#endif // EOS_BULKREQUESTPROCCLEANER_HH
