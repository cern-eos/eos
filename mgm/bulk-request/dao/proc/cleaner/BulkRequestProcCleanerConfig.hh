//------------------------------------------------------------------------------
//! @file BulkRequestProcCleanerConfig.hh
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

#ifndef EOS_BULKREQUESTPROCCLEANERCONFIG_HH
#define EOS_BULKREQUESTPROCCLEANERCONFIG_HH

#include "mgm/Namespace.hh"
#include <chrono>
#include <memory>

EOSBULKNAMESPACE_BEGIN

class BulkRequestProcCleanerConfig {
public:

  BulkRequestProcCleanerConfig(const std::chrono::seconds & interval, const std::chrono::seconds & bulkReqLastAccessTimeBeforeCleaning);
  /**
   * Run the BulkRequestProcCleaner thread this many seconds
   */
  std::chrono::seconds mInterval;

  /**
   * If a bulk-request has not been queried since this many seconds,
   * it will be deleted from the /proc/ directory
   */
  std::chrono::seconds mBulkReqLastAccessTimeBeforeCleaning;

  /**
   * Returns the default cleaner configuration
   * @return the default cleaner configuration
   */
  static std::unique_ptr<BulkRequestProcCleanerConfig> getDefaultConfig();
};

EOSBULKNAMESPACE_END

#endif // EOS_BULKREQUESTPROCCLEANERCONFIG_HH
