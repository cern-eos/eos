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

#include "BulkRequestProcCleanerConfig.hh"

EOSBULKNAMESPACE_BEGIN

BulkRequestProcCleanerConfig::BulkRequestProcCleanerConfig(const std::chrono::seconds& interval, const std::chrono::seconds& bulkReqLastAccessTimeBeforeCleaning):mInterval(interval),mBulkReqLastAccessTimeBeforeCleaning(bulkReqLastAccessTimeBeforeCleaning){}

std::unique_ptr<BulkRequestProcCleanerConfig> BulkRequestProcCleanerConfig::getDefaultConfig(){
  //By default, the interval to run this thread is 1 hour
  //By default, a bulk-request that was not queried for one week will be deleted from the /proc/ directory
  return std::make_unique<BulkRequestProcCleanerConfig>(std::chrono::seconds(3600),std::chrono::seconds(604800));
}

EOSBULKNAMESPACE_END
