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

BulkRequestProcCleanerConfig BulkRequestProcCleanerConfig::getDefaultConfig(){
  BulkRequestProcCleanerConfig defaultConfig;
  //By default, the interval to run this thread is 1 hour
  defaultConfig.interval = std::chrono::seconds(3600);
  //By default, a bulk-request that was not queried for one week will be deleted from the /proc/ directory
  defaultConfig.bulkReqLastAccessTimeBeforeCleaning = std::chrono::seconds(604800);
  return defaultConfig;
}

EOSBULKNAMESPACE_END
