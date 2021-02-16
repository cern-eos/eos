//------------------------------------------------------------------------------
// @file QuarkConfigHandler.hh
// @author Georgios Bitzes - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2020 CERN/Switzerland                                  *
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
#include "mgm/Namespace.hh"
#include "common/Status.hh"
#include "namespace/ns_quarkdb/QdbContactDetails.hh"
#include "proto/ChangelogEntry.pb.h"
#include <map>
#include <folly/futures/Future.h>

namespace folly
{
class Executor;
}

namespace qclient
{
class QClient;
}

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Class to perform reads and writes on the MGM configuration stored in QDB
//------------------------------------------------------------------------------
class QuarkConfigHandler
{
public:
  //----------------------------------------------------------------------------
  // Constructor
  //----------------------------------------------------------------------------
  QuarkConfigHandler(const QdbContactDetails& cd);

  //----------------------------------------------------------------------------
  // Ensure connection is established
  //----------------------------------------------------------------------------
  common::Status checkConnection(std::chrono::milliseconds timeout =
                                   std::chrono::seconds(5));

  //----------------------------------------------------------------------------
  // Fetch a given configuration
  //----------------------------------------------------------------------------
  common::Status fetchConfiguration(const std::string& name,
                                    std::map<std::string, std::string>& out);

  //----------------------------------------------------------------------------
  // Write the given configuration
  //----------------------------------------------------------------------------
  folly::Future<common::Status>
  writeConfiguration(const std::string& name,
                     const std::map<std::string, std::string>& config,
                     bool overwrite, const std::string& backup = "");

  //----------------------------------------------------------------------------
  // Check if configuration key exists already
  //----------------------------------------------------------------------------
  common::Status checkExistence(const std::string& name, bool& existence);

  //----------------------------------------------------------------------------
  // Obtain list of available configurations, and backups
  //----------------------------------------------------------------------------
  common::Status listConfigurations(std::vector<std::string>& configs,
                                    std::vector<std::string>& backups);

  //----------------------------------------------------------------------------
  // Append an entry to the changelog
  //----------------------------------------------------------------------------
  folly::Future<common::Status>
  appendChangelog(const eos::mgm::ConfigChangelogEntry& entry);

  //----------------------------------------------------------------------------
  // Show configuration changelog
  //----------------------------------------------------------------------------
  common::Status tailChangelog(int nlines, std::vector<std::string>& changelog);

  //----------------------------------------------------------------------------
  // Trim backups to the nth most recent ones. If no more than N backups exist
  // anyway, do nothing.
  //
  // We will delete a maximum of 200 backups at a time -- you may have to call
  // this function multiple times to trim everything.
  //----------------------------------------------------------------------------
  common::Status trimBackups(const std::string& name, size_t limit,
                             size_t& deleted);

  //----------------------------------------------------------------------------
  // Form hash key
  //----------------------------------------------------------------------------
  static std::string formHashKey(const std::string& name);

  //----------------------------------------------------------------------------
  // Form backup key
  //----------------------------------------------------------------------------
  static std::string formBackupHashKey(const std::string& name, time_t timestamp);

private:
  QdbContactDetails mContactDetails;
  std::unique_ptr<qclient::QClient> mQcl;
  std::unique_ptr<folly::Executor> mExecutor;
};

EOSMGMNAMESPACE_END

