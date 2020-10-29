/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

//------------------------------------------------------------------------------
//! @author Georgios Bitzes <georgios.bitzes@cern.ch>
//! @brief  Function to enforce a minimum version of QuarkDB
//------------------------------------------------------------------------------

#include "namespace/ns_quarkdb/VersionEnforcement.hh"
#include "common/Logging.hh"
#include <qclient/QClient.hh>
#include <qclient/QuarkDBVersion.hh>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Check if quarkdb version is good enough
//------------------------------------------------------------------------------
bool enforceQuarkDBVersion(qclient::QClient* qcl)
{
  qclient::redisReplyPtr reply = qcl->exec("quarkdb-version").get();
  eos_static_info("QuarkDB version: %s",
                  qclient::describeRedisReply(reply).c_str());
  std::string str = std::string(reply->str, reply->len);
  qclient::QuarkDBVersion actual;

  if (!qclient::QuarkDBVersion::fromString(str, actual)) {
    eos_static_crit("Could not parse reply to quarkdb-version");
    return false;
  }

  qclient::QuarkDBVersion target(0, 4, 2, "");

  if (target > actual) {
    eos_static_crit("Outdated QuarkDB version (%s), we need at least %s. Update!",
                    actual.toString().c_str(), target.toString().c_str());
    return false;
  }

  return true;
}

EOSNSNAMESPACE_END
