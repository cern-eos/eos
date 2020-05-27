// ----------------------------------------------------------------------
// File: QuarkConfigHandler.cc
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

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

#include "mgm/config/QuarkConfigHandler.hh"
#include "common/Assert.hh"

#include <qclient/QClient.hh>
#include <qclient/ResponseParsing.hh>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
QuarkConfigHandler::QuarkConfigHandler(const QdbContactDetails &cd)
: mContactDetails(cd) {
  mQcl = std::unique_ptr<qclient::QClient>(
    new qclient::QClient(mContactDetails.members, mContactDetails.constructOptions()));
}

//------------------------------------------------------------------------------
// Check if a given configuration exists
//------------------------------------------------------------------------------
common::Status QuarkConfigHandler::checkExistence(const std::string &name, bool &existence) {
  qclient::IntegerParser existsResp(mQcl->exec("EXISTS", SSTR("eos-config:" << name)).get());

  if (!existsResp.ok()) {
    return common::Status(EINVAL,
      SSTR("Received unexpected response in EXISTS check: " << existsResp.err()));
  }

  existence = (existsResp.value() != 0);
  return common::Status();
}

//------------------------------------------------------------------------------
// Fetch a given configuration
//------------------------------------------------------------------------------
common::Status QuarkConfigHandler::fetchConfiguration(const std::string &name, std::map<std::string, std::string> &out) {
  qclient::redisReplyPtr reply = mQcl->exec("HGETALL", formHashKey(name)).get();
  qclient::HgetallParser parser(reply);

  if(!parser.ok()) {
    return common::Status(EINVAL, parser.err());
  }

  out = parser.value();
  return common::Status();
}

//------------------------------------------------------------------------------
// Form target key
//------------------------------------------------------------------------------
std::string QuarkConfigHandler::formHashKey(const std::string &name) {
  return SSTR("eos-config:" << name);
}

//----------------------------------------------------------------------------
//! Form backup key
//----------------------------------------------------------------------------
std::string QuarkConfigHandler::formBackupHashKey(const std::string& name, time_t timestamp) {
  char buff[128];
  strftime(buff, 127, "%Y%m%d%H%M%S", localtime(&timestamp));
  return SSTR("eos-config-backup" << ":" << name << "-" << buff);
}

EOSMGMNAMESPACE_END
