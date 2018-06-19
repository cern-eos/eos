/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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
//! @brief Class for parsing QdbContactDetails out of a configuration map
//------------------------------------------------------------------------------

#ifndef EOS_NS_QDB_CONFIGURATION_PARSER_HH
#define EOS_NS_QDB_CONFIGURATION_PARSER_HH

#include <chrono>

#include <qclient/Members.hh>
#include <qclient/Options.hh>
#include <qclient/Handshake.hh>
#include "namespace/Namespace.hh"
#include "namespace/MDException.hh"
#include "namespace/ns_quarkdb/QdbContactDetails.hh"

EOSNSNAMESPACE_BEGIN

class ConfigurationParser {
public:

  //----------------------------------------------------------------------------
  //! Parse a configuration map, and extract a QdbContactDetails out of it.
  //! Throws in case that's not possible.
  //----------------------------------------------------------------------------
  static QdbContactDetails parse(const
    std::map<std::string, std::string> &configuration)
  {
    QdbContactDetails contactDetails;

    const std::string key_cluster = "qdb_cluster";
    const std::string key_password = "qdb_password";

    auto it = configuration.find(key_cluster);
    if(it == configuration.end()) {
      throw_mdexception(EINVAL, "Could not find qdb_cluster in NS configuration!");
    }

    if(!contactDetails.members.parse(it->second)) {
      throw_mdexception(EINVAL, "Could not parse qdb_cluster");
    }

    it = configuration.find(key_password);
    if(it != configuration.end()) {
      contactDetails.password = it->second;
    }

    return contactDetails;
  }


};


EOSNSNAMESPACE_END

#endif
