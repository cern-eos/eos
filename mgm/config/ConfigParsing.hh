//------------------------------------------------------------------------------
// File: ConfigParsing.hh
// Author: Georgios Bitzes - CERN
//------------------------------------------------------------------------------

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

#ifndef EOS_MGM_CONFIG_PARSING_HH
#define EOS_MGM_CONFIG_PARSING_HH

#include "mgm/Namespace.hh"
#include <string>
#include <map>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class ConfigParsing
//------------------------------------------------------------------------------
class ConfigParsing {
public:
  //----------------------------------------------------------------------------
  //! Parse filesystem configuration into a map. We should have a dedicated
  //! object that represents filesystem configuration ideally, but this will
  //! do for now..
  //!
  //! Returns if parsing was successful or not.
  //----------------------------------------------------------------------------
  static bool parseFilesystemConfig(const std::string &config,
    std::map<std::string, std::string> &out);

  //----------------------------------------------------------------------------
  //! Parse configuration file
  //!
  //! Returns if parsing was successful or not.
  //----------------------------------------------------------------------------
  static bool parseConfigurationFile(const std::string &contents,
    std::map<std::string, std::string> &out, std::string &err);

};


EOSMGMNAMESPACE_END

#endif