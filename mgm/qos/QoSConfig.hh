// ----------------------------------------------------------------------
// File: QoSConfig.hh
// Author: Mihai Patrascoiu - CERN
// ----------------------------------------------------------------------

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

#pragma once
#include "common/Logging.hh"
#include "mgm/Namespace.hh"
#include "mgm/qos/QoSClass.hh"

#include <json/json.h>

//------------------------------------------------------------------------------
//! @brief The QoS config file parser
//------------------------------------------------------------------------------

EOSMGMNAMESPACE_BEGIN

class QoSConfig {
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  QoSConfig(const char* filename);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~QoSConfig() = default;

  //----------------------------------------------------------------------------
  //! Check if the config file could be open
  //----------------------------------------------------------------------------
  inline bool IsValid() const {
    return mFile.good();
  }

  //----------------------------------------------------------------------------
  //! Load the config file into a map of QoS Classes
  //----------------------------------------------------------------------------
  std::map<std::string, eos::mgm::QoSClass> LoadConfig();

  //----------------------------------------------------------------------------
  //! QoSClass factory method - build a QoS Class object from JSON
  //----------------------------------------------------------------------------
  static std::shared_ptr<eos::mgm::QoSClass> CreateQoSClass(
    const Json::Value& qos_json);

  //----------------------------------------------------------------------------
  //! Return string representation of a QoS class
  //----------------------------------------------------------------------------
  static std::string QoSClassToString(const eos::mgm::QoSClass& qos);

  //----------------------------------------------------------------------------
  //! Return JSON representation of a QoS class
  //----------------------------------------------------------------------------
  static Json::Value QoSClassToJson(const eos::mgm::QoSClass& qos);

private:
  std::string mFilename; ///< QoS config file name
  std::ifstream mFile; ///< QoS config file stream
};

EOSMGMNAMESPACE_END
