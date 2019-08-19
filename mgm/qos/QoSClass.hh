// ----------------------------------------------------------------------
// File: QoSClass.hh
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
#include "namespace/interface/IFileMD.hh"

//! Forward declaration
class QoSConfig;

//------------------------------------------------------------------------------
//! @brief Representation of a QoS class
//------------------------------------------------------------------------------

#define CDMI_REDUNDANCY_TAG "cdmi_data_redundancy_provided"
#define CDMI_LATENCY_TAG "cdmi_latency_provided"
#define CDMI_PLACEMENT_TAG "cdmi_geographic_placement_provided"

EOSMGMNAMESPACE_BEGIN

class QoSClass {
  friend class QoSConfig;

public:
  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~QoSClass() = default;

  const std::string name; ///< Class name
  const int cdmi_redundancy; ///< Redundancy provided
  const int cdmi_latency; ///< Latency provided
  const std::vector<std::string> transitions; ///< Allowed class transitions
  const std::vector<std::string> locations; ///< Placement locations
  const eos::IFileMD::QoSAttrMap attributes; ///< Class attributes

private:
  //----------------------------------------------------------------------------
  //! Private constructor
  //! Note: Instantiate this object via QoSConfig factory method
  //----------------------------------------------------------------------------
  QoSClass(const std::string& _name,
           const int _cdmi_redundancy,
           const int _cdmi_latency,
           std::vector<std::string>& _transitions,
           std::vector<std::string>& _locations,
           eos::IFileMD::QoSAttrMap& _attributes) :
    name(_name),
    cdmi_redundancy(_cdmi_redundancy),
    cdmi_latency(_cdmi_latency),
    transitions(_transitions),
    locations(_locations),
    attributes(_attributes) {}

};

EOSMGMNAMESPACE_END
