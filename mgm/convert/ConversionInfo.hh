//------------------------------------------------------------------------------
// File: ConversionInfo.hh
// Author: Mihai Patrascoiu - CERN
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

#pragma once
#include "mgm/Namespace.hh"
#include "common/FileId.hh"
#include "common/Logging.hh"
#include "common/LayoutId.hh"
#include "common/FileSystem.hh"

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! @brief Structure holding conversion details
//------------------------------------------------------------------------------
struct ConversionInfo {
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  ConversionInfo(const eos::common::FileId::fileid_t fid,
                 const eos::common::LayoutId::layoutid_t lid,
                 const eos::common::GroupLocator& location,
                 const std::string& plct_policy);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~ConversionInfo() = default;

  //----------------------------------------------------------------------------
  //! String representation of the conversion info
  //----------------------------------------------------------------------------
  inline std::string ToString() const
  {
    return mConversionString;
  }

  //----------------------------------------------------------------------------
  //! Parse a conversion string representation into a conversion info object.
  //!
  //! A conversion string has the following format:
  //! <fid(016hex)>:<space[.group]>#<layoutid(08hex)>[~<placement_policy>]
  //!
  //! @param sconversion the conversion string representation
  //! @param conversion the conversion info object to fill
  //!
  //! @return true if parsing succeeds, false otherwise
  //----------------------------------------------------------------------------
  static std::shared_ptr<ConversionInfo> parseConversionString(
    std::string sconversion);

  const eos::common::FileId::fileid_t mFid; ///< File id
  const eos::common::LayoutId::layoutid_t mLid; ///< Target layout id
  const eos::common::GroupLocator mLocation; ///< Target space/group placement
  const std::string mPlctPolicy; ///< Placement policy

private:
  std::string mConversionString; ///< Conversion string representation
};

EOSMGMNAMESPACE_END
