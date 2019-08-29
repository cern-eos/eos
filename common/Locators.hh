//------------------------------------------------------------------------------
// File: BaseViewLocator.hh
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

#ifndef EOS_COMMON_BASE_VIEW_LOCATOR_HH
#define EOS_COMMON_BASE_VIEW_LOCATOR_HH

#include "common/Namespace.hh"
#include <string>

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! This type helps figure out how to locate the appropriate shared hash for
//! a given node / group / space.
//!
//! Abstracts away the "config queue" / "broadcast queue" madness.
//------------------------------------------------------------------------------
class SharedHashLocator {
public:
  enum class Type {
    kSpace,
    kGroup,
    kNode
  };

  //----------------------------------------------------------------------------
  //! Constructor: Pass the EOS instance name, BaseView type, and name.
  //!
  //! Once we drop the MQ entirely, the instance name can be removed.
  //----------------------------------------------------------------------------
  SharedHashLocator(const std::string &instanceName, Type type,
    const std::string &name);

  //----------------------------------------------------------------------------
  //! Get "config queue" for shared hash
  //----------------------------------------------------------------------------
  std::string getConfigQueue() const;

  //----------------------------------------------------------------------------
  //! Get "broadcast queue" for shared hash
  //----------------------------------------------------------------------------
  std::string getBroadcastQueue() const;


private:
  std::string mInstanceName;
  Type mType;
  std::string mName;

  std::string mMqSharedHashPath;
  std::string mBroadcastQueue;

  std::string mChannel;
};


EOSCOMMONNAMESPACE_END

#endif

