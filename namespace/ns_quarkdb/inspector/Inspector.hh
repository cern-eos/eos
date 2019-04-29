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
//! @brief Class for inspecting namespace contents - talks directly to QDB
//------------------------------------------------------------------------------

#pragma once
#include "namespace/Namespace.hh"
#include <string>

namespace qclient {
  class QClient;
}

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Inspector class
//------------------------------------------------------------------------------
class Inspector {
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  Inspector(qclient::QClient &qcl);

  //----------------------------------------------------------------------------
  //! Is the connection to QDB ok? If not, pointless to run anything else.
  //----------------------------------------------------------------------------
  bool checkConnection(std::string &err);

  //----------------------------------------------------------------------------
  //! Dump contents of the given path. ERRNO-like integer return value, 0
  //! means no error.
  //----------------------------------------------------------------------------
  int dump(const std::string &path, std::ostream &out);

  //----------------------------------------------------------------------------
  //! Check intra-container conflicts, such as a container having two entries
  //! with the name name.
  //----------------------------------------------------------------------------
  int checkNamingConflicts(std::ostream &out, std::ostream &err);


private:
  qclient::QClient &mQcl;

};

EOSNSNAMESPACE_END
