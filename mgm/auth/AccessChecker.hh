// ----------------------------------------------------------------------
// File: AccessChecker.hh
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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

#ifndef EOS_MGM_ACCESS_CHECKER_HH
#define EOS_MGM_ACCESS_CHECKER_HH

#include "mgm/Namespace.hh"
#include "common/Mapping.hh"

namespace eos {
  class IContainerMD;
  class IFileMD;
}

EOSMGMNAMESPACE_BEGIN

class Acl;

class AccessChecker {
public:

  //----------------------------------------------------------------------------
  //! Check access to the given container - all information required to make
  //! a decision are passed to this function, no external information should
  //! be needed.
  //----------------------------------------------------------------------------
  static bool checkContainer(IContainerMD *cont, const Acl &acl, int mode,
    const eos::common::Mapping::VirtualIdentity &vid);

  //----------------------------------------------------------------------------
  //! Check access to the given file. The parent directory of the file
  //! needs to be checked separately!
  //----------------------------------------------------------------------------
  static bool checkFile(IFileMD *file, int mode,
    const eos::common::Mapping::VirtualIdentity &vid);

};

EOSMGMNAMESPACE_END

#endif