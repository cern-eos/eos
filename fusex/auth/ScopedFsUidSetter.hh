//----------------------------------------------------------------------
// File: ScopedfsUidSetter.hh
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

#ifndef EOS_FUSEX_SCOPED_FS_UID_SETTER_HH
#define EOS_FUSEX_SCOPED_FS_UID_SETTER_HH

#ifdef __linux__

#include "common/Logging.hh"
#include <sys/fsuid.h>

//------------------------------------------------------------------------------
//! Scoped fsuid and fsgid setter, restoring original values on destruction
//------------------------------------------------------------------------------
class ScopedFsUidSetter
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  ScopedFsUidSetter(uid_t fsuid_, gid_t fsgid_)
    : fsuid(fsuid_), fsgid(fsgid_)
  {
    ok = true;
    prevFsuid = -1;
    prevFsgid = -1;

    //--------------------------------------------------------------------------
    //! Set fsuid
    //--------------------------------------------------------------------------
    if (fsuid >= 0) {
      prevFsuid = setfsuid(fsuid);

      if (setfsuid(fsuid) != fsuid) {
        eos_static_crit("Unable to set fsuid to %d!", fsuid);
        ok = false;
        return;
      }
    }

    //--------------------------------------------------------------------------
    //! Set fsgid
    //--------------------------------------------------------------------------
    if (fsgid >= 0) {
      prevFsgid = setfsgid(fsgid);

      if (setfsgid(fsgid) != fsgid) {
        eos_static_crit("Unable to set fsuid to %d!", fsgid);
        ok = false;
        return;
      }
    }
  }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~ScopedFsUidSetter()
  {
    if (prevFsuid >= 0) {
      int retcode = setfsuid(prevFsuid);
      eos_static_debug("Restored fsuid from %d to %d", retcode, prevFsuid);
    }

    if (prevFsgid >= 0) {
      int retcode = setfsgid(prevFsgid);
      eos_static_debug("Restored fsgid from %d to %d", retcode, prevFsgid);
    }
  }

  bool IsOk() const
  {
    return ok;
  }

private:
  int fsuid;
  int fsgid;

  int prevFsuid;
  int prevFsgid;

  bool ok;
};

#endif

#endif