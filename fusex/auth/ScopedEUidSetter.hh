//----------------------------------------------------------------------
// File: ScopedEUidSetter.hh
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2024 CERN/Switzerland                                  *
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

#ifndef EOS_FUSEX_SCOPED_EUID_SETTER_HH
#define EOS_FUSEX_SCOPED_EUID_SETTER_HH

#ifdef __linux__

#include "common/Logging.hh"
#include <sys/syscall.h>

//------------------------------------------------------------------------------
//! Scoped euid and egid setter, restoring original values on destruction
//------------------------------------------------------------------------------
class ScopedEUidSetter
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  ScopedEUidSetter(uid_t euid_, gid_t egid_)
    : euid(euid_), egid(egid_)
  {
    ok = true;
    prevEuid = -1;
    prevEgid = -1;

    //--------------------------------------------------------------------------
    //! Set euid
    //--------------------------------------------------------------------------
    if (euid >= 0) {
      prevEuid = getuid();
      if (syscall(SYS_setresuid, euid, -1, -1) != 0) {
        eos_static_crit("Unable to set euid to %d!", euid);
        ok = false;
        return;
      }
    }

    //--------------------------------------------------------------------------
    //! Set egid
    //--------------------------------------------------------------------------
    if (egid >= 0) {
      prevEgid = getgid();

      if (syscall(SYS_setresgid, egid, -1, -1) != 0) {
        eos_static_crit("Unable to set euid to %d!", egid);
        ok = false;
        return;
      }
    }
  }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~ScopedEUidSetter()
  {
    if (prevEuid >= 0) {
      int retcode = syscall(SYS_setresuid, prevEuid, -1, -1);
      eos_static_debug("Restored euid from %d to %d [%d/%d]", euid, prevEuid, retcode, getuid());
    }

    if (prevEgid >= 0) {
      int retcode = syscall(SYS_setresgid, prevEgid, -1, -1);
      eos_static_debug("Restored fsgid from %d to %d [%d/%d]", egid, prevEgid, retcode, getgid());
    }
  }

  bool IsOk() const
  {
    return ok;
  }

private:
  int euid;
  int egid;

  int prevEuid;
  int prevEgid;

  bool ok;
};

#endif

#endif
