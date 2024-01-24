// ----------------------------------------------------------------------
// File: Untraceable.hh
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2022 CERN/Switzerland                                  *
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

#include "common/Namespace.hh"
#include <stdio.h>
#include <stdlib.h>
#ifdef APPLE
#include <sys/ptrace.h>
#else
#include <sys/prctl.h>
#include <sys/ptrace.h>
#endif

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class Untraceable
//------------------------------------------------------------------------------
class Untraceable 
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  Untraceable() {
#ifdef APPLE
    if (ptrace(PT_DENY_ATTACH, 0, 0, 0) == -1 ) {
      fprintf(stderr,"error: failed to make the process untraceable\n");
      exit(-1);
    }
#else

    if (ptrace(PTRACE_TRACEME,0,0,0) || (prctl(PR_SET_DUMPABLE, 0) != 0 )) {
      fprintf(stderr,"error: failed to make the process untraceable\n");
      exit(-1);
    } else {
    }
#endif
  }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~Untraceable() {
#ifdef APPLE
    // nothing
#else
    prctl(PR_SET_DUMPABLE, 1);
#endif
  }

};

EOSCOMMONNAMESPACE_END
