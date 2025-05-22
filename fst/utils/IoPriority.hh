// ----------------------------------------------------------------------
//! @file: IoPriority.hh
//! @author: Andreas Joachim Peters <andreas.joachim.peters@cern.ch>
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2021 CERN/Switzerland                                  *
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
#include "fst/Namespace.hh"
#include <string>

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! 8 best effort priority levels are supported
//------------------------------------------------------------------------------
#define IOPRIO_BE_NR (8)

//------------------------------------------------------------------------------
//! Gives us 8 prio classes with 13-bits of data for each class
//------------------------------------------------------------------------------
#define IOPRIO_BITS             (16)
#define IOPRIO_CLASS_SHIFT      (13)
#define IOPRIO_PRIO_MASK        ((1UL << IOPRIO_CLASS_SHIFT) - 1)
#define IOPRIO_PRIO_CLASS(mask) ((mask) >> IOPRIO_CLASS_SHIFT)
#define IOPRIO_PRIO_DATA(mask)  ((mask) & IOPRIO_PRIO_MASK)
#define IOPRIO_PRIO_VALUE(class, data)  (((class) << IOPRIO_CLASS_SHIFT) | data)

#define ioprio_valid(mask)      (IOPRIO_PRIO_CLASS((mask)) != IOPRIO_CLASS_NONE)

//------------------------------------------------------------------------------
//! These are the io priority groups as implemented by CFQ. RT is the realtime
//! class, it always gets premium service. BE is the best-effort scheduling
//! class, the default for any process. IDLE is the idle scheduling class, it
//! is only served when no one else is using the disk.
//------------------------------------------------------------------------------
enum {
  IOPRIO_CLASS_NONE,
  IOPRIO_CLASS_RT,
  IOPRIO_CLASS_BE,
  IOPRIO_CLASS_IDLE,
};

enum {
  IOPRIO_WHO_PROCESS = 1,
  IOPRIO_WHO_PGRP,
  IOPRIO_WHO_USER,
};

//------------------------------------------------------------------------------
//! Set IO priority
//!
//! @return 0 if successful, otherwise -1
//------------------------------------------------------------------------------
int ioprio_set(int which, int ioprio);

//------------------------------------------------------------------------------
//! Get IO priority
//------------------------------------------------------------------------------
int ioprio_get(int which);

//------------------------------------------------------------------------------
//! Convert string to IO priority class
//!
//! @param c string representation of IO priority class
//!
//! @return IO priority class numeric
//------------------------------------------------------------------------------
int ioprio_class(const std::string& c);

//------------------------------------------------------------------------------
//! Convert string to IO priority level (0..7)
//!
//! @param v string representation of IO priority level
//!
//! @return IO priority level numeric
//------------------------------------------------------------------------------
int ioprio_value(const std::string& v);

//------------------------------------------------------------------------------
//! Check if requested IO priority class requires sysadm rights
//!
//! @param iopriority IO priority class
//!
//! @return 0 if false, 1 if true
//------------------------------------------------------------------------------
int ioprio_needs_sysadm(int iopriority);

//------------------------------------------------------------------------------
//! Change IO priority
//!
//! @return 0 if successful, otherwise non-zero
//------------------------------------------------------------------------------
int ioprio_begin(int which, int iopriority, int local_iopriority);

//------------------------------------------------------------------------------
//! Change back to default IO priority BE 4
//------------------------------------------------------------------------------
int ioprio_end(int which, int iopriority);

EOSFSTNAMESPACE_END
