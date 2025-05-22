// ----------------------------------------------------------------------
//! @file IoPriority.cc
//! @author Elvin Sindrilaru <esindril@cern.ch>
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2025 CERN/Switzerland                                  *
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

#include "fst/utils/IoPriority.hh"
#include <unistd.h>
#include <fcntl.h>
#ifndef __APPLE__
#include <sys/syscall.h>
#include <asm/unistd.h>
#include <sys/capability.h>
#endif

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Set IO priority
//------------------------------------------------------------------------------
int ioprio_set(int which, int ioprio)
{
#ifdef __APPLE__
  return 0;
#else
  return syscall(SYS_ioprio_set, which, 0, ioprio);
#endif
}

//------------------------------------------------------------------------------
// Get IO priority
//------------------------------------------------------------------------------
int ioprio_get(int which)
{
#ifdef __APPLE__
  return 0;
#else
  return syscall(SYS_ioprio_get, which, 0);
#endif
}

//------------------------------------------------------------------------------
// Convert string to IO priority class
//------------------------------------------------------------------------------
int ioprio_class(const std::string& c)
{
  if (c == "idle") {
    return IOPRIO_CLASS_IDLE;
  } else if (c == "be") {
    return IOPRIO_CLASS_BE;
  } else if (c == "rt") {
    return IOPRIO_CLASS_RT;
  } else {
    return IOPRIO_CLASS_NONE;
  }
}

//------------------------------------------------------------------------------
// Convert string to IO priority level (0..7)
//------------------------------------------------------------------------------
int ioprio_value(const std::string& v)
{
  if (v.length()) {
    int level = std::atoi(v.c_str());

    if ((level < 0) || (level > 7)) {
      return 0;
    } else {
      return level;
    }
  } else {
    return 0;
  }
}

//------------------------------------------------------------------------------
// Check if requested IO priority requires sysadm rights
//------------------------------------------------------------------------------
int ioprio_needs_sysadm(int iopriority)
{
  if ((IOPRIO_PRIO_CLASS(iopriority) == IOPRIO_CLASS_RT) ||
      (IOPRIO_PRIO_CLASS(iopriority) == IOPRIO_CLASS_IDLE)) {
    return true;
  } else {
    return false;
  }
}

//------------------------------------------------------------------------------
// Change IO priority
//------------------------------------------------------------------------------
int ioprio_begin(int which, int iopriority, int local_iopriority)
{
  int rc = 0;

  if (local_iopriority == iopriority) {
    return 0;
  }

#ifndef __APPLE__

  if (ioprio_needs_sysadm(iopriority)) {
    struct __user_cap_header_struct cap_header;
    struct __user_cap_data_struct cap_data;
    cap_header.pid = 0;
    cap_header.version = _LINUX_CAPABILITY_VERSION_1;
    cap_data.effective = cap_data.permitted = ~0u;
    cap_data.inheritable = 0;
    rc |= capset(&cap_header, &cap_data);
  }

#endif
  rc |= ioprio_set(which, iopriority);
  return rc;
}

//------------------------------------------------------------------------------
// Change back to default IO priority to BE 4
//------------------------------------------------------------------------------
int ioprio_end(int which, int iopriority)
{
#ifndef __APPLE__

  if (ioprio_needs_sysadm(iopriority)) {
    struct __user_cap_header_struct cap_header;
    struct __user_cap_data_struct cap_data;
    cap_header.pid = 0;
    cap_header.version = _LINUX_CAPABILITY_VERSION_1;
    cap_data.permitted = ~0u;
    cap_data.effective = 0;
    cap_data.inheritable = 0;
    capset(&cap_header, &cap_data);
  }

#endif
  ioprio_set(which, IOPRIO_PRIO_VALUE(IOPRIO_CLASS_BE, 4));
  int current_iopriority = ioprio_get(which);
  return current_iopriority;
}

EOSFSTNAMESPACE_END
