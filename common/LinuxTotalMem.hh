// ----------------------------------------------------------------------
// File: LinuxTotalMem.hh
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/ASwitzerland                                  *
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

/**
 * @file   LinuxTotalMem.hh
 *
 * @brief  Class providing total memory information
 *
 */

#ifndef __EOSCOMMON__LINUXTOTALMEM__HH
#define __EOSCOMMON__LINUXTOTALMEM__HH

#include "common/Namespace.hh"
#include <sys/sysinfo.h>
#include <mutex>

EOSCOMMONNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
//! Static Class to retrieve total memory values
//!
//! Example: linux_total_mem_t mem; GetTotalMemory(mem);
//!
/*----------------------------------------------------------------------------*/
class LinuxTotalMem
{
public:
  typedef struct sysinfo linux_total_mem_t;

  LinuxTotalMem()
  {
    update();
  }

  bool update()
  {
    std::lock_guard<std::mutex> lock(locker);

    if (!sysinfo((struct sysinfo*) &meminfo)) {
      return true;
    } else {
      return false;
    }
  }

  linux_total_mem_t get()
  {
    std::lock_guard<std::mutex> lock(locker);
    return meminfo;
  }

  std::mutex& mutex()
  {
    return locker;
  }
  linux_total_mem_t& getref()
  {
    return meminfo;
  }

private:
  linux_total_mem_t meminfo;
  std::mutex locker;
};

EOSCOMMONNAMESPACE_END

#endif
