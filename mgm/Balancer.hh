// ----------------------------------------------------------------------
// File: Balancer.hh
// Author: Andreas-Joachim Peters - CERN
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

#ifndef __EOSMGM_BALANCER__
#define __EOSMGM_BALANCER__

#include "mgm/Namespace.hh"
#include "common/Logging.hh"
#include "common/FileSystem.hh"
#include "XrdSys/XrdSysPthread.hh"
#include <vector>
#include <string>
#include <deque>
#include <cstring>


EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! @brief Class steering the balancing activity
//!
//! This class run's as singleton per space on the MGM and checks all
//! existing groups if they are balanced.
//! In case there is an inbalance, it signals balancing to all nodes in the
//! group.
//------------------------------------------------------------------------------
class Balancer
{
public:

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param space_name space name for which this balancer is responsible
  //----------------------------------------------------------------------------
  Balancer(const char* spacename);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~Balancer();

  //----------------------------------------------------------------------------
  //! Method used to stop balancing thread
  //----------------------------------------------------------------------------
  void Stop();

  //----------------------------------------------------------------------------
  //! Static function used to start thread
  //----------------------------------------------------------------------------
  static void* StaticBalance(void*);

  //----------------------------------------------------------------------------
  //! Balancer implementation - this method en-/disables balancing within
  //! groups depending on the current settings.
  //----------------------------------------------------------------------------
  void* Balance();

private:
  pthread_t mThread; ///< Balancer thread id
  std::string mSpaceName; ///< Space of this balancer object
};

EOSMGMNAMESPACE_END
#endif
