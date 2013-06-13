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

/*----------------------------------------------------------------------------*/
#include "mgm/Namespace.hh"
#include "common/Logging.hh"
#include "common/FileSystem.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/
#include <vector>
#include <string>
#include <deque>
#include <cstring>
/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/
/**
 * @file   Balancer.hh
 * 
 * @brief  Class steering in-group balancing
 *
 */
/*----------------------------------------------------------------------------*/
EOSMGMNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
//! @brief class steering the balancing activity
//! 
//! This class run's as singleton per space on the MGM and checks all 
//! existing groups if they are balanced.
//! In case there is an inbalance, it signals balancing to all nodes in the 
//! group.
/*----------------------------------------------------------------------------*/
class Balancer
{
private:
  pthread_t thread; //< thread id of the balancer thread
  std::string mSpaceName; //< responsable space of this balancer object

public:

  // ---------------------------------------------------------------------------
  // Constructor
  // ---------------------------------------------------------------------------
  Balancer (const char* spacename);

  // ---------------------------------------------------------------------------
  // Destructor
  // ---------------------------------------------------------------------------
  ~Balancer ();

  // ---------------------------------------------------------------------------
  // static thread start function
  // ---------------------------------------------------------------------------
  static void* StaticBalance (void*);

  // ---------------------------------------------------------------------------
  // balancer implementation
  // ---------------------------------------------------------------------------
  void* Balance ();
};

EOSMGMNAMESPACE_END
#endif

