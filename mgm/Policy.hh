// ----------------------------------------------------------------------
// File: Policy.hh
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

#ifndef __EOSMGM_POLICY__HH__
#define __EOSMGM_POLICY__HH__

/*----------------------------------------------------------------------------*/
#include "mgm/Namespace.hh"
#include "mgm/Scheduler.hh"
#include "common/Mapping.hh"
#include "namespace/interface/IContainerMD.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucEnv.hh"
/*----------------------------------------------------------------------------*/
#include <sys/types.h>

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

class Policy
{
public:

  Policy () { };

  ~Policy () { };

  static void GetLayoutAndSpace (const char* path,
                                 eos::IContainerMD::XAttrMap &map,
                                 const eos::common::VirtualIdentity &vid,
                                 unsigned long &layoutId,
                                 XrdOucString &space,
                                 XrdOucEnv &env,
                                 unsigned long &forcedfsid,
                                 long &forcedgroup, 
				 std::string& bandwidth,
				 bool lock_view = false);

  static void GetPlctPolicy (const char* path,
                             eos::IContainerMD::XAttrMap &map,
                             const eos::common::VirtualIdentity &vid,
                             XrdOucEnv &env,
                             eos::mgm::Scheduler::tPlctPolicy &plctpo,
                             std::string &targetgeotag);

  static unsigned long GetSpacePolicyLayout(const char* space);


  static bool Set (const char* value);
  static bool Set (XrdOucEnv &env, int &retc, XrdOucString &stdOut, XrdOucString &stdErr);
  static void Ls (XrdOucEnv &env, int &retc, XrdOucString &stdOut, XrdOucString &stdErr);
  static bool Rm (XrdOucEnv &env, int &retc, XrdOucString &stdOut, XrdOucString &stdErr);


  static bool IsProcConversion(const char* path);

  static const char* Get (const char* key);
};

EOSMGMNAMESPACE_END

#endif
