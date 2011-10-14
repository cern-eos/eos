// ----------------------------------------------------------------------
// File: Vid.hh
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

#ifndef __EOSMGM_VID__HH__
#define __EOSMGM_VID__HH__

/*----------------------------------------------------------------------------*/
#include "mgm/Namespace.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucEnv.hh"
/*----------------------------------------------------------------------------*/
#include <sys/types.h>
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

class Vid {
public:
  Vid(){};
  ~Vid(){};

  static bool Set(const char* value);
  static bool Set(XrdOucEnv &env, int &retc, XrdOucString &stdOut, XrdOucString &stdErr);
  static void Ls(XrdOucEnv &env, int &retc, XrdOucString &stdOut, XrdOucString &stdErr);
  static bool Rm(XrdOucEnv &env, int &retc, XrdOucString &stdOut, XrdOucString &stdErr);

  static const char* Get(const char* key);
};

EOSMGMNAMESPACE_END

#endif
