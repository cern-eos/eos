// ----------------------------------------------------------------------
// File: Workflow.hh
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

#ifndef __EOSMGM_WORKFLOW__HH__
#define __EOSMGM_WORKFLOW__HH__

/*----------------------------------------------------------------------------*/
#include "mgm/Namespace.hh"
#include "namespace/ContainerMD.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucEnv.hh"
/*----------------------------------------------------------------------------*/
#include <sys/types.h>

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

class Workflow {
public:

  Workflow ()
  {
    mAttr = 0;
  };

  void Init (eos::ContainerMD::XAttrMap *attr)
  {
    mAttr = attr;
  }

  int Trigger (std::string event, std::string workflow);

  std::string getCGICloseW (std::string workflow);

  std::string getCGICloseR (std::string workflow);

  ~Workflow ()
  {
  };

private:

  eos::ContainerMD::XAttrMap* mAttr;
};

EOSMGMNAMESPACE_END

#endif
