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
#include "common/FileId.hh"
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

  void Init (eos::ContainerMD::XAttrMap *attr, std::string path = "", eos::common::FileId::fileid_t fid = 0)
  {
    mAttr = attr;
    mPath = path;
    mFid = fid;
  }

  int Trigger (std::string event, std::string workflow);

  std::string getCGICloseW (std::string workflow);

  std::string getCGICloseR (std::string workflow);

  ~Workflow ()
  {
  };

  void Reset ()
  {
    mPath = "";
    mFid = 0;
    mEvent = "";
    mWorkflow = "";
    mAttr = 0;
    mAction = "";
  }
  bool Create ();
  bool Attach (const char* path);
  bool Delete ();

private:

  eos::ContainerMD::XAttrMap* mAttr;
  std::string mPath;
  eos::common::FileId::fileid_t mFid;
  std::string mEvent;
  std::string mWorkflow;
  std::string mAction;
};

EOSMGMNAMESPACE_END

#endif
