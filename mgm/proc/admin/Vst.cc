// ----------------------------------------------------------------------
// File: proc/admin/Vst.cc
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
 * You should have received a copy of the AGNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

/*----------------------------------------------------------------------------*/
#include "mgm/ProcInterface.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/VstView.hh"
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

int
ProcCommand::Vst ()
{
  if (pVid->uid == 0)
  {
    if (mSubCmd == "ls")
    {
      std::string option = pOpaque->Get("mgm.option") ? pOpaque->Get("mgm.option") : "";
      std::string out;
      VstView::gVstView.Print(out, option.c_str());
      stdOut += out.c_str();
      retc = 0;
    }
  }
  else
  {
    stdErr += "error: you have to be root to list VSTs";
    retc = EPERM;
  }
  return SFS_OK;
}

EOSMGMNAMESPACE_END
