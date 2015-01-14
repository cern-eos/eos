// ----------------------------------------------------------------------
// File: proc/admin/GeoSched.cc
// Author: Geoffray Adde - CERN
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
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

int
ProcCommand::GeoSched ()
{
  if (pVid->uid == 0)
  {
    retc = SFS_ERROR;
    if(mSubCmd == "showtree" || mSubCmd=="showsnapshot" || mSubCmd=="showstate" || mSubCmd=="showparam")
    {
      XrdOucString schedgroup = "";
      XrdOucString optype = "";
      schedgroup = pOpaque->Get("mgm.schedgroup");
      optype = pOpaque->Get("mgm.optype");
      bool btree = (mSubCmd == "showtree");
      bool bsnapsh = (mSubCmd == "showsnapshot");
      bool bprm =  (mSubCmd == "showparam");
      bool bst =  (mSubCmd == "showstate");
      std::string info;
      gGeoTreeEngine.printInfo(info,btree,bsnapsh,bprm,bst,schedgroup.c_str(),optype.c_str());
      stdOut += info.c_str();
      retc = SFS_OK;
    }
    if(mSubCmd == "set")
    {
      XrdOucString param = pOpaque->Get("mgm.param");
      XrdOucString paramidx = pOpaque->Get("mgm.paramidx");
      XrdOucString value = pOpaque->Get("mgm.value");
      double dval = 0.0;
      sscanf(value.c_str(),"%lf",&dval);
      int ival = (int)dval;
      int iparamidx = paramidx.atoi();
      bool ok = false;
      ok = gGeoTreeEngine.setParameter(param.c_str(),value.c_str(),iparamidx);
      retc = ok?SFS_OK:SFS_ERROR;
    }
    if(mSubCmd == "updtpause")
    {
      gGeoTreeEngine.PauseUpdater();
      stdOut += "GeoTreeEngine has been paused\n";
      retc = SFS_OK;
    }
    if(mSubCmd == "updtresume")
    {
      gGeoTreeEngine.ResumeUpdater();
      stdOut += "GeoTreeEngine has been resumed\n";
      retc = SFS_OK;
    }
  }
  else
  {
    retc = EPERM;
    stdErr = "error: you have to take role 'root' to execute this command";
  }
  return retc;
}

EOSMGMNAMESPACE_END
