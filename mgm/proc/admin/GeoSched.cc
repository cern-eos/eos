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
    if(mSubCmd == "showtree" || mSubCmd=="showsnapahot" || mSubCmd=="showstate")
    {
      XrdOucString schedgroup = "";
      XrdOucString optype = "";
      schedgroup = pOpaque->Get("mgm.schedgroup");
      optype = pOpaque->Get("mgm.optype");
      bool btree = (mSubCmd == "showtree");
      bool bsnapsh = (mSubCmd == "showsnapshot");
      bool bls =  (mSubCmd == "showstate");
      std::string info;
      gGeoTreeEngine.printInfo(info,btree,bsnapsh,bls,schedgroup.c_str(),optype.c_str());
      stdOut += info.c_str();
      retc = SFS_OK;
    }
    if(mSubCmd == "set")
    {
      XrdOucString param = pOpaque->Get("mgm.param");
      XrdOucString value = pOpaque->Get("mgm.value");
      int ival = value.atoi();
      bool ok = false;
      if(param == "timeFrameDurationMs")
      {
	ok = gGeoTreeEngine.setTimeFrameDurationMs(ival);
      }
      else if(param == "saturationThres")
      {
	ok = gGeoTreeEngine.setSaturationThres((char)ival);
      }
      else if(param == "fillRatioCompTol")
      {
	ok = gGeoTreeEngine.setFillRatioCompTol((char)ival);
      }
      else if(param == "fillRatioLimit")
      {
	ok = gGeoTreeEngine.setFillRatioLimit((char)ival);
      }
      else if(param == "accessUlScorePenalty")
      {
	ok = gGeoTreeEngine.setAccessUlScorePenalty((char)ival);
      }
      else if(param == "accessDlScorePenalty")
      {
	ok = gGeoTreeEngine.setAccessDlScorePenalty((char)ival);
      }
      else if(param == "plctUlScorePenalty")
      {
	ok = gGeoTreeEngine.setPlctUlScorePenalty((char)ival);
      }
      else if(param == "plctDlScorePenalty")
      {
	ok = gGeoTreeEngine.setPlctDlScorePenalty((char)ival);
      }
      else if(param == "skipSaturatedBlcPlct")
      {
	ok = gGeoTreeEngine.setSkipSaturatedBlcPlct((bool)ival);
      }
      else if(param == "skipSaturatedDrnPlct")
      {
	ok = gGeoTreeEngine.setSkipSaturatedDrnPlct((bool)ival);
      }
      else if(param == "skipSaturatedBlcAccess")
      {
	ok = gGeoTreeEngine.setSkipSaturatedBlcAccess((bool)ival);
      }
      else if(param == "skipSaturatedDrnAccess")
      {
	ok = gGeoTreeEngine.setSkipSaturatedDrnAccess((bool)ival);
      }
      else if(param == "skipSaturatedAccess")
      {
	ok = gGeoTreeEngine.setSkipSaturatedAccess((bool)ival);
      }
      else if(param == "skipSaturatedPlct")
      {
	ok = gGeoTreeEngine.setSkipSaturatedPlct((bool)ival);
      }

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
