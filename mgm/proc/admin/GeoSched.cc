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
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include "mgm/proc/ProcInterface.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/GeoTreeEngine.hh"

EOSMGMNAMESPACE_BEGIN

int
ProcCommand::GeoSched()
{
  if (pVid->uid == 0) {
    retc = SFS_ERROR;

    if (mSubCmd == "showtree" || mSubCmd == "showsnapshot" ||
        mSubCmd == "showstate" || mSubCmd == "showparam") {
      XrdOucString schedgroup = "";
      XrdOucString optype = "";
      schedgroup = pOpaque->Get("mgm.schedgroup");
      optype = pOpaque->Get("mgm.optype");
      bool btree = (mSubCmd == "showtree");
      bool bsnapsh = (mSubCmd == "showsnapshot");
      bool bprm = (mSubCmd == "showparam");
      bool bst = (mSubCmd == "showstate");
      bool useColors = false;
      bool monitoring = pOpaque->Get("mgm.monitoring");

      if (pOpaque->Get("mgm.usecolors")) {
        useColors = (bool)XrdOucString(pOpaque->Get("mgm.usecolors")).atoi();
      }

      std::string info;
      gOFS->mGeoTreeEngine->printInfo(info, btree, bsnapsh, bprm, bst, schedgroup.c_str(),
                               optype.c_str(), useColors, monitoring);
      stdOut += info.c_str();
      retc = SFS_OK;
    }

    if (mSubCmd == "set") {
      XrdOucString param = pOpaque->Get("mgm.param");
      XrdOucString paramidx = pOpaque->Get("mgm.paramidx");
      XrdOucString value = pOpaque->Get("mgm.value");
      int iparamidx = paramidx.atoi();
      bool ok = false;
      // -> save it to the config
      ok = gOFS->mGeoTreeEngine->setParameter(param.c_str(), value.c_str(), iparamidx, true);
      retc = ok ? SFS_OK : SFS_ERROR;
    }

    if (mSubCmd == "updtpause") {
      if (gOFS->mGeoTreeEngine->PauseUpdater()) {
        stdOut += "GeoTreeEngine has been paused\n";
      } else {
        stdOut += "GeoTreeEngine could not be paused at the moment\n";
      }

      retc = SFS_OK;
    }

    if (mSubCmd == "updtresume") {
      gOFS->mGeoTreeEngine->ResumeUpdater();
      stdOut += "GeoTreeEngine has been resumed\n";
      retc = SFS_OK;
    }

    if (mSubCmd == "forcerefresh") {
      gOFS->mGeoTreeEngine->forceRefresh();
      stdOut += "GeoTreeEngine has been refreshed\n";
      retc = SFS_OK;
    }

    if (mSubCmd.beginswith("disabled")) {
      XrdOucString geotag = pOpaque->Get("mgm.geotag");
      XrdOucString group = pOpaque->Get("mgm.schedgroup");
      XrdOucString optype = pOpaque->Get("mgm.optype");

      if (mSubCmd == "disabledadd") {
        gOFS->mGeoTreeEngine->addDisabledBranch(group.c_str(), optype.c_str(), geotag.c_str(),
                                         &stdOut, true); // -> save it to the config
        retc = SFS_OK;
      }

      if (mSubCmd == "disabledrm") {
        gOFS->mGeoTreeEngine->rmDisabledBranch(group.c_str(), optype.c_str(), geotag.c_str(),
                                        &stdOut, true); // -> save it to the config
        retc = SFS_OK;
      }

      if (mSubCmd == "disabledshow") {
        gOFS->mGeoTreeEngine->showDisabledBranches(group.c_str(), optype.c_str(),
                                            geotag.c_str(), &stdOut);
        retc = SFS_OK;
      }
    }

    if (mSubCmd.beginswith("access")) {
      XrdOucString geotag = pOpaque->Get("mgm.geotag");
      XrdOucString geotag_list = pOpaque->Get("mgm.geotaglist");
      bool monitoring = pOpaque->Get("mgm.monitoring");

      if (mSubCmd == "accesssetdirect") {
        gOFS->mGeoTreeEngine->setAccessGeotagMapping(&stdOut, geotag.c_str(),
                                              geotag_list.c_str(), true);
        retc = SFS_OK;
      }

      if (mSubCmd == "accesscleardirect") {
        gOFS->mGeoTreeEngine->clearAccessGeotagMapping(&stdOut,
                                                geotag == "all" ? "" : geotag.c_str(), true);
        retc = SFS_OK;
      }

      if (mSubCmd == "accessshowdirect") {
        gOFS->mGeoTreeEngine->showAccessGeotagMapping(&stdOut, monitoring);
        retc = SFS_OK;
      }

      if (mSubCmd == "accesssetproxygroup") {
        gOFS->mGeoTreeEngine->setAccessProxygroup(&stdOut, geotag.c_str(), geotag_list.c_str(),
                                           true);
        retc = SFS_OK;
      }

      if (mSubCmd == "accessclearproxygroup") {
        gOFS->mGeoTreeEngine->clearAccessProxygroup(&stdOut,
                                             geotag == "all" ? "" : geotag.c_str(), true);
        retc = SFS_OK;
      }

      if (mSubCmd == "accessshowproxygroup") {
        gOFS->mGeoTreeEngine->showAccessProxygroup(&stdOut, monitoring);
        retc = SFS_OK;
      }
    }
  } else {
    retc = EPERM;
    stdErr = "error: you have to take role 'root' to execute this command";
  }

  return retc;
}

EOSMGMNAMESPACE_END
