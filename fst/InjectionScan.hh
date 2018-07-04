// ----------------------------------------------------------------------
// File: InjectionScan.hh
// Author: Mihai Patrascoiu - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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

#ifndef __EOSFST_INJECTIONSCAN_HH__
#define __EOSFST_INJECTIONSCAN_HH__

/*----------------------------------------------------------------------------*/
#include "fst/Namespace.hh"
#include "common/FileId.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucTokenizer.hh"

/*----------------------------------------------------------------------------*/

class XrdOucEnv;

EOSFSTNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
class InjectionScan
{
public:

  unsigned long fsId;
  XrdOucString managerId;
  XrdOucString extPath;
  XrdOucString lclPath;
  XrdOucString opaque;

  InjectionScan(unsigned long fsid, const char* managerid, const char* extpath,
                const char* lclpath, const char* inopaque)
  {
    fsId = fsid;
    managerId = managerid;
    extPath = extpath;
    lclPath = lclpath;
    opaque = inopaque;
  }

  static InjectionScan*
  Create(XrdOucEnv* capOpaque)
  {
    // decode the opaque tags
    const char* sfsid = 0;
    const char* smanager = 0;
    const char* extPath = 0;
    const char* lclPath = 0;
    unsigned long fsid = 0;

    sfsid = capOpaque->Get("mgm.fsid");
    smanager = capOpaque->Get("mgm.manager");
    extPath = capOpaque->Get("mgm.extpath");
    lclPath = capOpaque->Get("mgm.lclpath");

    if (!sfsid  || !extPath || !lclPath || !smanager) {
      return 0;
    }

    int envlen = 0;
    fsid = atoi(sfsid);
    return new InjectionScan(fsid, smanager, extPath, lclPath,
                             capOpaque->Env(envlen));
  };

  ~InjectionScan() { };

  //----------------------------------------------------------------------------
  //! Display information about current injection scan job
  //----------------------------------------------------------------------------
  void
  Show(const char* show = "")
  {
    eos_static_info("InjectionScan fs=%u external_path=%s local_path=%s %s",
                    fsId, extPath.c_str(), lclPath.c_str(), show);
  }
};

EOSFSTNAMESPACE_END

#endif
