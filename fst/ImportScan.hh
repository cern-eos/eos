// ----------------------------------------------------------------------
// File: ImportScan.hh
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

#ifndef __EOSFST_IMPORTSCAN_HH__
#define __EOSFST_IMPORTSCAN_HH__

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
class ImportScan
{
public:

  unsigned long fsId;
  XrdOucString id;
  XrdOucString managerId;
  XrdOucString extPath;
  XrdOucString lclPath;
  XrdOucString opaque;

  ImportScan(const char* importid, unsigned long fsid, const char* managerid,
             const char* extpath, const char* lclpath, const char* inopaque)
  {
    id = importid;
    fsId = fsid;
    managerId = managerid;
    extPath = extpath;
    lclPath = lclpath;
    opaque = inopaque;
  }

  static ImportScan*
  Create(XrdOucEnv* capOpaque)
  {
    // decode the opaque tags
    const char* id = 0;
    const char* sfsid = 0;
    const char* smanager = 0;
    const char* extPath = 0;
    const char* lclPath = 0;
    unsigned long fsid = 0;

    id = capOpaque->Get("mgm.id");
    sfsid = capOpaque->Get("mgm.fsid");
    smanager = capOpaque->Get("mgm.manager");
    extPath = capOpaque->Get("mgm.extpath");
    lclPath = capOpaque->Get("mgm.lclpath");

    if (!id || !sfsid || !smanager || !extPath || !lclPath) {
      return 0;
    }

    int envlen = 0;
    fsid = atoi(sfsid);
    return new ImportScan(id, fsid, smanager, extPath,
                          lclPath, capOpaque->Env(envlen));
  };

  ~ImportScan() { };

  //----------------------------------------------------------------------------
  //! Display information about current import scan job
  //----------------------------------------------------------------------------
  void
  Show(const char* show = "")
  {
    eos_static_info("ImportScan[id=%s] fs=%u external_path=%s local_path=%s %s",
                    id.c_str(), fsId, extPath.c_str(), lclPath.c_str(), show);
  }
};

EOSFSTNAMESPACE_END

#endif
