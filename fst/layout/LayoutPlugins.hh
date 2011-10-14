// ----------------------------------------------------------------------
// File: LayoutPlugins.hh
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

#ifndef __EOSFST_LAYOUTPLUGIN_HH__
#define __EOSFST_LAYOUTPLUGIN_HH__

/*----------------------------------------------------------------------------*/
#include "common/LayoutId.hh"
#include "fst/Namespace.hh"
#include "fst/XrdFstOfsFile.hh"
#include "fst/layout/Layout.hh"
#include "fst/layout/PlainLayout.hh"
#include "fst/layout/ReplicaLayout.hh"
#include "fst/layout/ReplicaParLayout.hh"
#include "fst/layout/Raid5Layout.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

class LayoutPlugins {
public:
  LayoutPlugins() {};
  ~LayoutPlugins() {};

  static Layout* GetLayoutObject(XrdFstOfsFile* thisFile, unsigned int layoutid, XrdOucErrInfo *error) {
    if (eos::common::LayoutId::GetLayoutType(layoutid) == eos::common::LayoutId::kPlain) {
      return (Layout*)new PlainLayout(thisFile, layoutid, error);
    }
    if (eos::common::LayoutId::GetLayoutType(layoutid) == eos::common::LayoutId::kReplica) {
      return (Layout*)new ReplicaParLayout(thisFile,layoutid, error);
    }
    if (eos::common::LayoutId::GetLayoutType(layoutid) == eos::common::LayoutId::kRaid5) {
      return (Layout*)new Raid5Layout(thisFile,layoutid, error);
    }
    return 0;
  }
};

EOSFSTNAMESPACE_END

#endif
