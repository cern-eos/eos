// ----------------------------------------------------------------------
// File: XrdMgmOfsDirectory.hh
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

/*----------------------------------------------------------------------------*/
/**
 * @file   XrdMgmOfsDirectory.hh
 * 
 * @brief  XRootD OFS plugin class implementing directory handling of EOS
 * 
 */
/*----------------------------------------------------------------------------*/

#ifndef __EOSMGM_MGMOFSDIRECTORY__HH__
#define __EOSMGM_MGMOFSDIRECTORY__HH__

/*----------------------------------------------------------------------------*/
#include "common/Logging.hh"
#include "common/Mapping.hh"
#include "namespace/ContainerMD.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucErrInfo.hh"
#include "XrdSec/XrdSecEntity.hh"
#include "XrdSfs/XrdSfsInterface.hh"
/*----------------------------------------------------------------------------*/
#include <dirent.h>
#include <string>
#include <set>
/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/
//! Class implementing directories and operations
/*----------------------------------------------------------------------------*/
class XrdMgmOfsDirectory : public XrdSfsDirectory, public eos::common::LogId
{
public:

  // ---------------------------------------------------------------------------
  // open a directory
  // ---------------------------------------------------------------------------
  int open (const char *dirName,
            const XrdSecClientName *client = 0,
            const char *opaque = 0);

  // ---------------------------------------------------------------------------
  // open a directory by vid
  // ---------------------------------------------------------------------------
  int open (const char *dirName,
            eos::common::Mapping::VirtualIdentity &vid,
            const char *opaque = 0);

  // ---------------------------------------------------------------------------
  // open a directory by vid
  // ---------------------------------------------------------------------------
  int _open (const char *dirName,
            eos::common::Mapping::VirtualIdentity &vid,
            const char *opaque = 0);
  
  // ---------------------------------------------------------------------------
  // return entry of an open directory
  // ---------------------------------------------------------------------------
  const char *nextEntry ();

  // ---------------------------------------------------------------------------
  // return error message
  // ---------------------------------------------------------------------------
  int Emsg (const char *, XrdOucErrInfo&, int, const char *x,
            const char *y = "");

  // ---------------------------------------------------------------------------
  // close an open directory
  // ---------------------------------------------------------------------------
  int close ();

  // ---------------------------------------------------------------------------
  //! return name of an open directory
  // ---------------------------------------------------------------------------

  const char *
  FName ()
  {
    return (const char *) dirName.c_str ();
  }

  // ---------------------------------------------------------------------------
  //! Constructor
  // ---------------------------------------------------------------------------

  XrdMgmOfsDirectory (char *user = 0, int MonID = 0) : XrdSfsDirectory (user, MonID)
  {
    dirName = "";
    dh = 0;
    d_pnt = &dirent_full.d_entry;
    eos::common::Mapping::Nobody (vid);
    eos::common::LogId ();
  }

  // ---------------------------------------------------------------------------
  //! Destructor
  // ---------------------------------------------------------------------------

  ~XrdMgmOfsDirectory () { }
private:

  struct
  {
    struct dirent d_entry;
    char pad[MAXNAMLEN]; // This is only required for Solaris!
  } dirent_full;

  struct dirent *d_pnt;
  std::string dirName;
  eos::common::Mapping::VirtualIdentity vid;

  eos::ContainerMD* dh;
  std::set<std::string> dh_list;
  std::set<std::string>::const_iterator dh_it;
};


#endif
