// ----------------------------------------------------------------------
// File: proc_fs.hh
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

#ifndef __EOSMGM_PROC_FS__HH__
#define __EOSMGM_PROC_FS__HH__

/*----------------------------------------------------------------------------*/
#include "mgm/Namespace.hh"
#include "common/Logging.hh"
#include "common/Mapping.hh"
#include "mgm/FileSystem.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdSfs/XrdSfsInterface.hh"
#include "XrdSec/XrdSecEntity.hh"
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

int proc_fs_dumpmd(std::string &fsidst, XrdOucString &dp, XrdOucString &df, XrdOucString &ds, XrdOucString &stdOut, XrdOucString  &stdErr, std::string &tident, eos::common::Mapping::VirtualIdentity &vid_in);

int proc_fs_config(std::string &identifier, std::string &key, std::string &value, XrdOucString &stdOut, XrdOucString  &stdErr, std::string &tident, eos::common::Mapping::VirtualIdentity &vid_in);

int proc_fs_add(std::string &sfsid, std::string &uuid, std::string &nodename, std::string &mountpoint, std::string &space, std::string &configstatus, XrdOucString &stdOut, XrdOucString  &stdErr, std::string &tident, eos::common::Mapping::VirtualIdentity &vid_in);

FileSystem* proc_fs_source(std::string source_group, std::string target_group);

std::string proc_fs_target(std::string target_group);

int proc_fs_mv(std::string &sfsid, std::string &space, XrdOucString &stdOut, XrdOucString  &stdErr, std::string &tident, eos::common::Mapping::VirtualIdentity &vid_in);

int proc_fs_rm(std::string &nodename, std::string &mountpoint, std::string &id, XrdOucString &stdOut, XrdOucString  &stdErr, std::string &tident, eos::common::Mapping::VirtualIdentity &vid_in);

int proc_fs_dropdeletion(std::string &id, XrdOucString &stdOut, XrdOucString  &stdErr, std::string &tident, eos::common::Mapping::VirtualIdentity &vid_in);

EOSMGMNAMESPACE_END

#endif
