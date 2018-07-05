//------------------------------------------------------------------------------
//! @file SubMount.cc
//! @author Andreas-Joachim Peters CERN
//! @brief Class managing sub-mounts
//------------------------------------------------------------------------------

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



#include "common/ShellCmd.hh"
#include "common/Path.hh"
#include "submount/SubMount.hh"
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

int
/* -------------------------------------------------------------------------- */
SubMount::mount(std::string &params, std::string &localpath, std::string &env)
/* -------------------------------------------------------------------------- */
{
  int rc = 0;
  std::string mountcmd = env + " mount ";
  mountcmd += params.substr(6);

  if (geteuid())
    params = "/var/tmp/eosxd/mnt/";
  else
    params = "/var/run/eosxd/mnt/";

  params += localpath;

  struct stat buf;
  if (::stat(params.c_str(), &buf)) {
    std::string mkpath = params + "/dummy";
    eos::common::Path mountpath(mkpath.c_str());
    if (!mountpath.MakeParentPath(S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH)) {
      eos_static_warning("failed to create local mount point path='%s'", params.c_str());
      return -1;
    }

    mountcmd += " ";
    mountcmd += params;
    eos_static_warning("mount='%s' local-path='%s'", mountcmd.c_str(), localpath.c_str());
#ifndef __APPLE__
    eos::common::ShellCmd cmd(mountcmd);
    eos::common::cmd_status st = cmd.wait(5);
    rc = st.exit_code;
    if (!rc) {
      XrdSysMutexHelper lock(iMutex);
      mtab[params] = localpath;
    }
#else
    rc = EOPNOTSUPP;
#endif
  }
  return rc;
}

int
/* -------------------------------------------------------------------------- */
SubMount::squashfuse(std::string &params, std::string &localpath, std::string &env)
/* -------------------------------------------------------------------------- */
{
  int rc = 0;
  std::string mountcmd = env + " squashfuse -o allow_other ";
  std::string imagepath = localpath;
  fprintf(stderr, "%s %s\n", params.c_str(), localpath.c_str());
  size_t spos = imagepath.rfind("/");
  imagepath.insert(spos + 1, ".");
  imagepath += ".sqsh";
  mountcmd += imagepath;
  mountcmd += " ";

  if (geteuid())
    params = "/var/tmp/eosxd/mnt/";
  else
    params = "/var/run/eosxd/mnt/";

  params += localpath;

  struct stat buf1;
  struct stat buf2;
  eos::common::Path ppath(params.c_str());

  if (::stat(params.c_str(), &buf1) || // the mount path does not exist at all
      ((!stat(ppath.GetParentPath(), &buf2)) && (buf2.st_dev == buf1.st_dev))) // there is nothing mounted here
  {
    std::string mkpath = params + "/dummy";
    eos::common::Path mountpath(mkpath.c_str());
    if (!mountpath.MakeParentPath(S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH)) {
      eos_static_warning("failed to create local mount point path='%s'", params.c_str());
      return -1;
    }

    mountcmd += " ";
    mountcmd += params;
    eos_static_warning("mount='%s' local-path='%s'", mountcmd.c_str(), localpath.c_str());
#ifndef __APPLE__
    eos::common::ShellCmd cmd(mountcmd);
    eos::common::cmd_status st = cmd.wait(5);
    rc = st.exit_code;
    if (!rc) {
      XrdSysMutexHelper lock(iMutex);
      mtab[params] = localpath;
    }
#else
    rc = EOPNOTSUPP;
#endif
  }
  return rc;
}

/* -------------------------------------------------------------------------- */
void
SubMount::terminate()
/* -------------------------------------------------------------------------- */
{
  XrdSysMutexHelper lock(iMutex);
  for (auto it = mtab.begin(); it != mtab.end(); ++it) {
    eos_static_warning("umount='%s' local-path='%s'", it->first.c_str(), it->second.c_str());
    std::string umountcmd = "umount -fl ";
    umountcmd += it->first;
    eos::common::ShellCmd cmd(umountcmd);
    eos::common::cmd_status st = cmd.wait(2);
    int rc = st.exit_code;
    if (rc) {
      eos_static_warning("umount='%s' failed", it->first.c_str());
    }
    rc = rmdir(it->first.c_str());
    if (rc) {
      eos_static_warning("rmdir of '%s' failed - errno = %d", it->first.c_str(), errno);
    }
  }
  mtab.clear();
}
