// ----------------------------------------------------------------------
// File: com_fuse.cc
// Author: Andreas-Joachim Peters - CERN
// Author: Joaquim Rocha - CERN
// -----------------------------------B-----------------------------------

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
#include "console/ConsoleMain.hh"
/*----------------------------------------------------------------------------*/

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

extern XrdOucString serveruri;

/* mount/umount via fuse */
int
com_fuse (char* arg1)
{
  if (interactive)
  {
    fprintf(stderr, "Error: don't call <fuse> from an interactive shell - call via 'eos fuse ...'!\n");
    global_retc = -1;
    return 0;
  }

  ConsoleCliCommand *parsedCmd, *fuseCmd, *mountSubCmd, *umountSubCmd;
  XrdOucString mountpoint = "";
  XrdOucString option = "";
  XrdOucString logfile = "";
  XrdOucString fsname = serveruri;
  fsname.replace("root://", "");
  XrdOucString params = "kernel_cache,attr_timeout=30,entry_timeout=30,max_readahead=131072,max_write=4194304,fsname=";
  params += fsname;

  fuseCmd = new ConsoleCliCommand("fuse", "mount or unmount EOS pool");

  CliPositionalOption mountpointOption("mountpoint", "", 1, 1,
                                       "<mount-point>", true);

  mountSubCmd = new ConsoleCliCommand("mount", "mount connected EOS pool on "
                                  "<mount-point>");
  mountSubCmd->addOption(mountpointOption);
  mountSubCmd->addOptions(std::vector<CliOptionWithArgs>
			  {{"parameters", "", "-o", "<parameter-list>", false},
                           {"log-file", "", "-l", "<log-file>", false}
                          });
  fuseCmd->addSubcommand(mountSubCmd);

  umountSubCmd = new ConsoleCliCommand("umount", "unmount EOS pool from "
                                   "<mount-point>");
  umountSubCmd->addOption(mountpointOption);
  fuseCmd->addSubcommand(umountSubCmd);

  addHelpOptionRecursively(fuseCmd);

  parsedCmd = fuseCmd->parse(arg1);

  if (checkHelpAndErrors(parsedCmd))
  {
    exit(parsedCmd->hasValue("help") ? 0 : -1);
    return 0;
  }

  mountpoint = parsedCmd->getValue("mountpoint").c_str();

  if (!mountpoint.beginswith("/"))
  {
    fprintf(stderr, "warning: assuming you gave a relative path with respect to current working directory => mountpoint=%s\n", mountpoint.c_str());
    XrdOucString pwd = getenv("PWD");
    if (!pwd.endswith("/"))
    {
      pwd += "/";
    }
    mountpoint.insert(pwd.c_str(), 0);
  }

  if (parsedCmd == mountSubCmd)
  {
    if (mountSubCmd->hasValue("parameters"))
      params = mountSubCmd->getValue("parameters").c_str();

    if (mountSubCmd->hasValue("log-file"))
      logfile = mountSubCmd->getValue("log-file").c_str();

    struct stat buf;
    struct stat buf2;
    if (stat(mountpoint.c_str(), &buf))
    {
      XrdOucString createdir = "mkdir -p ";
      createdir += mountpoint;
      createdir += " >& /dev/null";
      fprintf(stderr, ".... trying to create ... %s\n", mountpoint.c_str());
      int rc = system(createdir.c_str());
      if (WEXITSTATUS(rc))
      {
        fprintf(stderr, "Error: creation of mountpoint failed");
      }
    }

    if (stat(mountpoint.c_str(), &buf))
    {
      fprintf(stderr, "\nerror: cannot create mountpoint %s !\n", mountpoint.c_str());
      exit(-1);
    }
    else
    {
      fprintf(stderr, "OK\n");
    }

    params += " ";
    params += serveruri.c_str();
    if ((params.find("//eos/") == STR_NPOS))
    {
      params += "//eos/";
    }

    fprintf(stderr, "===> Mountpoint   : %s\n", mountpoint.c_str());
    fprintf(stderr, "===> Fuse-Options : %s\n", params.c_str());
    if (logfile.length())
    {
      fprintf(stderr, "===> Log File     : %s\n", logfile.c_str());
    }

    XrdOucString env = "env";
    if (getenv("EOS_FUSE_READAHEADSIZE"))
    {
      env += " EOS_FUSE_READAHEADSIZE=";
      env += getenv("EOS_FUSE_READAHEADSIZE");
    }
    else
    {
      setenv("EOS_FUSE_READAHEADSIZE", "131072", 1);
      env += " EOS_FUSE_READAHEADSIZE=131072";
    }

    if (getenv("EOS_FUSE_READCACHESIZE"))
    {
      env += " EOS_FUSE_READCACHESIZE=";
      env += getenv("EOS_FUSE_READCACHESIZE");
    }
    else
    {
      setenv("EOS_FUSE_READCACHESIZE", "393216", 1);
      env += " EOS_FUSE_READCACHESIZE=393216";
    }

    if (getenv("EOS_FUSE_CACHE_WRITE"))
    {
      env += " EOS_FUSE_CACHE_WRITE=";
      env += getenv("EOS_FUSE_CACHE_WRITE");
    }
    else
    {
      setenv("EOS_FUSE_CACHE_WRITE", "1", 1);
      env += " EOS_FUSE_CACHE_WRITE=1";
    }

    if (getenv("EOS_FUSE_CACHE_READ"))
    {
      env += " EOS_FUSE_CACHE_READ=";
      env += getenv("EOS_FUSE_CACHE_READ");
    }
    else
    {
      setenv("EOS_FUSE_CACHE_READ", "0", 1);
      env += " EOS_FUSE_CACHE_READ=0";
    }

    if (getenv("EOS_FUSE_CACHE_SIZE"))
    {
      env += " EOS_FUSE_CACHE_SIZE=";
      env += getenv("EOS_FUSE_CACHE_SIZE");
    }
    else
    {
      setenv("EOS_FUSE_CACHE_SIZE", "100000000", 1);
      env += " EOS_FUSE_CACHE_SIZE=100000000";
    }

    if (getenv("EOS_FUSE_CACHE"))
    {
      env += " EOS_FUSE_CACHE=";
      env += getenv("EOS_FUSE_CACHE");
    }
    else
    {
      setenv("EOS_FUSE_CACHE", "1", 1);
      env += " EOS_FUSE_CACHE=1";
    }

    if (getenv("EOS_FUSE_DEBUG"))
    {
      env += " EOS_FUSE_DEBUG=";
      env += getenv("EOS_FUSE_DEBUG");
    }
    else
    {
      setenv("EOS_FUSE_DEBUG", "0", 1);
      env += " EOS_FUSE_DEBUG=0";
    }

    fprintf(stderr, "===> xrootd ra             : %s\n", getenv("EOS_FUSE_READAHEADSIZE"));
    fprintf(stderr, "===> xrootd cache          : %s\n", getenv("EOS_FUSE_READCACHESIZE"));
    fprintf(stderr, "===> fuse debug            : %s\n", getenv("EOS_FUSE_DEBUG"));
    fprintf(stderr, "===> fuse write-cache      : %s\n", getenv("EOS_FUSE_CACHE_WRITE"));
    fprintf(stderr, "===> fuse write-cache-size : %s\n", getenv("EOS_FUSE_CACHE_SIZE"));

    XrdOucString mount = env;
    mount += " eosfsd ";
    mount += mountpoint.c_str();
    mount += " -f";
    mount += " -o";
    mount += params;
    if (logfile.length())
    {
      mount += " -d >& ";
      mount += logfile;
      mount += " &";
      int rc = system(mount.c_str());
      if (WEXITSTATUS(rc))
      {
        fprintf(stderr, "Error: mount failed");
      }
      sleep(1);
    }
    else
    {
      mount += " -s >& /dev/null ";
      int rc = system(mount.c_str());
      if (WEXITSTATUS(rc))
      {
        fprintf(stderr, "Error: mount failed");
      }
    }


    if ((stat(mountpoint.c_str(), &buf2) || (buf2.st_ino == buf.st_ino)))
    {
      fprintf(stderr, "Error: mount failed at %s\n", mountpoint.c_str());
      exit(-1);
    }

  }
  else if (parsedCmd == umountSubCmd)
  {
    struct stat buf2;

#ifndef __APPLE__
    struct stat buf;

    if ((stat(mountpoint.c_str(), &buf) || (buf.st_ino != 1)))
    {
      fprintf(stderr, "Error: there is no eos mount at %s\n", mountpoint.c_str());
      exit(-1);
    }
#endif

    XrdOucString umount;
#ifdef __APPLE__
    umount = "umount -f ";
    umount += mountpoint.c_str();
    umount += " >& /dev/null";
#else
    umount = "fusermount -z -u ";
    umount += mountpoint.c_str();
    umount += " >& /dev/null";
#endif

    int rc = system(umount.c_str());
    if (WEXITSTATUS(rc))
    {
      fprintf(stderr, "Error: umount failed\n");
    }
    if ((stat(mountpoint.c_str(), &buf2)))
    {
      fprintf(stderr, "Error: mount directoy disappeared from %s\n", mountpoint.c_str());
      exit(-1);
    }

#ifndef __APPLE__
    if (buf.st_ino == buf2.st_ino)
    {
      fprintf(stderr, "Error: umount didn't work\n");
      exit(-1);
    }
#endif
  }

  delete fuseCmd;

  return 0;
}
