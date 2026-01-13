// ----------------------------------------------------------------------
// File: com_fuse.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright(C) 2011 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 *(at your option) any later version.                                  *
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
#include "common/StringTokenizer.hh"
/*----------------------------------------------------------------------------*/

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>

extern XrdOucString serveruri;

/* mount/umount via fuse */
int
com_fuse(char* arg1)
{
  if (interactive) {
    fprintf(stderr,
            "error: don't call <fuse> from an interactive shell - call via 'eos fuse ...'!\n");
    global_retc = -1;
    return 0;
  }

// split subcommands
  XrdOucString mountpoint = "";
  eos::common::StringTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString cmd = subtokenizer.GetToken();
  XrdOucString option = "";
  XrdOucString logfile = "";
  XrdCl::URL url(serveruri.c_str());
  XrdOucString params = "fsname=";

  if (url.GetHostName() == "localhost") {
    params += "localhost.localdomain";
  } else {
    params += url.GetHostName().c_str();
  }

  params += ":";
  params += url.GetPath().c_str();

  if (wants_help(arg1)) {
    goto com_fuse_usage;
  }

  if ((cmd != "mount") && (cmd != "umount")) {
    goto com_fuse_usage;
  }

  do {
    option = subtokenizer.GetToken();

    if (!option.length()) {
      break;
    }

    if (option.beginswith("-o")) {
      params = subtokenizer.GetToken();

      if (!params.length()) {
        goto com_fuse_usage;
      }
    } else {
      break;
    }
  } while (1);

  mountpoint = option;

  if (!mountpoint.length()) {
    goto com_fuse_usage;
  }

  if (mountpoint.beginswith("-")) {
    goto com_fuse_usage;
  }

  if (!mountpoint.beginswith("/")) {
    fprintf(stderr,
            "warning: assuming you gave a relative path with respect to current working directory => mountpoint=%s\n",
            mountpoint.c_str());
    XrdOucString pwd = getenv("PWD");

    if (!pwd.endswith("/")) {
      pwd += "/";
    }

    mountpoint.insert(pwd.c_str(), 0);
  }

  if (cmd == "mount") {
    struct stat buf;
    struct stat buf2;

    if (stat(mountpoint.c_str(), &buf)) {
      XrdOucString createdir = "mkdir -p ";
      createdir += mountpoint;
      createdir += " >& /dev/null";
      fprintf(stderr, ".... trying to create ... %s\n", mountpoint.c_str());
      int rc = system(createdir.c_str());

      if (WEXITSTATUS(rc)) {
        fprintf(stderr, "error: creation of mountpoint failed");
      }
    }

    if (stat(mountpoint.c_str(), &buf)) {
      fprintf(stderr, "error: cannot create mountpoint %s !\n", mountpoint.c_str());
      exit(-1);
    } else {
      if (buf.st_dev == 19) {
        fprintf(stderr, "error: already/still mounted on %s !\n", mountpoint.c_str());
        exit(EBUSY);
      }
    }

#ifdef __APPLE__
    params += " -onoappledouble,allow_root,defer_permissions,volname=EOS,iosize=65536,fsname=eos@cern.ch";
#endif
    fprintf(stderr, "===> Mountpoint   : %s\n", mountpoint.c_str());
    fprintf(stderr, "===> Fuse-Options : %s\n", params.c_str());
    XrdOucString mount;
    mount = "eosxd ";
    mount += mountpoint.c_str();
    mount += " -o";
    mount += params;
    fprintf(stderr, "running %s\n", mount.c_str());
#ifdef __APPLE__
    mount += " >& /dev/null";
#else
    mount += " >& /dev/null";
#endif
    int rc = system(mount.c_str());

    if (WEXITSTATUS(rc)) {
      fprintf(stderr, "error: failed mount, maybe still mounted? Check with "
              "df and eventually 'killall eosd'\n");
      exit(-1);
    }

#ifdef __APPLE__
    int cnt = 5;

    for (cnt = 5; cnt > 0; cnt--) {
      fprintf(stderr, "\r[wait] %i seconds ...", cnt);
      fflush(stderr);
      sleep(1);
    }

    fprintf(stderr, "\n");
#endif
    bool mountok = false;

    // Keep checking for 5 seconds
    for (size_t i = 0; i < 50; i++) {
      if (stat(mountpoint.c_str(), &buf2) || (buf2.st_ino == buf.st_ino)) {
        usleep(100000);

        if (i && (!(i % 10))) {
          fprintf(stderr, "[check] %zu. time for mount ...\n", i / 10);
        }
      } else {
        mountok = true;
        break;
      }
    }

    if (!mountok) {
      fprintf(stderr, "error: failed mount, maybe still mounted? Check with "
              "df and eventually 'killall eosd'\n");
      exit(-1);
    } else {
      fprintf(stderr, "info: successfully mounted EOS [%s] under %s\n",
              serveruri.c_str(), mountpoint.c_str());
    }
  }

  if (cmd == "umount") {
    struct stat buf2;
#ifndef __APPLE__
    struct stat buf1;
    XrdOucString pmount = mountpoint;

    if (pmount.endswith("/")) {
      pmount.erase(pmount.length() - 1);
    }

    pmount.erase(pmount.rfind('/'));
    int r1 = stat(mountpoint.c_str(), &buf1);
    int r2 = stat(pmount.c_str(), &buf2);

    if ((r1 || r2) || (buf1.st_dev == buf2.st_dev)) {
      fprintf(stderr, "error: there is no eos mount at %s\n", mountpoint.c_str());
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
#endif
    int rc = system(umount.c_str());

    if (WEXITSTATUS(rc)) {
      fprintf(stderr, "error: umount failed - maybe wasn't mounted?\n");
    }

    if ((stat(mountpoint.c_str(), &buf2))) {
      fprintf(stderr, "error: mount directory disappeared from %s\n",
              mountpoint.c_str());
      exit(-1);
    }

#ifndef __APPLE__

    if (buf1.st_ino == buf2.st_ino) {
      fprintf(stderr, "error: umount didn't work\n");
      exit(-1);
    }

#endif
  }

  exit(0);
com_fuse_usage:
  fprintf(stdout,
          "usage: fuse mount  <mount-point>                                         : mount connected eos instance on <mount-point>\n");
  fprintf(stdout,
          "       fuse umount <mount-point>                                         : unmount eos pool from <mount-point>\n");
  exit(-1);
}
