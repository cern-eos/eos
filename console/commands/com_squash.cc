// ----------------------------------------------------------------------
// File: com_squash.cc
// Author: Andreas-Joachim Peters - CERN
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

#include "console/ConsoleMain.hh"
#include "common/StringTokenizer.hh"
#include "common/StringConversion.hh"
#include "common/Path.hh"
#include "XrdPosix/XrdPosixXrootd.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "pwd.h"

/* List a directory */
int
com_squash(char* arg1)
{
  eos::common::StringTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString cmd = "";
  XrdOucString path = "";
  XrdOucString option = "";
  XrdOucString fulloption = "";
  bool ok = false;
  const int len = 4096;
  char username[len];
  struct passwd* pw = getpwuid(geteuid());

  if (pw == nullptr) {
    fprintf(stderr, "error: failed to get effective UID username of calling "
            "process\n");
    goto com_squash_usage;
  }

  (void) strncpy(username, pw->pw_name, len - 1);
  username[len - 1] = '\0';

  do {
    cmd = subtokenizer.GetToken();
    path = subtokenizer.GetToken();
    
    if (!cmd.length()) {
      goto com_squash_usage;
    }
    
    if (cmd == "--help") {
      goto com_squash_usage;
    }
    
    if (cmd == "-h") {
      goto com_squash_usage;
    }

    if (path.length() && (path[0] == '-')) {
      option = path[1];
      fulloption = path;
      path = subtokenizer.GetToken();
    }

    if (!path.length()) {
      goto com_squash_usage;
    }

    XrdOucString garbage = subtokenizer.GetToken();

    if (garbage.length()) {
      goto com_squash_usage;
    } else {
      break;
    }
  } while (1);

  path = abspath(path.c_str());

  if (cmd == "new") {
    struct stat buf;
    eos::common::Path packagepath(path.c_str());

    if (!stat(packagepath.GetPath(), &buf)) {
      fprintf(stderr, "error: package path='%s' exists already\n",
              packagepath.GetPath());
      global_retc = EEXIST;
      return (0);
    }

    std::string mkpath = "/var/tmp/";
    mkpath += username;
    mkpath += "/eosxd/mksquash/";
    mkpath += packagepath.GetContractedPath();
    mkpath += "/dummy";
    eos::common::Path mountpath(mkpath.c_str());

    if (!mountpath.MakeParentPath(S_IRWXU)) {
      fprintf(stderr, "error: failed to create local mount point path='%s'\n",
              mountpath.GetParentPath());
      global_retc = errno;
      return (0);
    }

    if (symlink(mountpath.GetParentPath(), packagepath.GetPath())) {
      fprintf(stderr, "error: failed to create symbolic link from '%s' => '%s'\n",
              mountpath.GetParentPath(), packagepath.GetPath());
      global_retc = errno;
      return (0);
    }

    ok = true;
    fprintf(stderr, "info: ready to install your software under '%s'\n",
            packagepath.GetPath());
    fprintf(stderr, "info: when done run 'eos squash pack %s' to create an "
            "image file and a smart link in EOS!\n", packagepath.GetPath());
  }
  
  if (cmd == "install") {
    if (fulloption.beginswith("--curl=")) {
      ok = true;
      std::string url = fulloption.c_str() + 7;
      if (fulloption.endswith(".tgz") ||
	  fulloption.endswith(".tar.gz")) {
	int sub_rc = 0;
	// squash rm
	std::string subcommand = "rm ";
	subcommand += path.c_str();
	com_squash((char*)subcommand.c_str());
	sub_rc |= global_retc;
	// squash new
	subcommand = "new ";
	subcommand += path.c_str();
	com_squash((char*)subcommand.c_str());
	sub_rc |= global_retc;
	// download
	std::string shellcmd="cd "; 
	shellcmd += path.c_str();
	shellcmd += ";";
	shellcmd += "curl ";
	shellcmd += url;
	shellcmd += " /dev/stdout | ";
	shellcmd += "tar xvzf -";
	
	int rc = system(shellcmd.c_str());
	if (WEXITSTATUS(rc)) {
	  fprintf(stderr, "error: curl download failed with retc='%d'\n", WEXITSTATUS(rc));
	  global_retc = WEXITSTATUS(rc);
	  return (0);
	}
	// squash pack
	subcommand = "pack ";
	subcommand += path.c_str();
	com_squash((char*)subcommand.c_str());
	sub_rc |= global_retc;
	if (sub_rc) {
	  global_retc = sub_rc;
	  return 0;
	}
      } else {
	fprintf(stderr,"error: suffix of '%s' is not supported\n", url.c_str());
	global_retc = EINVAL;
	return (0);
      }
    } else {
      goto com_squash_usage;
    }
  }

  if (cmd == "pack") {
    eos::common::Path packagepath(path.c_str());
    std::string squashpack = packagepath.GetParentPath();
    squashpack += ".";
    squashpack += packagepath.GetName();
    squashpack += ".sqsh";
    std::string shellcmd = "mksquashfs ";
    char linktarget[4096];
    ssize_t rl;

    // resolve symlink
    if ((rl = readlink(packagepath.GetPath(), linktarget,
                       sizeof(linktarget))) == -1) {
      fprintf(stderr,
              "error: failed to resolve symbolic link of squashfs package '%s'\n - errno '%d'",
              packagepath.GetPath(), errno);
      global_retc = errno;
      return (0);
    } else {
      linktarget[rl] = 0;
    }

    struct stat buf;

    if (stat(linktarget, &buf)) {
      fprintf(stderr, "error: cannot find local package directory '%s'\n",
              linktarget);
      global_retc = errno;
      return (0);
    }

    shellcmd += linktarget;
    shellcmd += " ";
    shellcmd += squashpack;
    shellcmd += "~";
    shellcmd += " -noappend";
    shellcmd += " -force-uid ";
    shellcmd += std::to_string(geteuid());
    shellcmd += " -force-gid ";
    shellcmd += std::to_string(getegid());
    shellcmd += " && mv -f -T ";
    shellcmd += squashpack;
    shellcmd += "~";
    shellcmd += " ";
    shellcmd += squashpack;

    fprintf(stderr, "running %s\n", shellcmd.c_str());
    int rc = system(shellcmd.c_str());

    if (WEXITSTATUS(rc)) {
      fprintf(stderr, "error: mksquashfs failed with retc='%d'\n", WEXITSTATUS(rc));
      global_retc = WEXITSTATUS(rc);
      return (0);
    } else {
      if (option!= "f") {
	if (unlink(packagepath.GetPath())) {
	  fprintf(stderr,
		  "error: failed to unlink locally staged squashfs archive '%s' - errno '%d'\n",
		  squashpack.c_str(), errno);
	  global_retc = errno;
	  return (0);
	} else {
	  std::string targetline = "eosxd get eos.hostport ";
	  targetline += packagepath.GetParentPath();
	  
	  std::string hostport = eos::common::StringConversion::StringFromShellCmd(targetline.c_str());
	  if (!hostport.length()) {
	    fprintf(stderr, "error: failed to get eos.hostport from mountpoint '%s'\n", targetline.c_str());
	    global_retc = EIO;
	    return (0);
	  }
	  
	  std::string target = "/eos/squashfs/";
	  target += hostport; 
	  target += "@";
	  XrdOucString spackagepath = squashpack.c_str();
	  while(spackagepath.replace("/","---")) {}
	  target += spackagepath.c_str();
	  
	  if (symlink(target.c_str(), packagepath.GetPath())) {
	    fprintf(stderr, "error: failed to create squashfs symlink '%s' => '%s'\n",
		    packagepath.GetPath(),
		    target.c_str());
	  }
	}
      }
    }

    ok = true;
  }

  if (cmd == "relabel") {
    ok = true;
    struct stat buf;
    eos::common::Path packagepath(path.c_str());
    std::string squashpack = packagepath.GetParentPath();
    squashpack += ".";
    squashpack += packagepath.GetName();
    squashpack += ".sqsh";

    if (!lstat(packagepath.GetPath(), &buf)) {
      if (unlink(packagepath.GetPath())) {
        fprintf(stderr,
                "error: failed to remove existing squashfs archive '%s' - errno '%d'\n",
                packagepath.GetPath(), errno);
        global_retc = errno;
        return (0);
      }
    }
    std::string targetline = "eosxd get eos.hostport ";
    targetline += packagepath.GetParentPath();

    std::string hostport = eos::common::StringConversion::StringFromShellCmd(targetline.c_str());
    if (!hostport.length()) {
      fprintf(stderr, "error: failed to get eos.hostport from mountpoint '%s'\n", targetline.c_str());
      global_retc = EIO;
      return (0);
    }

    std::string target = "/eos/squashfs/";
    target += hostport; 
    target += "@";
    XrdOucString spackagepath = squashpack.c_str();
    while(spackagepath.replace("/","---")) {}
    target += spackagepath.c_str();
    
    if (symlink(target.c_str(), packagepath.GetPath())) {
      fprintf(stderr, "error: failed to create squashfs symlink '%s' => '%s'\n",
	      packagepath.GetPath(),
	      target.c_str());
    }
  }

  if (cmd == "unpack") {
    ok = true;
    eos::common::Path packagepath(path.c_str());
    std::string squashpack = packagepath.GetParentPath();
    squashpack += ".";
    squashpack += packagepath.GetName();
    squashpack += ".sqsh";
    char linktarget[4096];
    ssize_t rl;

    // resolve symlink
    if ((rl = readlink(packagepath.GetPath(), linktarget,
                       sizeof(linktarget))) == -1) {
      fprintf(stderr,
              "error: failed to resolve symbolic link of squashfs package '%s'\n - errno '%d'",
              packagepath.GetPath(), errno);
      global_retc = errno;
      return (0);
    } else {
      linktarget[rl] = 0;
    }

    XrdOucString mounttarget = linktarget;
    std::string mkpath = "/var/tmp/";
    mkpath += username;
    mkpath += "/eosxd/mksquash/";

    if (option != "f") {
      if (mounttarget.beginswith(mkpath.c_str())) {
	fprintf(stderr, "error: squash image is already unpacked!\n");
	global_retc = EINVAL;
	return (0);
      }
      
      // remove any mounts
      std::string umountcmd = "umount -f -l ";
      umountcmd += mounttarget.c_str();
      system(umountcmd.c_str());
      
      if (rmdir(mounttarget.c_str())) {
	if (errno != ENOENT) {
	  fprintf(stderr,
		  "error: failed to unlink local mount directory path='%s' errno=%d\n",
		  mounttarget.c_str(), errno);
	}
      }
    }

    std::string shellcmd = "unsquashfs -f -d ";
    mkpath += packagepath.GetContractedPath();
    mkpath += "/dummy";
    eos::common::Path mountpath(mkpath.c_str());

    if (!mountpath.MakeParentPath(S_IRWXU)) {
      fprintf(stderr, "error: failed to create local mount point path='%s'\n",
              mountpath.GetParentPath());
      global_retc = errno;
      return (0);
    }

    if (unlink(packagepath.GetPath())) {
      fprintf(stderr,
              "error: failed to unlink smart link for squashfs archive '%s' - errno '%d'\n",
              squashpack.c_str(), errno);
      global_retc = errno;
      return (0);
    }

    if (symlink(mountpath.GetParentPath(), packagepath.GetPath())) {
      fprintf(stderr, "error: failed to create symbolic link from '%s' => '%s'\n",
              mountpath.GetParentPath(), packagepath.GetPath());
      global_retc = errno;
      return (0);
    }

    shellcmd += mountpath.GetParentPath();
    shellcmd.erase(shellcmd.length()-1);
    shellcmd += "~";
    shellcmd += " ";
    shellcmd += squashpack.c_str();
    shellcmd += " && rsync -aq --delete "; 
    shellcmd += mountpath.GetParentPath();
    shellcmd.erase(shellcmd.length()-1);
    shellcmd += "~/";
    shellcmd += " ";
    shellcmd += mountpath.GetParentPath();

    shellcmd += " && rm -rf ";
    shellcmd += mountpath.GetParentPath();
    shellcmd.erase(shellcmd.length()-1);
    shellcmd += "~";

    fprintf(stdout, "%s\n", shellcmd.c_str());
    int rc = system(shellcmd.c_str());

    if (WEXITSTATUS(rc)) {
      fprintf(stderr, "error: unsquashfs failed with retc='%d'\n", WEXITSTATUS(rc));
      global_retc = WEXITSTATUS(rc);
      return (0);
    } else {
      fprintf(stderr, "info: squashfs image is available unpacked under '%s'\n",
              packagepath.GetPath());
      fprintf(stderr,
              "info: when done with modifications run 'eos squash pack %s' to create an image file and a smart link in EOS!\n",
              packagepath.GetPath());
    }
  }

  if (cmd == "info") {
    ok = true;
    eos::common::Path packagepath(path.c_str());
    std::string squashpack = packagepath.GetParentPath();
    squashpack += ".";
    squashpack += packagepath.GetName();
    squashpack += ".sqsh";
    struct stat buf;

    if (!stat(squashpack.c_str(), &buf)) {
      fprintf(stderr, "info: '%s' has a squashfs image with size=%lu bytes\n",
              squashpack.c_str(), (unsigned long)buf.st_size);
    } else {
      fprintf(stderr, "info: '%s' has no squashfs image\n", squashpack.c_str());
    }

    char linktarget[4096];
    ssize_t rl;

    // resolve symlink
    if ((rl = readlink(packagepath.GetPath(), linktarget,
                       sizeof(linktarget))) == -1) {
      fprintf(stderr,
              "error: failed to resolve symbolic link of squashfs package '%s'\n - errno '%d'",
              packagepath.GetPath(), errno);
      global_retc = errno;
      return (0);
    } else {
      linktarget[rl] = 0;
    }

    if (stat(linktarget, &buf)) {
      if (!S_ISLNK(buf.st_mode)) {
        fprintf(stderr, "error: this does not look like a squash image smart link\n");
      } else {
        fprintf(stderr, "error: cannot find local package directory '%s'\n",
                linktarget);
      }

      global_retc = EINVAL;
      return (0);
    }

    XrdOucString mounttarget = linktarget;
    std::string mkpath = "/var/tmp/";
    mkpath += username;
    mkpath += "/eosxd/mksquash/";

    if (mounttarget.beginswith(mkpath.c_str())) {
      fprintf(stderr,
              "info: squashfs image is currently unpacked/open for local RW mode - use 'eos squash pack %s' to close image\n",
              packagepath.GetPath());
    } else {
      fprintf(stderr,
              "info: squashfs image is currently open in RO mode - use 'eos squash unpack %s' to open image locally\n",
              packagepath.GetPath());
    }
  }

  if (cmd == "rm") {
    ok = true;
    eos::common::Path packagepath(path.c_str());
    std::string squashpack = packagepath.GetParentPath();
    squashpack += ".";
    squashpack += packagepath.GetName();
    squashpack += ".sqsh";
    struct stat buf;

    if (!stat(squashpack.c_str(), &buf)) {
      if (unlink(squashpack.c_str())) {
        fprintf(stderr,
                "error: failed to remove existing squashfs archive '%s' - errno '%d'\n",
                squashpack.c_str(), errno);
        global_retc = errno;
        return (0);
      } else {
        fprintf(stderr, "info: removed squashfs image '%s'\n", squashpack.c_str());
      }
    }

    if (!lstat(packagepath.GetPath(), &buf)) {
      if (unlink(packagepath.GetPath())) {
        fprintf(stderr,
                "error: failed to unlink locally staged squashfs archive '%s' - errno '%d'\n",
                squashpack.c_str(), errno);
        global_retc = errno;
        return (0);
      } else {
        fprintf(stderr, "info: removed squashfs smart link '%s\n",
                packagepath.GetPath());
      }
    }
  }

  if (!ok) {
    goto com_squash_usage;
  }

  return (0);
com_squash_usage:
  fprintf(stdout,
          "usage: squash new <path>                                                  : create a new squashfs under <path>\n");
  fprintf(stdout,"\n");
  fprintf(stdout,
          "       squash pack [-f] <path>                                            : pack a squashfs image\n");
  fprintf(stdout, 
	  "                                                                            -f will recreate the package but keeps the symbolic link locally\n");         
  fprintf(stdout,"\n");

  fprintf(stdout,
          "       squash unpack [-f] <path>                                          : unpack a squashfs image for modification\n");
  fprintf(stdout, 
	  "                                                                            -f will atomically update the local package\n");
  fprintf(stdout,"\n");
  
  fprintf(stdout,
          "       squash info <path>                                                 : squashfs information about <path>\n");
  fprintf(stdout,"\n");
  fprintf(stdout,
          "       squash rm <path>                                                   : delete a squashfs attached image and its smart link\n");
  fprintf(stdout,"\n");
  fprintf(stdout,       
	  "       squash relabel <path>                                              : relable a squashfs image link e.g. after an image move in the namespace\n");
  fprintf(stdout,"\n");

  fprintf(stdout, 
	  "       squash roll <path>                                                 : will create a squash package from the EOS directory pointed by <path\n");
  fprintf(stdout,"\n");
  fprintf(stdout, 
	  "       squash unroll <path>                                               : will store the squash package contents unpacked into the EOS package directory\n");
  fprintf(stdout,"\n");
  fprintf(stdout, 
	  "       squash install --curl=https://<package>.tgz|.tar.gz <path>         : create a squashfs package from a web archive under <path>\n");
  global_retc = EINVAL;
  return (0);
}
