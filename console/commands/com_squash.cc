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

    if ((cmd != "trim-release") && (cmd != "new-release")) {
      XrdOucString garbage = subtokenizer.GetToken();

      if (garbage.length()) {
        goto com_squash_usage;
      } else {
        break;
      }
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

    if (!mountpath.MakeParentPath(S_IRWXU | S_IROTH | S_IXOTH | S_IRGRP |
                                  S_IXGRP)) {
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
        std::string subcommand = "rm \"";
        subcommand += path.c_str();
        subcommand += "\"";
        com_squash((char*)subcommand.c_str());
        sub_rc |= global_retc;
        // squash new
        subcommand = "new \"";
        subcommand += path.c_str();
        subcommand += "\"";
        com_squash((char*)subcommand.c_str());
        sub_rc |= global_retc;
        // download
        std::string shellcmd = "cd \"";
        shellcmd += path.c_str();
        shellcmd += "\";";
        shellcmd += "curl ";
        shellcmd += url;
        shellcmd += " /dev/stdout | ";
        shellcmd += "tar xvzf -";
        int rc = system(shellcmd.c_str());

        if (WEXITSTATUS(rc)) {
          fprintf(stderr, "error: curl download failed with retc='%d'\n",
                  WEXITSTATUS(rc));
          global_retc = WEXITSTATUS(rc);
          return (0);
        }

        // squash pack
        subcommand = "pack \"";
        subcommand += path.c_str();
        subcommand += "\"";
        com_squash((char*)subcommand.c_str());
        sub_rc |= global_retc;

        if (sub_rc) {
          global_retc = sub_rc;
          return 0;
        }
      } else {
        fprintf(stderr, "error: suffix of '%s' is not supported\n", url.c_str());
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
    memset(linktarget, 0, sizeof(linktarget));
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
      if (option != "f") {
        if (unlink(packagepath.GetPath())) {
          fprintf(stderr,
                  "error: failed to unlink locally staged squashfs archive '%s' - errno '%d'\n",
                  squashpack.c_str(), errno);
          global_retc = errno;
          return (0);
        } else {
          std::string targetline = "eosxd get eos.hostport ";
          targetline += packagepath.GetParentPath();
          std::string hostport = eos::common::StringConversion::StringFromShellCmd(
                                   targetline.c_str());

          if (!hostport.length()) {
            fprintf(stderr, "error: failed to get eos.hostport from mountpoint '%s'\n",
                    targetline.c_str());
            global_retc = EIO;
            return (0);
          }

          std::string target = "/eos/squashfs/";
          target += hostport;
          target += "@";
          XrdOucString spackagepath = squashpack.c_str();

          while (spackagepath.replace("/", "---")) {}

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

    if (stat(squashpack.c_str(), &buf)) {
      fprintf(stderr,
              "error: the squashfs package file is missing for this label!\n");
      global_retc = ENOENT;
      return (0);
    }

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
    std::string hostport = eos::common::StringConversion::StringFromShellCmd(
                             targetline.c_str());

    if (!hostport.length()) {
      fprintf(stderr, "error: failed to get eos.hostport from mountpoint '%s'\n",
              targetline.c_str());
      global_retc = EIO;
      return (0);
    }

    std::string target = "/eos/squashfs/";
    target += hostport;
    target += "@";
    XrdOucString spackagepath = squashpack.c_str();

    while (spackagepath.replace("/", "---")) {}

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

      if (!geteuid()) {
        // remove any mounts - only possible as root
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
    }

    std::string shellcmd = "unsquashfs -f -d ";
    mkpath += packagepath.GetContractedPath();
    mkpath += "/dummy";
    eos::common::Path mountpath(mkpath.c_str());

    if (!mountpath.MakeParentPath(S_IRWXU | S_IROTH | S_IXOTH | S_IRGRP |
                                  S_IXGRP)) {
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
    shellcmd.erase(shellcmd.length() - 1);
    shellcmd += "~";
    shellcmd += " ";
    shellcmd += squashpack.c_str();
    shellcmd += " && rsync -aq --delete ";
    shellcmd += mountpath.GetParentPath();
    shellcmd.erase(shellcmd.length() - 1);
    shellcmd += "~/";
    shellcmd += " ";
    shellcmd += mountpath.GetParentPath();
    shellcmd += " && rm -rf ";
    shellcmd += mountpath.GetParentPath();
    shellcmd.erase(shellcmd.length() - 1);
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

    XrdOucString mounttarget = linktarget;
    std::string mkpath = "/var/tmp/";
    mkpath += username;
    mkpath += "/eosxd/mksquash/";

    if (mounttarget.beginswith(mkpath.c_str())) {
      if (stat(linktarget, &buf)) {
        fprintf(stderr, "error: cannot find local package directory '%s'\n",
                linktarget);
        global_retc = EINVAL;
        return (0);
      }

      fprintf(stderr,
              "info: squashfs image is currently unpacked/open for local RW mode - use 'eos squash pack %s' to close image\n",
              packagepath.GetPath());
    } else {
      fprintf(stderr,
              "info: squashfs image is currently packed - use 'eos squash unpack %s' to open image locally\n",
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

  if (cmd == "rm-release") {
    std::string scmd = "info-release ";
    scmd += path.c_str();
    com_squash((char*) scmd.c_str());

    if (!global_retc) {
      fprintf(stderr, "info: wiping squashfs releases under '%s'\n", path.c_str());
      eos::common::Path packagepath(path.c_str());
      std::string nextrelease = std::string(packagepath.GetPath()) +
                                std::string("/next");
      std::string currentrelease = std::string(packagepath.GetPath()) +
                                   std::string("/current");
      std::string archive = std::string(packagepath.GetPath()) +
                            std::string("/.archive");
      fprintf(stdout, "info: wiping links current,next ... \n");
      ::unlink(currentrelease.c_str());
      ::unlink(nextrelease.c_str());

      if (archive.substr(0, 5) == "/eos/") {
        fprintf(stdout, "info: wiping archive ...\n");
        scmd = "eos rm -rf ";
        scmd += archive;
        std::string out = eos::common::StringConversion::StringFromShellCmd(
                            scmd.c_str());
        fprintf(stdout, "%s", out.c_str());
      }

      if (::rmdir(packagepath.GetPath())) {
        fprintf(stderr, "error: failed to clean squashfs release under '%s'\n",
                path.c_str());
        global_retc = errno;
      }

      return (0);
    } else {
      fprintf(stderr, "info: there is no squashfs release uner '%s'\n", path.c_str());
      return (0);
    }
  }

  if (cmd == "new-release") {
    eos::common::Path packagepath(path.c_str());
    XrdOucString version = subtokenizer.GetToken();
    std::string packagename = packagepath.GetName();
    std::string now =
      eos::common::StringConversion::StringFromShellCmd("date '+%Y%m%d%H%M%S'");

    if (version.length()) {
      // overwrite with the given version number
      now = version.c_str();
    }

    now.erase(now.find_last_not_of(" \n\r\t") + 1);
    std::string archivepath = std::string(packagepath.GetPath()) + std::string("/")
                              + std::string(".archive/");
    std::string archivepackage = archivepath + packagename + std::string("-") + now;
    std::string nextrelease = std::string(packagepath.GetPath()) +
                              std::string("/next");
    eos::common::Path archpath(archivepackage.c_str());

    if (!archpath.MakeParentPath(S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH)) {
      fprintf(stderr, "error: couldn't create '%s'\n", archpath.GetParentPath());
      global_retc = errno;
      return (0);
    }

    // in the unlikely case this command was executed within 1 second twice ...
    ::unlink(archivepackage.c_str());
    ::unlink(nextrelease.c_str());
    std::string scmd = "new \"";
    scmd += archivepackage;
    scmd += "\"";
    int rc = com_squash((char*)scmd.c_str());

    if (!rc) {
      rc = ::symlink(archivepackage.c_str(), nextrelease.c_str());

      if (rc) {
        fprintf(stderr, "error: failed to create symbolic link for next release '%s'\n",
                nextrelease.c_str());
        global_retc = errno;
        return (0);
      }
    } else {
      fprintf(stderr, "error: failed to create squash package for a new release\n");
      return (0);
    }

    fprintf(stderr, "info: install the new release under '%s'\n",
            nextrelease.c_str());
    return (0);
  }

  if (cmd == "pack-release") {
    char lname[4096];
    memset(lname, 0, sizeof(lname));
    eos::common::Path packagepath(path.c_str());
    std::string nextrelease = std::string(packagepath.GetPath()) +
                              std::string("/next");
    std::string currentrelease = std::string(packagepath.GetPath()) +
                                 std::string("/current");
    std::string hiddencurrentrelease = std::string(packagepath.GetPath()) +
                                       std::string("/.current");

    if (::readlink(nextrelease.c_str(), lname, sizeof(lname)) < 0) {
      fprintf(stderr, "error: failed to find an open release package under '%s'\n",
              nextrelease.c_str());
      global_retc = errno;
      return (0);
    } else {
      std::string scmd = "pack ";
      scmd += "\"";
      scmd += lname;
      scmd += "\"";
      int rc = com_squash((char*)scmd.c_str());

      if (!rc) {
        rc = ::unlink(nextrelease.c_str());

        if (rc) {
          fprintf(stderr, "error: failed to unlink open release package under '%s'\n",
                  nextrelease.c_str());
          global_retc = errno;
          return (0);
        }

        rc = ::symlink(lname, hiddencurrentrelease.c_str());

        if (rc) {
          fprintf(stderr, "error: failed to symlink current release package under '%s'\n",
                  hiddencurrentrelease.c_str());
          global_retc = errno;
          return (0);
        } else {
          rc = ::rename(hiddencurrentrelease.c_str(), currentrelease.c_str());

          if (rc) {
            fprintf(stderr, "error: failed to move '%s' to '%s'\n",
                    hiddencurrentrelease.c_str(), currentrelease.c_str());
            global_retc = errno;
            return (0);
          } else {
            fprintf(stdout, "info: new release available under '%s'\n",
                    currentrelease.c_str());
            return (0);
          }
        }
      } else {
        fprintf(stderr, "error: failed to pack squash package for a new release\n");
        return (0);
      }
    }
  }

  if (cmd == "info-release") {
    std::string scmd = "trim-release \"";
    scmd += path.c_str();
    scmd += "\" ";
    scmd += "999999 999999";
    com_squash((char*)scmd.c_str());
    return (0);
  }

  if (cmd == "trim-release") {
    eos::common::Path packagepath(path.c_str());
    XrdOucString keepdays = subtokenizer.GetToken();
    XrdOucString keepversions  = subtokenizer.GetToken();
    std::string current = packagepath.GetPath();
    std::string archive = packagepath.GetPath();
    current += "/current";
    archive += "/.archive";
    struct stat buf;

    if (::lstat(current.c_str(), &buf)) {
      fprintf(stderr, "error: I cannot find any current release under '%s'\n",
              current.c_str());
      global_retc = EINVAL;
      return (0);
    }

    if (::lstat(archive.c_str(), &buf)) {
      fprintf(stderr, "error: I cannot find any archive release under '%s'\n",
              archive.c_str());
      global_retc = EINVAL;
      return (0);
    }

    if (!keepdays.length()) {
      fprintf(stderr,
              "error: you have to specify the number of days you want to keep releases : squash trim-release <path> <n-days> [<max-versions]\n");
      global_retc = EINVAL;
      return (0);
    }

    size_t n_keepdays = strtol(keepdays.c_str(), 0, 10);

    if (!n_keepdays) {
      fprintf(stderr,
              "error: you have to specify the number of days you want to keep releases : squash trim-release <path> <n-days>\n");
      global_retc = EINVAL;
      return (0);
    }

    size_t n_keepversions = keepversions.length() ? strtol(keepversions.c_str(), 0,
                            10) : 0;

    if (!n_keepversions) {
      fprintf(stderr, "info: no !=0 version limit specified ...\n");
      keepversions = "1000000";
    } else {
      // we have to pass keepversions + 1 to the find commands
      keepversions = std::to_string(n_keepversions + 1).c_str();
    }

    std::string find1, find2, find3, find4, find5;
    find1 = find2 = find3 = find4 = find5 = "find ";
    find1 += packagepath.GetPath();
    find2 += packagepath.GetPath();
    find3 += packagepath.GetPath();
    find4 += packagepath.GetPath();
    find5 += packagepath.GetPath();
    find1 += " -type f -mtime +";
    find2 += " -type l -mtime +";
    find1 += keepdays.c_str();
    find2 += keepdays.c_str();
    find1 += " -delete";
    find2 += " -delete";
    find3 += "/.archive/ -type f -printf '%Ts\t%h/%f\n'     | sort -rn | tail -n +";
    find4 += "/.archive/ -type l -printf '%Ts\t%h/%f\n'     | sort -rn | tail -n +";
    find3 += keepversions.c_str();
    find4 += keepversions.c_str();
    find3 += " | cut -f2- | xargs -r rm";
    find4 += " | cut -f2- | xargs -r rm";
    find5 += " -type l";
    eos::common::StringConversion::StringFromShellCmd(find1.c_str());
    eos::common::StringConversion::StringFromShellCmd(find2.c_str());
    eos::common::StringConversion::StringFromShellCmd(find3.c_str());
    eos::common::StringConversion::StringFromShellCmd(find4.c_str());
    std::string out = eos::common::StringConversion::StringFromShellCmd(
                        find5.c_str());
    fprintf(stdout,
            "---------------------------------------------------------------------------\n");
    fprintf(stdout, "- releases of '%s' \n", packagepath.GetPath());
    fprintf(stdout,
            "---------------------------------------------------------------------------\n");
    fprintf(stdout, "%s", out.c_str());
    fprintf(stdout,
            "---------------------------------------------------------------------------\n");
    return (0);
  }

  if (!ok) {
    goto com_squash_usage;
  }

  return (0);
com_squash_usage:
  fprintf(stdout,
          "usage: squash new <path>                                                  : create a new squashfs under <path>\n");
  fprintf(stdout, "\n");
  fprintf(stdout,
          "       squash pack [-f] <path>                                            : pack a squashfs image\n");
  fprintf(stdout,
          "                                                                            -f will recreate the package but keeps the symbolic link locally\n");
  fprintf(stdout, "\n");
  fprintf(stdout,
          "       squash unpack [-f] <path>                                          : unpack a squashfs image for modification\n");
  fprintf(stdout,
          "                                                                            -f will atomically update the local package\n");
  fprintf(stdout, "\n");
  fprintf(stdout,
          "       squash info <path>                                                 : squashfs information about <path>\n");
  fprintf(stdout, "\n");
  fprintf(stdout,
          "       squash rm <path>                                                   : delete a squashfs attached image and its smart link\n");
  fprintf(stdout, "\n");
  fprintf(stdout,
          "       squash relabel <path>                                              : relable a squashfs image link e.g. after an image move in the namespace\n");
  fprintf(stdout, "\n");
  /*  fprintf(stdout,
    "       squash roll <path>                                                 : will create a squash package from the EOS directory pointed by <path\n");
  fprintf(stdout,"\n");
  fprintf(stdout,
    "       squash unroll <path>                                               : will store the squash package contents unpacked into the EOS package directory\n");
  fprintf(stdout,"\n");
  */
  fprintf(stdout,
          "       squash install --curl=https://<package>.tgz|.tar.gz <path>         : create a squashfs package from a web archive under <path>\n");
  fprintf(stdout,
          "       squash new-release <path> [<version>]                                : create a new squashfs release under <path> - by default versions are made from timestamp, but this can be overwritten using the version field\n");
  fprintf(stdout,
          "       squash pack-release <path>                                         : pack a squashfs release under <path>\n");
  fprintf(stdout,
          "       squash info-release <path>                                         : show all release revisions under <path> <path>\n");
  fprintf(stdout,
          "       squash trim-release <path> <keep-days> [<keep-versions>]           : trim  releases older than <keep-days> and keep maximum <keep-versions> of release\n");
  fprintf(stdout,
          "       squash rm-release <path>                                           : delete all squahfs releases udner <path>\n");
  global_retc = EINVAL;
  return (0);
}
