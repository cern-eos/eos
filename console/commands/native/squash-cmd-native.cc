// ----------------------------------------------------------------------
// File: squash-cmd-native.cc
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

#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include "common/StringTokenizer.hh"
#include "common/StringConversion.hh"
#include "common/Path.hh"
#include <XrdOuc/XrdOucString.hh>
#include <pwd.h>
#include <thread>
#include <chrono>
#include <memory>
#include <sstream>
#include <string>
#include <vector>
#include <cstring>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/wait.h>

/*----------------------------------------------------------------------------*/

namespace {

struct SquashParsed {
  std::string cmd;
  std::string path;
  std::string option;   // e.g. "f" for -f, or full "--curl=..." for install
  std::string version;  // for new-release
  std::string keepdays;
  std::string keepversions;
};

std::string MakeSquashHelp()
{
  return R"(usage: squash new <path>                                                  : create a new squashfs under <path>

       squash pack [-f] <path>                                            : pack a squashfs image
                                                                          -f will recreate the package but keeps the symbolic link locally

       squash unpack [-f] <path>                                          : unpack a squashfs image for modification
                                                                          -f will atomically update the local package

       squash info <path>                                                 : squashfs information about <path>

       squash rm <path>                                                   : delete a squashfs attached image and its smart link

       squash relabel <path>                                              : relabel a squashfs image link e.g. after an image move in the namespace

       squash install --curl=https://<package>.tgz|.tar.gz <path>         : create a squashfs package from a web archive under <path>
       squash new-release <path> [<version>]                              : create a new squashfs release under <path>
       squash pack-release <path>                                         : pack a squashfs release under <path>
       squash info-release <path>                                         : show all release revisions under <path>
       squash trim-release <path> <keep-days> [<keep-versions>]           : trim releases older than <keep-days> and keep maximum <keep-versions>
       squash rm-release <path>                                            : delete all squashfs releases under <path>
)";
}

static void squash_usage()
{
  std::cerr << MakeSquashHelp();
  global_retc = EINVAL;
}

/** Parse arg string into SquashParsed. Returns true on success. */
bool ParseSquashArgString(const char* arg1, SquashParsed& out)
{
  eos::common::StringTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString cmd = subtokenizer.GetToken();
  XrdOucString path = subtokenizer.GetToken();
  XrdOucString option;
  XrdOucString fulloption;

  if (!cmd.length())
    return false;

  if (cmd == "--help" || cmd == "-h")
    return false;

  if (path.length() && (path[0] == '-')) {
    option = path[1];
    fulloption = path;
    path = subtokenizer.GetToken();
  }

  if (!path.length())
    return false;

  if ((cmd != "trim-release") && (cmd != "new-release")) {
    XrdOucString garbage = subtokenizer.GetToken();
    if (garbage.length())
      return false;
  }

  out.cmd = cmd.c_str();
  out.path = path.c_str();
  if (fulloption.beginswith("--"))
    out.option = fulloption.c_str();
  else if (option.length())
    out.option = std::string(1, option[0]);

  if (out.cmd == "new-release") {
    XrdOucString version = subtokenizer.GetToken();
    out.version = version.c_str();
  }

  if (out.cmd == "trim-release") {
    XrdOucString keepdays = subtokenizer.GetToken();
    XrdOucString keepversions = subtokenizer.GetToken();
    out.keepdays = keepdays.c_str();
    out.keepversions = keepversions.c_str();
  }

  return true;
}

/** Core squash implementation - called directly and recursively */
static int squash_impl(const SquashParsed& p);

/** Entry point for recursive/legacy callers - parses string and calls squash_impl */
int com_squash(char* arg1)
{
  SquashParsed p;
  if (!ParseSquashArgString(arg1, p)) {
    squash_usage();
    return 0;
  }
  return squash_impl(p);
}

static int squash_impl(const SquashParsed& p)
{
  const std::string& cmd = p.cmd;
  std::string path = p.path;
  const std::string& option = p.option;
  const std::string& version = p.version;
  const std::string& keepdays = p.keepdays;
  const std::string& keepversions = p.keepversions;

  bool ok = false;
  const int len = 4096;
  char username[len];
  struct passwd* pw = getpwuid(geteuid());

  if (pw == nullptr) {
    std::cerr << "error: failed to get effective UID username of calling process\n";
    squash_usage();
    return 0;
  }

  (void)strncpy(username, pw->pw_name, len - 1);
  username[len - 1] = '\0';

  path = abspath(path.c_str());

  if (cmd == "new") {
    struct stat buf;
    eos::common::Path packagepath(path.c_str());

    if (!stat(packagepath.GetPath(), &buf)) {
      std::cerr << "error: package path='" << packagepath.GetPath() << "' exists already\n";
      global_retc = EEXIST;
      return 0;
    }

    std::string mkpath = "/var/tmp/";
    mkpath += username;
    mkpath += "/eosxd/mksquash/";
    mkpath += packagepath.GetContractedPath();
    mkpath += "/dummy";
    eos::common::Path mountpath(mkpath.c_str());

    if (!mountpath.MakeParentPath(S_IRWXU | S_IROTH | S_IXOTH | S_IRGRP | S_IXGRP)) {
      std::cerr << "error: failed to create local mount point path='" << mountpath.GetParentPath() << "'\n";
      global_retc = errno;
      return 0;
    }

    if (symlink(mountpath.GetParentPath(), packagepath.GetPath())) {
      std::cerr << "error: failed to create symbolic link from '" << mountpath.GetParentPath()
                << "' => '" << packagepath.GetPath() << "'\n";
      global_retc = errno;
      return 0;
    }

    ok = true;
    std::cerr << "info: ready to install your software under '" << packagepath.GetPath() << "'\n";
    std::cerr << "info: when done run 'eos squash pack " << packagepath.GetPath()
              << "' to create an image file and a smart link in EOS!\n";
  }

  if (cmd == "install") {
    if (option.substr(0, 7) == "--curl=") {
      ok = true;
      std::string url = option.substr(7);

      if (url.size() >= 4 && (url.substr(url.size() - 4) == ".tgz" ||
                              (url.size() >= 7 && url.substr(url.size() - 7) == ".tar.gz"))) {
        int sub_rc = 0;
        std::string subcommand = "rm \"" + path + "\"";
        com_squash((char*)subcommand.c_str());
        sub_rc |= global_retc;
        subcommand = "new \"" + path + "\"";
        com_squash((char*)subcommand.c_str());
        sub_rc |= global_retc;
        std::string shellcmd = "cd \"" + path + "\"; curl " + url + " /dev/stdout | tar xvzf -";
        int rc = system(shellcmd.c_str());

        if (WEXITSTATUS(rc)) {
          std::cerr << "error: curl download failed with retc='" << WEXITSTATUS(rc) << "'\n";
          global_retc = WEXITSTATUS(rc);
          return 0;
        }

        subcommand = "pack \"" + path + "\"";
        com_squash((char*)subcommand.c_str());
        sub_rc |= global_retc;

        if (sub_rc) {
          global_retc = sub_rc;
          return 0;
        }
      } else {
        std::cerr << "error: suffix of '" << url << "' is not supported\n";
        global_retc = EINVAL;
        return 0;
      }
    } else {
      squash_usage();
      return 0;
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

    if ((rl = readlink(packagepath.GetPath(), linktarget, sizeof(linktarget))) == -1) {
      std::cerr << "error: failed to resolve symbolic link of squashfs package '" << packagepath.GetPath()
                << "' - errno '" << errno << "'\n";
      global_retc = errno;
      return 0;
    }
    linktarget[rl] = 0;

    struct stat buf;

    if (stat(linktarget, &buf)) {
      std::cerr << "error: cannot find local package directory '" << linktarget << "'\n";
      global_retc = errno;
      return 0;
    }

    shellcmd += linktarget;
    shellcmd += " ";
    shellcmd += squashpack;
    shellcmd += "~";
    shellcmd += " -noappend -force-uid ";
    shellcmd += std::to_string(geteuid());
    shellcmd += " -force-gid ";
    shellcmd += std::to_string(getegid());
    shellcmd += " && mv -f -T ";
    shellcmd += squashpack;
    shellcmd += "~ ";
    shellcmd += squashpack;
    std::cerr << "running " << shellcmd << "\n";
    int rc = system(shellcmd.c_str());

    if (WEXITSTATUS(rc)) {
      std::cerr << "error: mksquashfs failed with retc='" << WEXITSTATUS(rc) << "'\n";
      global_retc = WEXITSTATUS(rc);
      return 0;
    }

    if (option != "f") {
      if (unlink(packagepath.GetPath())) {
        std::cerr << "error: failed to unlink locally staged squashfs archive '" << squashpack
                  << "' - errno '" << errno << "'\n";
        global_retc = errno;
        return 0;
      }

      std::string targetline = "eosxd get eos.hostport ";
      targetline += packagepath.GetParentPath();
      std::string hostport = eos::common::StringConversion::StringFromShellCmd(targetline.c_str());

      if (!hostport.length()) {
        std::cerr << "error: failed to get eos.hostport from mountpoint '" << targetline << "'\n";
        global_retc = EIO;
        return 0;
      }

      std::string target = "/eos/squashfs/";
      target += hostport;
      target += "@";
      XrdOucString spackagepath = squashpack.c_str();
      while (spackagepath.replace("/", "---")) {}

      target += spackagepath.c_str();

      if (symlink(target.c_str(), packagepath.GetPath())) {
        std::cerr << "error: failed to create squashfs symlink '" << packagepath.GetPath()
                  << "' => '" << target << "'\n";
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
      std::cerr << "error: the squashfs package file is missing for this label!\n";
      global_retc = ENOENT;
      return 0;
    }

    if (!lstat(packagepath.GetPath(), &buf)) {
      if (unlink(packagepath.GetPath())) {
        std::cerr << "error: failed to remove existing squashfs archive '" << packagepath.GetPath()
                  << "' - errno '" << errno << "'\n";
        global_retc = errno;
        return 0;
      }
    }

    std::string targetline = "eosxd get eos.hostport ";
    targetline += packagepath.GetParentPath();
    std::string hostport = eos::common::StringConversion::StringFromShellCmd(targetline.c_str());

    if (!hostport.length()) {
      std::cerr << "error: failed to get eos.hostport from mountpoint '" << targetline << "'\n";
      global_retc = EIO;
      return 0;
    }

    std::string target = "/eos/squashfs/";
    target += hostport;
    target += "@";
    XrdOucString spackagepath = squashpack.c_str();
    while (spackagepath.replace("/", "---")) {}

    target += spackagepath.c_str();

    if (symlink(target.c_str(), packagepath.GetPath())) {
      std::cerr << "error: failed to create squashfs symlink '" << packagepath.GetPath()
                << "' => '" << target << "'\n";
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

    if ((rl = readlink(packagepath.GetPath(), linktarget, sizeof(linktarget))) == -1) {
      std::cerr << "error: failed to resolve symbolic link of squashfs package '" << packagepath.GetPath()
                << "' - errno '" << errno << "'\n";
      global_retc = errno;
      return 0;
    }
    linktarget[rl] = 0;

    XrdOucString mounttarget = linktarget;
    std::string mkpath = "/var/tmp/";
    mkpath += username;
    mkpath += "/eosxd/mksquash/";

    if (option != "f") {
      if (mounttarget.beginswith(mkpath.c_str())) {
        std::cerr << "error: squash image is already unpacked!\n";
        global_retc = EINVAL;
        return 0;
      }

      if (!geteuid()) {
        std::string umountcmd = "umount -f -l ";
        umountcmd += mounttarget.c_str();
        (void)!system(umountcmd.c_str());

        if (rmdir(mounttarget.c_str())) {
          if (errno != ENOENT) {
            std::cerr << "error: failed to unlink local mount directory path='" << mounttarget.c_str()
                      << "' errno=" << errno << "\n";
          }
        }
      }
    }

    std::string shellcmd = "unsquashfs -f -d ";
    mkpath += packagepath.GetContractedPath();
    mkpath += "/dummy";
    eos::common::Path mountpath(mkpath.c_str());

    if (!mountpath.MakeParentPath(S_IRWXU | S_IROTH | S_IXOTH | S_IRGRP | S_IXGRP)) {
      std::cerr << "error: failed to create local mount point path='" << mountpath.GetParentPath() << "'\n";
      global_retc = errno;
      return 0;
    }

    if (unlink(packagepath.GetPath())) {
      std::cerr << "error: failed to unlink smart link for squashfs archive '" << squashpack
                << "' - errno '" << errno << "'\n";
      global_retc = errno;
      return 0;
    }

    if (symlink(mountpath.GetParentPath(), packagepath.GetPath())) {
      std::cerr << "error: failed to create symbolic link from '" << mountpath.GetParentPath()
                << "' => '" << packagepath.GetPath() << "'\n";
      global_retc = errno;
      return 0;
    }

    shellcmd += mountpath.GetParentPath();
    shellcmd.erase(shellcmd.length() - 1);
    shellcmd += "~ ";
    shellcmd += squashpack.c_str();
    shellcmd += " && rsync -aq --delete ";
    shellcmd += mountpath.GetParentPath();
    shellcmd.erase(shellcmd.length() - 1);
    shellcmd += "~/ ";
    shellcmd += mountpath.GetParentPath();
    shellcmd += " && rm -rf ";
    shellcmd += mountpath.GetParentPath();
    shellcmd.erase(shellcmd.length() - 1);
    shellcmd += "~";
    std::cout << shellcmd << "\n";
    int rc = system(shellcmd.c_str());

    if (WEXITSTATUS(rc)) {
      std::cerr << "error: unsquashfs failed with retc='" << WEXITSTATUS(rc) << "'\n";
      global_retc = WEXITSTATUS(rc);
      return 0;
    }
    std::cerr << "info: squashfs image is available unpacked under '" << packagepath.GetPath() << "'\n";
    std::cerr << "info: when done with modifications run 'eos squash pack " << packagepath.GetPath()
              << "' to create an image file and a smart link in EOS!\n";
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
      std::cerr << "info: '" << squashpack << "' has a squashfs image with size=" << (unsigned long)buf.st_size
                << " bytes\n";
    } else {
      std::cerr << "info: '" << squashpack << "' has no squashfs image\n";
    }

    char linktarget[4096];
    ssize_t rl;

    if ((rl = readlink(packagepath.GetPath(), linktarget, sizeof(linktarget))) == -1) {
      std::cerr << "error: failed to resolve symbolic link of squashfs package '" << packagepath.GetPath()
                << "' - errno '" << errno << "'\n";
      global_retc = errno;
      return 0;
    }
    linktarget[rl] = 0;

    XrdOucString mounttarget = linktarget;
    std::string mkpath = "/var/tmp/";
    mkpath += username;
    mkpath += "/eosxd/mksquash/";

    if (mounttarget.beginswith(mkpath.c_str())) {
      if (stat(linktarget, &buf)) {
        std::cerr << "error: cannot find local package directory '" << linktarget << "'\n";
        global_retc = EINVAL;
        return 0;
      }
      std::cerr << "info: squashfs image is currently unpacked/open for local RW mode - use 'eos squash pack "
                << packagepath.GetPath() << "' to close image\n";
    } else {
      std::cerr << "info: squashfs image is currently packed - use 'eos squash unpack "
                << packagepath.GetPath() << "' to open image locally\n";
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
        std::cerr << "error: failed to remove existing squashfs archive '" << squashpack
                  << "' - errno '" << errno << "'\n";
        global_retc = errno;
        return 0;
      }
      std::cerr << "info: removed squashfs image '" << squashpack << "'\n";
    }

    if (!lstat(packagepath.GetPath(), &buf)) {
      if (unlink(packagepath.GetPath())) {
        std::cerr << "error: failed to unlink locally staged squashfs archive '" << squashpack
                  << "' - errno '" << errno << "'\n";
        global_retc = errno;
        return 0;
      }
      std::cerr << "info: removed squashfs smart link '" << packagepath.GetPath() << "'\n";
    }
  }

  if (cmd == "rm-release") {
    std::string scmd = "info-release " + path;
    com_squash((char*)scmd.c_str());

    if (!global_retc) {
      std::cerr << "info: wiping squashfs releases under '" << path << "'\n";
      eos::common::Path packagepath(path.c_str());
      std::string nextrelease = std::string(packagepath.GetPath()) + "/next";
      std::string currentrelease = std::string(packagepath.GetPath()) + "/current";
      std::string archive = std::string(packagepath.GetPath()) + "/.archive";
      std::cout << "info: wiping links current,next ... \n";
      ::unlink(currentrelease.c_str());
      ::unlink(nextrelease.c_str());

      if (archive.substr(0, 5) == "/eos/") {
        std::cout << "info: wiping archive ...\n";
        scmd = "eos rm -rf " + archive;
        std::string out = eos::common::StringConversion::StringFromShellCmd(scmd.c_str());
        std::cout << out;
      }

      for (size_t i = 0; i < 50; ++i) {
        struct stat buf;

        if (!::stat(archive.c_str(), &buf)) {
          std::this_thread::sleep_for(std::chrono::milliseconds(100));
        } else {
          break;
        }

        if (i == 49) {
          std::cerr << "=====================================\n";
          std::cerr << "warning: mount didn't see cleanup ...\n";
          std::cerr << "remote:\n";
          std::cerr << "=====================================\n";
          scmd = "eos ls -la " + std::string(packagepath.GetPath());
          std::string out = eos::common::StringConversion::StringFromShellCmd(scmd.c_str());
          std::cout << out;
          std::cerr << "=====================================\n";
          std::cerr << "local:\n";
          std::cerr << "=====================================\n";
          scmd = "ls -la " + std::string(packagepath.GetPath());
          out = eos::common::StringConversion::StringFromShellCmd(scmd.c_str());
          std::cout << out;
          std::cerr << "=====================================\n";
        }
      }

      if (::rmdir(packagepath.GetPath())) {
        global_retc = errno;
        std::cerr << "error: failed to clean squashfs release under '" << path << "' error=" << global_retc << "\n";
      }
      return 0;
    }
    std::cerr << "info: there is no squashfs release under '" << path << "'\n";
    return 0;
  }

  if (cmd == "new-release") {
    eos::common::Path packagepath(path.c_str());
    std::string now = eos::common::StringConversion::StringFromShellCmd("date '+%Y%m%d%H%M%S'");

    if (!version.empty()) {
      now = version;
    }

    now.erase(now.find_last_not_of(" \n\r\t") + 1);
    std::string archivepath = std::string(packagepath.GetPath()) + "/.archive/";
    std::string packagename = packagepath.GetName();
    std::string archivepackage = archivepath + packagename + "-" + now;
    std::string nextrelease = std::string(packagepath.GetPath()) + "/next";
    eos::common::Path archpath(archivepackage.c_str());

    if (!archpath.MakeParentPath(S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH)) {
      std::cerr << "error: couldn't create '" << archpath.GetParentPath() << "'\n";
      global_retc = errno;
      return 0;
    }

    ::unlink(archivepackage.c_str());
    ::unlink(nextrelease.c_str());
    std::string scmd = "new \"" + archivepackage + "\"";
    int rc = com_squash((char*)scmd.c_str());

    if (!rc) {
      rc = ::symlink(archivepackage.c_str(), nextrelease.c_str());

      if (rc) {
        std::cerr << "error: failed to create symbolic link for next release '" << nextrelease << "'\n";
        global_retc = errno;
        return 0;
      }
    } else {
      std::cerr << "error: failed to create squash package for a new release\n";
      return 0;
    }

    std::cerr << "info: install the new release under '" << nextrelease << "'\n";
    return 0;
  }

  if (cmd == "pack-release") {
    char lname[4096];
    memset(lname, 0, sizeof(lname));
    eos::common::Path packagepath(path.c_str());
    std::string nextrelease = std::string(packagepath.GetPath()) + "/next";
    std::string currentrelease = std::string(packagepath.GetPath()) + "/current";
    std::string hiddencurrentrelease = std::string(packagepath.GetPath()) + "/.current";

    if (::readlink(nextrelease.c_str(), lname, sizeof(lname)) < 0) {
      std::cerr << "error: failed to find an open release package under '" << nextrelease << "'\n";
      global_retc = errno;
      return 0;
    }

    std::string scmd = "pack \"" + std::string(lname) + "\"";
    int rc = com_squash((char*)scmd.c_str());

    if (!rc) {
      rc = ::unlink(nextrelease.c_str());

      if (rc) {
        std::cerr << "error: failed to unlink open release package under '" << nextrelease << "'\n";
        global_retc = errno;
        return 0;
      }

      rc = ::symlink(lname, hiddencurrentrelease.c_str());

      if (rc) {
        std::cerr << "error: failed to symlink current release package under '" << hiddencurrentrelease << "'\n";
        global_retc = errno;
        return 0;
      }

      rc = ::rename(hiddencurrentrelease.c_str(), currentrelease.c_str());

      if (rc) {
        std::cerr << "error: failed to move '" << hiddencurrentrelease << "' to '" << currentrelease << "'\n";
        global_retc = errno;
        return 0;
      }
      std::cout << "info: new release available under '" << currentrelease << "'\n";
      return 0;
    }
    std::cerr << "error: failed to pack squash package for a new release\n";
    return 0;
  }

  if (cmd == "info-release") {
    std::string scmd = "trim-release \"" + path + "\" 999999 999999";
    com_squash((char*)scmd.c_str());
    return 0;
  }

  if (cmd == "trim-release") {
    eos::common::Path packagepath(path.c_str());
    std::string current = packagepath.GetPath();
    std::string archive = packagepath.GetPath();
    current += "/current";
    archive += "/.archive";
    struct stat buf;

    if (::lstat(current.c_str(), &buf)) {
      std::cerr << "error: I cannot find any current release under '" << current << "'\n";
      global_retc = EINVAL;
      return 0;
    }

    if (::lstat(archive.c_str(), &buf)) {
      std::cerr << "error: I cannot find any archive release under '" << archive << "'\n";
      global_retc = EINVAL;
      return 0;
    }

    if (keepdays.empty()) {
      std::cerr << "error: you have to specify the number of days you want to keep releases : squash trim-release "
                   "<path> <n-days> [<max-versions>]\n";
      global_retc = EINVAL;
      return 0;
    }

    size_t n_keepdays = strtol(keepdays.c_str(), 0, 10);

    if (!n_keepdays) {
      std::cerr << "error: you have to specify the number of days you want to keep releases : squash trim-release "
                   "<path> <n-days>\n";
      global_retc = EINVAL;
      return 0;
    }

    std::string keepversions_val = keepversions;
    size_t n_keepversions = keepversions_val.length() ? strtol(keepversions_val.c_str(), 0, 10) : 0;

    if (!n_keepversions) {
      keepversions_val = "1000000";
    } else {
      keepversions_val = std::to_string(n_keepversions + 1);
    }

    std::string find1 = "find " + std::string(packagepath.GetPath()) + " -type f -mtime +" + keepdays + " -delete";
    std::string find2 = "find " + std::string(packagepath.GetPath()) + " -type l -mtime +" + keepdays + " -delete";
    std::string find3 = "find " + std::string(packagepath.GetPath()) + "/.archive/ -type f -printf '%Ts\t%h/%f\n' | sort -rn | tail -n +" +
                        keepversions_val + " | cut -f2- | xargs -r rm";
    std::string find4 = "find " + std::string(packagepath.GetPath()) + "/.archive/ -type l -printf '%Ts\t%h/%f\n' | sort -rn | tail -n +" +
                        keepversions_val + " | cut -f2- | xargs -r rm";
    std::string find5 = "find " + std::string(packagepath.GetPath()) + " -type l";

    eos::common::StringConversion::StringFromShellCmd(find1.c_str());
    eos::common::StringConversion::StringFromShellCmd(find2.c_str());
    eos::common::StringConversion::StringFromShellCmd(find3.c_str());
    eos::common::StringConversion::StringFromShellCmd(find4.c_str());
    std::string out = eos::common::StringConversion::StringFromShellCmd(find5.c_str());

    std::cout << "---------------------------------------------------------------------------\n";
    std::cout << "- releases of '" << packagepath.GetPath() << "' \n";
    std::cout << "---------------------------------------------------------------------------\n";
    std::cout << out;
    std::cout << "---------------------------------------------------------------------------\n";
    return 0;
  }

  if (!ok) {
    squash_usage();
    return 0;
  }

  return 0;
}

class SquashCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "squash";
  }
  const char*
  description() const override
  {
    return "Squashfs utility for EOS";
  }
  bool
  requiresMgm(const std::string& args) const override
  {
    return !wants_help(args.c_str());
  }
  int
  run(const std::vector<std::string>& args, CommandContext&) override
  {
    std::ostringstream oss;
    for (size_t i = 0; i < args.size(); ++i) {
      if (i)
        oss << ' ';
      oss << args[i];
    }
    std::string joined = oss.str();
    if (args.empty() || wants_help(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    return com_squash((char*)joined.c_str());
  }
  void
  printHelp() const override
  {
    std::cerr << MakeSquashHelp();
  }
};

} // namespace

void
RegisterSquashNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<SquashCommand>());
}
