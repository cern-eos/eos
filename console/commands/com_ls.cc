// ----------------------------------------------------------------------
// File: com_ls.cc
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

#include "console/ConsoleMain.hh"
#include "common/StringTokenizer.hh"
#include "common/StringConversion.hh"
#include "XrdPosix/XrdPosixXrootd.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "namespace/utils/Mode.hh"

/* List a directory */
int
com_ls(char* arg1)
{
  // split subcommands
  eos::common::StringTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString param = "";
  XrdOucString option = "";
  XrdOucString path = "";
  XrdOucString in = "mgm.cmd=ls";

  do {
    param = subtokenizer.GetToken();

    if (!param.length()) {
      break;
    }

    if (param == "--help") {
      goto com_ls_usage;
    }

    if (param == "-h") {
      goto com_ls_usage;
    }

    if (param.beginswith("-")) {
      option += param;
      if (param == "-c") {
	option += "-l";
      }
      if ((option.find("&")) != STR_NPOS) {
        goto com_ls_usage;
      }
    } else {
      if (path.length()) {
        // this allows for spaces in path names
        path += " ";
        path += param;
      } else {
        path = param;
      }
    }
  } while (1);

  if (!path.length()) {
    path = gPwd;
  }

  // remove escaped blanks
  while (path.replace("\\ ", " ")) {
  }

  if ((path.beginswith("as3:"))) {
    // extract evt. the hostname
    // the hostname is part of the URL like in ROOT
    XrdOucString hostport;
    XrdOucString protocol;
    XrdOucString sPath;
    const char* v = 0;

    if (!(v = eos::common::StringConversion::ParseUrl(path.c_str(), protocol,
              hostport))) {
      fprintf(stderr, "error: illegal url <%s>\n", path.c_str());
      global_retc = EINVAL;
      return (0);
    }

    sPath = v;

    if (hostport.length()) {
      setenv("S3_HOSTNAME", hostport.c_str(), 1);
    }

    XrdOucString envString = path;
    int qpos = 0;

    if ((qpos = envString.find("?")) != STR_NPOS) {
      envString.erase(0, qpos + 1);
      XrdOucEnv env(envString.c_str());

      // extract opaque S3 tags if present
      if (env.Get("s3.key")) {
        setenv("S3_SECRET_ACCESS_KEY", env.Get("s3.key"), 1);
      }

      if (env.Get("s3.id")) {
        setenv("S3_ACCESS_KEY_ID", env.Get("s3.id"), 1);
      }

      path.erase(path.find("?"));
      sPath.erase(sPath.find("?"));
    }

    // Apply the ROOT compatability environment variables
    const char* cstr = getenv("S3_ACCESS_KEY");

    if (cstr) {
      setenv("S3_SECRET_ACCESS_KEY", cstr, 1);
    }

    cstr = getenv("S3_ACESSS_ID");

    if (cstr) {
      setenv("S3_ACCESS_KEY_ID", cstr, 1);
    }

    // check that the environment is set
    if (!getenv("S3_ACCESS_KEY_ID") ||
        !getenv("S3_HOSTNAME") ||
        !getenv("S3_SECRET_ACCESS_KEY")) {
      fprintf(stderr,
              "error: you have to set the S3 environment variables S3_ACCESS_KEY_ID | S3_ACCESS_ID, S3_HOSTNAME (or use a URI), S3_SECRET_ACCESS_KEY | S3_ACCESS_KEY\n");
      exit(-1);
    }

    XrdOucString s3env;
    s3env = "env S3_ACCESS_KEY_ID=";
    s3env += getenv("S3_ACCESS_KEY_ID");
    s3env += " S3_HOSTNAME=";
    s3env += getenv("S3_HOSTNAME");
    s3env += " S3_SECRET_ACCESS_KEY=";
    s3env += getenv("S3_SECRET_ACCESS_KEY");
    XrdOucString s3arg = sPath.c_str();
    // do some bash magic ... sigh
    XrdOucString listcmd = "bash -c \"";
    listcmd += s3env;
    listcmd += " s3 list ";
    listcmd += s3arg;
    listcmd += " ";
    listcmd += "\"";
    global_retc = system(listcmd.c_str());
    return (0);
  }

  if ((path.beginswith("file:")) || (path.beginswith("root:"))) {
    // list a local or XRootD path
    bool XRootD = path.beginswith("root:");
    XrdOucString protocol;
    XrdOucString hostport;
    XrdOucString sPath;
    const char* v = 0;

    if (!(v = eos::common::StringConversion::ParseUrl(path.c_str(), protocol,
              hostport))) {
      global_retc = EINVAL;
      return (0);
    }

    sPath = v;
    std::string Path = v;

    if (sPath == "" && (protocol == "file")) {
      sPath = getenv("PWD");
      Path = getenv("PWD");

      if (!sPath.endswith("/")) {
        sPath += "/";
        Path += "/";
      }
    }

    XrdOucString url = "";
    eos::common::StringConversion::CreateUrl(protocol.c_str(), hostport.c_str(),
        Path.c_str(), url);
    DIR* dir = (XRootD) ? XrdPosixXrootd::Opendir(url.c_str()) : opendir(
                 url.c_str());

    if (dir) {
      struct dirent* entry;

      while ((entry = (XRootD) ? XrdPosixXrootd::Readdir(dir) : readdir(dir))) {
        struct stat buf;
        XrdOucString curl = "";
        XrdOucString cpath = Path.c_str();
        cpath += entry->d_name;
        eos::common::StringConversion::CreateUrl(protocol.c_str(), hostport.c_str(),
            cpath.c_str(), curl);

        if ((option.find("a")) == STR_NPOS) {
          if (entry->d_name[0] == '.') {
            // skip hidden files
            continue;
          }
        }

        if (!((XRootD) ? XrdPosixXrootd::Stat(curl.c_str(), &buf) : stat(curl.c_str(),
              &buf))) {
          if ((option.find("l")) == STR_NPOS) {
            // no details
            fprintf(stdout, "%s\n", entry->d_name);
          } else {
            char t_creat[14];
            char modestr[11];
            eos::modeToBuffer(buf.st_mode, modestr);
            XrdOucString suid = "";
            suid += (int) buf.st_uid;
            XrdOucString sgid = "";
            sgid += (int) buf.st_gid;
            XrdOucString sizestring = "";
            struct tm* t_tm;
            struct tm t_tm_local;
            t_tm = localtime_r(&buf.st_ctime, &t_tm_local);
            strftime(t_creat, 13, "%b %d %H:%M", t_tm);
            XrdOucString dirmarker = "";

            if ((option.find("F")) != STR_NPOS) {
              dirmarker = "/";
            }

            if (modestr[0] != 'd') {
              dirmarker = "";
            }

            fprintf(stdout, "%s %3d %-8.8s %-8.8s %12s %s %s%s\n", modestr,
                    (int) buf.st_nlink,
                    suid.c_str(), sgid.c_str(),
                    eos::common::StringConversion::GetSizeString(sizestring,
                        (unsigned long long) buf.st_size), t_creat, entry->d_name, dirmarker.c_str());
          }
        }
      }

      (XRootD) ? XrdPosixXrootd::Closedir(dir) : closedir(dir);
    }

    global_retc = 0;
    return (0);
  }

  path = abspath(path.c_str());
  in += "&mgm.path=";
  in += path;
  in += "&mgm.option=";
  in += option;
  global_retc = output_result(client_command(in));
  return (0);
com_ls_usage:
  fprintf(stdout,
          "usage: ls [-laniyF] <path>                                             :  list directory <path>\n");
  fprintf(stdout, "                    -l : show long listing\n");
  fprintf(stdout,
          "                    -y : show long listing with backend(tape) status\n");
  fprintf(stdout,
          "                    -lh: show long listing with readable sizes\n");
  fprintf(stdout, "                    -a : show hidden files\n");
  fprintf(stdout, "                    -i : add inode information\n");
  fprintf(stdout, "                    -c : add checksum value\n");
  fprintf(stdout, "                    -n : show numerical user/group ids\n");
  fprintf(stdout, "                    -F : append indicator '/' to directories \n");
  fprintf(stdout,
          "                    -s : checks only if the directory exists without listing\n");
  fprintf(stdout, "         path=file:... : list on a local file system\n");
  fprintf(stdout,
          "         path=root:... : list on a plain XRootD server (does not work on native XRootD clusters\n");
  fprintf(stdout,
          "         path=...      : all other paths are considered to be EOS paths!\n");
  global_retc = EINVAL;
  return (0);
}
