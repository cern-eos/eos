// ----------------------------------------------------------------------
// File: com_cp.cc
// Author: Andreas-Joachim Peters - CERN
// Author: Joaquim Rocha - CERN
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
#include "console/ConsoleMain.hh"
#include "common/Path.hh"
#include "console/ConsoleCliCommand.hh"
/*----------------------------------------------------------------------------*/
#include <vector>
/*----------------------------------------------------------------------------*/

extern XrdOucString serveruri;
extern char* com_fileinfo (char* arg1);
extern int com_transfer (char* argin);

bool verifySrcAndDstEvalFunc(const CliOptionWithArgs *option,
                             std::vector<std::string> &args,
                             std::string **error,
                             void *userData)
{
  if (args.size() < 2)
  {
    *error = new std::string("Error: Please provide the <src> and "
                             "<dst> arguments");

    return false;
  }

  const std::string &lastArg = args[args.size() - 1];
  if (lastArg[lastArg.length() - 1] != '/')
  {
    *error = new std::string("Error: The <dst> argument needs to "
                             "end in a '/'\n");

    return false;
  }

  return true;
}

int
com_cp_examples ()
{
  fprintf(stdout, "\nExamples: \n");
  fprintf(stdout, "       eos cp /var/data/myfile /eos/foo/user/data/                   : copy 'myfile' to /eos/foo/user/data/myfile\n");
  fprintf(stdout, "       eos cp /var/data/ /eos/foo/user/data/                         : copy all plain files in /var/data to /eos/foo/user/data/\n");
  fprintf(stdout, "       eos cp -r /var/data/ /eos/foo/user/data/                      : copy the full hierarchy from /var/data/ to /var/data to /eos/foo/user/data/ => empty directories won't show up on the target!\n");
  fprintf(stdout, "       eos cp -r --checksum --silent /var/data/ /eos/foo/user/data/  : copy the full hierarchy and just printout the checksum information for each file copied!\n");
  fprintf(stdout, "\nS3:\n");
  fprintf(stdout, "      URLs have to be written as:\n");
  fprintf(stdout, "         as3://<hostname>/<bucketname>/<filename> as implemented in ROOT\n");
  fprintf(stdout, "      or as3:<bucketname>/<filename> with environment variable S3_HOSTNAME set\n");
  fprintf(stdout, "     and as3:....?s3.id=<id>&s3.key=<key>\n\n");
  fprintf(stdout, "      The access id can be defined in 3 ways:\n");
  fprintf(stdout, "      env S3_ACCESS_ID=<access-id>          [as used in ROOT  ]\n");
  fprintf(stdout, "      env S3_ACCESS_KEY_ID=<access-id>      [as used in libs3 ]\n\n");
  fprintf(stdout, "      <as3-url>?s3.id=<access-id>           [as used in EOS transfers\n");
  fprintf(stdout, "      The access key can be defined in 3 ways:\n");
  fprintf(stdout, "      env S3_ACCESS_KEY=<access-key>        [as used in ROOT  ]\n");
  fprintf(stdout, "      env S3_SECRET_ACCESS_KEY=<access-key> [as used in libs3 ]\n");
  fprintf(stdout, "      <as3-url>?s3.key=<access-key>         [as used in EOS transfers\n");
  fprintf(stdout, "\n");
  fprintf(stdout, "      If <src> and <dst> are using S3, we are using the same credentials on both ands and the target credentials will overwrite source credentials!\n");

  return (0);
}

/* Cp Interface */
int
com_cp (char* argin)
{
  char fullcmd[4096];
  XrdOucString sarg = argin;

  // split subcommands
  XrdOucString rate = "0";
  XrdOucString streams = "0";
  XrdOucString option = "";
  XrdOucString arg1 = "";
  XrdOucString arg2 = "";
  XrdOucString upload_target = "";
  XrdOucString cmdline;
  std::vector<XrdOucString> source_list;
  std::vector<unsigned long long> source_size;
  std::vector < std::pair<timespec, timespec >> source_utime;
  std::vector<XrdOucString> source_base_list;
  std::vector<XrdOucString> source_find_list;

  XrdOucString target;
  XrdOucString nextarg = "";
  XrdOucString lastarg = "";

  bool recursive = false;
  bool summary = false;
  bool noprogress = false;
  bool append = false;
  bool debug = false;
  bool checksums = false;
  bool silent = false;
  bool nooverwrite = false;
  bool preserve = false;
  unsigned long long copysize = 0;
  int retc = 0;
  int copiedok = 0;
  unsigned long long copiedsize = 0;
  struct timeval tv1, tv2;
  struct timezone tz;

  CliOption helpOption("help", "print help", "-h,--help");
  helpOption.setHidden(true);

  ConsoleCliCommand cpCmd("cp", "provides copy functionality to EOS");
  cpCmd.addOption(helpOption);
  cpCmd.addOptions(std::vector<CliOptionWithArgs>
                   {{"rate", "limit the cp rate to <rate>", "--rate=", 1,
                     "<rate>", false},
                    {"streams", "use <#> parallel streams", "--streams=", 1,
                     "<#>", false}
                   });
  cpCmd.addOptions(std::vector<CliOption>
                   {{"async", "run an asynchronous transfer via a gateway "
                     "server (see 'transfer submit --sync' for the full "
                     "options)", "--async"},
                    {"recursive", "copy directories recursively",
                     "-R,-r,--recursive"},
                    {"append", "append to the target, don't truncate",
                     "-a,--append"},
                    {"no-progress", "hide progress bar", "-n"},
                    {"summary", "print summary", "-S"},
                    {"debug", "enable debug information", "-d"},
                    {"checksum", "output the checksums", "--checksum"},
                    {"no-overwrite", "disable overwriting of files",
                     "-k,--no-overwrite"},
                    {"preserve", "preserve the modification times",
                     "--preserve,-p"},
                    {"silent", "run silently", "--silent,-s"}
                   });
  CliPositionalOption srcDst("src-dst", "", 1, -1,
                             "<src1> [<src2> ...] <dst>", true);
  srcDst.addEvalFunction((evalFuncCb) verifySrcAndDstEvalFunc, 0);
  cpCmd.addOption(srcDst);

  cpCmd.parse(argin);

  if (cpCmd.hasValue("help"))
  {
    cpCmd.printUsage();
    com_cp_examples();
    return 0;
  }
  else if (cpCmd.hasErrors())
  {
    cpCmd.printErrors();
    cpCmd.printUsage();
    return 0;
  }

  // check if this is an 'async' command
  if (cpCmd.hasValue("async"))
  {
    sarg.replace("--async", "submit --sync");
    snprintf(fullcmd, sizeof (fullcmd) - 1, "%s", sarg.c_str());
    return com_transfer(fullcmd);
  }

  if (cpCmd.hasValue("rate"))
    rate = cpCmd.getValue("rate").c_str();

  if (cpCmd.hasValue("streams"))
    streams = cpCmd.getValue("streams").c_str();

  recursive = cpCmd.hasValue("recursive");
  noprogress = cpCmd.hasValue("no-progress");
  append = cpCmd.hasValue("append");
  summary = cpCmd.hasValue("summary");
  silent = cpCmd.hasValue("silent");
  nooverwrite = cpCmd.hasValue("no-overwrite");
  checksums = cpCmd.hasValue("checksum");
  debug = cpCmd.hasValue("debug");
  preserve = cpCmd.hasValue("preserve");

  if (silent)
    noprogress = true;

  if (!hasterminal)
    noprogress = true;

  std::vector<std::string> paths = cpCmd.getValues("src-dst");

  if (paths.size() < 2)
  {
    fprintf(stdout, "Error: Please provide the <src> and <dst> arguments\n");
    cpCmd.printUsage();

    return 0;
  }

  for (size_t i = 0; i < paths.size() - 1; i++)
  {
    source_list.push_back(paths[i].c_str());
  }

  target = paths.back().c_str();

  if (debug)
    fprintf(stderr, "[eos-cp] Setting target %s\n", target.c_str());

  if (debug)
  {
    for (size_t l = 0; l < source_list.size(); l++)
    {
      fprintf(stderr, "[eos-cp] Copylist: %s\n", source_list[l].c_str());
    }
  }

  if (!recursive)
  {
    source_find_list = source_list;

    source_list.clear();

    for (size_t l = 0; l < source_find_list.size(); l++)
    {
      XrdOucString source_opaque;
      int opos = source_find_list[l].find("?");
      if (opos != STR_NPOS) 
      {
	source_opaque = source_find_list[l];
	source_opaque.erase(0,opos+1);
	source_find_list[l].erase(opos);
      }


      if (((source_find_list[l].beginswith("http:")) ||
           (source_find_list[l].beginswith("gsiftp:"))) &&
          (source_find_list[l].endswith("/")))
      {
        fprintf(stderr, "Error: directory copy not implemented for that protocol\n");
        continue;
      }

      // one wildcard file or directory
      if (((source_find_list[l].find("*") != STR_NPOS) || (source_find_list[l].endswith("/"))))
      {
        arg1 = source_find_list[l];
        eos::common::Path cPath(arg1.c_str());
        std::string dname = "";

        XrdOucString l = "eos -b ";
        if (user_role.length() && group_role.length())
        {
          l += "--role ";
          l += user_role;
          l += " ";
          l += group_role;
          l += " ";
        }

        l += "ls -l ";
        if (!arg1.endswith("/"))
        {
          dname = cPath.GetParentPath();
        }
        else
        {
          dname = arg1.c_str();
        }

        l += dname.c_str();

        l += " | grep -v ^d | awk '{print $9}'";

        if (!arg1.endswith("/"))
        {
          XrdOucString match = cPath.GetName();
          if (match.endswith("*"))
          {
            match.erase(match.length() - 1);
            match.insert("^", 0);
          }
          if (match.beginswith("*"))
          {
            match.erase(0, 1);
            match += "$";
          }
          if (match.length())
          {
            l += " | egrep \"";
            l += match.c_str();
            l += "\"";
          }
        }

        l += " 2>/dev/null";
        if (debug) fprintf(stderr, "[eos-cp] running %s\n", l.c_str());
        FILE* fp = popen(l.c_str(), "r");
        if (!fp)
        {
          fprintf(stderr, "Error: unable to run 'eos' - I need it in the path");
          exit(-1);
        }
        int item;
        char f2c[4096];
        while ((item = fscanf(fp, "%s", f2c) == 1))
        {
          std::string fullpath = dname;
          fullpath += f2c;
	  if (source_opaque.length()) 
          {
	    fullpath += "?";
	    fullpath += source_opaque.c_str();
	  }
          if (debug) fprintf(stdout, "[eos-cp] add file %s\n", fullpath.c_str());
          source_list.push_back(fullpath.c_str());
        }
        pclose(fp);
      }
      else
      {
        source_list.push_back(source_find_list[l].c_str());
      }
    }
  }
  else
  {
    // use find to get a file list
    source_find_list = source_list;

    source_list.clear();
    for (size_t nfile = 0; nfile < source_find_list.size(); nfile++)
    {
      XrdOucString source_opaque;
      int opos = source_find_list[nfile].find("?");
      if (opos != STR_NPOS) {
	source_opaque = source_find_list[nfile];
	source_opaque.erase(0,opos+1);
	source_find_list[nfile].erase(opos);
      }
      if (!source_find_list[nfile].beginswith("as3:"))
      {
        if (!source_find_list[nfile].endswith("/"))
        {
          fprintf(stderr, "Error: for recursive copy you have to give a directory name ending with '/'\n");
          cpCmd.printUsage();
          return 0;
        }
      }
      if ((source_find_list[nfile].beginswith("http:")) ||
          (source_find_list[nfile].beginswith("gsiftp:")))
      {
        fprintf(stderr, "Error: recursive copy not implemented for that protocol\n");
        continue;
      }
      XrdOucString l = "";
      XrdOucString sourceprefix = ""; // this is the URL part of root://<host>/ for XRootD or as3://<hostname>/

      l += "eos -b ";
      if (user_role.length() && group_role.length())
      {
        l += "--role ";
        l += user_role;
        l += " ";
        l += group_role;
        l += " ";
      }
      l += "find -f ";
      if (source_find_list[nfile].beginswith("/") && (!source_find_list[nfile].beginswith("/eos")))
      {
        l += "file:";

      }

      l += source_find_list[nfile];

      l += " 2> /dev/null";

      if (debug) fprintf(stderr, "[eos-cp] running %s\n", l.c_str());
      FILE* fp = popen(l.c_str(), "r");
      if (!fp)
      {
        fprintf(stderr, "error; unable to run 'eos' - I need it in the path");
        exit(-1);
      }

      if (l.length())
      {
        int item;
        char f2c[4096];
        while ((item = fscanf(fp, "%s", f2c) == 1))
        {
          if (debug) fprintf(stdout, "[eos-cp] add file %s\n", f2c);
          XrdOucString sf2c = f2c;
          sf2c.insert(sourceprefix, 0);
	  if (source_opaque.length()) {
	    sf2c += "?";
	    sf2c += source_opaque;
	  }
          source_list.push_back(sf2c.c_str());
          source_base_list.push_back(source_find_list[nfile]);
        }
      }
      if (fp)fclose(fp);
    }
  }

  // check if there is any file in the list
  if (!source_list.size())
  {
    fprintf(stderr, "warning: there is no file to copy!\n");
    //    fprintf(stderr,"Error: your source seems not to exist or does not match any file!\n");
    global_retc = 0;
    exit(0);
  }

  // create the target directory if it is a local one
  if ((!target.beginswith("/eos")))
  {
    if ((target.find(":/") == STR_NPOS) && (!target.beginswith("as3:")))
    {
      if (target.endswith("/"))
      {
        XrdOucString mktarget = "mkdir --mode 755 -p ";
        mktarget += target.c_str();
        int rc = system(mktarget.c_str());
        if (WEXITSTATUS(rc))
        {
          // we check with access if it worked 
          rc = 0;
        }
        if (access(target.c_str(), R_OK | W_OK))
        {
          fprintf(stderr, "Error: cannot create/access your target directory!\n");
          exit(0);
        }
      }
      else
      {
        eos::common::Path cTarget(target.c_str());
        XrdOucString mktarget = "mkdir --mode 755 -p ";
        mktarget += cTarget.GetParentPath();
        int rc = system(mktarget.c_str());
        if (WEXITSTATUS(rc))
        {
          // we check with access if it worked 
          rc = 0;
        }
        if (access(cTarget.GetParentPath(), R_OK | W_OK))
        {
          fprintf(stderr, "Error: cannot create/access your target directory!\n");
          exit(0);
        }
      }
    }
  }


  // compute the size to copy
  std::vector<std::string> file_info;
  for (size_t nfile = 0; nfile < source_list.size(); nfile++)
  {
    bool statok = false;
    // ------------------------------------------
    // EOS file
    // ------------------------------------------

    if (source_list[nfile].beginswith("/eos/"))
    {
      struct stat buf;
      XrdOucString url = serveruri.c_str();
      url += "/";
      url += source_list[nfile];
      if (!XrdPosixXrootd::Stat(url.c_str(), &buf))
      {
        if (S_ISDIR(buf.st_mode))
        {
          fprintf(stderr, "Error: %s is a directory - use '-r' to copy directories!\n",
                  source_list[nfile].c_str());
          cpCmd.printUsage();
          return 0;
        }
        if (debug)
        {
          fprintf(stderr, "[eos-cp] path=%s size=%llu\n", source_list[nfile].c_str(),
                  (unsigned long long) buf.st_size);
        }

        if (!silent)
        {
          fprintf(stderr, "[eos-cp] path=%s size=%llu\n", source_list[nfile].c_str(),
                  (unsigned long long) buf.st_size);
        }

        copysize += buf.st_size;
        source_size.push_back((unsigned long long) buf.st_size);

        timespec atime;
        timespec mtime;

        atime.tv_sec = time(NULL);
        atime.tv_nsec = 0;
        mtime.tv_sec = buf.st_mtime;
        mtime.tv_nsec = 0;

        // store the a/m-time
        source_utime.push_back(std::make_pair(atime, mtime));
        statok = true;
      }
    }
    XrdOucString s3env = "";
    // ------------------------------------------
    // S3 file
    // ------------------------------------------
    if (source_list[nfile].beginswith("as3:"))
    {
      // extract evt. the hostname
      // the hostname is part of the URL like in ROOT
      XrdOucString hostport;
      XrdOucString protocol;
      XrdOucString sPath;
      const char* v = 0;
      if (!(v = eos::common::StringConversion::ParseUrl(source_list[nfile].c_str(), protocol, hostport)))
      {
        fprintf(stderr, "Error: illegal url <%s>\n", source_list[nfile].c_str());
        global_retc = EINVAL;
        return (0);
      }
      sPath = v;

      if (hostport.length())
      {
        setenv("S3_HOSTNAME", hostport.c_str(), 1);
      }

      XrdOucString envString = source_list[nfile];
      int qpos = 0;
      if ((qpos = envString.find("?")) != STR_NPOS)
      {
        envString.erase(0, qpos + 1);
        XrdOucEnv env(envString.c_str());
        // extract opaque S3 tags if present
        if (env.Get("s3.key"))
        {
          setenv("S3_SECRET_ACCESS_KEY", env.Get("s3.key"), 1);
        }
        if (env.Get("s3.id"))
        {
          setenv("S3_ACCESS_KEY_ID", env.Get("s3.id"), 1);
        }
        source_list[nfile].erase(source_list[nfile].find("?"));
        sPath.erase(sPath.find("?"));
      }

      // apply the ROOT compatability environment variables
      if (getenv("S3_ACCESS_KEY")) setenv("S3_SECRET_ACCESS_KEY", getenv("S3_ACCESS_KEY"), 1);
      if (getenv("S3_ACESSS_ID")) setenv("S3_ACCESS_KEY_ID", getenv("S3_ACCESS_ID"), 1);

      // check that the environment is set
      if (!getenv("S3_ACCESS_KEY_ID") ||
          !getenv("S3_HOSTNAME") ||
          !getenv("S3_SECRET_ACCESS_KEY"))
      {
        fprintf(stderr, "Error: you have to set the S3 environment variables "
                "S3_ACCESS_KEY_ID | S3_ACCESS_ID, S3_HOSTNAME (or use a URI), "
                "S3_SECRET_ACCESS_KEY | S3_ACCESS_KEY\n");
        exit(-1);
      }
      s3env = "env S3_ACCESS_KEY_ID=";
      s3env += getenv("S3_ACCESS_KEY_ID");
      s3env += " S3_HOSTNAME=";
      s3env += getenv("S3_HOSTNAME");
      s3env += " S3_SECRET_ACCESS_KEY=";
      s3env += getenv("S3_SECRET_ACCESS_KEY");

      XrdOucString s3arg = sPath.c_str();

      // do some bash magic ... sigh
      XrdOucString sizecmd = "bash -c \"";
      sizecmd += s3env;
      sizecmd += " s3 head ";
      sizecmd += s3arg;
      sizecmd += " | grep Content-Length| awk '{print \\$2}' 2>/dev/null";
      sizecmd += "\"";

      if (debug) fprintf(stderr, "[eos-cp] running %s\n", sizecmd.c_str());

      long long size = eos::common::StringConversion::LongLongFromShellCmd(sizecmd.c_str());
      if ((!size) || (size == LLONG_MAX))
      {
        fprintf(stderr, "Error: cannot obtain the size of the <s3> source file or it has 0 size!\n");
        exit(-1);
      }
      if (debug)fprintf(stderr, "[eos-cp] path=%s size=%lld\n", source_list[nfile].c_str(), size);
      copysize += size;
      source_size.push_back(size);
      statok = true;
    }

    if (source_list[nfile].beginswith("http:") ||
        source_list[nfile].beginswith("https://") ||
        source_list[nfile].beginswith("gsiftp://"))
    {
      fprintf(stderr, "warning: disabling size check for http/https/gsidftp\n");
      statok = true;
      source_size.push_back(0);
    }

    if ((source_list[nfile].beginswith("root:")))
    {
      // ------------------------------------------
      // XRootD file
      // ------------------------------------------
      struct stat buf;
      if (!XrdPosixXrootd::Stat(source_list[nfile].c_str(), &buf))
      {
        if (S_ISDIR(buf.st_mode))
        {
          fprintf(stderr, "Error: %s is a directory - use '-r' to copy directories\n",
                  source_list[nfile].c_str());
          cpCmd.printUsage();
          return 0;
        }

        if (debug)
        {
          fprintf(stderr, "[eos-cp] path=%s size=%llu\n", source_list[nfile].c_str(),
                  (unsigned long long) buf.st_size);
        }

        copysize += buf.st_size;
        source_size.push_back((unsigned long long) buf.st_size);

        timespec atime;
        timespec mtime;

        atime.tv_sec = time(NULL);
        atime.tv_nsec = 0;
        mtime.tv_sec = buf.st_mtime;
        mtime.tv_nsec = 0;

        // store the a/m-time
        source_utime.push_back(std::make_pair(atime, mtime));
        statok = true;
      }
    }

    if ((source_list[nfile].find(":/") == STR_NPOS) && (!source_list[nfile].beginswith("/eos")))
    {
      // ------------------------------------------
      // local file
      // ------------------------------------------
      struct stat buf;
      if (!stat(source_list[nfile].c_str(), &buf))
      {
        if (S_ISDIR(buf.st_mode))
        {
          fprintf(stderr, "Error: %s is a directory - use '-r' to copy directories\n",
                  source_list[nfile].c_str());
          cpCmd.printUsage();
          return 0;
        }
        if (debug)
        {
          fprintf(stderr, "[eos-cp] path=%s size=%llu\n", source_list[nfile].c_str(),
                  (unsigned long long) buf.st_size);
        }

        copysize += buf.st_size;
        source_size.push_back((unsigned long long) buf.st_size);

        timespec atime;
        timespec mtime;

        atime.tv_sec = buf.st_atime;
        atime.tv_nsec = 0;
        mtime.tv_sec = buf.st_mtime;
        mtime.tv_nsec = 0;

        // store the a/m-time
        source_utime.push_back(std::make_pair(atime, mtime));
        statok = true;
      }
    }

    if (!statok)
    {
      fprintf(stderr, "Error: we don't support this protocol or we cannot get the file size of source file %s\n", source_list[nfile].c_str());
      exit(-1);
    }
  }

  XrdOucString sizestring1;
  if (!silent)
  {
    fprintf(stderr, "[eos-cp] going to copy %d files and %s\n", (int) source_list.size(),
            eos::common::StringConversion::GetReadableSizeString(sizestring1, copysize, "B"));
  }

  gettimeofday(&tv1, &tz);
  // process the file list for wildcards
  for (size_t nfile = 0; nfile < source_list.size(); nfile++)
  {
    XrdOucString targetfile = "";
    XrdOucString transfersize = ""; // used for STDIN pipes to specify the target size ot eoscp

    cmdline = "";
    eos::common::Path cPath(source_list[nfile].c_str());
    arg1 = source_list[nfile];

    if (arg1.beginswith("./"))
    {
      arg1.erase(0, 2);
    }

    arg2 = target;

    if (arg2 == "-")
    {
      // if we have stdout as target we disable all output
      silent = true;
      noprogress = true;
    }

    if (arg2.beginswith("as3://"))
    {
      // apply the ROOT compatability environment variables
      if (getenv("S3_ACCESS_KEY")) setenv("S3_SECRET_ACCESS_KEY", getenv("S3_ACCESS_KEY"), 1);
      if (getenv("S3_ACESSS_ID")) setenv("S3_ACCESS_KEY_ID", getenv("S3_ACCESS_ID"), 1);
      // extract opaque S3 tags if present

      XrdOucString envString = arg2;
      int qpos = 0;
      if ((qpos = envString.find("?")) != STR_NPOS)
      {
        envString.erase(0, qpos + 1);
        XrdOucEnv env(envString.c_str());
        // extract opaque S3 tags if present
        if (env.Get("s3.key"))
        {
          setenv("S3_SECRET_ACCESS_KEY", env.Get("s3.key"), 1);
        }
        if (env.Get("s3.id"))
        {
          setenv("S3_ACCESS_KEY_ID", env.Get("s3.id"), 1);
        }
        arg2.erase(source_list[nfile].find("?"));
      }

      // the hostname is part of the URL like in ROOT
      int spos = arg2.find("/", 6);
      if (spos != STR_NPOS)
      {
        XrdOucString hname;
        hname.assign(arg2, 6, spos - 1);
        setenv("S3_HOSTNAME", hname.c_str(), 1);
        arg2.erase(4, spos - 3);
      }

    }

    if (arg2.beginswith("."))
    {
      arg2.erase(0, 2);
    }

    if (arg1.beginswith("/eos"))
    {
      arg1.insert("/", 0);
      arg1.insert(serveruri.c_str(), 0);
    }

    if (arg2.endswith("/"))
    {
      if (recursive)
      {
        // append the source directory structure
        XrdOucString targetname = source_list[nfile];
        targetname.replace(source_base_list[nfile], "");
        arg2.append(targetname.c_str());
      }
      else
      {
        arg2.append(cPath.GetName());
      }
    }

    if (arg2.beginswith("/") && !arg2.beginswith("/eos/")) 
    {
      // remove the opaque info for local files
      arg2.erase(arg2.find("?"));
    }

    targetfile = arg2;

    if (arg2.beginswith("/eos") || arg2.beginswith("root://"))
    {
      if (arg2.beginswith("/eos"))
      {
        arg2.insert("/", 0);
        arg2.insert(serveruri.c_str(), 0);
      }
      char targetadd[1024];
      if ((arg2.find("?") == STR_NPOS))
      {
        arg2 += "?";
      }
      else
      {
        arg2 += "&";
      }
      snprintf(targetadd, sizeof (targetadd) - 1, "eos.targetsize=%llu&eos.bookingsize=%llu&eos.app=eoscp", source_size[nfile], source_size[nfile]);
      arg2.append(targetadd);
      // put the proper role switches
      if (user_role.length() && group_role.length())
      {
        arg2 += "&eos.ruid=";
        arg2 += user_role;
        arg2 += "&eos.rgid=";
        arg2 += group_role;
      }
    }

    // ------------------------------
    // check for external copy tools
    // ------------------------------
    if (arg1.beginswith("http:") || arg2.beginswith("https:"))
    {
      int rc = system("which curl >&/dev/null");
      if (WEXITSTATUS(rc))
      {
        fprintf(stderr, "Error: you miss the <curl> executable in your PATH\n");
        exit(-1);
      }
    }

    if (arg1.beginswith("as3:") || (arg2.beginswith("as3:")))
    {
      int rc = system("which s3 >&/dev/null");
      if (WEXITSTATUS(rc))
      {
        fprintf(stderr, "Error: you miss the <s3> executable provided by libs3 in your PATH\n");
        exit(-1);
      }
    }

    if (arg1.beginswith("gsiftp:") || arg2.beginswith("gsiftp:"))
    {
      int rc = system("which globus-url-copy >&/dev/null");
      if (WEXITSTATUS(rc))
      {
        fprintf(stderr, "Error: you miss the <globus-url-copy> executable in your PATH\n");
        exit(-1);
      }
    }

    if (((arg2.find(":/") != STR_NPOS) && (!arg2.beginswith("root:"))))
    {
      // if the target is any other protocol than root: we download to a temporary file
      upload_target = arg2;
      arg2 = tmpnam(NULL);
      targetfile = arg2;
    }


    if (nooverwrite)
    {
      struct stat buf;
      // check if target exists
      if (targetfile.beginswith("/eos/"))
      {
        XrdOucString url = serveruri.c_str();
        url += "/";
        url += targetfile;

	if ((url.find("?") == STR_NPOS))
        {
	  url += "?";
	}
	else
	  {
            url += "&";
        }
	url += "eos.app=eoscp";

        // add the 'role' switches to the URL
        if (user_role.length() && group_role.length())
        {
          url += "&eos.ruid=";
          url += user_role;
          url += "&eos.rgid=";
          url += group_role;
        }
        if (!XrdPosixXrootd::Stat(url.c_str(), &buf))
        {
          fprintf(stderr, "Error: target file %s exists and you specified no overwrite!\n", targetfile.c_str());
          continue;
        }
      }
      else
      {
        if (targetfile.beginswith("/"))
        {
          if (!stat(targetfile.c_str(), &buf))
          {
            fprintf(stderr, "Error: target file %s exists and you specified no overwrite!\n", targetfile.c_str());
            continue;
          }
        }
      }
    }

    if (interactive)
    {
      if (!arg1.beginswith("/"))
      {
        arg1.insert(pwd.c_str(), 0);
      }
      if (!arg2.beginswith("/"))
      {
        arg2.insert(pwd.c_str(), 0);
      }
    }

    bool rstdin = false;
    bool rstdout = false;

    if ((arg1.beginswith("http:")) || (arg1.beginswith("https:")))
    {
      cmdline += "curl ";
      if (arg1.beginswith("https:"))
      {
        cmdline += "-k ";
      }
      cmdline += "\"";
      cmdline += arg1;
      cmdline += "\"";
      cmdline += " |";
      rstdin = true;
      noprogress = true;
    }

    if (arg1.beginswith("as3:") || arg2.beginswith("as3:"))
    {
      char ts[1024];
      snprintf(ts, sizeof (ts) - 1, "%llu", source_size[nfile]);
      transfersize = ts;
    }

    if (arg1.beginswith("as3:"))
    {
      XrdOucString s3arg = arg1;
      s3arg.replace("as3:", "");
      cmdline += "s3 get ";
      cmdline += "\"";
      cmdline += s3arg;
      cmdline += "\"";
      cmdline += " |";
      rstdin = true;
    }

    if (arg1.beginswith("gsiftp:"))
    {
      cmdline += "globus-url-copy ";
      cmdline += "\"";
      cmdline += arg1;
      cmdline += "\"";
      cmdline += " - |";
      rstdin = true;
      noprogress = true;
    }

    if (arg2.beginswith("as3:"))
    {
      rstdout = true;
    }

    if (arg1.beginswith("root:"))
    {
      if ((arg1.find("?") == STR_NPOS))
      { 
	arg1 += "?";
      }
      else
      {
	arg1 += "&";
      }

      arg1 += "eos.app=eoscp";
      // add the 'role' switches to the URL
      if (user_role.length() && group_role.length())
      {
        arg1 += "&eos.ruid=";
        arg1 += user_role;
        arg1 += "&eos.rgid=";
        arg1 += group_role;
      }
    }

    // everything goes either via a stage file or direct
    cmdline += "eoscp -p ";
    if (append) cmdline += "-a ";
    if (!summary) cmdline += "-s ";
    if (noprogress) cmdline += "-n ";
    if (transfersize.length()) cmdline += "-T ";
    cmdline += transfersize;
    cmdline += " ";
    cmdline += "-N ";
    cmdline += cPath.GetName();
    cmdline += " ";
    if (rstdin)
    {
      cmdline += "- ";
    }
    else
    {
      cmdline += "\"";
      cmdline += arg1;
      cmdline += "\"";
      cmdline += " ";
    }
    if (rstdout)
    {
      cmdline += "- ";
    }
    else
    {
      cmdline += "\"";
      cmdline += arg2;
      cmdline += "\"";
    }

    if (arg2.beginswith("as3:"))
    {
      // s3 can upload via STDIN setting the upload size externally - yeah!
      cmdline += "| s3 put ";
      XrdOucString s3arg = arg2;
      s3arg.replace("as3:", "");
      cmdline += s3arg;
      cmdline += " contentLength=";
      cmdline += transfersize.c_str();
      cmdline += " > /dev/null";
    }

    if (debug)fprintf(stderr, "[eos-cp] running: %s\n", cmdline.c_str());
    int lrc = system(cmdline.c_str());
    int erc = lrc ; // the original return code
    // check the target size
    struct stat buf;

    if ((targetfile.beginswith("/eos/") || (targetfile.beginswith("root://"))))
    {
      buf.st_size = 0;
      XrdOucString url = serveruri.c_str();
      url += "/";
      if (targetfile.beginswith("root://"))
      {
        url = targetfile;
      }
      else
      {
        url += targetfile;
      }

      if ((url.find("?") == STR_NPOS))
      {
	url += "?";
      }
      else
      {
	url += "&";
      }

      url += "eos.app=eoscp";
      // add the 'role' switches to the URL
      if (user_role.length() && group_role.length())
      {
        url += "&eos.ruid=";
        url += user_role;
        url += "&eos.rgid=";
        url += group_role;
      }

      if (!XrdPosixXrootd::Stat(url.c_str(), &buf))
      {
        if ((source_size[nfile]) && (buf.st_size != (off_t) source_size[nfile]))
        {
          fprintf(stderr, "Error: filesize differ between source and target file!\n");
          lrc = 0xffff00;
        }
        else
        {
          if (preserve && (source_size.size() == source_utime.size()))
          {
            char value[4096];
            value[0] = 0;
            ;
            XrdOucString request;
            request = url.c_str();
            if ((url.find("?") == STR_NPOS))
            {
              request += "?";
            }
            else
            {
              request += "&";
            }
            request += "mgm.pcmd=utimes&tv1_sec=";
            char lltime[1024];
            sprintf(lltime, "%llu", (unsigned long long) source_utime[nfile].first.tv_sec);
            request += lltime;
            request += "&tv1_nsec=";
            sprintf(lltime, "%llu", (unsigned long long) source_utime[nfile].first.tv_nsec);
            request += lltime;
            request += "&tv2_sec=";
            sprintf(lltime, "%llu", (unsigned long long) source_utime[nfile].second.tv_sec);
            request += lltime;
            request += "&tv2_nsec=";
            sprintf(lltime, "%llu", (unsigned long long) source_utime[nfile].second.tv_nsec);
            request += lltime;

            long long doutimes = XrdPosixXrootd::QueryOpaque(request.c_str(), value, 4096);
            if (doutimes >= 0)
            {
              char tag[1024];
              int retc;
              // parse the stat output
              int items = sscanf(value, "%s retc=%d", tag, &retc);
              if ((items != 2) || (strcmp(tag, "utimes:")))
              {
                fprintf(stderr, "warning: creation/modification time could not be preserved for %s\n", targetfile.c_str());
              }
            }
            else
            {
              fprintf(stderr, "warning: creation/modification time could not be preserved for %s\n", targetfile.c_str());
            }
          }
        }
      }
      else
      {
        fprintf(stderr, "Error: target file was not created!\n");
        lrc = 0xffff00;
      }
    }

    if (((arg2.find(":/") == STR_NPOS) && (!arg2.beginswith("as3:"))))
    {
      // exclude STDOUT
      if (arg2 != "-")
      {
        // this is a local file
        buf.st_size = 0;
        if (!stat(targetfile.c_str(), &buf))
        {
          if ((source_size[nfile]) && (buf.st_size != (off_t) source_size[nfile]))
          {
            fprintf(stderr, "Error: filesize differ between source and target file!\n");
            lrc = 0xffff00;
          }
          else
          {
            if (preserve && (source_size.size() == source_utime.size()))
            {
              struct timeval times[2];
              times[0].tv_sec = source_utime[nfile].first.tv_sec;
              times[0].tv_usec = source_utime[nfile].first.tv_nsec / 1000;
              times[1].tv_sec = source_utime[nfile].second.tv_sec;
              times[1].tv_usec = source_utime[nfile].second.tv_nsec / 1000;

              if (utimes(targetfile.c_str(), times))
              {
                fprintf(stderr, "warning: creation/modification time could not be preserved for %s\n", targetfile.c_str());
              }
            }
          }
        }
        else
        {
          fprintf(stderr, "Error: target file was not created!\n");
          lrc = 0xffff00;
        }
      }
    }
    if (!WEXITSTATUS(lrc))
    {
      if (target.beginswith("/eos"))
      {
        if (checksums)
        {
          XrdOucString address = serveruri.c_str();
          address += "//dummy";
          XrdCl::URL url(address.c_str());

          if (!url.IsValid())
          {
            fprintf(stderr, "Error: the file system URL is not valid.\n");
            return (0);
          }

          XrdCl::FileSystem* fs = new XrdCl::FileSystem(url);

          if (!fs)
          {
            fprintf(stderr, "erroe: failed to get new FS object. \n");
            return (0);
          }

          XrdCl::Buffer arg;
          XrdCl::Buffer* response = 0;
          XrdCl::XRootDStatus status;
          arg.FromString(targetfile.c_str());
          status = fs->Query(XrdCl::QueryCode::Checksum, arg, response);

          if (status.IsOK())
          {
            XrdOucString sanswer = response->GetBuffer();
            sanswer.replace("eos ", "");
            fprintf(stdout, "path=%s size=%llu checksum=%s\n",
                    source_list[nfile].c_str(), source_size[nfile], sanswer.c_str());
          }
          else
          {
            fprintf(stdout, "Error: getting checksum for path=%s size=%llu\n",
                    source_list[nfile].c_str(), source_size[nfile]);
          }

          delete response;
          delete fs;
        }
      }

      if (upload_target.length())
      {
        bool uploadok = false;
        if (upload_target.beginswith("as3:"))
        {
          XrdOucString s3arg = upload_target;
          s3arg.replace("as3:", "");
          cmdline = "s3 put ";
          cmdline += s3arg;
          cmdline += " filename=";
          cmdline += arg2;
          if (noprogress || silent)
          {
            cmdline += " >& /dev/null";
          }
          if (debug)
          {
            fprintf(stderr, "[eos-cp] running: %s\n", cmdline.c_str());
          }
          int rc = system(cmdline.c_str());
          if (WEXITSTATUS(rc))
          {
            fprintf(stderr, "Error: failed to upload to <s3>\n");
            uploadok = false;
          }
          else
          {
            uploadok = true;
          }
        }
        if (upload_target.beginswith("http:"))
        {
          fprintf(stderr, "Error: we don't support file uploads with http/https protocol\n");
          uploadok = false;
        }
        if (upload_target.beginswith("https:"))
        {
          fprintf(stderr, "Error: we don't support file uploads with http/https protocol\n");
          uploadok = false;
        }
        if (upload_target.beginswith("gsiftp:"))
        {
          cmdline = "globus-url-copy file://";
          cmdline += arg2;
          cmdline += " ";
          cmdline += upload_target;
          if (silent)
          {
            cmdline += " >&/dev/null";
          }
          if (debug)
          {
            fprintf(stderr, "[eos-cp] running: %s\n", cmdline.c_str());
          }
          int rc = system(cmdline.c_str());
          if (WEXITSTATUS(rc))
          {
            fprintf(stderr, "Error: failed to upload to <gsiftp>\n");
            uploadok = false;
          }
          else
          {
            uploadok = true;
          }
          uploadok = true;
        }
        // clean-up the tmp file in any case
        unlink(arg2.c_str());

        if (!uploadok)
        {
          lrc |= 0xffff00;
        }
        else
        {
          copiedok++;
          copiedsize += source_size[nfile];
        }
      }
      else
      {
        copiedok++;
        copiedsize += source_size[nfile];
      }
    } 
    // check if we got a CONTROL-C
    if ( erc == EINTR) 
    {
      fprintf(stderr,"<Control-C>\n");
      retc |= lrc;
      break;
    }

    retc |= lrc;
  }

  gettimeofday(&tv2, &tz);

  float passed = (float) (((tv2.tv_sec - tv1.tv_sec) *1000000 + (tv2.tv_usec - tv1.tv_usec)) / 1000000.0);
  float crate = (copiedsize * 1.0 / passed);
  XrdOucString sizestring = "";
  XrdOucString sizestring2 = "";
  XrdOucString warningtag = "";
  if (retc) warningtag = "#WARNING ";
  if (!silent)
  {
    fprintf(stderr, "%s[eos-cp] copied %d/%d files and %s in %.02f seconds with %s\n",
            warningtag.c_str(),
            copiedok,
            (int) source_list.size(),
            eos::common::StringConversion::GetReadableSizeString(sizestring, copiedsize, "B"),
            passed,
            eos::common::StringConversion::GetReadableSizeString(sizestring2, (unsigned long long) crate, "B/s"));
  }
  exit(WEXITSTATUS(retc));
}
