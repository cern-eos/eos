// ----------------------------------------------------------------------
// File: com_find.cc
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
#include "console/ConsoleMain.hh"
/*----------------------------------------------------------------------------*/

extern int com_file (char*);

/* Find files/directories */
int
com_find (char* arg1)
{
  // split subcommands
  XrdOucString oarg = arg1;

  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString s1;
  XrdOucString path;
  XrdOucString option = "";
  XrdOucString attribute = "";
  XrdOucString olderthan = "";
  XrdOucString youngerthan = "";
  XrdOucString printkey = "";
  XrdOucString filter = "";
  XrdOucString stripes = "";

  XrdOucString in = "mgm.cmd=find&";

  bool valid = false;

  while ((s1 = subtokenizer.GetToken()).length() && (s1.beginswith("-")))
  {
    if (s1 == "-s")
    {
      option += "s";
      valid = true;
    }

    if (s1 == "-d")
    {
      option += "d";
      valid = true;
    }

    if (s1 == "-f")
    {
      option += "f";
      valid = true;
    }

    if (s1 == "-0")
    {
      option += "f0";
      valid = true;
    }

    if (s1 == "-m")
    {
      option += "fG";
      valid = true;
    }

    if (s1 == "--size")
    {
      option += "S";
      valid = true;
    }


    if (s1 == "--fs")
    {
      option += "L";
      valid = true;
    }

    if (s1 == "--checksum")
    {
      option += "X";
      valid = true;
    }


    if (s1 == "--ctime")
    {
      option += "C";
      valid = true;
    }

    if (s1 == "--mtime")
    {
      option += "M";
      valid = true;
    }

    if (s1 == "--fid")
    {
      option += "F";
      valid = true;
    }

    if (s1 == "--nrep")
    {
      option += "R";
      valid = true;
    }

    if (s1 == "--online")
    {
      option += "O";
      valid = true;
    }

    if (s1 == "--fileinfo")
    {
      option += "I";
      valid = true;
    }

    if (s1 == "--nunlink")
    {
      option += "U";
      valid = true;
    }

    if (s1 == "--stripediff")
    {
      option += "D";
      valid = true;
    }

    if (s1 == "--faultyacl")
    {
      option += "A";
      valid = true;
    }

    if (s1 == "--count")
    {
      option += "Z";
      valid = true;
    }

    if (s1 == "--hosts")
    {
      option += "H";
      valid = true;
    }

    if (s1 == "--partition")
    {
      option += "P";
      valid = true;
    }

    if (s1 == "--childcount")
    {
      option += "l";
      valid = true;
    }

    if (s1 == "-1")
    {
      option += "1";
      valid = true;
    }

    if (s1.beginswith("-h") || (s1.beginswith("--help")))
    {
      goto com_find_usage;
    }

    if (s1 == "-x")
    {
      option += "x";
      valid = true;

      attribute = subtokenizer.GetToken();

      if (!attribute.length())
        goto com_find_usage;

      if ((attribute.find("&")) != STR_NPOS)
        goto com_find_usage;
    }

    if ((s1 == "-ctime") || (s1 == "-mtime"))
    {
      valid = true;
      XrdOucString period = "";
      period = subtokenizer.GetToken();

      if (!period.length())
        goto com_find_usage;

      bool do_olderthan;
      do_olderthan = false;
      bool do_youngerthan;
      do_youngerthan = false;

      if (period.beginswith("+"))
      {
        do_olderthan = true;
      }

      if (period.beginswith("-"))
      {
        do_youngerthan = true;
      }

      if ((!do_olderthan) && (!do_youngerthan))
      {
        goto com_find_usage;
      }

      period.erase(0, 1);
      time_t now = time(NULL);
      now -= (86400 * strtoul(period.c_str(), 0, 10));
      char snow[1024];
      snprintf(snow, sizeof (snow) - 1, "%lu", now);
      if (do_olderthan)
      {
        olderthan = snow;
      }
      if (do_youngerthan)
      {
        youngerthan = snow;
      }
      if (s1 == "-ctime")
      {
        option += "C";
      }
      if (s1 == "-mtime")
      {
        option += "M";
      }
    }

    if (s1 == "-c")
    {
      valid = true;
      option += "c";

      filter = subtokenizer.GetToken();
      if (!filter.length())
        goto com_find_usage;

      if ((filter.find("%%")) != STR_NPOS)
      {
        goto com_find_usage;
      }
    }

    if (s1 == "-layoutstripes")
    {
      valid = true;
      stripes = subtokenizer.GetToken();
      if (!stripes.length())
        goto com_find_usage;
    }

    if (s1 == "-p")
    {
      valid = true;
      option += "p";

      printkey = subtokenizer.GetToken();

      if (!printkey.length())
        goto com_find_usage;
    }

    if (s1 == "-b")
    {
      valid = true;
      option += "b";
    }

    if (!valid)
      goto com_find_usage;
  }

  if (s1.length())
  {
    path = s1;
  }

  if (path == "help")
  {
    goto com_find_usage;
  }

  if (!path.endswith("/"))
  {
    if (!path.endswith(":"))
    {
      // if the user gave file: as a search path we shouldn't add '/'=root
      path += "/";
    }
  }

  if (path.beginswith("root://") || path.beginswith("file:"))
  {
    // -------------------------------------------------------------
    // do a find with XRootd or local file system
    // -------------------------------------------------------------

    bool XRootD = path.beginswith("root:");

    std::vector< std::vector<std::string> > found_dirs;
    std::map<std::string, std::set<std::string> > found;
    XrdOucString protocol;
    XrdOucString hostport;
    XrdOucString sPath;

    if (path == "/")
    {
      fprintf(stderr, "error: I won't do a find on '/'\n");
      global_retc = EINVAL;
      return (0);
    }

    const char* v = 0;
    if (!(v = eos::common::StringConversion::ParseUrl(path.c_str(), protocol, hostport)))
    {
      global_retc = EINVAL;
      return (0);
    }

    sPath = v;
    std::string Path = v;

    if (sPath == "" && (protocol == "file"))
    {
      sPath = getenv("PWD");
      Path = getenv("PWD");
      if (!sPath.endswith("/"))
      {
        sPath += "/";
        Path += "/";
      }
    }

    found_dirs.resize(1);
    found_dirs[0].resize(1);
    found_dirs[0][0] = Path.c_str();
    int deepness = 0;
    do
    {
      struct stat buf;
      found_dirs.resize(deepness + 2);
      // loop over all directories in that deepness
      for (unsigned int i = 0; i < found_dirs[deepness].size(); i++)
      {
        Path = found_dirs[deepness][i].c_str();
        XrdOucString url = "";
        eos::common::StringConversion::CreateUrl(protocol.c_str(), hostport.c_str(), Path.c_str(), url);
        int rstat = 0;
        rstat = (XRootD) ? XrdPosixXrootd::Stat(url.c_str(), &buf) : stat(url.c_str(), &buf);
        if (!rstat)
        {
          //
          if (S_ISDIR(buf.st_mode))
          {
            // add all children 
            DIR* dir = (XRootD) ? XrdPosixXrootd::Opendir(url.c_str()) : opendir(url.c_str());
            if (dir)
            {
              struct dirent* entry;
              while ((entry = (XRootD) ? XrdPosixXrootd::Readdir(dir) : readdir(dir)))
              {
                XrdOucString curl = "";
                XrdOucString cpath = Path.c_str();
                cpath += entry->d_name;
                if ((!strcmp(entry->d_name, ".")) || (!strcmp(entry->d_name, "..")))
                  continue; // skip . and .. directories

                eos::common::StringConversion::CreateUrl(protocol.c_str(), hostport.c_str(), cpath.c_str(), curl);
                if (!((XRootD) ? XrdPosixXrootd::Stat(curl.c_str(), &buf) : stat(curl.c_str(), &buf)))
                {
                  if (S_ISDIR(buf.st_mode))
                  {
                    curl += "/";
                    cpath += "/";
                    found_dirs[deepness + 1].push_back(cpath.c_str());
                    found[curl.c_str()].size();
                  }
                  else
                  {
                    found[url.c_str()].insert(entry->d_name);
                  }
                }
              }
              (XRootD) ? XrdPosixXrootd::Closedir(dir) : closedir(dir);
            }
          }
        }
      }
      deepness++;
    }
    while (found_dirs[deepness].size());

    bool show_files = false;
    bool show_dirs = false;
    if ((option.find("f") == STR_NPOS) && (option.find("d") == STR_NPOS))
    {
      show_files = show_dirs = true;
    }
    else
    {
      if (option.find("f") != STR_NPOS) show_files = true;
      if (option.find("d") != STR_NPOS) show_dirs = true;
    }
    std::map<std::string, std::set<std::string> >::const_iterator it;
    for (it = found.begin(); it != found.end(); it++)
    {
      std::set<std::string>::const_iterator sit;

      if (show_dirs) fprintf(stdout, "%s\n", it->first.c_str());
      for (sit = it->second.begin(); sit != it->second.end(); sit++)
      {
        if (show_files) fprintf(stdout, "%s%s\n", it->first.c_str(), sit->c_str());
      }
    }
    return 0;
  }

  if (path.beginswith("as3:"))
  {
    // ----------------------------------------------------------------
    // this is nightmare code because of a missing proper CLI for S3
    // ----------------------------------------------------------------
    XrdOucString hostport;
    XrdOucString protocol;

    int rc = system("which s3 >&/dev/null");
    if (WEXITSTATUS(rc))
    {
      fprintf(stderr, "error: you miss the <s3> executable provided by libs3 in your PATH\n");
      exit(-1);
    }

    if (path.endswith("/"))
    {
      path.erase(path.length() - 1);
    }

    XrdOucString sPath = path.c_str();
    XrdOucString sOpaque;
    int qpos = 0;
    if ((qpos = sPath.find("?")) != STR_NPOS)
    {
      sOpaque.assign(sPath, qpos + 1);
      sPath.erase(qpos);
    }

    XrdOucString fPath = eos::common::StringConversion::ParseUrl(sPath.c_str(), protocol, hostport);
    XrdOucEnv env(sOpaque.c_str());
    if (env.Get("s3.key"))
    {
      setenv("S3_SECRET_ACCESS_KEY", env.Get("s3.key"), 1);
    }
    if (env.Get("s3.id"))
    {
      setenv("S3_ACCESS_KEY_ID", env.Get("s3.id"), 1);
    }
    // apply the ROOT compatability environment variables
    if (getenv("S3_ACCESS_KEY")) setenv("S3_SECRET_ACCESS_KEY", getenv("S3_ACCESS_KEY"), 1);
    if (getenv("S3_ACESSS_ID")) setenv("S3_ACCESS_KEY_ID", getenv("S3_ACCESS_ID"), 1);

    // check that the environment is set
    if (!getenv("S3_ACCESS_KEY_ID") ||
        !getenv("S3_HOSTNAME") ||
        !getenv("S3_SECRET_ACCESS_KEY"))
    {
      fprintf(stderr, "error: you have to set the S3 environment variables S3_ACCESS_KEY_ID | S3_ACCESS_ID, S3_HOSTNAME (or use a URI), S3_SECRET_ACCESS_KEY | S3_ACCESS_KEY\n");
      global_retc = EINVAL;
      return (0);
    }

    XrdOucString s3env;
    s3env = "env S3_ACCESS_KEY_ID=";
    s3env += getenv("S3_ACCESS_KEY_ID");
    s3env += " S3_HOSTNAME=";
    s3env += getenv("S3_HOSTNAME");
    s3env += " S3_SECRET_ACCESS_KEY=";
    s3env += getenv("S3_SECRET_ACCESS_KEY");

    XrdOucString cmd = "bash -c \"";
    cmd += s3env;
    cmd += " s3 list ";
    // extract bucket from path
    int bpos = fPath.find("/");
    XrdOucString bucket;
    if (bpos != STR_NPOS)
    {
      bucket.assign(fPath, 0, bpos - 1);
    }
    else
    {
      bucket = fPath.c_str();
    }
    XrdOucString match;
    if (bpos != STR_NPOS)
    {
      match.assign(fPath, bpos + 1);
    }
    else
    {
      match = "";
    }
    if ((!bucket.length()) || (bucket.find("*") != STR_NPOS))
    {
      fprintf(stderr, "error: no bucket specified or wildcard in bucket name!\n");
      global_retc = EINVAL;
      return (0);
    }

    cmd += bucket.c_str();
    cmd += " | awk '{print \\$1}' ";
    if (match.length())
    {
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
      cmd += " | egrep '";
      cmd += match.c_str();
      cmd += "'";
    }
    cmd += " | grep -v 'Bucket' | grep -v '\\-\\-\\-\\-\\-\\-\\-\\-\\-\\-' | grep -v 'Key' | awk -v prefix=";
    cmd += "'";
    cmd += bucket.c_str();
    cmd += "' ";
    cmd += "'{print \\\"as3:\\\"prefix\\\"/\\\"\\$1}'";

    cmd += "\"";
    rc = system(cmd.c_str());
    if (WEXITSTATUS(rc))
    {
      fprintf(stderr, "error: failed to run %s\n", cmd.c_str());
    }
  }

  // the find to change a layout
  if ((stripes.length()))
  {
    XrdOucString subfind = oarg;
    XrdOucString repstripes = " ";
    repstripes += stripes;
    repstripes += " ";
    subfind.replace("-layoutstripes", "");
    subfind.replace(repstripes, " -f -s ");
    int rc = com_find((char*) subfind.c_str());
    std::vector<std::string> files_found;
    files_found.clear();
    command_result_stdout_to_vector(files_found);
    std::vector<std::string>::const_iterator it;
    unsigned long long cnt = 0;
    unsigned long long goodentries = 0;
    unsigned long long badentries = 0;
    for (unsigned int i = 0; i < files_found.size(); i++)
    {
      if (!files_found[i].length())
        continue;

      XrdOucString cline = "layout ";
      cline += files_found[i].c_str();
      cline += " -stripes ";
      cline += stripes;
      rc = com_file((char*) cline.c_str());
      if (rc)
      {
        badentries++;
      }
      else
      {
        goodentries++;
      }
      cnt++;
    }
    rc = 0;
    if (!silent)
    {
      fprintf(stderr, "nentries=%llu good=%llu bad=%llu\n", cnt, goodentries, badentries);
    }
    return 0;
  }

  // the find with consistency check 
  if ((option.find("c")) != STR_NPOS)
  {
    XrdOucString subfind = oarg;
    subfind.replace("-c", "-s -f");
    subfind.replace(filter, "");
    int rc = com_find((char*) subfind.c_str());
    std::vector<std::string> files_found;
    files_found.clear();
    command_result_stdout_to_vector(files_found);
    std::vector<std::string>::const_iterator it;
    unsigned long long cnt = 0;
    unsigned long long goodentries = 0;
    unsigned long long badentries = 0;
    for (unsigned int i = 0; i < files_found.size(); i++)
    {
      if (!files_found[i].length())
        continue;

      XrdOucString cline = "check ";
      cline += files_found[i].c_str();
      cline += " ";
      cline += filter;
      rc = com_file((char*) cline.c_str());
      if (rc)
      {
        badentries++;
      }
      else
      {
        goodentries++;
      }
      cnt++;
    }
    rc = 0;
    if (!silent)
    {
      fprintf(stderr, "nentries=%llu good=%llu bad=%llu\n", cnt, goodentries, badentries);
    }
    return 0;
  }


  path = abspath(path.c_str());

  if (!s1.length() && (path == "/"))
  {
    fprintf(stderr, "error: you didnt' provide any path and would query '/' - will not do that!\n");
    return EINVAL;
  }

  in += "mgm.path=";
  in += path;
  in += "&mgm.option=";
  in += option;
  if (attribute.length())
  {
    in += "&mgm.find.attribute=";
    in += attribute;
  }
  if (olderthan.length())
  {
    in += "&mgm.find.olderthan=";
    in += olderthan;
  }

  if (youngerthan.length())
  {
    in += "&mgm.find.youngerthan=";
    in += youngerthan;
  }

  if (printkey.length())
  {
    in += "&mgm.find.printkey=";
    in += printkey;
  }

  XrdOucEnv* result;
  result = client_user_command(in);
  if ((option.find("s")) == STR_NPOS)
  {
    global_retc = output_result(result);
  }
  else
  {
    if (result)
    {
      global_retc = 0;
    }
    else
    {
      global_retc = EINVAL;
    }
  }
  return (0);

com_find_usage:
  fprintf(stdout, "usage: find [--childcount] [--count] [-s] [-d] [-f] [-0] [-1] [-ctime +<n>|-<n>] [-m] [-x <key>=<val>] [-p <key>] [-b] [-c %%tags] [-layoutstripes <n>] <path>\n");
  fprintf(stdout, "                                                                        -f -d :  find files(-f) or directories (-d) in <path>\n");
  fprintf(stdout, "                                                               -x <key>=<val> :  find entries with <key>=<val>\n");
  fprintf(stdout, "                                                                           -0 :  find 0-size files \n");
  fprintf(stdout, "                                                                           -g :  find files with mixed scheduling groups\n");
  fprintf(stdout, "                                                                     -p <key> :  additionally print the value of <key> for each entry\n");
  fprintf(stdout, "                                                                           -b :  query the server balance of the files found\n");
  fprintf(stdout, "                                                                    -c %%tags  :  find all files with inconsistencies defined by %%tags [ see help of 'file check' command]\n");
  fprintf(stdout, "                                                                           -s :  run as a subcommand (in silent mode)\n");
  fprintf(stdout, "                                                                  -ctime +<n> :  find files older than <n> days\n");
  fprintf(stdout, "                                                                  -ctime -<n> :  find files younger than <n> days\n");
  fprintf(stdout, "                                                           -layoutstripes <n> :  apply new layout with <n> stripes to all files found\n");
  fprintf(stdout, "                                                                           -1 :  find files which are atleast 1 hour old\n");
  fprintf(stdout, "                                                                 --stripediff :  find files which have not the nominal number of stripes(replicas)\n");
  fprintf(stdout, "                                                                  --faultyacl :  find directories with illegal ACLs");
  fprintf(stdout, "                                                                      --count :  just print global counters for files/dirs found\n");
  fprintf(stdout, "                                                                 --childcount :  print the number of children in each directory\n");
  fprintf(stdout, "                                                                      default :  find files and directories\n");
  fprintf(stdout, "       find [--nrep] [--nunlink] [--size] [--fileinfo] [--online] [--hosts] [--partition] [--fid] [--fs] [--checksum] [--ctime] [--mtime] <path>   :  find files and print out the requested meta data as key value pairs\n");
  fprintf(stdout, "                                                               path=file:...  :  do a find in the local file system (options ignored) - 'file:' is the current working directory \n");
  fprintf(stdout, "                                                               path=root:...  :  do a find on a plain XRootD server (options ignored) - does not work on native XRootD clusters\n");
  fprintf(stdout, "                                                               path=as3:...   :  do a find on an S3 bucket\n");
  fprintf(stdout, "                                                               path=...       :  all other paths are considered to be EOS paths!\n");
  return (0);
}
