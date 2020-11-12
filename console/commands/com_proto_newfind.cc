// ----------------------------------------------------------------------
// @file: com_proto_newfind.cc
// @author: Fabio Luchetti - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2020 CERN/Switzerland                                  *
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
#include "console/commands/ICmdHelper.hh"
#include "common/StringTokenizer.hh"
#include "common/StringConversion.hh"
#include "XrdPosix/XrdPosixXrootd.hh"
#include "XrdOuc/XrdOucEnv.hh"
/*----------------------------------------------------------------------------*/


extern int com_newfind(char*);

void com_newfind_help();

//------------------------------------------------------------------------------
//! Class NewfindHelper
//------------------------------------------------------------------------------
class NewfindHelper : public ICmdHelper
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param opts global options
  //----------------------------------------------------------------------------
  NewfindHelper(const GlobalOptions& opts):
      ICmdHelper(opts)
  {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~NewfindHelper() override = default;

  //----------------------------------------------------------------------------
  //! Parse command line input
  //!
  //! @param arg input
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool ParseCommand(const char* arg) override;

  int FindXroot(std::string path);
  int FindAs3(std::string path);

};


//------------------------------------------------------------------------------
// Parse command line input
//------------------------------------------------------------------------------
bool
NewfindHelper::ParseCommand(const char* arg)
{
  auto* find = mReq.mutable_find();
  XrdOucString s1;
  std::string token;
  eos::common::StringTokenizer subtokenizer(arg);
  subtokenizer.GetLine();

  while ((s1 = subtokenizer.GetToken()).length() > 0 && (s1.beginswith("-"))) {
    if (s1 == "-s") {
      find->set_silent(true);
    } else if (s1 == "-d") {
      find->set_directories(true);
    } else if (s1 == "-f") {
      find->set_files(true);
    } else if (s1 == "-0") {
      find->set_files(true);
      find->set_zerosizefiles(true);
    } else if (s1 == "--size") {
      find->set_size(true);
    } else if (s1 == "--fs") {
      find->set_fs(true);
    } else if (s1 == "--checksum") {
      find->set_checksum(true);
    } else if (s1 == "--ctime") {
      find->set_ctime(true);
    } else if (s1 == "--mtime") {
      find->set_mtime(true);
    } else if (s1 == "--fid") {
      find->set_fid(true);
    } else if (s1 == "--nrep") {
      find->set_nrep(true);
    } else if (s1 == "--online") {
      find->set_online(true);
    } else if (s1 == "--fileinfo") {
      find->set_fileinfo(true);
    } else if (s1 == "--nunlink") {
      find->set_nunlink(true);
    } else if (s1 == "--uid") {
      find->set_printuid(true);
    } else if (s1 == "--gid") {
      find->set_printgid(true);
    } else if (s1 == "--stripediff") {
      find->set_stripediff(true);
    } else if (s1 == "--faultyacl") {
      find->set_faultyacl(true);
    } else if (s1 == "--count") {
      find->set_count(true);
    } else if (s1 == "--hosts") {
      find->set_hosts(true);
    } else if (s1 == "--partition") {
      find->set_partition(true);
    } else if (s1 == "--childcount") {
      find->set_childcount(true);
    } else if (s1 == "--xurl") {
      find->set_xurl(true);
//    } else if (s1 == "-1") {
//      find->set_onehourold(true);
    } else if (s1 == "-b") {
      find->set_balance(true);
    } else if (s1 == "-g") {
      find->set_mixedgroups(true);
    } else if (s1 == "-uid") {
      find->set_searchuid(true);
      if (!subtokenizer.NextToken(token)) {
        return false;
      }
      try {
        find->set_uid(std::stoul(token));
      } catch (std::invalid_argument& error) {
        return false;
      }
    } else if (s1 == "-nuid") {
      find->set_searchnotuid(true);
      if (!subtokenizer.NextToken(token)) {
        return false;
      }
      try {
        find->set_notuid(std::stoul(token));
      } catch (std::invalid_argument& error) {
        return false;
      }
    } else if (s1 == "-gid") {
      find->set_searchgid(true);
      if (!subtokenizer.NextToken(token)) {
        return false;
      }
      try {
        find->set_gid(std::stoul(token));
      } catch (std::invalid_argument& error) {
        return false;
      }
    } else if (s1 == "-ngid") {
      find->set_searchnotgid(true);
      if (!subtokenizer.NextToken(token)) {
        return false;
      }
      try {
        find->set_notgid(std::stoul(token));
      } catch (std::invalid_argument& error) {
        return false;
      }
    } else if (s1 == "-flag") {
      find->set_searchpermission(true);
      if (!subtokenizer.NextToken(token)) {
        return false;
      }
      if (token.length() != 3 || token.find_first_not_of("01234567") != std::string::npos) {
        return false;
      }
      find->set_permission(token);
    } else if (s1 == "-nflag") {
      find->set_searchnotpermission(true);
      if (!subtokenizer.NextToken(token)) {
        return false;
      }
      if (token.length() != 3 || token.find_first_not_of("01234567") != std::string::npos) {
        return false;
      }
      find->set_notpermission(token);
    } else if (s1 == "-x") {
      if (!subtokenizer.NextToken(token)) {
        return false;
      }
      if (token.length() > 0 && token.find('=') != std::string::npos &&
          token.find('&') == std::string::npos) {
        auto key = token;
        auto value = token;
        key.erase(token.find('='));
        value.erase(0, token.find('=') + 1);
        find->set_attributekey(std::move(key));
        find->set_attributevalue(std::move(value));
      } else {
        return false;
      }
    } else if (s1 == "--maxdepth") {
      if (!subtokenizer.NextToken(token)) {
        return false;
      }
      if (token.length() > 0) {
        try {
          find->set_maxdepth(std::stoul(token));
        } catch (std::invalid_argument& error) {
          return false;
        }
      } else {
        return false;
      }
    } else if (s1 == "--purge") {
      std::string versions = subtokenizer.GetToken();

      if (versions.length() > 0) {
        try {
          std::stoul(versions);
        } catch (std::logic_error& err) {
          if (versions != "atomic") {
            return false;
          }
        }

        find->set_purge(versions);
      } else {
        return false;
      }
    // @todo drop "-name" sometime later
    } else if (s1 == "--name" || s1 == "-name") {
      std::string filematch = subtokenizer.GetToken();

      if (filematch.length() > 0) {
        find->set_name(std::move(filematch));
      } else {
        return false;
      }
    } else if (s1 == "--layoutstripes") {
      std::string stripes = subtokenizer.GetToken();

      if (stripes.length() > 0) {
        find->set_dolayoutstripes(true);
        find->set_layoutstripes(std::stoul(stripes));
      } else {
        return false;
      }
    } else if (s1 == "-p") {
      std::string printkey = subtokenizer.GetToken();

      if (printkey.length() > 0) {
        find->set_printkey(std::move(printkey));
      } else {
        return false;
      }
    } else if ((s1 == "-ctime") || (s1 == "-mtime")) {
      XrdOucString period = "";
      period = subtokenizer.GetToken();

      if (period.length() > 0) {
        bool do_olderthan = false;
        bool do_youngerthan = false;

        if (period.beginswith("+")) {
          do_olderthan = true;
        } else if (period.beginswith("-")) {
          do_youngerthan = true;
        }

        if ((!do_olderthan) && (!do_youngerthan)) {
          return false;
        }

        period.erase(0, 1);
        time_t now = time(NULL);
        now -= (86400 * strtoul(period.c_str(), 0, 10));
        char snow[1024];
        snprintf(snow, sizeof(snow) - 1, "%lu", now);

        if (s1 == "-ctime") {
          find->set_ctime(true);
        } else if (s1 == "-mtime") {
          find->set_mtime(true);
        }

        if (do_olderthan) {
          try {
            find->set_olderthan(std::stoul(snow));
          } catch (std::invalid_argument& error) {
            return false;
          }
        }

        if (do_youngerthan) {
          try {
            find->set_youngerthan(std::stoul(snow));
          } catch (std::invalid_argument& error) {
            return false;
          }
        }
      } else {
        return false;
      }
    } else {
      return false;
    }
  }

  if (s1.length() > 0) {
    auto path = s1;

    if (!path.endswith("/") && !path.endswith(":")) {
      // if the user gave file: as a search path we shouldn't add '/'=root
      path += "/";
    }

    path = abspath(path.c_str());
    find->set_path(path.c_str());
  } else {
    return false;
  }

  return true;
}

int
NewfindHelper::FindXroot(std::string path)
{
  XrdPosixXrootd Xroot;

  if (path.rfind('/') != path.length() - 1) {
    if (path.rfind(':') != path.length() - 1) {
      // if the user gave file: as a search path we shouldn't add '/'=root
      path += "/";
    }
  }

  bool XRootD = path.find("root:") == 0;
  std::vector< std::vector<std::string> > found_dirs;
  std::map<std::string, std::set<std::string> > found;
  XrdOucString protocol;
  XrdOucString hostport;
  XrdOucString sPath;

  if (path == "/") {
    std::cerr << "error: I won't do a find on '/'" << std::endl;
    global_retc = EINVAL;
    return (0);
  }

  const char* v = nullptr;

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

  found_dirs.resize(1);
  found_dirs[0].resize(1);
  found_dirs[0][0] = Path.c_str();
  int deepness = 0;

  do {
    struct stat buf;
    found_dirs.resize(deepness + 2);

    // loop over all directories in that deepness
    for (unsigned int i = 0; i < found_dirs[deepness].size(); i++) {
      Path = found_dirs[deepness][i].c_str();
      XrdOucString url = "";
      eos::common::StringConversion::CreateUrl(protocol.c_str(), hostport.c_str(),
          Path.c_str(), url);
      int rstat = 0;
      rstat = (XRootD) ? XrdPosixXrootd::Stat(url.c_str(), &buf) : stat(url.c_str(),
              &buf);

      if (rstat == 0) {
        //
        if (S_ISDIR(buf.st_mode)) {
          // add all children
          DIR* dir = (XRootD) ? XrdPosixXrootd::Opendir(url.c_str()) : opendir(
                       url.c_str());

          if (dir != nullptr) {
            struct dirent* entry;

            while ((entry = (XRootD) ? XrdPosixXrootd::Readdir(dir) : readdir(dir))) {
              XrdOucString curl = "";
              XrdOucString cpath = Path.c_str();
              cpath += entry->d_name;

              if ((!strcmp(entry->d_name, ".")) || (!strcmp(entry->d_name, ".."))) {
                continue;  // skip . and .. directories
              }

              eos::common::StringConversion::CreateUrl(protocol.c_str(), hostport.c_str(),
                  cpath.c_str(), curl);

              if (!((XRootD) ? XrdPosixXrootd::Stat(curl.c_str(), &buf) : stat(curl.c_str(),
                    &buf))) {
                if (S_ISDIR(buf.st_mode)) {
                  curl += "/";
                  cpath += "/";
                  found_dirs[deepness + 1].push_back(cpath.c_str());
                  (void) found[curl.c_str()].size();
                } else {
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
  } while (found_dirs[deepness].size());

  for (const auto& it : found) {
    std::cout << it.first << std::endl;

    for (const auto& sit : it.second) {
      std::cout << it.first << sit << std::endl;
    }
  }

  return 0;
}

int
NewfindHelper::FindAs3(std::string path)
{
  // ----------------------------------------------------------------
  // this is nightmare code because of a missing proper CLI for S3
  // ----------------------------------------------------------------
  XrdOucString hostport;
  XrdOucString protocol;
  int rc = system("which s3 >&/dev/null");

  if (WEXITSTATUS(rc)) {
    std::cerr <<
              "error: you miss the <s3> executable provided by libs3 in your PATH" <<
              std::endl;
    exit(-1);
  }

  if (path.rfind('/') == path.length()) {
    path.erase(path.length() - 1);
  }

  XrdOucString sPath = path.c_str();
  XrdOucString sOpaque;
  int qpos = 0;

  if ((qpos = sPath.find("?")) != STR_NPOS) {
    sOpaque.assign(sPath, qpos + 1);
    sPath.erase(qpos);
  }

  XrdOucString fPath = eos::common::StringConversion::ParseUrl(sPath.c_str(),
                       protocol, hostport);
  XrdOucEnv env(sOpaque.c_str());

  if (env.Get("s3.key")) {
    setenv("S3_SECRET_ACCESS_KEY", env.Get("s3.key"), 1);
  }

  if (env.Get("s3.id")) {
    setenv("S3_ACCESS_KEY_ID", env.Get("s3.id"), 1);
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
    std::cerr <<
              "error: you have to set the S3 environment variables S3_ACCESS_KEY_ID | S3_ACCESS_ID, S3_HOSTNAME (or use a URI), S3_SECRET_ACCESS_KEY | S3_ACCESS_KEY"
              << std::endl;
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

  if (bpos != STR_NPOS) {
    bucket.assign(fPath, 0, bpos - 1);
  } else {
    bucket = fPath.c_str();
  }

  XrdOucString match;

  if (bpos != STR_NPOS) {
    match.assign(fPath, bpos + 1);
  } else {
    match = "";
  }

  if ((!bucket.length()) || (bucket.find("*") != STR_NPOS)) {
    std::cerr << "error: no bucket specified or wildcard in bucket name!" <<
              std::endl;
    global_retc = EINVAL;
    return (0);
  }

  cmd += bucket.c_str();
  cmd += " | awk '{print \\$1}' ";

  if (match.length()) {
    if (match.endswith("*")) {
      match.erase(match.length() - 1);
      match.insert("^", 0);
    }

    if (match.beginswith("*")) {
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

  if (WEXITSTATUS(rc)) {
    std::cerr << "error: failed to run " << cmd << std::endl;
    return rc;
  }

  return 0;
}

int
com_protonewfind(char* arg)
{
  if (wants_help(arg)) {
    com_newfind_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  NewfindHelper find(gGlobalOpts);
  // Handle differently if it's an xroot, file or as3 path
  std::string argStr(arg);
  auto xrootAt = argStr.rfind("root://");
  auto fileAt = argStr.rfind("file:");
  auto as3At = argStr.rfind("as3:");

  if (xrootAt != std::string::npos) {
    auto path = argStr.substr(xrootAt);
    // remove " from the path
    path.erase(std::remove(path.begin(), path.end(), '"'), path.end());
    return find.FindXroot(path);
  } else if (fileAt != std::string::npos) {
    auto path = argStr.substr(fileAt);
    // remove " from the path
    path.erase(std::remove(path.begin(), path.end(), '"'), path.end());
    return find.FindXroot(path);
  } else if (as3At != std::string::npos) {
    auto path = argStr.substr(as3At);
    // remove " from the path
    path.erase(std::remove(path.begin(), path.end(), '"'), path.end());
    return find.FindAs3(path);
  }

  if (!find.ParseCommand(arg)) {
    com_newfind_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  global_retc = find.Execute();
//  if (global_retc) {
//    std::cerr << find.GetError();
//  }
  return global_retc;

}

void com_newfind_help()
{
  std::ostringstream oss;
  oss
      << " usage\n"
      << "newfind [OPTIONS] <path> : find files and directories\n"
      << "OPTIONS can be filters, actions to perform, or output integrations/modifiers for the found items\n"
      << "Filters: [--maxdepth <n>] [--name <pattern>] [-d] [-f] [-0] [-g] [-uid <n>] [-nuid <n>] [-gid <n>] [-ngid <n>] [-flag <n>] [-nflag <n>] [-ctime +<n>|-<n>] [-x <key>=<val>]\n"
//      << "\t                   -1 : \n"
      << "\t       --maxdepth <n> : descend only <n> levels (note that level refers to absolute paths, '/eos' being the 1st level and 0 resetting the limit)\n"
      << "\t     --name <pattern> : find by name or wildcard match\n"
      << "\t                -f,-d : find only files(-f) or directories (-d) in <path>\n"
      << "\t       -x <key>=<val> : find entries with <key>=<val>\n"
      << "\t   -uid <n>,-nuid <n> : find entries owned / not owned by a given user id number\n"
      << "\t   -gid <n>,-ngid <n> : find entries owned / not owned by a given group id number\n"
      << "\t -flag <n>,-nflag <n> : find entries with / without specified UNIX access flag, e.g. 755\n"
      << "\t    -ctime <+n>, <-n> : find files older (+n) or younger (-n) than <n> days\n"
      << "\t          --faultyacl : find files and directories with illegal ACLs\n"
      << "\t                   -0 : find 0-size files only\n"
      << "\t                   -g : find files with mixed scheduling groups\n"
      << "\t         --stripediff : find files that do not have the nominal number of stripes(replicas)\n"
      << "Actions: [-b] [--layoutstripes <n>] [--purge <n> ] [--fileinfo]"
      << "\t                   -b : query the server balance of the files found\n"
      << "\t  --layoutstripes <n> : apply new layout with <n> stripes to the files found\n"
      << "\t --purge <n> | atomic : remove versioned files keeping <n> versions (use --purge 0 to remove all old versions)\n"
      << "\t                        To apply the settings of the extended attribute definition use --purge -1\n"
      << "\t                        To remove all atomic upload left-overs older than a day use --purge atomic\n"
      << "\t         [--fileinfo] : invoke `eos fileinfo` on the entry\n"
      << "\t              --count : just print global counters for files/dirs found\n"
      << "\t         --childcount : print the number of children in each directory\n"
      << "Output Mod: [--xurl] [-p <key>] [--nrep] [--nunlink] [--size] [--online] [--hosts] [--partition] [--fid] [--fs] [--checksum] [--ctime] [--mtime] [--uid] [--gid]\n"
//      << "                   -s :  run in silent mode"
      << "\t                      : print out the requested meta data as key value pairs\n"
      << " <path> :\n"
      << "\t path=file:...  :  do a find in the local file system (options ignored) - 'file:' is the current working directory\n"
      << "\t path=root:...  :  do a find on a plain XRootD server (options ignored) - does not work on native XRootD clusters\n"
      << "\t path=as3:...   :  do a find on an S3 bucket\n"
      << "\t path=...       :  all other paths are considered to be EOS paths!\n";
  std::cerr << oss.str() << std::endl;
}
