// ----------------------------------------------------------------------
// @file: NewfindHelper.cc
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

#include "console/commands/helpers/NewfindHelper.hh"
#include "common/StringConversion.hh"
#include "common/StringTokenizer.hh"
#include "console/ConsoleMain.hh"
#include <XrdOuc/XrdOucEnv.hh>
#include <XrdPosix/XrdPosixXrootd.hh>
#include <cerrno>
#include <cstring>
#include <regex.h>
#include <spawn.h>
#include <sstream>
#include <sys/wait.h>
#include <unistd.h>

extern char** environ;

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
    } else if (s1 == "--skip-version-dirs") {
      find->set_skipversiondirs(true);
    } else if (s1 == "--faultyacl") {
      find->set_faultyacl(true);
    } else if (s1 == "--count") {
      find->set_count(true);
    } else if (s1 == "--cache") {
      find->set_cache(true);
    } else if (s1 == "--du") {
      find->set_du(true);
    } else if (s1 == "--du-si") {
      find->set_dusi(true);
    } else if (s1 == "--du-h") {
      find->set_dureadable(true);
    } else if (s1 == "--hosts") {
      find->set_hosts(true);
    } else if (s1 == "--partition") {
      find->set_partition(true);
    } else if (s1 == "--childcount") {
      find->set_childcount(true);
    } else if (s1 == "--treecount") {
      find->set_treecount(true);
    } else if (s1 == "--format") {
      if (!subtokenizer.NextToken(token)) {
        return false;
      }

      find->set_format(token);
    } else if (s1 == "--xurl") {
      find->set_xurl(true);
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

      if (token.length() != 3 ||
          token.find_first_not_of("01234567") != std::string::npos) {
        return false;
      }

      find->set_permission(token);
    } else if (s1 == "-nflag") {
      find->set_searchnotpermission(true);

      if (!subtokenizer.NextToken(token)) {
        return false;
      }

      if (token.length() != 3 ||
          token.find_first_not_of("01234567") != std::string::npos) {
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
      // @todo(esindril) drop the single dash option at some point
    } else if ((s1 == "--ctime") || (s1 == "-ctime") ||
               (s1 == "--mtime") || (s1 == "-mtime")) {
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

        if ((s1 == "--ctime") || (s1 == "-ctime")) {
          find->set_ctime(true);
        } else if ((s1 == "--mtime") || (s1 == "-mtime")) {
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

  // --fileinfo reports on the entries, --purge, --layoutstripes and -b act on
  // them. Each of those only handles one kind of entry - directories for
  // --purge, files for the other two - so the other kind would still be
  // reported on and the two outputs would interleave, and -b prints its
  // summary once the traversal is over
  if (find->fileinfo() &&
      (!find->purge().empty() || find->dolayoutstripes() || find->balance())) {
    std::cerr << "error: --fileinfo cannot be combined with --purge, "
              << "--layoutstripes or -b" << std::endl;
    return false;
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
    return EINVAL;
  }

  const char* v = nullptr;

  if (!(v = eos::common::StringConversion::ParseUrl(path.c_str(), protocol,
            hostport))) {
    return EINVAL;
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
  XrdOucString hostport;
  XrdOucString protocol;
  XrdOucString sPath = path.c_str();
  XrdOucString sOpaque;
  int qpos = 0;

  if ((qpos = sPath.find("?")) != STR_NPOS) {
    sOpaque.assign(sPath, qpos + 1);
    sPath.erase(qpos);
  }

  const char* parsed =
      eos::common::StringConversion::ParseUrl(sPath.c_str(), protocol, hostport);
  if (!parsed || protocol != "as3") {
    std::cerr << "error: invalid S3 URI" << std::endl;
    return EINVAL;
  }
  const std::string filePath = parsed;
  if (hostport.length()) {
    setenv("S3_HOSTNAME", hostport.c_str(), 1);
  }
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
    return EINVAL;
  }

  const auto separator = filePath.find('/');
  const std::string bucket = filePath.substr(0, separator);
  std::string match =
      separator == std::string::npos ? "" : filePath.substr(separator + 1);
  if (bucket.empty() || bucket.find('*') != std::string::npos) {
    std::cerr << "error: no bucket specified or wildcard in bucket name!" <<
              std::endl;
    return EINVAL;
  }

  struct MatchExpression {
    regex_t value{};
    bool initialized = false;
    ~MatchExpression()
    {
      if (initialized) {
        regfree(&value);
      }
    }
  } expression;

  if (!match.empty()) {
    // Preserve the original egrep filter and its prefix/suffix wildcard shorthand.
    if (match.back() == '*') {
      match.pop_back();
      match.insert(0, "^");
    }
    if (match.front() == '*') {
      match.erase(0, 1);
      match += '$';
    }
    if (regcomp(&expression.value, match.c_str(), REG_EXTENDED | REG_NOSUB) != 0) {
      std::cerr << "error: invalid S3 path filter expression" << std::endl;
      return EINVAL;
    }
    expression.initialized = true;
  }

  int output[2];
  if (pipe(output) != 0) {
    return errno;
  }
  posix_spawn_file_actions_t actions;
  int setup = posix_spawn_file_actions_init(&actions);
  if (setup) {
    close(output[0]);
    close(output[1]);
    return setup;
  }
  setup = posix_spawn_file_actions_addclose(&actions, output[0]);
  if (!setup) {
    setup = posix_spawn_file_actions_adddup2(&actions, output[1], STDOUT_FILENO);
  }
  if (!setup && output[1] != STDOUT_FILENO) {
    setup = posix_spawn_file_actions_addclose(&actions, output[1]);
  }
  char* arguments[] = {const_cast<char*>("s3"), const_cast<char*>("list"),
                       const_cast<char*>(bucket.c_str()), nullptr};
  pid_t child = 0;
  const int spawned =
      setup ? setup : posix_spawnp(&child, "s3", &actions, nullptr, arguments, environ);
  posix_spawn_file_actions_destroy(&actions);
  close(output[1]);
  if (spawned) {
    close(output[0]);
    std::cerr << "error: cannot execute s3: " << strerror(spawned) << std::endl;
    return spawned;
  }

  const auto printEntry = [&](const std::string& line) {
    std::istringstream fields(line);
    std::string key;
    fields >> key;
    if (key.find("Bucket") == std::string::npos &&
        key.find("----------") == std::string::npos &&
        key.find("Key") == std::string::npos &&
        (!expression.initialized ||
         regexec(&expression.value, key.c_str(), 0, nullptr, 0) == 0)) {
      std::cout << "as3:" << bucket << '/' << key << std::endl;
    }
  };
  std::string pending;
  char buffer[4096];
  int readError = 0;
  while (true) {
    const ssize_t count = read(output[0], buffer, sizeof(buffer));
    if (count == 0) {
      break;
    }
    if (count < 0) {
      if (errno == EINTR) {
        continue;
      }
      readError = errno;
      break;
    }
    pending.append(buffer, static_cast<size_t>(count));
    size_t begin = 0, end = 0;
    while ((end = pending.find('\n', begin)) != std::string::npos) {
      printEntry(pending.substr(begin, end - begin));
      begin = end + 1;
    }
    pending.erase(0, begin);
  }
  close(output[0]);
  if (!readError && !pending.empty()) {
    printEntry(pending);
  }
  int status = 0;
  while (waitpid(child, &status, 0) < 0) {
    if (errno != EINTR) {
      return errno;
    }
  }
  if (readError) {
    return readError;
  }
  const int result = WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status);
  if (result) {
    std::cerr << "error: s3 list failed with status " << result << std::endl;
  }
  return result;
}
