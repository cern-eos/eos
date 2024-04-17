// ----------------------------------------------------------------------
// File: com_rclone.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright(C) 2023 CERN/Switzerland                                  *
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
#include "console/commands/helpers/NewfindHelper.hh"
#include "common/StringTokenizer.hh"
#include "common/Timing.hh"
#include "common/Path.hh"
#include "common/LayoutId.hh"
/*----------------------------------------------------------------------------*/

#include <XrdCl/XrdClFileSystem.hh>
#include <XrdCl/XrdClCopyProcess.hh>
#include <XrdCl/XrdClPropertyList.hh>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <iostream>
#include <filesystem>
#include <chrono>
#include <optional>

extern XrdOucString serveruri;

struct fs_entry {
  struct timespec mtime;
  size_t size;
  std::string type;
  std::string target;

  bool newer(struct timespec& cmptime)
  {
    if (mtime.tv_sec < cmptime.tv_sec) {
      return true;
    } else if (mtime.tv_sec > cmptime.tv_sec) {
      return false;
    } else if (mtime.tv_nsec < cmptime.tv_nsec) {
      return true;
    } else {
      return false;
    }
  }
};

struct fs_result {
  std::map<std::string, fs_entry> directories;
  std::map<std::string, fs_entry> files;
  std::map<std::string, fs_entry> links;
};

bool dryrun = false;
bool noreplace = false;
bool nodelete = false;
bool verbose = false;
bool is_silent = false;
bool filter_versions = true;
bool filter_atomic = true;
bool filter_hidden = true;

fs_result fs_find(const char* path)
{
  fs_result result;
  std::stringstream s;
  eos::common::Path cPath(path);
  namespace fs = std::filesystem;
  fs::path path_to_traverse = path;
  struct stat buf;

  try {
    for (const auto& entry : fs::recursive_directory_iterator(path_to_traverse,
         std::filesystem::directory_options::skip_permission_denied)) {
      std::string p = entry.path().string();
      // filter functions
      eos::common::Path iPath(p.c_str());

      if (filter_versions) {
        if (iPath.isVersionPath()) {
          continue;
        }
      }

      if (filter_atomic) {
        if (iPath.isAtomicFile()) {
          continue;
        }
      }

      if (filter_hidden && iPath.GetFullPath().find("/.") != STR_NPOS) {
        if (!iPath.isVersionPath() && !iPath.isAtomicFile()) {
          continue;
        }
      }

      std::string t = p;

      if (!::lstat(p.c_str(), &buf)) {
        p.erase(0, cPath.GetFullPath().length());

        switch ((buf.st_mode & S_IFMT)) {
        case S_IFDIR :
          p += "/";
          result.directories[p].mtime = buf.st_mtim;
          result.directories[p].size  = buf.st_size;
          //  s << "path=\"" << p << "/\" mtime=" << eos::common::Timing::TimespecToString(buf.st_mtim) << " size=" << buf.st_size << std::endl;
          break;

        case S_IFREG :
          result.files[p].mtime = buf.st_mtim;
          result.files[p].size  = buf.st_size;
          //s << "path=\"" << p << "\" mtime=" << eos::common::Timing::TimespecToString(buf.st_mtim) << " size=" << buf.st_size << std::endl;
          break;

        case S_IFLNK :
          result.links[p].size = 0;
          result.links[p].mtime = buf.st_mtim;
          char link[4096];
          ssize_t target = readlink(t.c_str(), link, sizeof(link));

          if (target >= 0) {
            result.links[p].target = std::string(link, target);
          }

          break;
        }
      }
    }
  } catch (std::filesystem::filesystem_error const& ex) {
    std::cerr
        << "error:  " << ex.what() << '\n'
        << "#      path  : " << ex.path1() << '\n'
        << "#      errc  :    " << ex.code().value() << '\n'
        << "#      msg   :  " << ex.code().message() << '\n'
        << "#      class : " << ex.code().category().name() << '\n';
    exit(-1);
  }

  //  std::cout << s.str();
  return result;
}

fs_result eos_find(const char* path)
{
  fs_result result;
  eos::common::Path cPath(path);
  NewfindHelper find(gGlobalOpts);
  std::string args = "--format type,mtime,size,link ";
  args += path;

  if (!find.ParseCommand(args.c_str())) {
    std::cerr << "error: illegal subcommand '" << args << "'" << std::endl;
  }

  find.Silent();
  int rc = find.Execute();

  if (!rc) {
    std::string findresult = find.GetResult();
    std::vector<std::string> lines;
    eos::common::StringConversion::Tokenize(findresult, lines, "\n");

    for (auto l : lines) {
      std::vector<std::string> kvs;
      eos::common::StringConversion::Tokenize(l, kvs, " ");
      struct timespec ts {
        0, 0
      };
      size_t size{0};
      std::string path;
      std::string type;

      for (auto k : kvs) {
        std::string tag, value;
        eos::common::StringConversion::SplitKeyValue(k, tag, value, "=");

        if (tag == "mtime") {
          eos::common::Timing::Timespec_from_TimespecStr(value, ts);

          if (type == "directory") {
            result.directories[path].mtime = ts;
          } else if (type == "file") {
            result.files[path].mtime = ts;
          } else if (type == "symlink") {
            result.links[path].mtime = ts;
          }
        }

        if (tag == "size") {
          size = std::stoull(value.c_str(), 0, 10);

          if (type == "directory") {
            result.directories[path].size = size;
          } else if (type == "file") {
            result.files[path].size = size;
          } else if (type == "symlink") {
            result.links[path].size = 0;
          }
        }

        if (tag == "path") {
          // remove quotes
          value.erase(0, 1);
          value.erase(value.length() - 1);
          value.erase(0, cPath.GetFullPath().length());
          path = value;
        }

        if (tag == "type") {
          type = value;
        }

        if (tag == "target" && type == "symlink") {
          value.erase(0, 1);
          value.erase(value.length() - 1);
          result.links[path].target = value;
        }

        // filter functions
        eos::common::Path iPath(path.c_str());

        if (filter_versions) {
          if (iPath.isVersionPath()) {
            break;
          }
        }

        if (filter_atomic) {
          if (iPath.isAtomicFile()) {
            break;
          }
        }

        if (filter_hidden && iPath.GetFullPath().find("/.") != STR_NPOS) {
          if (!iPath.isVersionPath() && !iPath.isAtomicFile()) {
            break;
          }
        }
      }
    }
  } else {
    std::cerr << "error: " << find.GetError() << std::endl;
    exit(rc);
  }

  return result;
}

int createDir(const std::string& i, eos::common::Path& prefix)
{
  if (!prefix.GetFullPath().beginswith("/eos/")) {
    int rc = 0;
    std::string mkpath = std::string(prefix.GetFullPath().c_str()) +
                         std::string("/") + i;

    if (!dryrun) {
      rc = ::mkdir(mkpath.c_str(), S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH);
    }

    std::cerr << "[ mkdir                 ] : path:" << "[mkdir] path: " <<
              mkpath.c_str() << " retc: " << rc << std::endl;
    return rc;
  } else {
    XrdCl::URL url(serveruri.c_str());
    url.SetPath(std::string(prefix.GetFullPath().c_str()) + std::string("/") + i);

    if (!url.IsValid()) {
      std::cerr << "error: invalid url " << i.c_str() << std::endl;
      return 0;
    }

    XrdCl::FileSystem fs(url);
    mode_t mode = S_IRUSR | S_IWUSR | S_IXUSR | S_IRGRP;
    XrdCl::Access::Mode mode_xrdcl = eos::common::LayoutId::MapModeSfs2XrdCl(mode);
    XrdCl::XRootDStatus status = fs.MkDir(url.GetPath(),
                                          XrdCl::MkDirFlags::MakePath,
                                          mode_xrdcl);
    std::cerr << "[ mkdir                 ] : url:" << url.GetURL() << " : " <<
              status.IsOK() << std::endl;
    return (!status.IsOK());
  }
}

int removeDir(const std::string& i, eos::common::Path& prefix)
{
  if (!prefix.GetFullPath().beginswith("/eos/")) {
    int rc = 0;
    std::string rmpath = std::string(prefix.GetFullPath().c_str()) +
                         std::string("/") + i;

    if (!dryrun) {
      rc = ::rmdir(rmpath.c_str());
    }

    std::cerr << "[ rmdir                 ] : path:" << rmpath.c_str() << " retc: "
              << rc << std::endl;
    return rc;
  } else {
    XrdCl::URL url(serveruri.c_str());
    url.SetPath(std::string(prefix.GetFullPath().c_str()) + std::string("/") + i);

    if (!url.IsValid()) {
      std::cerr << "error: invalid url " << i.c_str() << std::endl;
      return 0;
    }

    XrdCl::FileSystem fs(url);
    XrdCl::XRootDStatus status = fs.RmDir(url.GetPath());
    std::cerr << "[ rmdir                 ] : url:" << url.GetURL() << " : " <<
              status.IsOK() << std::endl;
    return (!status.IsOK());
  }
}

int removeFile(const std::string& i, eos::common::Path& prefix)
{
  if (!prefix.GetFullPath().beginswith("/eos/")) {
    int rc = 0;
    std::string rmpath = std::string(prefix.GetFullPath().c_str()) +
                         std::string("/") + i;

    if (!dryrun) {
      rc = ::unlink(rmpath.c_str());
    }

    std::cerr << "[ unlink                ] : path:" << rmpath.c_str() << " retc: "
              << rc << std::endl;
    return rc;
  } else {
    XrdCl::URL url(serveruri.c_str());
    url.SetPath(std::string(prefix.GetFullPath().c_str()) + std::string("/") + i);

    if (!url.IsValid()) {
      std::cerr << "error: invalid url " << i.c_str() << std::endl;
      return 0;
    }

    XrdCl::FileSystem fs(url);
    XrdCl::XRootDStatus status = fs.Rm(url.GetPath());
    std::cerr << "[ unlink                ] : url:" << url.GetURL() << " : " <<
              status.IsOK() << std::endl;
    return (!status.IsOK());
  }
}

int createLink(const std::string& i, eos::common::Path& prefix,
               const std::string& target, eos::common::Path& targetprefix,
               struct timespec& mtime)
{
  std::string targetpath = target;

  if (targetpath.find(prefix.GetFullPath().c_str()) == 0) {
    // might need to rewrite the link target with a new prefix!
    targetpath.erase(0, prefix.GetFullPath().length());
    targetpath.insert(0, targetprefix.GetFullPath().c_str());
  }

  if (!prefix.GetFullPath().beginswith("/eos/")) {
    int rc = 0;
    std::string linkpath = std::string(prefix.GetFullPath().c_str()) +
                           std::string("/") + i;

    if (!is_silent && verbose) {
      std::cout << "[ link  ] linking " << linkpath.c_str() << " => " <<
                target.c_str() << " " << mtime.tv_sec << "." << mtime.tv_nsec << std::endl;
    }

    if (!dryrun) {
      rc = ::symlink(target.c_str(), linkpath.c_str());

      if (rc) {
        std::cerr << "error: symlink rc=" << rc << " errno=" << errno << std::endl;
      }
    }

    struct timespec times[2];

    times[0] = mtime;

    times[1] = mtime;

    if (!dryrun) {
      int rc2 = utimensat(0, linkpath.c_str(), times, AT_SYMLINK_NOFOLLOW);
      rc |= rc2;

      if (rc2) {
        std::cerr << "error: utimesat rc=" << rc << " errno=" << errno << std::endl;
      }
    }

    if (!is_silent && verbose) {
      std::cout << "[ symlink               ] : path:" << linkpath.c_str() <<
                " retc: " << rc << std::endl;
    }

    return rc;
  } else {
    XrdCl::URL url(serveruri.c_str());
    url.SetPath(std::string(prefix.GetFullPath().c_str()) + std::string("/") + i);

    if (!url.IsValid()) {
      std::cerr << "error: invalid url " << i.c_str() << std::endl;
      return 0;
    }

    int retc = 0;
    std::string request;
    {
      // create link
      XrdCl::Buffer arg;
      XrdCl::Buffer* response = nullptr;
      request = eos::common::StringConversion::curl_escaped(std::string(
                  prefix.GetFullPath().c_str()) + std::string("/") + i);
      request += "?";
      request += "mgm.pcmd=symlink&target=";
      request += eos::common::StringConversion::curl_escaped(targetpath);
      request += "&eos.encodepath=1";
      arg.FromString(request);
      XrdCl::FileSystem fs(url);
      XrdCl::XRootDStatus status = fs.Query(XrdCl::QueryCode::OpaqueFile, arg,
                                            response);

      if (response) {
        delete response;
      }

      retc = !status.IsOK();
    }
    {
      // fix mtime
      XrdCl::Buffer arg;
      XrdCl::Buffer* response = nullptr;
      request = eos::common::StringConversion::curl_escaped(std::string(
                  prefix.GetFullPath().c_str()) + std::string("/") + i);
      request += "?";
      request += "mgm.pcmd=utimes";
      request += "&tv1_sec=0";  //ignored
      request += "&tv1_nsec=0"; // ignored
      request += "&tv2_sec=";
      request += std::to_string(mtime.tv_sec);
      request += "&tv2_nsec=";
      std::stringstream oss;
      oss << std::setfill('0') << std::setw(9) << mtime.tv_nsec;
      request += oss.str();
      request += "&eos.encodepath=1";
      arg.FromString(request);
      XrdCl::FileSystem fs(url);
      XrdCl::XRootDStatus status = fs.Query(XrdCl::QueryCode::OpaqueFile, arg,
                                            response);

      if (response) {
        delete response;
      }

      retc |= !status.IsOK();
    }

    if (!is_silent && verbose) {
      std::cout << "[ symlink               ] : url:" << url.GetURL() << " : " << retc
                << std::endl;
    }

    return retc;
  }
}


int setDirMtime(const std::string& i, eos::common::Path& prefix,
                struct timespec mtime)
{
  std::string mtpath = std::string(prefix.GetFullPath().c_str()) +
                       std::string("/") + i;

  if (!prefix.GetFullPath().beginswith("/eos/")) {
    // apply local mtime;
    struct timespec times[2];
    times[0] = mtime;
    times[1] = mtime;
    int rc = 0;

    if (!dryrun) {
      rc = utimensat(0, mtpath.c_str(), times, AT_SYMLINK_NOFOLLOW);
    }

    if (!is_silent && verbose) {
      std::cout << "[ mtime                 ] : path:" << "[utime] path: " <<
                mtpath.c_str() << " retc: " << rc <<  " " << mtime.tv_sec << ":" <<
                mtime.tv_nsec << std::endl;
    }

    return rc;
  } else {
    XrdCl::URL url(serveruri.c_str());
    url.SetPath(std::string(prefix.GetFullPath().c_str()) + std::string("/") + i);

    if (!url.IsValid()) {
      std::cerr << "error: invalid url " << i.c_str() << std::endl;
      return 0;
    }

    std::string request;
    XrdCl::Buffer arg;
    XrdCl::Buffer* response = nullptr;
    request = eos::common::StringConversion::curl_escaped(std::string(
                prefix.GetFullPath().c_str()) + std::string("/") + i);
    request += "?";
    request += "mgm.pcmd=utimes";
    request += "&tv1_sec=0";  //ignored
    request += "&tv1_nsec=0"; // ignored
    request += "&tv2_sec=";
    request += std::to_string(mtime.tv_sec);
    request += "&tv2_nsec=";
    std::stringstream oss;
    oss << std::setfill('0') << std::setw(9) << mtime.tv_nsec;
    request += oss.str();
    request += "&eos.encodepath=1";
    arg.FromString(request);
    XrdCl::FileSystem fs(url);
    XrdCl::XRootDStatus status = fs.Query(XrdCl::QueryCode::OpaqueFile, arg,
                                          response);

    if (response) {
      delete response;
    }

    int rc = !status.IsOK();

    if (!is_silent && verbose) {
      std::cerr << "[ mtime                 ] : path:" << "[utime] path: " <<
                mtpath.c_str() << " retc: " << rc << std::endl;
    }

    return rc;
  }
}

XrdCl::CopyProcess copyProcess;
std::vector<XrdCl::PropertyList*> tprops;

XrdCl::PropertyList* copyFile(const std::string& i, eos::common::Path& src,
                              eos::common::Path& dst, struct timespec mtime)
{
  XrdCl::PropertyList props;
  XrdCl::PropertyList* result = new XrdCl::PropertyList();
  std::string srcurl = std::string(src.GetFullPath().c_str()) + i;
  std::string dsturl = std::string(dst.GetFullPath().c_str()) + i;

  if (srcurl.substr(0, 5) == "/eos/") {
    XrdCl::URL surl(serveruri.c_str());
    surl.SetPath(srcurl);
    srcurl = surl.GetURL();
  }

  if (dsturl.substr(0, 5) == "/eos/") {
    XrdCl::URL durl(serveruri.c_str());
    durl.SetPath(dsturl);
    XrdCl::URL::ParamsMap params;
    params["eos.mtime"] = eos::common::Timing::TimespecToString(mtime);
    durl.SetParams(params);
    dsturl = durl.GetURL();
  } else {
    XrdCl::URL durl(dsturl);
    XrdCl::URL::ParamsMap params;
    params["local.mtime"] = eos::common::Timing::TimespecToString(mtime);
    durl.SetParams(params);
    dsturl = durl.GetURL();
  }

  props.Set("source", srcurl);
  props.Set("target", dsturl);
  props.Set("force", true); // allows overwrite
  result->Set("source", srcurl);
  result->Set("target", dsturl);

  //  props.Set("parallel", 10);
  if (verbose) {
    std::cout << "[ copy file             ] : srcurl: " << srcurl << " dsturl: " <<
              dsturl << std::endl;
  }

  copyProcess.AddJob(props, result);
  return result;
}

void rclone_usage()
{
  fprintf(stderr,
          "usage: rclone copy src-dir dst-dir [--delete] [--noupdate] [--dryrun] [--atomic] [--versions] [--hidden] [-v|--verbose] [-s|--silent]\n");
  fprintf(stderr,
          "                                       : copy from source to destination [one-way sync]\n");
  fprintf(stderr,
          "       rclone sync dir1 dir2 [--delete] [--noupdate] [--dryrun] [--atomic] [--versions] [--hidden] [-v|--verbose] [-s|--silent]\n");
  fprintf(stderr,
          "                                       : bi-directional sync based on modification times\n");
  fprintf(stderr,
          "                              --delete : delete based on mtimes (currently unsupported)!\n");
  fprintf(stderr,
          "                            --noupdate : never update files, only create new ones!\n");
  fprintf(stderr,
          "                            --dryrun   : simulate the command and show all actions, but don't do it!\n");
  fprintf(stderr,
          "                            --atomic   : copy/sync also EOS atomic files\n");
  fprintf(stderr,
          "                            --versions : copy/sync also EOS atomic files\n");
  fprintf(stderr,
          "                            --hidden   : copy/sync also hidden files/directories\n");
  fprintf(stderr,
          "                         -v --verbose  : display all actions, not only a summary\n");
  fprintf(stderr,
          "                         -s --silent   : only show errors\n");
  exit(-1);
}


std::string parent(const std::string& path)
{
  std::filesystem::path p(path);
  return p.parent_path();
}


std::optional<bool> parent_newer(std::map<std::string, fs_entry>& a,
                                 std::map<std::string, fs_entry>& b, const std::string& path)
{
  // checks if the parent mtime of b is newer than parent mtime of a !
  std::string p_path = parent(path);

  if (!a.count(path) || !b.count(path) ||
      !a.count(p_path) || !b.count(p_path)) {
    return {};
  }

  if ((b[p_path].mtime.tv_sec == a[p_path].mtime.tv_sec) &&
      (b[p_path].mtime.tv_nsec == a[p_path].mtime.tv_nsec)) {
    return {};
  }

  if (b[p_path].newer(a[p_path].mtime)) {
    return true;
  } else {
    return false;
  }
}


int
com_rclone(char* arg1)
{
  if (interactive) {
    fprintf(stderr,
            "error: don't call <rclone> from an interactive shell - run 'eos -b rclone ...'!\n");
    global_retc = -1;
    return 0;
  }

// split subcommands
  XrdOucString mountpoint = "";
  eos::common::StringTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString cmd = subtokenizer.GetToken();
  std::set<std::string> target_create_dirs;
  std::set<std::string> target_delete_dirs;
  std::set<std::string> target_mtime_dirs;
  std::set<std::string> target_create_files;
  std::set<std::string> target_delete_files;
  std::set<std::string> target_updated_files;
  std::set<std::string> target_mismatch_files;
  std::set<std::string> target_create_links;
  std::set<std::string> target_delete_links;
  std::set<std::string> target_updated_links;
  std::set<std::string> target_mismatch_links;
  std::set<std::string> source_create_dirs;
  std::set<std::string> source_delete_dirs;
  std::set<std::string> source_mtime_dirs;
  std::set<std::string> source_create_files;
  std::set<std::string> source_delete_files;
  std::set<std::string> source_updated_files;
  std::set<std::string> source_mismatch_files;
  std::set<std::string> source_create_links;
  std::set<std::string> source_delete_links;
  std::set<std::string> source_updated_links;
  std::set<std::string> source_mismatch_links;
  std::set<std::string> cp_target_files;
  std::set<std::string> cp_source_files;
  uint64_t copySize = 0;
  uint64_t copyTransactions = 0;
  enum eActions {
    kTargetDirCreate, kSourceDirCreate,
    kTargetDirDelete, kSourceDirDelete,
    kTargetFileCreate, kSourceFileCreate,
    kTargetFileUpdate, kSourceFileUpdate,
    kTargetFileDelete, kSourceFileDelete,
    kTargetFileMismatch, kSourceFileMismatch,
    kTargetLinkCreate, kSourceLinkCreate,
    kTargetLinkUpdate, kSourceLinkUpdate,
    kTargetLinkDelete, kSourceLinkDelete,
    kTargetLinkMismatch, kSourceLinkMismatch,
    kTargetDirMtime, kSourceDirMtime
  };
  std::vector<eActions> actions;

  if (cmd == "copy") {
    actions.push_back(kTargetDirCreate);
    actions.push_back(kTargetFileCreate);

    if (!noreplace) {
      actions.push_back(kTargetFileUpdate);
    }

    actions.push_back(kTargetFileMismatch);
    actions.push_back(kTargetLinkCreate);

    if (!noreplace) {
      actions.push_back(kTargetLinkUpdate);
    }

    actions.push_back(kTargetLinkMismatch);

    if (!nodelete) {
      actions.push_back(kTargetLinkDelete);
      actions.push_back(kTargetFileDelete);
      actions.push_back(kTargetDirDelete);
    }

    actions.push_back(kTargetDirMtime);
  } else if (cmd == "sync") {
    actions.push_back(kTargetDirCreate);
    actions.push_back(kTargetFileCreate);

    if (!noreplace) {
      actions.push_back(kTargetFileUpdate);
    }

    actions.push_back(kTargetFileMismatch);
    actions.push_back(kTargetLinkCreate);

    if (!noreplace) {
      actions.push_back(kTargetLinkUpdate);
    }

    actions.push_back(kTargetLinkMismatch);
    // we cannot detect two-way deletion without history
    // if (!nodelete) {
    //   actions.push_back(kTargetLinkDelete);
    //   actions.push_back(kTargetFileDelete);
    //   actions.push_back(kTargetDirDelete);
    // }
    actions.push_back(kTargetDirMtime);
    actions.push_back(kSourceDirCreate);
    actions.push_back(kSourceFileCreate);
    actions.push_back(kSourceFileUpdate);
    //    actions.push_back(kSourceFileMismatch);
    actions.push_back(kSourceLinkCreate);

    if (!noreplace) {
      actions.push_back(kSourceLinkUpdate);
    }

    //    actions.push_back(kSourceLinkMismatch);
    // we cannot detec two-way deletion without historya
    // if (!nodelete) {
    //   actions.push_back(kSourceLinkDelete);
    //   actions.push_back(kSourceFileDelete);
    //   actions.push_back(kSourceDirDelete);
    // }
    actions.push_back(kSourceDirMtime);
  } else {
    rclone_usage();
  }

  XrdOucString src = subtokenizer.GetToken();
  XrdOucString dst = subtokenizer.GetToken();
  eos::common::Path srcPath(src.c_str());
  eos::common::Path dstPath(dst.c_str());
  src = srcPath.GetFullPath();
  dst = dstPath.GetFullPath();

  if (!src.length() || !dst.length()) {
    rclone_usage();
  }

  nodelete = true;
  noreplace = false;
  dryrun = false;
  XrdOucString option;

  do {
    option = subtokenizer.GetToken();

    if (!option.length()) {
      break;
    }

    if (option == "--delete") {
      nodelete = false;
    } else if (option == "--noreplace") {
      noreplace = true;
    } else if (option == "--dryrun") {
      dryrun = true;
    } else if (option == "--atomic") {
      filter_atomic = false;
    } else if (option == "--versions") {
      filter_versions = false;
    } else if (option == "--hidden") {
      filter_hidden = false;
    } else if (option == "-v" || option == "--verbose") {
      verbose = true;
    } else if (option == "-s" || option == "--silent") {
      is_silent = true;
    } else {
      rclone_usage();
    }
  } while (1);

  fs_result srcmap;
  fs_result dstmap;
  bool ignore_errors = false;

  if (src.beginswith("/eos/")) {
    // get the sync informtion using newfind
    srcmap = eos_find(src.c_str());
  } else {
    // travers using UNIX find
    srcmap = fs_find(src.c_str());
  }

  if (dst.beginswith("/eos/")) {
    // get the sync information using newfind
    dstmap = eos_find(dst.c_str());
  } else {
    // travers using UNIX find
    dstmap = fs_find(dst.c_str());
  }

  srcmap.directories.erase("/");
  dstmap.directories.erase("/");

  // forward comparison
  for (auto d : srcmap.directories) {
    if (!dstmap.directories.count(d.first)) {
      if (!is_silent && verbose) {
        std::cout << "[ target folder missing ] : " << d.first << std::endl;
      }

      target_create_dirs.insert(d.first);
      target_mtime_dirs.insert(d.first);
    } else {
      if (dstmap.directories[d.first].newer(srcmap.directories[d.first].mtime)) {
        target_mtime_dirs.insert(d.first);
      }
    }
  }

  /// backward
  for (auto d : dstmap.directories) {
    if (!srcmap.directories.count(d.first)) {
      if (!is_silent && verbose) {
        std::cout << "[ source folder missing ] : " << d.first << std::endl;
      }

      if (!nodelete) {
        target_delete_dirs.insert(d.first);
        target_mtime_dirs.insert(parent(d.first));
      } else {
        source_create_dirs.insert(d.first);
        source_mtime_dirs.insert(d.first);
      }
    } else {
      if (srcmap.directories[d.first].newer(dstmap.directories[d.first].mtime)) {
        source_mtime_dirs.insert(d.first);
      }
    }
  }

  // forward comparison
  for (auto d : srcmap.files) {
    if (!dstmap.files.count(d.first)) {
      if (!is_silent && verbose) {
        std::cout << "[ target file   missing ] : " << d.first << std::endl;
      }

      target_create_files.insert(d.first);
      copySize += d.second.size;
      copyTransactions++;
    } else {
      if (dstmap.files[d.first].newer(srcmap.files[d.first].mtime)) {
        if (!is_silent && verbose) {
          std::cout << "[ target file   older   ] : " << d.first << std::endl;
        }

        target_updated_files.insert(d.first);

        if (!noreplace) {
          copySize += d.second.size;
          copyTransactions++;
        }
      } else {
        if (dstmap.files[d.first].size != srcmap.files[d.first].size) {
          if (!is_silent && verbose) {
            std::cout << "[ target file diff size ] : " << d.first << std::endl;
          }

          target_mismatch_files.insert(d.first);
        }
      }
    }
  }

  // backward comparison
  for (auto d : dstmap.files) {
    if (!srcmap.files.count(d.first)) {
      if (!is_silent && verbose) {
        std::cout << "[ source file   missing ] : " << d.first << std::endl;
      }

      if (!nodelete) {
        target_delete_files.insert(d.first);
      } else {
        source_create_files.insert(d.first);
        copySize += d.second.size;
        copyTransactions++;
      }
    } else {
      if (srcmap.files[d.first].newer(dstmap.files[d.first].mtime)) {
        if (!is_silent && verbose) {
          std::cout << "[ source file   older   ] : " << d.first << std::endl;
        }

        source_updated_files.insert(d.first);

        if (!noreplace) {
          copySize += d.second.size;
          copyTransactions++;
        }
      } else {
        if (dstmap.files[d.first].size != srcmap.files[d.first].size) {
          if (!is_silent && verbose) {
            std::cout << "[ source file diff size ] : " << d.first << std::endl;
          }

          source_mismatch_files.insert(d.first);
        }
      }
    }
  }

  // forward comparison
  for (auto d : srcmap.links) {
    if (!dstmap.links.count(d.first)) {
      if (!is_silent && verbose) {
        std::cout << "[ target link   missing ] : " << d.first << std::endl;
      }

      target_create_links.insert(d.first);
    } else {
      if (dstmap.links[d.first].newer(srcmap.links[d.first].mtime)) {
        if (!is_silent && verbose) {
          std::cout << "[ target link   older   ] : " << d.first << std::endl;
        }

        target_updated_links.insert(d.first);
      } else {
        if (dstmap.links[d.first].target != srcmap.links[d.first].target) {
          if (!is_silent && verbose) {
            std::cout << "[ target link diff size ] : " << d.first << std::endl;
          }

          target_mismatch_links.insert(d.first);
        }
      }
    }
  }

  // backward comparison
  for (auto d : dstmap.links) {
    if (!srcmap.links.count(d.first)) {
      if (!is_silent && verbose) {
        std::cout << "[ source link   missing ] : " << d.first << std::endl;
      }

      if (!nodelete) {
        target_delete_links.insert(d.first);
      } else {
        source_create_links.insert(d.first);
      }
    } else {
      if (srcmap.links[d.first].newer(dstmap.links[d.first].mtime)) {
        if (!is_silent && verbose) {
          std::cout << "[ source link   older   ] : " << d.first << std::endl;
        }

        source_updated_links.insert(d.first);
      } else {
        if (dstmap.links[d.first].target != srcmap.links[d.first].target) {
          if (!is_silent && verbose) {
            std::cout << "[ source link diff size ] : " << d.first << std::endl;
          }

          source_mismatch_links.insert(d.first);
        }
      }
    }
  }

  if (!is_silent) {
    std::cout << "[ --------------------------- ]" << std::endl;
    std::cout << "[ EOS remote sync tool (beta) ]" << std::endl;
  }

  if (!dryrun) {
    if (!is_silent) {
      std::cout << "[ --------------------------- ]" << std::endl;
      std::cout << "[ target                      ]" << std::endl;
      std::cout << "[ --------------------------- ]" << std::endl;
      std::cout << "[ # dir,files,links to create ] : " << target_create_dirs.size()
                << "," << target_create_files.size() << "," << target_create_links.size() <<
                std::endl;
      std::cout << "[ # dir,files,links to delete ] : " << target_delete_dirs.size()
                << "," << target_delete_files.size() << "," << target_delete_links.size() <<
                std::endl;
      std::cout << "[ # files,links to update     ] : " << target_updated_files.size()
                << "," << target_updated_links.size() << std::endl;
      std::cout << "[ # files,links mismatch      ] : " <<
                target_mismatch_files.size() << "," << target_mismatch_links.size() <<
                std::endl;
      std::cout << "[ --------------------------- ]" << std::endl;
      std::cout << "[ source                      ]" << std::endl;
      std::cout << "[ --------------------------- ]" << std::endl;
      std::cout << "[ # dir,files,links to create ] : " << source_create_dirs.size()
                << "," << source_create_files.size() << "," << source_create_links.size() <<
                std::endl;
      std::cout << "[ # dir,files,links to delete ] : " << source_delete_dirs.size()
                << "," << source_delete_files.size() << "," << source_delete_links.size() <<
                std::endl;
      std::cout << "[ # files,links to update     ] : " << source_updated_files.size()
                << "," << source_updated_links.size() << std::endl;
      std::cout << "[ # files,links mismatch      ] : " <<
                source_mismatch_files.size() << "," << source_mismatch_links.size() <<
                std::endl;
      std::cout << "[ --------------------------- ]" << std::endl;
      std::cout << "[ volume                      ]" << std::endl;
      std::cout << "[ --------------------------- ]" << std::endl;
      XrdOucString sizestring;
      eos::common::StringConversion::GetReadableSizeString(sizestring, copySize, "B");
      std::cout << "[ # data size                 ] : " << sizestring.c_str() <<
                std::endl;
      eos::common::StringConversion::GetReadableSizeString(sizestring,
          copyTransactions, "");
      std::cout << "[ # copy transactions         ] : " << sizestring.c_str() <<
                std::endl;
      std::cout << "[ --------------------------- ]" << std::endl;
    }
  }

  for (auto a : actions) {
    if (a == kTargetDirCreate) {
      for (auto i : target_create_dirs) {
        int rc = createDir(i, dstPath);

        if (rc && !ignore_errors) {
          std::cerr << "error: failed to create directory '" << dstPath.GetFullPath() << i
                    << "'" << std::endl;
          exit(-1);
        }
      }
    }

    if (a == kSourceDirCreate) {
      for (auto i : source_create_dirs) {
        int rc = createDir(i, srcPath);

        if (rc && !ignore_errors) {
          std::cerr << "error: failed to create directory '" << dstPath.GetFullPath() << i
                    << "'" << std::endl;
          exit(-1);
        }
      }
    }

    if (a == kTargetDirDelete) {
      for (auto i : target_delete_dirs) {
        int rc = removeDir(i, dstPath);

        if (rc && !ignore_errors) {
          std::cerr << "error: failed to remove directory '" << dstPath.GetFullPath() << i
                    << "'" << std::endl;
          exit(-1);
        }
      }
    }

    if (a == kTargetFileDelete) {
      for (auto i : target_delete_files) {
        int rc = removeFile(i, dstPath);

        if (rc && !ignore_errors) {
          std::cerr << "error: failed to remove file '" << dstPath.GetFullPath() << i <<
                    "'" << std::endl;
          exit(-1);
        }
      }
    }

    if (a == kTargetLinkDelete) {
      for (auto i : target_delete_links) {
        int rc = removeFile(i, dstPath);

        if (rc && !ignore_errors) {
          std::cerr << "error: failed to remove link '" << dstPath.GetFullPath() << i <<
                    "'" << std::endl;
          exit(-1);
        }
      }
    }

    if (a == kSourceDirDelete) {
      for (auto i : source_delete_dirs) {
        int rc = removeDir(i, srcPath);

        if (rc && !ignore_errors) {
          std::cerr << "error: failed to remove directory '" << srcPath.GetFullPath() << i
                    << "'" << std::endl;
          exit(-1);
        }
      }
    }

    if (a == kSourceFileDelete) {
      for (auto i : source_delete_files) {
        int rc = removeFile(i, srcPath);

        if (rc && !ignore_errors) {
          std::cerr << "error: failed to remove file '" << srcPath.GetFullPath() << i <<
                    "'" << std::endl;
          exit(-1);
        }
      }
    }

    if (a == kSourceLinkDelete) {
      for (auto i : source_delete_links) {
        int rc = removeFile(i, srcPath);

        if (rc && !ignore_errors) {
          std::cerr << "error: failed to remove link '" << srcPath.GetFullPath() << i <<
                    "'" << std::endl;
          exit(-1);
        }
      }
    }

    if (a == kTargetFileCreate) {
      for (auto i : target_create_files) {
        cp_target_files.insert(i);
      }
    }

    if (a == kTargetFileUpdate) {
      for (auto i : target_updated_files) {
        cp_target_files.insert(i);
      }
    }

    if (a == kTargetFileMismatch) {
      for (auto i : target_mismatch_files) {
        cp_target_files.insert(i);
      }
    }

    if (a == kTargetLinkCreate) {
      for (auto i : target_create_links) {
        if (!is_silent && verbose) {
          std::cout << " link  ] create link " << i.c_str() << " => " <<
                    srcmap.links[i].target.c_str() << std::endl;
        }

        if (!dryrun) {
          int rc = createLink(i, dstPath, srcmap.links[i].target, srcPath,
                              srcmap.links[i].mtime);

          if (rc && !ignore_errors) {
            std::cerr << "error: failed to create link '" << dstPath.GetFullPath() << i <<
                      "'" << std::endl;
            exit(-1);
          }
        }
      }
    }

    if (a == kTargetLinkUpdate) {
      for (auto i : target_updated_links) {
        if (!is_silent && verbose) {
          std::cout << "[ link  ] update link " << i.c_str() << " => " <<
                    srcmap.links[i].target.c_str() << std::endl;
        }

        if (!dryrun) {
          int rc = removeFile(i, dstPath);
          rc |= createLink(i, dstPath, srcmap.links[i].target, srcPath,
                           srcmap.links[i].mtime);

          if (rc && !ignore_errors) {
            std::cerr << "error: failed to update link '" << dstPath.GetFullPath() << i <<
                      "'" << std::endl;
            exit(-1);
          }
        }
      }
    }

    if (a == kTargetLinkMismatch) {
      for (auto i : target_mismatch_links) {
        if (!is_silent && verbose) {
          std::cout << "[ link  ] remove link " << i.c_str() << std::endl;
        }

        if (!dryrun) {
          int rc = removeFile(i, dstPath);
          rc |= createLink(i, dstPath, srcmap.links[i].target, srcPath,
                           srcmap.links[i].mtime);

          if (rc && !ignore_errors) {
            std::cerr << "error: failed to update mismatching link '" <<
                      dstPath.GetFullPath() << i << "'" << std::endl;
            exit(-1);
          }
        }
      }
    }

    if (a == kSourceFileCreate) {
      for (auto i : source_create_files) {
        cp_source_files.insert(i);
      }
    }

    if (a == kSourceFileUpdate) {
      for (auto i : source_updated_files) {
        cp_source_files.insert(i);
      }
    }

    if (a == kSourceFileMismatch) {
      for (auto i : source_mismatch_files) {
        cp_source_files.insert(i);
      }
    }

    if (a == kSourceLinkCreate) {
      for (auto i : source_create_links) {
        if (!is_silent && verbose) {
          std::cout << "[ link  ] create link " << i.c_str() << " => " <<
                    dstmap.links[i].target.c_str() << std::endl;
        }

        if (!dryrun) {
          int rc = createLink(i, srcPath, dstmap.links[i].target, dstPath,
                              dstmap.links[i].mtime);

          if (rc && !ignore_errors) {
            std::cerr << "error: failed to create link '" << srcPath.GetFullPath() << i <<
                      "'" << std::endl;
            exit(-1);
          }
        }
      }
    }

    if (a == kSourceLinkUpdate) {
      for (auto i : source_updated_links) {
        if (!is_silent && verbose) {
          std::cout << "[ link  ] update link " <<  i.c_str() <<
                    dstmap.links[i].target.c_str() << std::endl;
        }

        if (!dryrun) {
          int rc = removeFile(i, srcPath);
          rc |= createLink(i, srcPath, dstmap.links[i].target, dstPath,
                           dstmap.links[i].mtime);

          if (rc && !ignore_errors) {
            std::cerr << "error: failed to update link '" << srcPath.GetFullPath() << i <<
                      "'" << std::endl;
            exit(-1);
          }
        }
      }
    }

    if (a == kSourceLinkMismatch) {
      for (auto i : source_mismatch_links) {
        if (!is_silent && verbose) {
          std::cout << "[ link  ]remove link " << i.c_str() << std::endl;
        }

        if (!dryrun) {
          int rc = removeFile(i, srcPath);
          rc |= createLink(i, srcPath, dstmap.links[i].target, dstPath,
                           dstmap.links[i].mtime);

          if (rc && !ignore_errors) {
            std::cerr << "error: failed to update mismatching link '" <<
                      srcPath.GetFullPath() << i << "'" << std::endl;
            exit(-1);
          }
        }
      }
    }
  }

  for (auto i : cp_target_files) {
    if (!dryrun) {
      tprops.push_back(copyFile(i, srcPath, dstPath, srcmap.files[i].mtime));
    } else {
      if (!is_silent && verbose) {
        std::cout << "[ copy ] : " << i.c_str() << " " << srcPath.GetFullPath().c_str()
                  << " => " << dstPath.GetFullPath().c_str() << std::endl;
      }
    }
  }

  for (auto i : cp_source_files) {
    if (!dryrun) {
      tprops.push_back(copyFile(i, dstPath, srcPath, dstmap.files[i].mtime));
    } else {
      if (!is_silent && verbose) {
        std::cout << "[ copy ] : " << i.c_str() << " " << dstPath.GetFullPath().c_str()
                  << " => " << srcPath.GetFullPath().c_str() << std::endl;
      }
    }
  }

  class RCloneProgressHandler : public XrdCl::CopyProgressHandler
  {
  public:
    virtual void BeginJob(uint32_t   jobNum,
                          uint32_t   jobTotal,
                          const URL* source,
                          const URL* destination)
    {
      n = jobNum;
      tot = jobTotal;
    }

    virtual void EndJob(uint32_t            jobNum,
                        const PropertyList* result)
    {
      (void)jobNum;
      (void)result;
      std::string src;
      std::string dst;
      result->Get("source", src);
      result->Get("target", dst);
      XrdCl::URL durl(dst.c_str());
      auto param = durl.GetParams();

      if (param.count("local.mtime")) {
        // apply mtime changes when done to local files
        struct timespec ts;
        std::string tss = param["local.mtime"];

        if (!eos::common::Timing::Timespec_from_TimespecStr(tss, ts)) {
          // apply local mtime;
          struct timespec times[2];
          times[0] = ts;
          times[1] = ts;

          if (utimensat(0, durl.GetPath().c_str(), times, AT_SYMLINK_NOFOLLOW)) {
            std::cerr << "error: failed to update modification time of '" << durl.GetPath()
                      << "'" << std::endl;
          }
        }
      }
    };

    virtual void JobProgress(uint32_t jobNum,
                             uint64_t bytesProcessed,
                             uint64_t bytesTotal)
    {
      bp = bytesProcessed;
      bt = bytesTotal;
      n  = jobNum;

      if (verbose) {
        std::cerr << "[ " << jobNum << "/" << tot << " ] files copied" << std::endl;
      } else {
        if (!is_silent) {
          std::cerr << "[ " << jobNum << "/" << tot << " ] files copied" << "\r";
        }
      }
    }

    virtual bool ShouldCancel(uint32_t jobNum)
    {
      (void)jobNum;
      return false;
    }

    std::atomic<uint64_t> bp;
    std::atomic<uint64_t> bt;
    std::atomic<uint32_t> n;
    std::atomic<uint32_t> tot;
  };

  RCloneProgressHandler copyProgress;

  if (!is_silent && verbose) {
    std::cerr << "# preparing" << std::endl;
  }

  copyProcess.Prepare();

  if (!is_silent && verbose) {
    std::cerr << "# running" << std::endl;
  }

  copyProcess.Run(&copyProgress);

  if (!is_silent) {
    std::cout << std::endl;
  }

  // last step is to adjust directory mtimes
  for (auto a : actions) {
    if (a == kTargetDirMtime) {
      for (auto i : target_mtime_dirs) {
        if (!is_silent && verbose) {
          std::cout << "[ mtime ] updating target mtime " << i << std::endl;
        }

        if (!dryrun) {
          int rc = setDirMtime(i, dstPath, srcmap.directories[i].mtime);

          if (rc && !ignore_errors) {
            std::cerr << "error: failed to update directory mtime  '" <<
                      dstPath.GetFullPath() << i << "'" << std::endl;
            exit(-1);
          }
        }
      }
    }

    if (a == kSourceDirMtime) {
      for (auto i : source_mtime_dirs) {
        if (!is_silent && verbose) {
          std::cout << "[ mtime ] updating source mtime "  << i << std::endl;
        }

        if (!dryrun) {
          int rc = setDirMtime(i, srcPath, dstmap.directories[i].mtime);

          if (rc && !ignore_errors) {
            std::cerr << "error: failed to update directory mtime  '" <<
                      srcPath.GetFullPath() << i << "'" << std::endl;
            exit(-1);
          }
        }
      }
    }
  }

  if (dryrun && !is_silent) {
    std::cout << "[ --------------------------- ]" << std::endl;
    std::cout << "[ target                      ]" << std::endl;
    std::cout << "[ --------------------------- ]" << std::endl;
    std::cout << "[ # dir,files,links to create ] : " << target_create_dirs.size()
              << "," << target_create_files.size() << "," << target_create_links.size() <<
              std::endl;
    std::cout << "[ # dir,files,links to delete ] : " << target_delete_dirs.size()
              << "," << target_delete_files.size() << "," << target_delete_links.size() <<
              std::endl;
    std::cout << "[ # files,links to update     ] : " << target_updated_files.size()
              << "," << target_updated_links.size() << std::endl;
    std::cout << "[ # files,links mismatch      ] : " <<
              target_mismatch_files.size() << "," << target_mismatch_links.size() <<
              std::endl;
    std::cout << "[ --------------------------- ]" << std::endl;
    std::cout << "[ source                      ]" << std::endl;
    std::cout << "[ --------------------------- ]" << std::endl;
    std::cout << "[ # dir,files,links to create ] : " << source_create_dirs.size()
              << "," << source_create_files.size() << "," << source_create_links.size() <<
              std::endl;
    std::cout << "[ # dir,files,links to delete ] : " << source_delete_dirs.size()
              << "," << source_delete_files.size() << "," << source_delete_links.size() <<
              std::endl;
    std::cout << "[ # files,links to update     ] : " << source_updated_files.size()
              << "," << source_updated_links.size() << std::endl;
    std::cout << "[ # files,links mismatch      ] : " <<
              source_mismatch_files.size() << "," << source_mismatch_links.size() <<
              std::endl;
    std::cout << "[ --------------------------- ]" << std::endl;
    std::cout << "[ volume                      ]" << std::endl;
    std::cout << "[ --------------------------- ]" << std::endl;
    XrdOucString sizestring;
    eos::common::StringConversion::GetReadableSizeString(sizestring, copySize, "B");
    std::cout << "[ # data size                 ] : " << sizestring.c_str() <<
              std::endl;
    eos::common::StringConversion::GetReadableSizeString(sizestring,
        copyTransactions, "");
    std::cout << "[ # copy transactions         ] : " << sizestring.c_str() <<
              std::endl;
    std::cout << "[ --------------------------- ]" << std::endl;
  }

  exit(0);
}
