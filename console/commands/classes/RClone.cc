// ----------------------------------------------------------------------
// File: RClone.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2025 CERN/Switzerland                                  *
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

#include "console/commands/classes/RClone.hh"
#include "common/StringConversion.hh"
#include "common/table_formatter/TableFormatterBase.hh"
#include "common/table_formatter/TableFormatting.hh"
#include <fstream>
#include <iomanip>
#include <limits>
#include <sstream>

extern XrdOucString serveruri;

namespace eos {
namespace console {

/**
 * Compare modification times between two entries
 * @param cmptime The timespec to compare against
 * @param debug Whether to print debug output
 * @param lowres Whether to consider low resolution mode
 * @return true if this entry is newer than cmptime
 */
bool
FSEntry::newer(const struct timespec& cmptime, bool debug, bool lowres) const
{
  if (debug) {
    std::cout << "[ DEBUG ] : \033[1mnewer\033[0m - mtime.tv_sec: " << mtime.tv_sec << " cmptime.tv_sec: " << cmptime.tv_sec << std::endl;
  }
  if (mtime.tv_sec < cmptime.tv_sec) {
    return true;
  } else if (mtime.tv_sec > cmptime.tv_sec) {
    return false;
  } else {
    // If in low resolution mode, consider equal seconds as not newer
    if (lowres) {
      return false;
    }
    // Otherwise compare nanoseconds
    if (mtime.tv_nsec < cmptime.tv_nsec) {
      return true;
    } else {
      return false;
    }
  }
}


/**
 * Recursively scan a filesystem path and collect information about files,
 * directories, and links Applies configured filters (versions, atomic files,
 * hidden files)
 * @param path The filesystem path to scan
 * @return FSResult containing maps of found files, directories, and links
 */
FSResult
RClone::fsFind(const char* path) const
{
  FSResult result;
  std::stringstream s;
  eos::common::Path cPath(path);
  namespace fs = std::filesystem;
  fs::path path_to_traverse = path;
  struct stat buf;

  // Add the input path itself to the result
  if (!::lstat(path, &buf)) {
    if (S_ISDIR(buf.st_mode)) {
      result.directories["/"].mtime = buf.st_mtim;
      result.directories["/"].size = buf.st_size;
    }
  }

  try {
    for (const auto& entry : fs::recursive_directory_iterator(
             path_to_traverse,
             std::filesystem::directory_options::skip_permission_denied)) {
      std::string p = entry.path().string();
      // filter functions
      eos::common::Path iPath(p.c_str());

      if (mFilterVersions) {
        if (iPath.isVersionPath()) {
          continue;
        }
      }

      if (mFilterAtomic) {
        if (iPath.isAtomicFile()) {
          continue;
        }
      }

      if (mFilterHidden) {
        std::string::size_type pos = iPath.GetFullPath().find("/.");
        if (pos != std::string::npos) {
          if (!iPath.isVersionPath() && !iPath.isAtomicFile()) {
            continue;
          }
        }
      }

      std::string t = p;

      if (!::lstat(p.c_str(), &buf)) {
        p.erase(0, cPath.GetFullPath().length());

        switch ((buf.st_mode & S_IFMT)) {
        case S_IFDIR:
          p += "/";
          result.directories[p].mtime = buf.st_mtim;
          result.directories[p].size = buf.st_size;
          break;

        case S_IFREG:
          result.files[p].mtime = buf.st_mtim;
          result.files[p].size = buf.st_size;
          break;

        case S_IFLNK:
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
    std::cerr << "error: " << ex.what() << '\n'
              << "#      path  : " << ex.path1() << '\n'
              << "#      errc  : " << ex.code().value() << '\n'
              << "#      msg   : " << ex.code().message() << '\n'
              << "#      class : " << ex.code().category().name() << '\n';
    exit(-1);
  }

  // If this is dry run second pass, remove the deleted entries
  if (mDryRun && mIsDryRunSecondPass) {
    // Remove deleted directories
    for (const auto& dir : mDryRunDeletedDirs) {
      result.directories.erase(dir);
    }
    // Remove deleted files
    for (const auto& file : mDryRunDeletedFiles) {
      result.files.erase(file);
    }
    // Remove deleted links
    for (const auto& link : mDryRunDeletedLinks) {
      result.links.erase(link);
    }
  }

  return result;
}

/**
 * Scan an EOS filesystem path and collect information about files, directories,
 * and links Uses the EOS find command and applies configured filters
 * @param path The EOS path to scan
 * @return FSResult containing maps of found files, directories, and links
 */
FSResult
RClone::eosFind(const char* path) const
{
  FSResult result;
  eos::common::Path cPath(path);
  NewfindHelper find(gGlobalOpts);
  std::string args = "--format type,mtime,size,link ";
  args += path;

  // Add the input path itself to the result
  struct timespec ts {0, 0};
  size_t size{0};
  std::string request;
  {
    XrdCl::Buffer arg;
    XrdCl::Buffer* response = nullptr;
    request = common::StringConversion::curl_escaped(path);
    request += "?mgm.pcmd=stat&eos.encodepath=1";
    arg.FromString(request);
    XrdCl::URL url(serveruri.c_str());
    url.SetPath(path);
    XrdCl::FileSystem fs(url);
    XrdCl::XRootDStatus status = fs.Query(XrdCl::QueryCode::OpaqueFile, arg, response);
    if (status.IsOK() && response) {
      std::string resp = response->ToString();
      std::vector<std::string> lines;
      eos::common::StringConversion::Tokenize(resp, lines, "\n");
      for (const auto& line : lines) {
        if (line.find("mtime=") != std::string::npos) {
          std::string mtime = line.substr(line.find("=") + 1);
          eos::common::Timing::Timespec_from_TimespecStr(mtime, ts);
        }
        if (line.find("size=") != std::string::npos) {
          std::string sizestr = line.substr(line.find("=") + 1);
          size = std::stoull(sizestr.c_str(), 0, 10);
        }
      }
      result.directories["/"].mtime = ts;
      result.directories["/"].size = size;
    }
    if (response) {
      delete response;
    }
  }

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

        if (mFilterVersions) {
          if (iPath.isVersionPath()) {
            break;
          }
        }

        if (mFilterAtomic) {
          if (iPath.isAtomicFile()) {
            break;
          }
        }

        if (mFilterHidden) {
          std::string::size_type pos = iPath.GetFullPath().find("/.");
          if (pos != std::string::npos) {
            if (!iPath.isVersionPath() && !iPath.isAtomicFile()) {
              break;
            }
          }
        }
      }
    }
  } else {
    std::cerr << "error: " << find.GetError() << std::endl;
    exit(rc);
  }

  // If this is dry run second pass, remove the deleted entries
  if (mDryRun && mIsDryRunSecondPass) {
    // Remove deleted directories
    for (const auto& dir : mDryRunDeletedDirs) {
      result.directories.erase(dir);
    }
    // Remove deleted files
    for (const auto& file : mDryRunDeletedFiles) {
      result.files.erase(file);
    }
    // Remove deleted links
    for (const auto& link : mDryRunDeletedLinks) {
      result.links.erase(link);
    }
  }

  return result;
}

/**
 * Handler for the beginning of a copy job
 * @param jobNum Current job number
 * @param jobTotal Total number of jobs
 * @param source Source URL
 * @param destination Destination URL
 */
void
RCloneProgressHandler::BeginJob(uint16_t jobNum, uint16_t jobTotal,
                                const XrdCl::URL* source,
                                const XrdCl::URL* destination)
{
  n = jobNum;
  tot = jobTotal;
  if (mParent->isVerbose()) {
    mParent->verboseOutput("copy", destination->GetPath(),
                          "copying file",
                          "from: " + source->GetPath());
  }
}

/**
 * Handler for the end of a copy job
 * Updates modification times for local files if specified
 * @param jobNum Job number that completed
 * @param result Properties of the completed job
 */
void
RCloneProgressHandler::EndJob(uint16_t jobNum,
                              const XrdCl::PropertyList* result)
{
  (void)jobNum;
  std::string src;
  std::string dst;
  result->Get("source", src);
  result->Get("target", dst);
  
  if (mParent->isVerbose()) {
    mParent->verboseOutput("copy", dst,
                          "copy complete",
                          "from: " + src);
  }
  
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
        std::cerr << "error: failed to update modification time of '"
                  << durl.GetPath() << "'" << std::endl;
      }
    }
  }
}

/**
 * Handler for copy job progress updates
 * Updates progress counters and displays progress information
 * @param jobNum Current job number
 * @param bytesProcessed Number of bytes processed so far
 * @param bytesTotal Total number of bytes to process
 */
void
RCloneProgressHandler::JobProgress(uint16_t jobNum, uint64_t bytesProcessed,
                                   uint64_t bytesTotal)
{
  bp = bytesProcessed;
  bt = bytesTotal;
  n = jobNum;

  if (mParent->isVerbose()) {
    mParent->verboseOutput("progress", std::to_string(jobNum) + "/" + std::to_string(tot),
                          "copying files", 
                          bytesProcessed > 0 ? 
                            std::to_string(bytesProcessed) + "/" + std::to_string(bytesTotal) + " bytes" : "");
  } else {
    if (!mParent->isSilent()) {
      mParent->verboseOutput("progress", std::to_string(jobNum) + "/" + std::to_string(tot),
                            "copying files", "");
    }
  }
}

/**
 * Check if the current job should be cancelled
 * @param jobNum Current job number
 * @return true if the job should be cancelled, false otherwise
 */
bool
RCloneProgressHandler::ShouldCancel(uint16_t jobNum)
{
  (void)jobNum;
  return false;
}

/**
 * Gather file system maps for both source and destination paths
 * Handles both EOS and local filesystem paths
 * @param srcPath Source path to scan
 * @param dstPath Destination path to scan
 * @return Pair of FSResults containing source and destination file system
 * information
 */
std::pair<FSResult, FSResult>
RClone::gatherFileMaps(const eos::common::Path& srcPath,
                       const eos::common::Path& dstPath) const
{
  if (mDebug) {
    eos::common::Path srcPathCopy(srcPath);
    eos::common::Path dstPathCopy(dstPath);
    std::cout << "[ DEBUG ] : \033[1mgatherFileMaps\033[0m - Source path: " << srcPathCopy.GetFullPath() 
              << ", Destination path: " << dstPathCopy.GetFullPath() << std::endl;
  }

  FSResult srcmap;
  FSResult dstmap;

  // Create non-const copies to call GetFullPath()
  eos::common::Path srcPathCopy(srcPath);
  eos::common::Path dstPathCopy(dstPath);

  const char* srcPathStr = srcPathCopy.GetFullPath().c_str();
  const char* dstPathStr = dstPathCopy.GetFullPath().c_str();

  if (srcPathStr && strncmp(srcPathStr, "/eos/", 5) == 0) {
    srcmap = eosFind(srcPathStr);
  } else {
    srcmap = fsFind(srcPathStr);
  }

  if (dstPathStr && strncmp(dstPathStr, "/eos/", 5) == 0) {
    if (mMakeSparse) {
      mMakeSparse = 0; // This is now allowed because mMakeSparse is mutable
    }
    dstmap = eosFind(dstPathStr);
  } else {
    dstmap = fsFind(dstPathStr);
  }

  // Don't erase root directory as we need its mtime information
  // srcmap.directories.erase("/");
  // dstmap.directories.erase("/");

  return {srcmap, dstmap};
}

/**
 * Analyze differences between source and destination directories
 * Identifies directories that need to be created or have their mtimes updated
 * @param srcmap Source filesystem map
 * @param dstmap Destination filesystem map
 */
void
RClone::analyzeDirectories(const FSResult& srcmap, const FSResult& dstmap)
{
  if (mDebug) {
    std::cout << "[ DEBUG ] : \033[1manalyzeDirectories\033[0m - Source directories: " << srcmap.directories.size() 
              << ", Destination directories: " << dstmap.directories.size() << std::endl;
  }

  for (const auto& d : srcmap.directories) {
    auto dst_it = dstmap.directories.find(d.first);
    if (dst_it == dstmap.directories.end()) {
      if (!mSilent && mVerbose) {
        verboseOutput("directory", d.first, "missing in destination", "will create");
      }
      target_create_dirs.insert(d.first);
      target_mtime_dirs.insert(d.first);
    } else {
      if (dst_it->second.newer(d.second.mtime, mDebug, mLowRes)) {
        if (!mSilent && mVerbose) {
          verboseOutput("directory", d.first, "timestamp mismatch", "will update mtime");
        }
        target_mtime_dirs.insert(d.first);
      } else {
        if (!mSilent && mVerbose) {
          verboseOutput("directory", d.first, "identical", "no action needed");
        }
      }
    }
  }
}

/**
 * Analyze differences between source and destination files
 * Identifies files that need to be created or updated
 * Respects the noReplace setting
 * @param srcmap Source filesystem map
 * @param dstmap Destination filesystem map
 */
void
RClone::analyzeFiles(const FSResult& srcmap, const FSResult& dstmap)
{
  if (mDebug) {
    std::cout << "[ DEBUG ] : \033[1manalyzeFiles\033[0m - Source files: " << srcmap.files.size() 
              << ", Destination files: " << dstmap.files.size() << std::endl;
  }

  for (const auto& f : srcmap.files) {
    origSize_ += f.second.size;
    origTransactions_++;
    auto dst_it = dstmap.files.find(f.first);
    if (dst_it == dstmap.files.end()) {
      if (!mSilent && mVerbose) {
        verboseOutput("file", f.first, "missing in destination", "will create");
      }
      target_create_files.insert(f.first);
      copySize_ += f.second.size;
      copyTransactions_++;
    } else {
      if (dst_it->second.newer(f.second.mtime, mDebug, mLowRes)) {
        if (!mNoReplace) {
          if (!mSilent && mVerbose) {
            verboseOutput("file", f.first, "timestamp mismatch", "will update");
          }
          target_updated_files.insert(f.first);
          copySize_ += f.second.size;
          copyTransactions_++;
        }
      } else {
        if (dst_it->second.size != f.second.size) {
          if (!mNoReplace) { 
            if (!mSilent && mVerbose) {
              verboseOutput("file", f.first, "size mismatch", 
                          "src: " + std::to_string(f.second.size) + 
                          " dst: " + std::to_string(dst_it->second.size));
            }
            target_mismatch_files.insert(f.first);
          }
        } else {
          if (!mSilent && mVerbose) {
            verboseOutput("file", f.first, "identical", "no action needed");
          }
        }
      }
    }
  }
}

/**
 * Analyze differences between source and destination symbolic links
 * Identifies links that need to be created, updated, or fixed due to target
 * mismatches
 * @param srcmap Source filesystem map
 * @param dstmap Destination filesystem map
 */
void
RClone::analyzeLinks(const FSResult& srcmap, const FSResult& dstmap)
{
  if (mDebug) {
    std::cout << "[ DEBUG ] : \033[1manalyzeLinks\033[0m - Source links: " << srcmap.links.size() 
              << ", Destination links: " << dstmap.links.size() << std::endl;
  }

  for (const auto& l : srcmap.links) {
    auto dst_it = dstmap.links.find(l.first);
    if (dst_it == dstmap.links.end()) {
      if (!mSilent && mVerbose) {
        verboseOutput("symlink", l.first, "missing in destination", "will create");
      }
      target_create_links.insert(l.first);
    } else {
      if (dst_it->second.newer(l.second.mtime, mDebug, mLowRes)) {
        if (!mNoReplace) {
          if (!mSilent && mVerbose) {
            verboseOutput("symlink", l.first, "timestamp mismatch", "will update");
          }
          target_updated_links.insert(l.first);
        }
      } else {
        if (dst_it->second.target != l.second.target) {
          if (!mSilent && mVerbose) {
            verboseOutput("symlink", l.first, "target mismatch", 
                        "src: " + l.second.target + " dst: " + dst_it->second.target);
          }
          target_mismatch_links.insert(l.first);
        } else {
          if (!mSilent && mVerbose) {
            verboseOutput("symlink", l.first, "identical", "no action needed");
          }
        }
      }
    }
  }
}

/**
 * Handle deletion of files, directories, and links that exist in destination
 * but not in source. Only performs deletions if noDelete is false and if the
 * parent directory in source is newer than in target.
 * @param srcmap Source filesystem map
 * @param dstmap Destination filesystem map
 */
void
RClone::handleDeletions(const FSResult& srcmap, const FSResult& dstmap)
{
  if (mDebug) {
    std::cout << "[ DEBUG ] : \033[1mhandleDeletions\033[0m - Source entries: " 
              << "(dirs: " << srcmap.directories.size() 
              << ", files: " << srcmap.files.size() 
              << ", links: " << srcmap.links.size() << ")"
              << ", Destination entries: "
              << "(dirs: " << dstmap.directories.size() 
              << ", files: " << dstmap.files.size() 
              << ", links: " << dstmap.links.size() << ")" << std::endl;
  }

  if (!mNoDelete) {
    // Helper function to get parent directory path
    auto getParentDir = [](const std::string& path) -> std::string {
      // Remove trailing slashes except for root
      std::string cleanPath = path;
      while (cleanPath.length() > 1 && cleanPath.back() == '/') {
        cleanPath.pop_back();
      }
      
      size_t lastSlash = cleanPath.find_last_of('/');
      if (lastSlash == std::string::npos) return "/";
      if (lastSlash == 0) return "/";
      return cleanPath.substr(0, lastSlash + 1);
    };

    // Helper function to find first existing parent directory in a map
    auto findFirstExistingParent = [&](const std::string& path, const FSResult& map) -> std::optional<FSEntry> {
      std::string currentPath = path;
      
      while (true) {
        // First check the current path
        auto it = map.directories.find(currentPath);
        if (it != map.directories.end()) {
          return it->second;
        }

        // Get parent directory
        std::string parentDir = getParentDir(currentPath);
        
        // If we've reached root or we're not moving up anymore, check root and return
        if (parentDir == "/" || currentPath == parentDir) {
          auto rootIt = map.directories.find("/");
          if (rootIt != map.directories.end()) {
            return rootIt->second;
          }
          return std::nullopt;
        }
        
        currentPath = parentDir;
      }
    };

    // Helper function to check if deletion is allowed based on parent directory timestamps
    auto shouldDelete = [&](const std::string& path, const FSResult& srcmap, const FSResult& dstmap) -> bool {
      if (mDebug) {
        std::cout << "[ DEBUG ] : Checking deletion for path: " << path << std::endl;
      }

      // Find first existing parent in source map
      auto srcParent = findFirstExistingParent(path, srcmap);
      if (!srcParent) {
        if (mDebug) {
          std::cout << "[ DEBUG ] : No parent found in source map" << std::endl;
        }
        return false;
      }

      // Find first existing parent in destination map
      auto dstParent = findFirstExistingParent(path, dstmap);
      if (!dstParent) {
        if (mDebug) {
          std::cout << "[ DEBUG ] : No parent found in destination map" << std::endl;
        }
        return false;
      }

      if (mDebug) {
        std::cout << "[ DEBUG ] : Comparing parent timestamps" << std::endl;
      }

      // Compare timestamps using the newer() function
      return dstParent->newer(srcParent->mtime, mDebug, mLowRes);
    };

    // Handle directory deletions
    for (const auto& d : dstmap.directories) {
      if (mDebug) {
        std::cout << "[ DEBUG ] : Checking directory: " << d.first << std::endl;
      }
      if (!srcmap.directories.count(d.first)) {
        if (mDebug) {
          std::cout << "[ DEBUG ] : Directory missing in source: " << d.first << std::endl;
        }
        if (shouldDelete(d.first, srcmap, dstmap)) {
          if (!mSilent && mVerbose) {
            std::cout << "[ target dir delete     ] : " << d.first << std::endl;
          }
          target_delete_dirs.insert(d.first);
        }
      }
    }

    // Handle file deletions
    for (const auto& f : dstmap.files) {
      if (!srcmap.files.count(f.first) && shouldDelete(f.first, srcmap, dstmap)) {
        if (!mSilent && mVerbose) {
          std::cout << "[ target file delete    ] : " << f.first << std::endl;
        }
        target_delete_files.insert(f.first);
      }
    }

    // Handle link deletions
    for (const auto& l : dstmap.links) {
      if (!srcmap.links.count(l.first) && shouldDelete(l.first, srcmap, dstmap)) {
        if (!mSilent && mVerbose) {
          std::cout << "[ target link delete    ] : " << l.first << std::endl;
        }
        target_delete_links.insert(l.first);
      }
    }

    // Store deletions for dry run second pass
    if (mDryRun && !mIsDryRunSecondPass) {
      mDryRunDeletedDirs = target_delete_dirs;
      mDryRunDeletedFiles = target_delete_files;
      mDryRunDeletedLinks = target_delete_links;
    }
  }
}

/**
 * Execute file copy operations
 * Handles both regular and sparse file copies
 * @param srcPath Source path
 * @param dstPath Destination path
 * @param srcmap Source filesystem map containing file information
 * @param ignore_errors Whether to continue on errors
 * @return 0 on success, -1 on error
 */
int
RClone::executeFileOperations(eos::common::Path& srcPath,
                              eos::common::Path& dstPath,
                              const FSResult& srcmap, bool ignore_errors)
{
  std::vector<XrdCl::PropertyList*> tprops;
  std::set<std::string> cp_target_files;

  // Prepare files to copy
  for (const auto& i : target_create_files)
    cp_target_files.insert(i);
  for (const auto& i : target_updated_files)
    cp_target_files.insert(i);
  for (const auto& i : target_mismatch_files)
    cp_target_files.insert(i);

  // Execute copy operations
  for (const auto& i : cp_target_files) {
    auto src_it = srcmap.files.find(i);
    if (src_it == srcmap.files.end())
      continue;

    if (!mDryRun) {
      if (mMakeSparse && src_it->second.size >= mMakeSparse) {
        int rc =
            copySparse(i, dstPath, src_it->second.mtime, src_it->second.size);
        if (rc && !ignore_errors) {
          std::cerr << "error: failed to create sparse file '"
                    << dstPath.GetFullPath().c_str() << i << "'" << std::endl;
          return -1;
        }
        if (!mSparseFilesDump.empty()) {
          std::ofstream ofs(mSparseFilesDump, std::ios::app);
          if (ofs.is_open()) {
            ofs << dstPath.GetFullPath().c_str() << i << std::endl;
          }
        }
      } else {
        tprops.push_back(copyFile(i, srcPath, dstPath, src_it->second.mtime));
      }
    } else {
      if (!mSilent && mVerbose) {
        verboseOutput("copy", std::string(dstPath.GetFullPath().c_str()) + i, "copying file",
                     "from: " + std::string(srcPath.GetFullPath().c_str()) + i + (mDryRun ? " (dry-run)" : ""));
      }
    }
  }

  // Run copy process if needed
  if (!mDryRun && !tprops.empty()) {
    XrdCl::XRootDStatus status = mCopyProcess.Prepare(mCopyParallelism);
    if (!status.IsOK()) {
      std::cerr << "error: failed to prepare copy process" << std::endl;
      return -1;
    }

    status = mCopyProcess.Run(&mCopyProgress);
    if (!status.IsOK()) {
      std::cerr << "error: failed to run copy process" << std::endl;
      return -1;
    }
  }

  // Cleanup
  for (auto prop : tprops) {
    delete prop;
  }

  return 0;
}

/**
 * Copy files from source to destination
 * Main entry point for the copy operation
 * Performs analysis of differences and executes necessary operations
 * @param src Source path
 * @param dst Destination path
 * @return 0 on success, -1 on error
 */
int
RClone::copy(const std::string& src, const std::string& dst)
{
  eos::common::Path srcPath(src);
  eos::common::Path dstPath(dst);
  bool ignore_errors = false;

  target_create_dirs.clear();
  target_mtime_dirs.clear();
  target_create_files.clear();
  target_updated_files.clear();
  target_mismatch_files.clear();
  target_create_links.clear();
  target_updated_links.clear();
  target_mismatch_links.clear();
  target_delete_dirs.clear();
  target_delete_files.clear();
  target_delete_links.clear();
  origSize_ = 0;
  origTransactions_ = 0;
  copySize_ = 0;
  copyTransactions_ = 0;

  // Gather file maps
  auto [srcmap, dstmap] = gatherFileMaps(srcPath, dstPath);

  // Analyze differences
  analyzeDirectories(srcmap, dstmap);
  analyzeFiles(srcmap, dstmap);
  analyzeLinks(srcmap, dstmap);
  handleDeletions(srcmap, dstmap);

  // Create directories
  for (auto i : target_create_dirs) {
    int rc = createDir(i, dstPath);
    if (rc && !ignore_errors) {
      std::cerr << "error: failed to create directory '"
                << dstPath.GetFullPath() << i << "'" << std::endl;
      return -1;
    }
  }

  // Create links
  for (auto i : target_create_links) {
    int rc = createLink(i, dstPath, srcmap.links[i].target, srcPath,
                        srcmap.links[i].mtime);

    if (rc && !ignore_errors) {
      std::cerr << "error: failed to create link '" << dstPath.GetFullPath()
                << i << "'" << std::endl;
      return -1;
    }
  }

  // Update links
  for (auto i : target_updated_links) {
    if (!mDryRun) {
      int rc = removeFile(i, dstPath);
      rc |= createLink(i, dstPath, srcmap.links[i].target, srcPath,
                       srcmap.links[i].mtime);
      if (rc && !ignore_errors) {
        std::cerr << "error: failed to update link '" << dstPath.GetFullPath()
                  << i << "'" << std::endl;
        return -1;
      }
    }
  }

  // Fix mismatched links
  for (auto i : target_mismatch_links) {
    if (!mDryRun) {
      int rc = removeFile(i, dstPath);
      rc |= createLink(i, srcPath, srcmap.links[i].target, dstPath,
                       srcmap.links[i].mtime);
      if (rc && !ignore_errors) {
        std::cerr << "error: failed to fix mismatching link '"
                  << dstPath.GetFullPath() << i << "'" << std::endl;
        return -1;
      }
    }
  }

  // Delete links
  for (auto i : target_delete_links) {
    int rc = removeFile(i, dstPath);
    if (rc && !ignore_errors) {
      std::cerr << "error: failed to remove link '" << dstPath.GetFullPath()
                << i << "'" << std::endl;
      return -1;
    }
  }

  // Delete files
  for (auto i : target_delete_files) {
    int rc = removeFile(i, dstPath);
    if (rc && !ignore_errors) {
      std::cerr << "error: failed to remove file '" << dstPath.GetFullPath()
                << i << "'" << std::endl;
      return -1;
    }
  }

  // Delete directories in reverse order (deepest first)
  std::vector<std::string> dirs_to_delete(target_delete_dirs.begin(),
                                          target_delete_dirs.end());
  std::sort(dirs_to_delete.begin(), dirs_to_delete.end(),
            std::greater<std::string>());
  for (const auto& i : dirs_to_delete) {
    int rc = removeDir(i, dstPath);
    if (rc && !ignore_errors) {
      std::cerr << "error: failed to remove directory '"
                << dstPath.GetFullPath() << i << "'" << std::endl;
      return -1;
    }
  }

  if (!mSilent) {
    printSummary();
    printFinalSummary();
  }

  // Execute file operations
  int rc = executeFileOperations(srcPath, dstPath, srcmap, ignore_errors);
  if (rc != 0) {
    return rc;
  }

  // Set directory mtimes
  for (auto i : target_mtime_dirs) {
    rc = setDirMtime(i, dstPath, srcmap.directories[i].mtime);
    if (rc && !ignore_errors) {
      std::cerr << "error: failed to set mtime on directory '"
                << dstPath.GetFullPath() << i << "'" << std::endl;
      return -1;
    }
  }

  return 0;
}

/**
 * Synchronize two directories bidirectionally
 * First copies from dir1 to dir2, then from dir2 to dir1
 * Prevents deletions during the reverse sync
 * @param dir1 First directory
 * @param dir2 Second directory
 * @return 0 on success, error code on failure
 */
int
RClone::sync(const std::string& dir1, const std::string& dir2)
{
  operation_name = "\033[1mEOS Remote Sync Tool\033[0m \033[31m(sync forward)\033[0m";
  
  // Clear any previous dry run deletions
  mDryRunDeletedDirs.clear();
  mDryRunDeletedFiles.clear();
  mDryRunDeletedLinks.clear();
  mIsDryRunSecondPass = false;
  
  // First copy from dir1 to dir2
  int rc = copy(dir1, dir2);
  if (rc)
    return rc;

  // Then copy from dir2 to dir1
  bool oldNoDelete = mNoDelete;
  mNoDelete = true; // Don't delete during reverse sync
  operation_name = "\033[1mEOS Remote Sync Tool\033[0m \033[31m(sync backward)\033[0m";
  
  // Set second pass flag for dry run
  if (mDryRun) {
    mIsDryRunSecondPass = true;
  }
  
  rc = copy(dir2, dir1);
  mNoDelete = oldNoDelete;
  
  // Reset dry run state
  if (mDryRun) {
    mIsDryRunSecondPass = false;
    mDryRunDeletedDirs.clear();
    mDryRunDeletedFiles.clear();
    mDryRunDeletedLinks.clear();
  }

  return rc;
}

/**
 * Create a directory at the specified path
 * Handles both local and EOS filesystem directories
 * @param i Directory path relative to prefix
 * @param prefix Base path
 * @return 0 on success, error code on failure
 */
int
RClone::createDir(const std::string& i, common::Path& prefix)
{
  if (!prefix.GetFullPath().beginswith("/eos/")) {
    int rc = 0;
    std::string mkpath =
        std::string(prefix.GetFullPath().c_str()) + std::string("/") + i;
    if (mDebug) {
      std::cerr << "createDir: " << mkpath.c_str() << std::endl; 
    }
    if (!mDryRun) {
      rc = ::mkdir(mkpath.c_str(),
                   S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH);
    }
    verboseOutput("mkdir", mkpath, "directory missing in destination", 
                 mDryRun ? "(dry-run)" : "retc: " + std::to_string(rc));
    return rc;
  } else {
    XrdCl::URL url(serveruri.c_str());
    url.SetPath(std::string(prefix.GetFullPath().c_str()) + std::string("/") +
                i);

    if (!url.IsValid()) {
      std::cerr << "error: invalid url " << i.c_str() << std::endl;
      return 0;
    }

    XrdCl::FileSystem fs(url);
    mode_t mode = S_IRUSR | S_IWUSR | S_IXUSR | S_IRGRP;
    XrdCl::Access::Mode mode_xrdcl = common::LayoutId::MapModeSfs2XrdCl(mode);
    XrdCl::XRootDStatus status;
    if (!mDryRun) {
      status = fs.MkDir(url.GetPath(), XrdCl::MkDirFlags::MakePath, mode_xrdcl);
    }
    if (mVerbose) {
      verboseOutput("mkdir", url.GetURL(), "directory missing in destination", 
                   mDryRun ? "(dry-run)" : (status.IsOK() ? "success" : "failed"));
    }
    return (!mDryRun && !status.IsOK());
  }
}

/**
 * Remove a directory at the specified path
 * Handles both local and EOS filesystem directories
 * @param i Directory path relative to prefix
 * @param prefix Base path
 * @return 0 on success, error code on failure
 */
int
RClone::removeDir(const std::string& i, common::Path& prefix)
{
  if (!prefix.GetFullPath().beginswith("/eos/")) {
    int rc = 0;
    std::string rmpath =
        std::string(prefix.GetFullPath().c_str()) + std::string("/") + i;

    if (!mDryRun) {
      rc = ::rmdir(rmpath.c_str());
    }

    verboseOutput("rmdir", rmpath, "directory no longer exists in source", 
                 mDryRun ? "(dry-run)" : "retc: " + std::to_string(rc));
    return rc;
  } else {
    XrdCl::URL url(serveruri.c_str());
    url.SetPath(std::string(prefix.GetFullPath().c_str()) + std::string("/") +
                i);

    if (!url.IsValid()) {
      std::cerr << "error: invalid url " << i.c_str() << std::endl;
      return 0;
    }

    XrdCl::FileSystem fs(url);
    XrdCl::XRootDStatus status;
    if (!mDryRun) {
      status = fs.RmDir(url.GetPath());
    }
    verboseOutput("rmdir", url.GetURL(), "directory no longer exists in source", 
                 mDryRun ? "(dry-run)" : (status.IsOK() ? "success" : "failed"));
    return (!mDryRun && !status.IsOK());
  }
}

/**
 * Remove a file at the specified path
 * Handles both local and EOS filesystem files
 * @param i File path relative to prefix
 * @param prefix Base path
 * @return 0 on success, error code on failure
 */
int
RClone::removeFile(const std::string& i, common::Path& prefix)
{
  if (!prefix.GetFullPath().beginswith("/eos/")) {
    int rc = 0;
    std::string rmpath =
        std::string(prefix.GetFullPath().c_str()) + std::string("/") + i;

    if (!mDryRun) {
      rc = ::unlink(rmpath.c_str());
    }

    verboseOutput("unlink", rmpath, "file no longer exists in source", 
                 mDryRun ? "(dry-run)" : "retc: " + std::to_string(rc));
    return rc;
  } else {
    XrdCl::URL url(serveruri.c_str());
    url.SetPath(std::string(prefix.GetFullPath().c_str()) + std::string("/") +
                i);

    if (!url.IsValid()) {
      std::cerr << "error: invalid url " << i.c_str() << std::endl;
      return 0;
    }

    XrdCl::FileSystem fs(url);
    XrdCl::XRootDStatus status = fs.Rm(url.GetPath());
    verboseOutput("unlink", url.GetURL(), "file no longer exists in source", 
                 mDryRun ? "(dry-run)" : (status.IsOK() ? "success" : "failed"));
    return (!status.IsOK());
  }
}

/**
 * Create a symbolic link
 * Handles both local and EOS filesystem links
 * Updates the link's modification time
 * @param i Link path relative to prefix
 * @param prefix Base path
 * @param target Link target
 * @param targetprefix Target base path
 * @param mtime Modification time to set
 * @return 0 on success, error code on failure
 */
int
RClone::createLink(const std::string& i, common::Path& prefix,
                   const std::string& target, common::Path& targetprefix,
                   struct timespec mtime)
{
  std::string targetpath = target;

  if (targetpath.find(prefix.GetFullPath().c_str()) == 0) {
    targetpath.erase(0, prefix.GetFullPath().length());
    targetpath.insert(0, targetprefix.GetFullPath().c_str());
  }

  if (!prefix.GetFullPath().beginswith("/eos/")) {
    int rc = 0;
    std::string linkpath =
        std::string(prefix.GetFullPath().c_str()) + std::string("/") + i;

    verboseOutput("symlink", linkpath, "link missing or outdated",
                 "target: " + targetpath + (mDryRun ? " (dry-run)" : ""));

    if (!mDryRun) {
      rc = ::symlink(target.c_str(), linkpath.c_str());
      if (rc) {
        std::cerr << "error: symlink rc=" << rc << " errno=" << errno << std::endl;
      }
    }

    struct timespec times[2];
    times[0] = mtime;
    times[1] = mtime;

    if (!mDryRun) {
      int rc2 = utimensat(0, linkpath.c_str(), times, AT_SYMLINK_NOFOLLOW);
      rc |= rc2;
      if (rc2) {
        std::cerr << "error: utimesat rc=" << rc << " errno=" << errno << std::endl;
      }
    }

    return rc;
  } else {
    XrdCl::URL url(serveruri.c_str());
    url.SetPath(std::string(prefix.GetFullPath().c_str()) + std::string("/") +
                i);

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
      request = common::StringConversion::curl_escaped(
          std::string(prefix.GetFullPath().c_str()) + std::string("/") + i);
      request += "?";
      request += "mgm.pcmd=symlink&target=";
      request += common::StringConversion::curl_escaped(targetpath);
      request += "&eos.encodepath=1";
      arg.FromString(request);
      XrdCl::FileSystem fs(url);
      XrdCl::XRootDStatus status =
          fs.Query(XrdCl::QueryCode::OpaqueFile, arg, response);

      if (response) {
        delete response;
      }

      retc = !status.IsOK();
    }
    {
      // fix mtime
      XrdCl::Buffer arg;
      XrdCl::Buffer* response = nullptr;
      request = common::StringConversion::curl_escaped(
          std::string(prefix.GetFullPath().c_str()) + std::string("/") + i);
      request += "?";
      request += "mgm.pcmd=utimes";
      request += "&tv1_sec=0";  // ignored
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
      XrdCl::XRootDStatus status =
          fs.Query(XrdCl::QueryCode::OpaqueFile, arg, response);

      if (response) {
        delete response;
      }

      retc |= !status.IsOK();
    }

    verboseOutput("symlink", url.GetURL(), "link missing or outdated",
                 "target: " + targetpath + (mDryRun ? " (dry-run)" : " retc: " + std::to_string(retc)));
    return retc;
  }
}

/**
 * Set directory modification time
 * Handles both local and EOS filesystem directories
 * @param i Directory path relative to prefix
 * @param prefix Base path
 * @param mtime Modification time to set
 * @return 0 on success, error code on failure
 */
int
RClone::setDirMtime(const std::string& i, common::Path& prefix,
                    struct timespec mtime)
{
  std::string mtpath =
      std::string(prefix.GetFullPath().c_str()) + std::string("/") + i;

  if (!prefix.GetFullPath().beginswith("/eos/")) {
    // apply local mtime;
    struct timespec times[2];
    times[0] = mtime;
    times[1] = mtime;
    int rc = 0;

    if (!mDryRun) {
      rc = utimensat(0, mtpath.c_str(), times, AT_SYMLINK_NOFOLLOW);
    }

    verboseOutput("mtime", mtpath, "updating timestamp",
                 std::to_string(mtime.tv_sec) + ":" + std::to_string(mtime.tv_nsec) +
                 (mDryRun ? " (dry-run)" : " retc: " + std::to_string(rc)));

    return rc;
  } else {
    XrdCl::URL url(serveruri.c_str());
    url.SetPath(std::string(prefix.GetFullPath().c_str()) + std::string("/") +
                i);

    if (!url.IsValid()) {
      std::cerr << "error: invalid url " << i.c_str() << std::endl;
      return 0;
    }

    std::string request;
    XrdCl::Buffer arg;
    XrdCl::Buffer* response = nullptr;
    request = common::StringConversion::curl_escaped(
        std::string(prefix.GetFullPath().c_str()) + std::string("/") + i);
    request += "?";
    request += "mgm.pcmd=utimes";
    request += "&tv1_sec=0";  // ignored
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
    XrdCl::XRootDStatus status =
        fs.Query(XrdCl::QueryCode::OpaqueFile, arg, response);

    if (response) {
      delete response;
    }

    int retc = !status.IsOK();

    verboseOutput("mtime", url.GetURL(), "updating timestamp",
                 std::to_string(mtime.tv_sec) + ":" + std::to_string(mtime.tv_nsec) +
                 (mDryRun ? " (dry-run)" : " retc: " + std::to_string(retc)));
    return retc;
  }
}

/**
 * Create a property list for copying a file
 * Sets up source and destination URLs with appropriate parameters
 * @param i File path relative to source
 * @param src Source base path
 * @param dst Destination base path
 * @param mtime Modification time to set on destination
 * @return PropertyList pointer for the copy operation
 */
XrdCl::PropertyList*
RClone::copyFile(const std::string& i, common::Path& src, common::Path& dst,
                 struct timespec mtime)
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
    params["eos.mtime"] = common::Timing::TimespecToString(mtime);
    durl.SetParams(params);
    dsturl = durl.GetURL();
  } else {
    XrdCl::URL durl(dsturl);
    XrdCl::URL::ParamsMap params;
    params["local.mtime"] = common::Timing::TimespecToString(mtime);
    durl.SetParams(params);
    dsturl = durl.GetURL();
  }

  props.Set("source", srcurl);
  props.Set("target", dsturl);
  props.Set("force", true); // allows overwrite
  result->Set("source", srcurl);
  result->Set("target", dsturl);

  verboseOutput("copy", dsturl, "copying file",
               "from: " + srcurl + (mDryRun ? " (dry-run)" : ""));

  mCopyProcess.AddJob(props, result);
  return result;
}

/**
 * Create a sparse file
 * Creates an empty file of the specified size
 * @param i File path relative to destination
 * @param dst Destination base path
 * @param mtime Modification time to set
 * @param size Size of the sparse file
 * @return 0 on success, error code on failure
 */
int
RClone::copySparse(const std::string& i, common::Path& dst,
                   struct timespec mtime, size_t size)
{
  std::string dstpath = std::string(dst.GetFullPath().c_str()) + i;

  verboseOutput("sparse-copy", dstpath, "creating sparse file",
               "size: " + std::to_string(size) + " bytes" + (mDryRun ? " (dry-run)" : ""));

  mode_t mode = S_IRUSR | S_IWUSR | S_IXUSR | S_IRGRP;
  auto fd = ::creat(dstpath.c_str(), mode);
  if (fd < 0) {
    std::cerr << "error: failed to create '" << dstpath.c_str() << "'" << std::endl;
    return -1;
  }
  if (ftruncate(fd, size)) {
    ::close(fd);
    std::cerr << "error: failed to truncate '" << dstpath.c_str() << "'"
              << std::endl;
    return -1;
  }

  struct timespec times[2];
  times[0] = mtime;
  times[1] = mtime;

  if (utimensat(0, dstpath.c_str(), times, AT_SYMLINK_NOFOLLOW)) {
    ::close(fd);
    std::cerr << "error: failed to update modification time of '"
              << dstpath.c_str() << "'" << std::endl;
    return -1;
  }
  ::close(fd);

  return 0;
}

/**
 * Print a summary of pending operations
 * Shows counts of files to create, delete, update, etc.
 */
void
RClone::printSummary()
{
  using namespace eos::mgm;

  // Title section
  TableFormatterBase title_table;
  title_table.AddString(operation_name);
  std::cout << title_table.GenerateTable(FULL) << std::endl;

  // Target section
  TableFormatterBase target_table;
  TableHeader stats_header = {
      HeadCell("Type", 15, "s"), HeadCell("Directories", 10, "s"),
      HeadCell("Files", 10, "s"), HeadCell("Links", 10, "s")};

  // target_table.AddString("TARGET");
  target_table.SetHeader(stats_header);
  TableData target_data;

  // Create row
  TableRow create_row;
  create_row.push_back(TableCell("Create", "", "", false, BGREEN));
  create_row.push_back(
      TableCell(std::to_string(target_create_dirs.size()), "", ""));
  create_row.push_back(
      TableCell(std::to_string(target_create_files.size()), "", ""));
  create_row.push_back(
      TableCell(std::to_string(target_create_links.size()), "", ""));
  target_data.push_back(create_row);

  // Delete row
  TableRow delete_row;
  delete_row.push_back(TableCell("Delete", "", "", false, BRED));
  delete_row.push_back(
      TableCell(std::to_string(target_delete_dirs.size()), "", ""));
  delete_row.push_back(
      TableCell(std::to_string(target_delete_files.size()), "", ""));
  delete_row.push_back(
      TableCell(std::to_string(target_delete_links.size()), "", ""));
  target_data.push_back(delete_row);

  // Update row
  TableRow update_row;
  update_row.push_back(TableCell("Update", "", "", false, BYELLOW));
  update_row.push_back(TableCell("-", "", ""));
  update_row.push_back(
      TableCell(std::to_string(target_updated_files.size()), "", ""));
  update_row.push_back(
      TableCell(std::to_string(target_updated_links.size()), "", ""));
  target_data.push_back(update_row);

  // Mismatch row
  TableRow mismatch_row;
  mismatch_row.push_back(TableCell("Mismatch", "", "", false, BRED));
  mismatch_row.push_back(TableCell("-", "", ""));
  mismatch_row.push_back(
      TableCell(std::to_string(target_mismatch_files.size()), "", ""));
  mismatch_row.push_back(
      TableCell(std::to_string(target_mismatch_links.size()), "", ""));
  target_data.push_back(mismatch_row);

  target_table.AddRows(target_data);
  std::cout << target_table.GenerateTable(FULL) << std::endl;

  // Volume section
  TableFormatterBase volume_table;
  TableHeader volume_header = {HeadCell("Metric", 20, "s"),
                               HeadCell("Value", 20, "s")};

  // volume_table.AddString("VOLUME");
  volume_table.SetHeader(volume_header);
  TableData volume_data;
  XrdOucString sizestring;

  // Original size
  eos::common::StringConversion::GetReadableSizeString(sizestring, origSize_,
                                                       "B");
  TableRow orig_size_row;
  orig_size_row.push_back(TableCell("Original Size", "", ""));
  orig_size_row.push_back(TableCell(sizestring.c_str(), "", ""));
  volume_data.push_back(orig_size_row);

  // Original transactions
  eos::common::StringConversion::GetReadableSizeString(sizestring,
                                                       origTransactions_, "");
  TableRow orig_trans_row;
  orig_trans_row.push_back(TableCell("Original Transactions", "", ""));
  orig_trans_row.push_back(TableCell(sizestring.c_str(), "", ""));
  volume_data.push_back(orig_trans_row);

  // Copy size
  eos::common::StringConversion::GetReadableSizeString(sizestring, copySize_,
                                                       "B");
  TableRow copy_size_row;
  copy_size_row.push_back(TableCell("Copy Size", "", ""));
  copy_size_row.push_back(TableCell(sizestring.c_str(), "", ""));
  volume_data.push_back(copy_size_row);

  // Copy transactions
  eos::common::StringConversion::GetReadableSizeString(sizestring,
                                                       copyTransactions_, "");
  TableRow copy_trans_row;
  copy_trans_row.push_back(TableCell("Copy Transactions", "", ""));
  copy_trans_row.push_back(TableCell(sizestring.c_str(), "", ""));
  volume_data.push_back(copy_trans_row);

  volume_table.AddRows(volume_data);
  std::cout << volume_table.GenerateTable(FULL) << std::endl;
}

/**
 * Print the final summary after operations complete
 */
void
RClone::printFinalSummary()
{
}

void RClone::verboseOutput(const std::string& operation, const std::string& path, const std::string& reason, const std::string& extra) const {
  if (!mSilent && mVerbose) {
    std::cout << "\033[1m[ " << std::left << std::setw(20) << operation << " ]\033[0m "
              << "path: " << std::left << std::setw(40) << path
              << " reason: " << reason
              << (extra.empty() ? "" : " " + extra)
              << std::endl;
  }
}

} // namespace console
} // namespace eos
