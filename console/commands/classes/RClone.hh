// ----------------------------------------------------------------------
// File: RClone.hh
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

#ifndef EOS_CONSOLE_RCLONE_HH
#define EOS_CONSOLE_RCLONE_HH

#include "common/LayoutId.hh"
#include "common/Path.hh"
#include "common/StringTokenizer.hh"
#include "common/Timing.hh"
#include "common/CopyProcess.hh"
#include "console/ConsoleMain.hh"
#include "console/commands/helpers/NewfindHelper.hh"
#include "XrdOuc/XrdOucString.hh"

#include <XrdCl/XrdClCopyProcess.hh>
#include <XrdCl/XrdClFileSystem.hh>
#include <XrdCl/XrdClPropertyList.hh>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <optional>
#include <string>
#include <map>
#include <set>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <atomic>

namespace eos {
namespace console {

/**
 * @brief Structure to hold file system entry information
 */
struct FSEntry {
  struct timespec mtime;
  size_t size;
  std::string type;
  std::string target;

  /**
   * @brief Compare modification times
   * @param cmptime Time to compare against
   * @param debug Whether to print debug output
   * @param lowres Whether to use low resolution comparison (seconds only)
   * @return true if this entry is newer than cmptime
   */
  bool newer(const struct timespec& cmptime, bool debug, bool lowres = false) const;
};

/**
 * @brief Structure to hold file system scan results
 */
struct FSResult {
  std::map<std::string, FSEntry> directories;
  std::map<std::string, FSEntry> files;
  std::map<std::string, FSEntry> links;
};

class RClone;  // Forward declaration

class RCloneProgressHandler : public XrdCl::CopyProgressHandler {
public:
  RCloneProgressHandler(RClone* parent) : s_sp(0), mParent(parent) {}

  virtual void BeginJob(uint16_t jobNum, uint16_t jobTotal, const XrdCl::URL* source,
                       const XrdCl::URL* destination) override;

  virtual void EndJob(uint16_t jobNum, const XrdCl::PropertyList* result) override;

  virtual void JobProgress(uint16_t jobNum, uint64_t bytesProcessed, uint64_t bytesTotal) override;

  virtual bool ShouldCancel(uint16_t jobNum) override;

  std::atomic<uint64_t> bp;
  std::atomic<uint64_t> bt;
  std::atomic<uint16_t> n;
  std::atomic<uint16_t> tot;
  std::atomic<uint64_t> s_sp;

private:
  RClone* mParent;
};

/**
 * @brief Class to handle rclone operations between EOS and local filesystem
 */
class RClone {
  friend class RCloneProgressHandler;  // Add friend declaration

public:
  /**
   * @brief Constructor
   */
  RClone()
      : mDryRun(false), mNoReplace(false), mNoDelete(true), mVerbose(false),
        mSilent(false), mFilterVersions(true), mFilterAtomic(true),
        mFilterHidden(true), mDebug(false), mLowRes(false), mMakeSparse(std::numeric_limits<size_t>::max()),
        mCopyProgress(this)
  {
    operation_name = "\033[1mEOS Remote Sync Tool\033[0m \033[31m(copy)\033[0m";
  }

  /**
   * @brief Set dry run mode
   * @param value True to enable dry run mode
   */
  void setDryRun(bool value) { mDryRun = value; }

  /**
   * @brief Set no replace mode
   * @param value True to enable no replace mode
   */
  void setNoReplace(bool value) { mNoReplace = value; }

  /**
   * @brief Set no delete mode
   * @param value True to enable no delete mode
   */
  void setNoDelete(bool value) { mNoDelete = value; }

  /**
   * @brief Set verbose mode
   * @param value True to enable verbose mode
   */
  void setVerbose(bool value) { mVerbose = value; }

  /**
   * @brief Set silent mode
   * @param value True to enable silent mode
   */
  void setSilent(bool value) { mSilent = value; }

  /**
   * @brief Set filter versions
   * @param value True to enable version filtering
   */
  void setFilterVersions(bool value) { mFilterVersions = value; }

  /**
   * @brief Set filter atomic
   * @param value True to enable atomic filtering
   */
  void setFilterAtomic(bool value) { mFilterAtomic = value; }

  /**
   * @brief Set filter hidden
   * @param value True to enable hidden filtering
   */
  void setFilterHidden(bool value) { mFilterHidden = value; }

  /**
   * @brief Set sparse file size threshold
   * @param value Size in bytes above which files are treated as sparse
   */
  void setMakeSparse(size_t value) { mMakeSparse = value; }

  /**
   * @brief Set sparse files dump path
   * @param value Path where to dump sparse files list
   */
  void setSparseFilesDump(const std::string& value) { mSparseFilesDump = value; }

  /**
   * @brief Set debug mode
   * @param debug True to enable debug mode
   */
  void setDebug(bool debug) { mDebug = debug; }

  /**
   * @brief Set low resolution mode for timestamp comparison
   * @param value True to enable low resolution mode (seconds only)
   */
  void setLowRes(bool value) { mLowRes = value; }

  /**
   * @brief Set copy parallelism
   * @param value Number of parallel streams to use for copying
   */
  void setCopyParallelism(size_t value) { mCopyParallelism = value; }

  /**
   * @brief Get verbose mode
   * @return True if verbose mode is enabled
   */
  bool isVerbose() const { return mVerbose; }

  /**
   * @brief Get silent mode
   * @return True if silent mode is enabled
   */
  bool isSilent() const { return mSilent; }

  /**
   * @brief Get debug mode
   * @return True if debug mode is enabled
   */
  bool isDebug() const { return mDebug; }

  /**
   * @brief Get low resolution mode
   * @return True if low resolution mode is enabled
   */
  bool isLowRes() const { return mLowRes; }

  /**
   * @brief Copy from source to destination
   * @param src Source path
   * @param dst Destination path
   * @return 0 on success, error code otherwise
   */
  int copy(const std::string& src, const std::string& dst);

  /**
   * @brief Sync between two directories
   * @param dir1 First directory
   * @param dir2 Second directory
   * @return 0 on success, error code otherwise
   */
  int sync(const std::string& dir1, const std::string& dir2);

  /**
   * @brief Print summary of pending operations
   */
  void printSummary();

  /**
   * @brief Print final summary after operations complete
   */
  void printFinalSummary();

private:
  /**
   * @brief Helper function for consistent verbose output formatting
   * @param operation The operation being performed (mkdir, copy, etc.)
   * @param path The path being operated on
   * @param reason The reason for the operation
   * @param extra Any additional information (optional)
   */
  void verboseOutput(const std::string& operation, const std::string& path, 
                    const std::string& reason, const std::string& extra = "") const;

  /**
   * @brief Scan filesystem and return results
   * @param path Path to scan
   * @return FSResult containing scan results
   */
  FSResult fsFind(const char* path) const;

  /**
   * @brief Scan EOS filesystem and return results
   * @param path Path to scan
   * @return FSResult containing scan results
   */
  FSResult eosFind(const char* path) const;

  /**
   * @brief Copy a file
   * @param path File path
   * @param src Source path
   * @param dst Destination path
   * @param mtime Modification time to set
   * @return Property list for the copy operation
   */
  XrdCl::PropertyList* copyFile(const std::string& path, 
                               common::Path& src,
                               common::Path& dst,
                               struct timespec mtime);

  /**
   * @brief Create a directory
   * @param path Directory path
   * @param prefix Path prefix
   * @return 0 on success, error code otherwise
   */
  int createDir(const std::string& path, common::Path& prefix);

  /**
   * @brief Remove a directory
   * @param path Directory path
   * @param prefix Path prefix
   * @return 0 on success, error code otherwise
   */
  int removeDir(const std::string& path, common::Path& prefix);

  /**
   * @brief Create a symbolic link
   * @param path Link path
   * @param src Source path
   * @param target Link target
   * @param dst Destination path
   * @param mtime Modification time to set
   * @return 0 on success, error code otherwise
   */
  int createLink(const std::string& path,
                common::Path& src,
                const std::string& target,
                common::Path& dst,
                struct timespec mtime);

  /**
   * @brief Remove a file
   * @param path File path
   * @param prefix Path prefix
   * @return 0 on success, error code otherwise
   */
  int removeFile(const std::string& path, common::Path& prefix);

  /**
   * @brief Set directory modification time
   * @param path Directory path
   * @param prefix Path prefix
   * @param mtime Modification time to set
   * @return 0 on success, error code otherwise
   */
  int setDirMtime(const std::string& path,
                  common::Path& prefix,
                  struct timespec mtime);

  /**
   * @brief Copy a sparse file
   * @param path File path
   * @param dst Destination path
   * @param mtime Modification time to set
   * @param size File size
   * @return 0 on success, error code otherwise
   */
  int copySparse(const std::string& path,
                 common::Path& dst,
                 struct timespec mtime,
                 size_t size);

  // Helper function to gather source and destination file maps
  std::pair<FSResult, FSResult> gatherFileMaps(const eos::common::Path& srcPath, const eos::common::Path& dstPath) const;

  // Helper function to analyze directory differences
  void analyzeDirectories(const FSResult& srcmap, const FSResult& dstmap);

  // Helper function to analyze file differences
  void analyzeFiles(const FSResult& srcmap, const FSResult& dstmap);

  // Helper function to analyze symbolic links
  void analyzeLinks(const FSResult& srcmap, const FSResult& dstmap);

  // Helper function to handle deletions
  void handleDeletions(const FSResult& srcmap, const FSResult& dstmap);

  // Helper function to execute file operations
  int executeFileOperations(eos::common::Path& srcPath, eos::common::Path& dstPath, 
                          const FSResult& srcmap, bool ignore_errors);

private:
  bool mDryRun;
  bool mNoReplace;
  bool mNoDelete;
  bool mVerbose;
  bool mSilent;
  bool mFilterVersions;
  bool mFilterAtomic;
  bool mFilterHidden;
  bool mDebug;
  bool mLowRes;  // New: controls whether to use low resolution time comparison
  bool mIsDryRunSecondPass{false};  // Track if we're in the second pass of a dry run sync
  mutable size_t mMakeSparse;
  size_t mCopyParallelism{1};  // Default to 1 parallel stream
  std::string mSparseFilesDump;
  std::string mSparseFilesList;
  eos::common::CopyProcess mCopyProcess;
  RCloneProgressHandler mCopyProgress;

  // Target operation sets
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

  // Dry run deletion tracking
  mutable std::set<std::string> mDryRunDeletedDirs;   // Track directories deleted in first pass
  mutable std::set<std::string> mDryRunDeletedFiles;  // Track files deleted in first pass
  mutable std::set<std::string> mDryRunDeletedLinks;  // Track links deleted in first pass

  // Volume statistics
  uint64_t origSize_{0};
  uint64_t origTransactions_{0};
  uint64_t copySize_{0};
  uint64_t copyTransactions_{0};

  std::string operation_name;
};

} // namespace console
} // namespace eos

#endif // EOS_CONSOLE_RCLONE_HH 