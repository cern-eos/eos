/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

//------------------------------------------------------------------------------
//! @author Georgios Bitzes <georgios.bitzes@cern.ch>
//! @brief Class for inspecting namespace contents - talks directly to QDB
//------------------------------------------------------------------------------

#pragma once
#include "namespace/Namespace.hh"
#include "namespace/ns_quarkdb/persistency/RequestBuilder.hh"
#include "proto/ContainerMd.pb.h"
#include <string>
#include <map>
#include <vector>
#include <set>

namespace qclient
{
class QClient;
}

EOSNSNAMESPACE_BEGIN

class ContainerScanner;
class FileScanner;
class OutputSink;
class FileMetadataFilter;

struct CacheNotifications {
  CacheNotifications() {}

  std::vector<uint64_t> fids;
  std::vector<uint64_t> cids;
};

//------------------------------------------------------------------------------
//! Inspector class
//------------------------------------------------------------------------------
class Inspector
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  Inspector(qclient::QClient& qcl, OutputSink& sink);

  //----------------------------------------------------------------------------
  //! Is the connection to QDB ok? If not, pointless to run anything else.
  //----------------------------------------------------------------------------
  bool checkConnection(std::string& err);

  //----------------------------------------------------------------------------
  //! Dump contents of the given path. ERRNO-like integer return value, 0
  //! means no error.
  //----------------------------------------------------------------------------
  int dump(const std::string& path, bool relative, bool rawPaths, bool noDirs,
           bool noFiles, bool showSize, bool showMtime, const std::string& attrQuery,
           std::ostream& out);

  //----------------------------------------------------------------------------
  //! Scan contents of the given path.
  //----------------------------------------------------------------------------
  int scan(const std::string& path, bool relative, bool rawPaths, bool noDirs,
           bool noFiles);

  //----------------------------------------------------------------------------
  //! Scan all directories in the namespace, and print out some information
  //! about each one. (even potentially unreachable directories)
  //----------------------------------------------------------------------------
  int scanDirs(bool onlyNoAttrs, bool fullPaths, bool countContents,
               size_t countThreshold);

  //----------------------------------------------------------------------------
  //! Scan all file metadata in the namespace, and print out some information
  //! about each one. (even potentially unreachable directories)
  //----------------------------------------------------------------------------
  int scanFileMetadata(bool onlySizes, bool fullPaths, bool onlyUnknownFsids);

  //----------------------------------------------------------------------------
  //! Scan all deathrow entries
  //----------------------------------------------------------------------------
  int scanDeathrow(std::ostream& out, std::ostream& err);

  //----------------------------------------------------------------------------
  //! Forcefully overwrite the given ContainerMD - USE WITH CAUTION
  //----------------------------------------------------------------------------
  int overwriteContainerMD(bool dryRun, uint64_t id, uint64_t parentId,
                           const std::string& name, std::ostream& out, std::ostream& err);

  //----------------------------------------------------------------------------
  //! Check intra-container conflicts, such as a container having two entries
  //! with the name name.
  //----------------------------------------------------------------------------
  int checkNamingConflicts(bool onePerLine, std::ostream& out, std::ostream& err);

  //----------------------------------------------------------------------------
  //! Search for files / containers with cursed names
  //----------------------------------------------------------------------------
  int checkCursedNames(std::ostream& out, std::ostream& err);

  //----------------------------------------------------------------------------
  //! Search for holes in FsView: Items which should be in FsView according to
  //! FMD locations / unlinked locations, but are not there.
  //----------------------------------------------------------------------------
  int checkFsViewMissing(std::ostream& out, std::ostream& err);

  //----------------------------------------------------------------------------
  //! Search for elements which are present in FsView, but not FMD locations
  //----------------------------------------------------------------------------
  int checkFsViewExtra(std::ostream& out, std::ostream& err);

  //----------------------------------------------------------------------------
  //! Search for shadow directories
  //----------------------------------------------------------------------------
  int checkShadowDirectories(std::ostream& out, std::ostream& err);

  //----------------------------------------------------------------------------
  //! Check for corrupted ...eos.ino... hardlink-simulation files
  //----------------------------------------------------------------------------
  int checkSimulatedHardlinks(std::ostream& out, std::ostream& err);

  //----------------------------------------------------------------------------
  // Find files with layout = 1 replica
  //----------------------------------------------------------------------------
  int oneReplicaLayout(bool showName, bool showPaths, bool filterInternal,
                       std::ostream& out, std::ostream& err);

  //----------------------------------------------------------------------------
  //! Find files with non-nominal number of stripes (replicas)
  //----------------------------------------------------------------------------
  int stripediff(bool printTime, std::ostream& out, std::ostream& err);

  //----------------------------------------------------------------------------
  //! Find orphan files and orphan directories
  //----------------------------------------------------------------------------
  int checkOrphans(std::ostream& out, std::ostream& err);

  //----------------------------------------------------------------------------
  //! Fix detached parent
  //----------------------------------------------------------------------------
  int fixDetachedParentContainer(bool dryRun, uint64_t cid,
                                 const std::string& destinationPath, std::ostream& out, std::ostream& err);
  int fixDetachedParentFile(bool dryRun, uint64_t fid,
                            const std::string& destinationPath, std::ostream& out, std::ostream& err);

  //------------------------------------------------------------------------------
  //! Fix naming conflict
  //------------------------------------------------------------------------------
  int fixShadowFile(bool dryRun, uint64_t fid, const std::string& destinationPath,
                    std::ostream& out, std::ostream& err);

  //----------------------------------------------------------------------------
  //! Drop file currently stuck in deathrow. Please note that any pending
  //! replicas, if they exist, are not deleted.
  //----------------------------------------------------------------------------
  int dropFromDeathrow(bool dryRun, uint64_t fid, std::ostream& out,
                       std::ostream& err);

  //------------------------------------------------------------------------------
  //! Drop empty container
  //------------------------------------------------------------------------------
  int dropEmptyCid(bool dryRun, uint64_t cid);

  //----------------------------------------------------------------------------
  //! Change the given fid - USE WITH CAUTION
  //----------------------------------------------------------------------------
  int changeFid(bool dryRun, uint64_t id, uint64_t newParent,
                const std::string& newChecksum, int64_t newSize,
                uint64_t newLayoutId, std::ostream& out, std::ostream& err);

  //----------------------------------------------------------------------------
  //! Rename the given cid fully, taking care of the container maps as well
  //----------------------------------------------------------------------------
  int renameCid(bool dryRun, uint64_t cid, uint64_t newParent,
                const std::string& newName, std::ostream& out, std::ostream& err);

  //----------------------------------------------------------------------------
  //! Rename the given fid fully, taking care of the container maps as well
  //----------------------------------------------------------------------------
  int renameFid(bool dryRun, uint64_t id, uint64_t newParent,
                const std::string& newName, std::ostream& out, std::ostream& err);

  //----------------------------------------------------------------------------
  //! Print out _everything_ known about the given file.
  //----------------------------------------------------------------------------
  int printFileMD(uint64_t fid, bool withParents, std::ostream& out,
                  std::ostream& err);

  //----------------------------------------------------------------------------
  //! Print out _everything_ known about the given directory.
  //----------------------------------------------------------------------------
  int printContainerMD(uint64_t cid, bool withParents, std::ostream& out,
                       std::ostream& err);

  //----------------------------------------------------------------------------
  //! Load configuration
  //----------------------------------------------------------------------------
  bool loadConfiguration();

  //----------------------------------------------------------------------------
  //! Activate the given metadata filter
  //----------------------------------------------------------------------------
  void setMetadataFilter(std::unique_ptr<FileMetadataFilter> filter);

private:
  std::map<std::string, std::string> mgmConfiguration;
  std::set<int64_t> validFsIds;

  qclient::QClient& mQcl;
  OutputSink& mOutputSink;

  std::unique_ptr<FileMetadataFilter> mMetadataFilter;

  //----------------------------------------------------------------------------
  //! Check if given path is a good choice as a destination for repaired
  //! files / containers
  //----------------------------------------------------------------------------
  bool isDestinationPathSane(const std::string& path, ContainerIdentifier& cid,
                             std::ostream& out);

  //----------------------------------------------------------------------------
  //! Run the given write batch towards QDB - print the requests, as well as
  //! the output.
  //----------------------------------------------------------------------------
  void executeRequestBatch(const std::vector<RedisRequest>& requestBatch,
                           const CacheNotifications& notif, bool dryRun, std::ostream& out,
                           std::ostream& err);

};

EOSNSNAMESPACE_END
