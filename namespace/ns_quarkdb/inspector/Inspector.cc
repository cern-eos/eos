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

#include "namespace/ns_quarkdb/inspector/Inspector.hh"
#include "namespace/ns_quarkdb/explorer/NamespaceExplorer.hh"
#include "namespace/ns_quarkdb/persistency/MetadataFetcher.hh"
#include "namespace/ns_quarkdb/inspector/ContainerScanner.hh"
#include "namespace/ns_quarkdb/inspector/FileScanner.hh"
#include "common/IntervalStopwatch.hh"
#include <folly/executors/IOThreadPoolExecutor.h>
#include <qclient/QClient.hh>
#include <google/protobuf/util/json_util.h>

#define DBG(message) std::cerr << __FILE__ << ":" << __LINE__ << " -- " << #message << " = " << message << std::endl

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
Inspector::Inspector(qclient::QClient& qcl) : mQcl(qcl) { }

//------------------------------------------------------------------------------
// Is the connection to QDB ok? If not, pointless to run anything else.
//------------------------------------------------------------------------------
bool Inspector::checkConnection(std::string& err)
{
  qclient::redisReplyPtr reply = mQcl.exec("PING").get();

  if (!reply) {
    err = "Could not connect to the given QDB cluster";
    return false;
  }

  if (reply->type != REDIS_REPLY_STATUS ||
      std::string(reply->str, reply->len) != "PONG") {
    err = SSTR("Received unexpected response in checkConnection: " <<
               qclient::describeRedisReply(reply));
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Dump contents of the given path. ERRNO-like integer return value, 0
// means no error.
//------------------------------------------------------------------------------
int Inspector::dump(const std::string& dumpPath, std::ostream& out)
{
  ExplorationOptions explorerOpts;
  std::unique_ptr<folly::Executor> executor(new folly::IOThreadPoolExecutor(4));
  NamespaceExplorer explorer(dumpPath, explorerOpts, mQcl, executor.get());
  NamespaceItem item;

  while (explorer.fetch(item)) {
    out << "path=" << item.fullPath << std::endl;
  }

  return 0;
}

//----------------------------------------------------------------------------
// Check naming conflicts, only for containers, and only for the given
// parent ID.
//----------------------------------------------------------------------------
void Inspector::checkContainerConflicts(uint64_t parentContainer,
                                        std::map<std::string, uint64_t>& containerMap,
                                        ContainerScanner& scanner,
                                        std::ostream& out, std::ostream& err)
{
  containerMap.clear();
  eos::ns::ContainerMdProto proto;

  for (; scanner.valid(); scanner.next()) {
    if (!scanner.getItem(proto)) {
      break;
    }

    if (parentContainer != proto.parent_id()) {
      break;
    }

    if (proto.name() == "." || proto.name() == "..") {
      out << "Container " << proto.id() << " has cursed name: '" << proto.name() <<
          "'" << std::endl;
    }

    auto conflict = containerMap.find(proto.name());

    if (conflict != containerMap.end()) {
      out << "Detected conflict for '" << proto.name() << "' in container " <<
          parentContainer << ", between containers " << conflict->second << " and " <<
          proto.id() << std::endl;
    }

    containerMap[proto.name()] = proto.id();
  }
}

//----------------------------------------------------------------------------
// Check naming conflicts, only for files, and only for the given
// parent ID.
//----------------------------------------------------------------------------
void Inspector::checkFileConflicts(uint64_t parentContainer,
                                   std::map<std::string, uint64_t>& fileMap,
                                   FileScanner& scanner,
                                   std::ostream& out, std::ostream& err)
{
  fileMap.clear();
  eos::ns::FileMdProto proto;

  for (; scanner.valid(); scanner.next()) {
    if (!scanner.getItem(proto)) {
      break;
    }

    if (parentContainer != proto.cont_id()) {
      break;
    }

    if (proto.name() == "." || proto.name() == "..") {
      out << "File " << proto.id() << " has cursed name: '" << proto.name() << "'" <<
          std::endl;
    }

    auto conflict = fileMap.find(proto.name());

    if (conflict != fileMap.end()) {
      out << "Detected conflict for '" << proto.name() << "' in container " <<
          parentContainer << ", betewen files " << conflict->second << " and " <<
          proto.id() << std::endl;
    }

    fileMap[proto.name()] = proto.id();
  }
}


//------------------------------------------------------------------------------
// Check if there's naming conflicts between files and containers.
//------------------------------------------------------------------------------
void Inspector::checkDifferentMaps(const std::map<std::string, uint64_t>&
                                   containerMap,
                                   const std::map<std::string, uint64_t>& fileMap, uint64_t parentContainer,
                                   std::ostream& out)
{
  for (auto it = containerMap.begin(); it != containerMap.end(); it++) {
    auto conflict = fileMap.find(it->first);

    if (conflict != fileMap.end()) {
      out << "Detected conflict for '" << conflict->first << "' in container " <<
          parentContainer << ", between container " << it->second << " and file " <<
          conflict->second << std::endl;
    }
  }
}

//------------------------------------------------------------------------------
// Check intra-container conflicts, such as a container having two entries
// with the name name.
//------------------------------------------------------------------------------
int Inspector::checkNamingConflicts(std::ostream& out, std::ostream& err)
{
  ContainerScanner containerScanner(mQcl);
  FileScanner fileScanner(mQcl);
  common::IntervalStopwatch stopwatch(std::chrono::seconds(10));

  while (containerScanner.valid()) {
    eos::ns::ContainerMdProto proto;

    if (!containerScanner.getItem(proto)) {
      break;
    }

    std::map<std::string, uint64_t> containerMap;
    checkContainerConflicts(proto.parent_id(), containerMap, containerScanner, out,
                            err);
    eos::ns::FileMdProto fileProto;

    if (!fileScanner.getItem(fileProto)) {
      break;
    }

    //--------------------------------------------------------------------------
    // Bring file scanner at-least-or-after our current parent container, while
    // checking for file conflicts in the way
    //--------------------------------------------------------------------------
    while (proto.parent_id() > fileProto.cont_id()) {
      std::map<std::string, uint64_t> fileMap;
      checkFileConflicts(fileProto.cont_id(), fileMap, fileScanner, out, err);
      fileScanner.next();

      if (!fileScanner.getItem(fileProto)) {
        goto out;
      }
    }

    //--------------------------------------------------------------------------
    // Check for conflicts between files and containers
    //--------------------------------------------------------------------------
    if (proto.parent_id() == fileProto.cont_id()) {
      std::map<std::string, uint64_t> fileMap;
      checkFileConflicts(fileProto.cont_id(), fileMap, fileScanner, out, err);
      checkDifferentMaps(containerMap, fileMap, fileProto.cont_id(), out);
    }

    if (stopwatch.restartIfExpired()) {
      err << "Progress: Processed " << containerScanner.getScannedSoFar() <<
          " containers, " << fileScanner.getScannedSoFar() << " files" << std::endl;
    }
  }

out:
  return 0;
}

//------------------------------------------------------------------------------
// Print out _everything_ known about the given file.
//------------------------------------------------------------------------------
int Inspector::printFileMD(uint64_t fid, std::ostream& out, std::ostream& err)
{
  folly::Future<eos::ns::FileMdProto> fut = MetadataFetcher::getFileFromId(mQcl,
      FileIdentifier(fid));
  eos::ns::FileMdProto val;

  try {
    val = fut.get();
  } catch (const MDException& e) {
    err << "Error while fetching metadata for FileMD #" << fid << ": " << e.what()
        << std::endl;
    return 1;
  }

  google::protobuf::util::JsonPrintOptions opts;
  opts.add_whitespace = true;
  std::string jsonString;
  google::protobuf::util::MessageToJsonString(val, &jsonString, opts);
  err << jsonString << std::endl;
  return 0;
}

EOSNSNAMESPACE_END
