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
#include "namespace/ns_quarkdb/inspector/Printing.hh"
#include "namespace/ns_quarkdb/FileMD.hh"
#include "namespace/ns_quarkdb/ContainerMD.hh"
#include "namespace/ns_quarkdb/persistency/RequestBuilder.hh"
#include "namespace/ns_quarkdb/persistency/FileSystemIterator.hh"
#include "namespace/ns_quarkdb/accounting/FileSystemHandler.hh"
#include "namespace/ns_quarkdb/Constants.hh"
#include "namespace/utils/Checksum.hh"
#include "namespace/Constants.hh"
#include "common/LayoutId.hh"
#include "common/IntervalStopwatch.hh"
#include "common/InodeTranslator.hh"
#include "common/ParseUtils.hh"
#include "common/StringUtils.hh"
#include <folly/executors/IOThreadPoolExecutor.h>
#include <qclient/QClient.hh>
#include <google/protobuf/util/json_util.h>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Escape non-printable string
//------------------------------------------------------------------------------
static std::string escapeNonPrintable(const std::string &str) {
  std::stringstream ss;

  for(size_t i = 0; i < str.size(); i++) {
    if(isprint(str[i])) {
      ss << str[i];
    }
    else if(str[i] == '\0') {
      ss << "\\x00";
    }
    else {
      char buff[16];
      snprintf(buff, 16, "\\x%02X", (unsigned char) str[i]);
      ss << buff;
    }
  }
  return ss.str();
}

//------------------------------------------------------------------------------
// Turn bool to yes / no
//------------------------------------------------------------------------------
static std::string toYesOrNo(bool val) {
  if(val) {
    return "Yes";
  }

  return "No";
}

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
int Inspector::dump(const std::string& dumpPath, bool relative, bool rawPaths, bool noDirs, bool showSize, bool showMtime, std::ostream& out)
{
  ExplorationOptions explorerOpts;
  std::unique_ptr<folly::Executor> executor(new folly::IOThreadPoolExecutor(4));
  NamespaceExplorer explorer(dumpPath, explorerOpts, mQcl, executor.get());
  NamespaceItem item;

  while (explorer.fetch(item)) {
    if(noDirs && !item.isFile) {
      continue;
    }

    if(!rawPaths) {
      out << "path=";
    }

    if(relative) {
      out << item.fullPath.substr(dumpPath.size());
    }
    else {
      out << item.fullPath;
    }

    if(showSize && item.isFile) {
      out << " size=" << item.fileMd.size();
    }

    if(showMtime && item.isFile) {
      out << " mtime=" << Printing::timespecToTimestamp(Printing::parseTimespec(item.fileMd.mtime()));
    }

    out << std::endl;
  }

  return 0;
}

//------------------------------------------------------------------------------
// Fetch path or name from a combination of ContainerMdProto +
// ContainerScanner::Item, return as much information as is available
//------------------------------------------------------------------------------
std::string fetchNameOrPath(const eos::ns::ContainerMdProto &proto, ContainerScanner::Item &item) {
  item.fullPath.wait();
  if(item.fullPath.hasException()) {
    return proto.name();
  }

  std::string fullPath = item.fullPath.get();

  if(fullPath.empty()) {
    return proto.name();
  }

  return fullPath;
}

//------------------------------------------------------------------------------
// Get count as string
//------------------------------------------------------------------------------
std::string countAsString(folly::Future<uint64_t> &fut) {
  fut.wait();

  if(fut.hasException()) {
    return "N/A";
  }

  uint64_t val = fut.get();
  fut = val;

  return SSTR(val);
}

//------------------------------------------------------------------------------
// Safe uint64_t get, without exceptions. Return 0 in case of exception.
//------------------------------------------------------------------------------
uint64_t safeGet(folly::Future<uint64_t> &fut) {
  fut.wait();

  if(fut.hasException()) {
    return 0;
  }

  uint64_t val = fut.get();
  fut = val;
  return val;
}

//------------------------------------------------------------------------------
// Scan all directories in the namespace, and print out some information
// about each one. (even potentially unreachable directories)
//------------------------------------------------------------------------------
int Inspector::scanDirs(bool onlyNoAttrs, bool fullPaths, bool countContents, size_t countThreshold, std::ostream &out, std::ostream &err) {
  if(countThreshold > 0) {
    countContents = true;
  }

  ContainerScanner containerScanner(mQcl, fullPaths, countContents);

  while(containerScanner.valid()) {
    eos::ns::ContainerMdProto proto;
    ContainerScanner::Item item;

    if (!containerScanner.getItem(proto, &item)) {
      break;
    }

    if(onlyNoAttrs && !proto.xattrs().empty()) {
      containerScanner.next();
      continue;
    }

    if(countThreshold > 0 && (safeGet(item.fileCount) + safeGet(item.containerCount)) < countThreshold) {
      containerScanner.next();
      continue;
    }

    out << "cid=" << proto.id() << " name=" << fetchNameOrPath(proto, item) << " parent=" << proto.parent_id() << " uid=" << proto.uid() << " mode=" << proto.mode();

    if(countContents) {
      out << " file-count=" << countAsString(item.fileCount);
      out << " container-count=" << countAsString(item.containerCount);
    }

    out << std::endl;
    containerScanner.next();
  }

  std::string errorString;
  if(containerScanner.hasError(errorString)) {
    err << errorString;
    return 1;
  }

  return 0;
}

//------------------------------------------------------------------------------
// Fetch path or name from a combination of FileMdProto +
// FileScanner::Item, return as much information as is available
//------------------------------------------------------------------------------
std::string fetchNameOrPath(const eos::ns::FileMdProto &proto, FileScanner::Item &item) {
  item.fullPath.wait();
  if(item.fullPath.hasException()) {
    return proto.name();
  }

  std::string fullPath = item.fullPath.get();

  if(fullPath.empty()) {
    return proto.name();
  }

  return SSTR(fullPath << proto.name());
}

//------------------------------------------------------------------------------
// Scan all file metadata in the namespace, and print out some information
// about each one. (even potentially unreachable ones)
//------------------------------------------------------------------------------
int Inspector::scanFileMetadata(bool onlySizes, bool fullPaths, std::ostream &out, std::ostream &err) {
  FileScanner fileScanner(mQcl, fullPaths);

  while(fileScanner.valid()) {
    FileScanner::Item item;
    eos::ns::FileMdProto proto;

    if (!fileScanner.getItem(proto, &item)) {
      break;
    }

    if(!onlySizes) {
      std::string xs;
      eos::appendChecksumOnStringProtobuf(proto, xs);
      out << "fid=" << proto.id() << " name=" << fetchNameOrPath(proto, item) << " pid=" << proto.cont_id() << " uid=" << proto.uid() << " size=" << proto.size() << " xs=" << xs << std::endl;
    }
    else {
      out << proto.size() << std::endl;
    }

    fileScanner.next();
  }

  std::string errorString;
  if(fileScanner.hasError(errorString)) {
    err << errorString;
    return 1;
  }

  return 0;
}

//------------------------------------------------------------------------------
// Forcefully overwrite the given ContainerMD - USE WITH CAUTION
//------------------------------------------------------------------------------
int Inspector::overwriteContainerMD(bool dryRun, uint64_t id, uint64_t parentId, const std::string &name, std::ostream &out, std::ostream &err) {
  eos::ns::ContainerMdProto val;

  val.set_id(id);
  val.set_parent_id(parentId);
  val.set_name(name);

  QuarkContainerMD containerMD;
  containerMD.initialize(std::move(val), IContainerMD::FileMap(), IContainerMD::ContainerMap() );

  std::vector<RedisRequest> requests;
  requests.emplace_back(RequestBuilder::writeContainerProto(&containerMD));
  executeRequestBatch(requests, {}, dryRun, out, err);
  return 0;
}

//------------------------------------------------------------------------------
// Serialize locations vector
//------------------------------------------------------------------------------
template<typename T>
static std::string serializeLocations(const T& vec) {
  std::ostringstream stream;

  for(int i = 0; i < vec.size(); i++) {
    stream << vec[i];
    if(i != vec.size() - 1) {
      stream << ",";
    }
  }

  return stream.str();
}

//------------------------------------------------------------------------------
// Check if we should print based on name and internal filter
//------------------------------------------------------------------------------
bool shouldPrint(bool filterInternal, const std::string &fullPath) {
  if(!filterInternal) {
    return true;
  }

  //----------------------------------------------------------------------------
  // Filter out aborted atomic uploads..
  //----------------------------------------------------------------------------
  if(fullPath.find("/.sys.a#.") != std::string::npos) {
    return false;
  }

  //----------------------------------------------------------------------------
  // Filter out files under proc..
  //----------------------------------------------------------------------------
  if(common::startsWith(fullPath, "/eos/")) {
    std::string chopped = std::string(fullPath.c_str()+5, fullPath.size()-5);
    size_t nextSlash = chopped.find("/");

    if(nextSlash != std::string::npos) {
      chopped = std::string(chopped.c_str()+nextSlash, chopped.size()-nextSlash);

      if(common::startsWith(chopped, "/proc/")) {
        return false;
      }
    }
  }

  //----------------------------------------------------------------------------
  // Filter out versioned files..
  //----------------------------------------------------------------------------
  if(fullPath.find("/.sys.v#.") != std::string::npos) {
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Find files with layout = 1 replica
//------------------------------------------------------------------------------
int Inspector::oneReplicaLayout(bool showName, bool showPaths, bool filterInternal, std::ostream &out, std::ostream &err) {
  FileScanner fileScanner(mQcl, showPaths | filterInternal);
  common::IntervalStopwatch stopwatch(std::chrono::seconds(10));

  while(fileScanner.valid()) {
    eos::ns::FileMdProto proto;
    FileScanner::Item item;

    if (!fileScanner.getItem(proto, &item)) {
      break;
    }

    int64_t actual = proto.locations().size();
    int64_t expected = eos::common::LayoutId::GetStripeNumber(proto.layout_id()) + 1;
    int64_t unlinked = proto.unlink_locations().size();
    int64_t size = proto.size();

    if(!proto.link_name().empty()) {
      expected = 0;
    }

    if(expected == 1 && size != 0 && shouldPrint(filterInternal, fetchNameOrPath(proto, item))) {
      out << "id=" << proto.id();

      if(showName || showPaths) {
          out << " name=" << fetchNameOrPath(proto, item);
      }

      out << " container=" << proto.cont_id() << " size=" << size << " actual-stripes=" << actual << " expected-stripes=" << expected << " unlinked-stripes=" << unlinked <<  " locations=" << serializeLocations(proto.locations()) << " unlinked-locations=" << serializeLocations(proto.unlink_locations());
      out << " mtime=" << Printing::timespecToTimestamp(Printing::parseTimespec(proto.mtime()));
      out << " ctime=" << Printing::timespecToTimestamp(Printing::parseTimespec(proto.ctime()));
      out << std::endl;
    }

    fileScanner.next();

    if (stopwatch.restartIfExpired()) {
      err << "Progress: Processed " << fileScanner.getScannedSoFar() << " files so far..." << std::endl;
    }
  }

  std::string errorString;
  if(fileScanner.hasError(errorString)) {
    err << errorString;
    return 1;
  }

  return 0;
}

//----------------------------------------------------------------------------
// Find files with non-nominal number of stripes (replicas)
//----------------------------------------------------------------------------
int Inspector::stripediff(bool printTime, std::ostream &out, std::ostream &err) {
  FileScanner fileScanner(mQcl);

  while(fileScanner.valid()) {
    eos::ns::FileMdProto proto;
    if (!fileScanner.getItem(proto)) {
      break;
    }

    int64_t actual = proto.locations().size();
    int64_t expected = eos::common::LayoutId::GetStripeNumber(proto.layout_id()) + 1;
    int64_t unlinked = proto.unlink_locations().size();
    int64_t size = proto.size();

    if(!proto.link_name().empty()) {
      expected = 0;
    }

    if(actual != expected && size != 0) {
      out << "id=" << proto.id() << " container=" << proto.cont_id() << " size=" << size << " actual-stripes=" << actual << " expected-stripes=" << expected << " unlinked-stripes=" << unlinked <<  " locations=" << serializeLocations(proto.locations()) << " unlinked-locations=" << serializeLocations(proto.unlink_locations());
      if(printTime) {
        out << " mtime=" << Printing::timespecToTimestamp(Printing::parseTimespec(proto.mtime()));
        out << " ctime=" << Printing::timespecToTimestamp(Printing::parseTimespec(proto.ctime()));
      }

      out << std::endl;
    }

    fileScanner.next();
  }

  std::string errorString;
  if(fileScanner.hasError(errorString)) {
    err << errorString;
    return 1;
  }

  return 0;
}

class ConflictSet {
public:
  std::set<uint64_t> files;
  std::set<uint64_t> containers;

  bool hasConflict() const {
    return (files.size() + containers.size()) > 1;
  }

  std::string serializeFiles() const {
    return serialize(files);
  }

  std::string serializeContainers() const {
    return serialize(containers);
  }

  void printMultipleLines(const std::string &name, uint64_t parentContainer, std::ostream &out) const {
    if(!hasConflict()) {
      return;
    }

    for(auto it = files.begin(); it != files.end(); it++) {
      out << "name=" << name << " under-container=" << parentContainer << " conflicting-file=" << *it << std::endl;
    }

    for(auto it = containers.begin(); it != containers.end(); it++) {
      out << "name=" << name << "  under-container=" << parentContainer << " conflicting-container=" << *it << std::endl;
    }
  }

  void printSingleLine(const std::string &name, uint64_t parentContainer,  std::ostream &out) const {
    if(!hasConflict()) {
      return;
    }

    out << "name=" << name << " under-container=" << parentContainer;

    if(!files.empty()) {
      out << " conflicting-files=" << serializeFiles();
    }

    if(!containers.empty()) {
      out << " conflicting-containers=" << serializeContainers();
    }

    out << std::endl;
  }

private:
  static std::string serialize(const std::set<uint64_t> &target) {
    std::ostringstream ss;

    for(auto it = target.begin(); it != target.end(); it++) {
      if(std::next(it) == target.end()) {
        ss << *it;
      }
      else {
        ss << *it << ",";
      }
    }

    return ss.str();
  }
};

//------------------------------------------------------------------------------
// Find conflicts
//------------------------------------------------------------------------------
void findConflicts(bool onePerLine, std::ostream& out, uint64_t parentContainer, const std::map<std::string, ConflictSet> &nameMapping) {
  for(auto it = nameMapping.begin(); it != nameMapping.end(); it++) {
    const ConflictSet &conflictSet = it->second;

    if(!onePerLine) {
      conflictSet.printSingleLine(it->first, parentContainer, out);
    }
    else {
      conflictSet.printMultipleLines(it->first, parentContainer, out);
    }
  }
}

//------------------------------------------------------------------------------
// Check intra-container conflicts, such as a container having two entries
// with the name name.
//------------------------------------------------------------------------------
int Inspector::checkNamingConflicts(bool onePerLine, std::ostream& out, std::ostream& err)
{
  std::string errorString;
  ContainerScanner containerScanner(mQcl);
  FileScanner fileScanner(mQcl);
  common::IntervalStopwatch stopwatch(std::chrono::seconds(10));

  eos::ns::FileMdProto fileProto;
  fileProto.set_cont_id(0);

  while (containerScanner.valid()) {
    eos::ns::ContainerMdProto proto;
    if (!containerScanner.getItem(proto)) {
      break;
    }

    if(proto.parent_id() == 0) {
      containerScanner.next();
      continue;
    }

    uint64_t currentParentId = proto.parent_id();
    std::map<std::string, ConflictSet> nameMapping;

    while(containerScanner.valid() && proto.parent_id() == currentParentId) {
      nameMapping[proto.name()].containers.insert(proto.id());

      containerScanner.next();
      containerScanner.getItem(proto);
    }

    while(fileScanner.valid() && fileProto.cont_id() <= currentParentId) {
      if(fileProto.cont_id() == currentParentId) {
        nameMapping[fileProto.name()].files.insert(fileProto.id());
      }

      fileScanner.next();
      fileScanner.getItem(fileProto);
    }

    findConflicts(onePerLine, out, currentParentId, nameMapping);
    nameMapping.clear();
  }

  if(containerScanner.hasError(errorString) || fileScanner.hasError(errorString)) {
    err << errorString;
    return 1;
  }

  return 0;
}

//------------------------------------------------------------------------------
// Check if file / container name is cursed
//------------------------------------------------------------------------------
static bool isCursedName(const std::string &name) {
  if (name == "" || name == "." || name == ".." || name.find("/") != std::string::npos) {
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Search for files / containers with cursed names
//------------------------------------------------------------------------------
int Inspector::checkCursedNames(std::ostream &out, std::ostream &err) {
  ContainerScanner containerScanner(mQcl);
  while(containerScanner.valid()) {
    eos::ns::ContainerMdProto proto;
    if (!containerScanner.getItem(proto)) {
      break;
    }

    if(proto.id() != 1 && isCursedName(proto.name())) {
      out << "cid=" << proto.id() << " cursed-name=" << escapeNonPrintable(proto.name()) << std::endl;
    }

    containerScanner.next();
  }

  FileScanner fileScanner(mQcl);
  while(fileScanner.valid()) {
    eos::ns::FileMdProto proto;
    if (!fileScanner.getItem(proto)) {
      break;
    }

    if (isCursedName(proto.name())) {
      out << "fid=" << proto.id() << " cursed-name=" << escapeNonPrintable(proto.name()) << std::endl;
    }

    fileScanner.next();
  }

  std::string errorString;
  if(containerScanner.hasError(errorString) || fileScanner.hasError(errorString)) {
    err << errorString;
    return 1;
  }

  return 0;
}

//------------------------------------------------------------------------------
// Helper struct used in checkOrphans
//------------------------------------------------------------------------------
struct PendingFile {
  folly::Future<bool> validParent;
  eos::ns::FileMdProto proto;

  PendingFile(folly::Future<bool> &&f, const eos::ns::FileMdProto &p)
  : validParent(std::move(f)), proto(p) {}
};

void consumePendingEntries(std::deque<PendingFile> &futs, bool unconditional, std::ostream &out) {
  while(!futs.empty() && (unconditional || futs.front().validParent.isReady())) {
    PendingFile &entry = futs.front();

    entry.validParent.wait();

    if(entry.validParent.hasException()) {
      out << "ERROR: Exception occurred when fetching container " << entry.proto.cont_id() << " as part of checking existence of parent of container " << entry.proto.id() << std::endl;
    }
    else if(entry.validParent.get() == false) {
      out << "file-id=" << entry.proto.id() << " invalid-parent-id=" << entry.proto.cont_id() << " size=" << entry.proto.size() << " locations=" << serializeLocations(entry.proto.locations()) << " unlinked-locations=" << serializeLocations(entry.proto.unlink_locations()) << std::endl;
    }

    futs.pop_front();
  }
}

struct PendingContainer {
  folly::Future<bool> validParent;
  eos::ns::ContainerMdProto proto;

  PendingContainer(folly::Future<bool> &&f, const eos::ns::ContainerMdProto &p)
  : validParent(std::move(f)), proto(p) {}
};

void consumePendingEntries(std::deque<PendingContainer> &futs, bool unconditional, std::ostream &out) {
  while(!futs.empty() && (unconditional || futs.front().validParent.isReady())) {
    PendingContainer &entry = futs.front();

    entry.validParent.wait();

    if(entry.validParent.hasException()) {
      out << "ERROR: Exception occurred when fetching container " << entry.proto.parent_id() << " as part of checking existence of parent of container " << entry.proto.id() << std::endl;
    }
    else if(entry.validParent.get() == false) {
      out << "container-id=" << entry.proto.id() << " invalid-parent-id=" << entry.proto.parent_id() << std::endl;
    }

    futs.pop_front();
  }
}

//------------------------------------------------------------------------------
// Find orphan files and orphan directories
//------------------------------------------------------------------------------
int Inspector::checkOrphans(std::ostream &out, std::ostream &err) {
  //----------------------------------------------------------------------------
  // Look for orphan containers..
  //----------------------------------------------------------------------------
  std::string errorString;
  ContainerScanner containerScanner(mQcl);
  common::IntervalStopwatch stopwatch(std::chrono::seconds(10));

  std::deque<PendingContainer> containers;
  while (containerScanner.valid()) {
    consumePendingEntries(containers, false, out);

    eos::ns::ContainerMdProto proto;
    if (!containerScanner.getItem(proto)) {
      break;
    }

    containers.emplace_back(
      MetadataFetcher::doesContainerMdExist(mQcl, ContainerIdentifier(proto.parent_id())),
      proto
    );

    if (stopwatch.restartIfExpired()) {
      err << "Progress: Processed " << containerScanner.getScannedSoFar() << " containers so far..." << std::endl;
    }

    containerScanner.next();
  }

  consumePendingEntries(containers, true, out);
  if(containerScanner.hasError(errorString)) {
    err << errorString;
    return 1;
  }

  err << "All containers processed, checking files..." << std::endl;

  //----------------------------------------------------------------------------
  // Look for orphan files..
  //----------------------------------------------------------------------------
  FileScanner fileScanner(mQcl);

  std::deque<PendingFile> files;
  while (fileScanner.valid()) {
    consumePendingEntries(files, false, out);

    eos::ns::FileMdProto proto;
    if (!fileScanner.getItem(proto)) {
      break;
    }

    files.emplace_back(
      MetadataFetcher::doesContainerMdExist(mQcl, ContainerIdentifier(proto.cont_id())),
      proto
    );

    if (stopwatch.restartIfExpired()) {
      err << "Progress: Processed " << fileScanner.getScannedSoFar() << " files so far..." << std::endl;
    }

    fileScanner.next();
  }

  consumePendingEntries(files, true, out);
  if(fileScanner.hasError(errorString)) {
    err << errorString;
    return 1;
  }

  return 0;
}

//------------------------------------------------------------------------------
// Helper struct for checkFsViewMissing
//------------------------------------------------------------------------------
struct FsViewItemExists {
  folly::Future<bool> valid;
  eos::ns::FileMdProto proto;
  int64_t location;
  bool unlinked;

  FsViewItemExists(folly::Future<bool> &&v, const eos::ns::FileMdProto &pr, int64_t loc, bool unl)
  : valid(std::move(v)), proto(pr), location(loc), unlinked(unl) {}
};

void consumeFsViewQueue(std::deque<FsViewItemExists> &futs, bool unconditional, std::ostream &out) {
  while(!futs.empty() && (unconditional || futs.front().valid.isReady())) {
    FsViewItemExists &entry = futs.front();
    entry.valid.wait();

    if(entry.valid.hasException()) {
      out << "ERROR: Exception occurred when checking validity of location " << entry.location << " (unlinked=" << entry.unlinked << ") of FileMD " << entry.proto.id() << std::endl;
    }
    else if(entry.valid.get() == false) {

      if(entry.unlinked) {
        out << "id=" << entry.proto.id() << " parent-id=" << entry.proto.cont_id() << " size=" << entry.proto.size() << " locations=" << serializeLocations(entry.proto.locations()) << " unlinked-locations=" << serializeLocations(entry.proto.unlink_locations()) << " missing-unlinked-location=" << entry.location << std::endl;
      }
      else {
        out << "id=" << entry.proto.id() << " parent-id=" << entry.proto.cont_id() << " size=" << entry.proto.size() << " locations=" << serializeLocations(entry.proto.locations()) << " unlinked-locations=" << serializeLocations(entry.proto.unlink_locations()) << " missing-location=" << entry.location << std::endl;
      }
    }

    futs.pop_front();
  }
}

//------------------------------------------------------------------------------
// Search for holes in FsView: Items which should be in FsView according to
// FMD locations / unlinked locations, but are not there.
//------------------------------------------------------------------------------
int Inspector::checkFsViewMissing(std::ostream &out, std::ostream &err) {
  //----------------------------------------------------------------------------
  // Search through all FileMDs..
  //----------------------------------------------------------------------------
  std::deque<FsViewItemExists> queue;
  FileScanner fileScanner(mQcl);
  common::IntervalStopwatch stopwatch(std::chrono::seconds(10));

  while(fileScanner.valid()) {
    consumeFsViewQueue(queue, false, out);

    eos::ns::FileMdProto proto;
    if (!fileScanner.getItem(proto)) {
      break;
    }

    for(auto it = proto.locations().cbegin(); it != proto.locations().cend(); it++) {
      queue.emplace_back(MetadataFetcher::locationExistsInFsView(mQcl, FileIdentifier(proto.id()),
        *it, false), proto, *it, false);
    }

    for(auto it = proto.unlink_locations().cbegin(); it != proto.unlink_locations().cend(); it++) {
      queue.emplace_back(MetadataFetcher::locationExistsInFsView(mQcl, FileIdentifier(proto.id()),
        *it, true), proto, *it, true);
    }

    if (stopwatch.restartIfExpired()) {
      err << "Progress: Processed " << fileScanner.getScannedSoFar() << " files so far" << std::endl;
    }

    fileScanner.next();
  }

  consumeFsViewQueue(queue, true, out);
  std::string errorString;
  if(fileScanner.hasError(errorString)) {
    err << errorString;
    return 1;
  }

  return 0;
}

struct FsViewExpectInLocations {
  folly::Future<eos::ns::FileMdProto> proto;
  int64_t futureFid;
  int64_t expectedLocation;
  bool unlinked;

  FsViewExpectInLocations(folly::Future<eos::ns::FileMdProto> &&p, int64_t fid, int64_t expected,
    bool unl) : proto(std::move(p)), futureFid(fid), expectedLocation(expected), unlinked(unl) {}
};

void consumeFsViewQueue(std::deque<FsViewExpectInLocations> &futs, bool unconditional, std::ostream &out) {
  while(!futs.empty() && (unconditional || futs.front().proto.isReady())) {
    FsViewExpectInLocations &entry = futs.front();
    entry.proto.wait();

    if(entry.proto.hasException()) {
      out << "ERROR: Exception occurred when fetching file with id " << entry.futureFid << std::endl;
    }
    else if(!entry.unlinked) {
      eos::ns::FileMdProto proto = entry.proto.get();

      bool found = false;
      for(auto it = proto.locations().cbegin(); it != proto.locations().cend(); it++) {
        if(*it == entry.expectedLocation) {
          found = true;
          break;
        }
      }

      if(!found) {
        out << "id=" << proto.id() << " parent-id=" << proto.cont_id() << " size=" << proto.size() << " locations=" << serializeLocations(proto.locations()) << " unlinked-locations=" << serializeLocations(proto.unlink_locations()) << " extra-location=" << entry.expectedLocation << std::endl;
      }
    }
    else {
      eos::ns::FileMdProto proto = entry.proto.get();

      bool found = false;
      for(auto it = proto.unlink_locations().cbegin(); it != proto.unlink_locations().cend(); it++) {
        if(*it == entry.expectedLocation) {
          found = true;
          break;
        }
      }

      if(!found) {
        out << "id=" << proto.id() << " parent-id=" << proto.cont_id() << " size=" << proto.size() << " locations=" << serializeLocations(proto.locations()) << " unlinked-locations=" << serializeLocations(proto.unlink_locations()) << " extra-unlink-location=" << entry.expectedLocation << std::endl;
      }
    }

    futs.pop_front();
  }
}

//------------------------------------------------------------------------------
// Search for elements which are present in FsView, but not FMD locations
//------------------------------------------------------------------------------
int Inspector::checkFsViewExtra(std::ostream &out, std::ostream &err) {
  //----------------------------------------------------------------------------
  // Scan through the entire filesystem view..
  //----------------------------------------------------------------------------
  std::deque<FsViewExpectInLocations> queue;
  FileSystemIterator fsIter(mQcl);

  while(fsIter.valid()) {
    StreamingFileListIterator fsScanner(mQcl, fsIter.getRedisKey());

    while(fsScanner.valid()) {
      consumeFsViewQueue(queue, false, out);

      queue.emplace_back(MetadataFetcher::getFileFromId(mQcl, FileIdentifier(fsScanner.getElement())),
        fsScanner.getElement(),
        fsIter.getFileSystemID(),
        fsIter.isUnlinked()
      );

      fsScanner.next();
    }

    fsIter.next();
  }

  consumeFsViewQueue(queue, true, out);
  return 0;
}

//------------------------------------------------------------------------------
// Search for shadow directories
//------------------------------------------------------------------------------
int Inspector::checkShadowDirectories(std::ostream& out, std::ostream& err)
{
  ContainerScanner containerScanner(mQcl);
  common::IntervalStopwatch stopwatch(std::chrono::seconds(10));

  eos::ns::ContainerMdProto prevContainer;

  while (containerScanner.valid()) {
    eos::ns::ContainerMdProto proto;
    if (!containerScanner.getItem(proto)) {
      break;
    }

    if(proto.parent_id() != 0 && proto.name() == prevContainer.name() && proto.parent_id() == prevContainer.parent_id()) {
      out << "id=" << proto.id()
          << " name=" << proto.name()
          << " parent=" << proto.parent_id()
          << " mtime=" << Printing::timespecToTimestamp(Printing::parseTimespec(proto.mtime()))
          << " ctime=" << Printing::timespecToTimestamp(Printing::parseTimespec(proto.ctime()))
          << " is-quotanode=" << (proto.flags() & QUOTA_NODE_FLAG)
          << " conflicts-with=" << prevContainer.id()
          << std::endl;
    }

    prevContainer = std::move(proto);
    containerScanner.next();
  }

  std::string errorString;
  if(containerScanner.hasError(errorString)) {
    err << errorString;
    return 1;
  }

  return 0;
}

//------------------------------------------------------------------------------
// Helper class to run next on destruction
//------------------------------------------------------------------------------
class NextGuard {
public:
  NextGuard(FileScanner &sc) : scanner(sc) {}
  ~NextGuard() {
    scanner.next();
  }

private:
  FileScanner &scanner;
};

//------------------------------------------------------------------------------
// Hardlink information
//------------------------------------------------------------------------------
struct HardlinkInfo {
  HardlinkInfo() {}
  HardlinkInfo(uint64_t t, const std::string &n) : target(t), name(n) {}

  uint64_t target;
  std::string name;
};

struct InodeUseCount {
  InodeUseCount() : count(0) {}
  InodeUseCount(int64_t c, const std::string &n) : count(c), name(n) {}

  int64_t count;
  std::string name;
};

//------------------------------------------------------------------------------
// Cross-check inode use counts against hardlink mappings
//------------------------------------------------------------------------------
void crossCheckHardlinkMaps(std::map<uint64_t, InodeUseCount> &inodeUseCount,
  std::map<uint64_t, HardlinkInfo> &hardlinkMapping, uint64_t parent, std::ostream &out) {

  std::set<uint64_t> zeroGroup;

  for(auto it = inodeUseCount.begin(); it != inodeUseCount.end(); it++) {
    if(it->second.count == 0) {
      zeroGroup.insert(it->first);
    }
  }

  for(auto it = hardlinkMapping.begin(); it != hardlinkMapping.end(); it++) {
    auto useCount = inodeUseCount.find(it->second.target);

    if(useCount == inodeUseCount.end()) {
      out << "id=" << it->first << " name=" << it->second.name << " parent=" << parent << " invalid-target=" << it->second.target << std::endl;
    }
    else {
      inodeUseCount[it->second.target].count--;
    }
  }

  for(auto it = inodeUseCount.begin(); it != inodeUseCount.end(); it++) {
    if(it->second.count != 0) {
      out << "id=" << it->first << " name=" << it->second.name << " parent=" << parent << " reference-count-diff=" << it->second.count << std::endl;
      zeroGroup.erase(it->first);
    }
  }

  for(auto it = zeroGroup.begin(); it != zeroGroup.end(); it++) {
      out << "id=" << *it << " name=" << inodeUseCount[*it].name  << " parent=" << parent << " true-zero-count" << std::endl;
  }
}

//------------------------------------------------------------------------------
// Check for corrupted ...eos.ino... hardlink-simulation files
//------------------------------------------------------------------------------
int Inspector::checkSimulatedHardlinks(std::ostream &out, std::ostream &err) {
  FileScanner fileScanner(mQcl);
  common::InodeTranslator translator;

  std::map<uint64_t, InodeUseCount> inodeUseCount;
  std::map<uint64_t, HardlinkInfo> hardlinkMapping;
  uint64_t currentContainer = 0;

  while (fileScanner.valid()) {
    eos::ns::FileMdProto proto;
    if (!fileScanner.getItem(proto)) {
      break;
    }

    NextGuard nextGuard(fileScanner);

    if(proto.cont_id() == 0) {
      continue;
    }

    if(proto.cont_id() != currentContainer) {
      crossCheckHardlinkMaps(inodeUseCount, hardlinkMapping, currentContainer, out);
      inodeUseCount.clear();
      hardlinkMapping.clear();
    }

    currentContainer = proto.cont_id();

    auto it = proto.xattrs().find("sys.eos.mdino");
    if(it != proto.xattrs().end()) {
      uint64_t inode = 0;
      if(!common::ParseUInt64(it->second.c_str(), inode)) {
        err << "Could not parse sys.eos.mdino: " << it->second.c_str() << std::endl;
        continue;
      }

      uint64_t target = translator.InodeToFid(inode);
      hardlinkMapping[proto.id()] = HardlinkInfo(target, proto.name());
      continue;
    }

    it = proto.xattrs().find("sys.eos.nlink");
    if(it != proto.xattrs().end()) {
      size_t count = atoi(it->second.c_str());
      inodeUseCount[proto.id()] = InodeUseCount(count, proto.name());
    }
  }

  return 0;
}

//------------------------------------------------------------------------------
// Print out _everything_ known about the given directory.
//------------------------------------------------------------------------------
int Inspector::printContainerMD(uint64_t cid, bool withParents, std::ostream& out, std::ostream& err)
{
  eos::ns::ContainerMdProto val;

  try {
    val = MetadataFetcher::getContainerFromId(mQcl, ContainerIdentifier(cid)).get();
  } catch (const MDException& e) {
    err << "Error while fetching metadata for ContainerMD #" << cid << ": " << e.what()
        << std::endl;
  }

  Printing::printMultiline(val, out);

  try {
    std::string fullPath = MetadataFetcher::resolveFullPath(mQcl, ContainerIdentifier(val.id())).get();
    out << "Full path: " << fullPath << std::endl;
  } catch(const MDException& e) {
    err << "Full path: Could not reconstruct" << std::endl;
  }

  IContainerMD::FileMap fileMap;
  IContainerMD::FileMap containerMap;

  try {
    fileMap = MetadataFetcher::getFileMap(mQcl, ContainerIdentifier(cid)).get();
  } catch (const MDException& e) {
    err << "Error while fetching file map for ContainerMD #" << cid << ": " << e.what()
        << std::endl;
  }

  try {
    containerMap = MetadataFetcher::getContainerMap(mQcl, ContainerIdentifier(cid)).get();
  } catch (const MDException& e) {
    err << "Error while fetching container map for ContainerMD #" << cid << ": " << e.what()
        << std::endl;
  }

  out << "------------------------------------------------" << std::endl;
  out << "FileMap:" << std::endl;
  for(auto it = fileMap.cbegin(); it != fileMap.cend(); it++) {
    out << it->first << ": " << it->second << std::endl;
  }

  out << "------------------------------------------------" << std::endl;
  out << "ContainerMap:" << std::endl;
  for(auto it = containerMap.cbegin(); it != containerMap.cend(); it++) {
    out << it->first << ": " << it->second << std::endl;
  }

  if(withParents && val.parent_id() != 0 && val.id() != val.parent_id()) {
    out << std::endl << std::endl << std::endl << std::endl << std::endl;
    return printContainerMD(val.parent_id(), withParents, out, err);
  }

  return 0;
}

//------------------------------------------------------------------------------
// Print out _everything_ known about the given file.
//------------------------------------------------------------------------------
int Inspector::printFileMD(uint64_t fid, bool withParents, std::ostream& out, std::ostream& err)
{
  eos::ns::FileMdProto val;

  try {
    val = MetadataFetcher::getFileFromId(mQcl, FileIdentifier(fid)).get();
  } catch (const MDException& e) {
    err << "Error while fetching metadata for FileMD #" << fid << ": " << e.what()
        << std::endl;
    return 1;
  }

  Printing::printMultiline(val, out);

  try {
    std::string fullPath = MetadataFetcher::resolveFullPath(mQcl, ContainerIdentifier(val.cont_id())).get();
    out << "Full path: " << fullPath << val.name() << std::endl;
  } catch(const MDException& e) {
    err << "Full path: Could not reconstruct" << std::endl;
  }

  if(withParents && val.cont_id() != 0) {
    out << std::endl << std::endl << std::endl << std::endl << std::endl;
    return printContainerMD(val.cont_id(), withParents, out, err);
  }

  return 0;
}

//------------------------------------------------------------------------------
// Serialize RedisRequest
//------------------------------------------------------------------------------
static std::string serializeRequest(const RedisRequest &req) {
  std::ostringstream ss;

  for(size_t i = 0; i < req.size(); i++) {
    ss << "\"" << escapeNonPrintable(req[i]) << "\"" << " ";
  }

  return ss.str();
}

//------------------------------------------------------------------------------
//! Check if given path is a good choice as a destination for repaired
//! files / containers
//------------------------------------------------------------------------------
bool Inspector::isDestinationPathSane(const std::string &path, ContainerIdentifier &cid, std::ostream &out) {
  try {
    FileOrContainerIdentifier id = MetadataFetcher::resolvePathToID(mQcl, path).get();

    if(id.isFile()) {
      out << "Destination path '" << path << "' is a file, not a directory." << std::endl;
      return false;
    }

    cid = id.toContainerIdentifier();
  }
  catch(const MDException& e) {
    out << "Destination path '" << path << "' does not exist." << std::endl;
    return false;
  }

  if(cid == ContainerIdentifier(1) || cid == ContainerIdentifier(2) || cid == ContainerIdentifier(3)) {
    out << "Destination path '" << path << "' does not look like a good place, too top-level." << std::endl;
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Attempt to fix detached parent
//------------------------------------------------------------------------------
int Inspector::fixDetachedParentContainer(bool dryRun, uint64_t cid, const std::string &destinationPath, std::ostream &out, std::ostream &err) {
  //----------------------------------------------------------------------------
  // Ensure given container exists
  //----------------------------------------------------------------------------
  if(!MetadataFetcher::doesContainerMdExist(mQcl, ContainerIdentifier(cid)).get()) {
    out << "Container #" << cid << " does not exist." << std::endl;
    return 1;
  }

  //----------------------------------------------------------------------------
  // Ensure given destination path is sane
  //----------------------------------------------------------------------------
  ContainerIdentifier destination;
  if(!isDestinationPathSane(destinationPath, destination, out)) {
    return 1;
  }

  //----------------------------------------------------------------------------
  // Try and figure out what the problem with the given container is
  //----------------------------------------------------------------------------
  out << "Finding all parents of Container #" << cid << "..." << std::endl;

  uint64_t nextToCheck = cid;

  eos::ns::ContainerMdProto val;
  while(nextToCheck != 0 && nextToCheck != 1) {
    try {
      val = MetadataFetcher::getContainerFromId(mQcl, ContainerIdentifier(nextToCheck)).get();
      out << val.name() << ": #" << val.id() << " with parent #" << val.parent_id() << std::endl;
      nextToCheck = val.parent_id();
    }
    catch(const MDException& e) {
      break;
    }
  }

  if(nextToCheck == 1 || nextToCheck == 0) {
    err << "Unable to continue - given container (" << cid << ") looks fine? No changes have been made." << std::endl;
    return 1;
  }

  //----------------------------------------------------------------------------
  // One of its parents is detached from the main tree, rename
  //----------------------------------------------------------------------------
  out << std::endl << std::endl << "Found detached container #" << val.id() << " since its parent #" << val.parent_id() << " does not exist." << std::endl;

  // Paranoid check
  eos_assert(!MetadataFetcher::doesContainerMdExist(mQcl, ContainerIdentifier(val.parent_id())).get());

  // Go
  std::string newName = SSTR("recovered-dir___id=" << val.id() << "___name=" << val.name() << "___detached-parent=" << val.parent_id());
  return renameCid(dryRun, val.id(), destination.getUnderlyingUInt64(), newName, out, err);
}

//------------------------------------------------------------------------------
// Attempt to fix naming conflict
//------------------------------------------------------------------------------
int Inspector::fixShadowFile(bool dryRun, uint64_t fid, const std::string &destinationPath, std::ostream &out, std::ostream &err) {
  //----------------------------------------------------------------------------
  // Ensure given file exists..
  //----------------------------------------------------------------------------
  eos::ns::FileMdProto val;

  try {
    val = MetadataFetcher::getFileFromId(mQcl, FileIdentifier(fid)).get();
  } catch(const MDException& e) {
    err << "Error while fetching metadata for FileMD #" << fid << ": " << e.what()
        << std::endl;
    return 1;
  }

  //----------------------------------------------------------------------------
  // Ensure given destination path is sane
  //----------------------------------------------------------------------------
  ContainerIdentifier destination;
  if(!isDestinationPathSane(destinationPath, destination, out)) {
    return 1;
  }

  //----------------------------------------------------------------------------
  // Ensure the given fid is indeed shadowed
  //----------------------------------------------------------------------------
  bool cidExists = MetadataFetcher::doesContainerMdExist(mQcl, ContainerIdentifier(val.cont_id())).get();
  IContainerMD::FileMap cidFilemap = MetadataFetcher::getFileMap(mQcl, ContainerIdentifier(val.cont_id())).get();
  bool filemapEntryExists = cidFilemap.find(val.name()) != cidFilemap.cend();
  bool filemapEntryValid = (cidFilemap[val.name()] == val.id());

  IContainerMD::ContainerMap cidContainermap = MetadataFetcher::getContainerMap(mQcl, ContainerIdentifier(val.cont_id())).get();
  bool containerMapConflict = cidContainermap.find(val.name()) != cidContainermap.cend();

  out << "Parent exists? " << toYesOrNo(cidExists) << std::endl;
  out << "Filemap entry exists? " << toYesOrNo(filemapEntryExists) << std::endl;
  out << "Filemap entry valid? " << toYesOrNo(filemapEntryValid) << std::endl;
  out << "Containermap conflict? " << toYesOrNo(containerMapConflict) << std::endl;

  if(!cidExists) {
    err << "Parent container does not exist, use fix-detached-parent." << std::endl;
    return 1;
  }

  if(filemapEntryExists && filemapEntryValid && !containerMapConflict) {
    err << "File looks fine? No naming conflict detected, nothing to be done." << std::endl;
    return 1;
  }

  if(!filemapEntryExists) {
    out << "Detected problem: Filemap entry does not exist." << std::endl;
  }
  else if(!filemapEntryValid) {
    out << "Detected problem: Filemap entry is not valid, and instead points to fid " << cidFilemap[val.name()] << std::endl;
  }

  if(containerMapConflict) {
    out << "Detected problem: Conflict with containermap entry, points to cid " << cidContainermap[val.name()] << std::endl;
  }

  // Go
  std::string newName = SSTR("recovered-file___id=" << val.id() << "___name=" << val.name() << "___naming-conflict-in-parent=" << val.cont_id());
  return renameFid(dryRun, val.id(), destination.getUnderlyingUInt64(), newName, out, err);
}

//------------------------------------------------------------------------------
// Attempt to fix detached parent
//------------------------------------------------------------------------------
int Inspector::fixDetachedParentFile(bool dryRun, uint64_t fid, const std::string &destinationPath, std::ostream &out, std::ostream &err) {
  //----------------------------------------------------------------------------
  // Ensure given file exists..
  //----------------------------------------------------------------------------
  eos::ns::FileMdProto val;

  try {
    val = MetadataFetcher::getFileFromId(mQcl, FileIdentifier(fid)).get();
  } catch(const MDException& e) {
    err << "Error while fetching metadata for FileMD #" << fid << ": " << e.what()
        << std::endl;
    return 1;
  }

  //----------------------------------------------------------------------------
  // Ensure given destination path is sane
  //----------------------------------------------------------------------------
  ContainerIdentifier destination;
  if(!isDestinationPathSane(destinationPath, destination, out)) {
    return 1;
  }

  //----------------------------------------------------------------------------
  // If immediate parent is not missing,
  // switch over to fixDetachedParentContainer
  //----------------------------------------------------------------------------
  if(MetadataFetcher::doesContainerMdExist(mQcl, ContainerIdentifier(val.cont_id())).get()) {
    out << "File #" << val.id() << " not detached, but one of its parents might be." << std::endl;
    out << "Continuing search onto its parent, container #" << val.cont_id() << "..." << std::endl;
    return fixDetachedParentContainer(dryRun, val.cont_id(), destinationPath, out, err);
  }

  //----------------------------------------------------------------------------
  // Immediate parent is missing, rename fid itself.
  //----------------------------------------------------------------------------
  out << "Found detached file #" << val.id() << ", its direct parent #" << val.cont_id() << " is missing." << std::endl;

  // Paranoid check
  eos_assert(!MetadataFetcher::doesContainerMdExist(mQcl, ContainerIdentifier(val.cont_id())).get());

  // Go
  std::string newName = SSTR("recovered-file___id=" << val.id() << "___name=" << val.name() << "___detached-parent=" << val.cont_id());
  return renameFid(dryRun, val.id(), destination.getUnderlyingUInt64(), newName, out, err);
}

//------------------------------------------------------------------------------
// Change the given fid - USE WITH CAUTION
//------------------------------------------------------------------------------
int Inspector::changeFid(bool dryRun, uint64_t fid, uint64_t newParent, const std::string &newChecksum, int64_t newSize, std::ostream &out, std::ostream &err) {
  eos::ns::FileMdProto val;

  try {
    val = MetadataFetcher::getFileFromId(mQcl, FileIdentifier(fid)).get();
  } catch(const MDException& e) {
    err << "Error while fetching metadata for FileMD #" << fid << ": " << e.what()
        << std::endl;
    return 1;
  }

  Printing::printMultiline(val, out);

  bool ok = false;
  out << "----- CHANGING THE FOLLOWING ATTRIBUTES:" << std::endl;
  if(newParent != 0) {
    ok = true;
    err << "    Container ID: " << val.cont_id() << " --> " << newParent << std::endl;
    val.set_cont_id(newParent);
  }

  if(!newChecksum.empty()) {
    std::string existingChecksum;
    eos::appendChecksumOnStringProtobuf(val, existingChecksum);

    std::string newChecksumBytes;
    if(!eos::hexArrayToByteArray(newChecksum.c_str(), newChecksum.size(), newChecksumBytes)) {
      err << "Error: Could not decode checksum, needs to be in hex: " << newChecksum << std::endl;
      return 1;
    }

    ok = true;
    err << "    Checksum: " << existingChecksum << " --> " << newChecksum << std::endl;
    val.set_checksum(newChecksumBytes.c_str(), newChecksumBytes.size());
  }

  if(newSize >= 0) {
    ok = true;
    err << "    Size: " << val.size() << " --> " << newSize << std::endl;
    val.set_size(newSize);
  }

  if(!ok) {
    err << "Error: No attributes specified to update." << std::endl;
    return 1;
  }

  QuarkFileMD fileMD;
  fileMD.initialize(std::move(val));

  std::vector<RedisRequest> requests;
  requests.emplace_back(RequestBuilder::writeFileProto(&fileMD));
  executeRequestBatch(requests, {}, dryRun, out, err);
  return 0;
}

//------------------------------------------------------------------------------
// Rename the given cid fully, taking care of the container maps as well.
//------------------------------------------------------------------------------
int Inspector::renameCid(bool dryRun, uint64_t cid, uint64_t newParent, const std::string &newName, std::ostream &out, std::ostream &err) {
  eos::ns::ContainerMdProto val;
  bool protoExists = false;

  try {
    val = MetadataFetcher::getContainerFromId(mQcl, ContainerIdentifier(cid)).get();
    protoExists = true;
  } catch(const MDException &e) {
    val.set_id(cid);
    err << "Error while fetching metadata for ContainerMD #" << cid << ": " << e.what();
  }

  out << "------------------------------------------------------ Container overview" << std::endl;

  bool parentExists = false;
  IContainerMD::ContainerMap parentContainermap;
  bool containermapEntryExists = false;
  bool containermapEntryValid = false;
  std::string oldName = "";
  uint64_t oldContainer = 0;

  if(protoExists) {
    Printing::printMultiline(val, out);

    parentExists = MetadataFetcher::doesContainerMdExist(mQcl, ContainerIdentifier(val.parent_id())).get();
    parentContainermap = MetadataFetcher::getContainerMap(mQcl, ContainerIdentifier(val.parent_id())).get();
    containermapEntryExists = parentContainermap.find(val.name()) != parentContainermap.cend();
    containermapEntryValid = (parentContainermap[val.name()] == val.id());
    oldName = val.name();
    oldContainer = val.parent_id();

    out << "------------------------------------------------------ Sanity check" << std::endl;
    out << "Parent (" << (val.parent_id()) << ") exists? " << toYesOrNo(parentExists) << std::endl;
    out << "Containermap entry exists? " << toYesOrNo(containermapEntryExists) << std::endl;

    if(containermapEntryExists) {
      out << "Containermap entry (" << val.name() << " -> " << parentContainermap[val.name()] << ") valid? " << toYesOrNo(containermapEntryValid) << std::endl;
    }
  }
  else {
    out << "Protobuf for cid=" << cid << " does not exist!" << std::endl;
  }

  if(!protoExists && newName.empty()) {
    out << "Name needs to be specified if original container did not exist! Aborting operation." << std::endl;
    return 1;
  }

  val.set_parent_id(newParent);
  if(!newName.empty()) {
    val.set_name(newName);
  }

  std::vector<RedisRequest> requests;
  CacheNotifications notifications;

  QuarkContainerMD containerMD;
  containerMD.initialize(std::move(val), IContainerMD::FileMap(), IContainerMD::ContainerMap());
  requests.emplace_back(RequestBuilder::writeContainerProto(&containerMD));

  if(containermapEntryExists && containermapEntryValid) {
    RedisRequest req = {"HDEL", SSTR(oldContainer << constants::sMapDirsSuffix), oldName};
    notifications.cids.emplace_back(oldContainer);
    requests.emplace_back(req);
  }

  RedisRequest req = {"HSET", SSTR(newParent << constants::sMapDirsSuffix), containerMD.getName(), SSTR(containerMD.getId()) };
  notifications.cids.emplace_back(newParent);
  notifications.cids.emplace_back(containerMD.getId());
  requests.emplace_back(req);

  executeRequestBatch(requests, notifications, dryRun, out, err);
  return 0;
}

//------------------------------------------------------------------------------
// Rename the given fid fully, taking care of the container maps as well
//------------------------------------------------------------------------------
int Inspector::renameFid(bool dryRun, uint64_t fid, uint64_t newParent, const std::string &newName, std::ostream &out, std::ostream &err) {
  eos::ns::FileMdProto val;

  try {
    val = MetadataFetcher::getFileFromId(mQcl, FileIdentifier(fid)).get();
  } catch(const MDException &e) {
    err << "Error while fetching metadata for FileMD #" << fid << ": " << e.what()
        << std::endl;
    return 1;
  }

  out << "------------------------------------------------------ FMD overview" << std::endl;
  Printing::printMultiline(val, out);

  bool cidExists = MetadataFetcher::doesContainerMdExist(mQcl, ContainerIdentifier(val.cont_id())).get();
  IContainerMD::FileMap cidFilemap = MetadataFetcher::getFileMap(mQcl, ContainerIdentifier(val.cont_id())).get();
  bool filemapEntryExists = cidFilemap.find(val.name()) != cidFilemap.cend();
  bool filemapEntryValid = (cidFilemap[val.name()] == val.id());
  std::string oldName = val.name();
  uint64_t oldContainer = val.cont_id();

  out << "------------------------------------------------------ Sanity check" << std::endl;
  out << "Old container (" << (val.cont_id()) << ") exists? " << toYesOrNo(cidExists) << std::endl;
  out << "Filemap entry exists? " << toYesOrNo(filemapEntryExists) << std::endl;

  if(filemapEntryExists) {
    out << "Filemap entry (" << val.name() << " -> " << cidFilemap[val.name()] << ") valid? " << toYesOrNo(filemapEntryValid) << std::endl;
  }
  out << "------------------------------------------------------ FMD changes" << std::endl;

  out << "    Parent ID: " << val.cont_id() << " --> " << newParent << std::endl;
  val.set_cont_id(newParent);

  if(!newName.empty()) {
    out << "    Name: " << val.name() << " --> " << newName << std::endl;
    val.set_name(newName);
  }


  std::vector<RedisRequest> requests;
  CacheNotifications notifications;

  QuarkFileMD fileMD;
  fileMD.initialize(std::move(val));
  requests.emplace_back(RequestBuilder::writeFileProto(&fileMD));

  if(filemapEntryExists && filemapEntryValid) {
    RedisRequest req = {"HDEL", SSTR(oldContainer << constants::sMapFilesSuffix), oldName};
    requests.emplace_back(req);
    notifications.cids.emplace_back(oldContainer);
  }

  RedisRequest req = {"HSET", SSTR(newParent << constants::sMapFilesSuffix), fileMD.getName(), SSTR(fileMD.getId()) };
  requests.emplace_back(req);

  notifications.cids.emplace_back(newParent);
  notifications.fids.emplace_back(fileMD.getId());

  executeRequestBatch(requests, notifications, dryRun, out, err);
  return 0;
}

//------------------------------------------------------------------------------
// Run the given write batch towards QDB - print the requests, as well as the
// output.
//------------------------------------------------------------------------------
void Inspector::executeRequestBatch(const std::vector<RedisRequest> &requests, const CacheNotifications &notif, bool dryRun, std::ostream& out, std::ostream& err) {
  out << "------------------------------------------------------ QDB commands to execute" << std::endl;

  for(size_t i = 0; i < requests.size(); i++) {
    out << i+1 << ". " << serializeRequest(requests[i]) << std::endl;
  }

  std::vector<RedisRequest> cacheNotifications;

  if(notif.cids.size() + notif.fids.size() != 0) {
    out << "------------------------------------------------------ Cache notifications" << std::endl;
    for(size_t i = 0; i < notif.cids.size(); i++) {
      cacheNotifications.emplace_back(RequestBuilder::notifyCacheInvalidationCid(ContainerIdentifier(notif.cids[i])));
    }

    for(size_t i = 0; i < notif.fids.size(); i++) {
      cacheNotifications.emplace_back(RequestBuilder::notifyCacheInvalidationFid(FileIdentifier(notif.fids[i])));
    }

    for(size_t i = 0; i < cacheNotifications.size(); i++) {
      out << i+1 << ". " << serializeRequest(cacheNotifications[i]) << std::endl;
    }
  }

  if(dryRun) {
    out << "------------------------------------------------------ DRY RUN, CHANGES NOT APPLIED" << std::endl;
    return;
  }

  std::vector<std::future<qclient::redisReplyPtr>> replies;
  std::vector<std::future<qclient::redisReplyPtr>> notificationReplies;

  for(size_t i = 0; i < requests.size(); i++) {
    replies.push_back(mQcl.execute(requests[i]));
  }

  for(size_t i = 0; i < cacheNotifications.size(); i++) {
    notificationReplies.push_back(mQcl.execute(cacheNotifications[i]));
  }

  out << "------------------------------------------------------ Replies" << std::endl;

  for(size_t i = 0; i < replies.size(); i++) {
    out << i+1 << ". " << qclient::describeRedisReply(replies[i].get()) << std::endl;
  }

  if(!notificationReplies.empty()) {
    out << "------------------------------------------------------ Notification replies" << std::endl;

    for(size_t i = 0; i < notificationReplies.size(); i++) {
      out << i+1 << ". " << qclient::describeRedisReply(notificationReplies[i].get()) << std::endl;
    }
  }
}

EOSNSNAMESPACE_END
