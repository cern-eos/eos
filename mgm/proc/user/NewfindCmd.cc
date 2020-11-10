//------------------------------------------------------------------------------
// @file: NewfindCmd.cc
// @author: Fabio Luchetti, Georgios Bitzes, Jozsef Makai - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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

#include "NewfindCmd.hh"
#include "common/LayoutId.hh"
#include "common/Path.hh"
#include "mgm/Acl.hh"
#include "mgm/FsView.hh"
#include "mgm/Stat.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/auth/AccessChecker.hh"
#include "namespace/interface/IView.hh"
#include "namespace/ns_quarkdb/BackendClient.hh"
#include "namespace/ns_quarkdb/ContainerMD.hh"
#include "namespace/ns_quarkdb/FileMD.hh"
#include "namespace/ns_quarkdb/NamespaceGroup.hh"
#include "namespace/ns_quarkdb/explorer/NamespaceExplorer.hh"
#include "namespace/utils/BalanceCalculator.hh"
#include "namespace/utils/Checksum.hh"
#include "namespace/utils/Stat.hh"
#include <mgm/Access.hh>

EOSMGMNAMESPACE_BEGIN

static bool eliminateBasedOnMaxDepth(const eos::console::FindProto& req,
                                      const std::string& fullpath)
{
  eos::common::Path cpath {fullpath};
  return (req.maxdepth()>0 && cpath.GetSubPathSize()>req.maxdepth()+1);
  // not using File/ContainerMD->getTreeSize() to avoid taking a lock
}

template<typename T>
static bool eliminateBasedOnFileMatch(const eos::console::FindProto& req,
                                      const T& md)
{
  XrdOucString name = md->getName().c_str();
  return (!req.name().empty()) && (name.matches(req.name().c_str()) == 0);
}

//------------------------------------------------------------------------------
// Based on the Uid/Gid of given FileMd / ContainerMd, should it be included
// in the search results?
//------------------------------------------------------------------------------
template<typename T>
static bool eliminateBasedOnUidGid(const eos::console::FindProto& req,
                                   const T& md)
{
  if (req.searchuid() && md->getCUid() != req.uid()) {
    return true;
  }

  if (req.searchnotuid() && md->getCUid() == req.notuid()) {
    return true;
  }

  if (req.searchgid() && md->getCGid() != req.gid()) {
    return true;
  }

  if (req.searchnotgid() && md->getCGid() == req.notgid()) {
    return true;
  }

  return false;
}

//----------------------------------------------------------------------------------
// Check whether to eliminate depending on modification time and options passed to NewfindCmd.
// @note Assume ctime_t is always of type "struct timespec" for both containers and files
//----------------------------------------------------------------------------------
template<typename T>
static bool eliminateBasedOnTime(const eos::console::FindProto& req,
                                 const T& md)
{

  struct timespec xtime;

  if (req.ctime()) {
    md->getCTime(xtime);
  } else {
    md->getMTime(xtime);
  }

  if (req.onehourold()) {
    if (xtime.tv_sec > (time(nullptr) - 3600)) {
      return true;
    }
  }

  time_t selectoldertime = (time_t) req.olderthan();
  time_t selectyoungertime = (time_t) req.youngerthan();

  if (selectoldertime > 0) {
    if (xtime.tv_sec > selectoldertime) {
      return true;
    }
  }

  if (selectyoungertime > 0) {
    if (xtime.tv_sec < selectyoungertime) {
      return true;
    }
  }

  return false;
}

//------------------------------------------------------------------------------
// Check whether to select depending on permissions.
//------------------------------------------------------------------------------
template<typename T>
static bool eliminateBasedOnPermissions(const eos::console::FindProto& req,
                                        const T& md)
{
  if (!req.searchpermission() && !req.searchnotpermission()) {
    return false;
  }

  mode_t st_mode = eos::modeFromMetadataEntry(md);
  std::ostringstream flagOstr;
  flagOstr << std::oct << st_mode;
  std::string flagStr = flagOstr.str();
  std::string permString = flagStr.substr(flagStr.length() - 3);

  if (req.searchpermission() && permString != req.permission()) {
    return true;
  }

  if (req.searchnotpermission() && permString == req.notpermission()) {
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Check whether to select depending on attributes.
//------------------------------------------------------------------------------
template<typename T>
static bool eliminateBasedOnAttr(const eos::console::FindProto& req,
                                 const T& md)
{
  if (req.attributekey().empty() || req.attributevalue().empty()) {
    return false;
  }

  std::string attr;

  if (!gOFS->_attr_get(*md.get(), req.attributekey(), attr)) {
    return true;
  }

  return (attr != req.attributevalue());
}

//------------------------------------------------------------------------------
// Check whether to select depending on the file/container having faulty ACLs.
//------------------------------------------------------------------------------
template<typename T>
static bool eliminateBasedOnFaultyAcl(const eos::console::FindProto& req, const T& md)
{
  // if not asking for faulty acls, just don't eliminate it
  if (!req.faultyacl()) {
    return false;
  } else {

    XrdOucErrInfo errInfo;
    std::string sysacl;
    std::string useracl;

    // return jumps makes it convoluted, sight...
    if (gOFS->_attr_get(*md.get(), "sys.acl", sysacl)) {
      if (!Acl::IsValid(sysacl.c_str(), errInfo)) {
        return false;
      }
    }
    if (gOFS->_attr_get(*md.get(), "user.acl", useracl)) {
      if (!Acl::IsValid(useracl.c_str(), errInfo)) {
        return false;
      }
    }
    return true;
  }
}

template<typename T>
static bool FilterOut (const eos::console::FindProto& req,
                       const T& md) {
  return (
      eliminateBasedOnFileMatch(req, md) ||
      eliminateBasedOnUidGid(req, md) ||
      eliminateBasedOnTime(req, md) ||
      eliminateBasedOnPermissions(req, md) ||
      eliminateBasedOnAttr(req, md) ||
      eliminateBasedOnFaultyAcl(req, md)
  );
}

// For files only. Check whether file replicas belong to different scheduling groups
static bool hasSizeZero(std::shared_ptr<eos::IFileMD>& fmd) {
  return (fmd->getSize() == 0);
}

// For files only. Check whether file replicas belong to different scheduling groups
static bool hasMixedSchedGroups(std::shared_ptr<eos::IFileMD>& fmd)
{
  // find files which have replicas on mixed scheduling groups
  std::string sGroupRef = "";
  std::string sGroup = "";

  for (auto lociter : fmd->getLocations()) {
    // ignore filesystem id 0
    if (!lociter) {
      eos_static_err("fsid 0 found fxid=%08llx", fmd->getId());
      continue;
    }

    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
    eos::common::FileSystem* filesystem = FsView::gFsView.mIdView.lookupByID(
        lociter);

    if (filesystem != nullptr) {
      sGroup = filesystem->GetString("schedgroup");
    } else {
      sGroup = "none";
    }

    if (!sGroupRef.empty()) {
      if (sGroup != sGroupRef) {
        return true;
      }
    } else {
      sGroupRef = sGroup;
    }
  }

  return false;
}

// For files only. Check whether a file has not the nominal number of stripes(replicas)
static bool hasStripeDiff(std::shared_ptr<eos::IFileMD>& fmd) {
  return (fmd->getNumLocation() == eos::common::LayoutId::GetStripeNumber(fmd->getLayoutId()) + 1);
}


//------------------------------------------------------------------------------
// Print path.
//------------------------------------------------------------------------------
static void printPath(std::ofstream& ss, const std::string& path,
                      bool url)
{
  if (url) {
    ss << "root://" << gOFS->MgmOfsAlias << "/";
  }
  ss << path;
}


//------------------------------------------------------------------------------
// Print uid / gid of a FileMD or ContainerMD, if requested by req.
//------------------------------------------------------------------------------
template<typename T>
static void printUidGid(std::ofstream& ss, const eos::console::FindProto& req,
                        const T& md)
{
  ss << "\t";

  if (req.printuid()) {
    ss << " uid=" << md->getCUid();
  }
  if (req.printgid()) {
    ss << " gid=" << md->getCGid();
  }

}


template<typename T>
static void printAttributes(std::ofstream& ss,
                const eos::console::FindProto& req, const T& md) {

  if (!req.printkey().empty()) {
    std::string attr;
    if (!gOFS->_attr_get(*md.get(), req.printkey(), attr)) {
      attr = "undef";
    }
    ss << "\t" << req.printkey() << "=" << attr;
  }

}

//------------------------------------------------------------------------------
// Print directories and files count of a ContainerMD, if requested by req.
//------------------------------------------------------------------------------
static void printChildCount(std::ofstream& ss, const eos::console::FindProto& req,
                            const std::shared_ptr<eos::IContainerMD>& cmd)
{
  if (req.childcount()) {
    ss << "\t"
       <<" ndirs=" << cmd->getNumContainers()
       << " nfiles=" << cmd->getNumFiles();
  }

}


//------------------------------------------------------------------------------
// Print hex checksum of given fmd, if requested by req.
//------------------------------------------------------------------------------
static void printChecksum(std::ofstream& ss, const eos::console::FindProto& req,
                          const std::shared_ptr<eos::IFileMD>& fmd)
{
  if (req.checksum()) {
    ss << " checksum=";
    std::string checksum;
    eos::appendChecksumOnStringAsHex(fmd.get(), checksum);
    ss << checksum;
  }
}

//------------------------------------------------------------------------------
// Print replica location of an fmd.
//------------------------------------------------------------------------------
static void printReplicas(std::ofstream& ss,
                          const std::shared_ptr<eos::IFileMD>& fmd, bool onlyhost, bool selectonline)
{
  if (onlyhost) {
    ss << " hosts=";
  } else {
    ss << " partition=";
  }

  std::set<std::string> results;

  for (auto lociter : fmd->getLocations()) {
    // lookup filesystem
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
    eos::common::FileSystem* filesystem = FsView::gFsView.mIdView.lookupByID(
        lociter);

    if (!filesystem) {
      continue;
    }

    eos::common::FileSystem::fs_snapshot_t fs;

    if (filesystem->SnapShotFileSystem(fs, true)) {
      if (selectonline && filesystem->GetActiveStatus(true) !=
                          eos::common::ActiveStatus::kOnline) {
        continue;
      }

      std::string item;

      if (onlyhost) {
        item = fs.mHost;
      } else {
        item = fs.mHost;
        item += ":";
        item += fs.mPath;
      }

      results.insert(item);
    }
  }

  for (auto it = results.begin(); it != results.end(); it++) {
    if (it != results.begin()) {
      ss << ",";
    }

    ss << it->c_str();
  }
}

//------------------------------------------------------------------------------
// Print fs of a FileMD.
//------------------------------------------------------------------------------
static void printFs(std::ofstream& ss, const std::shared_ptr<eos::IFileMD>& fmd)
{
  ss << " fsid=";
  eos::IFileMD::LocationVector loc_vect = fmd->getLocations();
  eos::IFileMD::LocationVector::const_iterator lociter;

  for (lociter = loc_vect.begin(); lociter != loc_vect.end(); ++lociter) {
    if (lociter != loc_vect.begin()) {
      ss << ',';
    }

    ss << *lociter;
  }
}

//------------------------------------------------------------------------------
// Print a selected FileMD, according to formatting settings in req.
//------------------------------------------------------------------------------
static void printFMD(std::ofstream& ss, const eos::console::FindProto& req,
                     const std::shared_ptr<eos::IFileMD>& fmd)
{
  if (req.size()) {
    ss << " size=" << fmd->getSize();
  }

  if (req.fid()) {
    ss << " fid=" << fmd->getId();
  }

  if (req.printuid()) {
    ss << " uid=" << fmd->getCUid();
  }
  if (req.printgid()) {
    ss << " gid=" << fmd->getCGid();
  }

  if (req.fs()) {
    printFs(ss, fmd);
  }

  if (req.partition()) {
    printReplicas(ss, fmd, false, req.online());
  }

  if (req.hosts()) {
    printReplicas(ss, fmd, true, req.online());
  }

  printChecksum(ss, req, fmd);

  if (req.ctime()) {
    eos::IFileMD::ctime_t ctime;
    fmd->getCTime(ctime);
    ss << " ctime=" << (unsigned long long) ctime.tv_sec;
    ss << '.' << (unsigned long long) ctime.tv_nsec;
  }

  if (req.mtime()) {
    eos::IFileMD::ctime_t mtime;
    fmd->getMTime(mtime);
    ss << " mtime=" << (unsigned long long) mtime.tv_sec;
    ss << '.' << (unsigned long long) mtime.tv_nsec;
  }

  if (req.nrep()) {
    ss << " nrep=" << fmd->getNumLocation();
  }

  if (req.nunlink()) {
    ss << " nunlink=" << fmd->getNumUnlinkedLocation();
  }
}


//------------------------------------------------------------------------------
// Purge atomic files
//------------------------------------------------------------------------------
void
NewfindCmd::ProcessAtomicFilePurge(std::ofstream& ss,
                                     const std::string& fspath,
                                     eos::IFileMD& fmd)
{
  if (fspath.find(EOS_COMMON_PATH_ATOMIC_FILE_PREFIX) == std::string::npos) {
    return;
  }

  ss << "# found atomic " << fspath << std::endl;

  if (!(mVid.uid == 0 || mVid.uid == fmd.getCUid())) {
    // Not allowed to remove file
    ss << "# skipping atomic " << fspath << " [no permission to remove]" <<
       std::endl;
    return;
  }

  time_t now = time(nullptr);
  eos::IFileMD::ctime_t atime;
  fmd.getCTime(atime);

  //----------------------------------------------------------------------------
  // Is the file older than 1 day?
  //----------------------------------------------------------------------------
  if (now - atime.tv_sec <= 86400) {
    ss << "# skipping atomic " << fspath << " [< 1d old ]" << std::endl;
    return;
  }

  //----------------------------------------------------------------------------
  // Perform the rm
  //----------------------------------------------------------------------------
  XrdOucErrInfo errInfo;

  if (!gOFS->_rem(fspath.c_str(), errInfo, mVid, (const char*) nullptr)) {
    ss << "# purging atomic " << fspath;
  } else {
    ss << "# could not purge atomic " << fspath;
  }
}

//------------------------------------------------------------------------------
// Purge version directory.
//------------------------------------------------------------------------------
void
NewfindCmd::PurgeVersions(std::ofstream& ss, int64_t maxVersion,
                          const std::string& dirpath)
{
  if (dirpath.find(EOS_COMMON_PATH_VERSION_PREFIX) == std::string::npos) {
    return;
  }

  struct stat buf;

  XrdOucErrInfo errInfo;

  if ((!gOFS->_stat(dirpath.c_str(), &buf, errInfo, mVid, nullptr,
                    nullptr)) && ((mVid.uid == 0) || (mVid.uid == buf.st_uid))) {
    ss << "# purging " << dirpath;
    gOFS->PurgeVersion(dirpath.c_str(), errInfo, maxVersion);
  }
}

//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Modify layout stripes
//------------------------------------------------------------------------------
void
NewfindCmd::ModifyLayoutStripes(std::ofstream& ss,
                                       const eos::console::FindProto& req,
                                       const std::string& fspath)
{
  XrdOucErrInfo errInfo;
  ProcCommand fileCmd;
  std::string info = "mgm.cmd=file&mgm.subcmd=layout&mgm.path=";
  info += fspath;
  info += "&mgm.file.layout.stripes=";
  info += std::to_string(req.layoutstripes());

  if (fileCmd.open("/proc/user", info.c_str(), mVid, &errInfo) == 0) {
    std::ostringstream outputStream;
    XrdSfsFileOffset offset = 0;
    constexpr uint32_t size = 512;
    auto bytesRead = 0ul;
    char buffer[size];

    do {
      bytesRead = fileCmd.read(offset, buffer, size);

      for (auto i = 0u; i < bytesRead; i++) {
        outputStream << buffer[i];
      }

      offset += bytesRead;
    } while (bytesRead == size);

    fileCmd.close();
    XrdOucEnv env(outputStream.str().c_str());

    if (std::stoi(env.Get("mgm.proc.retc")) == 0) {
      if (!req.silent()) {
        ofstdoutStream << env.Get("mgm.proc.stdout") << std::endl;
      }
    } else {
      ofstderrStream << env.Get("mgm.proc.stderr") << std::endl;
    }
  }
}

// Find result struct
//------------------------------------------------------------------------------
struct FindResult {
  std::string path;
  bool isdir;
  bool expansionFilteredOut;
  std::pair<bool, uint32_t> publicAccessAllowed; // second

  uint64_t numFiles = 0;
  uint64_t numContainers = 0;

  std::shared_ptr<eos::IContainerMD> toContainerMD()
  {
    eos::common::RWMutexReadLock eosViewMutexGuard(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__, __FILE__);

    try {
      return gOFS->eosView->getContainer(path);
    } catch (eos::MDException& e) {
      eos_static_err("caught exception %d %s\n", e.getErrno(),
                     e.getMessage().str().c_str());
      return {};
    }

  }

  std::shared_ptr<eos::IFileMD> toFileMD()
  {
    eos::common::RWMutexReadLock eosViewMutexGuard(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__, __FILE__);

    try {
      return gOFS->eosView->getFile(path);
    } catch (eos::MDException& e) {
      eos_static_err("caught exception %d %s\n", e.getErrno(),
                     e.getMessage().str().c_str());
      return {};
    }

  }
};

//------------------------------------------------------------------------------
// Filter-out directories which we have no permission to access
//------------------------------------------------------------------------------
class PermissionFilter : public ExpansionDecider
{
public:
  PermissionFilter(const eos::common::VirtualIdentity& v) : vid(v) {}

  virtual bool shouldExpandContainer(const eos::ns::ContainerMdProto& proto,
                                     const eos::IContainerMD::XAttrMap& attrs) override
  {
    eos::QuarkContainerMD cmd;
    cmd.initializeWithoutChildren(eos::ns::ContainerMdProto(proto));

    return AccessChecker::checkContainer(&cmd, attrs, R_OK | X_OK, vid)
        && AccessChecker::checkPublicAccess(gOFS->eosView->getUri(cmd.getId()),vid).first;
  }

private:
  const eos::common::VirtualIdentity& vid;
};

//------------------------------------------------------------------------------
// Find result provider class
//------------------------------------------------------------------------------
class FindResultProvider {
public:

  //----------------------------------------------------------------------------
  // QDB: Initialize NamespaceExplorer
  //----------------------------------------------------------------------------
  FindResultProvider(qclient::QClient* qc, const std::string& target, const uint32_t maxdepth, const bool ignore_files,
                     const eos::common::VirtualIdentity& v)
    : qcl(qc), path(target), maxdepth(maxdepth), ignore_files(ignore_files), vid(v)
  {
    restart();
  }

  //----------------------------------------------------------------------------
  // Restart
  //----------------------------------------------------------------------------
  void restart() {
    if(!found) {
      ExplorationOptions options;
      options.populateLinkedAttributes = true;
      options.expansionDecider.reset(new PermissionFilter(vid));
      options.view = gOFS->eosView;
      options.depthLimit = maxdepth;
      options.ignoreFiles = ignore_files;
      explorer.reset(new NamespaceExplorer(path, options, *qcl,
                                           static_cast<QuarkNamespaceGroup*>(gOFS->namespaceGroup.get())->getExecutor()));
    }
  }

  //----------------------------------------------------------------------------
  // In-memory: Check whether we need to take deep query mutex lock
  //----------------------------------------------------------------------------
  FindResultProvider(bool deepQuery)
  {
    if (deepQuery) {
      static eos::common::RWMutex deepQueryMutex;
      static std::unique_ptr<std::map<std::string, std::set<std::string>>>
      globalfound;
      deepQueryMutexGuard.Grab(deepQueryMutex);

      if (!globalfound) {
        globalfound.reset(new std::map<std::string, std::set<std::string>>());
      }

      found = globalfound.get();
    } else {
      localfound.reset(new std::map<std::string, std::set<std::string>>());
      found = localfound.get();
    }
  }

  ~FindResultProvider()
  {
    if (found) {
      found->clear();
    }
  }

  //----------------------------------------------------------------------------
  // In-memory: Get map for holding content results
  //----------------------------------------------------------------------------
  std::map<std::string, std::set<std::string>>* getFoundMap()
  {
    return found;
  }

  bool nextInMemory(FindResult& res)
  {
    res.expansionFilteredOut = false;

    // Search not started yet?
    if (!inMemStarted) {
      dirIterator = found->begin();
      targetFileSet = &dirIterator->second;
      fileIterator = targetFileSet->begin();
      inMemStarted = true;
      res.path = dirIterator->first;
      res.isdir = true;
      return true;
    }

    // At the end of a file iterator? Give out next directory.
    if (fileIterator == targetFileSet->end()) {
      dirIterator++;

      if (dirIterator == found->end()) {
        // Search has ended.
        return false;
      }

      targetFileSet = &dirIterator->second;
      fileIterator = targetFileSet->begin();
      res.path = dirIterator->first;
      res.isdir = true;
      return true;
    }

    // Nope, just give out another file.
    res.path = dirIterator->first + *fileIterator;
    res.isdir = false;
    fileIterator++;
    return true;
  }

  bool nextInQDB(FindResult& res)
  {
    // Just copy the result given by namespace explorer
    NamespaceItem item;

    if (!explorer->fetch(item)) {
      return false;
    }

    res.path = item.fullPath;
    res.isdir = !item.isFile;
    res.expansionFilteredOut = item.expansionFilteredOut;
    res.publicAccessAllowed = AccessChecker::checkPublicAccess(item.fullPath,
                                                               const_cast<common::VirtualIdentity&>(vid));

    if (item.isFile) {
      res.numFiles = 0;
      res.numContainers = 0;
    } else {
      res.numFiles = item.numFiles;
      res.numContainers = item.numContainers;
    }

    return true;
  }

  bool next(FindResult& res)
  {
    if (found) {
      // In-memory case
      return nextInMemory(res);
    }

    // QDB case
    return nextInQDB(res);
  }

private:
  //----------------------------------------------------------------------------
  // In-memory: Map holding results
  //----------------------------------------------------------------------------
  eos::common::RWMutexWriteLock deepQueryMutexGuard;
  std::unique_ptr<std::map<std::string, std::set<std::string>>> localfound;
  std::map<std::string, std::set<std::string>>* found = nullptr;
  bool inMemStarted = false;

  std::map<std::string, std::set<std::string>>::iterator dirIterator;
  std::set<std::string>* targetFileSet = nullptr;
  std::set<std::string>::iterator fileIterator;

  //----------------------------------------------------------------------------
  // QDB: NamespaceExplorer and QClient
  //----------------------------------------------------------------------------
  qclient::QClient* qcl = nullptr;
  std::string path;
  uint32_t maxdepth;
  bool ignore_files;
  std::unique_ptr<NamespaceExplorer> explorer;
  eos::common::VirtualIdentity vid;
};

//------------------------------------------------------------------------------
// Method implementing the specific behaviour of the command executed
//------------------------------------------------------------------------------
eos::console::ReplyProto
NewfindCmd::ProcessRequest() noexcept
{
  XrdOucString m_err {""};
  eos::console::ReplyProto reply;

  if (!OpenTemporaryOutputFiles()) {
    reply.set_retc(EIO);
    reply.set_std_err(SSTR("error: cannot write find result files on MGM" << std::endl));
    return reply;
  }

  const eos::console::FindProto& findRequest = mReqProto.find();
  auto& purgeversion = findRequest.purge();
  bool purge = false;
  bool purge_atomic = (purgeversion == "atomic");
  auto max_version = 999999ul;

  if (!purge_atomic) {
    try {
      max_version = std::stoul(purgeversion);
      purge = true;
    } catch (std::logic_error& err) {
      // this error is handled at client side, should not receive bad input from client
    }
  }

  // This hash is used to calculate the balance of the found files over the
  // filesystems involved
  eos::BalanceCalculator balanceCalculator;
  eos::common::Path cPath(findRequest.path().c_str());
  bool deepquery = cPath.GetSubPathSize() < 5 && (!findRequest.directories() ||
                   findRequest.files());
  XrdOucErrInfo errInfo;
  // check what <path> actually is ...
  XrdSfsFileExistence file_exists;

  if ((gOFS->_exists(findRequest.path().c_str(), file_exists, errInfo, mVid, nullptr))) {
    ofstderrStream << "error: failed to run exists on '" << findRequest.path() << "'" <<
                   std::endl;
    reply.set_retc(errno);
    return reply;
  } else {
    if (file_exists == XrdSfsFileExistNo) {
      ofstderrStream << "error: no such file or directory" << std::endl;
      reply.set_retc(ENOENT);
      return reply;
    }
  }

  errInfo.clear();
  std::unique_ptr<FindResultProvider> findResultProvider;

  if (!gOFS->NsInQDB) {
    findResultProvider.reset(new FindResultProvider(deepquery));
    std::map<std::string, std::set<std::string>>* found =
          findResultProvider->getFoundMap();

    if (gOFS->_find(findRequest.path().c_str(), errInfo, m_err, mVid, (*found),
                    findRequest.attributekey().length() ? findRequest.attributekey().c_str() : nullptr,
                    findRequest.attributevalue().length() ? findRequest.attributevalue().c_str() : nullptr,
                    findRequest.directories(), 0, true, findRequest.maxdepth(),
                    findRequest.name().length() ? findRequest.name().c_str() : nullptr)) {
      ofstderrStream << "error: unable to run find in directory" << std::endl;
      reply.set_retc(errno);
      return reply;
    } else {
      if (m_err.length()) {
        ofstderrStream << m_err;
        reply.set_retc(E2BIG);
      }
    }
  } else {
    // @note when findRequest.childcount() is true, the namespace explorer will skip the files during the namespace traversal.
    // This way we can have a fast aggregate sum of the file/container count for each directory
    findResultProvider.reset(new FindResultProvider(
                               eos::BackendClient::getInstance(gOFS->mQdbContactDetails, "find"),
                               findRequest.path(), findRequest.maxdepth(), findRequest.childcount(), mVid));
  }

  uint64_t dircounter = 0;
  uint64_t filecounter = 0;
  // Users cannot return more than 50k dirs and 100k files with one find,
  // unless there is an access rule allowing deeper queries
  static uint64_t dir_limit = 50000;
  static uint64_t file_limit = 100000;
  Access::GetFindLimits(mVid, dir_limit, file_limit);


  // @note assume that findResultProvider will serve results DFS-ordered
  FindResult findResult;
  std::shared_ptr<eos::IContainerMD> cMD;
  std::shared_ptr<eos::IFileMD> fMD;
  while (findResultProvider->next(findResult)) {

    if (dircounter>=dir_limit || filecounter>=file_limit) {
      ofstderrStream << "warning(" << E2BIG << "): find results are limited for you to "
          << dir_limit << " directories and " << file_limit << " files.\n"
          << "Result is truncated! (found "
          << dircounter << " directories and " << filecounter << " files so far)\n";
      reply.set_retc(E2BIG);
      break;
    }

    if (findResult.isdir) {
      if (!findRequest.directories() && findRequest.files()) { continue;}
      if (findResult.expansionFilteredOut) {
        ofstderrStream << "error(" << EACCES << "): no permissions to read directory ";
        ofstderrStream << findResult.path << std::endl;
        reply.set_retc(EACCES);
        continue;
      }

      cMD = findResult.toContainerMD();
      // Process next item if we don't have a cMD
      if (!cMD) { continue; }

      // Selection
      if (eliminateBasedOnMaxDepth(findRequest, findResult.path)) { continue; }
      if (FilterOut(findRequest, cMD)) { continue; }

      dircounter++;

      if (findRequest.count()) { continue; }

      // Purge version directory?
      if (purge) {
        this->PurgeVersions(ofstdoutStream, max_version, findResult.path);
        continue;
      }

      // Printing

      // Are we printing fileinfo -m? Then, that's it
      if (findRequest.fileinfo()) {
        this->PrintFileInfoMinusM(findResult.path, errInfo);
        continue;
      }

      printPath(ofstdoutStream, findResult.path, findRequest.xurl());
      printUidGid(ofstdoutStream, findRequest, cMD);
      printChildCount(ofstdoutStream,findRequest,cMD);
      printAttributes(ofstdoutStream, findRequest, cMD);
      ofstdoutStream << std::endl;


    } else if (!findResult.isdir) { // redundant, no problem
      if (!findRequest.files() && findRequest.directories()) { continue;}

      fMD = findResult.toFileMD();
      // Process next item if we don't have a fMD
      if (!fMD) { continue; }

      // Balance calculation? If yes,
      // ignore selection criteria (TODO: Change this?)
      // and simply account all fMD's.
      if (findRequest.balance()) {
        balanceCalculator.account(fMD);
        continue;
      }

      // Selection
      if (eliminateBasedOnMaxDepth(findRequest, findResult.path)) { continue; }
      if (FilterOut(findRequest, fMD)) { continue; }
      if (findRequest.zerosizefiles() && !hasSizeZero(fMD)) { continue; }
      if (findRequest.mixedgroups() && !hasMixedSchedGroups(fMD)) { continue; }
      if (findRequest.stripediff() && hasStripeDiff(fMD)) { continue; } // @note or the opposite?

      filecounter++;

      if (findRequest.count()) { continue; }

      // Purge atomic files?
      if (purge_atomic) {
        this->ProcessAtomicFilePurge(ofstdoutStream, findResult.path, *fMD.get());
        continue;
      }
      // Modify layout stripes?
      if (findRequest.dolayoutstripes()) {
        this->ModifyLayoutStripes(ofstdoutStream, findRequest, findResult.path);
        continue;
      }


      // Printing

      // Are we printing fileinfo -m? Then, that's it
      if (findRequest.fileinfo()) {
        this->PrintFileInfoMinusM(findResult.path, errInfo);
        continue;
      }

      printPath(ofstdoutStream,fMD->isLink() ? findResult.path + " -> " + fMD->getLink() : findResult.path, findRequest.xurl());
      printUidGid(ofstdoutStream, findRequest, fMD);
      printAttributes(ofstdoutStream, findRequest, fMD);
      printFMD(ofstdoutStream, findRequest, fMD);
      ofstdoutStream << std::endl;

    }

  }

  if (findRequest.count()) {
    ofstdoutStream << "nfiles=" << filecounter << " ndirectories=" << dircounter <<
                   std::endl;
  }
  if (findRequest.balance()) {
    balanceCalculator.printSummary(ofstdoutStream);
  }

  if (!CloseTemporaryOutputFiles()) {
    reply.set_retc(EIO);
    reply.set_std_err("error: cannot save find result files on MGM\n");
    return reply;
  }

  return reply;
}

//------------------------------------------------------------------------------
// Get fileinfo about path in monitoring format
//------------------------------------------------------------------------------
void
NewfindCmd::PrintFileInfoMinusM(const std::string& path,
                                  XrdOucErrInfo& errInfo)
{
  // print fileinfo -m
  ProcCommand Cmd;
  XrdOucString lStdOut = "";
  XrdOucString lStdErr = "";
  XrdOucString info = "&mgm.cmd=fileinfo&mgm.path=";
  info += path.c_str();
  info += "&mgm.file.info.option=-m";
  Cmd.open("/proc/user", info.c_str(), mVid, &errInfo);
  Cmd.AddOutput(lStdOut, lStdErr);

  if (lStdOut.length()) {
    ofstdoutStream << lStdOut << std::endl;
  }

  if (lStdErr.length()) {
    ofstderrStream << lStdErr << std::endl;
  }

  Cmd.close();
}


EOSMGMNAMESPACE_END
