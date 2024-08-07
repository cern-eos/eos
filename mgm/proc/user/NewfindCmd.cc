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

#include <regex>

#include "NewfindCmd.hh"
#include "common/LayoutId.hh"
#include "common/Path.hh"
#include "common/Timing.hh"
#include "mgm/Acl.hh"
#include "mgm/FsView.hh"
#include "mgm/Stat.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/auth/AccessChecker.hh"
#include "namespace/interface/IView.hh"
#include "namespace/ns_quarkdb/ContainerMD.hh"
#include "namespace/ns_quarkdb/FileMD.hh"
#include "namespace/ns_quarkdb/NamespaceGroup.hh"
#include "namespace/ns_quarkdb/explorer/NamespaceExplorer.hh"
#include "namespace/utils/BalanceCalculator.hh"
#include "namespace/utils/Checksum.hh"
#include "namespace/utils/Stat.hh"
#include "namespace/utils/Etag.hh"
#include <mgm/Access.hh>
#include <namespace/Prefetcher.hh>

EOSMGMNAMESPACE_BEGIN


template<typename T>
static bool eliminateBasedOnFileMatch(const eos::console::FindProto& req,
                                      const T& md)
{
  std::string toFilter = md->getName();
  std::regex filter(req.name(), std::regex_constants::egrep);
  return (!req.name().empty()) && (!std::regex_search(toFilter, filter));
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
static bool eliminateBasedOnFaultyAcl(const eos::console::FindProto& req,
                                      const T& md)
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
static bool FilterOut(const eos::console::FindProto& req,
                      const T& md)
{
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
static bool hasSizeZero(std::shared_ptr<eos::IFileMD>& fmd)
{
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
static bool hasStripeDiff(std::shared_ptr<eos::IFileMD>& fmd)
{
  return (fmd->getNumLocation() == eos::common::LayoutId::GetStripeNumber(
            fmd->getLayoutId()) + 1);
}


//------------------------------------------------------------------------------
// Print path.
//------------------------------------------------------------------------------
template<typename S>   // std::ofstream or std::stringstream
static void printPath(S& ss, const eos::console::FindProto& req,
                      const std::string& path)
{
  if (req.format().length() || req.treecount()) {
    ss << "path=\"";
  }

  if (req.xurl()) {
    ss << "root://" << gOFS->MgmOfsAlias << "/";
  }

  ss << path;

  if (req.format().length() || req.treecount()) {
    ss << "\"";
  }
}

//------------------------------------------------------------------------------
// Print target.
//------------------------------------------------------------------------------
template<typename S>   // std::ofstream or std::stringstream
static void printTarget(S& ss, const eos::console::FindProto& req,
                        const std::string& path)
{
  if (!path.empty()) {
    ss << " target=\"" << path << "\"";
  }
}

//------------------------------------------------------------------------------
// Print uid / gid of a FileMD or ContainerMD, if requested by req.
//------------------------------------------------------------------------------
template<typename S, typename T>
static void printUidGid(S& ss, const eos::console::FindProto& req, const T& md)
{
  if (req.format().length()) {
    return;
  }

  if (req.printuid()) {
    ss << " uid=" << md->getCUid();
  }

  if (req.printgid()) {
    ss << " gid=" << md->getCGid();
  }
}


template<typename S, typename T>
static void printAttributes(S& ss,
                            const eos::console::FindProto& req, const T& md)
{
  if (req.format().length()) {
    return;
  }

  if (!req.printkey().empty()) {
    std::string attr;

    if (!gOFS->_attr_get(*md.get(), req.printkey(), attr)) {
      attr = "undef";
    }

    ss << " " << req.printkey() << "=" << attr;
  }
}

//------------------------------------------------------------------------------
// Print directories and files count of a ContainerMD, if requested by req.
//------------------------------------------------------------------------------
static void printChildCount(std::ofstream& ss,
                            const eos::console::FindProto& req,
                            const std::shared_ptr<eos::IContainerMD>& cmd, size_t ndirs, size_t nfiles)
{
  if (req.format().length()) {
    return;
  }

  if (req.childcount()) {
    ss << " ndirs=" << ndirs
       << " nfiles=" << nfiles;
  }
}

//------------------------------------------------------------------------------
// Print du information
//------------------------------------------------------------------------------
static void printDu(std::ofstream& ss, const eos::console::FindProto& req,
                    const std::shared_ptr<eos::IContainerMD>& cmd, size_t ndirs, size_t nfiles)
{
  if (req.du()) {
    bool si = req.dusi();
    bool readable = req.dureadable();
    size_t treesize = cmd->getTreeSize();
    std::string size = readable ?
                       eos::common::StringConversion::GetReadableSizeString(treesize,
                           si ? ((treesize > (10 * si)) ? "iB" : "B") : "B",
                           si ? 1024 : 1000) : std::to_string(treesize);
    ss << std::left << std::setw(16) << size << " ";
  }
}

//------------------------------------------------------------------------------
// Print user defined format
//------------------------------------------------------------------------------
static void printFormat(std::ofstream& ss, const eos::console::FindProto& req,
                        const std::shared_ptr<eos::IContainerMD>& cmd, size_t ndirs, size_t nfiles)
{
  if (req.format().length()) {
    std::vector<std::string> tokens;
    eos::common::StringConversion::Tokenize(req.format(),
                                            tokens, ",");

    for (auto i : tokens) {
      if (i == "type") {
        ss << " type=directory ";
      }

      if (i == "size") {
        ss << " size=" << std::to_string(cmd->getTreeSize());
      }

      if (i == "cxid") {
        ss << " cxid="  << std::hex << cmd->getId() << std::dec;
      }

      if (i == "pxid") {
        ss << " pxid="  << std::hex << cmd->getParentId() << std::dec;
      }

      if (i == "cid") {
        ss << " cid="  << cmd->getId();
      }

      if (i == "pid") {
        ss << " pid="  << cmd->getParentId();
      }

      if (i == "uid") {
        ss << " uid=" << cmd->getCUid();
      }

      if (i == "gid") {
        ss << " gid=" << cmd->getCGid();
      }

      if (i == "mode") {
        ss << " mode=" << std::oct << cmd->getMode() << std::dec;
      }

      if (i == "files") {
        ss << " files=" << ndirs;
      }

      if (i == "directories") {
        ss << " directories=" << nfiles;
      }

      if (i == "mtime") {
        eos::IFileMD::ctime_t mtime {0, 0};
        cmd->getMTime(mtime);
        ss << " mtime=" << eos::common::Timing::TimespecToString(mtime);
      }

      if (i == "btime") {
        eos::IFileMD::ctime_t btime {0, 0};

        if (cmd->getAttributes().count("sys.eos.btime")) {
          eos::common::Timing::Timespec_from_TimespecStr(
            cmd->getAttributes()["sys.eos.btime"], btime);
        }

        ss << " btime=" << eos::common::Timing::TimespecToString(btime);
      }

      if (i == "ctime") {
        eos::IFileMD::ctime_t ctime {0, 0};
        cmd->getCTime(ctime);
        ss << " ctime=" << eos::common::Timing::TimespecToString(ctime);
      }

      if (i == "etag") {
        std::string etag;
        eos::calculateEtag(cmd.get(), etag);
        ss << " etag=" << etag;
      }

      if (i.substr(0, 5) == "attr.") {
        std::string attr = i.substr(5);

        if (attr == "*") {
          for (const auto& elem : cmd->getAttributes()) {
            ss << " attr." << elem.first << "=" << "\"" << elem.second << "\"";
          }
        } else {
          if (cmd->getAttributes().count(attr)) {
            ss << " " << i << "=\"" << cmd->getAttributes()[attr] << "\"";
          }
        }
      }
    }
  }
}

//------------------------------------------------------------------------------
// Print du information
//------------------------------------------------------------------------------
static void printDu(std::ofstream& ss, const eos::console::FindProto& req,
                    const std::shared_ptr<eos::IFileMD>& fmd)
{
  if (req.du()) {
    bool si = req.dusi();
    bool readable = req.dureadable();
    size_t filesize = fmd->getSize();
    std::string size = readable ?
                       eos::common::StringConversion::GetReadableSizeString(filesize,
                           si ? ((filesize > (10 * si)) ? "iB" : "B") : "B",
                           si ? 1024 : 1000) : std::to_string(filesize);
    ss << std::left << std::setw(16) << size << " ";
  }
}

//------------------------------------------------------------------------------
// Print user defined format
//------------------------------------------------------------------------------
static void printFormat(std::ofstream& ss, const eos::console::FindProto& req,
                        const std::shared_ptr<eos::IFileMD>& fmd)
{
  if (req.format().length()) {
    std::vector<std::string> tokens;
    eos::common::StringConversion::Tokenize(req.format(),
                                            tokens, ",");

    for (auto i : tokens) {
      if (i == "type") {
        if (fmd->isLink()) {
          ss << " type=symlink ";
        } else {
          ss << " type=file ";
        }
      }

      if (i == "link") {
        printTarget(ss, req, fmd->getLink());
      }

      if (i == "size") {
        ss << " size=" << std::to_string(fmd->getSize());
      }

      if (i == "fxid") {
        ss << " fxid="  << std::hex << fmd->getId() << std::dec;
      }

      if (i == "cxid") {
        ss << " cxid="  << std::hex << fmd->getContainerId() << std::dec;
      }

      if (i == "fid") {
        ss << " fid="  << fmd->getId();
      }

      if (i == "cid") {
        ss << " cid="  << fmd->getContainerId();
      }

      if (i == "uid") {
        ss << " uid=" << fmd->getCUid();
      }

      if (i == "gid") {
        ss << " gid=" << fmd->getCGid();
      }

      if (i == "flags") {
        ss << " flags=" << std::oct << fmd->getFlags() << std::dec;
      }

      if (i == "atime") {
        eos::IFileMD::ctime_t atime {0, 0};
        fmd->getCTime(atime);
        ss << " atime=" << eos::common::Timing::TimespecToString(atime);
      }

      if (i == "mtime") {
        eos::IFileMD::ctime_t mtime {0, 0};
        fmd->getMTime(mtime);
        ss << " mtime=" << eos::common::Timing::TimespecToString(mtime);
      }

      if (i == "btime") {
        eos::IFileMD::ctime_t btime {0, 0};

        if (fmd->getAttributes().count("sys.eos.btime")) {
          eos::common::Timing::Timespec_from_TimespecStr(
            fmd->getAttributes()["sys.eos.btime"], btime);
        }

        ss << " btime=" << eos::common::Timing::TimespecToString(btime);
      }

      if (i == "ctime") {
        eos::IFileMD::ctime_t ctime {0, 0};
        fmd->getCTime(ctime);
        ss << " ctime=" << eos::common::Timing::TimespecToString(ctime);
      }

      if (i == "etag") {
        std::string etag;
        eos::calculateEtag(fmd.get(), etag);
        ss << " etag=" << etag;
      }

      if (i == "checksum") {
        std::string xs;
        eos::appendChecksumOnStringAsHex(fmd.get(), xs);
        ss << " checksum=" << xs;
      }

      if (i == "checksumtype") {
        ss << " checksumtype=" << eos::common::LayoutId::GetChecksumString(
             fmd->getLayoutId());
      }

      if (i.substr(0, 5) == "attr.") {
        std::string attr = i.substr(5);

        if (attr == "*") {
          for (const auto& elem : fmd->getAttributes()) {
            ss << " attr." << elem.first << "=" << "\"" << elem.second << "\"";
          }
        } else {
          if (fmd->getAttributes().count(attr)) {
            ss << " " << i << "=\"" << fmd->getAttributes()[attr] << "\"";
          }
        }
      }
    }
  }
}


//------------------------------------------------------------------------------
// Print hex checksum of given fmd, if requested by req.
//------------------------------------------------------------------------------
template<typename S>   // std::ofstream or std::stringstream
static void printChecksum(S& ss, const eos::console::FindProto& req,
                          const std::shared_ptr<eos::IFileMD>& fmd)
{
  if (req.format().length()) {
    return;
  }

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
template<typename S>   // std::ofstream or std::stringstream
static void printReplicas(S& ss, const std::shared_ptr<eos::IFileMD>& fmd,
                          bool onlyhost, bool selectonline)
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
      if (selectonline && filesystem->GetActiveStatus() !=
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
template<typename S>   // std::ofstream or std::stringstream
static void printFs(S& ss, const std::shared_ptr<eos::IFileMD>& fmd)
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
template<typename S>   // std::ofstream or std::stringstream
static void printFMD(S& ss, const eos::console::FindProto& req,
                     const std::shared_ptr<eos::IFileMD>& fmd)
{
  if (req.format().length()) {
    return;
  }

  if (req.size()) {
    ss << " size=" << fmd->getSize();
  }

  if (req.fid()) {
    ss << " fid=" << fmd->getId();
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
template<typename S>   // std::ofstream or std::stringstream
void
NewfindCmd::ProcessAtomicFilePurge(S& ss,
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
template<typename S>   // std::ofstream or std::stringstream
void
NewfindCmd::PurgeVersions(S& ss, int64_t maxVersion,
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
// Filter-out directories which we have no permission to access
//------------------------------------------------------------------------------
class TraversalFilter : public ExpansionDecider
{
public:
  TraversalFilter(const eos::common::VirtualIdentity& v) : vid(v) {}

  virtual bool shouldExpandContainer(const eos::ns::ContainerMdProto& proto,
                                     const eos::IContainerMD::XAttrMap& attrs,
                                     const std::string& fullPath) override
  {
    eos::QuarkContainerMD cmd;
    cmd.initializeWithoutChildren(eos::ns::ContainerMdProto(proto));
    return AccessChecker::checkContainer(&cmd, attrs, R_OK | X_OK, vid)
           && AccessChecker::checkPublicAccess(fullPath, vid);
  }
private:
  const eos::common::VirtualIdentity& vid;
};


//------------------------------------------------------------------------------
// Find result struct
//------------------------------------------------------------------------------
struct FindResult {
  std::string path;
  bool isdir;
  bool iscache;
  bool expansionFilteredOut;
  // Filled out as long as populateLinkedAttributes set
  eos::IContainerMD::XAttrMap attrs;
  uint64_t numFiles = 0;
  uint64_t numContainers = 0;
  NamespaceItem item;

  //----------------------------------------------------------------------------
  // Convert FindResult of IContainerMD object
  //----------------------------------------------------------------------------
  std::shared_ptr<eos::IContainerMD> toContainerMD()
  {
    if (iscache) {
      try {
        auto cmd = gOFS->eosView->getContainer(path);
        numFiles = cmd->getNumFiles();
        numContainers = cmd->getNumContainers();
        return cmd;
      } catch (eos::MDException& e) {}

      return {};
    } else {
      if (item.isFile) {
        return {};
      } else {
        auto p = std::make_shared<eos::QuarkContainerMD>();
        p->initializeWithoutChildren(eos::ns::ContainerMdProto(item.containerMd));

        // copy xattributes
        for (auto i : item.attrs) {
          p->setAttribute(i.first, i.second);
        }

        return p;
      }
    }
  }

  //--------------------------------------------------------------------------
  //! Convert FindResult of IFileMD object
  //--------------------------------------------------------------------------
  std::shared_ptr<eos::IFileMD> toFileMD()
  {
    if (iscache) {
      try {
        return gOFS->eosView->getFile(path);
      } catch (eos::MDException& e) {}

      return {};
    } else {
      if (item.isFile) {
        auto p = std::make_shared<eos::QuarkFileMD>();
        p->initialize(eos::ns::FileMdProto(item.fileMd));

        // copy xattributes
        for (auto i : item.attrs) {
          p->setAttribute(i.first, i.second);
        }

        return p;
      } else {
        return {};
      }
    }
  }
};


//------------------------------------------------------------------------------
// Find result provider class
//------------------------------------------------------------------------------
class FindResultProvider
{
public:

  //----------------------------------------------------------------------------
  // QDB: Initialize NamespaceExplorer
  //----------------------------------------------------------------------------
  FindResultProvider(qclient::QClient* qc, const std::string& target,
                     const uint32_t depthlimit, const bool ignore_files,
                     const eos::common::VirtualIdentity& v)
    : qcl(qc), path(target), depthlimit(depthlimit), ignore_files(ignore_files),
      vid(v)
  {
    restart();
  }

  //----------------------------------------------------------------------------
  // Restart
  //----------------------------------------------------------------------------
  void restart()
  {
    if (!found) {
      ExplorationOptions options;
      options.populateLinkedAttributes = true;
      options.view = gOFS->eosView;
      options.depthLimit = depthlimit;
      options.expansionDecider.reset(new TraversalFilter(vid));
      options.ignoreFiles = ignore_files;
      explorer.reset(new NamespaceExplorer(path, options, *qcl,
                                           static_cast<QuarkNamespaceGroup*>(gOFS->namespaceGroup.get())->getExecutor()));
    }
  }

  //----------------------------------------------------------------------------
  // In-memory: Check whether we need to take deep query mutex lock
  //----------------------------------------------------------------------------
  FindResultProvider()
  {
    localfound.reset(new std::map<std::string, std::set<std::string>>());
    found = localfound.get();
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
    res.iscache = true;

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
    res.iscache = false;

    // Just copy the result given by namespace explorer
    if (!explorer->fetch(res.item)) {
      return false;
    }

    res.path = res.item.fullPath;
    res.isdir = !res.item.isFile;
    res.expansionFilteredOut = res.item.expansionFilteredOut;
    res.attrs = res.item.attrs;

    if (res.item.isFile) {
      res.numFiles = 0;
      res.numContainers = 0;
    } else {
      res.numFiles = res.item.numFiles;
      res.numContainers = res.item.numContainers;
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
  uint32_t depthlimit;
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
  const eos::console::FindProto& findRequest = mReqProto.find();

  // Early return if routing should happen
  if (ShouldRoute(findRequest.path(), reply)) {
    return reply;
  }

  if (!OpenTemporaryOutputFiles()) {
    reply.set_retc(EIO);
    reply.set_std_err(SSTR("error: cannot write find result files on MGM" <<
                           std::endl));
    return reply;
  }

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
  XrdOucErrInfo errInfo;
  // check what <path> actually is ...
  XrdSfsFileExistence file_exists;
  std::string real_path = findRequest.path();

  if (gOFS->_exists(real_path.c_str(), file_exists, errInfo, mVid, nullptr)) {
    mOfsErrStream << "error: failed to run exists on '"
                  << findRequest.path() << "'" << std::endl;
    reply.set_retc(errno);
    return reply;
  } else {
    if (file_exists == XrdSfsFileExistNo) {
      mOfsErrStream << "error: no such file or directory" << std::endl;
      reply.set_retc(ENOENT);
      return reply;
    }

    try {
      eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex);
      real_path = gOFS->eosView->getRealPath(findRequest.path());
      eos_static_info("msg=\"real path resolved\" rpath=\"%s\"",
                      real_path.c_str());
    } catch (eos::MDException& e) {
      mOfsErrStream << "error: could not resove real path" << std::endl;
      reply.set_retc(ENOENT);
      return reply;
    }
  }

  errInfo.clear();
  std::unique_ptr<qclient::QClient> qcl =
    std::make_unique<qclient::QClient>(gOFS->mQdbContactDetails.members,
                                       gOFS->mQdbContactDetails.constructOptions());
  std::unique_ptr<FindResultProvider> findResultProvider;
  int depthlimit = findRequest.Maxdepth__case() ==
                   eos::console::FindProto::MAXDEPTH__NOT_SET ?
                   eos::common::Path::MAX_LEVELS : cPath.GetSubPathSize() + findRequest.maxdepth();

  // @note Shortcut with bad input --name regex filters. Move to client side?
  // Looks like std::regex suffers from https://gcc.gnu.org/bugzilla/show_bug.cgi?id=86164#c7
  // Found by filtering like " newfind --name '*sometext' " (note the '*' prefix!),
  // which raises <std::regex_constants::error_paren> - although quite misleading.
  // Is this an opportunity for fuzzing?
  if (!findRequest.name().empty()) {
    try {
      std::regex filter(findRequest.name(), std::regex_constants::egrep);
    } catch (const std::regex_error& e) {
      eos_static_info("caught exception %d %s with newfind findRequest.name()=%s\n",
                      e.code(), e.what(), findRequest.name().c_str());
      mOfsErrStream << "error(caught exception " << e.code() << ' ' << e.what() <<
                    " with newfind --name=" << findRequest.name()
                    << ").\nPlease note that --name filters by 'egrep' style regex match, you may have to sanitize your input\n";

      if (!CloseTemporaryOutputFiles()) {
        reply.set_retc(EIO);
        reply.set_std_err("error: cannot save find result files on MGM\n");
        return reply;
      } else {
        return reply;
      }
    }
  }

  bool onlydirs = (findRequest.directories() &&
                   !findRequest.files()) | findRequest.count() | findRequest.treecount() |
                  findRequest.childcount();

  if (findRequest.cache()) {
    // read via our in-memory cache using _find
    findResultProvider.reset(new FindResultProvider());
    std::map<std::string, std::set<std::string>>* found =
          findResultProvider->getFoundMap();

    if (gOFS->_find(real_path.c_str(), errInfo, m_err, mVid, (*found),
                    findRequest.attributekey().length() ? findRequest.attributekey().c_str() :
                    nullptr,
                    findRequest.attributevalue().length() ? findRequest.attributevalue().c_str() :
                    nullptr,
                    onlydirs, 0, true, findRequest.maxdepth(),
                    findRequest.name().empty() ? nullptr : findRequest.name().c_str())) {
      mOfsErrStream << "error: unable to run find in directory" << std::endl;
      reply.set_retc(errno);
      return reply;
    } else {
      if (m_err.length()) {
        mOfsErrStream << m_err;
        reply.set_retc(E2BIG);
        return reply;
      }
    }
  } else {
    // read from the QDB backend
    try {
      findResultProvider.reset(new FindResultProvider(qcl.get(),
                               real_path, depthlimit,
                               onlydirs, mVid));
    } catch (eos::MDException& e) {
      eos_static_info("msg=\"caught newfind exception\" orig_path=\"%s\" "
                      "rpath=\"%s\" errno=%d what=\"%s\"",
                      findRequest.path().c_str(), real_path.c_str(),
                      e.getErrno(), e.what());

      if (e.getErrno() == ENOENT) {
        mOfsErrStream << "error: no such file or directory" << std::endl;
      } else {
        mOfsErrStream << "error: unable to start find" << std::endl;
      }

      reply.set_retc(e.getErrno());
      return reply;
    }
  }

  uint64_t treecount_aggregate_dircounter = 0;
  uint64_t treecount_aggregate_filecounter = 0;
  uint64_t dircounter = 0;
  uint64_t filecounter = 0;
  // For general users, cannot return more than 50k dirs and 100k files with one find,
  // unless there is an access rule allowing deeper queries.
  // Special users (like root) have the limit lifted by default.
  const bool limit_result = ((mVid.uid != 0) && (!mVid.hasUid(3)) &&
                             (!mVid.hasGid(4)) && (!mVid.sudoer));
  static uint64_t dir_limit = 50000;
  static uint64_t file_limit = 100000;
  Access::GetFindLimits(mVid, dir_limit, file_limit);
  // @note assume that findResultProvider will serve results DFS-ordered
  FindResult findResult;
  std::shared_ptr<eos::IContainerMD> cMD;
  std::shared_ptr<eos::IFileMD> fMD;
//  EXEC_TIMING_BEGIN("Newfind");
  std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();

  while (findResultProvider->next(findResult)) {
    if (limit_result) {
      if (dircounter >= dir_limit || filecounter >= file_limit) {
        mOfsErrStream << "warning(" << E2BIG
                      << "): find results are limited for you to " << dir_limit
                      << " directories and " << file_limit << " files.\n"
                      << "Result is truncated! (found " << dircounter
                      << " directories and " << filecounter
                      << " files so far)\n";
        reply.set_retc(E2BIG);
        break;
      }
    }

    if (findResult.isdir) {
      if (!findRequest.directories() && findRequest.files() && !findRequest.count() &&
          !findRequest.treecount()) {
        continue;
      }

      if (findResult.expansionFilteredOut) {
        // Returns a meaningful error message. Mirrors the checks in shouldExpandContainer
        if (!AccessChecker::checkContainer(findResult.toContainerMD().get(),
                                           findResult.attrs, R_OK | X_OK, mVid)) {
          mOfsErrStream << "error(" << EACCES << "): no permissions to read directory ";
          mOfsErrStream << findResult.path << std::endl;
          reply.set_retc(EACCES);
          continue;
        } else if (!AccessChecker::checkPublicAccess(findResult.path,
                   const_cast<common::VirtualIdentity&>(mVid))) {
          mOfsErrStream << "error(" << EACCES <<
                        "): public access level restriction on directory ";
          mOfsErrStream << findResult.path << std::endl;
          reply.set_retc(EACCES);
          continue;
        } else {
          // @note Empty branch, will be cut out from the compiler anyway.
          // Either the findResult container can't be expanded further as it reaches maxdepth
          // (this is not an error), either there is something fundamentally wrong. Should never happen.
        }
      }

      // Selection
      cMD = findResult.toContainerMD();

      if (!cMD) {
        continue;  // Process next item if we don't have a cMD
      }

      // --treecount nullify the filters, we don't want to bias the count because of intermediate filtered-out result
      // Alse, take the chance to update the total counter while traversing
      if (!findRequest.treecount()) {
        if (FilterOut(findRequest, cMD)) {
          continue;
        }
      } else {
        treecount_aggregate_dircounter += findResult.numContainers;
        treecount_aggregate_filecounter += findResult.numFiles;
      }

      dircounter++;
      filecounter += findResult.numFiles;

      if (findRequest.count() || findRequest.treecount()) {
        continue;
      }

      // Purge version directory?
      if (purge) {
        this->PurgeVersions(mOfsOutStream, max_version, findResult.path);
        continue;
      }

      // Printing

      // Are we printing fileinfo -m? Then, that's it
      if (findRequest.fileinfo()) {
        this->PrintFileInfoMinusM(findResult, errInfo);
        continue;
      }

      printDu(mOfsOutStream, findRequest, cMD, findResult.numContainers,
              findResult.numFiles);
      printPath(mOfsOutStream, findRequest, findResult.path);
      printChildCount(mOfsOutStream, findRequest, cMD, findResult.numContainers,
                      findResult.numFiles);
      printFormat(mOfsOutStream, findRequest, cMD, findResult.numContainers,
                  findResult.numFiles);
      printUidGid(mOfsOutStream, findRequest, cMD);
      printAttributes(mOfsOutStream, findRequest, cMD);
      mOfsOutStream << std::endl;
    } else {
      if (!findRequest.files() && findRequest.directories()) {
        continue;
      }

      // Selection
      fMD = findResult.toFileMD();

      if (!fMD) {
        continue;  // Process next item if we don't have a fMD
      }

      // Balance calculation? If yes,
      // ignore selection criteria (TODO: Change this?)
      // and simply account all fMD's.
      if (findRequest.balance()) {
        balanceCalculator.account(fMD);
        continue;
      }

      if (FilterOut(findRequest, fMD)) {
        continue;
      }

      if (findRequest.zerosizefiles() && !hasSizeZero(fMD)) {
        continue;
      }

      if (findRequest.mixedgroups() && !hasMixedSchedGroups(fMD)) {
        continue;
      }

      if (findRequest.stripediff() && hasStripeDiff(fMD)) {
        continue;  // @note or the opposite?
      }

      filecounter++;

      if (findRequest.count() || findRequest.treecount()) {
        continue;
      }

      // Purge atomic files?
      if (purge_atomic) {
        this->ProcessAtomicFilePurge(mOfsOutStream, findResult.path, *fMD.get());
        continue;
      }

      // Modify layout stripes?
      if (findRequest.dolayoutstripes()) {
        this->ModifyLayoutStripes(findRequest, findResult.path);
        continue;
      }

      // Printing

      // Are we printing fileinfo -m? Then, that's it
      if (findRequest.fileinfo()) {
        this->PrintFileInfoMinusM(findResult, errInfo);
        continue;
      }

      printDu(mOfsOutStream, findRequest, fMD);

      if (findRequest.format().length()) {
        // format printing for symlinks
        printPath(mOfsOutStream, findRequest, findResult.path);
      } else {
        // standard represenation for symlinks
        printPath(mOfsOutStream, findRequest,
                  fMD->isLink() ? findResult.path + " -> " + fMD->getLink() : findResult.path);
      }

      printFormat(mOfsOutStream, findRequest, fMD);
      printUidGid(mOfsOutStream, findRequest, fMD);
      printAttributes(mOfsOutStream, findRequest, fMD);
      printFMD(mOfsOutStream, findRequest, fMD);
      mOfsOutStream << std::endl;
    }
  }

//  EXEC_TIMING_END("Newfind");
  std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
  gOFS->MgmStats.AddExec("Newfind",
                         std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count());
  gOFS->MgmStats.Add("Newfind", mVid.uid, mVid.gid, 1);
  gOFS->MgmStats.Add("NewfindEntries", mVid.uid, mVid.gid, filecounter);

  if (findRequest.treecount()) {
    printPath(mOfsOutStream, findRequest, findResult.path);
    mOfsOutStream << " sum.nfiles=" << treecount_aggregate_filecounter
                  << " sum.ndirectories=" << treecount_aggregate_dircounter <<
                  std::endl;
  }

  if (findRequest.count()) {
    mOfsOutStream << "nfiles=" << filecounter << " ndirectories=" << dircounter <<
                  std::endl;
  }

  if (findRequest.balance()) {
    balanceCalculator.printSummary(mOfsOutStream);
  }

  if (!CloseTemporaryOutputFiles()) {
    reply.set_retc(EIO);
    reply.set_std_err("error: cannot save find result files on MGM\n");
    return reply;
  }

  return reply;
}

#ifdef EOS_GRPC
//------------------------------------------------------------------------------
// Method implementing the specific behaviour of the command executed for gRPC
//------------------------------------------------------------------------------
void
NewfindCmd::ProcessRequest(grpc::ServerWriter<eos::console::ReplyProto>* writer)
{
  XrdOucString m_err {""};
  eos::console::ReplyProto StreamReply;
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
  XrdOucErrInfo errInfo;
  // check what <path> actually is ...
  XrdSfsFileExistence file_exists;
  std::string real_path = findRequest.path();

  if ((gOFS->_exists(real_path.c_str(), file_exists, errInfo, mVid,
                     nullptr))) {
    StreamReply.set_std_out("");
    StreamReply.set_std_err(
      "error: failed to run exists on '" + real_path + "'\n");
    StreamReply.set_retc(errno);
    writer->Write(StreamReply);
    return;
  } else if (file_exists == XrdSfsFileExistNo) {
    StreamReply.set_std_out("");
    StreamReply.set_std_err("error: no such file or directory\n");
    StreamReply.set_retc(ENOENT);
    writer->Write(StreamReply);
    return;
  }

  errInfo.clear();
  std::unique_ptr<qclient::QClient> qcl =
    std::make_unique<qclient::QClient>(gOFS->mQdbContactDetails.members,
                                       gOFS->mQdbContactDetails.constructOptions());
  std::unique_ptr<FindResultProvider> findResultProvider;
  int depthlimit = findRequest.Maxdepth__case() ==
                   eos::console::FindProto::MAXDEPTH__NOT_SET ?
                   eos::common::Path::MAX_LEVELS : cPath.GetSubPathSize() + findRequest.maxdepth();

  // @note Shortcut with bad input --name regex filters. Move to client side?
  // Looks like std::regex suffers from https://gcc.gnu.org/bugzilla/show_bug.cgi?id=86164#c7
  // Found by filtering like " newfind --name '*sometext' " (note the '*' prefix!),
  // which raises <std::regex_constants::error_paren> - although quite misleading.
  // Is this an opportunity for fuzzing?
  if (!findRequest.name().empty()) {
    try {
      std::regex filter(findRequest.name(), std::regex_constants::egrep);
    } catch (const std::regex_error& e) {
      eos_static_info("caught exception %d %s with newfind findRequest.name()=%s\n",
                      e.code(), e.what(), findRequest.name().c_str());
      StreamReply.set_std_out("");
      StreamReply.set_std_err(
        "error(caught exception " + std::to_string(e.code()) + " " + e.what() +
        " with find --name=" + findRequest.name() +
        ").\nPlease note that --name filters by 'egrep' style regex match, you may have to sanitize your input\n");
      StreamReply.set_retc(errno);
      writer->Write(StreamReply);
      return;
    }
  }

  bool onlydirs = (findRequest.directories() && !findRequest.files()) ||
                  findRequest.treecount();

  if (findRequest.cache()) {
    // read via our in-memory cache using _find
    // TODO: check, may need to reset findResultProvider
    std::map<std::string, std::set<std::string>>* found =
          findResultProvider->getFoundMap();

    if (gOFS->_find(real_path.c_str(), errInfo, m_err, mVid, (*found),
                    findRequest.attributekey().length() ? findRequest.attributekey().c_str() :
                    nullptr,
                    findRequest.attributevalue().length() ? findRequest.attributevalue().c_str() :
                    nullptr,
                    findRequest.directories(), 0, true, findRequest.maxdepth(),
                    findRequest.name().empty() ? nullptr : findRequest.name().c_str())) {
      mOfsErrStream << "error: unable to run find in directory" << std::endl;
      StreamReply.set_retc(errno);
      return;
    } else {
      if (m_err.length()) {
        mOfsErrStream << m_err;
        StreamReply.set_retc(E2BIG);
        return;
      }
    }
  } else {
    // read from the back-end
    try {
      findResultProvider.reset(new FindResultProvider(qcl.get(),
                               real_path, depthlimit,
                               onlydirs, mVid));
    } catch (eos::MDException& e) {
      eos_static_info("msg=\"caught newfind exception\" orig_path=\"%s\" "
                      "rpath=\"%s\" errno=%d what=\"%s\"",
                      findRequest.path().c_str(), real_path.c_str(),
                      e.getErrno(), e.what());
      StreamReply.set_std_out("");

      if (e.getErrno() == ENOENT) {
        StreamReply.set_std_err("error: no such file or directory\n");
      } else {
        StreamReply.set_std_err("error: unable to start find\n");
      }

      StreamReply.set_retc(e.getErrno());
      writer->Write(StreamReply);
      return;
    }
  }

  uint64_t treecount_aggregate_dircounter = 0;
  uint64_t treecount_aggregate_filecounter = 0;
  uint64_t dircounter = 0;
  uint64_t filecounter = 0;
  // For general users, cannot return more than 50k dirs and 100k files with one find,
  // unless there is an access rule allowing deeper queries.
  // Special users (like root) have the limit lifted by default.
  const bool limit_result = ((mVid.uid != 0) && (!mVid.hasUid(3)) &&
                             (!mVid.hasGid(4)) && (!mVid.sudoer));
  static uint64_t dir_limit = 50000;
  static uint64_t file_limit = 100000;
  Access::GetFindLimits(mVid, dir_limit, file_limit);
  // @note assume that findResultProvider will serve results DFS-ordered
  FindResult findResult;
  std::shared_ptr<eos::IContainerMD> cMD;
  std::shared_ptr<eos::IFileMD> fMD;
//  EXEC_TIMING_BEGIN("Newfind");
  std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
  std::string output_str("");
  int counter = 0;

  while (findResultProvider->next(findResult)) {
    if (limit_result) {
      if (dircounter >= dir_limit || filecounter >= file_limit) {
        output_str += "warning(" + std::to_string(E2BIG) +
                      "): find results are limited for you to " + std::to_string(dir_limit) +
                      " directories and " + std::to_string(file_limit) +
                      " files.\nResult is truncated! (found " +
                      std::to_string(dircounter) + " directories and " + std::to_string(
                        filecounter) + " files so far)\n";
        break;
      }
    }

    std::ostringstream output;

    if (findResult.isdir) {
      if (!findRequest.directories() && findRequest.files() && !findRequest.count()) {
        continue;
      }

      if (findResult.expansionFilteredOut) {
        // Returns a meaningful error message. Mirrors the checks in shouldExpandContainer
        if (!AccessChecker::checkContainer(findResult.toContainerMD().get(),
                                           findResult.attrs, R_OK | X_OK, mVid)) {
          output_str += "error(" + std::to_string(EACCES) +
                        "): no permissions to read directory " + findResult.path + "\n";
          continue;
        } else if (!AccessChecker::checkPublicAccess(findResult.path,
                   const_cast<common::VirtualIdentity&>(mVid))) {
          output_str += "error(" + std::to_string(EACCES) +
                        "): public access level restriction on directory " + findResult.path + "\n";
          continue;
        } else {
          // @note Empty branch, will be cut out from the compiler anyway.
          // Either the findResult container can't be expanded further as it reaches maxdepth
          // (this is not an error), either there is something fundamentally wrong. Should never happen.
        }
      }

      // Selection
      cMD = findResult.toContainerMD();

      if (!cMD) {
        continue;  // Process next item if we don't have a cMD
      }

      // --treecount nullify the filters, we don't want to bias the count because of intermediate filtered-out result
      // Alse, take the chance to update the total counter while traversing
      if (!findRequest.treecount()) {
        if (FilterOut(findRequest, cMD)) {
          continue;
        }
      } else {
        treecount_aggregate_dircounter += findResult.numContainers;
        treecount_aggregate_filecounter += findResult.numFiles;
      }

      dircounter++;
      filecounter += findResult.numFiles;

      if (findRequest.count() || findRequest.treecount()) {
        continue;
      }

      // Purge version directory?
      if (purge) {
        this->PurgeVersions(output, max_version, findResult.path);
      }
      // Printing
      // Are we printing fileinfo -m? Then, that's it
      else if (findRequest.fileinfo()) {
        this->PrintFileInfoMinusM(output, findResult, errInfo);
      } else {
        printDu(mOfsOutStream, findRequest, cMD, findResult.numContainers,
                findResult.numFiles);
        printPath(output, findRequest, findResult.path);
        printChildCount(mOfsOutStream, findRequest, cMD, findResult.numContainers,
                        findResult.numFiles);
        printFormat(mOfsOutStream, findRequest, cMD, findResult.numContainers,
                    findResult.numFiles);
        printUidGid(output, findRequest, cMD);
        printAttributes(output, findRequest, cMD);

        // Print times for directory
        if (findRequest.ctime()) {
          eos::IFileMD::ctime_t ctime;
          cMD->getCTime(ctime);
          output << " ctime=" << (unsigned long long) ctime.tv_sec;
          output << '.' << (unsigned long long) ctime.tv_nsec;
        }

        if (findRequest.mtime()) {
          eos::IFileMD::ctime_t mtime;
          cMD->getMTime(mtime);
          output << " mtime=" << (unsigned long long) mtime.tv_sec;
          output << '.' << (unsigned long long) mtime.tv_nsec;
        }
      }
    } else if (!findResult.isdir) { // redundant, no problem
      if (!findRequest.files() && findRequest.directories()) {
        continue;
      }

      // Selection
      fMD = findResult.toFileMD();

      if (!fMD) {
        continue;  // Process next item if we don't have a fMD
      }

      // Balance calculation? If yes,
      // ignore selection criteria (TODO: Change this?)
      // and simply account all fMD's.
      if (findRequest.balance()) {
        balanceCalculator.account(fMD);
        continue;
      }

      if (FilterOut(findRequest, fMD)) {
        continue;
      }

      if (findRequest.zerosizefiles() && !hasSizeZero(fMD)) {
        continue;
      }

      if (findRequest.mixedgroups() && !hasMixedSchedGroups(fMD)) {
        continue;
      }

      if (findRequest.stripediff() && hasStripeDiff(fMD)) {
        continue;  // @note or the opposite?
      }

      if (findRequest.count() || findRequest.treecount()) {
        continue;
      }

      // Purge atomic files?
      if (purge_atomic) {
        this->ProcessAtomicFilePurge(output, findResult.path, *fMD.get());
      }
      // Modify layout stripes?
      else if (findRequest.dolayoutstripes()) {
        this->ModifyLayoutStripes(output, findRequest, findResult.path);
      }
      // Printing
      // Are we printing fileinfo -m? Then, that's it
      else if (findRequest.fileinfo()) {
        this->PrintFileInfoMinusM(output, findResult, errInfo);
      } else {
        printDu(mOfsOutStream, findRequest, fMD);
        printPath(output, findRequest,
                  fMD->isLink() ? findResult.path + " -> " + fMD->getLink() : findResult.path);
        printFormat(mOfsOutStream, findRequest, fMD);
        printUidGid(output, findRequest, fMD);
        printAttributes(output, findRequest, fMD);
        printFMD(output, findRequest, fMD);
      }
    }

    output_str += output.str();
    counter++;

    // Erase "\t" if it is on the end of entry
    if (!output_str.empty() && output_str.substr(output_str.size() - 1) == "\t") {
      output_str.erase(output_str.size() - 1);
    }

    // Add "\n" if doesn't exist
    if (!output_str.empty() && output_str.substr(output_str.size() - 1) != "\n") {
      output_str += "\n";
    }

    // Write every 100 lines separately to gRPC
    if (counter >= 100) {
      StreamReply.set_std_out(output_str);
      StreamReply.set_std_err("");
      StreamReply.set_retc(0);
      writer->Write(StreamReply);
      counter = 0;
      output_str.clear();
    }
  }

  // Write last part to gRPC, if exists
  if (!output_str.empty()) {
    StreamReply.set_std_out(output_str);
    StreamReply.set_std_err("");
    StreamReply.set_retc(0);
    writer->Write(StreamReply);
  }

//  EXEC_TIMING_END("Newfind");
  std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
  gOFS->MgmStats.AddExec("Newfind",
                         std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count());
  gOFS->MgmStats.Add("Newfind", mVid.uid, mVid.gid, 1);
  gOFS->MgmStats.Add("NewfindEntries", mVid.uid, mVid.gid, filecounter);

  if (findRequest.treecount()) {
    StreamReply.set_std_out(
      "path=\"" + findRequest.path() +
      "\" sum.nfiles=" + std::to_string(treecount_aggregate_filecounter) +
      " sum.ndirectories=" + std::to_string(treecount_aggregate_dircounter) + "\n");
    StreamReply.set_std_err("");
    StreamReply.set_retc(0);
    writer->Write(StreamReply);
  }

  if (findRequest.count()) {
    StreamReply.set_std_out(
      "nfiles=" + std::to_string(filecounter) + " ndirectories=" + std::to_string(
        dircounter) + "\n");
    StreamReply.set_std_err("");
    StreamReply.set_retc(0);
    writer->Write(StreamReply);
  }

  if (findRequest.balance()) {
    std::stringstream output;
    balanceCalculator.printSummary(output);
    StreamReply.set_std_out(output.str() + "\n");
    StreamReply.set_std_err("");
    StreamReply.set_retc(0);
    writer->Write(StreamReply);
  }
}
#endif

//------------------------------------------------------------------------------
// Get fileinfo about path in monitoring format
//------------------------------------------------------------------------------
void
NewfindCmd::PrintFileInfoMinusM(std::ostream& ss, const FindResult& find_obj,
                                XrdOucErrInfo& errInfo)
{
  ProcCommand Cmd;
  std::string output_stdout(""), output_stderr("");
  std::string info = "&mgm.cmd=fileinfo&mgm.file.info.option=-m";

  try {
    if (find_obj.isdir) {
      if (find_obj.item.containerMd.id()) {
        info += "&mgm.path=pid:";
        info += std::to_string(find_obj.item.containerMd.id());
      } else {
        info += "&mgm.path=";
        info += find_obj.path;
      }
    } else {
      if (find_obj.item.fileMd.id()) {
        info += "&mgm.path=fid:";
        info += std::to_string(find_obj.item.fileMd.id());
      } else {
        info += "&mgm.path=";
        info += find_obj.path;
      }
    }
  } catch (...) {
    eos_static_err("msg=\"failed to convert metadata id\" path=\"%s\"",
                   find_obj.path.c_str());
    return;
  }

  Cmd.open("/proc/user", info.c_str(), mVid, &errInfo);
  Cmd.AddOutput(output_stdout, output_stderr);
  Cmd.close();

  if (Cmd.GetRetc() == 0) {
    ss << output_stdout;
  } else {
    ss << output_stderr;
  }

  ss << std::endl;
}

//------------------------------------------------------------------------------
// Modify layout stripes for gRPC
//------------------------------------------------------------------------------
void
NewfindCmd::ModifyLayoutStripes(std::ostream& ss,
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
        ss << env.Get("mgm.proc.stdout");
      }
    } else {
      ss << env.Get("mgm.proc.stderr");
    }
  }
}

EOSMGMNAMESPACE_END
