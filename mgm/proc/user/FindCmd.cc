//------------------------------------------------------------------------------
// File: FindCmd.cc
// Author: Georgios Bitzes, Jozsef Makai - CERN
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

#include "FindCmd.hh"
#include "common/Path.hh"
#include "common/LayoutId.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Acl.hh"
#include "mgm/Stat.hh"
#include "mgm/FsView.hh"
#include "namespace/interface/IView.hh"
#include "namespace/utils/Stat.hh"
#include "namespace/utils/BalanceCalculator.hh"

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Based on the Uid/Gid of given FileMd / ContainerMd, should it be included
// in the search results?
//------------------------------------------------------------------------------
template<typename T>
static bool eliminateBasedOnUidGid(const eos::console::FindProto &req,
  const T& md)
{

  if(req.searchuid() && md->getCUid() != req.uid() ) {
    return true;
  }

  if(req.searchnotuid() && md->getCUid() == req.notuid()) {
    return true;
  }

  if(req.searchgid() && md->getCGid() != req.gid()) {
    return true;
  }

  if(req.searchnotgid() && md->getCGid() == req.gid()) {
    return true;
  }

  return false;
}


//------------------------------------------------------------------------------
// Print hex checksum of given fmd, if requested by req.
//------------------------------------------------------------------------------
static void printChecksum(std::ofstream &ss, const eos::console::FindProto &req,
  const std::shared_ptr<eos::IFileMD> &fmd)
{
  if(req.checksum()) {
    ss << " checksum=";

    for(unsigned int i = 0; i < eos::common::LayoutId::GetChecksumLen(fmd->getLayoutId()); i++) {
      ss << eos::common::StringConversion::char_to_hex(fmd->getChecksum().getDataPadded(i));
    }
  }
}

//------------------------------------------------------------------------------
// Print replica location of an fmd.
//------------------------------------------------------------------------------
static void printReplicas(std::ofstream &ss,
  const std::shared_ptr<eos::IFileMD> &fmd, bool onlyhost, bool selectonline)
{
  if(onlyhost) {
    ss << " hosts=";
  }
  else {
    ss << " partition=";
  }

  std::set<std::string> results;

  for(auto lociter : fmd->getLocations()) {
    // lookup filesystem
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
    eos::common::FileSystem* filesystem = nullptr;

    if (FsView::gFsView.mIdView.count(lociter)) {
      filesystem = FsView::gFsView.mIdView[lociter];
    }

    if(!filesystem) {
      continue;
    }

    eos::common::FileSystem::fs_snapshot_t fs;

    if (filesystem->SnapShotFileSystem(fs, true)) {

      if(selectonline && filesystem->GetActiveStatus(true) != eos::common::FileSystem::kOnline) {
        continue;
      }

      std::string item;
      if(onlyhost) {
        item = fs.mHost;
      }
      else {
        item = fs.mHost;
        item += ":";
        item += fs.mPath;
      }

      results.insert(item);
    }
  }

  for(auto it = results.begin(); it != results.end(); it++) {
    if(it != results.begin()) {
      ss << ",";
    }

    ss << it->c_str();
  }
}

//------------------------------------------------------------------------------
// Check whether file replicas belong to different scheduling groups
//------------------------------------------------------------------------------
static bool hasMixedSchedGroups(std::shared_ptr<eos::IFileMD> &fmd)
{
  // find files which have replicas on mixed scheduling groups
  std::string sGroupRef = "";
  std::string sGroup = "";

  for (auto lociter : fmd->getLocations()) {
    // ignore filesystem id 0
    if (!lociter) {
      eos_static_err("fsid 0 found fid=%lld", fmd->getId());
      continue;
    }

    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
    eos::common::FileSystem* filesystem = nullptr;

    if (FsView::gFsView.mIdView.count(lociter)) {
      filesystem = FsView::gFsView.mIdView[lociter];
    }

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

//------------------------------------------------------------------------------
// Check whether to eliminate depending on modification time and options passed
// to FindCmd.
//------------------------------------------------------------------------------
static bool eliminateBasedOnMTime(const eos::console::FindProto &req,
  const std::shared_ptr<eos::IFileMD> &fmd)
{

  eos::IFileMD::ctime_t mtime;
  fmd->getMTime(mtime);

  if(req.onehourold()) {
    if(mtime.tv_sec > (time(nullptr) - 3600)) {
      return true;
    }
  }

  time_t selectoldertime = (time_t) req.olderthan();
  time_t selectyoungertime = (time_t) req.youngerthan();

  if(selectoldertime > 0) {
    if (mtime.tv_sec > selectoldertime) {
      return true;
    }
  }

  if(selectyoungertime > 0) {
    if (mtime.tv_sec < selectyoungertime) {
      return true;
    }
  }

  return false;
}

//------------------------------------------------------------------------------
// Print uid / gid of a FileMD or ContainerMD, if requested by req.
//------------------------------------------------------------------------------
template<typename T>
static void printUidGid(std::ofstream &ss, const eos::console::FindProto &req,
  const T &md)
{
  if(req.printuid()) {
    ss << " uid=" << md->getCUid();
  }

  if(req.printgid()) {
    ss << " gid=" << md->getCGid();
  }
}

//------------------------------------------------------------------------------
// Print fs of a FileMD.
//------------------------------------------------------------------------------
static void printFs(std::ofstream &ss, const std::shared_ptr<eos::IFileMD> &fmd)
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
static void printFMD(std::ofstream &ss, const eos::console::FindProto &req,
  const std::shared_ptr<eos::IFileMD> &fmd)
{

  if(req.size()) {
    ss << " size=" << fmd->getSize();
  }

  if (req.fid()) {
    ss << " fid=" << fmd->getId();
  }

  printUidGid(ss, req, fmd);

  if (req.fs()) {
    printFs(ss, fmd);
  }

  if(req.partition()) {
    printReplicas(ss, fmd, false, req.online());
  }

  if(req.hosts()) {
    printReplicas(ss, fmd, true, req.online());
  }

  printChecksum(ss, req, fmd);

  if(req.ctime()) {
    eos::IFileMD::ctime_t ctime;
    fmd->getCTime(ctime);
    ss << " ctime=" << (unsigned long long) ctime.tv_sec;
    ss << '.' << (unsigned long long) ctime.tv_nsec;
  }

  if(req.mtime()) {
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
// Should I print in simple format, ie just the path, or more information
// is needed?
//------------------------------------------------------------------------------
static bool shouldPrintSimple(const eos::console::FindProto &req)
{
  return !(
    req.size() || req.fid() || req.printuid() || req.printgid() ||
    req.checksum() || req.fileinfo() || req.fs() || req.ctime() ||
    req.mtime() || req.nrep() || req.nunlink() || req.hosts()   ||
    req.partition() || req.stripediff() || (req.purge() == "atomic") ||
    req.dolayoutstripes()
  );
}

//------------------------------------------------------------------------------
// Check whether to select depending on permissions.
//------------------------------------------------------------------------------
static bool eliminateBasedOnPermissions(const eos::console::FindProto &req,
  const std::shared_ptr<IContainerMD> &cont)
{
  if(!req.searchpermission() && !req.searchnotpermission()) {
    return false;
  }

  mode_t st_mode = eos::modeFromMetadataEntry(cont);

  std::ostringstream flagOstr;
  flagOstr << std::oct << st_mode;
  std::string flagStr = flagOstr.str();
  std::string permString = flagStr.substr(flagStr.length() - 3);

  if(req.searchpermission() && permString != req.permission()) {
    return true;
  }

  if(req.searchnotpermission() && permString == req.notpermission()) {
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Purge atomic files
//------------------------------------------------------------------------------
void FindCmd::ProcessAtomicFilePurge(std::ofstream &ss, const std::string &fspath,
  eos::IFileMD &fmd) {

  if(fspath.find(EOS_COMMON_PATH_ATOMIC_FILE_PREFIX) == std::string::npos) {
    return;
  }

  ss << "# found atomic " << fspath << std::endl;
  if( !(mVid.uid == 0 || mVid.uid == fmd.getCUid()) ) {
    // Not allowed to remove file
    ss << "# skipping atomic " << fspath << " [no permission to remove]" << std::endl;
    return;
  }

  time_t now = time(nullptr);

  eos::IFileMD::ctime_t atime;
  fmd.getCTime(atime);

  //----------------------------------------------------------------------------
  // Is the file older than 1 day?
  //----------------------------------------------------------------------------
  if(now - atime.tv_sec <= 86400) {
    ss << "# skipping atomic " << fspath << " [< 1d old ]" << std::endl;
    return;
  }

  //----------------------------------------------------------------------------
  // Perform the rm
  //----------------------------------------------------------------------------
  XrdOucErrInfo errInfo;
  if(!gOFS->_rem(fspath.c_str(), errInfo, mVid, (const char*) nullptr)) {
    ss << "# purging atomic " << fspath;
  }
  else {
    ss << "# could not purge atomic " << fspath;
  }
}

//------------------------------------------------------------------------------
// Modify layout stripes
//------------------------------------------------------------------------------
void eos::mgm::FindCmd::ModifyLayoutStripes(std::ofstream &ss, const eos::console::FindProto &req, const std::string &fspath)
{
  XrdOucErrInfo errInfo;
  ProcCommand fileCmd;
  std::string info = "mgm.cmd=file&mgm.subcmd=layout&mgm.path=";
  info += fspath;
  info += "&mgm.file.layout.stripes=";
  info += std::to_string(req.layoutstripes());

  if(fileCmd.open("/proc/user", info.c_str(), mVid, &errInfo) == 0) {
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

    if(std::stoi(env.Get("mgm.proc.retc")) == 0) {
      if(!req.silent()) {
        ofstdoutStream << env.Get("mgm.proc.stdout") << std::endl;
      }
    } else {
      ofstderrStream << env.Get("mgm.proc.stderr") << std::endl;
    }
  }
}

//------------------------------------------------------------------------------
// Method implementing the specific behaviour of the command executed by the
// asynchronous thread
//------------------------------------------------------------------------------
eos::console::ReplyProto
eos::mgm::FindCmd::ProcessRequest()
{
  eos::console::ReplyProto reply;

  if (!OpenTemporaryOutputFiles()) {
    std::ostringstream error;
    error << "error: cannot write find result files on MGM" << std::endl;
    reply.set_retc(EIO);
    reply.set_std_err(error.str());
    return reply;
  }

  auto& findRequest = mReqProto.find();
  auto& spath = findRequest.path();
  auto& filematch = findRequest.name();
  auto& attributekey = findRequest.attributekey();
  auto& attributevalue = findRequest.attributevalue();
  auto& printkey = findRequest.printkey();
  auto finddepth = findRequest.maxdepth();
  auto& purgeversion = findRequest.purge();
  bool calcbalance = findRequest.balance();
  bool findzero = findRequest.zerosizefiles();
  bool findgroupmix = findRequest.mixedgroups();
  bool selectrepdiff = findRequest.stripediff();
  bool printcounter = findRequest.count();
  bool printchildcount = findRequest.childcount();
  bool printfileinfo = findRequest.fileinfo();
  bool selectfaultyacl = findRequest.faultyacl();
  bool purge = false;
  bool purge_atomic = purgeversion == "atomic";
  bool printxurl = findRequest.xurl();
  bool layoutstripes = findRequest.dolayoutstripes();
  bool nofiles = findRequest.directories() && !findRequest.files();
  bool nodirs = findRequest.files();
  bool dirs = findRequest.directories();
  auto max_version = 999999ul;

  bool printSimple = shouldPrintSimple(findRequest);

  if (!purge_atomic) {
    try {
      max_version = std::stoul(purgeversion);
      purge = true;
      dirs = true;
    } catch (std::logic_error& err) {
      // this error is handled at client side, should not receive bad input from client
    }
  }

  XrdOucString url = "root://";
  url += gOFS->MgmOfsAlias;
  url += "/";
  // this hash is used to calculate the balance of the found files over the filesystems involved
  eos::BalanceCalculator balanceCalculator;
  eos::common::Path cPath(spath.c_str());
  bool deepquery = cPath.GetSubPathSize() < 5 && (!findRequest.directories() ||
                   findRequest.files());
  static eos::common::RWMutex deepQueryMutex;
  static std::unique_ptr<std::map<std::string, std::set<std::string>>>
  globalfound;
  eos::common::RWMutexWriteLock deepQueryMutexGuard;
  std::unique_ptr<std::map<std::string, std::set<std::string>>> localfound;
  std::map<std::string, std::set<std::string>>* found = nullptr;
  XrdOucErrInfo errInfo;

  if (deepquery) {
    // we use a single once allocated map for deep searches to store the results to avoid memory explosion
    deepQueryMutexGuard.Grab(deepQueryMutex);

    if (!globalfound) {
      globalfound.reset(new std::map<std::string, std::set<std::string>>());
    }

    found = globalfound.get();
  } else {
    localfound.reset(new std::map<std::string, std::set<std::string>>());
    found = localfound.get();
  }

  // check what <path> actually is ...
  XrdSfsFileExistence file_exists;

  if ((gOFS->_exists(spath.c_str(), file_exists, errInfo, mVid, nullptr))) {
    std::ostringstream error;
    error << "error: failed to run exists on '" << spath << "'";
    ofstderrStream << error.str();

    if (deepquery) {
      deepQueryMutexGuard.Release();
    }

    reply.set_retc(errno);
    reply.set_std_err(error.str());
    return reply;
  } else {
    if (file_exists == XrdSfsFileExistIsFile) {
      // if this is already a file name, we switch off to find directories
      nodirs = true;
    }

    if (file_exists == XrdSfsFileExistNo) {
      std::ostringstream error;
      error << "error: no such file or directory";
      ofstderrStream << error.str();

      if (deepquery) {
        deepQueryMutexGuard.Release();
      }

      reply.set_retc(ENOENT);
      reply.set_std_err(error.str());
      return reply;
    }
  }

  errInfo.clear();

  if (gOFS->_find(spath.c_str(), errInfo, stdErr, mVid, (*found),
                  attributekey.length() ? attributekey.c_str() : nullptr,
                  attributevalue.length() ? attributevalue.c_str() : nullptr,
                  nofiles, 0, true, finddepth,
                  filematch.length() ? filematch.c_str() : nullptr)) {
    std::ostringstream error;
    error << stdErr;
    error << "error: unable to run find in directory";
    ofstderrStream << error.str();

    if (deepquery) {
      deepQueryMutexGuard.Release();
    }

    reply.set_retc(errno);
    reply.set_std_err(error.str());
    return reply;
  } else {
    if (stdErr.length()) {
      ofstderrStream << stdErr;
      reply.set_retc(E2BIG);
    }
  }

  unsigned int cnt = 0;
  unsigned long long filecounter = 0;
  unsigned long long dircounter = 0;

  if (findRequest.files() || !dirs) {
    for (auto& foundit : *found) {
      if (!findRequest.files() && !nodirs) {
        if (!printcounter) {
          if (printxurl) {
            ofstdoutStream << url;
          }

          ofstdoutStream << foundit.first << std::endl;
        }

        dircounter++;
      }

      for (auto& fileit : foundit.second) {
        cnt++;
        std::string fspath = foundit.first;
        fspath += fileit;

        //----------------------------------------------------------------------
        // Fetch fmd for target file
        //----------------------------------------------------------------------
        eos::common::RWMutexReadLock eosViewMutexGuard;
        eosViewMutexGuard.Grab(gOFS->eosViewRWMutex);
        std::shared_ptr<eos::IFileMD> fmd;
        bool selected = true;

        try {
          fmd = gOFS->eosView->getFile(fspath);
        }
        catch(eos::MDException& e) {
          eos_debug("caught exception %d %s\n", e.getErrno(),
                    e.getMessage().str().c_str());
        }

        eosViewMutexGuard.Release();

        //----------------------------------------------------------------------
        // Do we have the fmd? If not, skip this entry.
        //----------------------------------------------------------------------
        if(!fmd) {
          continue;
        }

        //----------------------------------------------------------------------
        // fmd looks OK, proceed.
        //----------------------------------------------------------------------


        //----------------------------------------------------------------------
        // Balance calculation? Ignore selection
        // criteria (TODO: Change this?) and simply
        // account all fmd's.
        //----------------------------------------------------------------------
        if(calcbalance) {
          balanceCalculator.account(fmd);
          continue;
        }

        //----------------------------------------------------------------------
        // Selection.
        //----------------------------------------------------------------------

        if(eliminateBasedOnMTime(findRequest, fmd)) {
          selected = false;
        }

        if(eliminateBasedOnUidGid(findRequest, fmd)) {
          selected = false;
        }

        // Check attribute key-value filter
        if (!attributekey.empty() && !attributevalue.empty()) {
          XrdOucString attr;
          errInfo.clear();
          gOFS->_attr_get(fspath.c_str(), errInfo, mVid, nullptr,
                          attributekey.c_str(), attr);

          if (attributevalue != std::string(attr.c_str())) {
            selected = false;
          }
        }

        if (findzero && fmd->getSize() != 0) {
          selected = false;
        }

        if (findgroupmix && !hasMixedSchedGroups(fmd)) {
          selected = false;
        }

        if (selectrepdiff &&
            fmd->getNumLocation() == eos::common::LayoutId::GetStripeNumber(
              fmd->getLayoutId() + 1)) {
          selected = false;
        }

        //----------------------------------------------------------------------
        // Printing.
        //----------------------------------------------------------------------
        if(!selected) {
          continue;
        }

        filecounter++;

        //----------------------------------------------------------------------
        // Skip printing each entry if we're only interested in the total file
        // count.
        //----------------------------------------------------------------------
        if(printcounter) {
          continue;
        }

        //----------------------------------------------------------------------
        // Purge atomic files?
        //----------------------------------------------------------------------
        if(purge_atomic) {
          this->ProcessAtomicFilePurge(ofstdoutStream, fspath, *fmd.get());
        }

        //----------------------------------------------------------------------
        // Modify layout stripes?
        //----------------------------------------------------------------------
        if (layoutstripes) {
          this->ModifyLayoutStripes(ofstdoutStream, findRequest, fspath);
        }

        //----------------------------------------------------------------------
        // If purge_atomic or layoutstripes is set, there's nothing more to
        // do, process next item.
        //----------------------------------------------------------------------
        if(purge_atomic || layoutstripes) {
          continue;
        }

        //----------------------------------------------------------------------
        // Print simple?
        //----------------------------------------------------------------------
        if(printSimple) {
          if (printxurl) {
            ofstdoutStream << url;
          }

          ofstdoutStream << fspath << std::endl;
          continue;
        }

        //----------------------------------------------------------------------
        // Nope, print fancy
        //----------------------------------------------------------------------

        // How to print, count, etc. when file is selected...
          if (!purge_atomic && !layoutstripes) {
            if (!printfileinfo) {
              ofstdoutStream << "path=";

              if (printxurl) {
                ofstdoutStream << url;
              }

              ofstdoutStream << fspath;
              printFMD(ofstdoutStream, findRequest, fmd);

            } else {
              // print fileinfo -m
              this->PrintFileInfoMinusM(fspath, errInfo);
            }

            ofstdoutStream << std::endl;
          }

    }
  }

  gOFS->MgmStats.Add("FindEntries", mVid.uid, mVid.gid, cnt);
  }

  eos_debug("Listing directories");

  if (dirs) {
    for (auto& foundit : *found) {
      // Filtering the directories
      bool selected = true;
      eos::common::RWMutexReadLock eosViewMutexGuard;
      eosViewMutexGuard.Grab(gOFS->eosViewRWMutex);
      std::shared_ptr<eos::IContainerMD> mCmd;

      try {
        mCmd = gOFS->eosView->getContainer(foundit.first);
        eosViewMutexGuard.Release();
      } catch (eos::MDException& e) {
        eos_debug("caught exception %d %s\n", e.getErrno(),
                  e.getMessage().str().c_str());
        eosViewMutexGuard.Release();
      }

      if(!mCmd) {
        continue;
      }

      if(eliminateBasedOnUidGid(findRequest, mCmd)) {
        selected = false;
      }

      if(eliminateBasedOnPermissions(findRequest, mCmd)) {
        selected = false;
      }

      if (selectfaultyacl) {
        // get the attributes and call the verify function
        eos::IContainerMD::XAttrMap map;

        if (!gOFS->_attr_ls(foundit.first.c_str(), errInfo,
                            mVid, nullptr, map)) {
          if ((map.count("sys.acl") || map.count("user.acl"))) {
            if (map.count("sys.acl")) {
              if (Acl::IsValid(map["sys.acl"].c_str(), errInfo)) {
                selected = false;
              }
            }

            if (map.count("user.acl")) {
              if (Acl::IsValid(map["user.acl"].c_str(), errInfo)) {
                selected = false;
              }
            }
          } else {
            selected = false;
          }
        }
      }

      // eventually call the version purge function if we own this version dir or we are root
      if (selected && purge &&
          (foundit.first.find(EOS_COMMON_PATH_VERSION_PREFIX) != std::string::npos)) {
        struct stat buf;

        if ((!gOFS->_stat(foundit.first.c_str(), &buf, errInfo, mVid, nullptr,
                          nullptr)) &&
            ((mVid.uid == 0) || (mVid.uid == buf.st_uid))) {
          ofstdoutStream << "# purging " << foundit.first;
          gOFS->PurgeVersion(foundit.first.c_str(), errInfo, max_version);
        }
      }

      if (selected && !purge && !printcounter) {
        if (printchildcount) {
          unsigned long long childfiles = 0;
          unsigned long long childdirs = 0;

          childfiles = mCmd->getNumFiles();
          childdirs = mCmd->getNumContainers();
          ofstdoutStream << foundit.first << " ndir=" << childdirs <<
              " nfiles=" << childfiles << std::endl;
        } else {
          if (!printfileinfo) {
            // print directories
            XrdOucString attr = "";

            if (!printkey.empty()) {
              gOFS->_attr_get(foundit.first.c_str(), errInfo, mVid, nullptr,
                              printkey.c_str(), attr);

              if (!printkey.empty()) {
                if (!attr.length()) {
                  attr = "undef";
                }

                if (!printcounter) {
                  ofstdoutStream << printkey << "=" << std::left << std::setw(
                                   32) << attr << " path=";
                }
              }
            }

            if (printxurl) {
              ofstdoutStream << url;
            }

            ofstdoutStream << foundit.first;
            printUidGid(ofstdoutStream, findRequest, mCmd);
          } else {
            // print fileinfo -m
            this->PrintFileInfoMinusM(foundit.first, errInfo);
          }

          ofstdoutStream << std::endl;
        }
      }

      if (selected) {
        dircounter++;
      }
    }
  }

  if (deepquery) {
    globalfound->clear();
    deepQueryMutexGuard.Release();
  }

  if (printcounter) {
    ofstdoutStream << "nfiles=" << filecounter << " ndirectories=" << dircounter <<
                   std::endl;
  }

  if (calcbalance) {
    balanceCalculator.printSummary(ofstdoutStream);
  }

  if (!CloseTemporaryOutputFiles()) {
    std::ostringstream error;
    error << "error: cannot save find result files on MGM" << std::endl;
    reply.set_retc(EIO);
    reply.set_std_err(error.str());
    return reply;
  }

  return reply;
}

void FindCmd::PrintFileInfoMinusM(const std::string& path,
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
    ofstdoutStream << lStdOut;
  }

  if (lStdErr.length()) {
    ofstderrStream << lStdErr;
  }

  Cmd.close();
}

EOSMGMNAMESPACE_END
