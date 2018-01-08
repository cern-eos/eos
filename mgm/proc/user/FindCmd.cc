//------------------------------------------------------------------------------
// File: FindCmd.cc
// Author: Jozsef Makai - CERN
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
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Acl.hh"

EOSMGMNAMESPACE_BEGIN


eos::console::ReplyProto
eos::mgm::FindCmd::ProcessRequest() {
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
  auto olderthan = findRequest.olderthan();
  auto youngerthan = findRequest.youngerthan();
  auto& purgeversion = findRequest.purge();
  auto uid = findRequest.uid();
  auto gid = findRequest.gid();
  auto notuid = findRequest.notuid();
  auto notgid = findRequest.notgid();
  auto& permission = findRequest.permission();
  auto& notpermission = findRequest.notpermission();
  auto stripes = findRequest.layoutstripes();

  bool silent = findRequest.silent();
  bool calcbalance = findRequest.balance();
  bool findzero = findRequest.zerosizefiles();
  bool findgroupmix = findRequest.mixedgroups();
  bool searchuid = findRequest.searchuid();
  bool searchnotuid = findRequest.searchnotuid();
  bool searchgid = findRequest.searchgid();
  bool searchnotgid = findRequest.searchnotgid();
  bool searchpermission = findRequest.searchpermission();
  bool searchnotpermission = findRequest.searchnotpermission();
  bool printsize = findRequest.size();
  bool printfid = findRequest.fid();
  bool printuid = findRequest.printuid();
  bool printgid = findRequest.printgid();
  bool printfs = findRequest.fs();
  bool printchecksum = findRequest.checksum();
  bool printctime = findRequest.ctime();
  bool printmtime = findRequest.mtime();
  bool printrep = findRequest.nrep();
  bool selectrepdiff = findRequest.stripediff();
  bool selectonehour = findRequest.onehourold();
  bool printunlink = findRequest.nunlink();
  bool printcounter = findRequest.count();
  bool printchildcount = findRequest.childcount();
  bool printhosts = findRequest.hosts();
  bool printpartition = findRequest.partition();
  bool selectonline = findRequest.online();
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
  time_t selectoldertime = (time_t) olderthan;
  time_t selectyoungertime = (time_t) youngerthan;

  if(!purge_atomic) {
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
  google::dense_hash_map<unsigned long, unsigned long long> filesystembalance;
  google::dense_hash_map<std::string, unsigned long long> spacebalance;
  google::dense_hash_map<std::string, unsigned long long> schedulinggroupbalance;
  google::dense_hash_map<int, unsigned long long> sizedistribution;
  google::dense_hash_map<int, unsigned long long> sizedistributionn;
  filesystembalance.set_empty_key(0);
  spacebalance.set_empty_key("");
  schedulinggroupbalance.set_empty_key("");
  sizedistribution.set_empty_key(-1);
  sizedistributionn.set_empty_key(-1);

  eos::common::Path cPath(spath.c_str());
  bool deepquery = cPath.GetSubPathSize() < 5 && (!findRequest.directories() || findRequest.files());
  static eos::common::RWMutex deepQueryMutex;
  static std::unique_ptr<std::map<std::string, std::set<std::string>>> globalfound;
  eos::common::RWMutexWriteLock deepQueryMutexGuard;

  std::unique_ptr<std::map<std::string, std::set<std::string>>> localfound;
  std::map<std::string, std::set<std::string>> *found = nullptr;
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

        if (!calcbalance) {
          //-------------------------------------------
          eos::common::RWMutexReadLock eosViewMutexGuard;
          eosViewMutexGuard.Grab(gOFS->eosViewRWMutex);
          std::shared_ptr<eos::IFileMD> fmd;

          try {
            bool selected = true;
            fmd = gOFS->eosView->getFile(fspath);
            eosViewMutexGuard.Release();
            //-------------------------------------------

            if (selectonehour) {
              eos::IFileMD::ctime_t mtime;
              fmd->getMTime(mtime);

              if (mtime.tv_sec > (time(nullptr) - 3600)) {
                selected = false;
              }
            }

            if (selectoldertime > 0) {
              eos::IFileMD::ctime_t mtime;
              fmd->getMTime(mtime);

              if (mtime.tv_sec > selectoldertime) {
                selected = false;
              }
            }

            if (selectyoungertime > 0) {
              eos::IFileMD::ctime_t mtime;
              fmd->getMTime(mtime);

              if (mtime.tv_sec < selectyoungertime) {
                selected = false;
              }
            }

            if (searchuid && fmd->getCUid() != uid) {
              selected = false;
            }

            if (searchnotuid && fmd->getCUid() == notuid) {
              selected = false;
            }

            if (searchgid && fmd->getCGid() != gid) {
              selected = false;
            }

            if (searchnotgid && fmd->getCGid() == notgid) {
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

            if (findgroupmix) {
              // find files which have replicas on mixed scheduling groups
              std::string sGroupRef = "";
              std::string sGroup = "";
              bool mixed = false;

              for (auto lociter : fmd->getLocations()) {
                // ignore filesystem id 0
                if (!lociter) {
                  eos_err("fsid 0 found fid=%lld", fmd->getId());
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
                    mixed = true;
                    break;
                  }
                } else {
                  sGroupRef = sGroup;
                }
              }

              if(!mixed) {
                selected = false;
              }
            }

            if (selectrepdiff &&
                fmd->getNumLocation() == eos::common::LayoutId::GetStripeNumber(fmd->getLayoutId() + 1)) {
              selected = false;
            }

            // How to print, count, etc. when file is selected...
            if (selected) {
              bool printSimple = !(printsize || printfid || printuid || printgid ||
                                   printchecksum || printfileinfo || printfs || printctime ||
                                   printmtime || printrep || printunlink || printhosts ||
                                   printpartition || selectrepdiff || purge_atomic || layoutstripes);

              if (printSimple) {
                if (!printcounter) {
                  if (printxurl) {
                    ofstdoutStream << url;
                  }

                  ofstdoutStream << fspath << std::endl;
                }
              }
              else {
                if (!purge_atomic && !layoutstripes) {
                  if (!printfileinfo) {
                    if (!printcounter) {
                      ofstdoutStream << "path=";

                      if (printxurl) {
                        ofstdoutStream << url;
                      }

                      ofstdoutStream << fspath;

                      if (printsize) {
                        ofstdoutStream << " size=" << fmd->getSize();
                      }

                      if (printfid) {
                        ofstdoutStream << " fid=" << fmd->getId();
                      }

                      if (printuid) {
                        ofstdoutStream << " uid=" << fmd->getCUid();
                      }

                      if (printgid) {
                        ofstdoutStream << " gid=" << fmd->getCGid();
                      }

                      if (printfs) {
                        ofstdoutStream << " fsid=";

                        eos::IFileMD::LocationVector loc_vect = fmd->getLocations();
                        eos::IFileMD::LocationVector::const_iterator lociter;

                        for (lociter = loc_vect.begin(); lociter != loc_vect.end(); ++lociter) {
                          if (lociter != loc_vect.begin()) {
                            ofstdoutStream << ',';
                          }

                          ofstdoutStream << *lociter;
                        }
                      }

                      if (printpartition) {
                        ofstdoutStream << " partition=";
                        std::set<std::string> fsPartition;

                        for (auto lociter : fmd->getLocations()) {
                          // get host name for fs id
                          eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
                          eos::common::FileSystem* filesystem = nullptr;

                          if (FsView::gFsView.mIdView.count(lociter)) {
                            filesystem = FsView::gFsView.mIdView[lociter];
                          }

                          if (filesystem != nullptr) {
                            eos::common::FileSystem::fs_snapshot_t fs;

                            if (filesystem->SnapShotFileSystem(fs, true)) {
                              std::string partition = fs.mHost;
                              partition += ":";
                              partition += fs.mPath;

                              if ((!selectonline) ||
                                  (filesystem->GetActiveStatus(true) == eos::common::FileSystem::kOnline)) {
                                fsPartition.insert(partition);
                              }
                            }
                          }
                        }

                        for (auto partitionit = fsPartition.begin(); partitionit != fsPartition.end();
                             partitionit++) {
                          if (partitionit != fsPartition.begin()) {
                            ofstdoutStream << ',';
                          }

                          ofstdoutStream << partitionit->c_str();
                        }
                      }

                      if (printhosts) {
                        ofstdoutStream << " hosts=";
                        std::set<std::string> fsHosts;

                        for (auto lociter : fmd->getLocations()) {
                          // get host name for fs id
                          eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
                          eos::common::FileSystem* filesystem = nullptr;

                          if (FsView::gFsView.mIdView.count(lociter)) {
                            filesystem = FsView::gFsView.mIdView[lociter];
                          }

                          if (filesystem != nullptr) {
                            eos::common::FileSystem::fs_snapshot_t fs;

                            if (filesystem->SnapShotFileSystem(fs, true)) {
                              fsHosts.insert(fs.mHost);
                            }
                          }
                        }

                        for (auto hostit = fsHosts.begin(); hostit != fsHosts.end(); hostit++) {
                          if (hostit != fsHosts.begin()) {
                            ofstdoutStream << ',';
                          }

                          ofstdoutStream << hostit->c_str();
                        }
                      }

                      if (printchecksum) {
                        ofstdoutStream << " checksum=";

                        for (unsigned int i = 0;
                             i < eos::common::LayoutId::GetChecksumLen(fmd->getLayoutId()); i++) {
                          ofstdoutStream << std::right << setfill('0') << std::setw(2)
                                         << (unsigned char) (fmd->getChecksum().getDataPadded(i));
                        }
                      }

                      if (printctime) {
                        eos::IFileMD::ctime_t ctime;
                        fmd->getCTime(ctime);

                        ofstdoutStream << " ctime=" << (unsigned long long) ctime.tv_sec;
                        ofstdoutStream << '.' << (unsigned long long) ctime.tv_nsec;
                      }

                      if (printmtime) {
                        eos::IFileMD::ctime_t mtime;
                        fmd->getMTime(mtime);

                        ofstdoutStream << " mtime=" << (unsigned long long) mtime.tv_sec;
                        ofstdoutStream << '.' << (unsigned long long) mtime.tv_nsec;
                      }

                      if (printrep) {
                        ofstdoutStream << " nrep=" << fmd->getNumLocation();
                      }

                      if (printunlink) {
                        ofstdoutStream << " nunlink=" << fmd->getNumUnlinkedLocation();
                      }
                    }
                  }
                  else {
                    // print fileinfo -m
                    this->PrintFileInfoMinusM(fspath, errInfo);
                  }

                  if (!printcounter) {
                    ofstdoutStream << std::endl;
                  }
                }

                // Do the purge if needed
                if (purge_atomic && fspath.find(EOS_COMMON_PATH_ATOMIC_FILE_PREFIX) != std::string::npos) {
                  ofstdoutStream << "# found atomic " << fspath << std::endl;
                  struct stat buf;

                  if ((!gOFS->_stat(fspath.c_str(), &buf, errInfo, mVid, (const char*) nullptr, nullptr)) &&
                      ((mVid.uid == 0) || (mVid.uid == buf.st_uid))) {
                    time_t now = time(nullptr);

                    if ((now - buf.st_ctime) > 86400) {
                      if (!gOFS->_rem(fspath.c_str(), errInfo, mVid, (const char*) nullptr)) {
                        ofstdoutStream << "# purging atomic " << fspath;
                      }
                    } else {
                      ofstdoutStream << "# skipping atomic " << fspath << " [< 1d old ]" << std::endl;
                    }
                  }
                }

                // Add layout stripes if needed
                if (layoutstripes) {
                  ProcCommand fileCmd;
                  std::string info = "mgm.cmd=file&mgm.subcmd=layout&mgm.path=";
                  info += fspath;
                  info += "&mgm.file.layout.stripes=";
                  info += std::to_string(stripes);

                  if (fileCmd.open("/proc/user", info.c_str(), mVid, &errInfo) == 0) {
                    std::ostringstream outputStream;
                    XrdSfsFileOffset offset = 0;
                    constexpr uint32_t size = 512;
                    auto bytesRead = 0ul;
                    char buffer[size];
                    do {
                      bytesRead = fileCmd.read(offset, buffer, size);
                      for(auto i = 0u; i < bytesRead; i++) {
                        outputStream << buffer[i];
                      }
                      offset += bytesRead;
                    } while (bytesRead == size);

                    fileCmd.close();

                    XrdOucEnv env(outputStream.str().c_str());
                    if (std::stoi(env.Get("mgm.proc.retc")) == 0) {
                      if (!silent) {
                        ofstdoutStream << env.Get("mgm.proc.stdout") << std::endl;
                      }
                    }
                    else {
                      ofstderrStream << env.Get("mgm.proc.stderr") << std::endl;
                    }
                  }
                }
              }

              filecounter++;
            }
          } catch (eos::MDException& e) {
            eos_debug("caught exception %d %s\n", e.getErrno(),
                      e.getMessage().str().c_str());
            eosViewMutexGuard.Release();
            //-------------------------------------------
          }
        } else {
          // get location
          //-------------------------------------------
          eos::common::RWMutexReadLock eosViewMutexGuard;
          eosViewMutexGuard.Grab(gOFS->eosViewRWMutex);
          std::shared_ptr<eos::IFileMD> fmd;

          try {
            fmd = gOFS->eosView->getFile(fspath);
          } catch (eos::MDException& e) {
            eos_debug("caught exception %d %s\n", e.getErrno(),
                      e.getMessage().str().c_str());
          }

          eosViewMutexGuard.Release();

          if (fmd) {
            for (unsigned int i = 0; i < fmd->getNumLocation(); i++) {
              auto loc = fmd->getLocation(i);
              size_t size = fmd->getSize();

              if (!loc) {
                eos_err("fsid 0 found %s %llu", fmd->getName().c_str(), fmd->getId());
                continue;
              }

              filesystembalance[loc] += size;

              if ((i == 0) && (size)) {
                auto bin = (int) log10((double) size);
                sizedistribution[ bin ] += size;
                sizedistributionn[ bin ]++;
              }

              eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
              eos::common::FileSystem* filesystem = nullptr;

              if (FsView::gFsView.mIdView.count(loc)) {
                filesystem = FsView::gFsView.mIdView[loc];
              }

              if (filesystem != nullptr) {
                eos::common::FileSystem::fs_snapshot_t fs;

                if (filesystem->SnapShotFileSystem(fs, true)) {
                  spacebalance[fs.mSpace] += size;
                  schedulinggroupbalance[fs.mGroup] += size;
                }
              }
            }
          }
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

      if (searchuid && mCmd->getCUid() != uid) {
        selected = false;
      }

      if (searchnotuid && mCmd->getCUid() == notuid) {
        selected = false;
      }

      if (searchgid && mCmd->getCGid() != gid) {
        selected = false;
      }

      if (searchnotgid && mCmd->getCGid() == notgid) {
        selected = false;
      }

      if (searchpermission || searchnotpermission) {
        struct stat buf;
        if (gOFS->_stat(foundit.first.c_str(), &buf, errInfo, mVid, nullptr, nullptr) == 0) {
          std::ostringstream flagOstr;
          flagOstr << std::oct << buf.st_mode;
          auto flagStr = flagOstr.str();
          auto permString = flagStr.substr(flagStr.length() - 3);

          if (searchpermission && permString != permission) {
            selected = false;
          }

          if (searchnotpermission && permString == notpermission) {
            selected = false;
          }
        }
        else {
          selected = false;
        }
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

        if ((!gOFS->_stat(foundit.first.c_str(), &buf, errInfo, mVid, nullptr, nullptr)) &&
            ((mVid.uid == 0) || (mVid.uid == buf.st_uid))) {
          ofstdoutStream << "# purging " << foundit.first;
          gOFS->PurgeVersion(foundit.first.c_str(), errInfo, max_version);
        }
      }

      if (selected && !purge && !printcounter) {
        if (printchildcount) {
          //-------------------------------------------
          eos::common::RWMutexReadLock nLock(gOFS->eosViewRWMutex);
          std::shared_ptr<eos::IContainerMD> mCmd;
          unsigned long long childfiles = 0;
          unsigned long long childdirs = 0;

          try {
            mCmd = gOFS->eosView->getContainer(foundit.first);
            childfiles = mCmd->getNumFiles();
            childdirs = mCmd->getNumContainers();
            ofstdoutStream << foundit.first << " ndir=" << childdirs << " nfiles=" << childfiles << std::endl;
          } catch (eos::MDException& e) {
            eos_debug("caught exception %d %s\n", e.getErrno(),
                      e.getMessage().str().c_str());
          }
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
                  ofstdoutStream << printkey << "=" << std::left << std::setw(32) << attr << " path=";
                }
              }
            }

            if (printxurl) {
              ofstdoutStream << url;
            }

            ofstdoutStream << foundit.first;

            if (printuid || printgid) {
              eos::common::RWMutexReadLock nLock(gOFS->eosViewRWMutex);
              std::shared_ptr<eos::IContainerMD> mCmd;

              try {
                mCmd = gOFS->eosView->getContainer(foundit.first.c_str());

                if (printuid) {
                  ofstdoutStream << " uid=" << mCmd->getCUid();
                }

                if (printgid) {
                  ofstdoutStream << " gid=" << mCmd->getCGid();
                }
              } catch (eos::MDException& e) {
                eos_debug("caught exception %d %s\n", e.getErrno(),
                          e.getMessage().str().c_str());
              }
            }
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
    ofstdoutStream << "nfiles=" << filecounter << " ndirectories=" << dircounter << std::endl;
  }

  if (calcbalance) {
    XrdOucString sizestring = "";

    for (const auto& it : filesystembalance) {
      ofstdoutStream << "fsid=" << it.first << " \tvolume=";
      ofstdoutStream << std::left << std::setw(12) << eos::common::StringConversion::GetReadableSizeString(sizestring, it.second,"B");
      ofstdoutStream << " \tnbytes=" << it.second << std::endl;
    }

    for (const auto& its : spacebalance) {
      ofstdoutStream << "space=" << its.first << " \tvolume=";
      ofstdoutStream << std::left << std::setw(12) << eos::common::StringConversion::GetReadableSizeString(sizestring, its.second,"B");
      ofstdoutStream << " \tnbytes=" << its.second << std::endl;
    }

    for (const auto& itg : schedulinggroupbalance) {
      ofstdoutStream << "sched=" << itg.first << " \tvolume=";
      ofstdoutStream << std::left << std::setw(12) << eos::common::StringConversion::GetReadableSizeString(sizestring, itg.second,"B");
      ofstdoutStream << " \tnbytes=" << itg.second << std::endl;
    }

    for (const auto& itsd : sizedistribution) {
      unsigned long long lowerlimit = 0;
      unsigned long long upperlimit = 0;

      if (((itsd.first) - 1) > 0) {
        lowerlimit = pow(10, (itsd.first));
      }

      if ((itsd.first) > 0) {
        upperlimit = pow(10, (itsd.first) + 1);
      }

      XrdOucString sizestring1;
      XrdOucString sizestring2;
      XrdOucString sizestring3;
      XrdOucString sizestring4;
      unsigned long long avgsize = (sizedistributionn[itsd.first]
                                    ? itsd.second / sizedistributionn[itsd.first] : 0);
      ofstdoutStream << "sizeorder=" << std::right << setfill('0') << std::setw(2) << itsd.first;
      ofstdoutStream << " \trange=[ " << setfill(' ') << std::left << std::setw(12);
      ofstdoutStream << eos::common::StringConversion::GetReadableSizeString(sizestring1, lowerlimit, "B");
      ofstdoutStream << " ... " << std::left << std::setw(12);
      ofstdoutStream << eos::common::StringConversion::GetReadableSizeString(sizestring2, upperlimit, "B") << " ]";
      ofstdoutStream << " volume=" << std::left << std::setw(12);
      ofstdoutStream << eos::common::StringConversion::GetReadableSizeString(sizestring3, itsd.second, "B");
      ofstdoutStream << " \tavgsize=" << std::left << std::setw(12);
      ofstdoutStream << eos::common::StringConversion::GetReadableSizeString(sizestring4, avgsize, "B");
      ofstdoutStream << " \tnbytes=" << itsd.second;
      ofstdoutStream << " \t avgnbytes=" << avgsize;
      ofstdoutStream << " \t nfiles=" << sizedistributionn[itsd.first];
    }
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

void FindCmd::PrintFileInfoMinusM(const std::string &path, XrdOucErrInfo &errInfo) {
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
