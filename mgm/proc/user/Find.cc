// ----------------------------------------------------------------------
// File: proc/user/Find.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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

#include "mgm/proc/ProcInterface.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Access.hh"
#include "mgm/Macros.hh"
#include "mgm/Acl.hh"
#include "mgm/Stat.hh"
#include "namespace/interface/IView.hh"
#include "namespace/utils/Checksum.hh"
#include "common/LayoutId.hh"
#include "common/Path.hh"

#ifdef __APPLE__
#define ENONET 64
#endif

EOSMGMNAMESPACE_BEGIN

int
ProcCommand::Find()
{
  mDoSort = true;
  XrdOucString spath = pOpaque->Get("mgm.path");
  XrdOucString filematch = pOpaque->Get("mgm.find.match");
  XrdOucString option = pOpaque->Get("mgm.option");
  XrdOucString attribute = pOpaque->Get("mgm.find.attribute");
  XrdOucString maxdepth = pOpaque->Get("mgm.find.maxdepth");
  XrdOucString olderthan = pOpaque->Get("mgm.find.olderthan");
  XrdOucString youngerthan = pOpaque->Get("mgm.find.youngerthan");
  XrdOucString purgeversion = pOpaque->Get("mgm.find.purge.versions");
  XrdOucString key = attribute;
  XrdOucString val = attribute;
  XrdOucString printkey = pOpaque->Get("mgm.find.printkey");
  const char* inpath = spath.c_str();
  bool deepquery = false;
  static XrdSysMutex deepQueryMutex;
  static std::map<std::string, std::set<std::string> >* globalfound = 0;
  NAMESPACEMAP;
  PROC_BOUNCE_ILLEGAL_NAMES;
  PROC_BOUNCE_NOT_ALLOWED;
  spath = path;
  PROC_TOKEN_SCOPE;

  if (!OpenTemporaryOutputFiles()) {
    stdErr += "error: cannot write find result files on MGM\n";
    retc = EIO;
    return SFS_OK;
  }

  eos::common::Path cPath(spath.c_str());

  if (cPath.GetSubPathSize() < 5) {
    if ((((option.find("d")) != STR_NPOS) && ((option.find("f")) == STR_NPOS))) {
      // directory queries are fine even for the complete namespace
      deepquery = false;
    } else {
      // deep queries are serialized by a mutex and use a single the output hashmap !
      deepquery = true;
    }
  }

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
  bool calcbalance = false;
  bool findzero = false;
  bool findgroupmix = false;
  bool printsize = false;
  bool printfid = false;
  bool printuid = false;
  bool printgid = false;
  bool printfs = false;
  bool printchecksum = false;
  bool printctime = false;
  bool printmtime = false;
  bool printrep = false;
  bool selectrepdiff = false;
  bool selectonehour = false;
  bool printunlink = false;
  bool printcounter = false;
  bool printchildcount = false;
  bool printhosts = false;
  bool printpartition = false;
  bool selectonline = false;
  bool printfileinfo = false;
  bool selectfaultyacl = false;
  bool purge = false;
  bool purge_atomic = false;
  bool printxurl = false;
  XrdOucString url = "root://";
  url += gOFS->MgmOfsAlias;
  url += "/";
  int  max_version = 999999;
  int  finddepth = 0;
  time_t selectoldertime = 0;
  time_t selectyoungertime = 0;

  if (olderthan.c_str()) {
    selectoldertime = (time_t) strtoull(olderthan.c_str(), 0, 10);
  }

  if (youngerthan.c_str()) {
    selectyoungertime = (time_t) strtoull(youngerthan.c_str(), 0, 10);
  }

  if (option.find("b") != STR_NPOS) {
    calcbalance = true;
  }

  if (option.find("0") != STR_NPOS) {
    findzero = true;
  }

  if (option.find("G") != STR_NPOS) {
    findgroupmix = true;
  }

  if (option.find("S") != STR_NPOS) {
    printsize = true;
  }

  if (option.find("F") != STR_NPOS) {
    printfid = true;
  }

  if (option.find("L") != STR_NPOS) {
    printfs = true;
  }

  if (option.find("X") != STR_NPOS) {
    printchecksum = true;
  }

  if (option.find("u") != STR_NPOS) {
    printuid = true;
  }

  if (option.find("g") != STR_NPOS) {
    printgid = true;
  }

  if (option.find("C") != STR_NPOS) {
    printctime = true;
  }

  if (option.find("M") != STR_NPOS) {
    printmtime = true;
  }

  if (option.find("R") != STR_NPOS) {
    printrep = true;
  }

  if (option.find("U") != STR_NPOS) {
    printunlink = true;
  }

  if (option.find("D") != STR_NPOS) {
    selectrepdiff = true;
  }

  if (option.find("1") != STR_NPOS) {
    selectonehour = true;
  }

  if (option.find("Z") != STR_NPOS) {
    printcounter = true;
  }

  if (option.find("l") != STR_NPOS) {
    printchildcount = true;
  }

  if (option.find("x") != STR_NPOS) {
    printxurl = true;
  }

  if (option.find("H") != STR_NPOS) {
    printhosts = true;
  }

  if (option.find("P") != STR_NPOS) {
    printpartition = true;
  }

  if (option.find("O") != STR_NPOS) {
    selectonline = true;
  }

  if (option.find("I") != STR_NPOS) {
    printfileinfo = true;

    if ((option.find("d") == STR_NPOS) && (option.find("f") == STR_NPOS)) {
      option += "df";
    }

    printchildcount = false;
  }

  if (option.find("A") != STR_NPOS) {
    selectfaultyacl = true;
    option += "d";
  }

  if (purgeversion.length()) {
    if (purgeversion == "atomic") {
      purge_atomic = true;
      option += "f";
    } else {
      if ((atoi(purgeversion.c_str()) == 0) && (purgeversion != "0")) {
        fprintf(fstderr,
                "error: the max. version given to --purge has to be a valid number >=0");
        retc = EINVAL;
        return SFS_OK;
      }

      max_version = atoi(purgeversion.c_str());
      purge = true;
      option += "d";
    }
  }

  if ((option.find('f') == STR_NPOS) && (option.find('d') == STR_NPOS)) {
    if (!printcounter) {
      option += "df";
    }
  }

  if (attribute.length()) {
    key.erase(attribute.find("="));
    val.erase(0, attribute.find("=") + 1);
  }

  if (maxdepth.length()) {
    finddepth = atoi(maxdepth.c_str());

    if (finddepth > 0) {
      deepquery = false;
    }
  }

  if (!spath.length()) {
    fprintf(fstderr, "error: you have to give a path name to call 'find'");
    retc = EINVAL;
  } else {
    std::map<std::string, std::set<std::string> >* found = 0;

    if (deepquery) {
      // we use a single once allocated map for deep searches to store the results to avoid memory explosion
      deepQueryMutex.Lock();

      if (!globalfound) {
        globalfound = new std::map<std::string, std::set<std::string> >;
      }

      found = globalfound;
    } else {
      found = new std::map<std::string, std::set<std::string> >;
    }

    std::map<std::string, std::set<std::string> >::const_iterator foundit;
    std::set<std::string>::const_iterator fileit;
    bool nofiles = false;

    if (((option.find("d")) != STR_NPOS) && ((option.find("f")) == STR_NPOS)) {
      nofiles = true;
    }

    // check what <path> actually is ...
    XrdSfsFileExistence file_exists;

    if ((gOFS->_exists(spath.c_str(), file_exists, *mError, *pVid, 0))) {
      stdErr += "error: failed to run exists on '";
      stdErr += spath.c_str();
      stdErr += "'";
      fprintf(fstderr, "%s", stdErr.c_str());
      retc = errno;

      if (deepquery) {
        deepQueryMutex.UnLock();
      } else {
        delete found;
      }

      return SFS_OK;
    } else {
      if (file_exists == XrdSfsFileExistIsFile) {
        // if this is already a file name, we switch off to find directories
        option += "f";
      }

      if (file_exists == XrdSfsFileExistNo) {
        stdErr += "error: no such file or directory";
        fprintf(fstderr, "%s", stdErr.c_str());
        retc = ENOENT;

        if (deepquery) {
          deepQueryMutex.UnLock();
        } else {
          delete found;
        }

        return SFS_OK;
      }
    }

    if (gOFS->_find(spath.c_str(), *mError, stdErr, *pVid, (*found),
                    key.c_str(), val.c_str(), nofiles, 0, true, finddepth,
                    filematch.length() ? filematch.c_str() : 0, false,
                    option.find("j") != STR_NPOS, fstdout)) {
      fprintf(fstderr, "%s", stdErr.c_str());
      fprintf(fstderr, "error: unable to run find in directory");
      retc = errno;

      if (deepquery) {
        deepQueryMutex.UnLock();
      } else {
        delete found;
      }

      return SFS_OK;
    } else {
      if (stdErr.length()) {
        fprintf(fstderr, "%s", stdErr.c_str());
        retc = E2BIG;
      }
    }

    int cnt = 0;
    unsigned long long filecounter = 0;
    unsigned long long dircounter = 0;

    if (((option.find("f")) != STR_NPOS) || ((option.find("d")) == STR_NPOS)) {
      for (foundit = (*found).begin(); foundit != (*found).end(); foundit++) {
        if ((option.find("d")) == STR_NPOS) {
          if (option.find("f") == STR_NPOS) {
            if (!printcounter) {
              if (printxurl) {
                fprintf(fstdout, "%s", url.c_str());
              }

              fprintf(fstdout, "%s\n", foundit->first.c_str());
            }

            dircounter++;
          }
        }

        for (fileit = foundit->second.begin(); fileit != foundit->second.end();
             fileit++) {
          cnt++;
          std::string fspath = foundit->first;
          fspath += *fileit;

          if (!calcbalance) {
            if (findgroupmix || findzero || printsize || printfid || printuid ||
                printgid || printfileinfo || printchecksum || printctime ||
                printmtime || printrep || printunlink || printhosts ||
                printpartition || selectrepdiff || selectonehour ||
                selectoldertime || selectyoungertime || purge_atomic) {
              //-------------------------------------------
              eos::common::RWMutexReadLock viewReadLock(gOFS->eosViewRWMutex);
              std::shared_ptr<eos::IFileMD> fmd;

              try {
                bool selected = true;
                fmd = gOFS->eosView->getFile(fspath.c_str());
                viewReadLock.Release();
                //-------------------------------------------

                if (selectonehour) {
                  eos::IFileMD::ctime_t mtime;
                  fmd->getMTime(mtime);

                  if (mtime.tv_sec > (time(NULL) - 3600)) {
                    selected = false;
                  }
                }

                if (selectoldertime) {
                  eos::IFileMD::ctime_t xtime;

                  if (printctime) {
                    fmd->getCTime(xtime);
                  } else {
                    fmd->getMTime(xtime);
                  }

                  if (xtime.tv_sec > selectoldertime) {
                    selected = false;
                  }
                }

                if (selectyoungertime) {
                  eos::IFileMD::ctime_t xtime;

                  if (printctime) {
                    fmd->getCTime(xtime);
                  } else {
                    fmd->getMTime(xtime);
                  }

                  if (xtime.tv_sec < selectyoungertime) {
                    selected = false;
                  }
                }

                if (selected && findgroupmix) {
                  if (findzero && !fmd->getSize()) {
                    if (!printcounter) {
                      if (printxurl) {
                        fprintf(fstdout, "%s", url.c_str());
                      }

                      fprintf(fstdout, "%s\n", fspath.c_str());
                    }
                  }

                  // find files which have replicas on mixed scheduling groups
                  XrdOucString sGroupRef = "";
                  XrdOucString sGroup = "";
                  bool mixed = false;
                  eos::IFileMD::LocationVector loc_vect = fmd->getLocations();
                  eos::IFileMD::LocationVector::const_iterator lociter;

                  for (lociter = loc_vect.begin(); lociter != loc_vect.end(); ++lociter) {
                    // ignore filesystem id 0
                    if (!(*lociter)) {
                      eos_err("fsid 0 found fxid=%08llx", fmd->getId());
                      continue;
                    }

                    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
                    eos::common::FileSystem* filesystem = FsView::gFsView.mIdView.lookupByID(
                                                            *lociter);

                    if (filesystem) {
                      sGroup = filesystem->GetString("schedgroup").c_str();
                    } else {
                      sGroup = "none";
                    }

                    if (sGroupRef.length()) {
                      if (sGroup != sGroupRef) {
                        mixed = true;
                        break;
                      }
                    } else {
                      sGroupRef = sGroup;
                    }
                  }

                  if (mixed) {
                    if (!printcounter) {
                      if (printxurl) {
                        fprintf(fstdout, "%s", url.c_str());
                      }

                      fprintf(fstdout, "%s\n", fspath.c_str());
                    }
                  }
                } else {
                  if (selected &&
                      (selectonehour || selectoldertime || selectyoungertime ||
                       findzero || printsize || printfid || printuid || printgid ||
                       printchecksum || printfileinfo || printfs || printctime ||
                       printmtime || printrep || printunlink || printhosts ||
                       printpartition || selectrepdiff || purge_atomic)) {
                    XrdOucString sizestring;
                    bool printed = true;

                    if (selectrepdiff) {
                      if (fmd->getNumLocation() != (eos::common::LayoutId::GetStripeNumber(
                                                      fmd->getLayoutId()) + 1)) {
                        printed = true;
                      } else {
                        printed = false;
                      }
                    }

                    if (findzero) {
                      printed = (fmd->getSize() == 0);
                    }

                    if (purge_atomic) {
                      printed = false;
                    }

                    if (printed) {
                      if (!printfileinfo) {
                        if (!printcounter) {
                          fprintf(fstdout, "path=");

                          if (printxurl) {
                            fprintf(fstdout, "%s", url.c_str());
                          }

                          fprintf(fstdout, "%s", fspath.c_str());
                        }

                        if (printsize) {
                          if (!printcounter) {
                            fprintf(fstdout, " size=%llu", (unsigned long long) fmd->getSize());
                          }
                        }

                        if (printfid) {
                          if (!printcounter) {
                            // @todo (esindril) this should be printed using the fxid tag
                            // But to avoid breaking scripts we won't change this for the
                            // moment.
                            fprintf(fstdout, " fid=%08llx", (unsigned long long) fmd->getId());
                          }
                        }

                        if (printuid) {
                          if (!printcounter) {
                            fprintf(fstdout, " uid=%u", (unsigned int) fmd->getCUid());
                          }
                        }

                        if (printgid) {
                          if (!printcounter) {
                            fprintf(fstdout, " gid=%u", (unsigned int) fmd->getCGid());
                          }
                        }

                        if (printfs) {
                          if (!printcounter) {
                            fprintf(fstdout, " fsid=");
                          }

                          eos::IFileMD::LocationVector loc_vect = fmd->getLocations();
                          eos::IFileMD::LocationVector::const_iterator lociter;

                          for (lociter = loc_vect.begin(); lociter != loc_vect.end(); ++lociter) {
                            if (lociter != loc_vect.begin()) {
                              if (!printcounter) {
                                fprintf(fstdout, ",");
                              }
                            }

                            if (!printcounter) {
                              fprintf(fstdout, "%d", (int) *lociter);
                            }
                          }
                        }

                        if ((printpartition) && (!printcounter)) {
                          fprintf(fstdout, " partition=");
                          std::set<std::string> fsPartition;
                          eos::IFileMD::LocationVector loc_vect = fmd->getLocations();
                          eos::IFileMD::LocationVector::const_iterator lociter;

                          for (lociter = loc_vect.begin(); lociter != loc_vect.end(); ++lociter) {
                            // get host name for fs id
                            eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
                            eos::common::FileSystem* filesystem = FsView::gFsView.mIdView.lookupByID(
                                                                    *lociter);

                            if (filesystem) {
                              eos::common::FileSystem::fs_snapshot_t fs;

                              if (filesystem->SnapShotFileSystem(fs, true)) {
                                std::string partition = fs.mHost;
                                partition += ":";
                                partition += fs.mPath;

                                if ((!selectonline) ||
                                    (filesystem->GetActiveStatus() == eos::common::ActiveStatus::kOnline)) {
                                  fsPartition.insert(partition);
                                }
                              }
                            }
                          }

                          for (auto partitionit = fsPartition.begin(); partitionit != fsPartition.end();
                               partitionit++) {
                            if (partitionit != fsPartition.begin()) {
                              fprintf(fstdout, ",");
                            }

                            fprintf(fstdout, "%s", partitionit->c_str());
                          }
                        }

                        if ((printhosts) && (!printcounter)) {
                          fprintf(fstdout, " hosts=");
                          std::set<std::string> fsHosts;
                          eos::IFileMD::LocationVector loc_vect = fmd->getLocations();
                          eos::IFileMD::LocationVector::const_iterator lociter;

                          for (lociter = loc_vect.begin(); lociter != loc_vect.end(); ++lociter) {
                            // get host name for fs id
                            eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
                            eos::common::FileSystem* filesystem = FsView::gFsView.mIdView.lookupByID(
                                                                    *lociter);

                            if (filesystem) {
                              eos::common::FileSystem::fs_snapshot_t fs;

                              if (filesystem->SnapShotFileSystem(fs, true)) {
                                fsHosts.insert(fs.mHost);
                              }
                            }
                          }

                          for (auto hostit = fsHosts.begin(); hostit != fsHosts.end(); hostit++) {
                            if (hostit != fsHosts.begin()) {
                              fprintf(fstdout, ",");
                            }

                            fprintf(fstdout, "%s", hostit->c_str());
                          }
                        }

                        if (printchecksum) {
                          if (!printcounter) {
                            fprintf(fstdout, " checksum=");
                            std::string str;
                            eos::appendChecksumOnStringAsHex(fmd.get(), str);

                            if (!str.empty()) {
                              fprintf(fstdout, "%s", str.c_str());
                            }
                          }
                        }

                        if (printctime) {
                          eos::IFileMD::ctime_t ctime;
                          fmd->getCTime(ctime);

                          if (!printcounter)
                            fprintf(fstdout, " ctime=%llu.%llu", (unsigned long long)
                                    ctime.tv_sec, (unsigned long long) ctime.tv_nsec);
                        }

                        if (printmtime) {
                          eos::IFileMD::ctime_t mtime;
                          fmd->getMTime(mtime);

                          if (!printcounter)
                            fprintf(fstdout, " mtime=%llu.%llu", (unsigned long long)
                                    mtime.tv_sec, (unsigned long long) mtime.tv_nsec);
                        }

                        if (printrep) {
                          if (!printcounter) {
                            fprintf(fstdout, " nrep=%d", (int) fmd->getNumLocation());
                          }
                        }

                        if (printunlink) {
                          if (!printcounter) {
                            fprintf(fstdout, " nunlink=%d", (int) fmd->getNumUnlinkedLocation());
                          }
                        }
                      } else {
                        // print fileinfo -m
                        ProcCommand Cmd;
                        XrdOucString lStdOut = "";
                        XrdOucString lStdErr = "";
                        XrdOucString info = "&mgm.cmd=fileinfo&mgm.path=";
                        info += fspath.c_str();
                        info += "&mgm.file.info.option=-m";
                        Cmd.open("/proc/user", info.c_str(), *pVid, mError);
                        Cmd.AddOutput(lStdOut, lStdErr);

                        if (lStdOut.length()) {
                          fprintf(fstdout, "%s", lStdOut.c_str());
                        }

                        if (lStdErr.length()) {
                          fprintf(fstderr, "%s", lStdErr.c_str());
                        }

                        Cmd.close();
                      }

                      if (!printcounter) {
                        fprintf(fstdout, "\n");
                      }
                    }

                    if (purge_atomic &&
                        (fspath.find(EOS_COMMON_PATH_ATOMIC_FILE_PREFIX) != std::string::npos)) {
                      fprintf(fstdout, "# found atomic %s\n", fspath.c_str());
                      struct stat buf;

                      if ((!gOFS->_stat(fspath.c_str(), &buf, *mError, *pVid, (const char*) 0, 0)) &&
                          ((pVid->uid == 0) || (pVid->uid == buf.st_uid))) {
                        time_t now = time(NULL);

                        if ((now - buf.st_ctime) > 86400) {
                          if (!gOFS->_rem(fspath.c_str(), *mError, *pVid, (const char*) 0)) {
                            fprintf(fstdout, "# purging atomic %s", fspath.c_str());
                          }
                        } else {
                          fprintf(fstdout, "# skipping atomic %s [< 1d old ]\n", fspath.c_str());
                        }
                      }
                    }
                  }
                }

                if (selected) {
                  filecounter++;
                }
              } catch (eos::MDException& e) {
                eos_debug("caught exception %d %s\n", e.getErrno(),
                          e.getMessage().str().c_str());
                viewReadLock.Release();
                //-------------------------------------------
              }
            } else {
              if ((!printcounter) && (!purge_atomic)) {
                if (printxurl) {
                  fprintf(fstdout, "%s", url.c_str());
                }

                fprintf(fstdout, "%s\n", fspath.c_str());
              }

              filecounter++;
            }
          } else {
            // get location
            //-------------------------------------------
            eos::common::RWMutexReadLock viewReadLock(gOFS->eosViewRWMutex);
            std::shared_ptr<eos::IFileMD> fmd;

            try {
              fmd = gOFS->eosView->getFile(fspath.c_str());
            } catch (eos::MDException& e) {
              eos_debug("caught exception %d %s\n", e.getErrno(),
                        e.getMessage().str().c_str());
            }

            if (fmd) {
              viewReadLock.Release();
              //-------------------------------------------

              for (unsigned int i = 0; i < fmd->getNumLocation(); i++) {
                int loc = fmd->getLocation(i);
                uint64_t size = eos::common::LayoutId::GetStripeFileSize(fmd->getLayoutId(),
                                fmd->getSize());

                if (!loc) {
                  eos_err("fsid 0 found %s %llu", fmd->getName().c_str(), fmd->getId());
                  continue;
                }

                filesystembalance[loc] += size;

                if ((i == 0) && (size)) {
                  int bin = (int) log10((double) size);
                  sizedistribution[ bin ] += size;
                  sizedistributionn[ bin ]++;
                }

                eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
                eos::common::FileSystem* filesystem = FsView::gFsView.mIdView.lookupByID(loc);

                if (filesystem) {
                  eos::common::FileSystem::fs_snapshot_t fs;

                  if (filesystem->SnapShotFileSystem(fs, true)) {
                    spacebalance[fs.mSpace.c_str()] += size;
                    schedulinggroupbalance[fs.mGroup.c_str()] += size;
                  }
                }
              }
            } else {
              viewReadLock.Release();
              //-------------------------------------------
            }
          }
        }
      }

      gOFS->MgmStats.Add("FindEntries", pVid->uid, pVid->gid, cnt);
    }

    eos_debug("Listing directories");

    if ((option.find("d")) != STR_NPOS) {
      for (foundit = (*found).begin(); foundit != (*found).end(); foundit++) {
        // eventually call the version purge function if we own this version dir or we are root
        if (purge &&
            (foundit->first.find(EOS_COMMON_PATH_VERSION_PREFIX) != std::string::npos)) {
          struct stat buf;

          if ((!gOFS->_stat(foundit->first.c_str(), &buf, *mError, *pVid, (const char*) 0,
                            0)) &&
              ((pVid->uid == 0) || (pVid->uid == buf.st_uid))) {
            fprintf(fstdout, "# purging %s", foundit->first.c_str());
            gOFS->PurgeVersion(foundit->first.c_str(), *mError, max_version);
          }
        }

        if (selectfaultyacl) {
          // get the attributes and call the verify function
          eos::IContainerMD::XAttrMap map;

          if (!gOFS->_attr_ls(foundit->first.c_str(),
                              *mError,
                              *pVid,
                              (const char*) 0,
                              map)
             ) {
            if ((map.count("sys.acl") || map.count("user.acl"))) {
              if (map.count("sys.acl")) {
                if (Acl::IsValid(map["sys.acl"].c_str(), *mError)) {
                  continue;
                }
              }

              if (map.count("user.acl")) {
                if (Acl::IsValid(map["user.acl"].c_str(), *mError)) {
                  continue;
                }
              }
            } else {
              continue;
            }
          }
        }

        // print directories
        std::string attr = "";

        if (printkey.length()) {
          gOFS->_attr_get(foundit->first.c_str(), *mError, vid, (const char*) 0,
                          printkey.c_str(), attr);

          if (printkey.length()) {
            if (!attr.length()) {
              attr = "undef";
            }

            if (!printcounter) {
              fprintf(fstdout, "%s=%-32s path=", printkey.c_str(), attr.c_str());
            }
          }
        }

        if (!purge && !printcounter) {
          if (printchildcount) {
            //-------------------------------------------
            eos::common::RWMutexReadLock nLock(gOFS->eosViewRWMutex);
            std::shared_ptr<eos::IContainerMD> cmd;
            unsigned long long childfiles = 0;
            unsigned long long childdirs = 0;

            try {
              cmd = gOFS->eosView->getContainer(foundit->first.c_str());
              childfiles = cmd->getNumFiles();
              childdirs = cmd->getNumContainers();
              fprintf(fstdout, "%s ndir=%llu nfiles=%llu\n", foundit->first.c_str(),
                      childdirs, childfiles);
            } catch (eos::MDException& e) {
              eos_debug("caught exception %d %s\n", e.getErrno(),
                        e.getMessage().str().c_str());
            }
          } else {
            if (!printfileinfo) {
              if (printxurl) {
                fprintf(fstdout, "%s", url.c_str());
              }

              fprintf(fstdout, "path=%s", foundit->first.c_str());

              if (printuid || printgid) {
                eos::common::RWMutexReadLock nLock(gOFS->eosViewRWMutex);
                std::shared_ptr<eos::IContainerMD> cmd;

                try {
                  cmd = gOFS->eosView->getContainer(foundit->first.c_str());

                  if (printuid) {
                    fprintf(fstdout, " uid=%u", (unsigned int) cmd->getCUid());
                  }

                  if (printgid) {
                    fprintf(fstdout, " gid=%u", (unsigned int) cmd->getCGid());
                  }
                } catch (eos::MDException& e) {
                  eos_debug("caught exception %d %s\n", e.getErrno(),
                            e.getMessage().str().c_str());
                }
              }
            } else {
              // print fileinfo -m
              ProcCommand Cmd;
              XrdOucString lStdOut = "";
              XrdOucString lStdErr = "";
              XrdOucString info = "&mgm.cmd=fileinfo&mgm.path=";
              info += foundit->first.c_str();
              info += "&mgm.file.info.option=-m";
              Cmd.open("/proc/user", info.c_str(), *pVid, mError);
              Cmd.AddOutput(lStdOut, lStdErr);

              if (lStdOut.length()) {
                fprintf(fstdout, "%s", lStdOut.c_str());
              }

              if (lStdErr.length()) {
                fprintf(fstderr, "%s", lStdErr.c_str());
              }

              Cmd.close();
            }

            fprintf(fstdout, "\n");
          }
        }
      }

      dircounter++;
    }

    if (deepquery) {
      globalfound->clear();
      deepQueryMutex.UnLock();
    } else {
      delete found;
    }

    if (printcounter) {
      fprintf(fstdout, "nfiles=%llu ndirectories=%llu\n", filecounter, dircounter);
    }
  }

  if (calcbalance) {
    XrdOucString sizestring = "";
    google::dense_hash_map<unsigned long, unsigned long long>::iterator it;

    for (it = filesystembalance.begin(); it != filesystembalance.end(); it++) {
      fprintf(fstdout, "fsid=%lu \tvolume=%-12s \tnbytes=%llu\n", it->first,
              eos::common::StringConversion::GetReadableSizeString(sizestring, it->second,
                  "B"), it->second);
    }

    google::dense_hash_map<std::string, unsigned long long>::iterator its;

    for (its = spacebalance.begin(); its != spacebalance.end(); its++) {
      fprintf(fstdout, "space=%s \tvolume=%-12s \tnbytes=%llu\n", its->first.c_str(),
              eos::common::StringConversion::GetReadableSizeString(sizestring, its->second,
                  "B"), its->second);
    }

    google::dense_hash_map<std::string, unsigned long long>::iterator itg;

    for (itg = schedulinggroupbalance.begin(); itg != schedulinggroupbalance.end();
         itg++) {
      fprintf(fstdout, "sched=%s \tvolume=%-12s \tnbytes=%llu\n", itg->first.c_str(),
              eos::common::StringConversion::GetReadableSizeString(sizestring, itg->second,
                  "B"), itg->second);
    }

    google::dense_hash_map<int, unsigned long long>::iterator itsd;

    for (itsd = sizedistribution.begin(); itsd != sizedistribution.end(); itsd++) {
      unsigned long long lowerlimit = 0;
      unsigned long long upperlimit = 0;

      if (((itsd->first) - 1) > 0) {
        lowerlimit = pow(10, (itsd->first));
      }

      if ((itsd->first) > 0) {
        upperlimit = pow(10, (itsd->first) + 1);
      }

      XrdOucString sizestring1;
      XrdOucString sizestring2;
      XrdOucString sizestring3;
      XrdOucString sizestring4;
      unsigned long long avgsize = (unsigned long long)(sizedistributionn[itsd->first]
                                   ? itsd->second / sizedistributionn[itsd->first] : 0);
      fprintf(fstdout,
              "sizeorder=%02d \trange=[ %-12s ... %-12s ] volume=%-12s \tavgsize=%-12s \tnbytes=%llu \t avgnbytes=%llu \t nfiles=%llu\n",
              itsd->first
              , eos::common::StringConversion::GetReadableSizeString(sizestring1, lowerlimit,
                  "B")
              , eos::common::StringConversion::GetReadableSizeString(sizestring2, upperlimit,
                  "B")
              , eos::common::StringConversion::GetReadableSizeString(sizestring3,
                  itsd->second, "B")
              , eos::common::StringConversion::GetReadableSizeString(sizestring4, avgsize,
                  "B")
              , itsd->second
              , avgsize
              , sizedistributionn[itsd->first]
             );
    }
  }

  return SFS_OK;
}

EOSMGMNAMESPACE_END
