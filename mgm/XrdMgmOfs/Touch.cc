// ----------------------------------------------------------------------
// File: Touch.cc
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


// -----------------------------------------------------------------------
// This file is included source code in XrdMgmOfs.cc to make the code more
// transparent without slowing down the compilation time.
// -----------------------------------------------------------------------
#include <sys/types.h>
#include <sys/fsuid.h>
#include "fst/checksum/ChecksumPlugins.hh"
#include "common/XattrCompat.hh"
/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_touch(const char* path,
                  XrdOucErrInfo& error,
                  eos::common::VirtualIdentity& vid,
                  const char* ininfo,
                  bool doLock,
                  bool useLayout,
                  bool truncate,
                  size_t size,
                  bool absorb,
                  const char* linkpath,
                  const char* xs_hex,
                  std::string* errmsg)
/*----------------------------------------------------------------------------*/
/*
 * @brief create(touch) a no-replica file in the namespace
 *
 * @param path file to touch
 * @param error error object
 * @param vid virtual identity of the client
 * @param ininfo CGI
 * @param doLock take the namespace lock
 * @param useLayout create a file using the layout an space policies
 * @param size to preset on the file
 * @param absorb - if true we will try to move the file into the FST tree without hardlinking it
 * @param link path - file to hardlink/symlink to the FST store - requires shared filesystem access
 * @param xs_hex - checksum value in hex format to register
 *
 * @return SFS_OK or SFS_ERROR
 */
/*----------------------------------------------------------------------------*/
{
  EXEC_TIMING_BEGIN("Touch");
  eos_info("path=%s vid.uid=%u vid.gid=%u", path, vid.uid, vid.gid);
  gOFS->MgmStats.Add("Touch", vid.uid, vid.gid, 1);
  // Perform the actual deletion
  errno = 0;
  std::shared_ptr<eos::IFileMD> fmd;
  bool existedAlready = false;

  if (_access(path, W_OK, error, vid, ininfo)) {
    return SFS_ERROR;
  }

  eos::Prefetcher::prefetchFileMDAndWait(gOFS->eosView, path);
  eos::common::RWMutexWriteLock lock;
  std::string fullpath;
  bool verify = true;
  std::vector<eos::IFileMD::location_t> locations;
  int linking = 0;
  bool create_symlink = false;
  bool create_hardlink = false;

  if (doLock) {
    lock.Grab(gOFS->eosViewRWMutex);
  }

  try {
    fmd = gOFS->eosView->getFile(path);
    existedAlready = true;
    errno = 0;
  } catch (eos::MDException& e) {
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
              e.getErrno(), e.getMessage().str().c_str());
  }

  if ((absorb && truncate) || (absorb && !useLayout) ||
      (linkpath && strlen(linkpath) && !useLayout)) {
    error.setErrInfo(EINVAL,
                     "error: -a can not be combined with -0 and -n - a linkpath can only be combined with -a!\n");
    eos_err("-a can not be combined with -0 and -n - a linkpath can only be combined with -a!\n");

    if (errmsg) {
      *errmsg +=
        "error: -a can not be combined with -0 and -n - a linkpath can only be combined with -a!\n";
    }

    return SFS_ERROR;
  }

  if ((absorb || (linkpath && strlen(linkpath))) && vid.uid) {
    error.setErrInfo(EINVAL,
                     "error: external files can only be registered by the root user\n");
    eos_err("external files can only be registred by the root user\n");

    if (errmsg) {
      *errmsg += "error: external files can only be registered by the root user\n";
    }

    return SFS_ERROR;
  }

  // ----------------------------------------------------------------------------------
  // for external filesystem registration:
  // - if this is registration of an existing file, check if this was already adopted
  // - check if we have write permission to create a hardlink
  // - fallback to a symlink if we do cross-device registration
  // ----------------------------------------------------------------------------------

  if (linkpath && strlen(linkpath)) {
    struct stat buf;

    if (!::stat(linkpath, &buf)) {
      if (!::access(linkpath, W_OK)) {
        size = buf.st_size;
        char xv[4096];

        // check if target has already an EOS lfn
        if (lgetxattr(linkpath, "user.eos.lfn", xv, sizeof(xv)) > 0) {
          eos_static_err("file had already an EOS lfn path='%s'", linkpath);
          error.setErrInfo(EEXIST,
                           "error: file has already a registered LFN stored on the extended attributes");

          if (errmsg) {
            *errmsg +=
              "error: file has already a registered LFN stored on the extended attributes";
          }

          return SFS_ERROR;
        }
      } else {
        eos_static_err("is not writable to us path='%s'", linkpath);
        error.setErrInfo(EPERM, "error: provided path is not writable for the MGM");

        if (errmsg) {
          *errmsg += "error: provided path is not writable for the MGM";
        }

        return SFS_ERROR;
      }
    } else {
      eos_err("link path does not exist path='%s'", linkpath);
      error.setErrInfo(ENOENT,
                       "error: provided path is not accessible on the MGM or does not exist");

      if (errmsg) {
        *errmsg +=
          "error: provided path is not accessible on the MGM or does not exist";
      }

      return SFS_ERROR;
    }
  } else {
    if (absorb) {
      error.setErrInfo(EINVAL,
                       "error: link path has to be provdied to absorb a file");
      eos_err("link path has to be provided to absorb a file");

      if (errmsg) {
        *errmsg +=
          "error: when using -a to absorb a file you have to privde the source path";
      }

      return SFS_ERROR;
    }
  }

  try {
    if (!fmd) {
      if (useLayout) {
        lock.Release();
        XrdMgmOfsFile* file = new XrdMgmOfsFile(const_cast<char*>(vid.tident.c_str()));
        XrdOucString opaque = ininfo;

        if (file) {
          int rc = file->open(&vid, path, SFS_O_RDWR | SFS_O_CREAT, 0755, 0,
                              "eos.bookingsize=0&eos.app=fuse");
          error.setErrInfo(strlen(file->error.getErrText()) + 1,
                           file->error.getErrText());

          if (rc != SFS_REDIRECT) {
            error.setErrCode(file->error.getErrInfo());
            errno = file->error.getErrInfo();
            eos_static_info("open failed");
            return SFS_ERROR;
          }

          delete file;
        } else {
          const char* emsg = "allocate file object";
          error.setErrInfo(strlen(emsg) + 1, emsg);
          error.setErrCode(ENOMEM);
          return SFS_ERROR;
        }

        lock.Grab(gOFS->eosViewRWMutex);
        fmd = gOFS->eosView->getFile(path);
      } else {
        fmd = gOFS->eosView->createFile(path, vid.uid, vid.gid);
      }

      // get the file
      fmd->setCUid(vid.uid);
      fmd->setCGid(vid.gid);
      fmd->setCTimeNow();
      fmd->setSize(0);
      fullpath = gOFS->eosView->getUri(fmd.get());

      if (linkpath && strlen(linkpath)) {
        for (unsigned int i = 0; i < fmd->getNumLocation(); i++) {
          const auto loc = fmd->getLocation(i);
          locations.push_back(loc);

          if (loc != 0 && loc != eos::common::TAPE_FS_ID) {
            eos::common::FileSystem::fs_snapshot_t local_snapshot;
            eos::mgm::FileSystem* local_fs = FsView::gFsView.mIdView.lookupByID(loc);
            local_fs->SnapShotFileSystem(local_snapshot);
            std::string local_path = eos::common::FileId::FidPrefix2FullPath(
                                       eos::common::FileId::Fid2Hex(fmd->getId()).c_str(),
                                       local_snapshot.mPath.c_str());

            if (absorb) {
              // try renaming
              int rc = ::rename(linkpath, local_path.c_str());
              fprintf(stderr, "rename gave %d %d\n", rc, errno);

              if (rc) {
                linking = errno;

                if (errmsg) {
                  *errmsg += "error: failed to rename path='" + std::string(
                               linkpath) + std::string("'\n");
                }
              } else {
                eos_info("renamed '%s' => '%s'", linkpath, local_path.c_str());

                if (errmsg) {
                  *errmsg += "info: renamed '";
                  *errmsg += linkpath;
                  *errmsg += "' => '";
                  *errmsg += local_path.c_str();
                  *errmsg += "'\n";
                }

                linkpath = local_path.c_str();
              }
            } else {
              // try with links
              int rc = ::link(linkpath, local_path.c_str());

              if (rc && (errno = EXDEV)) {
                eos_info("cross-device registration detected - using symlink for path='%s'",
                         linkpath);

                if (::symlink(linkpath, local_path.c_str())) {
                  linking = errno;

                  if (errmsg) {
                    *errmsg += "error: failed to create symlink for path='" + std::string(
                                 linkpath) + std::string("'\n");
                  }
                } else {
                  create_symlink = true;
                  eos_info("created symlink '%s' => '%s'", local_path.c_str(), linkpath);

                  if (errmsg) {
                    *errmsg += "info: created symlink '";
                  }

                  if (errmsg) {
                    *errmsg += local_path.c_str();
                  }

                  if (errmsg) {
                    *errmsg += "' => '";
                  }

                  if (errmsg) {
                    *errmsg += linkpath;
                  }

                  if (errmsg) {
                    *errmsg += "\n";
                  }
                }
              } else {
                create_hardlink = true;
                eos_info("created hardlink '%s' => '%s'", local_path.c_str(), linkpath);

                if (errmsg) {
                  *errmsg += "info: created hardlink '";
                }

                if (errmsg) {
                  *errmsg += local_path.c_str();
                }

                if (errmsg) {
                  *errmsg += "' => '";
                }

                if (errmsg) {
                  *errmsg += linkpath;
                }

                if (errmsg) {
                  *errmsg += "\n";
                }
              }

              if (create_hardlink) {
                if (lsetxattr(linkpath, "user.eos.lfn",
                              fullpath.c_str(), fullpath.length(), 0)) {
                  eos_err("can not set user.eos.lfn extended attribute on: '%s'", linkpath);

                  if (errmsg) {
                    *errmsg += "error: cannot set user.eos.lfn extended attribute on :'";
                  }

                  if (errmsg) {
                    *errmsg += linkpath;
                  }

                  if (errmsg) {
                    *errmsg += "'\n";
                  }
                }
              }
            }
          }
        }
      }
    }

    if (!linking && xs_hex) {
      size_t out_sz;
      unsigned long lid = fmd->getLayoutId();
      std::string checksum_name = eos::common::LayoutId::GetChecksumString(lid);
      auto xs_binary = eos::common::StringConversion::Hex2BinDataChar
                       (std::string(xs_hex), out_sz, SHA256_DIGEST_LENGTH);

      if (xs_binary == nullptr) {
        if (errmsg) {
          *errmsg += "error: failed to store checksum extended attributes on '";
          *errmsg += linkpath;
          *errmsg += "'\n";
        }
      } else {
        if (linkpath) {
          if (lsetxattr(linkpath, "user.eos.checksumtype",
                        checksum_name.c_str(), checksum_name.length(), 0) ||
              lsetxattr(linkpath, "user.eos.checksum",
                        xs_binary.get(), out_sz, 0)) {
            if (errmsg) {
              *errmsg += "error: failed to store checksum extended attributes on '";
              *errmsg += linkpath;
              *errmsg += "'\n";
            }
          } else {
            if (errmsg) {
              *errmsg += "info: stored checksum '";
              *errmsg += checksum_name.c_str();
              *errmsg += ":";
              *errmsg += xs_hex;
              *errmsg += "' for linked path '";
              *errmsg += linkpath;
              *errmsg += "'\n";
            }
          }

          // Store this checksum
          eos::Buffer xs_buff;
          xs_buff.putData(xs_binary.get(), SHA256_DIGEST_LENGTH);
          fmd->setChecksum(xs_buff);
          *errmsg += "info: stored checksum '";
          *errmsg += checksum_name.c_str();
          *errmsg += ":";
          *errmsg += xs_hex;
          *errmsg += "'\n";
        }
      }
    }

    fmd->setMTimeNow();
    eos::IFileMD::ctime_t mtime;
    fmd->getMTime(mtime);
    fmd->setCTime(mtime);

    if (truncate) {
      fmd->setSize(0);
    } else {
      if (size) {
        fmd->setSize(size);
      }
    }

    // Store the birth time as an extended attribute if this is a creation
    if (!existedAlready) {
      char btime[256];
      snprintf(btime, sizeof(btime), "%lu.%lu", mtime.tv_sec, mtime.tv_nsec);
      fmd->setAttribute("sys.eos.btime", btime);
    }

    if (create_hardlink) {
      fmd->setAttribute("sys.hardlink.path", linkpath);
    }

    if (create_symlink) {
      fmd->setAttribute("sys.symlink.path", linkpath);
    }

    if (absorb) {
      fmd->setAttribute("sys.absorbed.path", linkpath);
    }

    gOFS->eosView->updateFileStore(fmd.get());
    unsigned long long cid = fmd->getContainerId();
    std::shared_ptr<eos::IContainerMD> cmd =
      gOFS->eosDirectoryService->getContainerMD(cid);
    cmd->setMTime(mtime);
    cmd->notifyMTimeChange(gOFS->eosDirectoryService);

    // Check if there is any quota node to be updated
    if (!existedAlready) {
      try {
        eos::IQuotaNode* ns_quota = gOFS->eosView->getQuotaNode(cmd.get());

        if (ns_quota) {
          ns_quota->addFile(fmd.get());
        }
      } catch (const eos::MDException& eq) {
        // no quota node
      }
    }

    gOFS->eosView->updateContainerStore(cmd.get());
    const eos::FileIdentifier fid = fmd->getIdentifier();
    const eos::ContainerIdentifier did = cmd->getIdentifier();
    const eos::ContainerIdentifier pdid = cmd->getParentIdentifier();

    if (doLock) {
      lock.Release();
    }

    gOFS->FuseXCastMD(fid, did, mtime, true);
    gOFS->FuseXCastRefresh(did, pdid);
    errno = 0;
  } catch (eos::MDException& e) {
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
              e.getErrno(), e.getMessage().str().c_str());
  }

  if (linking) {
    errno = linking;
  } else {
    if (verify) {
      XrdOucString options;

      for (auto loc : locations) {
        if (gOFS->_verifystripe(fullpath.c_str(), error, vid, loc, options)) {
          // failed
        }
      }
    }
  }

  if (errno) {
    return Emsg("utimes", error, errno, "touch", path);
  }

  EXEC_TIMING_END("Touch");
  return SFS_OK;
}
