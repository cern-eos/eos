// ----------------------------------------------------------------------
// File: proc/user/File.cc
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
#include "mgm/Quota.hh"
#include "mgm/Stat.hh"
#include "common/table_formatter/TableFormatterBase.hh"
#include "common/table_formatter/TableCell.hh"
#include "common/LayoutId.hh"
#include "common/Timing.hh"
#include "common/Path.hh"
#include "namespace/interface/IView.hh"
#include "namespace/interface/ContainerIterators.hh"
#include "namespace/Prefetcher.hh"
#include "namespace/Resolver.hh"
#include "namespace/utils/Etag.hh"
#include "namespace/utils/Checksum.hh"
#include <json/json.h>

EOSMGMNAMESPACE_BEGIN
//------------------------------------------------------------------------------
// Fileinfo method
//------------------------------------------------------------------------------
int
ProcCommand::Fileinfo()
{
  gOFS->MgmStats.Add("FileInfo", pVid->uid, pVid->gid, 1);
  XrdOucString spath = pOpaque->Get("mgm.path");
  const char* inpath = spath.c_str();
  NAMESPACEMAP;
  PROC_BOUNCE_ILLEGAL_NAMES;
  PROC_BOUNCE_NOT_ALLOWED;

  struct stat buf;
  unsigned long long fid = 0;

  if ((!spath.beginswith("fid:")) && (!spath.beginswith("fxid:")) &&
      (!spath.beginswith("pid:")) && (!spath.beginswith("pxid:")) &&
      (!spath.beginswith("inode:"))) {
    if (gOFS->_stat(path, &buf, *mError, *pVid, (char*) 0, (std::string*)0,
                    false)) {
      stdErr = "error: cannot stat '";
      stdErr += path;
      stdErr += "'\n";
      retc = ENOENT;
      return SFS_OK;
    }

    if (S_ISDIR(buf.st_mode)) {
      fid = buf.st_ino;
    } else {
      fid = eos::common::FileId::InodeToFid(buf.st_ino);
    }
  } else {
    XrdOucString sfid = spath;

    if ((sfid.replace("inode:", ""))) {
      size_t pos = 0;

      try {
        fid = std::stoull(sfid.c_str(), &pos, 10);
      } catch (...) {
        stdErr = "error: inode option takes a fuse inode decimal value";
        retc = EINVAL;
        return SFS_OK;
      }

      if (pos != (size_t)sfid.length()) {
        stdErr = "error: inode option takes a fuse inode decimal value - some "
                 "characters were not converted";
        retc = EINVAL;
        return SFS_OK;
      }

      if (eos::common::FileId::IsFileInode(fid)) {
        buf.st_mode = S_IFREG;
        fid = eos::common::FileId::InodeToFid(fid);
        spath = "fid:";
        spath += eos::common::StringConversion::GetSizeString(sfid, fid);
        path = spath.c_str();
      } else {
        buf.st_mode = S_IFDIR;
        spath.replace("inode:", "pid:");
        path = spath.c_str();
      }
    } else { // one of fid, fxid, pid, pxid
      buf.st_mode = (sfid.beginswith("f")) ? S_IFREG : S_IFDIR;
      sfid.replace('p', 'f', 0, 1);
      fid = Resolver::retrieveFileIdentifier(sfid).getUnderlyingUInt64();
    }
  }

  if (mJsonFormat) {
    if (S_ISDIR(buf.st_mode)) {
      return DirJSON(fid, 0);
    } else {
      return FileJSON(fid, 0);
    }
  } else {
    if (S_ISDIR(buf.st_mode)) {
      return DirInfo(path);
    } else {
      return FileInfo(path);
    }
  }
}

//------------------------------------------------------------------------------
// Fileinfo given path
//------------------------------------------------------------------------------
int
ProcCommand::FileInfo(const char* path)
{
  XrdOucString option = pOpaque->Get("mgm.file.info.option");
  XrdOucString spath = path;
  bool detached = false;
  uint64_t clock = 0;
  {
    eos::common::RWMutexReadLock viewReadLock;
    std::shared_ptr<eos::IFileMD> fmd;

    if ((spath.beginswith("fid:") || (spath.beginswith("fxid:")))) {
      unsigned long long fid = Resolver::retrieveFileIdentifier(
                                 spath).getUnderlyingUInt64();
      // Reference by fid+fxid
      eos::Prefetcher::prefetchFileMDAndWait(gOFS->eosView, fid);
      viewReadLock.Grab(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__, __FILE__);
      std::string nspath;

      try {
        fmd = gOFS->eosFileService->getFileMD(fid, &clock);
        nspath = gOFS->eosView->getUri(fmd.get());

        if (fmd->isLink()) {
          try {
            spath = gOFS->eosView->getRealPath(nspath).c_str();
          } catch (const eos::MDException& e) {
            // The link points to a location outside the EOS namespace therefore
            // we return info about the symlink object
            spath = nspath.c_str();
          }
        } else {
          spath = nspath.c_str();
        }
      } catch (eos::MDException& e) {
        errno = e.getErrno();
        stdErr = "error: cannot retrieve file meta data - ";
        stdErr += e.getMessage().str().c_str();
        eos_debug("msg=\"exception retrieving file metadata\" ec=%d "
                  "emsg=\"%s\"\n", e.getErrno(), e.getMessage().str().c_str());
      }

      // Detect detached state for fid/fxid reference
      detached = nspath.empty();
    } else {
      // Reference by path
      eos::Prefetcher::prefetchFileMDAndWait(gOFS->eosView, spath.c_str());
      viewReadLock.Grab(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__, __FILE__);

      try {
        fmd = gOFS->eosView->getFile(spath.c_str());
      } catch (eos::MDException& e) {
        try {
          // Maybe this is a symlink pointing outside the EOS namespace
          fmd = gOFS->eosView->getFile(spath.c_str(), false);
        } catch (eos::MDException& ee) {
          errno = ee.getErrno();
          stdErr = "error: cannot retrieve file meta data - ";
          stdErr += ee.getMessage().str().c_str();
          eos_debug("msg=\"exception retrieving file metadata\" ec=%d "
                    "emsg=\"%s\"\n", ee.getErrno(), ee.getMessage().str().c_str());
        }
      }

      if (fmd) {
        try {
          std::string nspath = gOFS->eosView->getUri(fmd.get());

          if (fmd->isLink()) {
            spath = gOFS->eosView->getRealPath(nspath).c_str();
          } else {
            spath = nspath.c_str();
          }
        } catch (eos::MDException& ee) {
          fmd.reset();
          errno = ee.getErrno();
          stdErr = "error: cannot retrieve file meta data - ";
          stdErr += ee.getMessage().str().c_str();
          eos_debug("msg=\"exception retrieving file metadata\" ec=%d "
                    "emsg=\"%s\"\n", ee.getErrno(), ee.getMessage().str().c_str());
        }
      }
    }

    if (!fmd) {
      retc = errno;
      viewReadLock.Release();
      //-------------------------------------------
    } else {
      using eos::common::Timing;
      using eos::common::FileId;
      using eos::common::LayoutId;
      using eos::common::StringConversion;
      // Make a copy of the file metadata object
      std::shared_ptr<eos::IFileMD> fmd_copy(fmd->clone());
      fmd.reset();
      // TODO (esindril): All this copying should be reviewed
      viewReadLock.Release();
      //-------------------------------------------
      XrdOucString sizestring;
      bool Monitoring = false;
      bool Envformat = false;
      bool outputFilter = false;
      std::ostringstream out;
      const std::string hex_fid = FileId::Fid2Hex(fmd_copy->getId());
      const std::string hex_pid = FileId::Fid2Hex(fmd_copy->getContainerId());

      if ((option.find("-m")) != STR_NPOS) {
        Monitoring = true;
      }

      if ((option.find("-env")) != STR_NPOS) {
        Envformat = true;
        Monitoring = false;
      }

      if (Envformat) {
        std::string env;
        fmd_copy->getEnv(env);
        eos::common::Path cPath(spath.c_str());
        out << env << "&container=" << cPath.GetParentPath() << std::endl;
      } else {
        // Filter output according to requested filters
        // Note: filters affect only non-monitoring output
        if (!Monitoring) {
          if ((option.find("-path")) != STR_NPOS) {
            out << "path:   " << spath << std::endl;
          }

          if ((option.find("-fxid")) != STR_NPOS) {
            out << "fxid:   " << hex_fid << std::endl;
          }

          if ((option.find("-fid")) != STR_NPOS) {
            out << "fid:    " << fmd_copy->getId() << std::endl;
          }

          if ((option.find("-size")) != STR_NPOS) {
            out << "size:   " << fmd_copy->getSize() << std::endl;
          }

          if ((option.find("-checksum")) != STR_NPOS) {
            std::string xs;
            eos::appendChecksumOnStringAsHex(fmd_copy.get(), xs);
            out << "xstype: " << LayoutId::GetChecksumString(fmd_copy->getLayoutId())
                << std::endl
                << "xs:     " << xs << std::endl;
          }

          // Mark filter flag if out is not empty
          outputFilter = (out.tellp() != std::streampos(0));
        }

        if (Monitoring || !outputFilter) {
          eos::IFileMD::XAttrMap xattrs = fmd_copy->getAttributes();
          bool showFullpath = (option.find("-fullpath") != STR_NPOS);
          bool showProxygroup = (option.find("-proxy") != STR_NPOS);
          eos::IFileMD::ctime_t mtime;
          eos::IFileMD::ctime_t ctime;
          eos::IFileMD::ctime_t btime {0, 0};
          fmd_copy->getCTime(ctime);
          fmd_copy->getMTime(mtime);

          if (xattrs.count("sys.eos.btime")) {
            Timing::Timespec_from_TimespecStr(xattrs["sys.eos.btime"], btime);
          }

          time_t filectime = (time_t) ctime.tv_sec;
          time_t filemtime = (time_t) mtime.tv_sec;
          time_t filebtime = (time_t) btime.tv_sec;
          std::string etag, xs_spaces;
          eos::calculateEtag(fmd_copy.get(), etag);
          eos::appendChecksumOnStringAsHex(fmd_copy.get(), xs_spaces, ' ');
          std::string redundancy = eos::common::LayoutId::GetRedundancySymbol(
                                     fmd_copy->hasLocation(EOS_TAPE_FSID),
                                     eos::common::LayoutId::GetRedundancy(fmd_copy->getLayoutId(),
									  fmd_copy->getNumLocation()),
				     eos::common::LayoutId::GetExcessStripeNumber(fmd_copy->getLayoutId()));

          if (!Monitoring) {
            out << "  File: '" << spath << "'"
                << "  Flags: " << StringConversion::IntToOctal((int) fmd_copy->getFlags(), 4);

            if (clock) {
              out << "  Clock: " << FileId::Fid2Hex(clock);
            }

            out << std::endl;
            out << "  Size: " << fmd_copy->getSize() << std::endl
                << "Modify: " << eos::common::Timing::ltime(filemtime)
                << " Timestamp: " << eos::common::Timing::TimespecToString(mtime)
                << std::endl
                << "Change: " << eos::common::Timing::ltime(filectime)
                << " Timestamp: " << eos::common::Timing::TimespecToString(ctime)
                << std::endl
                << " Birth: " << eos::common::Timing::ltime(filebtime)
                << " Timestamp: " << eos::common::Timing::TimespecToString(btime)
                << std::endl
                << "  CUid: " << fmd_copy->getCUid()
                << " CGid: " << fmd_copy->getCGid()
                << " Fxid: " << hex_fid
                << " Fid: " << fmd_copy->getId()
                << " Pid: " << fmd_copy->getContainerId()
                << " Pxid: " << hex_pid
                << std::endl;
            out << "XStype: " << LayoutId::GetChecksumString(fmd_copy->getLayoutId())
                << "    XS: " << xs_spaces
                << "    ETAGs: " << etag
                << std::endl;
            out << "Layout: " << LayoutId::GetLayoutTypeString(fmd_copy->getLayoutId())
                << " Stripes: " << (LayoutId::GetStripeNumber(fmd_copy->getLayoutId()) + 1)
                << " Blocksize: " << LayoutId::GetBlockSizeString(fmd_copy->getLayoutId())
                << " LayoutId: " << FileId::Fid2Hex(fmd_copy->getLayoutId())
                << " Redundancy: " << redundancy
                << std::endl;
            out << "  #Rep: " << fmd_copy->getNumLocation() << std::endl;

            if (fmd_copy->hasLocation(EOS_TAPE_FSID)) {
              std::string storage_class = xattrs["sys.archive.storage_class"];
              std::string archive_id = xattrs["sys.archive.file_id"];
              out << "TapeID: " << (archive_id.length() ? archive_id : "undef") <<
                  " StorageClass: " << (storage_class.length() ? storage_class : "none")
                  << std::endl;
            }
          } else {
            std::string xs;

            if (LayoutId::GetChecksumLen(fmd_copy->getLayoutId())) {
              eos::appendChecksumOnStringAsHex(fmd_copy.get(), xs);
            } else {
              xs = "0";
            }

            out << "keylength.file=" << spath.length()
                << " file=" << spath
                << " size=" << fmd_copy->getSize()
                << " mtime=" << mtime.tv_sec << "." << mtime.tv_nsec
                << " ctime=" << ctime.tv_sec << "." << ctime.tv_nsec
                << " btime=" << btime.tv_sec << "." << btime.tv_nsec
                << " clock=" << clock
                << " mode=" << StringConversion::IntToOctal((int) fmd_copy->getFlags(), 4)
                << " uid=" << fmd_copy->getCUid()
                << " gid=" << fmd_copy->getCGid()
                << " fxid=" << hex_fid
                << " fid=" << fmd_copy->getId()
                << " ino=" << FileId::FidToInode(fmd_copy->getId())
                << " pid=" << fmd_copy->getContainerId()
                << " pxid=" << hex_pid
                << " xstype=" << LayoutId::GetChecksumString(fmd_copy->getLayoutId())
                << " xs=" << xs
                << " etag=" << etag
                << " detached=" << detached
                << " layout=" << LayoutId::GetLayoutTypeString(fmd_copy->getLayoutId())
                << " nstripes=" << (LayoutId::GetStripeNumber(fmd_copy->getLayoutId()) + 1)
                << " lid=" << FileId::Fid2Hex(fmd_copy->getLayoutId())
                << " nrep=" << fmd_copy->getNumLocation()
                << " ";

            for (const auto& elem : xattrs) {
              out << "xattrn=" << elem.first
                  << " xattrv=" << elem.second << " ";
            }
          }

          eos::IFileMD::LocationVector::const_iterator lociter;
          eos::IFileMD::LocationVector loc_vect = fmd_copy->getLocations();
          std::vector<unsigned int> selectedfs;
          std::vector<std::string> proxys;
          std::vector<std::string> firewalleps;
          std::vector<unsigned int> unavailfs;
          std::vector<unsigned int> replacedfs;
          size_t fsIndex;
          Scheduler::AccessArguments acsargs;
          int i = 0;
          int schedretc = -1;
          TableHeader table_mq_header;
          TableData table_mq_data;
          TableFormatterBase table_mq;
          bool table_mq_header_exist = false;

          for (lociter = loc_vect.begin(); lociter != loc_vect.end(); ++lociter) {
            // Ignore filesystem id 0
            if (!(*lociter)) {
              eos_err("msg=\"found file on fsid=0\" fxid=%08llx",
                      fmd_copy->getId());
              continue;
            }

            XrdOucString location = "";
            location += (int) * lociter;
            eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
            eos::common::FileSystem* filesystem = FsView::gFsView.mIdView.lookupByID(
                                                    *lociter);

            if (filesystem) {
              // For the fullpath option we output the physical location of the
              // replicas
              std::string fullpath;

              if (showFullpath) {
                fullpath = FileId::FidPrefix2FullPath(hex_fid.c_str(),
                                                      filesystem->GetPath().c_str());
              }

              if (!Monitoring) {
                std::string format =
                  "header=1|key=host:width=24:format=s|key=schedgroup:width=16:format=s|key=path:width=16:format=s|key=stat.boot:width=10:format=s|key=configstatus:width=14:format=s|key=local.drain:width=12:format=s|key=stat.active:width=8:format=s|key=stat.geotag:width=24:format=s";

                if (showProxygroup) {
                  format += "|key=proxygroup:width=24:format=s";
                }

                filesystem->Print(table_mq_header, table_mq_data, format);

                // Build header
                if (!table_mq_header.empty()) {
                  TableHeader table_mq_header_temp;
                  table_mq_header_temp.push_back(std::make_tuple("no.", 3, "-l"));
                  table_mq_header_temp.push_back(std::make_tuple("fs-id", 6, "l"));
                  std::copy(table_mq_header.begin(), table_mq_header.end(),
                            std::back_inserter(table_mq_header_temp));

                  if (showFullpath) {
                    table_mq_header_temp.push_back(std::make_tuple("physical location", 18, "s"));
                  }

                  table_mq.SetHeader(table_mq_header_temp);
                  table_mq_header_exist = true;
                }

                // Build body
                if (table_mq_header_exist) {
                  TableData table_mq_data_temp;

                  for (auto& row : table_mq_data) {
                    if (!row.empty()) {
                      table_mq_data_temp.emplace_back();
                      table_mq_data_temp.back().push_back(TableCell(i, "l"));
                      table_mq_data_temp.back().push_back(TableCell(*lociter, "l"));

                      for (auto& cell : row) {
                        table_mq_data_temp.back().push_back(cell);
                      }

                      if (showFullpath) {
                        table_mq_data_temp.back().push_back(TableCell(fullpath.c_str(), "s"));
                      }
                    }
                  }

                  table_mq.AddRows(table_mq_data_temp);
                  table_mq_data.clear();
                  table_mq_data_temp.clear();
                }

                if ((filesystem->GetString("proxygroup").size()) &&
                    (filesystem->GetString("proxygroup") != "<none>") &&
                    filesystem->GetString("filestickyproxydepth").size() &&
                    filesystem->GetLongLong("filestickyproxydepth") >= 0) {
                  // we do the scheduling only once when we meet a filesystem that requires it
                  if (schedretc == -1) {
                    acsargs.bookingsize = fmd_copy->getSize();
                    acsargs.dataproxys = &proxys;
                    acsargs.firewallentpts = NULL;
                    acsargs.forcedfsid = 0;
                    std::string space = filesystem->GetString("schedgroup");
                    space.resize(space.rfind("."));
                    acsargs.forcedspace = space.c_str();
                    acsargs.fsindex = &fsIndex;
                    acsargs.isRW = false;
                    acsargs.lid = fmd_copy->getLayoutId();
                    acsargs.inode = (ino64_t) fmd_copy->getId();
                    acsargs.locationsfs = &selectedfs;

                    for (auto it = loc_vect.begin(); it != loc_vect.end(); it++) {
                      selectedfs.push_back(*it);
                    }

                    std::string stried_cgi = "";
                    acsargs.tried_cgi = &stried_cgi;
                    acsargs.unavailfs = &unavailfs;
                    acsargs.vid = &vid;

                    if (!acsargs.isValid()) {
                      // there is something wrong in the arguments of file access
                      eos_static_err("msg=\"open - invalid access argument\"");
                    }

                    schedretc = Scheduler::FileAccess(&acsargs);

                    if (schedretc) {
                      eos_static_warning("msg=\"cannot schedule the proxy\"");
                    }
                  }

                  if (schedretc) {
                    out << "     sticky to undefined";
                  } else {
                    size_t k;

                    for (k = 0; k < loc_vect.size() && selectedfs[k] != loc_vect[i]; k++);

                    out << "sticky to " << proxys[k];
                  }
                }
              } else {
                out << "fsid=" << location << " ";

                if (showFullpath) {
                  out << "fullpath=" << fullpath << " ";
                }
              }
            } else {
              if (!Monitoring) {
                if (location != EOS_TAPE_FSID) {
                  out << std::setw(3) << i << std::setw(8) << location
                      << " NA" << std::endl;
                }
              } else {
                out << "fsid=" << location << " ";
              }
            }

            i++;
          }

          out << table_mq.GenerateTable(HEADER);
          eos::IFileMD::LocationVector unlink_vect = fmd_copy->getUnlinkedLocations();

          for (lociter = unlink_vect.begin(); lociter != unlink_vect.end(); ++lociter) {
            if (!Monitoring) {
              out << "(undeleted) $ " << *lociter << std::endl;
            } else {
              out << "fsdel=" << *lociter << " ";
            }
          }

          if (!Monitoring) {
            out << "*******";
          }
        }
      }

      stdOut += out.str().c_str();
    }
  }
  return SFS_OK;
}

//------------------------------------------------------------------------------
// DirInfo by path
//------------------------------------------------------------------------------
int
ProcCommand::DirInfo(const char* path)
{
  XrdOucString option = pOpaque->Get("mgm.file.info.option");
  XrdOucString spath = path;
  bool detached = false;
  uint64_t clock = 0;
  {
    eos::common::RWMutexReadLock viewReadLock;
    std::shared_ptr<eos::IContainerMD> dmd;

    if ((spath.beginswith("pid:") || (spath.beginswith("pxid:")))) {
      unsigned long long fid = 0;

      if (spath.beginswith("pid:")) {
        spath.replace("pid:", "");
        fid = strtoull(spath.c_str(), 0, 10);
      }

      if (spath.beginswith("pxid:")) {
        spath.replace("pxid:", "");
        fid = strtoull(spath.c_str(), 0, 16);
      }

      // reference by pid+pxid
      //-------------------------------------------
      eos::Prefetcher::prefetchContainerMDAndWait(gOFS->eosView, fid);
      viewReadLock.Grab(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__, __FILE__);
      std::string nspath;

      try {
        dmd = gOFS->eosDirectoryService->getContainerMD(fid, &clock);
        nspath = gOFS->eosView->getUri(dmd.get());
        spath = nspath.c_str();
      } catch (eos::MDException& e) {
        errno = e.getErrno();
        stdErr = "error: cannot retrieve directory meta data - ";
        stdErr += e.getMessage().str().c_str();
        eos_debug("msg=\"exception retrieving container metadata\" ec=%d "
                  "emsg=\"%s\"\n", e.getErrno(), e.getMessage().str().c_str());
      }

      // Detect detached state for pid/pxid reference
      detached = nspath.empty();
    } else {
      eos::Prefetcher::prefetchContainerMDAndWait(gOFS->eosView, spath.c_str());
      // reference by path
      //-------------------------------------------
      viewReadLock.Grab(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__, __FILE__);

      try {
        dmd = gOFS->eosView->getContainer(spath.c_str());
      } catch (eos::MDException& e) {
        errno = e.getErrno();
        stdErr = "error: cannot retrieve directory meta data - ";
        stdErr += e.getMessage().str().c_str();
        eos_debug("msg=\"exception retrieving container metadata\" ec=%d "
                  "emsg=\"%s\"\n", e.getErrno(), e.getMessage().str().c_str());
      }
    }

    if (!dmd) {
      retc = errno;
      viewReadLock.Release();
      //-------------------------------------------
    } else {
      using eos::common::Timing;
      using eos::common::FileId;
      using eos::common::LayoutId;
      using eos::common::StringConversion;
      size_t num_containers = dmd->getNumContainers();
      size_t num_files = dmd->getNumFiles();
      size_t tree_size = dmd->getTreeSize();

      std::shared_ptr<eos::IContainerMD> dmd_copy(dmd->clone());
      dmd.reset();
      viewReadLock.Release();
      //-------------------------------------------
      XrdOucString sizestring;
      bool Monitoring = false;
      bool outputFilter = false;
      std::ostringstream out;
      const std::string hex_fid = FileId::Fid2Hex(dmd_copy->getId());
      const std::string hex_pid = FileId::Fid2Hex(dmd_copy->getParentId());

      if ((option.find("-m")) != STR_NPOS) {
        Monitoring = true;
      }

      // Filter output according to requested filters
      // Note: filters affect only non-monitoring output
      if (!Monitoring) {
        if ((option.find("-path")) != STR_NPOS) {
          out << "path:   " << spath << std::endl;
        }

        if ((option.find("-fxid")) != STR_NPOS) {
          out << "fxid:   " << hex_fid << std::endl;
        }

        if ((option.find("-fid")) != STR_NPOS) {
          out << "fid:    " << dmd_copy->getId() << std::endl;
        }

        if ((option.find("-size")) != STR_NPOS) {
          out << "size:   " << (num_containers + num_files) << std::endl;
        }

        outputFilter = ((out.tellp() != std::streampos(0)) ||
                        (option.find("-checksum") != STR_NPOS));
      }

      if (Monitoring || !outputFilter) {
        eos::IFileMD::XAttrMap xattrs = dmd_copy->getAttributes();
        eos::IContainerMD::ctime_t ctime;
        eos::IContainerMD::mtime_t mtime;
        eos::IContainerMD::tmtime_t tmtime;
        eos::IContainerMD::ctime_t btime {0, 0};
        dmd_copy->getCTime(ctime);
        dmd_copy->getMTime(mtime);
        dmd_copy->getTMTime(tmtime);

        if (xattrs.count("sys.eos.btime")) {
          Timing::Timespec_from_TimespecStr(xattrs["sys.eos.btime"], btime);
        }

        time_t filectime = (time_t) ctime.tv_sec;
        time_t filemtime = (time_t) mtime.tv_sec;
        time_t filetmtime = (time_t) tmtime.tv_sec;
        time_t filebtime = (time_t) btime.tv_sec;
        char fid[32];
        snprintf(fid, 32, "%llu", (unsigned long long) dmd_copy->getId());
        std::string etag;
        eos::calculateEtag(dmd_copy.get(), etag);

        if (!Monitoring) {
          out << "  Directory: '" << spath << "'"
              << "  Treesize: " << tree_size << std::endl;
          out << "  Container: " << num_containers
              << "  Files: " << num_files
              << "  Flags: " << StringConversion::IntToOctal(dmd_copy->getMode(), 4);

          if (clock) {
            out << "  Clock: " << FileId::Fid2Hex(clock);
          }

          out << std::endl;
          out << "Modify: " << eos::common::Timing::ltime(filemtime)
              << " Timestamp: " << eos::common::Timing::TimespecToString(mtime)
              << std::endl
              << "Change: " << eos::common::Timing::ltime(filectime)
              << " Timestamp: " << eos::common::Timing::TimespecToString(ctime)
              << std::endl
              << "Sync  : " << eos::common::Timing::ltime(filetmtime)
              << " Timestamp: " << eos::common::Timing::TimespecToString(tmtime)
              << std::endl
              << "Birth : " << eos::common::Timing::ltime(filebtime)
              << " Timestamp: " << eos::common::Timing::TimespecToString(btime)
              << std::endl
              << "  CUid: " << dmd_copy->getCUid()
              << " CGid: " << dmd_copy->getCGid()
              << " Fxid: " << hex_fid
              << " Fid: " << dmd_copy->getId()
              << " Pid: " << dmd_copy->getParentId()
              << " Pxid: " << hex_pid
              << std::endl
              << "  ETAG: " << etag
              << std::endl;
        } else {
          out << "keylength.file=" << spath.length()
              << " file=" << spath
              << " treesize=" << tree_size
              << " container=" << num_containers
              << " files=" << num_files
              << " mtime=" << mtime.tv_sec << "." << mtime.tv_nsec
              << " ctime=" << ctime.tv_sec << "." << ctime.tv_nsec
              << " btime=" << btime.tv_sec << "." << btime.tv_nsec
              << " stime=" << tmtime.tv_sec << "." << tmtime.tv_nsec
              << " clock=" << clock
              << " mode=" << StringConversion::IntToOctal((int) dmd_copy->getMode(), 4)
              << " uid=" << dmd_copy->getCUid()
              << " gid=" << dmd_copy->getCGid()
              << " fxid=" << hex_fid
              << " fid=" << dmd_copy->getId()
              << " ino=" << dmd_copy->getId()
              << " pid=" << dmd_copy->getParentId()
              << " pxid=" << hex_pid
              << " etag=" << etag
              << " detached=" << detached
              << " ";

          for (const auto& elem : xattrs) {
            out << "xattrn=" << elem.first
                << " xattrv=" << elem.second << " ";
          }
        }
      }

      stdOut += out.str().c_str();
    }
  }
  return SFS_OK;
}

//------------------------------------------------------------------------------
// File info in JSON format
//------------------------------------------------------------------------------
int
ProcCommand::FileJSON(uint64_t fid, Json::Value* ret_json, bool dolock)
{
  eos::IFileMD::ctime_t ctime;
  eos::IFileMD::ctime_t mtime;
  eos::IFileMD::ctime_t btime {0, 0};
  eos_static_debug("msg=\"JSON fileinfo\" fxid=%08llx", fid);
  Json::Value json;
  json["id"] = (Json::Value::UInt64) fid;
  const std::string hex_fid = eos::common::FileId::Fid2Hex(fid);

  try {
    eos::Prefetcher::prefetchFileMDWithParentsAndWait(gOFS->eosView, fid);
    eos::common::RWMutexReadLock viewReadLock;
    std::shared_ptr<eos::IFileMD> fmd;
    std::string path;
    bool detached;

    if (dolock) {
      viewReadLock.Grab(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__, __FILE__);
    }

    try {
      fmd = gOFS->eosFileService->getFileMD(fid);
      path = gOFS->eosView->getUri(fmd.get());
    } catch (eos::MDException& e) {
      eos_static_debug("msg=\"exception retrieving file metadata\" ec=%d "
                       "emsg=\"%s\"\n", e.getErrno(),
                       e.getMessage().str().c_str());

      if (!fmd) {
        if (dolock) {
          viewReadLock.Release();
        }

        std::rethrow_exception(std::current_exception());
      }
    }

    std::shared_ptr<eos::IFileMD> fmd_copy(fmd->clone());
    fmd.reset();

    if (dolock) {
      viewReadLock.Release();
    }

    // TODO (esindril): All this copying should be reviewed
    //--------------------------------------------------------------------------
    fmd_copy->getCTime(ctime);
    fmd_copy->getMTime(mtime);
    unsigned long long nlink = (fmd_copy->isLink()) ? 1 :
                               fmd_copy->getNumLocation();
    eos::IFileMD::XAttrMap xattrs = fmd_copy->getAttributes();

    if (xattrs.count("sys.eos.btime")) {
      eos::common::Timing::Timespec_from_TimespecStr(xattrs["sys.eos.btime"],
          btime);
    }

    if ((detached = path.empty())) {
      path = SSTR("fid:" << fid);
    }

    json["fxid"] = hex_fid.c_str();
    json["inode"] = (Json::Value::UInt64) eos::common::FileId::FidToInode(fid);
    json["ctime"] = (Json::Value::UInt64) ctime.tv_sec;
    json["ctime_ns"] = (Json::Value::UInt64) ctime.tv_nsec;
    json["atime"] = (Json::Value::UInt64) ctime.tv_sec;
    json["atime_ns"] = (Json::Value::UInt64) ctime.tv_nsec;
    json["mtime"] = (Json::Value::UInt64) mtime.tv_sec;
    json["mtime_ns"] = (Json::Value::UInt64) mtime.tv_nsec;
    json["btime"] = (Json::Value::UInt64) btime.tv_sec;
    json["btime_ns"] = (Json::Value::UInt64) btime.tv_nsec;
    json["size"] = (Json::Value::UInt64) fmd_copy->getSize();
    json["uid"] = fmd_copy->getCUid();
    json["gid"] = fmd_copy->getCGid();
    json["mode"] = fmd_copy->getFlags();
    json["nlink"] = (Json::Value::UInt64) nlink;
    json["name"] = fmd_copy->getName();
    json["path"] = path;
    json["detached"] = detached;
    json["pid"] = (Json::Value::UInt64) fmd_copy->getContainerId();
    json["layout"] = eos::common::LayoutId::GetLayoutTypeString(
                       fmd_copy->getLayoutId());
    json["nstripes"] = (int)(eos::common::LayoutId::GetStripeNumber(
                               fmd_copy->getLayoutId())
                             + 1);

    if (fmd_copy->isLink()) {
      json["target"] = fmd_copy->getLink();
    }

    Json::Value jsonxattr;

    for (const auto& elem : xattrs) {
      jsonxattr[elem.first] = elem.second;
    }

    if (fmd_copy->numAttributes()) {
      json["xattr"] = jsonxattr;
    }

    Json::Value jsonfsids;
    eos::IFileMD::LocationVector loc_vect = fmd_copy->getLocations();

    // Get host name for the fs ids
    for (auto loc_it = loc_vect.begin(); loc_it != loc_vect.end(); ++loc_it) {
      eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
      Json::Value jsonfsinfo;
      eos::common::FileSystem* filesystem = FsView::gFsView.mIdView.lookupByID(
                                              *loc_it);

      if (filesystem) {
        eos::common::FileSystem::fs_snapshot_t fs;

        if (filesystem->SnapShotFileSystem(fs, true)) {
          std::string fstpath =
            eos::common::FileId::FidPrefix2FullPath(hex_fid.c_str(),
                fs.mPath.c_str());
          jsonfsinfo["fsid"] = fs.mId;
          jsonfsinfo["geotag"] = filesystem->GetString("stat.geotag");
          jsonfsinfo["host"] = fs.mHost;
          jsonfsinfo["mountpoint"] = fs.mPath;
          jsonfsinfo["fstpath"] = fstpath.c_str();
          jsonfsinfo["schedgroup"] = fs.mGroup;
          jsonfsinfo["status"] = eos::common::FileSystem::GetStatusAsString(fs.mStatus);

          if (!fs.mForceGeoTag.empty()) {
            jsonfsinfo["forcegeotag"] = fs.mForceGeoTag;
          }
        }
      } else {
        jsonfsinfo["fsid"] = *loc_it;
      }

      jsonfsids.append(jsonfsinfo);
    }

    json["locations"] = jsonfsids;
    json["checksumtype"] = eos::common::LayoutId::GetChecksumString(
                             fmd_copy->getLayoutId());
    std::string cks;
    eos::appendChecksumOnStringAsHex(fmd_copy.get(), cks);
    json["checksumvalue"] = cks;
    std::string etag;
    eos::calculateEtag(fmd_copy.get(), etag);
    json["etag"] = etag;
  } catch (eos::MDException& e) {
    errno = e.getErrno();
    eos_static_debug("msg=\"exception during JSON fileinfo\" ec=%d "
                     "emsg=\"%s\"\n", e.getErrno(),
                     e.getMessage().str().c_str());
    json["errc"] = errno;
    json["errmsg"] = e.getMessage().str().c_str();
  }

  if (!ret_json) {
    std::stringstream r;
    r << json;
    stdJson += r.str().c_str();
  } else {
    *ret_json = json;
  }

  retc = 0;
  return SFS_OK;
}

//------------------------------------------------------------------------------
// Get directory info in JSON format
//------------------------------------------------------------------------------
int
ProcCommand::DirJSON(uint64_t fid, Json::Value* ret_json, bool dolock)
{
  eos::IFileMD::ctime_t ctime;
  eos::IFileMD::ctime_t mtime;
  eos::IFileMD::ctime_t tmtime;
  eos::IFileMD::ctime_t btime {0, 0};
  eos_static_debug("msg=\"JSON dirinfo\" fxid=%08llx", fid);
  Json::Value json;
  json["id"] = (Json::Value::UInt64) fid;

  try {
    eos::common::RWMutexReadLock viewReadLock;
    std::shared_ptr<eos::IContainerMD> cmd;
    std::string path;
    bool detached;
    eos::Prefetcher::prefetchContainerMDWithParentsAndWait(gOFS->eosView, fid);

    if (dolock) {
      viewReadLock.Grab(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__, __FILE__);
    }

    try {
      cmd = gOFS->eosDirectoryService->getContainerMD(fid);
      path = gOFS->eosView->getUri(cmd.get());
    } catch (eos::MDException& e) {
      eos_static_debug("msg=\"exception retrieving container metadata\" "
                       "ec=%d emsg=\"%s\"\n", e.getErrno(),
                       e.getMessage().str().c_str());

      if (!cmd) {
        viewReadLock.Release();
        std::rethrow_exception(std::current_exception());
      }
    }

    eos::IFileMD::XAttrMap xattrs = cmd->getAttributes();
    cmd->getCTime(ctime);
    cmd->getMTime(mtime);
    cmd->getTMTime(tmtime);

    if (xattrs.count("sys.eos.btime")) {
      eos::common::Timing::Timespec_from_TimespecStr(xattrs["sys.eos.btime"],
          btime);
    }

    if ((detached = path.empty())) {
      path = SSTR("pid:" << fid);
    }

    json["inode"] = (Json::Value::UInt64) fid;
    json["ctime"] = (Json::Value::UInt64) ctime.tv_sec;
    json["ctime_ns"] = (Json::Value::UInt64) ctime.tv_nsec;
    json["atime"] = (Json::Value::UInt64) ctime.tv_sec;
    json["atime_ns"] = (Json::Value::UInt64) ctime.tv_nsec;
    json["mtime"] = (Json::Value::UInt64) mtime.tv_sec;
    json["mtime_ns"] = (Json::Value::UInt64) mtime.tv_nsec;
    json["tmtime"] = (Json::Value::UInt64) tmtime.tv_sec;
    json["tmtime_ns"] = (Json::Value::UInt64) tmtime.tv_nsec;
    json["btime"] = (Json::Value::UInt64) btime.tv_sec;
    json["btime_ns"] = (Json::Value::UInt64) btime.tv_nsec;
    json["treesize"] = (Json::Value::UInt64) cmd->getTreeSize();
    json["uid"] = cmd->getCUid();
    json["gid"] = cmd->getCGid();
    json["flags"] = cmd->getFlags();
    json["mode"] = cmd->getMode();
    json["nlink"] = 1;
    json["name"] = cmd->getName();
    json["path"] = path;
    json["detached"] = detached;
    json["pid"] = (Json::Value::UInt64) cmd->getParentId();
    json["nndirectories"] = (int)cmd->getNumContainers();
    json["nfiles"] = (int)cmd->getNumFiles();
    Json::Value chld;
    std::shared_ptr<eos::IContainerMD> cmd_copy(cmd->clone());
    cmd_copy->InheritChildren(*(cmd.get()));
    cmd.reset();
    viewReadLock.Release();

    if (!ret_json) {
      for (auto it = FileMapIterator(cmd_copy); it.valid(); it.next()) {
        Json::Value fjson;
        FileJSON(it.value(), &fjson, true);
        chld.append(fjson);
      }

      // Loop through all subcontainers
      for (auto dit = ContainerMapIterator(cmd_copy); dit.valid(); dit.next()) {
        Json::Value djson;
        DirJSON(dit.value(), &djson, true);
        chld.append(djson);
      }
    }

    if ((cmd_copy->getNumFiles() + cmd_copy->getNumContainers()) != 0) {
      json["children"] = chld;
    }

    Json::Value jsonxattr;

    for (const auto& elem : xattrs) {
      jsonxattr[elem.first] = elem.second;
    }

    if (cmd_copy->numAttributes()) {
      json["xattr"] = jsonxattr;
    }

    std::string etag;
    eos::calculateEtag(cmd_copy.get(), etag);
    json["etag"] = etag;
  } catch (eos::MDException& e) {
    errno = e.getErrno();
    eos_static_debug("msg=\"exception during JSON dirinfo\" ec=%d "
                     "emsg=\"%s\"\n", e.getErrno(),
                     e.getMessage().str().c_str());
    json["errc"] = errno;
    json["errmsg"] = e.getMessage().str().c_str();
  }

  if (!ret_json) {
    std::stringstream r;
    r << json;
    stdJson += r.str().c_str();
    retc = 0;
  } else {
    *ret_json = json;
  }

  return SFS_OK;
}

EOSMGMNAMESPACE_END
