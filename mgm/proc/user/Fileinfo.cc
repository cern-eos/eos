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
 * You should have received a copy of the AGNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include "mgm/proc/ProcInterface.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Access.hh"
#include "mgm/Macros.hh"
#include "mgm/Quota.hh"
#include "mgm/TableFormatter/TableCell.hh"
#include "common/LayoutId.hh"
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
  unsigned long long id = 0;

  if ((!spath.beginswith("inode:")) &&
      (!spath.beginswith("fid:")) &&
      (!spath.beginswith("fxid:")) &&
      (!spath.beginswith("pid:")) &&
      (!spath.beginswith("pxid:"))) {
    if (gOFS->_stat(path, &buf, *mError, *pVid, (char*) 0, (std::string*)0,
                    false)) {
      stdErr = "error: cannot stat ";
      stdErr += path;
      stdErr += "\n";
      retc = ENOENT;
      return SFS_OK;
    }

    if (S_ISDIR(buf.st_mode)) {
      id = buf.st_ino;
    } else {
      id = eos::common::FileId::InodeToFid(buf.st_ino);
    }
  } else {
    XrdOucString sid = spath;

    if ((sid.replace("inode:", ""))) {
      id = strtoull(sid.c_str(), 0, 10);

      if (id >=  eos::common::FileId::FidToInode(1)) {
        buf.st_mode = S_IFREG;
        spath = "fid:";
        id = eos::common::FileId::InodeToFid(id);
        spath += eos::common::StringConversion::GetSizeString(sid, id);
        path = spath.c_str();
      } else {
        buf.st_mode = S_IFDIR;
        spath.replace("inode:", "pid:");
        path = spath.c_str();
      }
    } else {
      if (spath.beginswith("f")) {
        buf.st_mode = S_IFREG;
      } else {
        buf.st_mode = S_IFDIR;
      }
    }
  }

  if (mJsonFormat) {
    if (S_ISDIR(buf.st_mode)) {
      return DirJSON(id, 0);
    } else {
      return FileJSON(id, 0);
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
  uint64_t clock = 0;
  {
    std::shared_ptr<eos::IFileMD> fmd;

    if ((spath.beginswith("fid:") || (spath.beginswith("fxid:")))) {
      unsigned long long fid = 0;

      if (spath.beginswith("fid:")) {
        spath.replace("fid:", "");
        fid = strtoull(spath.c_str(), 0, 10);
      }

      if (spath.beginswith("fxid:")) {
        spath.replace("fxid:", "");
        fid = strtoull(spath.c_str(), 0, 16);
      }

      // reference by fid+fxid
      //-------------------------------------------
      gOFS->eosViewRWMutex.LockRead();

      try {
        fmd = gOFS->eosFileService->getFileMD(fid, &clock);
        std::string fullpath = gOFS->eosView->getUri(fmd.get());
        spath = fullpath.c_str();
      } catch (eos::MDException& e) {
        errno = e.getErrno();
        stdErr = "error: cannot retrieve file meta data - ";
        stdErr += e.getMessage().str().c_str();
        eos_debug("caught exception %d %s\n", e.getErrno(),
                  e.getMessage().str().c_str());
      }
    } else {
      // reference by path
      //-------------------------------------------
      gOFS->eosViewRWMutex.LockRead();

      try {
        fmd = gOFS->eosView->getFile(spath.c_str());
      } catch (eos::MDException& e) {
        errno = e.getErrno();
        stdErr = "error: cannot retrieve file meta data - ";
        stdErr += e.getMessage().str().c_str();
        eos_debug("caught exception %d %s\n", e.getErrno(),
                  e.getMessage().str().c_str());
      }
    }

    if (!fmd) {
      retc = errno;
      gOFS->eosViewRWMutex.UnLockRead();
      //-------------------------------------------
    } else {
      // Make a copy of the file metadata object
      std::shared_ptr<eos::IFileMD> fmd_copy(fmd->clone());
      fmd.reset();
      // TODO (esindril): All this copying should be reviewed
      gOFS->eosViewRWMutex.UnLockRead();
      //-------------------------------------------
      XrdOucString sizestring;
      XrdOucString hexfidstring;
      XrdOucString hexpidstring;
      bool Monitoring = false;
      bool Envformat = false;
      eos::common::FileId::Fid2Hex(fmd_copy->getId(), hexfidstring);
      eos::common::FileId::Fid2Hex(fmd_copy->getContainerId(), hexpidstring);

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
        stdOut += env.c_str();
        eos::common::Path cPath(spath.c_str());
        stdOut += "&container=";
        stdOut += cPath.GetParentPath();
        stdOut += "\n";
      } else {
        if ((option.find("-path")) != STR_NPOS) {
          if (!Monitoring) {
            stdOut += "path:   ";
            stdOut += spath;
            stdOut += "\n";
          } else {
            stdOut += "path=";
            stdOut += spath;
            stdOut += " ";
          }
        }

        if ((option.find("-fxid")) != STR_NPOS) {
          if (!Monitoring) {
            stdOut += "fxid:   ";
            stdOut += hexfidstring;
            stdOut += "\n";
          } else {
            stdOut += "fxid=";
            stdOut += hexfidstring;
            stdOut += " ";
          }
        }

        if ((option.find("-fid")) != STR_NPOS) {
          char fid[32];
          snprintf(fid, 32, "%llu", (unsigned long long) fmd_copy->getId());

          if (!Monitoring) {
            stdOut += "fid:    ";
            stdOut += fid;
            stdOut += "\n";
          } else {
            stdOut += "fid=";
            stdOut += fid;
            stdOut += " ";
          }
        }

        if ((option.find("-size")) != STR_NPOS) {
          if (!Monitoring) {
            stdOut += "size:   ";
            stdOut += eos::common::StringConversion::GetSizeString(sizestring,
                      (unsigned long long) fmd_copy->getSize());
            stdOut += "\n";
          } else {
            stdOut += "size=";
            stdOut += eos::common::StringConversion::GetSizeString(sizestring,
                      (unsigned long long) fmd_copy->getSize());
            stdOut += " ";
          }
        }

        if ((option.find("-checksum")) != STR_NPOS) {
          if (!Monitoring) {
            stdOut += "xstype: ";
            stdOut += eos::common::LayoutId::GetChecksumString(fmd_copy->getLayoutId());
            stdOut += "\n";
            stdOut += "xs:     ";

            for (unsigned int i = 0;
                 i < eos::common::LayoutId::GetChecksumLen(fmd_copy->getLayoutId()); i++) {
              char hb[3];
              sprintf(hb, "%02x", (unsigned char)(fmd_copy->getChecksum().getDataPadded(i)));
              stdOut += hb;
            }

            stdOut += "\n";
          } else {
            stdOut += "xstype=";
            stdOut += eos::common::LayoutId::GetChecksumString(fmd_copy->getLayoutId());
            stdOut += " ";
            stdOut += "xs=";

            for (unsigned int i = 0;
                 i < eos::common::LayoutId::GetChecksumLen(fmd_copy->getLayoutId()); i++) {
              char hb[3];
              sprintf(hb, "%02x", (unsigned char)(fmd_copy->getChecksum().getDataPadded(i)));
              stdOut += hb;
            }

            stdOut += " ";
          }
        }

        if (Monitoring || (!(option.length())) || (option == "--fullpath") ||
            (option == "--proxy") || (option == "-m")) {
          char ctimestring[4096];
          char mtimestring[4096];
          eos::IFileMD::ctime_t mtime;
          eos::IFileMD::ctime_t ctime;
          fmd_copy->getCTime(ctime);
          fmd_copy->getMTime(mtime);
          time_t filectime = (time_t) ctime.tv_sec;
          time_t filemtime = (time_t) mtime.tv_sec;
          char fid[32];
          snprintf(fid, 32, "%llu", (unsigned long long) fmd_copy->getId());
          std::string etag;
          // if there is a checksum we use the checksum, otherwise we return inode+mtime
          size_t cxlen = eos::common::LayoutId::GetChecksumLen(fmd_copy->getLayoutId());

          if (cxlen) {
            // use inode + checksum
            char setag[256];
            snprintf(setag, sizeof(setag) - 1, "%llu:",
                     (unsigned long long)eos::common::FileId::FidToInode(fmd_copy->getId()));
            etag = setag;

            for (unsigned int i = 0; i < cxlen; i++) {
              char hb[3];
              sprintf(hb, "%02x", (i < cxlen) ? (unsigned char)(
                        fmd_copy->getChecksum().getDataPadded(i)) : 0);
              etag += hb;
            }
          } else {
            // use inode + mtime
            char setag[256];
            eos::IFileMD::ctime_t mtime;
            fmd_copy->getMTime(mtime);
            time_t filemtime = (time_t) mtime.tv_sec;
            snprintf(setag, sizeof(setag) - 1, "\"%llu:%llu\"",
                     (unsigned long long) eos::common::FileId::FidToInode(fmd_copy->getId()),
                     (unsigned long long) filemtime);
            etag = setag;
          }

          if (!Monitoring) {
            stdOut = "  File: '";
            stdOut += spath;
            stdOut += "'";
            stdOut += "  Flags: ";
            stdOut +=  eos::common::StringConversion::IntToOctal((int) fmd_copy->getFlags(),
                       4).c_str();

            if (clock) {
              XrdOucString hexclock;
              eos::common::FileId::Fid2Hex(clock, hexclock);
              stdOut += "  Clock: ";
              stdOut += hexclock;
            }

            stdOut += "\n";
            stdOut += "  Size: ";
            stdOut += eos::common::StringConversion::GetSizeString(sizestring,
                      (unsigned long long) fmd_copy->getSize());
            stdOut += "\n";
            stdOut += "Modify: ";
            stdOut += ctime_r(&filemtime, mtimestring);
            stdOut.erase(stdOut.length() - 1);
            stdOut += " Timestamp: ";
            stdOut += eos::common::StringConversion::GetSizeString(sizestring,
                      (unsigned long long) mtime.tv_sec);
            stdOut += ".";
            stdOut += eos::common::StringConversion::GetSizeString(sizestring,
                      (unsigned long long) mtime.tv_nsec);
            stdOut += "\n";
            stdOut += "Change: ";
            stdOut += ctime_r(&filectime, ctimestring);
            stdOut.erase(stdOut.length() - 1);
            stdOut += " Timestamp: ";
            stdOut += eos::common::StringConversion::GetSizeString(sizestring,
                      (unsigned long long) ctime.tv_sec);
            stdOut += ".";
            stdOut += eos::common::StringConversion::GetSizeString(sizestring,
                      (unsigned long long) ctime.tv_nsec);
            stdOut += "\n";
            stdOut += "  CUid: ";
            stdOut += (int) fmd_copy->getCUid();
            stdOut += " CGid: ";
            stdOut += (int) fmd_copy->getCGid();
            stdOut += "  Fxid: ";
            stdOut += hexfidstring;
            stdOut += " ";
            stdOut += "Fid: ";
            stdOut += fid;
            stdOut += " ";
            stdOut += "   Pid: ";
            stdOut += eos::common::StringConversion::GetSizeString(sizestring,
                      (unsigned long long) fmd_copy->getContainerId());
            stdOut += "   Pxid: ";
            stdOut += hexpidstring;
            stdOut += "\n";
            stdOut += "XStype: ";
            stdOut += eos::common::LayoutId::GetChecksumString(fmd_copy->getLayoutId());
            stdOut += "    XS: ";
            size_t cxlen = eos::common::LayoutId::GetChecksumLen(fmd_copy->getLayoutId());

            for (unsigned int i = 0; i < cxlen; i++) {
              char hb[4];
              sprintf(hb, "%02x ", (unsigned char)(fmd_copy->getChecksum().getDataPadded(i)));
              stdOut += hb;
            }

            stdOut += "    ETAG: ";
            stdOut += etag.c_str();
            stdOut += "\n";
            stdOut + "Layout: ";
            stdOut += eos::common::LayoutId::GetLayoutTypeString(fmd_copy->getLayoutId());
            stdOut += " Stripes: ";
            stdOut += (int)(eos::common::LayoutId::GetStripeNumber(fmd_copy->getLayoutId())
                            + 1);
            stdOut += " Blocksize: ";
            stdOut += eos::common::LayoutId::GetBlockSizeString(fmd_copy->getLayoutId());
            stdOut += " LayoutId: ";
            XrdOucString hexlidstring;
            eos::common::FileId::Fid2Hex(fmd_copy->getLayoutId(), hexlidstring);
            stdOut += hexlidstring;
            stdOut += "\n";
            stdOut += "  #Rep: ";
            stdOut += (int) fmd_copy->getNumLocation();
            stdOut += "\n";
          } else {
            stdOut = "keylength.file=";
            stdOut += spath.length();
            stdOut += " ";
            stdOut += "file=";
            stdOut += spath;
            stdOut += " ";
            stdOut += "size=";
            stdOut += eos::common::StringConversion::GetSizeString(sizestring,
                      (unsigned long long) fmd_copy->getSize());
            stdOut += " ";
            stdOut += "mtime=";
            stdOut += eos::common::StringConversion::GetSizeString(sizestring,
                      (unsigned long long) mtime.tv_sec);
            stdOut += ".";
            stdOut += eos::common::StringConversion::GetSizeString(sizestring,
                      (unsigned long long) mtime.tv_nsec);
            stdOut += " ";
            stdOut += "ctime=";
            stdOut += eos::common::StringConversion::GetSizeString(sizestring,
                      (unsigned long long) ctime.tv_sec);
            stdOut += ".";
            stdOut += eos::common::StringConversion::GetSizeString(sizestring,
                      (unsigned long long) ctime.tv_nsec);
            stdOut += " ";
            stdOut += "clock=";
            stdOut += eos::common::StringConversion::GetSizeString(sizestring,
                      (unsigned long long) clock);
            stdOut += " ";
            stdOut += "mode=";
            stdOut +=  eos::common::StringConversion::IntToOctal((int) fmd_copy->getFlags(),
                       4).c_str();
            stdOut += " ";
            stdOut += "uid=";
            stdOut += (int) fmd_copy->getCUid();
            stdOut += " gid=";
            stdOut += (int) fmd_copy->getCGid();
            stdOut += " ";
            stdOut += "fxid=";
            stdOut += hexfidstring;
            stdOut += " ";
            stdOut += "fid=";
            stdOut += fid;
            stdOut += " ";
            stdOut += "ino=";
            stdOut += eos::common::StringConversion::GetSizeString(sizestring,
                      (unsigned long long) eos::common::FileId::FidToInode(fmd_copy->getId()));
            stdOut += " ";
            stdOut += "pid=";
            stdOut += eos::common::StringConversion::GetSizeString(sizestring,
                      (unsigned long long) fmd_copy->getContainerId());
            stdOut += " ";
            stdOut += "pxid=";
            stdOut += hexpidstring;
            stdOut += " ";
            stdOut += "xstype=";
            stdOut += eos::common::LayoutId::GetChecksumString(fmd_copy->getLayoutId());
            stdOut += " ";
            stdOut += "xs=";
            size_t cxlen = eos::common::LayoutId::GetChecksumLen(fmd_copy->getLayoutId());

            if (cxlen) {
              for (unsigned int i = 0; i < cxlen; i++) {
                char hb[3];
                sprintf(hb, "%02x", (unsigned char)(fmd_copy->getChecksum().getDataPadded(i)));
                stdOut += hb;
              }
            } else {
              stdOut += "0";
            }

            stdOut += " ";
            stdOut += "etag=";
            stdOut += etag.c_str();
            stdOut += " ";
            stdOut += "layout=";
            stdOut += eos::common::LayoutId::GetLayoutTypeString(fmd_copy->getLayoutId());
            stdOut += " nstripes=";
            stdOut += (int)(eos::common::LayoutId::GetStripeNumber(fmd_copy->getLayoutId())
                            + 1);
            stdOut += " ";
            stdOut += "lid=";
            XrdOucString hexlidstring;
            eos::common::FileId::Fid2Hex(fmd_copy->getLayoutId(), hexlidstring);
            stdOut += hexlidstring;
            stdOut += " ";
            stdOut += "nrep=";
            stdOut += (int) fmd_copy->getNumLocation();
            stdOut += " ";
          }

          eos::IFileMD::LocationVector::const_iterator lociter;
          eos::IFileMD::LocationVector loc_vect = fmd_copy->getLocations();
          std::vector<unsigned int> selectedfs;
          std::vector<std::string> proxys;
          std::vector<std::string> firewalleps;
          std::vector<unsigned int> unavailfs;
          std::vector<unsigned int> replacedfs;
          std::vector<unsigned int>::const_iterator sfs;
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
              eos_err("fsid 0 found fid=%lld", fmd_copy->getId());
              continue;
            }

            char fsline[4096];
            XrdOucString location = "";
            location += (int) * lociter;
            XrdOucString si = "";
            si += (int) i;
            eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
            eos::common::FileSystem* filesystem = 0;

            if (FsView::gFsView.mIdView.count(*lociter)) {
              filesystem = FsView::gFsView.mIdView[*lociter];
            }

            if (filesystem) {
              // For the fullpath option we output the physical location of the
              // replicas
              XrdOucString fullpath;

              if ((option.find("-fullpath")) != STR_NPOS) {
                eos::common::FileId::FidPrefix2FullPath(
                  hexfidstring.c_str(), filesystem->GetPath().c_str(), fullpath);
              }

              if (!Monitoring) {
                std::string format =
                  "header=1|key=host:width=24:format=s|key=schedgroup:width=16:format=s|key=path:width=16:format=s|key=stat.boot:width=10:format=s|key=configstatus:width=14:format=s|key=stat.drain:width=12:format=s|key=stat.active:width=8:format=s|key=stat.geotag:width=24:format=s";

                if ((option.find("-proxy")) != STR_NPOS) {
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

                  if ((option.find("-fullpath")) != STR_NPOS) {
                    table_mq_header_temp.push_back(std::make_tuple("physical location", 18, "s"));
                  }

                  table_mq.SetHeader(table_mq_header_temp);
                  table_mq_header_exist = true;
                }

                //Build body
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

                      if ((option.find("-fullpath")) != STR_NPOS) {
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
                      eos_static_err("open - invalid access argument");
                    }

                    schedretc = Quota::FileAccess(&acsargs);

                    if (schedretc) {
                      eos_static_warning("cannot schedule the proxy");
                    }
                  }

                  if (schedretc) {
                    stdOut += "     sticky to undefined";
                  } else {
                    stdOut += "sticky to ";
                    size_t k;

                    for (k = 0; k < loc_vect.size() && selectedfs[k] != loc_vect[i]; k++);

                    stdOut += proxys[k].c_str();
                  }
                }
              } else {
                stdOut += "fsid=";
                stdOut += location.c_str();
                stdOut += " ";

                if ((option.find("-fullpath")) != STR_NPOS) {
                  stdOut += "fullpath=";
                  stdOut += fullpath;
                  stdOut += " ";
                }
              }
            } else {
              if (!Monitoring) {
                sprintf(fsline, "%3s   %5s ", si.c_str(), location.c_str());
                stdOut += fsline;
                stdOut += "NA\n";
              }
            }

            i++;
          }

          stdOut += table_mq.GenerateTable(HEADER).c_str();
          eos::IFileMD::LocationVector unlink_vect = fmd_copy->getUnlinkedLocations();

          for (lociter = unlink_vect.begin(); lociter != unlink_vect.end(); ++lociter) {
            if (!Monitoring) {
              stdOut += "(undeleted) $ ";
              stdOut += (int) * lociter;
              stdOut += "\n";
            } else {
              stdOut += "fsdel=";
              stdOut += (int) * lociter;
              stdOut += " ";
            }
          }

          if (!Monitoring) {
            stdOut += "*******";
          }
        }
      }
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
  uint64_t clock = 0;
  {
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

      // reference by fid+fsid
      //-------------------------------------------
      gOFS->eosViewRWMutex.LockRead();

      try {
        dmd = gOFS->eosDirectoryService->getContainerMD(fid, &clock);
        std::string fullpath = gOFS->eosView->getUri(dmd.get());
        spath = fullpath.c_str();
      } catch (eos::MDException& e) {
        errno = e.getErrno();
        stdErr = "error: cannot retrieve directory meta data - ";
        stdErr += e.getMessage().str().c_str();
        eos_debug("caught exception %d %s\n", e.getErrno(),
                  e.getMessage().str().c_str());
      }
    } else {
      // reference by path
      //-------------------------------------------
      gOFS->eosViewRWMutex.LockRead();

      try {
        dmd = gOFS->eosView->getContainer(spath.c_str());
      } catch (eos::MDException& e) {
        errno = e.getErrno();
        stdErr = "error: cannot retrieve directory meta data - ";
        stdErr += e.getMessage().str().c_str();
        eos_debug("caught exception %d %s\n", e.getErrno(),
                  e.getMessage().str().c_str());
      }
    }

    if (!dmd) {
      retc = errno;
      gOFS->eosViewRWMutex.UnLockRead();
      //-------------------------------------------
    } else {
      size_t num_containers = dmd->getNumContainers();
      size_t num_files = dmd->getNumFiles();
      std::shared_ptr<eos::IContainerMD> dmd_copy(dmd->clone());
      dmd_copy->InheritChildren(*(dmd.get()));
      dmd.reset();
      gOFS->eosViewRWMutex.UnLockRead();
      //-------------------------------------------
      XrdOucString sizestring;
      XrdOucString hexfidstring;
      XrdOucString hexpidstring;
      bool Monitoring = false;
      eos::common::FileId::Fid2Hex(dmd_copy->getId(), hexfidstring);
      eos::common::FileId::Fid2Hex(dmd_copy->getParentId(), hexpidstring);

      if ((option.find("-m")) != STR_NPOS) {
        Monitoring = true;
      }

      if ((option.find("-path")) != STR_NPOS) {
        if (!Monitoring) {
          stdOut += "path:   ";
          stdOut += spath;
          stdOut += "\n";
        } else {
          stdOut += "path=";
          stdOut += spath;
          stdOut += " ";
        }
      }

      if ((option.find("-fxid")) != STR_NPOS) {
        if (!Monitoring) {
          stdOut += "fxid:   ";
          stdOut += hexfidstring;
          stdOut += "\n";
        } else {
          stdOut += "fxid=";
          stdOut += hexfidstring;
          stdOut += " ";
        }
      }

      if ((option.find("-fid")) != STR_NPOS) {
        char fid[32];
        snprintf(fid, 32, "%llu", (unsigned long long) dmd_copy->getId());

        if (!Monitoring) {
          stdOut += "fid:    ";
          stdOut += fid;
          stdOut += "\n";
        } else {
          stdOut += "fid=";
          stdOut += fid;
          stdOut += " ";
        }
      }

      if ((option.find("-size")) != STR_NPOS) {
        if (!Monitoring) {
          stdOut += "size:   ";
          stdOut += eos::common::StringConversion::GetSizeString(sizestring,
                    (unsigned long long)(num_containers + num_files));
          stdOut += "\n";
        } else {
          stdOut += "size=";
          stdOut += eos::common::StringConversion::GetSizeString(sizestring,
                    (unsigned long long)(num_containers + num_files));
          stdOut += " ";
        }
      }

      if (Monitoring || (!(option.length())) || (option == "--fullpath") ||
          (option == "-m")) {
        char ctimestring[4096];
        char mtimestring[4096];
        char tmtimestring[4096];
        eos::IContainerMD::ctime_t ctime;
        eos::IContainerMD::mtime_t mtime;
        eos::IContainerMD::tmtime_t tmtime;
        dmd_copy->getCTime(ctime);
        dmd_copy->getMTime(mtime);
        dmd_copy->getTMTime(tmtime);
        //fprintf(stderr,"%lli.%lli %lli.%lli %lli.%lli\n",
        //fprintf(stderr,"%llu.%llu %llu.%llu %llu.%llu\n", (unsigned long long)ctime.tv_sec,
        //        (unsigned long long)ctime.tv_nsec, (unsigned long long)mtime.tv_sec,
        //        (unsigned long long)mtime.tv_nsec, (unsigned long long)tmtime.tv_sec,
        //        (unsigned long long)tmtime.tv_sec);
        time_t filectime = (time_t) ctime.tv_sec;
        time_t filemtime = (time_t) mtime.tv_sec;
        time_t filetmtime = (time_t) tmtime.tv_sec;
        char fid[32];
        snprintf(fid, 32, "%llu", (unsigned long long) dmd_copy->getId());
        std::string etag;
        char setag[256];
        snprintf(setag, sizeof(setag) - 1, "%llx:%llu.%03lu",
                 (unsigned long long)dmd_copy->getId(), (unsigned long long)tmtime.tv_sec,
                 (unsigned long)tmtime.tv_nsec / 1000000);
        etag = setag;

        if (!Monitoring) {
          stdOut = "  Directory: '";
          stdOut += spath;
          stdOut += "'";
          stdOut += "  Treesize: ";
          stdOut += eos::common::StringConversion::GetSizeString(sizestring,
                    (unsigned long long) dmd_copy->getTreeSize());
          stdOut += "\n";
          stdOut += "  Container: ";
          stdOut += eos::common::StringConversion::GetSizeString(sizestring,
                    (unsigned long long)num_containers);
          stdOut += "  Files: ";
          stdOut += eos::common::StringConversion::GetSizeString(sizestring,
                    (unsigned long long)num_files);
          stdOut += "  Flags: ";
          stdOut +=  eos::common::StringConversion::IntToOctal((int) dmd_copy->getMode(),
                     4).c_str();

          if (clock) {
            XrdOucString hexclock;
            eos::common::FileId::Fid2Hex(clock, hexclock);
            stdOut += "  Clock: ";
            stdOut += hexclock;
          }

          stdOut += "\n";
          stdOut += "Modify: ";
          stdOut += ctime_r(&filemtime, mtimestring);
          stdOut.erase(stdOut.length() - 1);
          stdOut += " Timestamp: ";
          stdOut += eos::common::StringConversion::GetSizeString(sizestring,
                    (unsigned long long) mtime.tv_sec);
          stdOut += ".";
          stdOut += eos::common::StringConversion::GetSizeString(sizestring,
                    (unsigned long long) mtime.tv_nsec);
          stdOut += "\n";
          stdOut += "Change: ";
          stdOut += ctime_r(&filectime, ctimestring);
          stdOut.erase(stdOut.length() - 1);
          stdOut += " Timestamp: ";
          stdOut += eos::common::StringConversion::GetSizeString(sizestring,
                    (unsigned long long) ctime.tv_sec);
          stdOut += ".";
          stdOut += eos::common::StringConversion::GetSizeString(sizestring,
                    (unsigned long long) ctime.tv_nsec);
          stdOut += "\n";
          stdOut += "Sync:   ";
          stdOut += ctime_r(&filetmtime, tmtimestring);
          stdOut.erase(stdOut.length() - 1);
          stdOut += " Timestamp: ";
          stdOut += eos::common::StringConversion::GetSizeString(sizestring,
                    (unsigned long long) tmtime.tv_sec);
          stdOut += ".";
          stdOut += eos::common::StringConversion::GetSizeString(sizestring,
                    (unsigned long long) tmtime.tv_nsec);
          stdOut += "\n";
          stdOut += "  CUid: ";
          stdOut += (int) dmd_copy->getCUid();
          stdOut += " CGid: ";
          stdOut += (int) dmd_copy->getCGid();
          stdOut += "  Fxid: ";
          stdOut += hexfidstring;
          stdOut += " ";
          stdOut += "Fid: ";
          stdOut += fid;
          stdOut += " ";
          stdOut += "   Pid: ";
          stdOut += eos::common::StringConversion::GetSizeString(sizestring,
                    (unsigned long long) dmd_copy->getParentId());
          stdOut += "   Pxid: ";
          stdOut += hexpidstring;
          stdOut += "\n";
          stdOut += "  ETAG: ";
          stdOut += etag.c_str();
          stdOut += "\n";
        } else {
          stdOut = "keylength.file=";
          stdOut += spath.length();
          stdOut += " ";
          stdOut += "file=";
          stdOut += spath;
          stdOut += " ";
          stdOut += "treesize=";
          stdOut += eos::common::StringConversion::GetSizeString(sizestring,
                    (unsigned long long) dmd_copy->getTreeSize());
          stdOut += " ";
          stdOut += "container=";
          stdOut += eos::common::StringConversion::GetSizeString(sizestring,
                    (unsigned long long)num_containers);
          stdOut += " ";
          stdOut += "files=";
          stdOut += eos::common::StringConversion::GetSizeString(sizestring,
                    (unsigned long long)num_files);
          stdOut += " ";
          stdOut += "mtime=";
          stdOut += eos::common::StringConversion::GetSizeString(sizestring,
                    (unsigned long long) mtime.tv_sec);
          stdOut += ".";
          stdOut += eos::common::StringConversion::GetSizeString(sizestring,
                    (unsigned long long) mtime.tv_nsec);
          stdOut += " ";
          stdOut += "ctime=";
          stdOut += eos::common::StringConversion::GetSizeString(sizestring,
                    (unsigned long long) ctime.tv_sec);
          stdOut += ".";
          stdOut += eos::common::StringConversion::GetSizeString(sizestring,
                    (unsigned long long) ctime.tv_nsec);
          stdOut += " ";
          stdOut += "clock=";
          stdOut += eos::common::StringConversion::GetSizeString(sizestring,
                    (unsigned long long) clock);
          stdOut += " ";
          stdOut += "mode=";
          stdOut +=  eos::common::StringConversion::IntToOctal((int) dmd_copy->getMode(),
                     4).c_str();
          stdOut += " ";
          stdOut += "uid=";
          stdOut += (int) dmd_copy->getCUid();
          stdOut += " gid=";
          stdOut += (int) dmd_copy->getCGid();
          stdOut += " ";
          stdOut += "fxid=";
          stdOut += hexfidstring;
          stdOut += " ";
          stdOut += "fid=";
          stdOut += fid;
          stdOut += " ";
          stdOut += "ino=";
          stdOut += fid;
          stdOut += " ";
          stdOut += "pid=";
          stdOut += eos::common::StringConversion::GetSizeString(sizestring,
                    (unsigned long long) dmd_copy->getParentId());
          stdOut += " ";
          stdOut += "pxid=";
          stdOut += hexpidstring;
          stdOut += " ";
          stdOut += "etag=";
          stdOut += etag.c_str();
          stdOut += " ";
          eos::IFileMD::XAttrMap xattrs = dmd_copy->getAttributes();

          for (const auto& elem : xattrs) {
            stdOut += "xattrn=";
            stdOut += elem.first.c_str();
            stdOut += " xattrv=";
            stdOut += elem.second.c_str();
            stdOut += " ";
          }
        }
      }
    }
  }
  return SFS_OK;
}

//------------------------------------------------------------------------------
// File info in JSON format
//------------------------------------------------------------------------------
int
ProcCommand::FileJSON(uint64_t fid, Json::Value* ret_json)
{
  std::string fullpath;
  eos::IFileMD::ctime_t ctime;
  eos::IFileMD::ctime_t mtime;
  eos_static_debug("fid=%llu", fid);
  Json::Value json;
  json["id"] = (Json::Value::UInt64)fid;

  try {
    gOFS->eosViewRWMutex.LockRead();
    std::shared_ptr<eos::IFileMD> fmd = gOFS->eosFileService->getFileMD(fid);
    fullpath = gOFS->eosView->getUri(fmd.get());
    std::shared_ptr<eos::IFileMD> fmd_copy(fmd->clone());
    fmd.reset();
    gOFS->eosViewRWMutex.UnLockRead();
    // TODO (esindril): All this copying should be reviewed
    //--------------------------------------------------------------------------
    fmd_copy->getCTime(ctime);
    fmd_copy->getMTime(mtime);
    json["inode"] = (Json::Value::UInt64) eos::common::FileId::FidToInode(fid);
    json["ctime"] = (Json::Value::UInt64) ctime.tv_sec;
    json["ctime_ns"] = (Json::Value::UInt64) ctime.tv_nsec;
    json["atime"] = (Json::Value::UInt64) ctime.tv_sec;
    json["atime_ns"] = (Json::Value::UInt64) ctime.tv_nsec;
    json["mtime"] = (Json::Value::UInt64) mtime.tv_sec;
    json["mtime_ns"] = (Json::Value::UInt64) mtime.tv_nsec;
    json["size"] = (Json::Value::UInt64) fmd_copy->getSize();
    json["uid"] = fmd_copy->getCUid();
    json["gid"] = fmd_copy->getCGid();
    json["mode"] = fmd_copy->getFlags();
    json["nlink"] = 1;
    json["name"] = fmd_copy->getName();

    if (fmd_copy->isLink()) {
      json["target"] = fmd_copy->getLink();
    }

    Json::Value jsonxattr;
    eos::IFileMD::XAttrMap xattrs = fmd_copy->getAttributes();

    for (const auto& elem : xattrs) {
      jsonxattr[elem.first] = elem.second;
    }

    if (fmd_copy->numAttributes()) {
      json["xattr"] = jsonxattr;
    }

    Json::Value jsonfsids;
    std::set<std::string> fsHosts;
    eos::IFileMD::LocationVector loc_vect = fmd_copy->getLocations();

    // Get host name for the fs ids
    for (auto loc_it = loc_vect.begin(); loc_it != loc_vect.end(); ++loc_it) {
      eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
      eos::common::FileSystem* filesystem = 0;
      Json::Value jsonfsinfo;

      if (FsView::gFsView.mIdView.count(*loc_it)) {
        filesystem = FsView::gFsView.mIdView[*loc_it];
      }

      if (filesystem) {
        eos::common::FileSystem::fs_snapshot_t fs;

        if (filesystem->SnapShotFileSystem(fs, true)) {
          jsonfsinfo["host"] = fs.mHost;
          jsonfsinfo["fsid"] = fs.mId;
          jsonfsinfo["mountpoint"] = fs.mPath;
          jsonfsinfo["geotag"] = fs.mGeoTag;
          jsonfsinfo["status"] = eos::common::FileSystem::GetStatusAsString(fs.mStatus);
          fsHosts.insert(fs.mHost);
          jsonfsids.append(jsonfsinfo);
        }
      }
    }

    json["locations"] = jsonfsids;
    json["checksumtype"] = eos::common::LayoutId::GetChecksumString(
                             fmd_copy->getLayoutId());
    std::string cks;

    for (unsigned int i = 0;
         i < eos::common::LayoutId::GetChecksumLen(fmd_copy->getLayoutId()); i++) {
      char hb[3];
      sprintf(hb, "%02x", (unsigned char)(fmd_copy->getChecksum().getDataPadded(i)));
      cks += hb;
    }

    json["checksumvalue"] = cks;
    std::string etag;
    size_t cxlen = 0;

    if ((cxlen = eos::common::LayoutId::GetChecksumLen(fmd_copy->getLayoutId()))) {
      // use inode + checksum
      char setag[256];
      snprintf(setag, sizeof(setag) - 1, "%llu:",
               (unsigned long long)eos::common::FileId::FidToInode(fmd_copy->getId()));
      etag = setag;

      for (unsigned int i = 0; i < cxlen; i++) {
        char hb[3];
        sprintf(hb, "%02x", (i < cxlen) ? (unsigned char)
                (fmd_copy->getChecksum().getDataPadded(i)) : 0);
        etag += hb;
      }
    } else {
      // use inode + mtime
      char setag[256];
      eos::IFileMD::ctime_t mtime;
      fmd_copy->getMTime(mtime);
      time_t filemtime = (time_t) mtime.tv_sec;
      snprintf(setag, sizeof(setag) - 1, "%llu:%llu",
               (unsigned long long) eos::common::FileId::FidToInode(fmd_copy->getId()),
               (unsigned long long) filemtime);
      etag = setag;
    }

    json["etag"] = etag;
    json["path"] = fullpath;
  } catch (eos::MDException& e) {
    gOFS->eosViewRWMutex.UnLockRead();
    errno = e.getErrno();
    eos_static_debug("caught exception %d %s\n", e.getErrno(),
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
ProcCommand::DirJSON(uint64_t fid, Json::Value* ret_json)
{
  std::string fullpath;
  eos::IFileMD::ctime_t ctime;
  eos::IFileMD::ctime_t mtime;
  eos::IFileMD::ctime_t tmtime;
  eos_static_debug("fid=%llu", fid);
  Json::Value json;
  json["id"] = (Json::Value::UInt64)fid;

  try {
    gOFS->eosViewRWMutex.LockRead();
    std::shared_ptr<eos::IContainerMD> cmd =
      gOFS->eosDirectoryService->getContainerMD(fid);
    fullpath = gOFS->eosView->getUri(cmd.get());
    cmd->getCTime(ctime);
    cmd->getMTime(mtime);
    cmd->getTMTime(tmtime);
    json["inode"] = (Json::Value::UInt64) fid;
    json["ctime"] = (Json::Value::UInt64) ctime.tv_sec;
    json["ctime_ns"] = (Json::Value::UInt64) ctime.tv_nsec;
    json["atime"] = (Json::Value::UInt64) ctime.tv_sec;
    json["atime_ns"] = (Json::Value::UInt64) ctime.tv_nsec;
    json["mtime"] = (Json::Value::UInt64) mtime.tv_sec;
    json["mtime_ns"] = (Json::Value::UInt64) mtime.tv_nsec;
    json["tmtime"] = (Json::Value::UInt64) tmtime.tv_sec;
    json["tmtime_ns"] = (Json::Value::UInt64) tmtime.tv_nsec;
    json["treesize"] = (Json::Value::UInt64) cmd->getTreeSize();
    json["uid"] = cmd->getCUid();
    json["gid"] = cmd->getCGid();
    json["mode"] = cmd->getFlags();
    json["nlink"] = 1;
    json["name"] = cmd->getName();
    json["nndirectories"] = (int)cmd->getNumContainers();
    json["nfiles"] = (int)cmd->getNumFiles();
    Json::Value chld;

    if (!ret_json) {
      auto fit_begin = cmd->filesBegin();
      auto fit_end = cmd->filesEnd();

      for (auto it = fit_begin; it != fit_end; ++it) {
        std::shared_ptr<IFileMD> fmd = cmd->findFile(it->first);
        Json::Value fjson;
        FileJSON(fmd->getId(), &fjson);
        chld.append(fjson);
      }

      // Loop through all subcontainers
      auto cit_begin = cmd->subcontainersBegin();
      auto cit_end = cmd->subcontainersEnd();

      for (auto dit = cit_begin; dit != cit_end; ++dit) {
        Json::Value djson;
        std::shared_ptr<IContainerMD> dmd = cmd->findContainer(dit->first);
        DirJSON(dmd->getId(), &djson);
        chld.append(djson);
      }
    }

    if ((cmd->getNumFiles() + cmd->getNumContainers()) != 0) {
      json["children"] = chld;
    }

    Json::Value jsonxattr;
    eos::IFileMD::XAttrMap xattrs = cmd->getAttributes();

    for (const auto& elem : xattrs) {
      jsonxattr[elem.first] = elem.second;
    }

    if (cmd->numAttributes()) {
      json["xattr"] = jsonxattr;
    }

    std::string etag;
    // use inode + mtime
    char setag[256];
    eos::IFileMD::ctime_t mtime;
    cmd->getMTime(mtime);
    time_t filemtime = (time_t) mtime.tv_sec;
    snprintf(setag, sizeof(setag) - 1, "%llu:%llu",
             (unsigned long long) eos::common::FileId::FidToInode(cmd->getId()),
             (unsigned long long) filemtime);
    etag = setag;
    json["etag"] = etag;
    json["path"] = fullpath;
    gOFS->eosViewRWMutex.UnLockRead();
  } catch (eos::MDException& e) {
    gOFS->eosViewRWMutex.UnLockRead();
    errno = e.getErrno();
    eos_static_debug("caught exception %d %s\n", e.getErrno(),
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
