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

/*----------------------------------------------------------------------------*/
#include "mgm/ProcInterface.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Access.hh"
#include "mgm/Macros.hh"
#include "common/LayoutId.hh"

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN


int
ProcCommand::Fileinfo ()
{
  gOFS->MgmStats.Add("FileInfo", pVid->uid, pVid->gid, 1);
  XrdOucString spath = pOpaque->Get("mgm.path");

  const char* inpath = spath.c_str();

  NAMESPACEMAP;
  info = 0;
  if (info)info = 0; // for compiler happyness
  PROC_BOUNCE_ILLEGAL_NAMES;
  PROC_BOUNCE_NOT_ALLOWED;

  // stat the path

  struct stat buf;

  if (
      (!spath.beginswith("inode:")) &&
      (!spath.beginswith("fid:")) &&
      (!spath.beginswith("fxid:")) &&
      (!spath.beginswith("pid:")) &&
      (!spath.beginswith("pxid:")) )
  {
    if (gOFS->_stat(path,&buf,*mError, *pVid, (char*) 0))
    {
      stdErr = "error: cannot stat ";
      stdErr += path;
      stdErr += "\n";
      retc = ENOENT;
      return SFS_OK;
    }
  }
  else
  {
    unsigned long long id=0;

    XrdOucString sid=spath;
    if ( (sid.replace("inode:","")) )
    {
      id = strtoull(sid.c_str(), 0, 10);

      if (id >=  eos::common::FileId::FidToInode(1))
      {
        buf.st_mode = S_IFREG;
        spath.replace("inode:","fid:");
        path = spath.c_str();
      }
      else
      {
        buf.st_mode = S_IFDIR;
        spath.replace("inode:","pid:");
        path = spath.c_str();
      }
    }
    else
    {
      if (spath.beginswith("f"))
        buf.st_mode = S_IFREG;
      else
        buf.st_mode = S_IFDIR;
    }
  }

  if (S_ISDIR(buf.st_mode))
    return DirInfo(path);
  else
    return FileInfo(path);
}

int
ProcCommand::FileInfo (const char* path)
{
  XrdOucString option = pOpaque->Get("mgm.file.info.option");
  XrdOucString spath = path;
  {
    eos::FileMD* fmd = 0;

    if ((spath.beginswith("fid:") || (spath.beginswith("fxid:"))))
    {
      unsigned long long fid = 0;
      if (spath.beginswith("fid:"))
      {
        spath.replace("fid:", "");
        fid = strtoull(spath.c_str(), 0, 10);
      }
      if (spath.beginswith("fxid:"))
      {
        spath.replace("fxid:", "");
        fid = strtoull(spath.c_str(), 0, 16);
      }

      if (fid >=  eos::common::FileId::FidToInode(1))
        fid = eos::common::FileId::InodeToFid(fid);

      // reference by fid+fsid
      //-------------------------------------------
      gOFS->eosViewRWMutex.LockRead();
      try
      {
        fmd = gOFS->eosFileService->getFileMD(fid);
        std::string fullpath = gOFS->eosView->getUri(fmd);
        spath = fullpath.c_str();
      }
      catch (eos::MDException &e)
      {
        errno = e.getErrno();
        stdErr = "error: cannot retrieve file meta data - ";
        stdErr += e.getMessage().str().c_str();
        eos_debug("caught exception %d %s\n", e.getErrno(), e.getMessage().str().c_str());
      }
    }
    else
    {
      // reference by path
      //-------------------------------------------
      gOFS->eosViewRWMutex.LockRead();
      try
      {
        fmd = gOFS->eosView->getFile(spath.c_str());
      }
      catch (eos::MDException &e)
      {
        errno = e.getErrno();
        stdErr = "error: cannot retrieve file meta data - ";
        stdErr += e.getMessage().str().c_str();
        eos_debug("caught exception %d %s\n", e.getErrno(), e.getMessage().str().c_str());
      }
    }



    if (!fmd)
    {
      retc = errno;
      gOFS->eosViewRWMutex.UnLockRead();
      //-------------------------------------------

    }
    else
    {
      // make a copy of the meta data
      eos::FileMD fmdCopy(*fmd);
      fmd = &fmdCopy;
      gOFS->eosViewRWMutex.UnLockRead();
      //-------------------------------------------

      XrdOucString sizestring;
      XrdOucString hexfidstring;
      XrdOucString hexpidstring;
      bool Monitoring = false;
      bool Envformat = false;

      eos::common::FileId::Fid2Hex(fmd->getId(), hexfidstring);
      eos::common::FileId::Fid2Hex(fmd->getContainerId(), hexpidstring);

      if ((option.find("-m")) != STR_NPOS)
      {
        Monitoring = true;
      }

      if ((option.find("-env")) != STR_NPOS)
      {
        Envformat = true;
        Monitoring = false;
      }

      if (Envformat)
      {
        std::string env;
        fmd->getEnv(env);
        stdOut += env.c_str();
        eos::common::Path cPath(spath.c_str());
        stdOut += "&container=";
        stdOut += cPath.GetParentPath();
        stdOut += "\n";
      }
      else
      {
        if ((option.find("-path")) != STR_NPOS)
        {
          if (!Monitoring)
          {
            stdOut += "path:   ";
            stdOut += spath;
            stdOut += "\n";
          }
          else
          {
            stdOut += "path=";
            stdOut += spath;
            stdOut += " ";
          }
        }

        if ((option.find("-fxid")) != STR_NPOS)
        {
          if (!Monitoring)
          {
            stdOut += "fxid:   ";
            stdOut += hexfidstring;
            stdOut += "\n";
          }
          else
          {
            stdOut += "fxid=";
            stdOut += hexfidstring;
            stdOut += " ";
          }
        }

        if ((option.find("-fid")) != STR_NPOS)
        {
          char fid[32];
          snprintf(fid, 32, "%llu", (unsigned long long) fmd->getId());
          if (!Monitoring)
          {
            stdOut += "fid:    ";
            stdOut += fid;
            stdOut += "\n";
          }
          else
          {
            stdOut += "fid=";
            stdOut += fid;
            stdOut += " ";
          }
        }

        if ((option.find("-size")) != STR_NPOS)
        {
          if (!Monitoring)
          {
            stdOut += "size:   ";
            stdOut += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) fmd->getSize());
            stdOut += "\n";
          }
          else
          {
            stdOut += "size=";
            stdOut += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) fmd->getSize());
            stdOut += " ";
          }
        }

        if ((option.find("-checksum")) != STR_NPOS)
        {
          if (!Monitoring)
          {
            stdOut += "xstype: ";
            stdOut += eos::common::LayoutId::GetChecksumString(fmd->getLayoutId());
            stdOut += "\n";
            stdOut += "xs:     ";
            for (unsigned int i = 0; i < eos::common::LayoutId::GetChecksumLen(fmd->getLayoutId()); i++)
            {
              char hb[3];
              sprintf(hb, "%02x", (unsigned char) (fmd->getChecksum().getDataPadded(i)));
              stdOut += hb;
            }
            stdOut += "\n";
          }
          else
          {
            stdOut += "xstype=";
            stdOut += eos::common::LayoutId::GetChecksumString(fmd->getLayoutId());
            stdOut += " ";
            stdOut += "xs=";
            for (unsigned int i = 0; i < eos::common::LayoutId::GetChecksumLen(fmd->getLayoutId()); i++)
            {
              char hb[3];
              sprintf(hb, "%02x", (unsigned char) (fmd->getChecksum().getDataPadded(i)));
              stdOut += hb;
            }
            stdOut += " ";
          }
        }

        if (Monitoring || (!(option.length())) || (option == "--fullpath") || (option == "-m"))
        {
          char ctimestring[4096];
          char mtimestring[4096];
          eos::FileMD::ctime_t mtime;
          eos::FileMD::ctime_t ctime;
          fmd->getCTime(ctime);
          fmd->getMTime(mtime);
          time_t filectime = (time_t) ctime.tv_sec;
          time_t filemtime = (time_t) mtime.tv_sec;
          char fid[32];
          snprintf(fid, 32, "%llu", (unsigned long long) fmd->getId());

          std::string etag;
          // if there is a checksum we use the checksum, otherwise we return inode+mtime
          size_t cxlen = eos::common::LayoutId::GetChecksumLen(fmd->getLayoutId());
          if (cxlen)
          {
            // use inode + checksum
            char setag[256];
        snprintf(setag,sizeof(setag)-1,"%llu:", (unsigned long long)fmd->getId()<<28);
            etag = setag;
            for (unsigned int i = 0; i < cxlen; i++)
            {
              char hb[3];
              sprintf(hb, "%02x", (i < cxlen) ? (unsigned char) (fmd->getChecksum().getDataPadded(i)) : 0);
              etag += hb;
            }
          }
          else
          {
            // use inode + mtime
            char setag[256];
            eos::FileMD::ctime_t mtime;
            fmd->getMTime(mtime);
            time_t filemtime = (time_t) mtime.tv_sec;
	    snprintf(setag,sizeof(setag)-1,"%llu:%llu", (unsigned long long)fmd->getId()<<28, (unsigned long long)filemtime);
            etag = setag;
          }

          if (!Monitoring)
          {
            stdOut = "  File: '";
            stdOut += spath;
            stdOut += "'";
            stdOut += "  Flags: ";
            stdOut +=  eos::common::StringConversion::IntToOctal( (int) fmd->getFlags(), 4).c_str();
            stdOut += "\n";
            stdOut += "  Size: ";
            stdOut += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) fmd->getSize());
            stdOut += "\n";
            stdOut += "Modify: ";
            stdOut += ctime_r(&filemtime, mtimestring);
            stdOut.erase(stdOut.length() - 1);
            stdOut += " Timestamp: ";
            stdOut += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) mtime.tv_sec);
            stdOut += ".";
            stdOut += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) mtime.tv_nsec);
            stdOut += "\n";
            stdOut += "Change: ";
            stdOut += ctime_r(&filectime, ctimestring);
            stdOut.erase(stdOut.length() - 1);
            stdOut += " Timestamp: ";
            stdOut += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) ctime.tv_sec);
            stdOut += ".";
            stdOut += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) ctime.tv_nsec);
            stdOut += "\n";
            stdOut += "  CUid: ";
            stdOut += (int) fmd->getCUid();
            stdOut += " CGid: ";
            stdOut += (int) fmd->getCGid();

            stdOut += "  Fxid: ";
            stdOut += hexfidstring;
            stdOut += " ";
            stdOut += "Fid: ";
            stdOut += fid;
            stdOut += " ";
            stdOut += "   Pid: ";
            stdOut += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) fmd->getContainerId());
            stdOut += "   Pxid: ";
            stdOut += hexpidstring;
            stdOut += "\n";
            stdOut += "XStype: ";
            stdOut += eos::common::LayoutId::GetChecksumString(fmd->getLayoutId());
            stdOut += "    XS: ";
            size_t cxlen = eos::common::LayoutId::GetChecksumLen(fmd->getLayoutId());
            for (unsigned int i = 0; i < cxlen; i++)
            {
              char hb[3];
              sprintf(hb, "%02x ", (unsigned char) (fmd->getChecksum().getDataPadded(i)));
              stdOut += hb;
            }
            stdOut += "    ETAG: ";
            stdOut += etag.c_str();
            stdOut += "\n";
            stdOut + "Layout: ";
            stdOut += eos::common::LayoutId::GetLayoutTypeString(fmd->getLayoutId());
            stdOut += " Stripes: ";
            stdOut += (int) (eos::common::LayoutId::GetStripeNumber(fmd->getLayoutId()) + 1);
            stdOut += " Blocksize: ";
            stdOut += eos::common::LayoutId::GetBlockSizeString(fmd->getLayoutId());
            stdOut += " LayoutId: ";
            XrdOucString hexlidstring;
            eos::common::FileId::Fid2Hex(fmd->getLayoutId(), hexlidstring);
            stdOut += hexlidstring;
            stdOut += "\n";
            stdOut += "  #Rep: ";
            stdOut += (int) fmd->getNumLocation();
            stdOut += "\n";
          }
          else
          {
            stdOut = "keylength.file=";
            stdOut += spath.length();
            stdOut += " ";
            stdOut += "file=";
            stdOut += spath;
            stdOut += " ";
            stdOut += "size=";
            stdOut += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) fmd->getSize());
            stdOut += " ";
            stdOut += "mtime=";
            stdOut += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) mtime.tv_sec);
            stdOut += ".";
            stdOut += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) mtime.tv_nsec);
            stdOut += " ";
            stdOut += "ctime=";
            stdOut += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) ctime.tv_sec);
            stdOut += ".";
            stdOut += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) ctime.tv_nsec);
            stdOut += " ";
            stdOut += "mode=";
            stdOut +=  eos::common::StringConversion::IntToOctal( (int) fmd->getFlags(), 4).c_str();
            stdOut += " ";
            stdOut += "uid=";
            stdOut += (int) fmd->getCUid();
            stdOut += " gid=";
            stdOut += (int) fmd->getCGid();
            stdOut += " ";
            stdOut += "fxid=";
            stdOut += hexfidstring;
            stdOut += " ";
            stdOut += "fid=";
            stdOut += fid;
            stdOut += " ";
            stdOut += "ino=";
            stdOut += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) eos::common::FileId::FidToInode(fmd->getId()));
            stdOut += " ";
            stdOut += "pid=";
            stdOut += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) fmd->getContainerId());
            stdOut += " ";
            stdOut += "pxid=";
            stdOut += hexpidstring;
            stdOut += " ";
            stdOut += "xstype=";
            stdOut += eos::common::LayoutId::GetChecksumString(fmd->getLayoutId());
            stdOut += " ";
            stdOut += "xs=";
            size_t cxlen = eos::common::LayoutId::GetChecksumLen(fmd->getLayoutId());

            if (cxlen)
            {
              for (unsigned int i = 0; i < cxlen; i++)
              {
                char hb[3];
                sprintf(hb, "%02x", (unsigned char)(fmd->getChecksum().getDataPadded(i)));
                stdOut += hb;
              }
            }
            else
            {
              stdOut += "0";
            }

            stdOut += " ";
            stdOut += "etag=";
            stdOut += etag.c_str();
            stdOut += " ";
            stdOut += "layout=";
            stdOut += eos::common::LayoutId::GetLayoutTypeString(fmd->getLayoutId());
            stdOut += " nstripes=";
            stdOut += (int) (eos::common::LayoutId::GetStripeNumber(fmd->getLayoutId()) + 1);
            stdOut += " ";
            stdOut += "lid=";
            XrdOucString hexlidstring;
            eos::common::FileId::Fid2Hex(fmd->getLayoutId(), hexlidstring);
            stdOut += hexlidstring;
            stdOut += " ";
            stdOut += "nrep=";
            stdOut += (int) fmd->getNumLocation();
            stdOut += " ";
          }


          eos::FileMD::LocationVector::const_iterator lociter;
          int i = 0;
          for (lociter = fmd->locationsBegin(); lociter != fmd->locationsEnd(); ++lociter)
          {
            // ignore filesystem id 0
            if (!(*lociter))
            {
              eos_err("fsid 0 found fid=%lld", fmd->getId());
              continue;
            }

            char fsline[4096];
            XrdOucString location = "";
            location += (int) *lociter;

            XrdOucString si = "";
            si += (int) i;
            eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
            eos::common::FileSystem* filesystem = 0;
            if (FsView::gFsView.mIdView.count(*lociter))
            {
              filesystem = FsView::gFsView.mIdView[*lociter];
            }
            if (filesystem)
            {
              if (i == 0)
              {
                if (!Monitoring)
                {
                  std::string out = "";
                  stdOut += " #   fs-id  ";
                  std::string format = "header=1|indent=12|headeronly=1|key=host:width=24:format=s|sep= |sep= |key=schedgroup:width=16:format=s|sep= |key=path:width=16:format=s|sep= |key=stat.boot:width=10:format=s|sep= |key=configstatus:width=14:format=s|sep= |key=stat.drain:width=12:format=s|sep= |key=stat.active:width=8:format=s|sep= |key=stat.geotag:width=24:format=s";
                  filesystem->Print(out, format);
                  stdOut += out.c_str();
                }
              }
              if (!Monitoring)
              {
                sprintf(fsline, "%3s   %5s ", si.c_str(), location.c_str());
                stdOut += fsline;

                std::string out = "";
                std::string format = "key=host:width=24:format=s|sep= |sep= |key=schedgroup:width=16:format=s|sep= |key=path:width=16:format=s|sep= |key=stat.boot:width=10:format=s|sep= |key=configstatus:width=14:format=s|sep= |key=stat.drain:width=12:format=s|sep= |key=stat.active:width=8:format=s|sep= |key=stat.geotag:width=24:format=s";
                filesystem->Print(out, format);
                stdOut += out.c_str();
              }
              else
              {
                stdOut += "fsid=";
                stdOut += location.c_str();
                stdOut += " ";
              }
              if ((option.find("-fullpath")) != STR_NPOS)
              {
                // for the fullpath option we output the full storage path for each replica
                XrdOucString fullpath;
                eos::common::FileId::FidPrefix2FullPath(hexfidstring.c_str(), filesystem->GetPath().c_str(), fullpath);
                if (!Monitoring)
                {
                  stdOut.erase(stdOut.length() - 1);
                  stdOut += " ";
                  stdOut += fullpath;
                  stdOut += "\n";
                }
                else
                {
                  stdOut += "fullpath=";
                  stdOut += fullpath;
                  stdOut += " ";
                }
              }
            }
            else
            {
              if (!Monitoring)
              {
                sprintf(fsline, "%3s   %5s ", si.c_str(), location.c_str());
                stdOut += fsline;
                stdOut += "NA\n";
              }
            }
            i++;
          }
          for (lociter = fmd->unlinkedLocationsBegin(); lociter != fmd->unlinkedLocationsEnd(); ++lociter)
          {
            if (!Monitoring)
            {
              stdOut += "(undeleted) $ ";
              stdOut += (int) *lociter;
              stdOut += "\n";
            }
            else
            {
              stdOut += "fsdel=";
              stdOut += (int) *lociter;
              stdOut += " ";
            }
          }
          if (!Monitoring)
          {
            stdOut += "*******";
          }
        }
      }
    }
  }
  return SFS_OK;
}

int
ProcCommand::DirInfo (const char* path)
{
  XrdOucString option = pOpaque->Get("mgm.file.info.option");
  XrdOucString spath = path;
  {
    eos::ContainerMD* fmd = 0;

    if ((spath.beginswith("pid:") || (spath.beginswith("pxid:"))))
    {
      unsigned long long fid = 0;
      if (spath.beginswith("pid:"))
      {
        spath.replace("pid:", "");
        fid = strtoull(spath.c_str(), 0, 10);
      }
      if (spath.beginswith("pxid:"))
      {
        spath.replace("pxid:", "");
        fid = strtoull(spath.c_str(), 0, 16);
      }

      // reference by fid+fsid
      //-------------------------------------------
      gOFS->eosViewRWMutex.LockRead();
      try
      {
        fmd = gOFS->eosDirectoryService->getContainerMD(fid);
        std::string fullpath = gOFS->eosView->getUri(fmd);
        spath = fullpath.c_str();
      }
      catch (eos::MDException &e)
      {
        errno = e.getErrno();
        stdErr = "error: cannot retrieve directory meta data - ";
        stdErr += e.getMessage().str().c_str();
        eos_debug("caught exception %d %s\n", e.getErrno(), e.getMessage().str().c_str());
      }
    }
    else
    {
      // reference by path
      //-------------------------------------------
      gOFS->eosViewRWMutex.LockRead();
      try
      {
        fmd = gOFS->eosView->getContainer(spath.c_str());
      }
      catch (eos::MDException &e)
      {
        errno = e.getErrno();
        stdErr = "error: cannot retrieve directory meta data - ";
        stdErr += e.getMessage().str().c_str();
        eos_debug("caught exception %d %s\n", e.getErrno(), e.getMessage().str().c_str());
      }
    }



    if (!fmd)
    {
      retc = errno;
      gOFS->eosViewRWMutex.UnLockRead();
      //-------------------------------------------

    }
    else
    {
      size_t num_containers = fmd->getNumContainers();
      size_t num_files = fmd->getNumFiles();

      // Make a copy of the meta data
      eos::ContainerMD fmdCopy(*fmd);
      fmd = &fmdCopy;
      gOFS->eosViewRWMutex.UnLockRead();
      //-------------------------------------------

      XrdOucString sizestring;
      XrdOucString hexfidstring;
      XrdOucString hexpidstring;
      bool Monitoring = false;

      eos::common::FileId::Fid2Hex(fmd->getId(), hexfidstring);
      eos::common::FileId::Fid2Hex(fmd->getParentId(), hexpidstring);

      if ((option.find("-m")) != STR_NPOS)
      {
        Monitoring = true;
      }

      if ((option.find("-path")) != STR_NPOS)
      {
        if (!Monitoring)
        {
          stdOut += "path:   ";
          stdOut += spath;
          stdOut += "\n";
        }
        else
        {
          stdOut += "path=";
          stdOut += spath;
          stdOut += " ";
        }
      }

      if ((option.find("-fxid")) != STR_NPOS)
      {
        if (!Monitoring)
        {
          stdOut += "fxid:   ";
          stdOut += hexfidstring;
          stdOut += "\n";
        }
        else
        {
          stdOut += "fxid=";
          stdOut += hexfidstring;
          stdOut += " ";
        }
      }

      if ((option.find("-fid")) != STR_NPOS)
      {
        char fid[32];
        snprintf(fid, 32, "%llu", (unsigned long long) fmd->getId());
        if (!Monitoring)
        {
          stdOut += "fid:    ";
          stdOut += fid;
          stdOut += "\n";
        }
        else
        {
          stdOut += "fid=";
          stdOut += fid;
          stdOut += " ";
        }
      }

      if ((option.find("-size")) != STR_NPOS)
      {
        if (!Monitoring)
        {
          stdOut += "size:   ";
          stdOut += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long)(num_containers + num_files));
          stdOut += "\n";
        }
        else
        {
          stdOut += "size=";
          stdOut += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long)(num_containers + num_files));
          stdOut += " ";
        }
      }

      if (Monitoring || (!(option.length())) || (option == "--fullpath") || (option == "-m"))
      {
        char ctimestring[4096];
        char mtimestring[4096];
        char tmtimestring[4096];
        eos::ContainerMD::mtime_t mtime;
        eos::ContainerMD::ctime_t ctime;
        eos::ContainerMD::tmtime_t tmtime;
        fmd->getCTime(ctime);
	fmd->getMTime(mtime);
	fmd->getTMTime(tmtime);

        {
          if (!mtime.tv_sec)
          {
            mtime.tv_sec = ctime.tv_sec;
            mtime.tv_nsec = ctime.tv_nsec;
          }
          if (!tmtime.tv_sec)
          {
            tmtime.tv_sec = mtime.tv_sec;
            tmtime.tv_nsec = mtime.tv_nsec;
          }
        }

        time_t filectime = (time_t) ctime.tv_sec;
        time_t filemtime = (time_t) mtime.tv_sec;
        time_t filetmtime = (time_t) tmtime.tv_sec;
        char fid[32];
        snprintf(fid, 32, "%llu", (unsigned long long) fmd->getId());

        std::string etag;

        // use inode + tmtime
        char setag[256];
        snprintf(setag,sizeof(setag)-1,"%llu:%llu", (unsigned long long)fmd->getId(), (unsigned long long)filetmtime);
        etag = setag;

        if (!Monitoring)
        {
          stdOut = "  Directory: '";
          stdOut += spath;
          stdOut += "'";
          stdOut += "  Container: ";
          stdOut += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long)num_containers);
          stdOut += "  Files: ";
          stdOut += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long)num_files);
          stdOut += "  Flags: ";
          stdOut +=  eos::common::StringConversion::IntToOctal( (int) fmd->getMode(), 4).c_str();
          stdOut += "\n";
          stdOut += "Modify: ";
          stdOut += ctime_r(&filemtime, mtimestring);
          stdOut.erase(stdOut.length() - 1);
          stdOut += " Timestamp: ";
          stdOut += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) mtime.tv_sec);
          stdOut += ".";
          stdOut += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) mtime.tv_nsec);
          stdOut += "\n";
          stdOut += "Change: ";
          stdOut += ctime_r(&filectime, ctimestring);
          stdOut.erase(stdOut.length() - 1);
          stdOut += " Timestamp: ";
          stdOut += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) ctime.tv_sec);
          stdOut += ".";
          stdOut += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) ctime.tv_nsec);
          stdOut += "\n";
          stdOut += "Sync:   ";
          stdOut += ctime_r(&filetmtime, tmtimestring);
          stdOut.erase(stdOut.length() - 1);
          stdOut += " Timestamp: ";
          stdOut += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) tmtime.tv_sec);
          stdOut += ".";
          stdOut += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) tmtime.tv_nsec);
          stdOut += "\n";
          stdOut += "  CUid: ";
          stdOut += (int) fmd->getCUid();
          stdOut += " CGid: ";
          stdOut += (int) fmd->getCGid();
          stdOut += "  Fxid: ";
          stdOut += hexfidstring;
          stdOut += " ";
          stdOut += "Fid: ";
          stdOut += fid;
          stdOut += " ";
          stdOut += "   Pid: ";
          stdOut += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) fmd->getParentId());
          stdOut += "   Pxid: ";
          stdOut += hexpidstring;
          stdOut += "\n";
          stdOut += "  ETAG: ";
          stdOut += etag.c_str();
          stdOut += "\n";
        }
        else
        {
          stdOut = "keylength.file=";
          stdOut += spath.length();
          stdOut += " ";
          stdOut += "file=";
          stdOut += spath;
          stdOut += " ";
          stdOut += "container=";
          stdOut += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long)num_containers);
          stdOut += " ";
          stdOut += "files=";
          stdOut += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long)num_files);
          stdOut += " ";
          stdOut += "mtime=";
          stdOut += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) mtime.tv_sec);
          stdOut += ".";
          stdOut += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) mtime.tv_nsec);
          stdOut += " ";
          stdOut += "ctime=";                                           \
          stdOut += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) ctime.tv_sec);
          stdOut += ".";
          stdOut += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) ctime.tv_nsec);
          stdOut += " ";
          stdOut += "mode=";
          stdOut +=  eos::common::StringConversion::IntToOctal( (int) fmd->getMode(), 4).c_str();
          stdOut += " ";
          stdOut += "uid=";
          stdOut += (int) fmd->getCUid();
          stdOut += " gid=";
          stdOut += (int) fmd->getCGid();
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
          stdOut += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) fmd->getParentId());
          stdOut += " ";
          stdOut += "pxid=";
          stdOut += hexpidstring;
          stdOut += " ";
          stdOut += "etag=";
          stdOut += etag.c_str();
          stdOut += " ";

          for (auto it_attr = fmd->attributesBegin(); it_attr != fmd->attributesEnd(); ++it_attr)
          {
            stdOut += "xattrn=";    stdOut += it_attr->first.c_str();
            stdOut += " xattrv=";  stdOut += it_attr->second.c_str();
            stdOut += " ";
          }
        }
      }
    }
  }

  return SFS_OK;
}
EOSMGMNAMESPACE_END
