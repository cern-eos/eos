// ----------------------------------------------------------------------
// File: proc/user/Fuse.cc
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
#include "mgm/XrdMgmOfsDirectory.hh"
#include "mgm/Access.hh"
#include "mgm/Macros.hh"
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

int
ProcCommand::Fuse ()
{
  gOFS->MgmStats.Add("Fuse-Dirlist", pVid->uid, pVid->gid, 1);
  XrdOucString spath = pOpaque->Get("mgm.path");
  bool statentries = pOpaque->GetInt("mgm.statentries")==-999999999?false:(bool)pOpaque->GetInt("mgm.statentries");
  bool encodepath  = pOpaque->Get("eos.encodepath");

  const char* inpath = spath.c_str();

  NAMESPACEMAP;
  info = 0;
  if (info)info = 0; // for compiler happyness
  PROC_BOUNCE_ILLEGAL_NAMES;
  PROC_BOUNCE_NOT_ALLOWED;

  spath = path;
  
  if(encodepath)
    mResultStream = "inodirlist_pathencode: retc=";
  else
    mResultStream = "inodirlist: retc=";

  if (!spath.length())
  {
    mResultStream += EINVAL;
  }
  else
  {
    XrdMgmOfsDirectory* inodir = (XrdMgmOfsDirectory*) gOFS->newDir((char*) "");
    if (!inodir)
    {
      mResultStream += ENOMEM;
      return SFS_ERROR;
    }

    if ((retc = inodir->_open(path, *pVid, 0)) != SFS_OK)
    {
      delete inodir;
      retc = -retc;
      mResultStream += retc;
      mLen = mResultStream.length();
      mOffset = 0;
      return SFS_OK;
    }

    const char* entry;

    mResultStream += 0;
    mResultStream += " ";

    unsigned long long inode = 0;

    char inodestr[256];
    size_t dotend = 0;
    size_t dotstart = mResultStream.length();

    while ((entry = inodir->nextEntry()))
    {
      bool isdot = false;
      bool isdotdot = false;

      XrdOucString whitespaceentry = entry;

      if (whitespaceentry == ".")
      {
        isdot = true;
      }
      if (whitespaceentry == "..")
      {
        isdotdot = true;
      }

      if(encodepath)
      {
        whitespaceentry = eos::common::StringConversion::curl_escaped(whitespaceentry.c_str()).c_str();
      }
      else
      {
        // encode spaces
        whitespaceentry.replace(" ", "%20");

        // encode \n
        whitespaceentry.replace("\n", "%0A");
      }

      if ((!isdot) && (!isdotdot))
      {
        mResultStream += whitespaceentry;
        mResultStream += " ";
      }
      if (isdot)
      {
        // the . and .. has to be streamed as first entries
        mResultStream.insert(". ", dotstart);
      }
      if (isdotdot)
      {
        mResultStream.insert(".. ", dotend);
      }

      XrdOucString statpath = path;
      statpath += "/";
      statpath += entry;

      eos::common::Path cPath(statpath.c_str());

      // attach MD to get inode number
      eos::FileMD* fmd = 0;
      eos::ContainerMD* dir = 0;
      inode = 0;

      //-------------------------------------------
      {
        eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
        try
        {
          fmd = gOFS->eosView->getFile(cPath.GetPath(), false);
          inode = fmd->getId() << 28;
        }
        catch (eos::MDException &e)
        {
          errno = e.getErrno();
          eos_debug("caught exception %d %s\n", e.getErrno(), e.getMessage().str().c_str());
        }
      }
      //-------------------------------------------

      // check if that is a directory in case
      if (!fmd)
      {
        //-------------------------------------------
        eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
        try
        {
          dir = gOFS->eosView->getContainer(cPath.GetPath(), false);
          inode = dir->getId();
        }
        catch (eos::MDException &e)
        {
          dir = 0;
          eos_debug("caught exception %d %s\n", e.getErrno(), e.getMessage().str().c_str());
        }
        //-------------------------------------------
      }
      sprintf(inodestr, "%lld", inode);
      if ((!isdot) && (!isdotdot) && inode)
      {
        mResultStream += inodestr;
        mResultStream += " ";
        if(statentries && (fmd || dir))
        {
          struct stat buf;
          std::string uri;
          if(!gOFS->_stat(cPath.GetPath(), &buf, *mError, *pVid, (const char*) 0, 0, false, &uri))
          {
            char cbuf[1024];
            char* ss=cbuf;
            (*(ss++))='{';
            ss = eos::common::StringConversion::FastUnsignedToAsciiHex(buf.st_atim.tv_nsec,ss);
            (*(ss++))=',';
            ss = eos::common::StringConversion::FastUnsignedToAsciiHex(buf.st_atim.tv_sec,ss);
            (*(ss++))=',';
            ss = eos::common::StringConversion::FastUnsignedToAsciiHex(buf.st_blksize,ss);
            (*(ss++))=',';
            ss = eos::common::StringConversion::FastUnsignedToAsciiHex(buf.st_blocks,ss);
            (*(ss++))=',';
            ss = eos::common::StringConversion::FastUnsignedToAsciiHex(buf.st_ctim.tv_nsec,ss);
            (*(ss++))=',';
            ss = eos::common::StringConversion::FastUnsignedToAsciiHex(buf.st_ctim.tv_sec,ss);
            (*(ss++))=',';
            ss = eos::common::StringConversion::FastUnsignedToAsciiHex(buf.st_dev,ss);
            (*(ss++))=',';
            ss = eos::common::StringConversion::FastUnsignedToAsciiHex(buf.st_gid,ss);
            (*(ss++))=',';
            ss = eos::common::StringConversion::FastUnsignedToAsciiHex(buf.st_ino,ss);
            (*(ss++))=',';
            ss = eos::common::StringConversion::FastUnsignedToAsciiHex(buf.st_mode,ss);
            (*(ss++))=',';
            ss = eos::common::StringConversion::FastUnsignedToAsciiHex(buf.st_mtim.tv_nsec,ss);
            (*(ss++))=',';
            ss = eos::common::StringConversion::FastUnsignedToAsciiHex(buf.st_mtim.tv_sec,ss);
            (*(ss++))=',';
            ss = eos::common::StringConversion::FastUnsignedToAsciiHex(buf.st_nlink,ss);
            (*(ss++))=',';
            ss = eos::common::StringConversion::FastUnsignedToAsciiHex(buf.st_rdev,ss);
            (*(ss++))=',';
            ss = eos::common::StringConversion::FastUnsignedToAsciiHex(buf.st_size,ss);
            (*(ss++))=',';
            ss = eos::common::StringConversion::FastUnsignedToAsciiHex(buf.st_uid,ss);
            (*ss++)='}';
            (*ss++)=' ';
            (*ss++)=0;
            mResultStream+=cbuf;
          }
        }
      }
      else
      {
        if (isdot)
        {
          mResultStream.insert(inodestr, dotstart + 2);
          mResultStream.insert(" ", dotstart + 2 + strlen(inodestr));
          dotend = dotstart + 2 + strlen(inodestr) + 1;
        }
        else if(isdotdot)
        {
          mResultStream.insert(inodestr, dotend + 3);
          mResultStream.insert(" ", dotend + strlen(inodestr) + 3);
        }
        else
        {
          eos_debug("null inode and not . or ..");
        }
      }
    }

    inodir->close();
    delete inodir;
    eos_debug("returning resultstream %s", mResultStream.c_str());
    mLen = mResultStream.length();
    mOffset = 0;
  }
  return SFS_OK;
}

EOSMGMNAMESPACE_END
