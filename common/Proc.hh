// ----------------------------------------------------------------------
// File: Proc.hh
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

#ifndef __EOSCOMMON_PROC__
#define __EOSCOMMON_PROC__

/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucHash.hh"
/*----------------------------------------------------------------------------*/
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <dirent.h>
/*----------------------------------------------------------------------------*/


EOSCOMMONNAMESPACE_BEGIN

class ProcFile
{
private:
  int fd;
  XrdOucString fname;
  bool procsync;
  time_t lastwrite;

public:
  bool Open();
  bool Close() { if (fd>=0) close(fd);return true;}
  bool Write(long long val, int writedelay=0);
  bool Write(double val, int writedelay=0);
  bool Write(const char* str, int writedelay=0);
  bool WriteKeyVal(const char* key, unsigned long long value, int writedelay, bool truncate=0);
  long long Read();
  bool Read(XrdOucString &str);
  

  ProcFile(const char* name, bool syncit=false){fname = name;fd=0;procsync = syncit;lastwrite=0;};
  virtual ~ProcFile() {Close();};
};

class Proc
{
private:
  bool procsync;
  XrdOucString procdirectory;
  XrdOucHash<ProcFile> files;

public:
  
  ProcFile* Handle(const char* name);

  Proc(const char* procdir, bool syncit) { 
    procdirectory = procdir; 
    procsync = syncit;
  };

  bool Open() {
    XrdOucString doit="mkdir -p ";
    doit+=procdirectory;
    int rc = system(doit.c_str());
    if (!rc)
      return false;

    DIR* pd=opendir(procdirectory.c_str());
    if (!pd) {
      return false;
    } else {
      closedir(pd);
      return true;
    }
  }

  virtual ~Proc() {};
};

EOSCOMMONNAMESPACE_END

#endif

