// ----------------------------------------------------------------------
// File: Proc.cc
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

/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
#include "common/Proc.hh"
#include <string.h>
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

bool
ProcFile::Open() {
  if (procsync) {
    fd = open(fname.c_str(),O_CREAT| O_SYNC|O_RDWR, S_IRWXU | S_IROTH | S_IRGRP );
  } else {
    fd = open(fname.c_str(),O_CREAT|O_RDWR, S_IRWXU | S_IROTH | S_IRGRP);
  }

  if (fd<0) {
    return false;
  }
  return true;
}

bool 
ProcFile::Write(long long val, int writedelay) {
  char pbuf[1024];
  sprintf(pbuf,"%lld\n",val);
  return Write(pbuf,writedelay);
}

bool 
ProcFile::Write(double val, int writedelay) {
  char pbuf[1024];
  sprintf(pbuf,"%.02f\n",val);
  return Write(pbuf,writedelay);
}

bool 
ProcFile::Write(const char* pbuf, int writedelay) {
  time_t now = time(0);
  if (writedelay) { 

    if (now-lastwrite <writedelay) {
      return true;
    }
  }

  int result;
  lseek(fd,0,SEEK_SET);
  while ( (result=::ftruncate(fd,0)) && (errno == EINTR ) ) {}
  lastwrite = now;
  if ( (write(fd,pbuf,strlen(pbuf))) == ((int)strlen(pbuf))) {
    return true;
  } else {
    return false;
  }
}

bool
ProcFile::WriteKeyVal(const char* key, unsigned long long value, int writedelay, bool dotruncate) {
  if (dotruncate) {
    time_t now = time(0);
    if (writedelay) {
      
      if (now-lastwrite <writedelay) {
        return false;
      }
    }

    //    printf("Truncating FD %d for %s\n",fd,key);
    lseek(fd,0,SEEK_SET);
    while ( (::ftruncate(fd,0)) && (errno == EINTR ) ) {}
    lastwrite = now;
  }
  char pbuf[1024];
  sprintf(pbuf,"%u %-32s %lld\n",(unsigned int)time(0),key,value);
  if ( ((write(fd,pbuf,strlen(pbuf))) == (int)strlen(pbuf))) {
    return true;
  } else {
    return false;
  }
}

long long
ProcFile::Read() {
  char pbuf[1024];
  lseek(fd,0,SEEK_SET);
  ssize_t rb = read(fd,pbuf,sizeof(pbuf));
  if (rb<=0) 
    return -1;

  return strtoll(pbuf,(char**)0,10);
}

bool 
ProcFile::Read(XrdOucString &str) {
  char pbuf[1025];
  pbuf[0] = 0;
  pbuf[1024] = 0;

  lseek(fd,0,SEEK_SET);
  ssize_t rb = read(fd,pbuf,sizeof(pbuf)-1);
  pbuf[1024] = 0;
  str = pbuf;
  
  if (rb<=0)
    return false;
  else
    return true;
}

ProcFile*
Proc::Handle(const char* name) {
  ProcFile* phandle=0;
  if (( phandle = files.Find(name))) {
    return phandle;
  } else {
    XrdOucString pfname=procdirectory;
    pfname += "/";
    pfname += name;
    phandle = new ProcFile(pfname.c_str());
    if (phandle && phandle->Open()) {
      files.Add(name,phandle);
      return phandle;
    }
    if (phandle)
      delete phandle;
  }
  return 0;
}

EOSCOMMONNAMESPACE_END




