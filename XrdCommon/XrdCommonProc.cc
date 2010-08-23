//          $Id: XrdCommonProc.cc,v 1.1 2008/09/15 10:04:02 apeters Exp $

#include "XrdCommon/XrdCommonProc.hh"
#include <string.h>

bool
XrdCommonProcFile::Open() {
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
XrdCommonProcFile::Write(long long val, int writedelay) {
  char pbuf[1024];
  sprintf(pbuf,"%lld\n",val);
  return Write(pbuf,writedelay);
}

bool 
XrdCommonProcFile::Write(double val, int writedelay) {
  char pbuf[1024];
  sprintf(pbuf,"%.02f\n",val);
  return Write(pbuf,writedelay);
}

bool 
XrdCommonProcFile::Write(const char* pbuf, int writedelay) {
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
XrdCommonProcFile::WriteKeyVal(const char* key, unsigned long long value, int writedelay, bool dotruncate) {
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
XrdCommonProcFile::Read() {
  char pbuf[1024];
  lseek(fd,0,SEEK_SET);
  ssize_t rb = read(fd,pbuf,sizeof(pbuf));
  if (rb<=0) 
    return -1;

  return strtoll(pbuf,(char**)0,10);
}

bool 
XrdCommonProcFile::Read(XrdOucString &str) {
  char pbuf[1025];
  pbuf[0] = 0;
  pbuf[1024] = 0;

  lseek(fd,0,SEEK_SET);
  ssize_t rb = read(fd,pbuf,sizeof(pbuf)-1);
  str = pbuf;
  
  if (rb<=0)
    return false;
  else
    return true;
}

XrdCommonProcFile*
XrdCommonProc::Handle(const char* name) {
  XrdCommonProcFile* phandle=0;
  if (( phandle = files.Find(name))) {
    return phandle;
  } else {
    XrdOucString pfname=procdirectory;
    pfname += "/";
    pfname += name;
    phandle = new XrdCommonProcFile(pfname.c_str());
    if (phandle && phandle->Open()) {
      files.Add(name,phandle);
      return phandle;
    }
    if (phandle)
      delete phandle;
  }
  return 0;
}


