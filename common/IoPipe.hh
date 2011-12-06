// ----------------------------------------------------------------------
// File: IoPipe.hh
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

#ifndef __EOSCOMMON_IOPIPE__
#define __EOSCOMMON_IOPIPE__

/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
#include "common/Path.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdSys/XrdSysLogger.hh"
#include "XrdNet/XrdNetOpts.hh"
#include "XrdNet/XrdNetSocket.hh"
/*----------------------------------------------------------------------------*/
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <iostream>
#include <fstream>
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN
class IoPipe {
 public:
  XrdOucString lPrefix;
  XrdOucString lPipeDir;
  XrdOucString lPipeProducerLock;
  XrdOucString lPipeConsumerLock;

  XrdOucString lStdInName ;
  XrdOucString lStdOutName;
  XrdOucString lStdErrName;
  XrdOucString lRetcName  ;

  XrdNetSocket* stdinsocket;
  XrdNetSocket* stdoutsocket;
  XrdNetSocket* stderrsocket;
  XrdNetSocket* retcsocket;

  int lConsumerFd;
  int lProducerFd;

  IoPipe(XrdOucString prefix = "/tmp/eos") {
    lPrefix = prefix;
    lPipeDir = prefix;
    lPipeDir += ".";
    lPipeDir += (int) getuid();
    lPipeDir += "/";
    lPipeDir += (int) getppid();
    lPipeDir += "/";
    lPipeProducerLock = lPipeDir + "producer.lock";
    lPipeConsumerLock = lPipeDir + "consumer.lock";
    lStdInName  = "xstdin";
    lStdOutName = "xstdout";
    lStdErrName = "xstderr";
    lRetcName   = "xretc";
    stdinsocket = stdoutsocket = stderrsocket = retcsocket = 0;
    lProducerFd = lConsumerFd = 0;
  }

  void WritePid(const char* path, pid_t pid) {
    std::ofstream pidfile(path, ios::binary);
    pidfile << pid ;
  }

  pid_t ReadPid(const char* path) {
    pid_t lpid=0;
    std::ifstream pidfile(path);
    pidfile  >> lpid;
    return lpid;
  }

  bool Init() {
    XrdOucString dummypipedir = lPipeDir; dummypipedir += "/dummy";
    eos::common::Path dPath(dummypipedir.c_str());
    if(!dPath.MakeParentPath(S_IRWXU)) {
      return false;
    }
    return true;
  }

  bool LockProducer() {
    int fd = open (lPipeProducerLock.c_str(),O_EXCL| O_CREAT, S_IRWXU);
    if (fd>=0) {
      close(fd);
      WritePid(lPipeProducerLock.c_str(), getpid());
      return true;
    }
    return false;
  }

  bool CheckProducer() {
    pid_t pid = ReadPid(lPipeProducerLock.c_str());
    if (pid) {
      if (!kill(pid,0)) 
	return true;
    }
    UnLockProducer();
    return false;
  }

  bool KillProducer() {
    pid_t pid = ReadPid(lPipeProducerLock.c_str());
    if (pid) {
      if (!kill(pid,3)) 
	return true;
    }
    UnLockProducer();
    return false;

  }

  bool LockConsumer() {
    do {
      int fd = open (lPipeConsumerLock.c_str(),O_EXCL| O_CREAT, S_IRWXU);
      if (fd>=0) {
	close(fd);
	WritePid(lPipeConsumerLock.c_str(), getpid());
	return true;
      }
      usleep(100000);
    } while (1);
    return false;
  }

  bool UnLockProducer() {
    int rc = unlink(lPipeProducerLock.c_str());
    if (!rc)
      return true;
    else
      return false;
  }

  bool UnLockConsumer() {
    int rc = unlink(lPipeConsumerLock.c_str());
    if (!rc)
      return true;
    else
      return false;
  }


  int AttachStdin(XrdSysError &eDest) {
    XrdNetSocket* socket    = XrdNetSocket::Create(&eDest, lPipeDir.c_str(),  lStdInName.c_str(),  S_IRWXU, XRDNET_FIFO);
    if (socket) {
      stdinsocket = socket;
      return socket->SockNum();
    } else {
      return -1;
    }
  }

  int AttachStdout(XrdSysError &eDest) {
    XrdNetSocket* socket = XrdNetSocket::Create(&eDest, lPipeDir.c_str(), lStdOutName.c_str(), S_IRWXU, XRDNET_FIFO);
    if (socket) {
      stdoutsocket = socket;
      return socket->SockNum();
    } else {
      return -1;
    }
  }
  
  int AttachStderr(XrdSysError &eDest) {
    XrdNetSocket* socket = XrdNetSocket::Create(&eDest, lPipeDir.c_str(), lStdErrName.c_str(), S_IRWXU, XRDNET_FIFO);
    if (socket) {
      stderrsocket = socket;
      return socket->SockNum();
    } else {
      return -1;
    } 
  }
  
  int AttachRetc(XrdSysError &eDest) {
    XrdNetSocket* socket = XrdNetSocket::Create(&eDest, lPipeDir.c_str(), lRetcName.c_str(), S_IRWXU, XRDNET_FIFO);
    if (socket) {
      retcsocket = socket;
      return socket->SockNum();
    } else {
      return -1;
    }
  }
  
  ~IoPipe() {};
};

EOSCOMMONNAMESPACE_END

#endif
