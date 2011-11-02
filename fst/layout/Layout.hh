// ----------------------------------------------------------------------
// File: Layout.hh
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

#ifndef __EOSFST_LAYOUT_HH__
#define __EOSFST_LAYOUT_HH__

/*----------------------------------------------------------------------------*/
#include <sys/types.h>
/*----------------------------------------------------------------------------*/
#include "common/LayoutId.hh"
#include "common/Logging.hh"
#include "fst/Namespace.hh"
#include "fst/XrdFstOfsFile.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdSfs/XrdSfsInterface.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

class XrdFstOfsFile;


// -------------------------------------------------------------------------------------------
// we use this truncate offset (1TB) to indicate that a file should be deleted during the close 
// there is no better interface usable via XrdClient to communicate a deletion on a open file

#define EOS_FST_DELETE_FLAG_VIA_TRUNCATE_LEN 1024 * 1024 * 1024 * 1024ll

class Layout : public eos::common::LogId {
protected:
  XrdOucString Name;
  XrdOucString LocalReplicaPath;

  XrdFstOfsFile* ofsFile;
  unsigned int layOutId;
  XrdOucErrInfo* error;
  bool isEntryServer;
  int  blockChecksum;

public:

  Layout(XrdFstOfsFile* thisFile=0){Name = "";ofsFile = thisFile;}
  Layout(XrdFstOfsFile* thisFile,const char* name, int lid, XrdOucErrInfo *outerror){
    Name = name; ofsFile = thisFile; layOutId = lid; error = outerror; isEntryServer=true;
    blockChecksum=eos::common::LayoutId::GetBlockChecksum(lid);
    LocalReplicaPath = "";
  }

  const char* GetName() {return Name.c_str();}
  const char* GetLocalReplicaPath() { return LocalReplicaPath.c_str();}

  unsigned int GetLayOutId() { return layOutId;}

  virtual int open(const char                *path,
                   XrdSfsFileOpenMode   open_mode,
                   mode_t               create_mode,
                   const XrdSecEntity        *client,
                   const char                *opaque) = 0;

  virtual bool IsEntryServer() { return isEntryServer; }

  virtual int read(XrdSfsFileOffset offset, char* buffer, XrdSfsXferSize length) = 0;
  virtual int write(XrdSfsFileOffset offset, char* buffer, XrdSfsXferSize length) = 0;
  virtual int truncate(XrdSfsFileOffset offset) = 0;
  virtual int fallocate(XrdSfsFileOffset lenght) {return 0;}
  virtual int remove() {return 0;} 
  virtual int sync() = 0;
  virtual int close() = 0;

  virtual int stat(struct stat *buf) = 0;
  
  virtual ~Layout(){};
};

EOSFSTNAMESPACE_END

#endif
