// ----------------------------------------------------------------------
// File: PlainLayout.cc
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
#include "fst/layout/PlainLayout.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOss/XrdOssApi.hh"
/*----------------------------------------------------------------------------*/
#include <xfs/xfs.h>
/*----------------------------------------------------------------------------*/

extern XrdOssSys *XrdOfsOss;

EOSFSTNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
int          
PlainLayout::open(const char                *path,
                  XrdSfsFileOpenMode   open_mode,
                  mode_t               create_mode,
                  const XrdSecEntity        *client,
                  const char                *opaque)
{
  LocalReplicaPath = path;
  return ofsFile->openofs(path, open_mode, create_mode, client, opaque);
}


/*----------------------------------------------------------------------------*/
int
PlainLayout::read(XrdSfsFileOffset offset, char* buffer, XrdSfsXferSize length)
{
  return ofsFile->readofs(offset, buffer,length);
}

/*----------------------------------------------------------------------------*/
int 
PlainLayout::write(XrdSfsFileOffset offset, char* buffer, XrdSfsXferSize length)
{
  return ofsFile->writeofs(offset, buffer, length);
}

/*----------------------------------------------------------------------------*/
int
PlainLayout::truncate(XrdSfsFileOffset offset)
{
  return ofsFile->truncateofs(offset);
}

/*----------------------------------------------------------------------------*/
int
PlainLayout::fallocate(XrdSfsFileOffset length)
{
  XrdOucErrInfo error;
  if(ofsFile->fctl(SFS_FCTL_GETFD,0,error)) 
    return -1;
  int fd = error.getErrInfo();

  if(platform_test_xfs_fd(fd)) {
    // select the fast XFS allocation function if available
    xfs_flock64_t fl;
    fl.l_whence= 0;
    fl.l_start= 0;
    fl.l_len= (off64_t)length;
    return xfsctl(NULL, fd, XFS_IOC_RESVSP64, &fl);
  } else {
    return posix_fallocate(fd,0,length);
  }
  return -1;
}

/*----------------------------------------------------------------------------*/
int
PlainLayout::fdeallocate(XrdSfsFileOffset fromoffset, XrdSfsFileOffset tooffset)
{
  XrdOucErrInfo error;
  if(ofsFile->fctl(SFS_FCTL_GETFD,0,error))
    return -1;
  int fd = error.getErrInfo();
  if (fd>0) {
    if(platform_test_xfs_fd(fd)) {
      // select the fast XFS deallocation function if available
      xfs_flock64_t fl;
      fl.l_whence= 0;
      fl.l_start= fromoffset;
      fl.l_len= (off64_t)tooffset-fromoffset;
      return xfsctl(NULL, fd, XFS_IOC_UNRESVSP64, &fl);
    } else {
      return 0;
    }
  }
  return -1;
}

/*----------------------------------------------------------------------------*/
int
PlainLayout::sync() 
{
  return ofsFile->syncofs();
}


/*----------------------------------------------------------------------------*/
int
PlainLayout::stat(struct stat *buf) 
{
  return XrdOfsOss->Stat(ofsFile->fstPath.c_str(),buf);
}


/*----------------------------------------------------------------------------*/
int
PlainLayout::close()
{
  return ofsFile->closeofs();
}

/*----------------------------------------------------------------------------*/
int
PlainLayout::remove()
{
  return ::unlink(LocalReplicaPath.c_str());
}

/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_END
