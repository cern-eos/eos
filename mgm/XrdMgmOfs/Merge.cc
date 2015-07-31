// ----------------------------------------------------------------------
// File: Merge.cc
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


// -----------------------------------------------------------------------
// This file is included source code in XrdMgmOfs.cc to make the code more
// transparent without slowing down the compilation time.
// -----------------------------------------------------------------------

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::merge (
                  const char* src,
                  const char* dst,
                  XrdOucErrInfo &error,
                  eos::common::Mapping::VirtualIdentity & vid
                  )
/*----------------------------------------------------------------------------*/
/**
 * @brief merge one file into another one
 * @param src to merge
 * @param dst to merge into
 * @return SFS_OK if success
 *
 * This command act's like a rename and keeps the ownership and creation time
 * of the target file.
 */
/*----------------------------------------------------------------------------*/
{
  eos::common::Mapping::VirtualIdentity rootvid;
  eos::common::Mapping::Root(rootvid);
  eos::IFileMD* src_fmd = 0;
  eos::IFileMD* dst_fmd = 0;

  if (!src || !dst)
  {
    return Emsg("merge", error, EINVAL, "merge source into destination path - source or target missing");
  }

  std::string src_path = src;
  std::string dst_path = dst;

  {
    eos::common::RWMutexWriteLock(gOFS->eosViewRWMutex);
    try
    {
      src_fmd = gOFS->eosView->getFile(src_path);
      dst_fmd = gOFS->eosView->getFile(dst_path);
      
      // -------------------------------------------------------------------------
      // inherit some core meta data, the checksum must be right by construction,
      // so we don't copy it
      // -------------------------------------------------------------------------
      
      // inherit the previous ownership
      src_fmd->setCUid(dst_fmd->getCUid());
      src_fmd->setCGid(dst_fmd->getCGid());
      // inherit the creation time
      eos::IFileMD::ctime_t ctime;
      dst_fmd->getCTime(ctime);
      src_fmd->setCTime(ctime);
      // change the owner of the source file
      eosView->updateFileStore(src_fmd);
    }
    catch (eos::MDException &e)
    {
      errno = e.getErrno();
      eos_debug("caught exception %d %s\n",
		e.getErrno(),
		e.getMessage().str().c_str());
    }
  }

  int rc = SFS_OK;

  if (src_fmd && dst_fmd)
  {
    // remove the destination file
    rc |= gOFS->_rem(dst_path.c_str(),
                     error,
                     rootvid,
                     "");

    // rename the source to destination
    rc |= gOFS->_rename(src_path.c_str(),
                        dst_path.c_str(),
                        error,
                        rootvid,
                        "",
                        "",
                        true,
                        false
                        );
  }
  else
  {

    return Emsg("merge", error, EINVAL, "merge source into destination path - "
                "cannot get file meta data ", src_path.c_str());
  }
  return rc;
}
