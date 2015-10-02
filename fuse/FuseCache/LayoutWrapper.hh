//------------------------------------------------------------------------------
// File LayoutWrapper.hh
// Author: Geoffray Adde <geoffray.adde@cern.ch> CERN
//------------------------------------------------------------------------------

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

#ifndef __EOS_FUSE_LAYOUTWRAPPER_HH__
#define __EOS_FUSE_LAYOUTWRAPPER_HH__

#include "FileAbstraction.hh"

// forward declaration
class FileAbstraction;

//------------------------------------------------------------------------------
//! Class that that wraps a FileLayout to keep track of change times and to
//  be able to close the layout and be able to automatically reopen it if it's needed
//  This is a trick needed because the flush function if the fuse API can be called
//  several times and it's not clear if the file can still be used in between
//  those times.
//------------------------------------------------------------------------------
class LayoutWrapper
{
public:
  eos::fst::Layout* mFile;
  bool mOpen;
  bool mHasWrite;
  std::string mPath;
  XrdSfsFileOpenMode mFlags;
  mode_t mMode;
  std::string mOpaque;
  FileAbstraction *fabs;
  timespec mLocalMtime, mLocalAtime;

  //--------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param file layout to be wrapped
  //!
  //--------------------------------------------------------------------------
  LayoutWrapper (eos::fst::Layout* file);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~LayoutWrapper ();

  //--------------------------------------------------------------------------
  //! Make sure that the file layout is open
  //! Reopen it if needed using (almost) the same argument as the previous open
  //--------------------------------------------------------------------------
  int MakeOpen ();

  //--------------------------------------------------------------------------
  //! overloading member functions of FileLayout class
  //--------------------------------------------------------------------------
  const char*
  GetName ();

  //--------------------------------------------------------------------------
  //! overloading member functions of FileLayout class
  //--------------------------------------------------------------------------
  const char*
  GetLocalReplicaPath ();

  //--------------------------------------------------------------------------
  //! overloading member functions of FileLayout class
  //--------------------------------------------------------------------------
  unsigned int
  GetLayoutId ();

  //--------------------------------------------------------------------------
  //! overloading member functions of FileLayout class
  //--------------------------------------------------------------------------
  const std::string&
  GetLastUrl ();

  //--------------------------------------------------------------------------
  //! overloading member functions of FileLayout class
  //--------------------------------------------------------------------------
  bool
  IsEntryServer ();

  //--------------------------------------------------------------------------
  //! overloading member functions of FileLayout class
  //--------------------------------------------------------------------------
  int Open (const std::string& path, XrdSfsFileOpenMode flags, mode_t mode, const char* opaque);

  //--------------------------------------------------------------------------
  //! overloading member functions of FileLayout class
  //--------------------------------------------------------------------------
  int64_t Read (XrdSfsFileOffset offset, char* buffer, XrdSfsXferSize length, bool readahead = false);

  //--------------------------------------------------------------------------
  //! overloading member functions of FileLayout class
  //--------------------------------------------------------------------------
  int64_t ReadV (XrdCl::ChunkList& chunkList, uint32_t len);

  //--------------------------------------------------------------------------
  //! overloading member functions of FileLayout class
  //--------------------------------------------------------------------------
  int64_t Write (XrdSfsFileOffset offset, const char* buffer, XrdSfsXferSize length);

  //--------------------------------------------------------------------------
  //! overloading member functions of FileLayout class
  //--------------------------------------------------------------------------
  int Truncate (XrdSfsFileOffset offset);

  //--------------------------------------------------------------------------
  //! overloading member functions of FileLayout class
  //--------------------------------------------------------------------------
  int Sync ();

  //--------------------------------------------------------------------------
  //! overloading member functions of FileLayout class
  //--------------------------------------------------------------------------
  int Close ();

  //--------------------------------------------------------------------------
  //! overloading member functions of FileLayout class
  //--------------------------------------------------------------------------
  int Stat (struct stat* buf);

  //--------------------------------------------------------------------------
  //! Set atime and mtime at current time
  //--------------------------------------------------------------------------
  void UtimesNow ();
};

#endif
