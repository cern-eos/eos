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
#include "Bufferll.hh"

//! Forward declaration
class LayoutWrapper;
namespace eos {
  namespace fst {
    class AsyncLayoutOpenHandler;
  }
}

//------------------------------------------------------------------------------
// Class that wraps a FileLayout to keep track of change times and to be able to
// close the layout and be able to automatically reopen it if it's needed.
// This is a trick needed because the flush function if the fuse API can be
// called several times and it's not clear if the file can still be used in
// between those times.
//------------------------------------------------------------------------------
class LayoutWrapper
{
  friend class AsyncOpenHandler;
  friend class FileAbstraction;

  eos::fst::Layout* mFile;
  bool mOpen;
  bool mClose;
  std::string mPath;
  unsigned long long mInode;
  XrdSfsFileOpenMode mFlags;
  mode_t mMode;
  std::string mOpaque;
  std::string mLazyUrl;
  FileAbstraction* mFabs;
  timespec mLocalUtime[2];
  XrdSysMutex mMakeOpenMutex;
  std::shared_ptr<Bufferll> mCache;

  struct CacheEntry
  {
    std::shared_ptr<Bufferll> mCache;
    time_t mLifeTime;
    time_t mOwnerLifeTime;
    int64_t  mSize;
    bool mPartial;
    int64_t mRestoreInode;
  };

  static XrdSysMutex gCacheAuthorityMutex;
  static std::map<unsigned long long, LayoutWrapper::CacheEntry> gCacheAuthority;

  bool mCanCache;
  bool mCacheCreator;
  off_t mMaxOffset;
  int64_t mSize;
  bool mInlineRepair;
  bool mRestore;
  //----------------------------------------------------------------------------
  //! Do the open on the mgm but not on the fst yet
  //----------------------------------------------------------------------------
  int LazyOpen(const std::string& path, XrdSfsFileOpenMode flags, mode_t mode,
               const char* opaque, const struct stat* buf);


 public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param file layout to be wrapped
  //!
  //----------------------------------------------------------------------------
  LayoutWrapper(eos::fst::Layout* file);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~LayoutWrapper();

  //----------------------------------------------------------------------------
  //! Make sure that the file layout is open
  //! Reopen it if needed using (almost) the same argument as the previous open
  //----------------------------------------------------------------------------
  int MakeOpen();

  //----------------------------------------------------------------------------
  //! Overloading member functions of FileLayout class
  //----------------------------------------------------------------------------
  const char* GetName();

  //----------------------------------------------------------------------------
  //! return the path of the file
  //----------------------------------------------------------------------------
  inline const char* GetPath()
  {
    return mPath.c_str();
  }

  //----------------------------------------------------------------------------
  //! Overloading member functions of FileLayout class
  //---------------------------------------------------------------------------
  const char*
  GetLocalReplicaPath();

  //----------------------------------------------------------------------------
  //! Overloading member functions of FileLayout class
  //----------------------------------------------------------------------------
  unsigned int
  GetLayoutId();

  //----------------------------------------------------------------------------
  //! Overloading member functions of FileLayout class
  //----------------------------------------------------------------------------
  const std::string&
  GetLastUrl();

  //----------------------------------------------------------------------------
  //! Overloading member functions of FileLayout class
  //----------------------------------------------------------------------------
  bool
  IsEntryServer();

  //----------------------------------------------------------------------------
  //! Overloading member functions of FileLayout class
  //----------------------------------------------------------------------------
  int Open(const std::string& path, XrdSfsFileOpenMode flags, mode_t mode,
           const char* opaque, const struct stat* buf, bool doOpen = true,
           size_t creator_lifetime = 30, bool inlineRepair = false);

  //----------------------------------------------------------------------------
  //! Overloading member functions of FileLayout class
  //----------------------------------------------------------------------------
  int64_t CacheSize();

  //----------------------------------------------------------------------------
  //! Overloading member functions of FileLayout class
  //----------------------------------------------------------------------------
  int64_t Read(XrdSfsFileOffset offset, char* buffer, XrdSfsXferSize length,
               bool readahead = false);

  //----------------------------------------------------------------------------
  //! Overloading member functions of FileLayout class
  //----------------------------------------------------------------------------
  int64_t ReadCache(XrdSfsFileOffset offset, char* buffer, XrdSfsXferSize length,
                    off_t maxcache = (64 * 1024 * 1024));

  //----------------------------------------------------------------------------
  //! Overloading member functions of FileLayout class
  //----------------------------------------------------------------------------
  int64_t ReadV(XrdCl::ChunkList& chunkList, uint32_t len);

  //----------------------------------------------------------------------------
  //! Overloading member functions of FileLayout class
  //----------------------------------------------------------------------------
  int64_t Write(XrdSfsFileOffset offset, const char* buffer,
                XrdSfsXferSize length, bool touchMtime = true);

  //----------------------------------------------------------------------------
  //! Overloading member functions of FileLayout class
  //----------------------------------------------------------------------------
  int64_t WriteCache(XrdSfsFileOffset offset, const char* buffer,
                     XrdSfsXferSize length, off_t maxcache = (64 * 1024 * 1024));

  //----------------------------------------------------------------------------
  //! Size known after open if this file was created here
  //----------------------------------------------------------------------------
  int64_t Size()
  {
    return mSize;
  }

  //----------------------------------------------------------------------------
  //! Overloading member functions of FileLayout class
  //----------------------------------------------------------------------------
  int Truncate(XrdSfsFileOffset offset, bool touchMtime = true);

  //----------------------------------------------------------------------------
  //! Overloading member functions of FileLayout class
  //----------------------------------------------------------------------------
  int Sync();

  //----------------------------------------------------------------------------
  //! Overloading member functions of FileLayout class
  //----------------------------------------------------------------------------
  int Close();

  //----------------------------------------------------------------------------
  //! Overloading member functions of FileLayout class
  //----------------------------------------------------------------------------
  int Stat(struct stat* buf);

  //----------------------------------------------------------------------------
  //! Set atime and mtime according to argument without commit at file closure
  //----------------------------------------------------------------------------
  void Utimes(const struct stat* buf);

  //----------------------------------------------------------------------------
  //! Get Last Opened Path
  //----------------------------------------------------------------------------
  std::string GetLastPath();

  //----------------------------------------------------------------------------
  //! Is the file Opened
  //----------------------------------------------------------------------------
  bool IsOpen();

  //----------------------------------------------------------------------------
  //! Repair a partially unavailable flie
  //----------------------------------------------------------------------------
  bool Repair(const std::string& path, const char* opaque);

  //----------------------------------------------------------------------------
  //! Restore a file from the cache into EOS
  //----------------------------------------------------------------------------
  bool Restore();
  
  //----------------------------------------------------------------------------
  //! Enable the restore flag when closing the file
  //----------------------------------------------------------------------------
  void SetRestore() { mRestore = true; }

  //----------------------------------------------------------------------------
  //! Migrate cache inode after a restore operation
  //----------------------------------------------------------------------------
  static unsigned long long CacheRestore(unsigned long long);

  //----------------------------------------------------------------------------
  //! Path accessor
  //----------------------------------------------------------------------------
  inline const std::string& GetOpenPath() const
  {
    return mPath;
  }

  //----------------------------------------------------------------------------
  //! Open Flags accessors
  //----------------------------------------------------------------------------
  inline const XrdSfsFileOpenMode& GetOpenFlags() const
  {
    return mFlags;
  }

  //----------------------------------------------------------------------------
  //! Utility function to import (key,value) from a cgi string to a map
  //----------------------------------------------------------------------------
  static bool ImportCGI(std::map<std::string, std::string>& m,
                        const std::string& cgi);

  //----------------------------------------------------------------------------
  //! Utility function to write the content of a(key,value) map to a cgi string
  //----------------------------------------------------------------------------
  static bool ToCGI(const std::map<std::string, std::string>& m ,
                    std::string& cgi);

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  static long long CacheAuthSize(unsigned long long);

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  static void CacheRemove(unsigned long long);

  //----------------------------------------------------------------------------
  //! Check if we can cache ..
  //----------------------------------------------------------------------------
  bool CanCache() const
  {
    return mCanCache;
  }
  
  //----------------------------------------------------------------------------
  //! Return FUSE inode
  //----------------------------------------------------------------------------
  unsigned long long GetInode()
  {
    return eos::common::FileId::FidToInode(mInode);
  }


 private:
  bool mDoneAsyncOpen; ///< Mark if async open was issued
  eos::fst::AsyncLayoutOpenHandler* mOpenHandler; ///< Asynchronous open handler
};

#endif
