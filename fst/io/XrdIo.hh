//------------------------------------------------------------------------------
//! @file XrdIo.hh
//! @author Elvin-Alin Sindrilaru - CERN
//! @brief Class used for doing remote IO operations unsing the xrd client
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

#ifndef __EOSFST_XRDFILEIO_HH__
#define __EOSFST_XRDFILEIO_HH__

/*----------------------------------------------------------------------------*/
#include "fst/io/FileIo.hh"
#include "fst/io/SimpleHandler.hh"
#include "common/FileMap.hh"
/*----------------------------------------------------------------------------*/
#include "XrdCl/XrdClFile.hh"
/*----------------------------------------------------------------------------*/
#include <queue>

EOSFSTNAMESPACE_BEGIN

//! Forward declarations
class AsyncMetaHandler;
struct ReadaheadBlock;

typedef std::map<uint64_t, ReadaheadBlock*> PrefetchMap;

//------------------------------------------------------------------------------
//! Struct that holds a readahead buffer and corresponding handler
//------------------------------------------------------------------------------

struct ReadaheadBlock {
  static const uint64_t sDefaultBlocksize; ///< default value for readahead

  //----------------------------------------------------------------------------
  //! Constuctor
  //!
  //! @param blocksize the size of the readahead
  //!
  //----------------------------------------------------------------------------

  ReadaheadBlock (uint64_t blocksize = sDefaultBlocksize)
  {
    buffer = new char[blocksize];
    handler = new SimpleHandler();
  }


  //----------------------------------------------------------------------------
  //! Update current request
  //!
  //! @param offset offset
  //! @param length length
  //! @param isWrite true if write request, otherwise false
  //!
  //----------------------------------------------------------------------------

  void Update (uint64_t offset, uint32_t length, bool isWrite)
  {
    handler->Update(offset, length, isWrite);
  }


  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------

  virtual ~ReadaheadBlock ()
  {
    delete[] buffer;
    delete handler;
  }

  char* buffer; ///< pointer to where the data is read
  SimpleHandler* handler; ///< async handler for the requests
};


//------------------------------------------------------------------------------
//! Class used for doing remote IO operations using the Xrd client
//------------------------------------------------------------------------------

class XrdIo : public FileIo {
public:

  static const uint32_t sNumRdAheadBlocks; ///< no. of blocks used for readahead

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param handle to logical file
  //! @param client security entity
  //! @param error error information
  //!
  //----------------------------------------------------------------------------
  XrdIo ();


  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~XrdIo ();


  //----------------------------------------------------------------------------
  //! Open file
  //!
  //! @param path file path
  //! @param flags open flags
  //! @param mode open mode
  //! @param opaque opaque information
  //! @param timeout timeout value
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //!
  //----------------------------------------------------------------------------
  virtual int Open (const std::string& path,
                    XrdSfsFileOpenMode flags,
                    mode_t mode = 0,
                    const std::string& opaque = "",
                    uint16_t timeout = 0);


  //----------------------------------------------------------------------------
  //! Read from file - sync
  //!
  //! @param offset offset in file
  //! @param buffer where the data is read
  //! @param length read length
  //! @param timeout timeout value
  //!
  //! @return number of bytes read or -1 if error
  //!
  //----------------------------------------------------------------------------
  virtual int64_t Read (XrdSfsFileOffset offset,
                        char* buffer,
                        XrdSfsXferSize length,
                        uint16_t timeout = 0);


  //--------------------------------------------------------------------------
  //! Write to file - sync
  //!
  //! @param offset offset
  //! @param buffer data to be written
  //! @param length length
  //! @param timeout timeout value
  //!
  //! @return number of bytes written or -1 if error
  //!
  //--------------------------------------------------------------------------
  virtual int64_t Write (XrdSfsFileOffset offset,
                         const char* buffer,
                         XrdSfsXferSize length,
                         uint16_t timeout = 0);


  //--------------------------------------------------------------------------
  //! Read from file - async
  //!
  //! @param offset offset in file
  //! @param buffer where the data is read
  //! @param length read length
  //! @param readahead true if readahead is to be enabled, otherwise false
  //! @param timeout timeout value
  //!
  //! @return number of bytes read or -1 if error
  //!
  //--------------------------------------------------------------------------
  virtual int64_t ReadAsync (XrdSfsFileOffset offset,
                             char* buffer,
                             XrdSfsXferSize length,
                             bool readahead = false,
                             uint16_t timeout = 0);


  //--------------------------------------------------------------------------
  //! Write to file - async
  //!
  //! @param offset offset
  //! @param buffer data to be written
  //! @param length length
  //! @param timeout timeout value
  //!
  //! @return number of bytes written or -1 if error
  //!
  //--------------------------------------------------------------------------
  virtual int64_t WriteAsync (XrdSfsFileOffset offset,
                              const char* buffer,
                              XrdSfsXferSize length,
                              uint16_t timeout = 0);


  //--------------------------------------------------------------------------
  //! Truncate
  //!
  //! @param offset truncate file to this value
  //! @param timeout timeout value
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //!
  //--------------------------------------------------------------------------
  virtual int Truncate (XrdSfsFileOffset offset,
                        uint16_t timeout = 0);


  //--------------------------------------------------------------------------
  //! Remove file
  //!
  //! @param timeout timeout value
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //!
  //--------------------------------------------------------------------------
  virtual int Remove (uint16_t timeout = 0);


  //--------------------------------------------------------------------------
  //! Sync file to disk
  //!
  //! @param timeout timeout value
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //!
  //--------------------------------------------------------------------------
  virtual int Sync (uint16_t timeout = 0);


  //--------------------------------------------------------------------------
  //! Close file
  //!
  //! @param timeout timeout value
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //!
  //--------------------------------------------------------------------------
  virtual int Close (uint16_t timeout = 0);


  //--------------------------------------------------------------------------
  //! Get stats about the file
  //!
  //! @param buf stat buffer
  //! @param timeout timeout value
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //!
  //--------------------------------------------------------------------------
  virtual int Stat (struct stat* buf, uint16_t timeout = 0);

  //--------------------------------------------------------------------------
  //! Check for the existance of a file
  //!
  //! @param path to the file
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //--------------------------------------------------------------------------
  virtual int Exists (const char* url);

  //--------------------------------------------------------------------------                                                                                                                                 //! Delete a file
  //!
  //! @param path to the file to be deleted
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //--------------------------------------------------------------------------
  virtual int Delete (const char* url);

  //--------------------------------------------------------------------------
  //! Get pointer to async meta handler object
  //!
  //! @return pointer to async handler, NULL otherwise
  //!
  //--------------------------------------------------------------------------
  virtual void* GetAsyncHandler ();


  //--------------------------------------------------------------------------
  //! Plug-in function to fill a statfs structure about the storage filling
  //! state
  //! @param path to statfs
  //! @param statfs return struct
  //! @return 0 if successful otherwise errno
  //--------------------------------------------------------------------------

  int Statfs (const char* path, struct statfs* statFs);

  //--------------------------------------------------------------------------
  //! Class implementing extended attribute support
  //--------------------------------------------------------------------------

  class Attr : public eos::common::Attr, public eos::common::LogId {
  private:
    std::string mUrl;
    eos::common::FileMap mFileMap; //< extended attribute file map
  public:
    // ------------------------------------------------------------------------
    //! Set a binary attribute (name has to start with 'user.' !!!)
    // ------------------------------------------------------------------------
    bool Set (const char* name, const char* value, size_t len);

    // ------------------------------------------------------------------------
    //! Set a string attribute (name has to start with 'user.' !!!)
    // ------------------------------------------------------------------------
    bool Set (std::string key, std::string value);

    // ------------------------------------------------------------------------
    //! Get a binary attribute by name (name has to start with 'user.' !!!)
    // ------------------------------------------------------------------------
    bool Get (const char* name, char* value, size_t &size);

    // ------------------------------------------------------------------------
    //! Get a string attribute by name (name has to start with 'user.' !!!)
    // ------------------------------------------------------------------------
    std::string Get (std::string name);

    // ------------------------------------------------------------------------
    //! Factory function to create an attribute object
    // ------------------------------------------------------------------------
    static Attr* OpenAttr (const char* url);

    // ------------------------------------------------------------------------
    //! Non static Factory function to create an attribute object
    // ------------------------------------------------------------------------
    Attr* OpenAttribute (const char* path);

    // ------------------------------------------------------------------------
    // Get URL to the attribute file
    // ------------------------------------------------------------------------

    std::string GetUrl ()
    {
      return mUrl;
    }

    // ------------------------------------------------------------------------
    // Constructor
    // ------------------------------------------------------------------------

    Attr () : mUrl ("")
    {
    }

    Attr (const char* path);

    virtual ~Attr ()
    {
    }
  };

  //--------------------------------------------------------------------------
  //! traversing filesystem/storage routines
  //--------------------------------------------------------------------------

  //--------------------------------------------------------------------------
  //! FTS search handle
  //--------------------------------------------------------------------------

  class FtsHandle {
    friend class XrdIo;
  protected:
    std::vector< std::vector<std::string> > found_dirs;
    std::deque< std::string> found_files;
    size_t deepness;
  public:

    FtsHandle (const char* dirp)
    {
      found_dirs.resize(1);
      deepness = 0;
    };

    virtual ~FtsHandle ()
    {
    }
  };

  //--------------------------------------------------------------------------
  //! Open a curser to traverse a storage system
  //! @param subtree where to start traversing
  //! @return returns implementation dependent handle or 0 in case of error
  //--------------------------------------------------------------------------
  FileIo::FtsHandle* ftsOpen (std::string subtree);

  //--------------------------------------------------------------------------
  //! Return the next path related to a traversal cursor obtained with ftsOpen
  //! @param fts_handle cursor obtained by ftsOpen
  //! @return returns implementation dependent handle or 0 in case of error
  //--------------------------------------------------------------------------
  std::string ftsRead (FileIo::FtsHandle* fts_handle);

  //--------------------------------------------------------------------------
  //! Close a traversal cursor
  //! @param fts_handle cursor to close
  //! @return 0 if fts_handle was an open cursor, otherwise -1
  //--------------------------------------------------------------------------
  virtual int ftsClose (FileIo::FtsHandle* fts_handle);

private:

  bool mDoReadahead; ///< mark if readahead is enabled
  uint32_t mBlocksize; ///< block size for rd/wr opertations
  std::string mPath; ///< path to file
  XrdCl::File* mXrdFile; ///< handler to xrd file
  AsyncMetaHandler* mMetaHandler; ///< async requests meta handler
  PrefetchMap mMapBlocks; ///< map of block read/prefetched
  std::queue<ReadaheadBlock*> mQueueBlocks; ///< queue containing available blocks
  XrdSysMutex mPrefetchMutex; ///< mutex to serialise the prefetch step

  //--------------------------------------------------------------------------
  //! Method used to prefetch the next block using the readahead mechanism
  //!
  //! @param offset begin offset of the current block we are reading
  //! @param isWrite true if block is for write, false otherwise
  //! @param timeout timeout value
  //!
  //! @return true if prefetch request was sent, otherwise false
  //!
  //--------------------------------------------------------------------------
  bool PrefetchBlock (int64_t offset,
                      bool isWrite,
                      uint16_t timeout = 0);


  //--------------------------------------------------------------------------
  //! Try to find a block in cache with contains the provided offset
  //!
  //! @param offset offset to be searched for
  //!
  //! @return iterator to the block containing the offset or if no such block
  //!         is found we return the iterator to the end of the map
  //!
  //--------------------------------------------------------------------------
  PrefetchMap::iterator FindBlock (uint64_t offset);


  //--------------------------------------------------------------------------
  //! Download a remote file into a string object
  //! @param url from where to download
  //! @param download string where to place the contents
  //! @return 0 success, otherwise -1 and errno
  //--------------------------------------------------------------------------
  static int Download (std::string url, std::string& download);

  //--------------------------------------------------------------------------
  //! Upload a string object into a remote file
  //! @param url from where to upload
  //! @param upload string to store into remote file
  //! @return 0 success, otherwise -1 and errno
  //--------------------------------------------------------------------------
  static int Upload (std::string url, std::string& upload);

  //--------------------------------------------------------------------------
  //! Get a directory listing - taken from XrdCl sources
  //--------------------------------------------------------------------------

  XrdCl::XRootDStatus GetDirList (XrdCl::FileSystem *fs,
                                  const XrdCl::URL &url,
                                  std::vector<std::string> *files,
                                  std::vector<std::string> *directories);

  //--------------------------------------------------------------------------
  //! Disable copy constructor
  //--------------------------------------------------------------------------
  XrdIo (const XrdIo&) = delete;


  //--------------------------------------------------------------------------
  //! Disable assign operator
  //--------------------------------------------------------------------------
  XrdIo& operator = (const XrdIo&) = delete;

};



EOSFSTNAMESPACE_END

#endif  // __EOSFST_XRDFILEIO_HH__

