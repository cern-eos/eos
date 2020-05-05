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

#include "fst/io/FileIo.hh"
#include "fst/io/SimpleHandler.hh"
#include "common/FileMap.hh"
#include "common/XrdConnPool.hh"
#include "XrdCl/XrdClFile.hh"
#include <queue>

EOSFSTNAMESPACE_BEGIN

//! Forward declarations
class XrdIo;
class AsyncMetaHandler;
struct ReadaheadBlock;

//------------------------------------------------------------------------------
//! Class used for handling asynchronous open responses
//------------------------------------------------------------------------------
class AsyncIoOpenHandler: public XrdCl::ResponseHandler,
  public eos::common::LogId
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param io_file file object
  //! @param layout_handler handler for the layout object
  //----------------------------------------------------------------------------
  AsyncIoOpenHandler(XrdIo* io_file, XrdCl::ResponseHandler* layout_handler) :
    mFileIO(io_file), mLayoutOpenHandler(layout_handler) {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~AsyncIoOpenHandler() {}

  //----------------------------------------------------------------------------
  //! Called when a response to associated request arrives or an error  occurs
  //!
  //! @param status   status of the request
  //! @param response an object associated with the response (request dependent)
  //! @param hostList list of hosts the request was redirected to
  //---------------------------------------------------------------------------
  virtual void HandleResponseWithHosts(XrdCl::XRootDStatus* status,
                                       XrdCl::AnyObject* response,
                                       XrdCl::HostList* hostList);

private:
  XrdIo* mFileIO; ///< File IO object corresponding to this handler
  XrdCl::ResponseHandler* mLayoutOpenHandler; ///< Open handler for the layout
};

typedef std::map<uint64_t, ReadaheadBlock*> PrefetchMap;

//------------------------------------------------------------------------------
//! Struct that holds a readahead buffer and corresponding handler
//------------------------------------------------------------------------------
struct ReadaheadBlock {

  //----------------------------------------------------------------------------
  //! Constuctor
  //!
  //! @param blocksize the size of the readahead
  //----------------------------------------------------------------------------
  ReadaheadBlock(uint64_t blocksize)
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
  //----------------------------------------------------------------------------
  void Update(uint64_t offset, uint32_t length, bool isWrite)
  {
    handler->Update(offset, length, isWrite);
  }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~ReadaheadBlock()
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
class XrdIo : public FileIo
{
  friend class AsyncIoOpenHandler;
public:
  //----------------------------------------------------------------------------
  //! InitBlocksize
  //!
  //! @return : block size that should be used to initialize
  //!           the mDefaultBlockSize
  //----------------------------------------------------------------------------
  static uint64_t InitBlocksize()
  {
    char* ptr = getenv("EOS_FST_XRDIO_BLOCK_SIZE");
    //default is 1M if the envar is not set
    return (ptr ? strtoul(ptr, 0, 10) : 1024 * 1024ull);
  }

  //----------------------------------------------------------------------------
  //! InitInitNumRdAheadBlocks
  //!
  //! @return : number of blocks that should be read ahead
  //----------------------------------------------------------------------------
  static uint32_t InitNumRdAheadBlocks()
  {
    char* ptr = getenv("EOS_FST_XRDIO_RDAHEAD_BLOCKS");
    // default is 2 if envar is not set
    return (ptr ? strtoul(ptr, 0, 10) : 2ul);
  }

  //----------------------------------------------------------------------------
  //! GetDefaultBlocksize
  //!
  //! @return : default block size
  //----------------------------------------------------------------------------
  int32_t GetBlockSize()
  {
    return mBlocksize;
  }

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param path path or URI for the file
  //----------------------------------------------------------------------------
  XrdIo(std::string path);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~XrdIo();

  //----------------------------------------------------------------------------
  //! Open file - synchronously
  //!
  //! @param flags open flags
  //! @param mode open mode
  //! @param opaque opaque information
  //! @param timeout timeout value
  //!
  //! @return 0 if successful, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  int fileOpen(XrdSfsFileOpenMode flags, mode_t mode = 0,
               const std::string& opaque = "", uint16_t timeout = 0);

  //----------------------------------------------------------------------------
  //! Open file - asynchronously. This call is to be used from one of the file
  //! layout classes and not on its own, as there is not mechanims buit into
  //! this class to wait for the response.
  //!
  //! @param handler handler called asynchronously
  //! @param flags open flags
  //! @param mode open mode
  //! @param opaque opaque information
  //! @param timeout timeout value
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  virtual int fileOpenAsync(void* io_handler, XrdSfsFileOpenMode flags,
                            mode_t mode = 0, const std::string& opaque = "",
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
  //----------------------------------------------------------------------------
  int64_t fileRead(XrdSfsFileOffset offset, char* buffer,
                   XrdSfsXferSize length, uint16_t timeout = 0);

  //----------------------------------------------------------------------------
  //! Write to file - sync
  //!
  //! @param offset offset
  //! @param buffer data to be written
  //! @param length length
  //! @param timeout timeout value
  //!
  //! @return number of bytes written or -1 if error
  //----------------------------------------------------------------------------
  int64_t fileWrite(XrdSfsFileOffset offset, const char* buffer,
                    XrdSfsXferSize length, uint16_t timeout = 0);

  //----------------------------------------------------------------------------
  //! Vector read - sync
  //!
  //! @param chunkList list of chunks for the vector read
  //! @param timeout timeout value
  //!
  //! @return number of bytes read of -1 if error
  //----------------------------------------------------------------------------
  virtual int64_t fileReadV(XrdCl::ChunkList& chunkList,
                            uint16_t timeout = 0);

  //----------------------------------------------------------------------------
  //! Vector read - async
  //!
  //! @param chunkList list of chunks for the vector read
  //! @param timeout timeout value
  //!
  //! @return 0(SFS_OK) if request successfully sent, otherwise -1(SFS_ERROR)
  //----------------------------------------------------------------------------
  virtual int64_t fileReadVAsync(XrdCl::ChunkList& chunkList,
                                 uint16_t timeout = 0);

  //----------------------------------------------------------------------------
  //! Read from file - async
  //!
  //! @param offset offset in file
  //! @param buffer where the data is read
  //! @param length read length
  //! @param readahead set if readahead is to be used
  //! @param timeout timeout value
  //!
  //! @return number of bytes read or -1 if error
  //! @note The buffer given by the user is not neccessarily populated with
  //!       any meaningful data when this function returns. The user should call
  //!       fileWaitAsyncIO to enforce this guarantee.
  //----------------------------------------------------------------------------
  int64_t fileReadAsync(XrdSfsFileOffset offset, char* buffer,
                        XrdSfsXferSize length, bool readahead = false,
                        uint16_t timeout = 0);

  //----------------------------------------------------------------------------
  //! Write to file - async
  //!
  //! @param offset offset
  //! @param buffer data to be written
  //! @param length length
  //! @param timeout timeout value
  //!
  //! @return number of bytes written or -1 if error
  //----------------------------------------------------------------------------
  int64_t fileWriteAsync(XrdSfsFileOffset offset, const char* buffer,
                         XrdSfsXferSize length, uint16_t timeout = 0);

  //--------------------------------------------------------------------------
  //! Wait for all async IO
  //!
  //! @return global return code of async IO
  //--------------------------------------------------------------------------
  virtual int fileWaitAsyncIO();

  //----------------------------------------------------------------------------
  //! Truncate
  //!
  //! @param offset truncate file to this value
  //! @param timeout timeout value
  //!
  //! @return 0 if successful, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  int fileTruncate(XrdSfsFileOffset offset, uint16_t timeout = 0);

  //----------------------------------------------------------------------------
  //! Allocate file space
  //!
  //! @param length space to be allocated
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  int fileFallocate(XrdSfsFileOffset length)
  {
    return 0;
  }

  //----------------------------------------------------------------------------
  //! Deallocate file space
  //!
  //! @param fromOffset offset start
  //! @param toOffset offset end
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  int fileFdeallocate(XrdSfsFileOffset fromOffset, XrdSfsFileOffset toOffset)
  {
    return 0;
  }

  //----------------------------------------------------------------------------
  //! Remove file
  //!
  //! @param timeout timeout value
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  int fileRemove(uint16_t timeout = 0);

  //----------------------------------------------------------------------------
  //! Delete not openedfile
  //!
  //! @param timeout timeout value
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  int fileDelete(const char* url);

  //----------------------------------------------------------------------------
  //! Sync file to disk
  //!
  //! @param timeout timeout value
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  int fileSync(uint16_t timeout = 0);

  //----------------------------------------------------------------------------
  //! Get pointer to async meta handler object
  //!
  //! @return pointer to async handler, NULL otherwise
  //----------------------------------------------------------------------------
  void* fileGetAsyncHandler();

  //----------------------------------------------------------------------------
  //! Check for the existence of a file
  //!
  //! @param path to the file
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  int fileExists();

  //----------------------------------------------------------------------------
  //! Close file
  //!
  //! @param timeout timeout value
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  int fileClose(uint16_t timeout = 0);

  //----------------------------------------------------------------------------
  //! Get stats about the file
  //!
  //! @param buf stat buffer
  //! @param timeout timeout value
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  int fileStat(struct stat* buf, uint16_t timeout = 0);

  //----------------------------------------------------------------------------
  //! Execute implementation dependant commands
  //!
  //! @param buf stat buffer
  //! @param timeout timeout value
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  int fileFctl(const std::string& cmd, uint16_t timeout = 0);

  //----------------------------------------------------------------------------
  //! Clean read cache
  //----------------------------------------------------------------------------
  virtual void CleanReadCache();

  //----------------------------------------------------------------------------
  //! Set a binary attribute (name has to start with 'user.' !!!)
  //!
  //! @param name attribute name
  //! @param value attribute value
  //! @param len value length
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  int attrSet(const char* name, const char* value, size_t len);

  //----------------------------------------------------------------------------
  //! Set a binary attribute (name has to start with 'user.' !!!)
  //!
  //! @param name attribute name
  //! @param value attribute value
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  int attrSet(string name, std::string value);

  //----------------------------------------------------------------------------
  //! Get a binary attribute by name
  //!
  //! @param name attribute name
  //! @param value contains attribute value upon success
  //! @param size the buffer size, after success the value size
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  int attrGet(const char* name, char* value, size_t& size);

  //----------------------------------------------------------------------------
  //! Get a binary attribute by name
  //!
  //! @param name attribute name
  //! @param value contains attribute value upon success
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  int attrGet(string name, std::string& value);

  //----------------------------------------------------------------------------
  //! Delete a binary attribute by name
  //!
  //! @param name attribute name
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  int attrDelete(const char* name);

  //----------------------------------------------------------------------------
  //! List all attributes for the associated path
  //!
  //! @param list contains all attribute names for the set path upon success
  //!
  //! @return 0 on success, -1 otherwise and error code is set
  //----------------------------------------------------------------------------
  int attrList(std::vector<std::string>& list);

  //----------------------------------------------------------------------------
  //! Set attribute synchronization mode
  //!
  //! @param on if true - every set attributes runs 'pull-modify-push',
  //!        otherwise the destructor finished a 'pull-modify-modify-....-push'
  //!        sequence
  //----------------------------------------------------------------------------
  void setAttrSync(bool mode = false)
  {
    mAttrSync = mode;
  }

  //----------------------------------------------------------------------------
  //! Plug-in function to fill a statfs structure about the storage filling
  //! state
  //!
  //! @param path to statfs
  //! @param statfs return struct
  //!
  //! @return 0 if successful otherwise errno
  //----------------------------------------------------------------------------
  int Statfs(struct statfs* statFs);

  //----------------------------------------------------------------------------
  //! Traversing filesystem/storage routines - FTS search handle
  //----------------------------------------------------------------------------
  class FtsHandle
  {
    friend class XrdIo;
  protected:
    std::vector< std::vector<std::string> > found_dirs;
    std::deque< std::string> found_files;
    size_t deepness;

  public:
    FtsHandle(const char* dirp)
    {
      found_dirs.resize(1);
      deepness = 0;
    };

    virtual ~FtsHandle() = default;
  };

  //----------------------------------------------------------------------------
  //! Open a curser to traverse a storage system
  //!
  //! @param subtree where to start traversing
  //!
  //! @return returns implementation dependent handle or 0 in case of error
  //----------------------------------------------------------------------------
  FileIo::FtsHandle* ftsOpen();

  //----------------------------------------------------------------------------
  //! Return the next path related to a traversal cursor obtained with ftsOpen
  //!
  //! @param fts_handle cursor obtained by ftsOpen
  //!
  //! @return returns implementation dependent handle or 0 in case of error
  //----------------------------------------------------------------------------
  std::string ftsRead(FileIo::FtsHandle* fts_handle);

  //----------------------------------------------------------------------------
  //! Close a traversal cursor
  //!
  //! @param fts_handle cursor to close
  //!
  //! @return 0 if fts_handle was an open cursor, otherwise -1
  //----------------------------------------------------------------------------
  virtual int ftsClose(FileIo::FtsHandle* fts_handle);

#ifdef IN_TEST_HARNESS
public:
#else
private:
#endif
  static eos::common::XrdConnPool mXrdConnPool; ///< Xrd connection pool
  bool mDoReadahead; ///< mark if readahead is enabled
  const uint32_t mNumRdAheadBlocks; ///< no. of blocks used for readahead
  int32_t mBlocksize; ///< block size for rd/wr opertations
  XrdCl::File* mXrdFile; ///< handler to xrd file
  AsyncMetaHandler* mMetaHandler; ///< async requests meta handler
  PrefetchMap mMapBlocks; ///< map of block read/prefetched
  std::queue<ReadaheadBlock*> mQueueBlocks; ///< queue containing available blocks
  XrdSysMutex mPrefetchMutex; ///< mutex to serialise the prefetch step
  eos::common::FileMap mFileMap; ///< extended attribute file map
  std::string mAttrUrl; ///< extended attribute url
  std::string mOpaque; ///< opaque tags in original url
  bool mAttrLoaded; ///< mark if remote attributes have been loaded
  bool mAttrDirty; ///< mark if local attr modfied and not committed
  bool mAttrSync; ///< mark if attributes are updated synchronously
  XrdCl::URL mTargetUrl; ///< URL used to avoid physical connection sharing
  ///< RAAI helper for connection ids
  std::unique_ptr<eos::common::XrdConnIdHelper> mXrdIdHelper;
  XrdCl::XRootDStatus mWriteStatus;
  uint64_t mPrefetchOffset; ///< Last block offset of a prefetch hit
  uint64_t mPrefetchHits; ///< Number of prefetch hits
  uint64_t mPrefetchBlocks; ///< Number of prefetched blocks

  //----------------------------------------------------------------------------
  //! Method used to prefetch the next block using the readahead mechanism
  //!
  //! @param offset begin offset of the current block we are reading
  //! @param timeout timeout value
  //!
  //! @return true if prefetch request was sent, otherwise false
  //----------------------------------------------------------------------------
  bool PrefetchBlock(int64_t offset, uint16_t timeout = 0);

  //----------------------------------------------------------------------------
  //! Try to find a block in cache with contains the provided offset
  //!
  //! @param offset offset to be searched for
  //!
  //! @return iterator to the block containing the offset or if no such block
  //!         is found we return the iterator to the end of the map
  //----------------------------------------------------------------------------
  PrefetchMap::iterator FindBlock(uint64_t offset);

  //------------------------------------------------------------------------------
  //! Recycle blocks from the map that are not useful since the current offset
  //! is already grater then their offset
  //!
  //! @param iter iterator in the map of prefetched blocks
  //------------------------------------------------------------------------------
  void RecycleBlocks(std::map<uint64_t, ReadaheadBlock*>::iterator iter);

  //----------------------------------------------------------------------------
  //! Download a remote file into a string object
  //!
  //! @param url from where to download
  //! @param download string where to place the contents
  //!
  //! @return 0 success, otherwise -1 and errno
  //----------------------------------------------------------------------------
  static int Download(std::string url, std::string& download);

  //----------------------------------------------------------------------------
  //! Upload a string object into a remote file
  //!
  //! @param url from where to upload
  //! @param upload string to store into remote file
  //!
  //! @return 0 success, otherwise -1 and errno
  //----------------------------------------------------------------------------
  static int Upload(std::string url, std::string& upload);

  //----------------------------------------------------------------------------
  //! Get a directory listing - taken from XrdCl sources
  //----------------------------------------------------------------------------
  XrdCl::XRootDStatus GetDirList(XrdCl::FileSystem* fs,
                                 const XrdCl::URL& url,
                                 std::vector<std::string>* files,
                                 std::vector<std::string>* directories);

  //----------------------------------------------------------------------------
  //! Process opaque info
  //!
  //! @param opaque input opaque information
  //! @param out output containing the path with any additional opaque info
  //----------------------------------------------------------------------------
  void ProcessOpaqueInfo(const std::string& opaque, std::string& out) const;

  //----------------------------------------------------------------------------
  //! Disable copy constructor
  //----------------------------------------------------------------------------
  XrdIo(const XrdIo&) = delete;

  //----------------------------------------------------------------------------
  //! Disable assign operator
  //----------------------------------------------------------------------------
  XrdIo& operator = (const XrdIo&) = delete;
};

EOSFSTNAMESPACE_END

#endif  // __EOSFST_XRDFILEIO_HH__
