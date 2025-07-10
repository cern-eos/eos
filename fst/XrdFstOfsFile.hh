//------------------------------------------------------------------------------
//! @file: XrdFstOfsFile.hh
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#ifndef __EOSFST_FSTOFSFILE_HH__
#define __EOSFST_FSTOFSFILE_HH__

#include "fst/Namespace.hh"
#include "fst/storage/Storage.hh"
#include "fst/checksum/CheckSum.hh"
#include "fst/utils/TpcInfo.hh"
#include "common/Fmd.hh"
#include "common/FileId.hh"
#include "common/SymKeys.hh"
#include "common/Timing.hh"
#include <XrdVersion.hh>
#include <XrdOfs/XrdOfs.hh>
#include <XrdOfs/XrdOfsTPCInfo.hh>
#include <XrdOuc/XrdOucString.hh>
#include <numeric>

namespace eos
{
namespace fst
{
class HttpHandler;
class HttpServer;
class S3Handler;
}
};

namespace eos
{
namespace common
{
class FmdHelper;
}
}

EOSFSTNAMESPACE_BEGIN;

// This defines for reports what is a large seek e.g. > 128 kB = default RA size
#define EOS_FSTOFS_LARGE_SEEKS 128*1024ll

// Forward declaration
class Layout;
class CheckSum;

#if XrdMajorVNUM( XrdVNUMBER ) > 4
#define XrdOfsFileBase XrdOfsFileFull
#else
#define XrdOfsFileBase XrdOfsFile
#endif

//------------------------------------------------------------------------------
//! Class XrdFstOfsFile
//------------------------------------------------------------------------------
class XrdFstOfsFile : public XrdOfsFileBase, public eos::common::LogId
{
  friend class ReplicaParLayout;
  friend class RainMetaLayout;
  friend class RaidDpLayout;
  friend class ReedSLayout;
  friend class LocalIo;
  friend class HttpHandler;
  friend class HttpServer;
  friend class S3Handler;

public:

  static constexpr uint16_t msDefaultTimeout {300};
  static int LayoutReadCB(eos::fst::CheckSum::ReadCallBack::callback_data_t* cbd);
  static int FileIoReadCB(eos::fst::CheckSum::ReadCallBack::callback_data_t* cbd);

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param user user identity (tident)
  //! @param MonID monitoring id
  //----------------------------------------------------------------------------
  XrdFstOfsFile(const char* user, int MonID = 0);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~XrdFstOfsFile();

  //----------------------------------------------------------------------------
  //! Open file - see XrdSfsInterface.hh for description of parameters
  //!
  //! @return One of SFS_OK, SFS_ERROR, SFS_REDIRECT, SFS_STALL, or SFS_STARTED
  //----------------------------------------------------------------------------
  int open(const char* fileName, XrdSfsFileOpenMode openMode,
           mode_t createMode, const XrdSecEntity* client,
           const char* opaque = 0) override;

  //----------------------------------------------------------------------------
  //! Read from file with comprehensive I/O management
  //!
  //! Performs file read operations with support for:
  //! - Round-robin scheduling for application fairness
  //! - Bandwidth throttling and I/O rate limiting
  //! - Third-party copy (TPC) transfer validation
  //! - Checksum calculation for read-only files
  //! - Data decryption/unobfuscation for encrypted files
  //! - Performance monitoring and statistics collection
  //!
  //! @param fileOffset starting offset for the read operation
  //! @param buffer destination buffer for read data
  //! @param buffer_size number of bytes to read
  //! @return number of bytes read on success, SFS_ERROR on failure
  //----------------------------------------------------------------------------
  XrdSfsXferSize read(XrdSfsFileOffset fileOffset, char* buffer,
                      XrdSfsXferSize buffer_size) override;

  //----------------------------------------------------------------------------
  //! Read AIO - not supported
  //!
  //! Asynchronous I/O read operations are not currently supported.
  //!
  //! @param aioparm asynchronous I/O parameters
  //! @return SFS_ERROR indicating operation not supported
  //----------------------------------------------------------------------------
  int read(XrdSfsAio* aioparm) override;

  //----------------------------------------------------------------------------
  //! Read file pages into a buffer and return corresponding checksums.
  //!
  //! @param  offset  - The offset where the read is to start. It may be
  //!                   unaligned with certain caveats relative to csvec.
  //! @param  buffer  - pointer to buffer where the bytes are to be placed.
  //! @param  rdlen   - The number of bytes to read. The amount must be an
  //!                   integral number of XrdSfsPage::Size bytes.
  //! @param  csvec   - A vector of entries to be filled with the cooresponding
  //!                   CRC32C checksum for each page. However, if the offset is
  //!                   unaligned, then csvec[0] contains the crc for the page
  //!                   fragment that brings it to alignment for csvec[1].
  //!                   It must be sized to hold all aligned XrdSys::Pagesize
  //!                   crc's plus additional ones for leading and ending page
  //!                   fragments, if any.
  //! @param  opts    - Processing options (see above).
  //!
  //! @return >= 0      The number of bytes that placed in buffer.
  //! @return SFS_ERROR File could not be read, error holds the reason.
  //----------------------------------------------------------------------------
  XrdSfsXferSize pgRead(XrdSfsFileOffset offset, char* buffer,
                        XrdSfsXferSize rdlen, uint32_t* csvec,
                        uint64_t opts = 0) override;

  //----------------------------------------------------------------------------
  //! Read file pages and checksums using asynchronous I/O - NOT SUPPORTED
  //!
  //! @param  aioparm - Pointer to async I/O object controlling the I/O.
  //! @param  opts    - Processing options (see above).
  //!
  //! @return SFS_OK    Request accepted and will be scheduled.
  //! @return SFS_ERROR File could not be read, error holds the reason.
  //-----------------------------------------------------------------------------
  int pgRead(XrdSfsAio* aioparm, uint64_t opts = 0) override;

  //----------------------------------------------------------------------------
  //! Pre-read blocks into file system cache
  //----------------------------------------------------------------------------
  int read(XrdSfsFileOffset fileOffset, XrdSfsXferSize amount) override;

  //----------------------------------------------------------------------------
  //! Vector read - OFS interface method
  //!
  //! @param readV vector read structure
  //! @param readCount number of entries in the vector read structure
  //!
  //! @return number of bytes read upon success, otherwise SFS_ERROR
  //!
  //----------------------------------------------------------------------------
  XrdSfsXferSize readv(XrdOucIOVec* readV, int readCount) override;

  //----------------------------------------------------------------------------
  //! Write to file with comprehensive I/O management and error handling
  //!
  //! Performs file write operations with support for:
  //! - Global and per-filesystem round-robin scheduling
  //! - Bandwidth throttling and I/O rate limiting
  //! - Checksum state management for write operations
  //! - Data encryption/obfuscation for secure storage
  //! - Error detection and recovery mechanisms
  //! - Performance monitoring and statistics collection
  //! - Special handling for sink/null device files
  //!
  //! @param fileOffset starting offset for the write operation
  //! @param buffer source buffer containing data to write
  //! @param buffer_size number of bytes to write
  //! @return number of bytes written on success, SFS_ERROR on failure
  //----------------------------------------------------------------------------
  XrdSfsXferSize write(XrdSfsFileOffset fileOffset, const char* buffer,
                       XrdSfsXferSize buffer_size) override;

  //----------------------------------------------------------------------------
  //! Write AIO - no supported
  //----------------------------------------------------------------------------
  int write(XrdSfsAio* aioparm) override;

  //----------------------------------------------------------------------------
  //! Write file pages into a file with corresponding checksums.
  //!
  //! @param  offset  - The offset where the write is to start. It may be
  //!                   unaligned with certain caveats relative to csvec.
  //! @param  buffer  - pointer to buffer containing the bytes to write.
  //! @param  wrlen   - The number of bytes to write. If amount is not an
  //!                   integral number of XrdSys::PageSize bytes, then this must
  //!                   be the last write to the file at or above the offset.
  //! @param  csvec   - A vector which contains the corresponding CRC32 checksum
  //!                   for each page or page fragment. If offset is unaligned
  //!                   then csvec[0] is the crc of the leading fragment to
  //!                   align the subsequent full page who's crc is in csvec[1].
  //!                   It must be sized to hold all aligned XrdSys::Pagesize
  //!                   crc's plus additional ones for leading and ending page
  //!                   fragments, if any.
  //! @param  opts    - Processing options (see above).
  //!
  //! @return >= 0      The number of bytes written.
  //! @return SFS_ERROR File could not be read, error holds the reason.
  //----------------------------------------------------------------------------
  XrdSfsXferSize pgWrite(XrdSfsFileOffset offset, char* buffer,
                         XrdSfsXferSize wrlen, uint32_t* csvec,
                         uint64_t opts = 0) override;

  //----------------------------------------------------------------------------
  //! Write file pages and checksums using asynchronous I/O - NOT SUPPORTED
  //!
  //! @param  aioparm - Pointer to async I/O object controlling the I/O.
  //! @param  opts    - Processing options (see above).
  //!
  //! @return SFS_OK    Request accepted and will be scheduled.
  //! @return SFS_ERROR File could not be read, error holds the reason.
  //----------------------------------------------------------------------------
  int pgWrite(XrdSfsAio* aioparm, uint64_t opts = 0) override;

  //----------------------------------------------------------------------------
  //! Get file stat information
  //!
  //! @param buf pointer to the structure where info it to be returned
  //!
  //! @return One of SFS_OK, SFS_ERROR, SFS_REDIRECT, or SFS_STALL. When
  //!         SFS_OK is returned, buf must hold stat information
  //----------------------------------------------------------------------------
  int stat(struct stat* buf) override;

  //----------------------------------------------------------------------------
  //! Sync file
  //!
  //! @return One of SFS_OK, SFS_ERROR, SFS_REDIRECT, SFS_STALL, or SFS_STARTED
  //----------------------------------------------------------------------------
  int sync() override;

  //----------------------------------------------------------------------------
  //! Sync AIO
  //----------------------------------------------------------------------------
  int sync(XrdSfsAio* aiop) override;

  //----------------------------------------------------------------------------
  //! Truncate file
  //!
  //! @param fsize truncate size of the file
  //!
  //! @return One of SFS_OK, SFS_ERROR, SFS_REDIRECT, or SFS_STALL
  //----------------------------------------------------------------------------
  int truncate(XrdSfsFileOffset fileOffset) override;

  //----------------------------------------------------------------------------
  //! Close file
  //----------------------------------------------------------------------------
  int close() override;

  //----------------------------------------------------------------------------
  //! Execute special operation on the file (version 2)
  //!
  //! @param  cmd    - The operation to be performed:
  //!                  SFS_FCTL_SPEC1    Perform implementation defined action
  //! @param  alen   - Length of data pointed to by args.
  //! @param  args   - Data sent with request, zero if alen is zero.
  //! @param  client - Client's identify (see common description).
  //!
  //! @return SFS_OK   a null response is sent.
  //! @return SFS_DATA error.code    length of the data to be sent.
  //!                  error.message contains the data to be sent.
  //!         o/w      one of SFS_ERROR, SFS_REDIRECT, or SFS_STALL.
  //----------------------------------------------------------------------------
  int fctl(const int cmd, int alen, const char* args,
           const XrdSecEntity* client = 0) override;

  //----------------------------------------------------------------------------
  //! Return logical path
  //----------------------------------------------------------------------------
  std::string GetPath() const
  {
    return mNsPath.c_str();
  }

  //----------------------------------------------------------------------------
  //! Return the file system id
  //----------------------------------------------------------------------------
  unsigned long GetFileSystemId() const
  {
    return mFsId;
  }

  //----------------------------------------------------------------------------
  //! We're being placed in a cache. We record this time so we can report more
  //! relevant information when we are eventually closed.
  //----------------------------------------------------------------------------
  void OnCacheInsert()
  {
    gettimeofday(&cacheITime, &tz);
  }

private:
#ifdef IN_TEST_HARNESS
public:
#endif
  eos::common::SymKey::hmac_t mHmac;
  std::unique_ptr<XrdOucEnv> mOpenOpaque; ///< Open opaque info (encrypted)
  std::unique_ptr<XrdOucEnv> mCapOpaque; ///< Capability opaque info (decrypted)
  std::string mFstPath; ///< Physical path on the FST
  long long mBookingSize;
  off_t mTargetSize;
  off_t mMinSize;
  off_t mMaxSize;
  bool viaDelete;
  bool mWrDelete; ///< Flag file to be deleted due to write errors
  uint64_t mRainSize; ///< Rain file size used during reconstruction
  XrdOucString mNsPath; /// Logical file path (from the namespace)
  XrdOucString mLocalPrefix; ///< Prefix on the local storage
  XrdOucString mRdrManager; ///< Manager host where we bounce back
  bool mTapeEnabled; ///< True if tape support is enabled
  XrdOucString mSecString; ///< string containing security summary
  std::map<std::string, std::string> mSecMap; ///< map of all sec keys
  std::string mEtag; ///< Current and new ETag (recomputed in close)
  unsigned long long mFileId; //! file id
  eos::common::FileSystem::fsid_t mFsId; //! file system id
  unsigned long mLid; //! layout id
  unsigned long long mCid; //! container id
  unsigned long long mForcedMtime;
  unsigned long long mForcedMtime_ms;
  bool mFusex; //! indicator that we are committing from a fusex client
  bool mFusexIsUnlinked; //! indicator for an already unlinked file
  bool mClosed; //! indicator the file is closed
  //! Close return code that we might need to return twice in case of TPC
  int mCloseRc;
  bool mOpened; //! indicator that file is opened
  bool mHasWrite; //! indicator that file was written/modified
  bool mHasWriteErr;// indicator for write errors to avoid message flooding
  bool mHasReadErr; //! indicator if a RAIN file could be reconstructed or not
  bool mIsRW; //! indicator that file is opened for rw
  bool mIsDevNull; ///< If true file act as a sink i.e. /dev/null
  bool mIsCreation; //! indicator that a new file is created
  bool mIsReplication; //! indicator that the opened file is a replica transfer
  bool noAtomicVersioning; //! indicate to disable atomic/versioning during commit
  //! Indicate that the opened file is a file injection where the size and
  //! checksum must match
  bool mIsInjection;
  bool mRainReconstruct; ///< indicator that the opened file is in a RAIN reconstruction process
  bool mDelOnClose; ///< indicator that the file has to be cleaned on close
  bool mRepairOnClose; ///< indicator that the file should get repaired on close
  bool mIsOCchunk; //! indicator this is an OC chunk upload
  int writeErrorFlag; //! uses kOFSxx enums to specify an error condition
  bool mEventOnClose; ///< Indicator to send a specified event to MGM on close
  bool mSyncOnClose; ///< Indicator to run a fsync on close
  //! Indicates the workflow to be triggered by an event
  XrdOucString mEventWorkflow;
  bool mSyncEventOnClose; //! Indicator to send a specified event to the mgm on close
  std::string mEventInstance;
  uint32_t mEventOwnerUid;
  uint32_t mEventOwnerGid;
  std::string mEventRequestor;
  std::string mEventRequestorGroup;
  std::string mEventAttributes;
  std::string mArchiveReqId;
  int mIoPriorityValue;
  int mIoPriorityClass;
  bool mIoPriorityErrorReported;
  std::string mAppRR;

  enum {
    kOfsIoError = 1, //! generic IO error
    kOfsMaxSizeError = 2, //! maximum file size error
    kOfsDiskFullError = 3, //! disk full error
    kOfsSimulatedIoError = 4, //! simulated IO error
    kOfsFsRemovedError = 5, //! filesystem has been unregistered
    kOfsSimulatedCloseError = 6, //! simulated error when closing the file
    kOfsSimulatedUnresponsive = 7 // ! simulated unresponsivness of an FST
  };

  ///< In-memory file meta data object
  std::unique_ptr<eos::common::FmdHelper> mFmd;
  std::unique_ptr<eos::fst::CheckSum> mCheckSum; ///< Checksum object
  // @todo(esindril) this is not properly enforced everywhere ...
  std::unique_ptr<Layout> mLayout; ///< Layout object
  std::string mTident; ///< Client identity using the file object
  // File statistics for monitoring purposes
  //! Largest byte position written of a newly created file
  long long mMaxOffsetWritten;
  long long mWritePosition;
  long long mOpenSize; //! file size when the file was opened
  long long mCloseSize; //! file size when the file was closed
  struct timeval openTime; //! time when a file was opened
  struct timeval currentTime; //! time when a write occurs
  unsigned long long totalBytes; //! total bytes IO
  unsigned long long msSleep; //! total ms sleeping during io
  struct timeval closeTime; //! time when a file was closed
  struct timeval closeStart; //! time when a file close started
  struct timeval closeStop; //! time when a file close stopped
  struct timeval cacheITime; //! time when object was put into a cache
  struct timezone tz; //! timezone
  int mBandwidth; //! bandwidth limitation setting
  XrdSysMutex vecMutex; //! mutex protecting the rvec/wvec variables
  //! vector with all read  sizes -> to compute sigma,min,max,total
  std::vector<unsigned long long> rvec;
  //! vector with all write sizes -> to compute sigma,min,max,total
  std::vector<unsigned long long> wvec;
  unsigned long long rBytes; //! sum bytes read
  unsigned long long wBytes; //! sum bytes written
  unsigned long long sFwdBytes; //! sum bytes seeked forward
  unsigned long long sBwdBytes; //! sum bytes seeked backward
  //! sum bytes with large forward seeks (> EOS_FSTOFS_LARGE_SEEKS)
  unsigned long long sXlFwdBytes;
  //! sum bytes with large backward seeks (> EOS_FSTOFS_LARGE_SEEKS)
  unsigned long long sXlBwdBytes;
  unsigned long rCalls; //! number of read calls
  unsigned long wCalls; //! number of write calls
  unsigned long nFwdSeeks; //! number of seeks forward
  unsigned long nBwdSeeks; //! number of seeks backward
  unsigned long nXlFwdSeeks; //! number of seeks forward
  unsigned long nXlBwdSeeks; //! number of seeks backward
  unsigned long long rOffset; //! offset since last read operation on this file
  unsigned long long wOffset; //! offset since last write operation on this file
  //! Vector with all readv sizes -> to compute min,max,etc.
  std::vector<unsigned long long> monReadvBytes;
  //! Size of each read call coming from readv requests -> to compute min,max, etc.
  std::vector<unsigned long long> monReadSingleBytes;
  //! Number of individual read op. in each readv call -> to compute min,max, etc.
  std::vector<unsigned long> monReadvCount;

  struct timeval cTime; ///< current time
  struct timeval rStart; ///< start of last read (layout)
  struct timeval rvStart; ///< start of last readv (layout)
  struct timeval wStart; ///< start of last read (layout)
  struct timeval lrTime; ///< last read time
  struct timeval lrvTime; ///< last readv time
  struct timeval lwTime; ///< last write time
  struct timeval rTime; ///< sum time to serve read requests in ms
  struct timeval rvTime; ///< sum time to server readv requests in ms
  struct timeval wTime; ///< sum time to serve write requests in ms
  //! Stat struct to check if a file is updated between open-close
  struct stat updateStat;

  double timeToOpen; ///< time the open call took
  double timeToClose; ///< time the close call took
  double timeToRead; ///< time of all layout reads
  double timeToReadV; ///< time of all layout readvs
  double timeToWrite; ///< time of all layout writes
  //! TPC related types and variables
  enum TpcType_t {
    kTpcNone = 0, ///< No TPC access
    kTpcSrcSetup = 1, ///< Access setting up a source TPC session
    kTpcDstSetup = 2, ///< Access setting up a destination TPC session
    kTpcSrcRead = 3, ///< Read access from a TPC destination
    kTpcSrcCanDo = 4, ///< Read access to evaluate if source available
  };

  enum TpcState_t {
    kTpcIdle = 0, ///< TPC is not enabled (1st sync)
    kTpcRun = 1, ///< TPC is running (2nd sync)
    kTpcDone = 2, ///< TPC has finished
  };

  int mTpcThreadStatus; ///< Status of the TPC thread - 0 valid otherwise error
  pthread_t mTpcThread; ///< Thread doing the TPC transfer
  TpcState_t mTpcState; ///< TPC transfer status
  TpcType_t mTpcFlag; ///< TPC access type
  XrdOfsTPCInfo mTpcInfo; ///< TPC info object used for callback
  XrdSysMutex mTpcJobMutex; ///< TPC job mutex
  std::string mTpcKey; ///< TPC key for a tpc file operation
  TpcInfo mFstTpcInfo; ///< FST TPC info struct
  bool mIsTpcDst; ///< If true this is a TPC destination, otherwise a source
  int mTpcRetc; ///< TPC job return code
  std::atomic<bool> mTpcCancel; ///< Mark TPC cancellation request
  uint16_t mTimeout; ///< timeout for layout operations
  bool mIsHttp; ///< Mark if this is HTTP acceess

  //----------------------------------------------------------------------------
  //! Get configured minimum file size for which the asynchronous close method
  //! is called.
  //!
  //! @return min file size
  //----------------------------------------------------------------------------
  static uint64_t GetAsyncCloseMinSize();

  //----------------------------------------------------------------------------
  //! Check if async close is configured
  //!
  //! @return true if enabled, otherwise false
  //----------------------------------------------------------------------------
  static bool IsAsyncCloseConfigured();

  //----------------------------------------------------------------------------
  //! Check if async sycn is configured - a bit of an oxymoron ...
  //!
  //! @return true if enabled, otherwise false
  //----------------------------------------------------------------------------
  static bool IsAsyncSyncConfigured();

  //----------------------------------------------------------------------------
  //! Get hostname from tident. This is used when checking the origin match for
  //! TPC transfers. It only extract the hostname without domain to avoid
  //! mismatch in cases where the same machine provides both IPV4 and IPV6
  //! interfaces. Eg. root.1.2@eosbackup.cern.ch and
  //! root.1.2@eosbackup.ipv6.cern.ch should match
  //!
  //! @param tident xrootd like tident
  //! @param hostname output parameter holding the hostname
  //!
  //! @return true if parsing successful and hostname stores the desired value,
  //!         otherwise false
  //----------------------------------------------------------------------------
  static bool GetHostFromTident(const std::string& tident,
                                std::string& hostname);

  //----------------------------------------------------------------------------
  //! Filter out particular tags from the opaque information
  //!
  //! @param opaque opaque information to process
  //! @param tags set of tags to be filtered out
  //----------------------------------------------------------------------------
  static void FilterTagsInPlace(std::string& opaque,
                                const std::set<std::string> tags);

  //----------------------------------------------------------------------------
  //! Get TPC key expiration timestamp by combinding the client specified
  //! tpc.ttl value with the system TPC key minimum validity. The resulting
  //! value should always be between the system TPC key minimum validity and
  //! 15 minutes, unless the former the biggern than 15 min and then this will
  //! apply.
  //!
  //! @param tpc_tll client specified key ttl
  //! @param now_ts used for testing
  //!
  //! @return expiration timestamp for the current key
  //----------------------------------------------------------------------------
  static time_t GetTpcKeyExpireTS(std::string_view tpc_ttl,
                                  time_t now_ts = 0);

  //----------------------------------------------------------------------------
  //! Close internal method that can be called synchronously (from XRootD) or
  //! asynchronously from the thread pool for long running close operations.
  //!
  //! @return SFS_OK if close successful, otherwise SFS_ERROR
  //----------------------------------------------------------------------------
  int _close();

  //----------------------------------------------------------------------------
  //! Close file opened for reading - internal
  //!
  //! @return SFS_OK if close successful, otherwise SFS_ERROR
  //----------------------------------------------------------------------------
  int _close_rd();

  //----------------------------------------------------------------------------
  //! Close file opened for writing - internal
  //!
  //! @return SFS_OK if close successful, otherwise SFS_ERROR
  //----------------------------------------------------------------------------
  int _close_wr();

  //----------------------------------------------------------------------------
  //! Low-level open calling the default XrdOfs plugin and being called from
  //! one of the layout implementations.
  //----------------------------------------------------------------------------
  int openofs(const char* fileName, XrdSfsFileOpenMode openMode,
              mode_t createMode, const XrdSecEntity* client,
              const char* opaque = 0);

  //----------------------------------------------------------------------------
  //! Low-level read calling the default XrdOfs plugin
  //----------------------------------------------------------------------------
  XrdSfsXferSize readofs(XrdSfsFileOffset fileOffset, char* buffer,
                         XrdSfsXferSize buffer_size);

  //----------------------------------------------------------------------------
  //! Low-level vector read calling the default XrdOfs plugin
  //----------------------------------------------------------------------------
  XrdSfsXferSize readvofs(XrdOucIOVec* readV, uint32_t readCount);

  //----------------------------------------------------------------------------
  //! Low-level write calling the default XrdOfs plugin
  //----------------------------------------------------------------------------
  XrdSfsXferSize writeofs(XrdSfsFileOffset fileOffset, const char* buffer,
                          XrdSfsXferSize buffer_size);

  //----------------------------------------------------------------------------
  //! Low-level sync calling the default XrdOfs plugin
  //----------------------------------------------------------------------------
  int syncofs();

  //----------------------------------------------------------------------------
  //! Low-level truncate calling the default XrdOfs plugin
  //----------------------------------------------------------------------------
  int truncateofs(XrdSfsFileOffset fileOffset);

  //----------------------------------------------------------------------------
  //! Low-level close calling the default XrdOfs plugin
  //----------------------------------------------------------------------------
  int closeofs();

  //----------------------------------------------------------------------------
  //! Get physical path on the FST (local)
  //!
  //! @return local physical path
  //----------------------------------------------------------------------------
  inline std::string GetFstPath() const
  {
    return mFstPath.c_str();
  }

  //----------------------------------------------------------------------------
  //! Return the Etag
  //----------------------------------------------------------------------------
  const char* GetETag() const
  {
    return mEtag.c_str();
  }

  //----------------------------------------------------------------------------
  //! Enforce an mtime on close
  //----------------------------------------------------------------------------
  void SetForcedMtime(unsigned long long mtime, unsigned long long mtime_ms)
  {
    mForcedMtime = mtime;
    mForcedMtime_ms = mtime_ms;
  }

  //----------------------------------------------------------------------------
  //! Return current mtime while open
  //----------------------------------------------------------------------------
  time_t GetMtime() const;

  //----------------------------------------------------------------------------
  //! Return the file size seen at open time
  //----------------------------------------------------------------------------
  inline off_t GetOpenSize() const
  {
    return mOpenSize;
  }

  //----------------------------------------------------------------------------
  //! Return the file id
  //----------------------------------------------------------------------------
  unsigned long long GetFileId() const
  {
    return mFileId;
  }

  //----------------------------------------------------------------------------
  //! Return checksum
  //----------------------------------------------------------------------------
  eos::fst::CheckSum* GetChecksum() const
  {
    return mCheckSum.get();
  }

  //----------------------------------------------------------------------------
  //! Return FMD checksum
  //----------------------------------------------------------------------------
  std::string GetFmdChecksum() const;

  //----------------------------------------------------------------------------
  //! Check for chunked upload flag
  //----------------------------------------------------------------------------
  bool IsChunkedUpload() const
  {
    return mIsOCchunk;
  }

  //----------------------------------------------------------------------------
  //! Check if the TpcKey is still valid e.g. member of gOFS.TpcMap
  //----------------------------------------------------------------------------
  bool TpcValid() const;

  //----------------------------------------------------------------------------
  //! Process TPC (third-party-copy) opaque information i.e handle tags like
  //! tpc.key, tpc.dst, tpc.stage etc and also extract and decrypt the cap
  //! opaque info
  //!
  //! @param opaque opaque information
  //! @param client XrdSecEntity of client
  //!
  //! @return SFS_OK if successful, otherwise SFS_ERROR
  //----------------------------------------------------------------------------
  int ProcessTpcOpaque(std::string& opaque, const XrdSecEntity* client);

  //----------------------------------------------------------------------------
  //! Process open opaque information - this comes directly from the client
  //! or from the MGM redirection but it's not encrypted but sent in plain
  //! text in the URL
  //!
  //! @return SFS_OK if successful, otherwise SFS_ERROR
  //----------------------------------------------------------------------------
  int ProcessOpenOpaque();

  //----------------------------------------------------------------------------
  //! Process cap opaque information - decisions that need to be taken based
  //! on the encrypted opaque info
  //!
  //! @param is_repair_read flag if this is a repair read
  //! @param vid client virtual identity
  //!
  //! @return SFS_OK if successful, otherwise SFS_ERROR
  //----------------------------------------------------------------------------
  int ProcessCapOpaque(bool& is_repair_read,
                       eos::common::VirtualIdentity& vid);

  //----------------------------------------------------------------------------
  //! Process mixed opaque information - decisions that need to be taken based
  //! on both the ecrypted and un-encrypted opaque info
  //!
  //! @return SFS_OK if successful, otherwise SFS_ERROR
  //----------------------------------------------------------------------------
  int ProcessMixedOpaque();

  //----------------------------------------------------------------------------
  //! Compute time to close a file
  //----------------------------------------------------------------------------
  void CloseTime();

  //----------------------------------------------------------------------------
  //! Compute total time to serve read requests
  //----------------------------------------------------------------------------
  void AddReadTime();
  void AddLayoutReadTime();

  //----------------------------------------------------------------------------
  //! Compute total time to serve vector read requests
  //----------------------------------------------------------------------------
  void AddReadVTime();
  void AddLayoutReadVTime();

  //----------------------------------------------------------------------------
  //! Compute total time to serve write requests
  //----------------------------------------------------------------------------
  void AddWriteTime();
  void AddLayoutWriteTime();

  //----------------------------------------------------------------------------
  //! Compute general statistics on a set of input values
  //!
  //! @param vect input collection
  //! @param min minimum element
  //! @param max maximum element
  //! @param sum sum of the elements
  //! @param avg average value
  //! @param sigma sigma of the elements
  //----------------------------------------------------------------------------
  template <typename T>
  void ComputeStatistics(const std::vector<T>& vect, T& min, T& max,
                         T& sum, double& sigma);

  //----------------------------------------------------------------------------
  //! Create report as a string
  //----------------------------------------------------------------------------
  void MakeReportEnv(XrdOucString& reportString);

  //----------------------------------------------------------------------------
  //! Static method used to start an asynchronous thread which is doing the
  //! TPC transfer
  //!
  //! @param arg XrdFstOfsFile instance object
  //----------------------------------------------------------------------------
  static void* StartDoTpcTransfer(void* arg);

  //----------------------------------------------------------------------------
  //! Do TPC transfer
  //----------------------------------------------------------------------------
  void* DoTpcTransfer();

  //----------------------------------------------------------------------------
  //! TPC clean up - invalidates the TPC keys at the end of a TPC transfer and
  //! also joins the TPC helper thread
  //----------------------------------------------------------------------------
  void TpcCleanup();

  //----------------------------------------------------------------------------
  //! Extract logid from the opaque info i.e. mgm.logid
  //!
  //! @param opaque opaque info
  //----------------------------------------------------------------------------
  std::string ExtractLogId(const char* opaque) const;

  //----------------------------------------------------------------------------
  //! Drop stripe/replica(s) from the MGM
  //!
  //! @param fid file identifier
  //! @param fsid file system id to drop, if 0 then drop all stripes
  //! @param path file logical path
  //! @param manager MGM hostname
  //!
  //! @return 0 if successful, otherwise error code
  //----------------------------------------------------------------------------
  int DropFromMgm(eos::common::FileId::fileid_t fid,
                  eos::common::FileSystem::fsid_t fsid,
                  const std::string& path, const std::string& manager);

  //----------------------------------------------------------------------------
  //! Check if file has been modified while in use
  //!
  //! @return -1 if modified, otherwise 0
  //----------------------------------------------------------------------------
  int ModifiedWhileInUse();

  //----------------------------------------------------------------------------
  //! Verify checksum
  //!
  //! @return true if ok, otherwise false
  //----------------------------------------------------------------------------
  bool VerifyChecksum();

  //----------------------------------------------------------------------------
  //! Queue file for CTA archiving
  //!
  //! @param statinfo The file stat structure
  //! @param queueing_errmsg Error message from CTA queueing
  //! @param archive_req_id Output parameter: The archive request ID returned by
  //! the ProtoEfEndPoint
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool QueueForArchiving(const struct stat& statinfo,
                         std::string& queueing_errmsg,
                         std::string& archive_req_id);

  //----------------------------------------------------------------------------
  //! Notify the workflow protobuf endpoint that the user has closed a file that
  //! they were writing to
  //!
  //! @param file_id The id of the file
  //! @param file_lid The layout id of the file
  //! @param file_size The size of the file
  //! @param file_checksum The checksum of the file
  //! @param owner_uid The user id of the file owner
  //! @param owner_gid The group id of the file owner
  //! @param requestor_name Tha name of the user that closed the file
  //! @param requestor_groupname The name of the group that closed the file
  //! @param instance_name Tha name of the EOS instance
  //! @param fullpath The full path of the file
  //! @param manager_name The name of the EOS manager
  //! @param xattrs The extended attributes of teh file to be passed to the
  //! workflow protobuf endpoint
  //! @param errmsg_wfe Output parameter: Error message back from the workflow
  //! protobuf endpoint
  //! @param archive_req_id Output parameter: The archive request ID returned by
  //! the ProtoEfEndPoint
  //!
  //! @return 0 if successful, error code otherwise
  //----------------------------------------------------------------------------
  int NotifyProtoWfEndPointClosew(uint64_t file_id,
                                  uint32_t file_lid, uint64_t file_size,
                                  const std::string& file_checksum,
                                  uint32_t owner_uid, uint32_t owner_gid,
                                  const std::string& requestor_name,
                                  const std::string& requestor_groupname,
                                  const std::string& instance_name,
                                  const std::string& fullpath,
                                  const std::string& manager_name,
                                  const std::map<std::string, std::string>& xattrs,
                                  std::string& errmsg_wfe,
                                  std::string& archive_req_id);

  //----------------------------------------------------------------------------
  //! Send archive failed event to the manager
  //!
  //! @param fid The file identifier
  //! @param errmsg The error message to enclosed in the archive failed event
  //!
  //! @return SFS_OK if successful
  //----------------------------------------------------------------------------
  int SendArchiveFailedToManager(const uint64_t fid,
                                 const std::string& errmsg);

  //----------------------------------------------------------------------------
  //! Decide if close should be done synchronously. There are cases when close
  //! should happen in the same thread eg. read, http tx, sink writes etc.
  //!
  //! @return true if close is synchronous, otherwise false
  //----------------------------------------------------------------------------
  bool DoSyncClose();

  //----------------------------------------------------------------------------
  //! Decide if sync should be done synchronously. There are cases when sync
  //! should happen in the same thread eg. read, http tx, sink writes etc.
  //! Keep the same boundary conditions as async close on purpose!
  //!
  //! @return true if sync is synchronous, otherwise false
  //----------------------------------------------------------------------------
  bool DoSyncSync();

  //----------------------------------------------------------------------------
  //! Populate and commit FMD info locally
  //!
  //! @param info stat info about the logical file
  //!
  //! @return 0 if successful, otherwise -1
  //----------------------------------------------------------------------------
  int CommitToLocalFmd(const struct stat& info);

  //----------------------------------------------------------------------------
  //! Commit file information to MGM
  //!
  //! @return 0 if successful, otherwise -1 or errno
  //----------------------------------------------------------------------------
  int CommitToMgm();

  //----------------------------------------------------------------------------
  //! Trigger event on close
  //!
  //! @param archive_req_id archive request id
  //!
  //! @return 0 if successful, other != 0
  //----------------------------------------------------------------------------
  int TriggerEventOnClose(const std::string& archive_req_id);

  //============================================================================
  // Helper functions for open() method refactoring
  //============================================================================

  //----------------------------------------------------------------------------
  //! Process TPC and capability information during file open
  //!
  //! @param in_opaque input opaque string containing TPC and capability info
  //! @param client client security entity
  //! @param tpc_retc output parameter for TPC return code
  //!
  //! @return 0 if successful, otherwise error code
  //----------------------------------------------------------------------------
  int ProcessTpcAndCapabilities(const std::string& in_opaque,
                                const XrdSecEntity* client,
                                int& tpc_retc);

  //----------------------------------------------------------------------------
  //! Validate path access and create layout object
  //!
  //! @param path file path
  //! @param client client security entity
  //! @param tident client identity
  //! @param epname error prefix name
  //! @param envlen environment length
  //!
  //! @return 0 if successful, otherwise error code
  //----------------------------------------------------------------------------
  int ValidatePathAndCreateLayout(const char* path,
                                  const XrdSecEntity* client,
                                  const char* tident,
                                  const char* epname,
                                  int& envlen);

  //----------------------------------------------------------------------------
  //! Handle file existence check and creation logic
  //!
  //! @param open_mode file open mode
  //! @param create_mode file creation mode
  //! @param hasCreationMode flag indicating if creation mode is set
  //! @param epname error prefix name
  //! @param envlen environment length
  //!
  //! @return 0 if successful, otherwise error code
  //----------------------------------------------------------------------------
  int HandleFileExistenceAndCreation(XrdSfsFileOpenMode& open_mode,
                                     mode_t& create_mode,
                                     bool hasCreationMode,
                                     const char* epname,
                                     int& envlen);

  //----------------------------------------------------------------------------
  //! Validate capability permissions and extract parameters
  //!
  //! @param path file path
  //! @param epname error prefix name
  //!
  //! @return 0 if successful, otherwise error code
  //----------------------------------------------------------------------------
  int ValidateCapabilityAndExtractParams(const char* path,
                                         const char* epname);

  //----------------------------------------------------------------------------
  //! Handle clone file system operations (copy-on-write)
  //!
  //! @param open_mode file open mode
  //!
  //! @return 0 if successful, otherwise error code
  //----------------------------------------------------------------------------
  int HandleCloneOperations(XrdSfsFileOpenMode open_mode);

  //----------------------------------------------------------------------------
  //! Open the layout implementation
  //!
  //! @param open_mode file open mode
  //! @param create_mode file creation mode
  //! @param hasCreationMode flag indicating if creation mode is set
  //! @param path file path
  //! @param epname error prefix name
  //!
  //! @return 0 if successful, otherwise error code
  //----------------------------------------------------------------------------
  int OpenLayoutImplementation(XrdSfsFileOpenMode open_mode,
                               mode_t create_mode,
                               bool hasCreationMode,
                               const char* path,
                               const char* epname);

  //----------------------------------------------------------------------------
  //! Get and synchronize file metadata (FMD)
  //!
  //! @param isRepairRead flag indicating if this is a repair read
  //! @param hasCreationMode flag indicating if creation mode is set
  //! @param path file path
  //! @param epname error prefix name
  //!
  //! @return 0 if successful, otherwise error code
  //----------------------------------------------------------------------------
  int GetAndSyncFileMetadata(bool isRepairRead,
                             bool hasCreationMode,
                             const char* path,
                             const char* epname);

  //----------------------------------------------------------------------------
  //! Handle space management and file allocation
  //!
  //! @param hasCreationMode flag indicating if creation mode is set
  //! @param path file path
  //! @param epname error prefix name
  //!
  //! @return 0 if successful, otherwise error code
  //----------------------------------------------------------------------------
  int HandleSpaceAndAllocation(bool hasCreationMode,
                               const char* path,
                               const char* epname);

  //----------------------------------------------------------------------------
  //! Setup file size and checksum information
  //!
  //! @param epname error prefix name
  //!
  //! @return 0 if successful, otherwise error code
  //----------------------------------------------------------------------------
  int SetupFileSizeAndChecksum(const char* epname);

  //----------------------------------------------------------------------------
  //! Create file I/O object and set extended attributes
  //!
  //! @param path file path
  //! @param epname error prefix name
  //!
  //! @return 0 if successful, otherwise error code
  //----------------------------------------------------------------------------
  int CreateFileIoAndSetAttributes(const char* path,
                                   const char* epname);

  //----------------------------------------------------------------------------
  //! Finalize open operation with accounting and statistics
  //!
  //! @param tm timing object for performance measurement
  //!
  //! @return 0 if successful, otherwise error code
  //----------------------------------------------------------------------------
  int FinalizeOpenOperation(eos::common::Timing& tm);
};

//------------------------------------------------------------------------------
// Compute general statistics on a set of input values
//------------------------------------------------------------------------------
template <typename T>
void XrdFstOfsFile::ComputeStatistics(const std::vector<T>& vect, T& min,
                                      T& max, T& sum, double& sigma)
{
  double avg, sum2;
  max = sum = sum2 = avg = sigma = 0;
  min = 0xffffffff;
  sum = std::accumulate(vect.begin(), vect.end(),
                        static_cast<unsigned long long>(0));
  avg = vect.size() ? (1.0 * sum / vect.size()) : 0;

  for (auto it = vect.begin(); it != vect.end(); ++it) {
    if (*it > max) {
      max = *it;
    }

    if (*it < min) {
      min = *it;
    }

    sum2 += std::pow((*it - avg), 2);
  }

  sigma = vect.size() ? (sqrt(sum2 / vect.size())) : 0;

  if (min == 0xffffffff) {
    min = 0;
  }
}

EOSFSTNAMESPACE_END

#endif
