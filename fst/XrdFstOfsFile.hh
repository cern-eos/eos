//------------------------------------------------------------------------------
//! @file: XrdFstOfsFile.hh
//! @author: Andreas-Joachim Peters - CERN
//! @brief
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

#ifndef __EOSFST_FSTOFSFILE_HH__
#define __EOSFST_FSTOFSFILE_HH__

#include <numeric>
#include "fst/Fmd.hh"
#include "fst/Namespace.hh"
#include "fst/checksum/CheckSum.hh"
#include "fst/storage/Storage.hh"
#include "common/FileId.hh"
#include "XrdOfs/XrdOfs.hh"
#include "XrdOfs/XrdOfsTPCInfo.hh"
#include "XrdOuc/XrdOucString.hh"

class XrdOucEnv;

EOSFSTNAMESPACE_BEGIN;

// This defines for reports what is a large seek e.g. > 128 kB = default RA size
#define EOS_FSTOFS_LARGE_SEEKS 128*1024ll

// Forward declaration
class Layout;
class CheckSum;
class FmdHelper;

//------------------------------------------------------------------------------
//! Class XrdFstOfsFile
//------------------------------------------------------------------------------
class XrdFstOfsFile : public XrdOfsFile, public eos::common::LogId
{
  friend class ReplicaParLayout;
  friend class RaidMetaLayout;
  friend class RaidDpLayout;
  friend class ReedSLayout;

public:

  static const uint16_t msDefaultTimeout; ///< default timeout value

  //--------------------------------------------------------------------------
  // Constructor
  //--------------------------------------------------------------------------
  XrdFstOfsFile(const char* user, int MonID = 0);

  //--------------------------------------------------------------------------
  // Destructor
  //--------------------------------------------------------------------------
  virtual ~XrdFstOfsFile();

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
  virtual int fctl(const int cmd,
                   int alen,
                   const char* args,
                   const XrdSecEntity* client = 0);

  //--------------------------------------------------------------------------
  //! Return the Etag
  //--------------------------------------------------------------------------
  const char* GetETag()
  {
    return mEtag.c_str();
  }

  //--------------------------------------------------------------------------
  //! Enforce an mtime on close
  //--------------------------------------------------------------------------

  void SetForcedMtime(unsigned long long mtime, unsigned long long mtime_ms)
  {
    mForcedMtime = mtime;
    mForcedMtime_ms = mtime_ms;
  }

  //--------------------------------------------------------------------------
  //! Return current mtime while open
  //--------------------------------------------------------------------------
  time_t GetMtime();

  //--------------------------------------------------------------------------
  //!
  //--------------------------------------------------------------------------
  int openofs(const char* fileName,
              XrdSfsFileOpenMode openMode,
              mode_t createMode,
              const XrdSecEntity* client,
              const char* opaque = 0);

  //--------------------------------------------------------------------------
  //!
  //--------------------------------------------------------------------------
  int dropall(eos::common::FileId::fileid_t fileid,
              std::string path,
              std::string manager);

  //--------------------------------------------------------------------------
  //!
  //--------------------------------------------------------------------------
  int open(const char* fileName,
           XrdSfsFileOpenMode openMode,
           mode_t createMode,
           const XrdSecEntity* client,
           const char* opaque = 0);

  //--------------------------------------------------------------------------
  //!
  //--------------------------------------------------------------------------
  int modified();

  //--------------------------------------------------------------------------
  //!
  //--------------------------------------------------------------------------
  int closeofs();

  //--------------------------------------------------------------------------
  //!
  //--------------------------------------------------------------------------
  int close();

  //--------------------------------------------------------------------------
  //!
  //--------------------------------------------------------------------------
  int read(XrdSfsFileOffset fileOffset,  // Preread only
           XrdSfsXferSize amount);

  //--------------------------------------------------------------------------
  //!
  //--------------------------------------------------------------------------
  XrdSfsXferSize read(XrdSfsFileOffset fileOffset,
                      char* buffer,
                      XrdSfsXferSize buffer_size);

  //--------------------------------------------------------------------------
  //!
  //--------------------------------------------------------------------------
  XrdSfsXferSize readofs(XrdSfsFileOffset fileOffset,
                         char* buffer,
                         XrdSfsXferSize buffer_size);

  //--------------------------------------------------------------------------
  //!
  //--------------------------------------------------------------------------
  int read(XrdSfsAio* aioparm);

  //--------------------------------------------------------------------------
  //! Vector read - low level ofs method which is called from one of the
  //! layout plugins
  //!
  //! @param readV vector read structure
  //! @param readCount number of entries in the vector read structure
  //!
  //! @return number of bytes read upon success, otherwise SFS_ERROR
  //!
  //--------------------------------------------------------------------------
  XrdSfsXferSize readvofs(XrdOucIOVec* readV,
                          uint32_t readCount);

  //--------------------------------------------------------------------------
  //! Vector read - OFS interface method
  //!
  //! @param readV vector read structure
  //! @param readCount number of entries in the vector read structure
  //!
  //! @return number of bytes read upon success, otherwise SFS_ERROR
  //!
  //--------------------------------------------------------------------------
  XrdSfsXferSize readv(XrdOucIOVec* readV,
                       int readCount);

  //--------------------------------------------------------------------------
  //!
  //--------------------------------------------------------------------------
  XrdSfsXferSize write(XrdSfsFileOffset fileOffset,
                       const char* buffer,
                       XrdSfsXferSize buffer_size);

  //--------------------------------------------------------------------------
  //!
  //--------------------------------------------------------------------------
  XrdSfsXferSize writeofs(XrdSfsFileOffset fileOffset,
                          const char* buffer,
                          XrdSfsXferSize buffer_size);

  //--------------------------------------------------------------------------
  //!
  //--------------------------------------------------------------------------
  int write(XrdSfsAio* aioparm);

  //--------------------------------------------------------------------------
  //!
  //--------------------------------------------------------------------------
  int stat(struct stat* buf);

  //--------------------------------------------------------------------------
  //!
  //--------------------------------------------------------------------------
  bool verifychecksum();

  //--------------------------------------------------------------------------
  //!
  //--------------------------------------------------------------------------
  int sync();

  //--------------------------------------------------------------------------
  //!
  //--------------------------------------------------------------------------
  int syncofs();

  //--------------------------------------------------------------------------
  //!
  //--------------------------------------------------------------------------
  int sync(XrdSfsAio* aiop);

  //--------------------------------------------------------------------------
  //!
  //--------------------------------------------------------------------------
  int truncate(XrdSfsFileOffset fileOffset);

  //--------------------------------------------------------------------------
  //!
  //--------------------------------------------------------------------------
  int truncateofs(XrdSfsFileOffset fileOffset);

  //--------------------------------------------------------------------------
  //! Get physical path on the FST (local)
  //--------------------------------------------------------------------------
  inline std::string GetFstPath()
  {
    return mFstPath.c_str();
  }

  //--------------------------------------------------------------------------
  //! Return logical path
  //--------------------------------------------------------------------------
  std::string GetPath()
  {
    return mNsPath.c_str();
  }

  //--------------------------------------------------------------------------
  //! Check if the TpcKey is still valid e.g. member of gOFS.TpcMap
  //--------------------------------------------------------------------------
  bool TpcValid();

  //--------------------------------------------------------------------------
  //! Return the file size seen at open time
  //--------------------------------------------------------------------------
  off_t getOpenSize()
  {
    return openSize;
  }

  //--------------------------------------------------------------------------
  //! Return the file id
  //--------------------------------------------------------------------------
  unsigned long long getFileId()
  {
    return mFileId;
  }

  //--------------------------------------------------------------------------
  //! Return the file system id
  //--------------------------------------------------------------------------
  unsigned long getFileSystemId()
  {
    return mFsId;
  }

  //--------------------------------------------------------------------------
  //! Return checksum
  //--------------------------------------------------------------------------
  eos::fst::CheckSum* GetChecksum()
  {
    return mCheckSum.get();
  }

  //--------------------------------------------------------------------------
  //! Return FMD checksum
  //--------------------------------------------------------------------------
  std::string GetFmdChecksum();

  //--------------------------------------------------------------------------
  //! Check for chunked upload flag
  //--------------------------------------------------------------------------
  bool IsChunkedUpload()
  {
    return mIsOCchunk;
  }

  //--------------------------------------------------------------------------
  static int LayoutReadCB(eos::fst::CheckSum::ReadCallBack::callback_data_t* cbd);
  static int FileIoReadCB(eos::fst::CheckSum::ReadCallBack::callback_data_t* cbd);

protected:
  std::unique_ptr<XrdOucEnv> mOpenOpaque; ///< Open opaque info (encrypted)
  std::unique_ptr<XrdOucEnv> mCapOpaque; ///< Capability opaque info (decrypted)
  XrdOucString mFstPath; ///< Physical path on the FST
  off_t mBookingSize;
  off_t mTargetSize;
  off_t mMinSize;
  off_t mMaxSize;
  bool viaDelete;
  bool writeDelete;
  uint64_t mRainSize; ///< Rain file size used during reconstruction

  XrdOucString mNsPath; /// Logical file path (from the namespace)
  XrdOucString mLocalPrefix; //! prefix on the local storage
  XrdOucString mRedirectManager; //! manager host where we bounce back
  XrdOucString mSecString; //! string containing security summary
  XrdSysMutex ChecksumMutex; //! mutex protecting the checksum class
  XrdOucString mTpcKey; //! TPC key for a tpc file operation
  XrdOucString mEtag; //! current and new ETag (recomputed in close)

  unsigned long long mFileId; //! file id
  unsigned long mFsId; //! file system id
  unsigned long mLid; //! layout id
  unsigned long long mCid; //! container id
  unsigned long long mForcedMtime;
  unsigned long long mForcedMtime_ms;
  bool mFusex; //! indicator that we are commiting from a fusex client
  bool mFusexIsUnlinked; //! indicator for an already unlinked file
  bool closed; //! indicator the file is closed
  bool opened; //! indicator that file is opened
  bool mHasWrite; //! indicator that file was written/modified
  bool hasWriteError;// indicator for write errros to avoid message flooding
  bool hasReadError; //! indicator if a RAIN file could be reconstructed or not
  bool isRW; //! indicator that file is opened for rw
  bool mIsTpcDst; ///< If true this is a TPC destination, otherwise a source
  bool mIsDevNull; ///< If true file act as a sink i.e. /dev/null
  bool isCreation; //! indicator that a new file is created
  bool isReplication; //! indicator that the opened file is a replica transfer
  //! Indicate that the opened file is a file injection where the size and
  //! checksum must match
  bool mIsInjection;
  bool mRainReconstruct; ///< indicator that the opened file is in a RAIN reconstruction process
  bool deleteOnClose; ///< indicator that the file has to be cleaned on close
  bool repairOnClose; ///< indicator that the file should get repaired on close
  //! Indicator that this FST has to commmit after reconstruction
  //! * if the reconstructed piece is not existing on disk we commit anyway
  //! since it is a creation.
  //! * if it does exist maybe from a previous movement where the replica was
  //! not yet deleted, we would register another stripe without deleting one
  //! * there fore we indicate with  mOpenOpaque->Get("eos.pio.commitfs") which
  //! filesystem should actually commit during reconstruction
  bool commitReconstruction;
  bool mEventOnClose; ///< Indicator to send a specified event to MGM on close
  //! Indicates the workflow to be triggered by an event
  XrdOucString mEventWorkflow;
  bool mSyncEventOnClose; //! indicator to send a specified event to the mgm on close
  std::string mEventInstance;
  std::string mEventOwner;
  std::string mEventOwnerGroup;
  std::string mEventRequestor;
  std::string mEventRequestorGroup;
  std::string mEventAttributes;

  enum {
    kOfsIoError = 1, //! generic IO error
    kOfsMaxSizeError = 2, //! maximum file size error
    kOfsDiskFullError = 3, //! disk full error
    kOfsSimulatedIoError = 4 //! simulated IO error
  };

  bool mIsOCchunk; //! indicator this is an OC chunk upload
  int writeErrorFlag; //! uses kOFSxx enums to specify an error condition

  enum {
    kTpcNone = 0, //! no TPC access
    kTpcSrcSetup = 1, //! access setting up a source TPC session
    kTpcDstSetup = 2, //! access setting up a destination TPC session
    kTpcSrcRead = 3, //! read access from a TPC destination
    kTpcSrcCanDo = 4, //! read access to evaluate if source available
  };

  int mTpcFlag; //! uses kTpcXYZ enums above to identify TPC access

  enum TpcState_t {
    kTpcIdle = 0, //! TPC is not enabled (1st sync)
    kTpcRun = 1, //! TPC is running (2nd sync)
    kTpcDone = 2, //! TPC has finished
  };

  FmdHelper* fMd; //! pointer to the in-memory file meta data object
  std::unique_ptr<eos::fst::CheckSum> mCheckSum; //! Checksum object
  Layout* layOut; //! pointer to a layout object

private:
  // File statistics for monitoring purposes
  //! Largest byte position written of a newly created file
  unsigned long long maxOffsetWritten;
  off_t openSize; //! file size when the file was opened
  off_t closeSize; //! file size when the file was closed
  struct timeval openTime; //! time when a file was opened
  struct timeval closeTime; //! time when a file was closed
  struct timezone tz; //! timezone
  XrdSysMutex vecMutex; //! mutex protecting the rvec/wvec variables
  std::vector<unsigned long long>
  rvec; //! vector with all read  sizes -> to compute sigma,min,max,total
  std::vector<unsigned long long>
  wvec; //! vector with all write sizes -> to compute sigma,min,max,total
  unsigned long long rBytes; //! sum bytes read
  unsigned long long wBytes; //! sum bytes written
  unsigned long long sFwdBytes; //! sum bytes seeked forward
  unsigned long long sBwdBytes; //! sum bytes seeked backward
  unsigned long long
  sXlFwdBytes; //! sum bytes with large forward seeks (> EOS_FSTOFS_LARGE_SEEKS)
  unsigned long long
  sXlBwdBytes; //! sum bytes with large backward seeks (> EOS_FSTOFS_LARGE_SEEKS)
  unsigned long rCalls; //! number of read calls
  unsigned long wCalls; //! number of write calls
  unsigned long nFwdSeeks; //! number of seeks forward
  unsigned long nBwdSeeks; //! number of seeks backward
  unsigned long nXlFwdSeeks; //! number of seeks forward
  unsigned long nXlBwdSeeks; //! number of seeks backward
  unsigned long long rOffset; //! offset since last read operation on this file
  unsigned long long wOffset; //! offset since last write operation on this file
  //! vector with all readv sizes -> to compute min,max,etc.
  std::vector<unsigned long long> monReadvBytes;
  //! size of each read call coming from readv requests -> to compute min,max, etc.
  std::vector<unsigned long long> monReadSingleBytes;
  //! number of individual read op. in each readv call -> to compute min,max, etc.
  std::vector<unsigned long> monReadvCount;

  struct timeval cTime; ///< current time
  struct timeval lrTime; ///<last read time
  struct timeval lrvTime; ///< last readv time
  struct timeval lwTime; ///< last write time
  struct timeval rTime; ///< sum time to serve read requests in ms
  struct timeval rvTime; ///< sum time to server readv requests in ms
  struct timeval wTime; ///< sum time to serve write requests in ms
  XrdOucString tIdent; ///< tident
  //! Stat struct to check if a file is updated between open-close
  struct stat updateStat;

  //--------------------------------------------------------------------------
  //! Process TPC (third-party-copy) opaque information i.e handle tags like
  //! tpc.key, tpc.dst, tpc.stage etc and also extact and decrypt the cap
  //! opaque info
  //!
  //! @param opaque opaque information
  //! @param client XrdSecEntity of client
  //!
  //! @return SFS_OK if succcessful, otherwise SFS_ERROR
  //--------------------------------------------------------------------------
  int ProcessTpcOpaque(std::string& opaque, const XrdSecEntity* client);

  //--------------------------------------------------------------------------
  //! Process open opaque information - this comes directly from the client
  //! or from the MGM redirection but it's not encrypted but sent in plain
  //! text in the URL
  //!
  //! @return SFS_OK if successful, otherwise SFS_ERROR
  //--------------------------------------------------------------------------
  int ProcessOpenOpaque();

  //--------------------------------------------------------------------------
  //! Process cap opaque information - decisions that need to be taken based
  //! on the ecrypted opaque info
  //!
  //! @param is_repair_read flag if this is a repair read
  //! @param vid client virtual identity
  //!
  //! @return SFS_OK if successful, otherwise SFS_ERROR
  //--------------------------------------------------------------------------
  int ProcessCapOpaque(bool& is_repair_read,
                       eos::common::VirtualIdentity& vid);

  //--------------------------------------------------------------------------
  //! Process mixed opaque information - decisions that need to be taken based
  //! on both the ecrypted and un-encrypted opaque info
  //!
  //! @return SFS_OK if successful, otherwise SFS_ERROR
  //--------------------------------------------------------------------------
  int ProcessMixedOpaque();

  //--------------------------------------------------------------------------
  //! Compute total time to serve read requests
  //--------------------------------------------------------------------------
  void AddReadTime();

  //--------------------------------------------------------------------------
  //! Compute total time to serve vector read requests
  //--------------------------------------------------------------------------
  void AddReadVTime();

  //--------------------------------------------------------------------------
  //! Compute total time to serve write requests
  //--------------------------------------------------------------------------
  void AddWriteTime();

  //--------------------------------------------------------------------------
  //! Compute general statistics on a set of input values
  //!
  //! @param vect input collection
  //! @param min miniumum element
  //! @param max maximum element
  //! @param sum sum of the elements
  //! @param avg average value
  //! @param sigma sigma of the elements
  //--------------------------------------------------------------------------
  template <typename T>
  void ComputeStatistics(const std::vector<T>& vect, T& min, T& max,
                         T& sum, double& sigma)
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
  //! Check if layout encoding indicates a RAIN layout
  //!
  //! @param lid layout id encoding
  //!
  //! @return true if RAIN layout, otherwise false
  //----------------------------------------------------------------------------
  static bool IsRainLayout(unsigned long long lid);

  //----------------------------------------------------------------------------
  //! Extract logid from the opaque info i.e. mgm.logid
  //!
  //! @param opaque opaque info
  //----------------------------------------------------------------------------
  std::string ExtractLogId(const char* opaque) const;

#ifdef IN_TEST_HARNESS
public:
#endif

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
  //! @return true if parsing succcessful and hostname stores the desired value,
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

  int mTpcThreadStatus; ///< status of the TPC thread - 0 valid otherwise error
  pthread_t mTpcThread; ///< thread doing the TPC transfer
  TpcState_t mTpcState; ///< uses kTPCXYZ enums to tag the TPC state
  XrdOfsTPCInfo mTpcInfo; ///< TPC info object used for callback
  XrdSysMutex mTpcJobMutex; ///< TPC job mutex
  int mTpcRetc; ///< TPC job return code
  uint16_t mTimeout; ///< timeout for layout operations

  //----------------------------------------------------------------------------
  //! Notify the workflow protobuf endpoint that the user has closed a file that
  //! they were writing to
  //!
  //! @param fmd The metadata of the file
  //! @param ownerName The name of user that owns the file
  //! @param ownerGroupName The name of the group that owns the file
  //! @param requestorName Tha name of the user that closed the file
  //! @param requestorGroupName The name of the group that closed the file
  //! @param instanceName Tha name of the EOS instance
  //! @param fullPath the full path of the file
  //! @param managerName The name of the EOS manager
  //! @param xattrs The extended attributes of teh file to be passed to the
  //! workflow protobuf endpoint
  //! @param errMsgBack Output parameter: Error message back from the workflow
  //! protobuf endpoint
  //! @return
  //----------------------------------------------------------------------------
  int NotifyProtoWfEndPointClosew(const Fmd& fmd, const string& ownerName,
                                  const string& ownerGroupName, const string& requestorName,
                                  const string& requestorGroupName, const string& instanceName,
                                  const string& fullPath, const string& managerName,
                                  const std::map<std::string, std::string>& xattrs, string& errMsgBack);

  //----------------------------------------------------------------------------
  //! Send archive failed event to the manager
  //!
  //! @param fid The file identifier
  //! @param errMsg The error message to enclosed in the archive failed event
  //! @return SFS_OK if successful
  //----------------------------------------------------------------------------
  int SendArchiveFailedToManager(const uint64_t fid,
                                 const std::string& errMsg);
};

EOSFSTNAMESPACE_END

#endif
