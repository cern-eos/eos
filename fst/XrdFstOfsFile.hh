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

/*----------------------------------------------------------------------------*/
#include <sys/types.h>
#include <numeric>
#include <cmath>
/*----------------------------------------------------------------------------*/
#include "common/Logging.hh"
#include "common/Fmd.hh"
#include "common/SecEntity.hh"
#include "fst/Namespace.hh"
#include "fst/checksum/CheckSum.hh"
#include "fst/FmdSqlite.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOfs/XrdOfs.hh"
#include "XrdOfs/XrdOfsTrace.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucString.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN;

// This defines for reports what is a large seek e.g. > 128 kB = default RA size
#define EOS_FSTOFS_LARGE_SEEKS 128*1024ll

// Forward declaration
class Layout;

//------------------------------------------------------------------------------
//! Class
//------------------------------------------------------------------------------

class XrdFstOfsFile : public XrdOfsFile, public eos::common::LogId {
  friend class ReplicaParLayout;
  friend class RaidMetaLayout;
  friend class RaidDpLayout;
  friend class ReedSLayout;

public:

  static const uint16_t msDefaultTimeout; ///< default timeout value

  //--------------------------------------------------------------------------
  // Constructor
  //--------------------------------------------------------------------------
  XrdFstOfsFile (const char* user, int MonID = 0);


  //--------------------------------------------------------------------------
  // Destructor
  //--------------------------------------------------------------------------
  virtual ~XrdFstOfsFile ();

  
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

  const char* GetETag ()
  {
    return ETag.c_str();
  }

  //--------------------------------------------------------------------------
  //! Enforce an mtime on close
  //--------------------------------------------------------------------------

  void SetForcedMtime (unsigned long long mtime, unsigned long long mtime_ms)
  {
    mForcedMtime = mtime;
    mForcedMtime_ms = mtime_ms;
  }

  //--------------------------------------------------------------------------
  //! Return current mtime while open
  //--------------------------------------------------------------------------

  time_t GetMtime ()
  {
    if (fMd)
      return fMd->fMd.mtime;
    else
      return 0;
  }

  //--------------------------------------------------------------------------
  //!
  //--------------------------------------------------------------------------
  int openofs (const char* fileName,
               XrdSfsFileOpenMode openMode,
               mode_t createMode,
               const XrdSecEntity* client,
               const char* opaque = 0);


  //--------------------------------------------------------------------------
  //!
  //--------------------------------------------------------------------------
  int open (const char* fileName,
            XrdSfsFileOpenMode openMode,
            mode_t createMode,
            const XrdSecEntity* client,
            const char* opaque = 0);


  //--------------------------------------------------------------------------
  //!
  //--------------------------------------------------------------------------
  int closeofs ();

  //--------------------------------------------------------------------------
  //!
  //--------------------------------------------------------------------------
  int close ();


  //--------------------------------------------------------------------------
  //!
  //--------------------------------------------------------------------------
  int read (XrdSfsFileOffset fileOffset, // Preread only
            XrdSfsXferSize amount);


  //--------------------------------------------------------------------------
  //!
  //--------------------------------------------------------------------------
  XrdSfsXferSize read (XrdSfsFileOffset fileOffset,
                       char* buffer,
                       XrdSfsXferSize buffer_size);



  //--------------------------------------------------------------------------
  //!
  //--------------------------------------------------------------------------
  XrdSfsXferSize readofs (XrdSfsFileOffset fileOffset,
                          char* buffer,
                          XrdSfsXferSize buffer_size);


  //--------------------------------------------------------------------------
  //! 
  //--------------------------------------------------------------------------
  int read (XrdSfsAio* aioparm);


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
  XrdSfsXferSize write (XrdSfsFileOffset fileOffset,
                        const char* buffer,
                        XrdSfsXferSize buffer_size);


  //--------------------------------------------------------------------------
  //!
  //--------------------------------------------------------------------------
  XrdSfsXferSize writeofs (XrdSfsFileOffset fileOffset,
                           const char* buffer,
                           XrdSfsXferSize buffer_size);


  //--------------------------------------------------------------------------
  //!
  //--------------------------------------------------------------------------
  int write (XrdSfsAio* aioparm);


  //--------------------------------------------------------------------------
  //!
  //--------------------------------------------------------------------------
  int stat (struct stat* buf);


  //--------------------------------------------------------------------------
  //!
  //--------------------------------------------------------------------------
  bool verifychecksum ();


  //--------------------------------------------------------------------------
  //!
  //--------------------------------------------------------------------------
  int sync ();


  //--------------------------------------------------------------------------
  //!
  //--------------------------------------------------------------------------
  int syncofs ();


  //--------------------------------------------------------------------------
  //!
  //--------------------------------------------------------------------------
  int sync (XrdSfsAio* aiop);


  //--------------------------------------------------------------------------
  //!
  //--------------------------------------------------------------------------
  int truncate (XrdSfsFileOffset fileOffset);


  //--------------------------------------------------------------------------
  //!
  //--------------------------------------------------------------------------
  int truncateofs (XrdSfsFileOffset fileOffset);


  //--------------------------------------------------------------------------
  //!
  //--------------------------------------------------------------------------
  std::string GetFstPath ();

  //--------------------------------------------------------------------------
  //! Check if the TpcKey is still valid e.g. member of gOFS.TpcMap
  //--------------------------------------------------------------------------
  bool TpcValid ();

  //--------------------------------------------------------------------------
  //! return the file size seen at open time
  //--------------------------------------------------------------------------

  off_t getOpenSize ()
  {
    return openSize;
  }

  //--------------------------------------------------------------------------
  //! return the file id
  //--------------------------------------------------------------------------

  unsigned long long getFileId ()
  {
    return fileid;
  }

  //--------------------------------------------------------------------------
  //! disable the checksumming before close
  //--------------------------------------------------------------------------
  void disableChecksum()
  {
    if (checkSum)
    {
      eos::fst::CheckSum* tmpSum = checkSum;
      checkSum = 0;
      delete tmpSum;
    }
  }
  
protected:
  XrdOucEnv* openOpaque;
  XrdOucEnv* capOpaque;
  XrdOucString fstPath;
  off_t bookingsize;
  off_t targetsize;
  off_t minsize;
  off_t maxsize;
  bool viaDelete;
  bool remoteDelete;
  bool writeDelete;
  bool store_recovery;

  XrdOucString Path; //! local storage path
  XrdOucString localPrefix; //! prefix on the local storage
  XrdOucString RedirectManager; //! manager host where we bounce back
  XrdOucString SecString; //! string containing security summary
  XrdSysMutex ChecksumMutex; //! mutex protecting the checksum class
  XrdOucString TpcKey; //! TPC key for a tpc file operation
  XrdOucString ETag; //! current and new ETag (recomputed in close)

  bool hasBlockXs; //! mark if file has blockxs assigned
  unsigned long long fileid; //! file id
  unsigned long fsid; //! file system id
  unsigned long lid; //! layout id
  unsigned long long cid; //! container id
  unsigned long long mForcedMtime;
  unsigned long long mForcedMtime_ms;
  XrdOucString hostName; //! our hostname

  bool closed; //! indicator the file is closed
  bool opened; //! indicator that file is opened
  bool haswrite; //! indicator that file was written/modified
  bool hasReadError; //! indicator if a RAIN file could be reconstructed or not
  bool isRW; //! indicator that file is opened for rw
  bool isCreation; //! indicator that a new file is created
  bool isReplication; //! indicator that the opened file is a replica transfer
  bool isReconstruction; //! indicator that the opened file is in a RAIN reconstruction process
  bool deleteOnClose; //! indicator that the file has to be cleaned on close
  bool repairOnClose; //! indicator that the file should get repaired on close
  bool commitReconstruction; //! indicator that this FST has to commmit after reconstruction
  // <- if the reconstructed piece is not existing on disk we commit anyway since it is a creation.
  // <- if it does exist maybe from a previous movement where the replica was not yet deleted, we would register another stripe without deleting one
  // <- there fore we indicate with  openOpaque->Get("eos.pio.commitfs") which filesystem should actually commit during reconstruction

  enum {
    kOfsIoError = 1, //! generic IO error
    kOfsMaxSizeError = 2, //! maximum file size error
    kOfsDiskFullError = 3, //! disk full error
    kOfsSimulatedIoError = 4 //! simulated IO error
  };

  bool isOCchunk; //! indicator this is an OC chunk upload

  int writeErrorFlag; //! uses kOFSxx enums to specify an error condition

  enum {
    kTpcNone = 0, //! no TPC access
    kTpcSrcSetup = 1, //! access setting up a source TPC session
    kTpcDstSetup = 2, //! access setting up a destination TPC session
    kTpcSrcRead = 3, //! read access from a TPC destination
    kTpcSrcCanDo = 4, //! read access to evaluate if source available
  };

  int tpcFlag; //! uses kTpcXYZ enums above to identify TPC access

  enum {
    kTpcIdle = 0, //! TPC is not enabled and not running (no sync received)
    kTpcEnabled = 1, //! TPC is enabled, but not running (1st sync received)
    kTpcRun = 2, //! TPC is running (2nd sync received)
    kTpcDone = 3, //! TPC has finished
  };

  int tpcState; //! uses kTPCXYZ enumgs above to tag the TPC state

  FmdSqlite* fMd; //! pointer to the in-memory file meta data object
  eos::fst::CheckSum* checkSum; //! pointer to a checksum object
  Layout* layOut; //! pointer to a layout object

  unsigned long long maxOffsetWritten; //! largest byte position written of a new created file

  off_t openSize; //! file size when the file was opened
  off_t closeSize; //! file size when the file was closed

 private:
  //----------------------------------------------------------------------------
  // File statistics for monitoring purposes
  //----------------------------------------------------------------------------
  struct timeval openTime; //! time when a file was opened
  struct timeval closeTime; //! time when a file was closed
  struct timezone tz; //! timezone
  XrdSysMutex vecMutex; //! mutex protecting the rvec/wvec variables
  std::vector<unsigned long long> rvec; //! vector with all read  sizes -> to compute sigma,min,max,total
  std::vector<unsigned long long> wvec; //! vector with all write sizes -> to compute sigma,min,max,total
  unsigned long long rBytes; //! sum bytes read
  unsigned long long wBytes; //! sum bytes written
  unsigned long long sFwdBytes; //! sum bytes seeked forward
  unsigned long long sBwdBytes; //! sum bytes seeked backward
  unsigned long long sXlFwdBytes; //! sum bytes with large forward seeks (> EOS_FSTOFS_LARGE_SEEKS)
  unsigned long long sXlBwdBytes; //! sum bytes with large backward seeks (> EOS_FSTOFS_LARGE_SEEKS)
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
  struct stat updateStat; ///< stat struct to check if a file is updated between open-close

  
  //--------------------------------------------------------------------------
  //! Compute total time to serve read requests
  //--------------------------------------------------------------------------
  void AddReadTime ();


  //--------------------------------------------------------------------------
  //! Compute total time to serve vector read requests
  //--------------------------------------------------------------------------
  void AddReadVTime ();

  
  //--------------------------------------------------------------------------
  //! Compute total time to serve write requests
  //--------------------------------------------------------------------------
  void AddWriteTime ();


  //--------------------------------------------------------------------------
  //! Compute general statistics on a set of input values
  //!
  //! @param vect input collection
  //! @param min miniumum element 
  //! @param max maximum element
  //! @param sum sum of the elements
  //! @param avg average value
  //! @param sigma sigma of the elements
  //!
  //--------------------------------------------------------------------------
  template <typename T>
  void ComputeStatistics(std::vector<T> vect, T& min, T& max,
                         T& sum, double& sigma)
  {
    double avg, sum2;
    max = sum = sum2 = avg = sigma = 0;
    min = 0xffffffff;
    sum = std::accumulate(vect.begin(), vect.end(), 0);
    avg = vect.size() ? (1.0 *sum / vect.size()) : 0;

    // For when the compiler will be smart enough 
    //sigma = std::accumulate(begin(vect), end(vect), 0,
    //                      [&avg] (const T& init, const T& elem)
    //                      {
    //                        return elem + std::pwd((elem - avg), 2);
    //                      });

    for (auto it = vect.begin(); it != vect.end(); ++it)
    {
      if (*it > max) max = *it;
      if (*it < min) min = *it;
      sum2 += std::pow((*it - avg), 2);
    }

    sigma = vect.size() ? (sqrt(sum2 / vect.size())) : 0;

    if (min == 0xffffffff)
      min = 0;    
  }
  

  //--------------------------------------------------------------------------
  //! Create report as a string
  //--------------------------------------------------------------------------
  void MakeReportEnv (XrdOucString& reportString);  
};

EOSFSTNAMESPACE_END;

#endif



