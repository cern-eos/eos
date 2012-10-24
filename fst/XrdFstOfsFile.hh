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

#ifndef __XRDFST_FSTOFSFILE_HH__
#define __XRDFST_FSTOFSFILE_HH__

/*----------------------------------------------------------------------------*/
#include <sys/types.h>
/*----------------------------------------------------------------------------*/
#include "common/Logging.hh"
#include "common/Fmd.hh"
#include "common/ClientAdmin.hh"
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

EOSFSTNAMESPACE_BEGIN

// Forward declaration 
class Layout;
//class ReplicaParLayout;
//class RaidMetaLayout;
//class RaidDpLayout;
//class ReedSLayout;


//------------------------------------------------------------------------------
//! Class
//------------------------------------------------------------------------------
class XrdFstOfsFile : public XrdOfsFile, public eos::common::LogId
{
  friend class ReplicaParLayout;
  friend class RaidMetaLayout;
  friend class RaidDpLayout;
  friend class ReedSLayout;
  
  public:

    //--------------------------------------------------------------------------
    //! Constructor
    //!
    //! @param user
    //! @param MonId
    //
    //--------------------------------------------------------------------------
    XrdFstOfsFile( const char* user, int MonID = 0 );


    //--------------------------------------------------------------------------
    //! Destructor
    //--------------------------------------------------------------------------
    virtual ~XrdFstOfsFile();


    //--------------------------------------------------------------------------
    //!
    //--------------------------------------------------------------------------
    int          openofs( const char*          fileName,
                          XrdSfsFileOpenMode   openMode,
                          mode_t               createMode,
                          const XrdSecEntity*  client,
                          const char*          opaque = 0,
                          bool                 openBlockXS = false,
                          unsigned long        lid = 0 );

  
    //--------------------------------------------------------------------------
    //!
    //--------------------------------------------------------------------------
    int          open( const char*         fileName,
                       XrdSfsFileOpenMode  openMode,
                       mode_t              createMode,
                       const XrdSecEntity* client,
                       const char*         opaque = 0 );


    //--------------------------------------------------------------------------
    //!
    //--------------------------------------------------------------------------
    int          closeofs();


    //--------------------------------------------------------------------------
    //!
    //--------------------------------------------------------------------------
    int          close();

  
    //--------------------------------------------------------------------------
    //!
    //--------------------------------------------------------------------------
    int          read( XrdSfsFileOffset   fileOffset,  // Preread only
                       XrdSfsXferSize     amount );


    //--------------------------------------------------------------------------
    //!
    //--------------------------------------------------------------------------
    XrdSfsXferSize read( XrdSfsFileOffset   fileOffset,
                         char*              buffer,
                         XrdSfsXferSize     buffer_size );



    //--------------------------------------------------------------------------
    //!
    //--------------------------------------------------------------------------
    XrdSfsXferSize readofs( XrdSfsFileOffset   fileOffset,
                            char*              buffer,
                            XrdSfsXferSize     buffer_size );


    //--------------------------------------------------------------------------
    //!
    //--------------------------------------------------------------------------
    int          read( XrdSfsAio* aioparm );


    //--------------------------------------------------------------------------
    //!
    //--------------------------------------------------------------------------
    XrdSfsXferSize write( XrdSfsFileOffset   fileOffset,
                          const char*        buffer,
                          XrdSfsXferSize     buffer_size );


    //--------------------------------------------------------------------------
    //!
    //--------------------------------------------------------------------------
    XrdSfsXferSize writeofs( XrdSfsFileOffset   fileOffset,
                             const char*        buffer,
                             XrdSfsXferSize     buffer_size );


    //--------------------------------------------------------------------------
    //!
    //--------------------------------------------------------------------------
    int          write( XrdSfsAio* aioparm );


    //--------------------------------------------------------------------------
    //!
    //--------------------------------------------------------------------------
    int          stat( struct stat* buf );

  
    //--------------------------------------------------------------------------
    //!
    //--------------------------------------------------------------------------
    bool         verifychecksum();


    //--------------------------------------------------------------------------
    //!
    //--------------------------------------------------------------------------
    int          sync();


    //--------------------------------------------------------------------------
    //!
    //--------------------------------------------------------------------------
    int          syncofs();


    //--------------------------------------------------------------------------
    //!
    //--------------------------------------------------------------------------
    int          sync( XrdSfsAio* aiop );


    //--------------------------------------------------------------------------
    //!
    //--------------------------------------------------------------------------
    int          truncate( XrdSfsFileOffset   fileOffset );


    //--------------------------------------------------------------------------
    //!
    //--------------------------------------------------------------------------
    int          truncateofs( XrdSfsFileOffset   fileOffset );


    //--------------------------------------------------------------------------
    //!
    //--------------------------------------------------------------------------
    std::string  GetFstPath();


  protected:
    XrdOucEnv*   openOpaque;
    XrdOucEnv*   capOpaque;
    XrdOucString fstPath;
    CheckSum*    fstBlockXS;
    off_t        bookingsize;
    off_t        targetsize;
    off_t        minsize;
    off_t        maxsize;
    bool         viaDelete;
    bool         remoteDelete;
    bool         writeDelete;
    unsigned long long fstBlockSize;


    XrdOucString Path;
    XrdOucString localPrefix;
    XrdOucString RedirectManager; // -> host where we bounce back
    XrdOucString SecString;
    XrdSysMutex  ChecksumMutex;

    unsigned long long fileid; // file id
    unsigned long fsid;        // file system id
    unsigned long lid;         // layout id
    unsigned long long cid;    // container id

    XrdOucString hostName;

    bool         closed;
    bool         opened;
    bool         haswrite;
    bool         isRW;
    bool         isCreation;
    bool         isReplication;
    bool         deleteOnClose;
    bool         repairOnClose;

    enum {
      kOfsIoError=1,
      kOfsMaxSizeError=2,
      kOfsDiskFullError=3,
      kOfsSimulatedIoError=4
    };
  
    int          writeErrorFlag; // uses the above enums to specify

    FmdSqlite*   fMd;
    eos::fst::CheckSum* checkSum;
    Layout*  layOut;

    unsigned long long maxOffsetWritten; // largest byte position written of a new created file

    off_t        openSize;
    off_t        closeSize;

    ///////////////////////////////////////////////////////////
    // file statistics
    struct timeval openTime;
    struct timeval closeTime;
    struct timezone tz;
    XrdSysMutex vecMutex;                 // protecting the rvec/wvec variables
    std::vector<unsigned long long> rvec; // vector with all read  sizes -> to compute sigma,min,max,total
    std::vector<unsigned long long> wvec; // vector with all write sizes -> to compute sigma,min,max,total
    unsigned long long rBytes;  ///< sum bytes read
    unsigned long long wBytes;  ///< sum bytes written
    unsigned long long srBytes; ///< sum bytes seeked
    unsigned long long swBytes; ///< sum bytes seeked
    unsigned long rCalls;       ///< number of read calls
    unsigned long wCalls;       ///< number of write calls
    unsigned long long rOffset; ///< offset since last read operation on this file
    unsigned long long wOffset; ///< offset since last write operation on this file

    struct timeval cTime;       ///< current time
    struct timeval lrTime;      ///< last read time
    struct timeval lwTime;      ///< last write time
    struct timeval rTime;       ///< sum time to serve read requests in ms
    struct timeval wTime;       ///< sum time to serve write requests in ms
    XrdOucString   tIdent;      ///< tident


    //--------------------------------------------------------------------------
    //!
    //--------------------------------------------------------------------------
    void AddReadTime();


    //--------------------------------------------------------------------------
    //!
    //--------------------------------------------------------------------------
    void AddWriteTime();

  
    //--------------------------------------------------------------------------
    //!
    //--------------------------------------------------------------------------
    void MakeReportEnv( XrdOucString& reportString );

};

EOSFSTNAMESPACE_END

#endif



