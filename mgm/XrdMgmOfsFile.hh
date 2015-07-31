// ----------------------------------------------------------------------
// File: XrdMgmOfsFile.hh
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
/**
 * @file   XrdMgmOfsFile.hh
 * 
 * @brief  XRootD OFS plugin class implementing file meta data handling of EOS
 * 
 * Many functions in the MgmOfs interface take CGI parameters. The supported
 * CGI parameter are:
 * "eos.ruid" - uid role the client wants
 * "eos.rgid" - gid role the client wants
 * "eos.space" - space a user wants to use for scheduling a write
 * "eos.checksum" - checksum a file should have
 * "eos.lfn" - use this name as path name not the path parameter (used by prefix
 * redirector MGM's ...
 * "eos.bookingsize" - reserve the requested bytes in a file placement
 * "eos.cli.access=pio" - ask for a parallel open (changes the response of an 
 * open for RAIN layouts)
 * "eos.app" - set the application name reported by monitoring
 * "eos.targetsize" - expected size of a file to be uploaded
 * "eos.blockchecksum=ignore" - disable block checksum verification
 * 
 */
/*----------------------------------------------------------------------------*/

#ifndef __EOSMGM_MGMOFSFILE__HH__
#define __EOSMGM_MGMOFSFILE__HH__

/*----------------------------------------------------------------------------*/
#include "common/Mapping.hh"  
#include "common/Logging.hh"
#include "mgm/Messaging.hh"
#include "mgm/ProcInterface.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucErrInfo.hh"
#include "XrdSec/XrdSecEntity.hh"
#include "XrdSfs/XrdSfsInterface.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/


USE_EOSMGMNAMESPACE

/*----------------------------------------------------------------------------*/
class XrdSfsAio;

/*----------------------------------------------------------------------------*/
//! Class implementing files and operations
/*----------------------------------------------------------------------------*/
class XrdMgmOfsFile : public XrdSfsFile, eos::common::LogId
{
public:
  bool isZeroSizeFile; //< true if the file has 0 size
  
  // ---------------------------------------------------------------------------
  // open a file 
  // ---------------------------------------------------------------------------
  int open (const char *fileName,
            XrdSfsFileOpenMode openMode,
            mode_t createMode,
            const XrdSecEntity *client = 0,
            const char *opaque = 0);

  // ---------------------------------------------------------------------------
  // close a file
  // ---------------------------------------------------------------------------
  int close ();

  // ---------------------------------------------------------------------------
  //! get file name
  // ---------------------------------------------------------------------------

  const char *
  FName ()
  {
    return fname;
  }


  // ---------------------------------------------------------------------------
  //! return mmap address (we don't need it)
  // ---------------------------------------------------------------------------

  int
  getMmap (void **Addr, off_t &Size)
  {
    if (Addr) Addr = 0;
    Size = 0;
    return SFS_OK;
  }

  // ---------------------------------------------------------------------------
  //! file pre-read fakes ok (we don't need it)
  // ---------------------------------------------------------------------------

  int
  read (XrdSfsFileOffset fileOffset,
        XrdSfsXferSize preread_sz)
  {
    return SFS_OK;
  }

  // ---------------------------------------------------------------------------
  // file read used to stream proc command results
  // ---------------------------------------------------------------------------
  XrdSfsXferSize read (XrdSfsFileOffset fileOffset,
                       char *buffer,
                       XrdSfsXferSize buffer_size);

  // ---------------------------------------------------------------------------
  // file read in async mode (not supported)
  // ---------------------------------------------------------------------------
  int read (XrdSfsAio *aioparm);

  XrdSfsXferSize write (XrdSfsFileOffset fileOffset,
                        const char *buffer,
                        XrdSfsXferSize buffer_size);

  // ---------------------------------------------------------------------------
  // file write in async mode (not supported)
  // ---------------------------------------------------------------------------
  int write (XrdSfsAio *aioparm);

  // ---------------------------------------------------------------------------
  //! file sync
  // ---------------------------------------------------------------------------
  int sync ();

  // ---------------------------------------------------------------------------
  //! file sync aio
  // ---------------------------------------------------------------------------
  int sync (XrdSfsAio *aiop);

  // ---------------------------------------------------------------------------
  // file stat 
  // ---------------------------------------------------------------------------
  int stat (struct stat *buf);

  // ---------------------------------------------------------------------------
  // file truncate
  // ---------------------------------------------------------------------------
  int truncate (XrdSfsFileOffset fileOffset);

  // ---------------------------------------------------------------------------
  //! get checksum info (returns nothing - not supported)
  // ---------------------------------------------------------------------------

  int
  getCXinfo (char cxtype[4], int &cxrsz)
  {
    return cxrsz = 0;
  }

  // ---------------------------------------------------------------------------
  //! fctl fakes ok
  // ---------------------------------------------------------------------------

  int
  fctl (int, const char*, XrdOucErrInfo&)
  {
    return 0;
  }

  // ---------------------------------------------------------------------------
  //! error message function
  // ---------------------------------------------------------------------------
  int Emsg (const char *, XrdOucErrInfo&, int, const char *x,
            const char *y = "");

  // ---------------------------------------------------------------------------
  //! Constructor
  // ---------------------------------------------------------------------------

  XrdMgmOfsFile (char *user = 0, int MonID = 0) : XrdSfsFile (user, MonID)
  {
    oh = 0;
    fname = 0;
    openOpaque = 0;
    eos::common::Mapping::Nobody (vid);
    fileId = 0;
    procCmd = 0;
    eos::common::LogId ();
    fmd = 0;
    isZeroSizeFile = false;
  }

  // ---------------------------------------------------------------------------
  // Destructor
  // ---------------------------------------------------------------------------
  virtual ~XrdMgmOfsFile ();

private:

  int oh; //< file handle
  char *fname; //< file name
  XrdOucEnv *openOpaque; //< opaque info given with 'open'
  unsigned long fileId; //< file id
  ProcCommand* procCmd; //< proc command object if a proc command was 'opened'
  eos::IFileMD* fmd; //< file meta data object
  eos::common::Mapping::VirtualIdentity vid; //< virtual ID of the client
};

#endif
