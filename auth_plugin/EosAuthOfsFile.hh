//------------------------------------------------------------------------------
// File: EosAuthOfsFile.hh
// Author: Elvin-Alin Sindrilau <esindril@cern.ch> CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2013 CERN/Switzerland                                  *
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

#ifndef __EOSAUTH_OFSFILE__HH__
#define __EOSAUTH_OFSFILE__HH__

/*----------------------------------------------------------------------------*/
#include "common/Logging.hh"
#include "common/Mapping.hh"
#include "Namespace.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSec/XrdSecEntity.hh"
#include "XrdSfs/XrdSfsInterface.hh"
/*----------------------------------------------------------------------------*/

EOSAUTHNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class implementing OFS files
//------------------------------------------------------------------------------
class EosAuthOfsFile: public XrdSfsFile, public eos::common::LogId
{
  public:

    //--------------------------------------------------------------------------
    //! Constructor
    //--------------------------------------------------------------------------
    EosAuthOfsFile(char* user = 0, int MonID = 0);


    //--------------------------------------------------------------------------
    //! Destructor
    //--------------------------------------------------------------------------
    ~EosAuthOfsFile();


    //--------------------------------------------------------------------------
    //! Open a file
    //--------------------------------------------------------------------------
    int open(const char* fileName,
             XrdSfsFileOpenMode openMode,
             mode_t createMode,
             const XrdSecEntity* client,
             const char* opaque = 0);


    //--------------------------------------------------------------------------
    //! Read function
    //--------------------------------------------------------------------------
    virtual XrdSfsXferSize read(XrdSfsFileOffset offset,
                                char* buffer,
                                XrdSfsXferSize length);


    //--------------------------------------------------------------------------
    //! Write function
    //--------------------------------------------------------------------------
    virtual XrdSfsXferSize write(XrdSfsFileOffset offset,
                                 const char* buffer,
                                 XrdSfsXferSize length);


    //--------------------------------------------------------------------------
    //! Stat function
    //--------------------------------------------------------------------------
    virtual int stat(struct stat* buf);


    //--------------------------------------------------------------------------
    //! Close file
    //--------------------------------------------------------------------------
    int close();


    //--------------------------------------------------------------------------
    //! Get name of file
    //--------------------------------------------------------------------------
    const char* FName();


//------------------------------------------------------------------------------
//!!!!!!!!! THE FOLLOWING OPERATIONS ARE NOT SUPPORTED !!!!!!!!!
//------------------------------------------------------------------------------


    //--------------------------------------------------------------------------
    //! fctl fakes ok (not supported)
    //--------------------------------------------------------------------------
    int fctl(int, const char*, XrdOucErrInfo&);


    //--------------------------------------------------------------------------
    //! Return mmap address (not supported)
    //--------------------------------------------------------------------------
    int getMmap(void** Addr, off_t& Size);


    //--------------------------------------------------------------------------
    //! File pre-read fakes ok (not supported)
    //--------------------------------------------------------------------------
    int read(XrdSfsFileOffset fileOffset, XrdSfsXferSize preread_sz);


    //--------------------------------------------------------------------------
    //! File read in async mode (not supported)
    //--------------------------------------------------------------------------
    int read(XrdSfsAio* aioparm);


    //--------------------------------------------------------------------------
    //! File write in async mode (not supported)
    //--------------------------------------------------------------------------
    int write(XrdSfsAio* aiop);


    //--------------------------------------------------------------------------
    //! File sync (not supported)
    //--------------------------------------------------------------------------
    int sync();


    //--------------------------------------------------------------------------
    //! File async sync (not supported)
    //--------------------------------------------------------------------------
    int sync(XrdSfsAio* aiop);


    //--------------------------------------------------------------------------
    //! File truncate (not supported)
    //--------------------------------------------------------------------------
    int truncate(XrdSfsFileOffset flen);


    //--------------------------------------------------------------------------
    //! get checksum info (returns nothing - not supported)
    //--------------------------------------------------------------------------
    int getCXinfo(char cxtype[4], int& cxrsz);


  private:

    std::string mName; ///< file name

    //--------------------------------------------------------------------------
    //! Create an error message for a file object
    //!
    //! @param pfx message prefix value
    //! @param einfo error text/code object
    //! @param ecode error code
    //! @param op name of the operation performed
    //! @param target target of the operation e.g. file name etc.
    //!
    //! @return SFS_ERROR in all cases
    //!
    //! This routines prints also an error message into the EOS log.
    //!
    //--------------------------------------------------------------------------
    int Emsg(const char* pfx,
             XrdOucErrInfo& einfo,
             int ecode,
             const char* op,
             const char* target);

};

EOSAUTHNAMESPACE_END

#endif // __EOSAUTH_OFSFILE_HH__
