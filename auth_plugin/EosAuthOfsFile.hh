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

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  EosAuthOfsFile(char *user = 0, int MonID = 0);

  
  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~EosAuthOfsFile();


  //----------------------------------------------------------------------------
  //! Open a file
  //----------------------------------------------------------------------------
  int open(const char* fileName,
           XrdSfsFileOpenMode openMode,
           mode_t createMode,
           const XrdSecEntity* client,
           const char* opaque = 0);  

  
  //----------------------------------------------------------------------------
  //! Read function
  //----------------------------------------------------------------------------
  virtual XrdSfsXferSize read(XrdSfsFileOffset offset,
                              char* buffer,
                              XrdSfsXferSize length);


  //----------------------------------------------------------------------------
  //! Write function
  //----------------------------------------------------------------------------
  virtual XrdSfsXferSize write(XrdSfsFileOffset offset,
                               const char* buffer,
                               XrdSfsXferSize length);
  

  //----------------------------------------------------------------------------
  //! Stat function
  //----------------------------------------------------------------------------
  virtual int stat(struct stat *buf);
  
 
  //----------------------------------------------------------------------------
  //! Close file 
  //----------------------------------------------------------------------------
  int close();

  
  //----------------------------------------------------------------------------
  //! Get name of file
  //----------------------------------------------------------------------------
  const char* FName();

  
  //!!! THE FOLLOWING OPERATIONS ARE NOT IMPLEMENTED !!!
  
  //----------------------------------------------------------------------------
  //! fctl fakes ok
  //----------------------------------------------------------------------------
  int fctl (int, const char*, XrdOucErrInfo&)
  {
    return 0;
  }


  //----------------------------------------------------------------------------
  //! Return mmap address (we don't need it)
  //----------------------------------------------------------------------------
  int getMmap (void **Addr, off_t &Size)
  {
    if (Addr) Addr = 0;
    Size = 0;
    return SFS_OK;
  }


  //----------------------------------------------------------------------------
  //! File pre-read fakes ok (we don't need it)
  //----------------------------------------------------------------------------
  int read (XrdSfsFileOffset fileOffset, XrdSfsXferSize preread_sz)
  {
    return SFS_OK;
  }

  //----------------------------------------------------------------------------
  //! File read in async mode (not supported)
  //----------------------------------------------------------------------------
  int read (XrdSfsAio *aioparm)
  {
    static const char *epname = "read";
    return Emsg(epname, error, EOPNOTSUPP, "read", mName.c_str());
  }


  //----------------------------------------------------------------------------
  //! File write in async mode (not supported)
  //----------------------------------------------------------------------------
  int write (XrdSfsAio * aiop)
  {
    static const char *epname = "write";
    return Emsg(epname, error, EOPNOTSUPP, "write", mName.c_str());
  }


  //----------------------------------------------------------------------------
  //! File sync (not supported)
  //----------------------------------------------------------------------------
  int sync ()
  {
    static const char *epname = "sync";
    return Emsg(epname, error, EOPNOTSUPP, "sync", mName.c_str());
  }


  //----------------------------------------------------------------------------
  //! File async sync (not supported)
  //----------------------------------------------------------------------------
  int sync (XrdSfsAio * aiop)
  {
    static const char *epname = "sync";
    return Emsg(epname, error, EOPNOTSUPP, "sync", mName.c_str());
  }


  //----------------------------------------------------------------------------
  //! File truncate (not supported)
  //----------------------------------------------------------------------------
  int truncate (XrdSfsFileOffset flen)
  {
    static const char *epname = "trunc";
    return Emsg(epname, error, EOPNOTSUPP, "truncate", mName.c_str());
  }


  //----------------------------------------------------------------------------
  //! get checksum info (returns nothing - not supported)
  //----------------------------------------------------------------------------
  int getCXinfo (char cxtype[4], int &cxrsz)
  {
    return cxrsz = 0;
  }

  
 private:

  std::string mName; ///< file name

  //----------------------------------------------------------------------------
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
  //----------------------------------------------------------------------------
  int Emsg (const char *pfx,
            XrdOucErrInfo &einfo,
            int ecode,
            const char *op,
            const char *target);
      
};

EOSAUTHNAMESPACE_END

#endif // __EOSAUTH_OFSFILE_HH__
