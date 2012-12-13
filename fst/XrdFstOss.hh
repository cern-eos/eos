//------------------------------------------------------------------------------
//! @file XrdFstOss.hh
//! @author Elvin-Alin Sindrilaru - CERN
//! @brief Oss plugin for EOS doing block checksumming for files
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

#ifndef __EOSFST_FSTOSS_HH__
#define __EOSFST_FSTOSS_HH__

/*----------------------------------------------------------------------------*/
#include <map>
#include <string>
/*----------------------------------------------------------------------------*/
#include "fst/Namespace.hh"
#include "fst/checksum/CheckSum.hh"
#include "fst/XrdFstOssFile.hh"
#include "common/Logging.hh"
#include "common/Namespace.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOss/XrdOssApi.hh"
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class XrdFstOss
//------------------------------------------------------------------------------
class XrdFstOss: public XrdOssSys, public eos::common::LogId
{
  public:

    int          mFdFence;   ///< smalest file FD number allowed
    int          mFdLimit;   ///< largest file FD number allowed

    //--------------------------------------------------------------------------
    //! Constuctor
    //--------------------------------------------------------------------------
    XrdFstOss();


    //--------------------------------------------------------------------------
    //! Destructor
    //--------------------------------------------------------------------------
    virtual ~XrdFstOss();


    //--------------------------------------------------------------------------
    //! Init function
    //!
    //! @param lp system logger
    //! @param configfn configuration file
    //!
    //! @return 0 upon success, -errno otherwise
    //!
    //--------------------------------------------------------------------------
    int Init( XrdSysLogger* lp, const char* configfn );


    //--------------------------------------------------------------------------
    //! Unlink a file
    //!
    //! @param path fully qualified name of the file to be removed
    //! @param opts extra options
    //! @param ep enviroment information
    //!
    //! @return XrdOssOK upon success, -errno otherwise
    //!
    //--------------------------------------------------------------------------
    virtual int Unlink( const char* path, int opts = 0, XrdOucEnv* ep = 0 );


    //--------------------------------------------------------------------------
    //! New file
    //!
    //! @param tident
    //!
    //! @return new oss file object
    //!
    //--------------------------------------------------------------------------
    virtual XrdOssDF* newFile( const char* tident );


    //--------------------------------------------------------------------------
    //! New directory
    //!
    //! @param tident
    //!
    //! @return new oss directory object
    //!
    //--------------------------------------------------------------------------
    virtual XrdOssDF* newDir( const char* tident );


    //--------------------------------------------------------------------------
    //! Add new entry to file <-> block checksum map
    //!
    //! @param fileName name of the file added to the mapping
    //! @param blockXs the blockxs object
    //! @param isRW tell if file is opened in read/write mode
    //!
    //! @return mutex for accessing the blockxs object
    //!
    //--------------------------------------------------------------------------
    XrdSysRWLock* AddMapping( const std::string& fileName,
                              CheckSum*&         blockXs,
                              bool               isRW );


    //--------------------------------------------------------------------------
    //! Get block checksum object for a file name
    //!
    //! @param fileName file name for which we search for a xs obj
    //! @param isRW mark if file is opened in read/write mode
    //!
    //! @return pair containing the the boockxs obj and its corresponding mutex
    //!
    //--------------------------------------------------------------------------
    std::pair<XrdSysRWLock*, CheckSum*> GetXsObj( const std::string& fileName,
                                                  bool               isRW );


    //--------------------------------------------------------------------------
    //! Drop block checksum object for a filname
    //!
    //! @param fileName file name entry to be dropped from the map
    //! @param force mark if removal is to be forced
    //!
    //--------------------------------------------------------------------------
    void DropXs( const std::string& fileName, bool force = false );

  
  private:

    XrdSysRWLock mRWMap;     ///< rw lock for the file <-> xs map

    //! map between file names and block xs objects
    std::map< std::string, std::pair<XrdSysRWLock*, CheckSum*> > mMapFileXs;

};

EOSFSTNAMESPACE_END

#endif // __EOSFST_FSTOSS_HH__

