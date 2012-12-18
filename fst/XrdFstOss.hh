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
#include "XrdOss/XrdOss.hh"
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class XrdFstOss
//------------------------------------------------------------------------------
class XrdFstOss: public XrdOss, public eos::common::LogId
{
  public:

    int mFdFence;   ///< smalest file FD number allowed
    int mFdLimit;   ///< largest file FD number allowed

    //--------------------------------------------------------------------------
    //! Constuctor
    //--------------------------------------------------------------------------
    XrdFstOss();


    //--------------------------------------------------------------------------
    //! Destructor
    //--------------------------------------------------------------------------
    virtual ~XrdFstOss();

  
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
    //! Chmod for a file
    //!
    //! @param path file path
    //! @param mode permission to be set
    //! @param eP environmental information
    //!
    //! @return XrdOssOK upon success and (-errno) upon failure.
    //!
    //--------------------------------------------------------------------------
    virtual int Chmod( const char*, mode_t mode, XrdOucEnv* eP = 0 );


    //--------------------------------------------------------------------------
    //! Create a file named 'path' with 'mode' access mode bits set
    //!
    //! @param tident client identity
    //! @param path name of the file to be created
    //! @param mode access mode bits to be set
    //! @param env environmental variable
    //! @param opts Set as follows:
    //!             XRDOSS_mkpath - create dir path if it does not exist.
    //!             XRDOSS_new    - the file must not already exist.
    //!             x00000000     - x are standard open flags (<<8)
    //!
    //! @return XrdOssOK upon success and (-errno) otherwise.
    //!
    //--------------------------------------------------------------------------
    virtual int Create( const char* tident,
                        const char* path ,
                        mode_t      mode,
                        XrdOucEnv&  env,
                        int         opts = 0 );


    //--------------------------------------------------------------------------
    //! Create a directory
    //!
    //! @param path the fully qualified name of the new directory
    //! @param mode the new mode that the directory is to have
    //! @param mkpath if true then it makes the full path
    //! @param envP environmental information
    //!
    //! @return XrdOssOK upon success and (-errno) otherwise
    //!
    //--------------------------------------------------------------------------
    virtual int Mkdir( const char* path,
                       mode_t      mode,
                       int         mkpath = 0,
                       XrdOucEnv*  eP = 0 );


    //--------------------------------------------------------------------------
    //! Delete a directory from the namespace.
    //!
    //! @param path the fully qualified name of the dir to be removed
    //! @param opts options
    //! @param envP environmental information
    //!
    //! @return XrdOssOK upon success and (-errno) otherwise
    //--------------------------------------------------------------------------
    virtual int Remdir( const char* path,
                        int         opts = 0,
                        XrdOucEnv*  eP = 0 );


    //--------------------------------------------------------------------------
    //! Renames a file with name 'old_name' to 'new_name'.
    //!
    //! @param old_name the fully qualified name of the file to be renamed
    //! @param new_name the fully qualified name that the file is to have
    //! @param old_env environmental information for old_name
    //! @param new_env environmental information for new_name
    //!
    //! @return XrdOssOK upon success and -errno otherwise
    //--------------------------------------------------------------------------
    int Rename( const char* oldname,
                const char* newname,
                XrdOucEnv*  old_env,
                XrdOucEnv*  new_env );


    //--------------------------------------------------------------------------
    //! Determine if file 'path' actually exists
    //!
    //! @param path the fully qualified name of the file to be tested
    //! @param buff pointer to a 'stat' structure to hold the file attributes
    //! @param opts options
    //! @param env environmental information
    //!
    //! @return XrdOssOK upon success and (-errno) otherwise
    //!
    //--------------------------------------------------------------------------
    virtual int Stat( const char*  path,
                      struct stat* buff,
                      int          opts = 0,
                      XrdOucEnv*   eP = 0 );


    //--------------------------------------------------------------------------
    //! Truncate a file
    //!
    //! @param path the fully qualified name of the target file
    //! @param size the new size that the file is to have
    //! @param envP environmental information
    //!
    //! @return XrdOssOK upon success and (-errno) otherwise
    //!
    //--------------------------------------------------------------------------
    virtual int Truncate( const char*        path,
                          unsigned long long size,
                          XrdOucEnv*         eP = 0 );


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


    //--------------------------------------------------------------------------
    //! Delete link 
    //!
    //! @param path the path of the link
    //! @param statbuff info about the target file
    //!
    //! @return XrdOssOK if successful and (-errno) otherwise
    //!
    //--------------------------------------------------------------------------
    int BreakLink( const char* local_path, struct stat& statbuff );

};

EOSFSTNAMESPACE_END

#endif // __EOSFST_FSTOSS_HH__

