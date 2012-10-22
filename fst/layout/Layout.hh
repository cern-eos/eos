//------------------------------------------------------------------------------
//! @file Layout.hh
//! @author Andreas-Joachim Peters - CERN
//! @brief Abstraction of the physical layout of a file
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

#ifndef __EOSFST_LAYOUT_HH__
#define __EOSFST_LAYOUT_HH__

/*----------------------------------------------------------------------------*/
#include <sys/types.h>
/*----------------------------------------------------------------------------*/
#include "common/LayoutId.hh"
#include "common/Logging.hh"
#include "fst/Namespace.hh"
#include "fst/XrdFstOfsFile.hh"
#include "fst/layout/FileIo.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdSfs/XrdSfsInterface.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

class XrdFstOfsFile;

//------------------------------------------------------------------------------
//! The truncate offset (1TB) is used to indicate that a file should be deleted
//! during the close as there is no better interface usable via XrdClient to
//! communicate a deletion on a open file
//------------------------------------------------------------------------------
#define EOS_FST_DELETE_FLAG_VIA_TRUNCATE_LEN 1024 * 1024 * 1024 * 1024ll


//------------------------------------------------------------------------------
//! Class which abstracts the physical layout of the file
//------------------------------------------------------------------------------
class Layout: public eos::common::LogId
{
  public:

    //--------------------------------------------------------------------------
    //! Constructor
    //!
    //! @param file file handler
    //!
    //--------------------------------------------------------------------------
    Layout( XrdFstOfsFile* file ):
      mOfsFile( file ) {
      mName = "";
    }


    //--------------------------------------------------------------------------
    //! Constructor
    //!
    //! @param file handler to current file
    //! @param name name of the layout
    //! @param lid layout id
    //! @param client security information
    //! @param outError error information
    //!
    //--------------------------------------------------------------------------
    Layout( XrdFstOfsFile*      file,
            int                 lid,
            const XrdSecEntity* client,
            XrdOucErrInfo*      outError ):
      mLayoutId( lid ),
      mOfsFile( file ),
      mError( outError )
    {
      mSecEntity = const_cast<XrdSecEntity*>( client );
      mName = eos::common::LayoutId::GetLayoutTypeString( mLayoutId );
      mBlockChecksum = eos::common::LayoutId::GetBlockChecksum( lid );
      mIsEntryServer = true;
      mLocalPath = "";
    }


    //--------------------------------------------------------------------------
    //! Destructor
    //--------------------------------------------------------------------------
    virtual ~Layout() {
      
      while ( !mPhysicalFile.empty() ) {
        FileIo* file_io = mPhysicalFile.back();
        mPhysicalFile.pop_back();
        delete file_io;
      }
    }


    //--------------------------------------------------------------------------
    //! Get the name of the layout
    //--------------------------------------------------------------------------
    const char* GetName() {
      return mName.c_str();
    }


    //--------------------------------------------------------------------------
    //! Get path to the local replica
    //--------------------------------------------------------------------------
    const char* GetLocalReplicaPath() {
      return mLocalPath.c_str();
    }


    //--------------------------------------------------------------------------
    //! Get layout id
    //--------------------------------------------------------------------------
    unsigned int GetLayOutId() {
      return mLayoutId;
    }


    //--------------------------------------------------------------------------
    //! Test if we are at the entry server
    //--------------------------------------------------------------------------
    virtual bool IsEntryServer() {
      return mIsEntryServer;
    }


    //--------------------------------------------------------------------------
    //! Open a file of the current layout type
    //!
    //! @param path file path
    //! @param flags open flags
    //! @param mode open mode
    //! @param opaque opaque information
    //!
    //! @return 0 if successfull, error code otherwise
    //!
    //--------------------------------------------------------------------------
    virtual int Open( const std::string&  path,
                      uint16_t            flags,
                      uint16_t            mode,
                      const char*         opaque ) = 0;


    //--------------------------------------------------------------------------
    //! Read from file
    //!
    //! @param offset offset
    //! @param buffer place to hold the read data
    //! @param length length
    //!
    //! @return number fo bytes read
    //!
    //--------------------------------------------------------------------------
    virtual int Read( uint64_t offset,
                      char*    buffer,
                      uint32_t length ) = 0;


    //--------------------------------------------------------------------------
    //! Write to file
    //!
    //! @param offset offset
    //! @paramm buffer data to be written
    //! @param length length
    //!
    //! @return number of bytes written
    //!
    //--------------------------------------------------------------------------
    virtual int Write( uint64_t offset,
                       char*    buffer,
                       uint32_t length ) = 0;


    //--------------------------------------------------------------------------
    //! Truncate
    //!
    //! @param offset truncate file to this value
    //!
    //! @return 0 if successful, error code otherwise
    //!
    //--------------------------------------------------------------------------
    virtual int Truncate( uint64_t offset ) = 0;


    //--------------------------------------------------------------------------
    //! Allocate file space
    //!
    //! @param length space to be allocated
    //!
    //! @return 0 on success, error code otherwise
    //!
    //--------------------------------------------------------------------------
    virtual int Fallocate( uint64_t lenght ) {
      return 0;
    }


    //--------------------------------------------------------------------------
    //! Deallocate file space
    //!
    //! @param fromOffset offset start
    //! @param toOffset offset end
    //!
    //! @return 0 on success, error code otherwise
    //!
    //--------------------------------------------------------------------------
    virtual int Fdeallocate( uint64_t fromOffset,
                             uint64_t toOffset ) {
      return 0;
    }


    //--------------------------------------------------------------------------
    //! Remove file
    //!
    //! @return 0 on success, error code otherwise
    //!
    //--------------------------------------------------------------------------
    virtual int Remove() {
      return 0;
    }


    //--------------------------------------------------------------------------
    //! Sync file to disk
    //!
    //! @return 0 on success, error code otherwise
    //!
    //--------------------------------------------------------------------------
    virtual int Sync() = 0;


    //--------------------------------------------------------------------------
    //! Close file
    //!
    //! @return 0 on success, error code otherwise
    //!
    //--------------------------------------------------------------------------
    virtual int Close() = 0;


    //--------------------------------------------------------------------------
    //! Get stats about the file
    //!
    //! @param buf stat buffer
    //!
    //! @return 0 on success, error code otherwise
    //!
    //--------------------------------------------------------------------------
    virtual int Stat( struct stat* buf ) = 0;

  protected:

    int            mBlockChecksum;      ///<
    bool           mIsEntryServer;      ///< mark entry server
    unsigned int   mLayoutId;           ///< layout id

    XrdOucString   mName;               ///< layout name
    XrdFstOfsFile* mOfsFile;            ///< handler to logical file
    std::string    mLocalPath;          ///< path to local file
    XrdOucErrInfo* mError;              ///< error information for current file
    XrdSecEntity*  mSecEntity;          ///< security information

    std::vector<FileIo*> mPhysicalFile; ///< vector of physical files depending on
                                        ///< access protocol
};

EOSFSTNAMESPACE_END

#endif
