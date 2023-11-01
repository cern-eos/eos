//------------------------------------------------------------------------------
// Copyright (c) 2011-2014 by European Organization for Nuclear Research (CERN)
// Author: Michal Simon <michal.simon@cern.ch>
//------------------------------------------------------------------------------
// This file is part of the XRootD software suite.
//
// XRootD is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// XRootD is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with XRootD.  If not, see <http://www.gnu.org/licenses/>.
//
// In applying this licence, CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.
//------------------------------------------------------------------------------

#ifndef SRC_XRDZIP_XRDZIPARCHIVE_HH_
#define SRC_XRDZIP_XRDZIPARCHIVE_HH_

#include "XrdCl/XrdClFile.hh"
#include "XrdCl/XrdClResponseJob.hh"
#include "XrdCl/XrdClJobManager.hh"
#include "XrdCl/XrdClDefaultEnv.hh"
#include "XrdCl/XrdClPostMaster.hh"
#include "XrdZip/XrdZipEOCD.hh"
#include "XrdZip/XrdZipCDFH.hh"
#include "XrdZip/XrdZipZIP64EOCD.hh"
#include "XrdZip/XrdZipLFH.hh"
#include "XrdCl/XrdClZipCache.hh"

#include <memory>
#include <unordered_map>

//-----------------------------------------------------------------------------
// Forward declaration needed for friendship
//-----------------------------------------------------------------------------
namespace XrdEc{ class StrmWriter; class Reader; template<bool> class OpenOnlyImpl; };
class MicroTest;

namespace XrdCl
{
  using namespace XrdZip;

  //---------------------------------------------------------------------------
  // ZipArchive provides following functionalities:
  // - parsing of existing ZIP archive
  // - reading data from existing ZIP archive
  // - appending data to existing ZIP archive
  // - querying stat info and checksum for given file in ZIP archive
  //---------------------------------------------------------------------------
  class ZipArchive
  {
    friend class XrdEc::StrmWriter;
    friend class XrdEc::Reader;
    template<bool>
    friend class XrdEc::OpenOnlyImpl;
    friend class ::MicroTest;

    template<typename RSP>
    friend XRootDStatus ReadFromImpl( ZipArchive&, const std::string&, uint64_t, uint32_t, void*, ResponseHandler*, uint16_t );

    public:
      //-----------------------------------------------------------------------
      //! Constructor
      //-----------------------------------------------------------------------
      ZipArchive( bool enablePlugIns = true );

      //-----------------------------------------------------------------------
      //! Destructor
      //-----------------------------------------------------------------------
      virtual ~ZipArchive();

      //-----------------------------------------------------------------------
      //! Open ZIP Archive (and parse the Central Directory)
      //!
      //! @param url     : the URL of the ZIP archive
      //! @param flags   : open flags to be used when openning the file
      //! @param handler : user callback
      //! @param timeout : operation timeout
      //! @return        : the status of the operation
      //-----------------------------------------------------------------------
      XRootDStatus OpenArchive( const std::string  &url,
                                OpenFlags::Flags    flags,
                                ResponseHandler    *handler,
                                uint16_t            timeout = 0 );

      //-----------------------------------------------------------------------
      //! Open a file within the ZIP Archive
      //!
      //! @param fn    : file name to be opened
      //! @param flags : open flags (either 'Read' or 'New | Write')
      //! @param size  : file size (to be included in the LFH)
      //! @param crc32 : file crc32 (to be included in the LFH)
      //! @return        : the status of the operation
      //-----------------------------------------------------------------------
      XRootDStatus OpenFile( const std::string  &fn,
                             OpenFlags::Flags    flags = OpenFlags::None,
                             uint64_t            size  = 0,
                             uint32_t            crc32 = 0 );

      //-----------------------------------------------------------------------
      //! Read data from an open file
      //!
      //! @param offset  : offset within the file to read at
      //! @param size    : number of bytes to be read
      //! @param buffer  : the buffer for the data
      //! @param handler : user callback
      //! @param timeout : operation timeout
      //! @return        : the status of the operation
      //-----------------------------------------------------------------------
      inline
      XRootDStatus Read( uint64_t         offset,
                         uint32_t         size,
                         void            *buffer,
                         ResponseHandler *handler,
                         uint16_t         timeout = 0 )
      {
        if( openfn.empty() ) return XRootDStatus( stError, errInvalidOp );
        return ReadFrom( openfn, offset, size, buffer, handler, timeout );
      }

      //-----------------------------------------------------------------------
      //! PgRead data from an open file
      //!
      //! @param offset  : offset within the file to read at
      //! @param size    : number of bytes to be read
      //! @param buffer  : the buffer for the data
      //! @param handler : user callback
      //! @param timeout : operation timeout
      //! @return        : the status of the operation
      //-----------------------------------------------------------------------
      inline
      XRootDStatus PgRead( uint64_t         offset,
                           uint32_t         size,
                           void            *buffer,
                           ResponseHandler *handler,
                           uint16_t         timeout = 0 )
      {
        if( openfn.empty() ) return XRootDStatus( stError, errInvalidOp );
        return PgReadFrom( openfn, offset, size, buffer, handler, timeout );
      }

      //-----------------------------------------------------------------------
      //! Read data from a given file
      //!
      //! @param fn      : the name of the file from which we are going to read
      //! @param offset  : offset within the file to read at
      //! @param size    : number of bytes to be read
      //! @param buffer  : the buffer for the data
      //! @param handler : user callback
      //! @param timeout : operation timeout
      //! @return        : the status of the operation
      //-----------------------------------------------------------------------
      XRootDStatus ReadFrom( const std::string &fn,
                             uint64_t           offset,
                             uint32_t           size,
                             void              *buffer,
                             ResponseHandler   *handler,
                             uint16_t           timeout = 0 );

      //-----------------------------------------------------------------------
      //! PgRead data from a given file
      //!
      //! @param fn      : the name of the file from which we are going to read
      //! @param offset  : offset within the file to read at
      //! @param size    : number of bytes to be read
      //! @param buffer  : the buffer for the data
      //! @param handler : user callback
      //! @param timeout : operation timeout
      //! @return        : the status of the operation
      //-----------------------------------------------------------------------
      XRootDStatus PgReadFrom( const std::string &fn,
                               uint64_t           offset,
                               uint32_t           size,
                               void              *buffer,
                               ResponseHandler   *handler,
                               uint16_t           timeout = 0 );

      //-----------------------------------------------------------------------
      //! Append data to a new file
      //!
      //! @param size    : number of bytes to be appended
      //! @param buffer  : the buffer with the data to be appended
      //! @param handler : user callback
      //! @param timeout : operation timeout
      //! @return        : the status of the operation
      //-----------------------------------------------------------------------
      inline XRootDStatus Write( uint32_t          size,
                                 const void       *buffer,
                                 ResponseHandler  *handler,
                                 uint16_t          timeout = 0 )
      {
        if( openstage != Done || openfn.empty() )
          return XRootDStatus( stError, errInvalidOp, 0, "Archive not opened." );

        return WriteImpl( size, buffer, handler, timeout );
      }

      //-----------------------------------------------------------------------
      //! Update the metadata of the currently open file
      //!
      //! @param crc32   : the crc32 checksum
      //! @return        : the status of the operation
      //-----------------------------------------------------------------------
      XRootDStatus UpdateMetadata( uint32_t crc32 );

      //-----------------------------------------------------------------------
      //! Create a new file in the ZIP archive and append the data
      //!
      //! @param fn      : the name of the new file to be created
      //! @param crc32   : the crc32 of the file
      //! @param size    : the size of the file
      //! @param buffer  : the buffer with the data
      //! @param handler : user callback
      //! @param timeout : operation timeout
      //! @return        : the status of the operation
      //-----------------------------------------------------------------------
      XRootDStatus AppendFile( const std::string &fn,
                               uint32_t           crc32,
                               uint32_t           size,
                               const void        *buffer,
                               ResponseHandler   *handler,
                               uint16_t           timeout = 0 );

      //-----------------------------------------------------------------------
      //! Get stat info for given file
      //!
      //! @param fn   : the name of the file
      //! @param info : output parameter
      //! @return     : the status of the operation
      //-----------------------------------------------------------------------
      inline XRootDStatus Stat( const std::string &fn, StatInfo *&info )
      { // make sure archive has been opened and CD has been parsed
        if( openstage != Done )
          return XRootDStatus( stError, errInvalidOp );
        // make sure the file is part of the archive
        auto cditr = cdmap.find( fn );
        if( cditr == cdmap.end() )
          return XRootDStatus( stError, errNotFound );
        // create the result
        info = make_stat( fn );
        if (info) 
          return XRootDStatus();
        else // have difficult to access the openned archive.
          return XRootDStatus( stError, errNotFound );
      }

      //-----------------------------------------------------------------------
      //! Get stat info for an open file
      //!
      //! @param info : output parameter
      //! @return     : the status of the operation
      //-----------------------------------------------------------------------
      inline XRootDStatus Stat( StatInfo *&info )
      {
        if( openfn.empty() )
          return XRootDStatus( stError, errInvalidOp );
        return Stat( openfn, info );
      }

      //-----------------------------------------------------------------------
      //! Get crc32 for a given file
      //!
      //! @param fn    : file name
      //! @param cksum : output parameter
      //! @return      : the status of the operation
      //-----------------------------------------------------------------------
      inline XRootDStatus GetCRC32( const std::string &fn, uint32_t &cksum )
      { // make sure archive has been opened and CD has been parsed
        if( openstage != Done )
          return XRootDStatus( stError, errInvalidOp );
        // make sure the file is part of the archive
        auto cditr = cdmap.find( fn );
        if( cditr == cdmap.end() )
          return XRootDStatus( stError, errNotFound );
        cksum = cdvec[cditr->second]->ZCRC32;
        return XRootDStatus();
      }

      inline XRootDStatus GetOffset( const std::string &fn, uint64_t &offset){
    	  if( openstage != XrdCl::ZipArchive::Done || !archive.IsOpen() )
    	  	        return XrdCl::XRootDStatus( XrdCl::stError, XrdCl::errInvalidOp );

    	  	      auto cditr = cdmap.find( fn );
    	  	      if( cditr == cdmap.end() )
    	  	        return XrdCl::XRootDStatus( XrdCl::stError, XrdCl::errNotFound,
    	  	        		XrdCl::errNotFound, "File not found." );

    	  	      XrdCl::CDFH *cdfh = cdvec[cditr->second].get();

    	  	      // check if the file is compressed, for now we only support uncompressed and inflate/deflate compression
    	  	      if( cdfh->compressionMethod != 0 && cdfh->compressionMethod != Z_DEFLATED )
    	  	        return XrdCl::XRootDStatus( XrdCl::stError, XrdCl::errNotSupported,
    	  	                             0, "The compression algorithm is not supported!" );

    	  	      // Now the problem is that at the beginning of our
    	  	      // file there is the Local-file-header, which size
    	  	      // is not known because of the variable size 'extra'
    	  	      // field, so we need to know the offset of the next
    	  	      // record and shift it by the file size.
    	  	      // The next record is either the next LFH (next file)
    	  	      // or the start of the Central-directory.
    	  	      uint64_t cdOffset = zip64eocd ? zip64eocd->cdOffset : eocd->cdOffset;
    	  	      uint64_t nextRecordOffset = ( cditr->second + 1 < cdvec.size() ) ?
    	  	    		  XrdCl::CDFH::GetOffset( *cdvec[cditr->second + 1] ) : cdOffset;
    	  	      uint64_t filesize = cdfh->compressedSize;
    	  	      if( filesize == std::numeric_limits<uint32_t>::max() && cdfh->extra )
    	  	        filesize = cdfh->extra->compressedSize;
    	  	      uint16_t descsize = cdfh->HasDataDescriptor() ?
    	  	    		  XrdCl::DataDescriptor::GetSize( cdfh->IsZIP64() ) : 0;
    	  	      offset  = nextRecordOffset - filesize - descsize;
    	  	      return XrdCl::XRootDStatus();
      }

      //-----------------------------------------------------------------------
      //! Create the central directory at the end of ZIP archive and close it
      //
      //! @param handler : user callback
      //! @param timeout : operation timeout
      //! @return        : the status of the operation
      //-----------------------------------------------------------------------
      XRootDStatus CloseArchive( ResponseHandler *handler,
                                 uint16_t         timeout = 0 );

      //-----------------------------------------------------------------------
      //! Close an open file within the ZIP archive
      //! @return : the status of the operation
      //-----------------------------------------------------------------------
      inline XRootDStatus CloseFile()
      {
        if( openstage != Done || openfn.empty() )
          return XRootDStatus( stError, errInvalidOp,
                               0, "Archive not opened." );
        openfn.clear();
        lfh.reset();
        return XRootDStatus();
      }

      //-----------------------------------------------------------------------
      //! List files in the ZIP archive
      //! @return : the status of the operation
      //-----------------------------------------------------------------------
      XRootDStatus List( DirectoryList *&list );

      //-----------------------------------------------------------------------
      //! @return : true if ZIP archive has been successfully opened
      //-----------------------------------------------------------------------
      inline bool IsOpen()
      {
        return openstage == Done;
      }

      //------------------------------------------------------------------------
      //! Check if the underlying file is using an encrypted connection
      //------------------------------------------------------------------------
      inline bool IsSecure()
      {
        return archive.IsSecure();
      }

      //-----------------------------------------------------------------------
      //! Set property on the underlying File object
      //-----------------------------------------------------------------------
      inline bool SetProperty( const std::string &name, const std::string &value )
      {
        return archive.SetProperty( name, value );
      }

      //-----------------------------------------------------------------------
      //! Get property on the underlying File object
      //-----------------------------------------------------------------------
      inline bool GetProperty( const std::string &name, std::string &value )
      {
        return archive.GetProperty( name, value );
      }

      //-----------------------------------------------------------------------
      //! Get the underlying File object
      //-----------------------------------------------------------------------
      inline File& GetFile()
      {
        return archive;
      }

    private:

      //-----------------------------------------------------------------------
      //! Append data to a new file, implementation
      //!
      //! @param size    : number of bytes to be appended
      //! @param buffer  : the buffer with the data to be appended
      //! @param handler : user callback
      //! @param timeout : operation timeout
      //! @return        : the status of the operation
      //-----------------------------------------------------------------------
      XRootDStatus WriteImpl( uint32_t               size,
                              const void            *buffer,
                              ResponseHandler       *handler,
                              uint16_t               timeout );

      //-----------------------------------------------------------------------
      //! Open the ZIP archive in read-only mode without parsing the central
      //! directory.
      //!
      //! @param url     : url of the ZIP archive
      //! @param handler : user callback
      //! @param timeout : operation timeout
      //! @return        : operation status
      //-----------------------------------------------------------------------
      XRootDStatus OpenOnly( const std::string  &url,
                             bool                update,
                             ResponseHandler    *handler,
                             uint16_t            timeout = 0 );

      //-----------------------------------------------------------------------
      //! Get a buffer with central directory of the ZIP archive
      //!
      //! @return : buffer with central directory
      //-----------------------------------------------------------------------
      buffer_t GetCD();

      //-----------------------------------------------------------------------
      //! Set central directory for the ZIP archive
      //!
      //! @param buffer : a buffer with the central directory to be set
      //-----------------------------------------------------------------------
      void SetCD( const buffer_t &buffer );

      //-----------------------------------------------------------------------
      //! Package a response into AnyObject (erase the type)
      //!
      //! @param rsp : the response to be packaged
      //! @return    : AnyObject with the response
      //-----------------------------------------------------------------------
      template<typename Response>
      inline static AnyObject* PkgRsp( Response *rsp )
      {
        if( !rsp ) return nullptr;
        AnyObject *pkg = new AnyObject();
        pkg->Set( rsp );
        return pkg;
      }

      //-----------------------------------------------------------------------
      //! Free status and response
      //-----------------------------------------------------------------------
      template<typename Response>
      inline static void Free( XRootDStatus *st, Response *rsp )
      {
        delete st;
        delete rsp;
      }

      //-----------------------------------------------------------------------
      //! Schedule a user callback to be executed in the thread-pool with given
      //! status and response.
      //!
      //! @param handler : user callback to be scheduled
      //! @param st      : status to be passed to the callback
      //! @param rsp     : response to be passed to the callback
      //-----------------------------------------------------------------------
      template<typename Response>
      inline static void Schedule( ResponseHandler *handler, XRootDStatus *st, Response *rsp = nullptr )
      {
        if( !handler ) return Free( st, rsp );
        ResponseJob *job = new ResponseJob( handler, st, PkgRsp( rsp ), 0 );
        DefaultEnv::GetPostMaster()->GetJobManager()->QueueJob( job );
      }

      //-----------------------------------------------------------------------
      //! Create a StatInfo object from ZIP archive stat info and the file size.
      //!
      //! @param starch : ZIP archive stat info
      //! @param size   : file size
      //! @return       : StatInfo object
      //-----------------------------------------------------------------------
      inline static StatInfo* make_stat( const StatInfo &starch, uint64_t size )
      {
        StatInfo *info = new StatInfo( starch );
        uint32_t flags = info->GetFlags();
        info->SetFlags( flags & ( ~StatInfo::IsWritable ) ); // make sure it is not listed as writable
        info->SetSize( size );
        return info;
      }

      //-----------------------------------------------------------------------
      //! Create a StatInfo object for a given file within the ZIP archive.
      //!
      //! @param fn : file name
      //! @return   : StatInfo object for the given file
      //-----------------------------------------------------------------------
      inline StatInfo* make_stat( const std::string &fn )
      {
        StatInfo *infoptr = 0;
        XRootDStatus st = archive.Stat( false, infoptr );
        if (!st.IsOK()) return nullptr;
        std::unique_ptr<StatInfo> stinfo( infoptr );
        auto itr = cdmap.find( fn );
        if( itr == cdmap.end() ) return nullptr;
        size_t index = itr->second;
        uint64_t uncompressedSize = cdvec[index]->uncompressedSize;
        if( cdvec[index]->extra && uncompressedSize == std::numeric_limits<uint32_t>::max() )
          uncompressedSize = cdvec[index]->extra->uncompressedSize;
        return make_stat( *stinfo, uncompressedSize );
      }

      //-----------------------------------------------------------------------
      //! Allocate new XRootDStatus object
      //-----------------------------------------------------------------------
      inline static XRootDStatus* make_status( const XRootDStatus &status = XRootDStatus() )
      {
        return new XRootDStatus( status );
      }

      //-----------------------------------------------------------------------
      //! Clear internal ZipArchive objects
      //-----------------------------------------------------------------------
      inline void Clear()
      {
        buffer.reset();
        eocd.reset();
        cdvec.clear();
        cdmap.clear();
        zip64eocd.reset();
        openstage = None;
      }

      //-----------------------------------------------------------------------
      //! Stages of opening and parsing a ZIP archive
      //-----------------------------------------------------------------------
      enum OpenStages
      {
        None = 0,          //< opening/parsing not started
        HaveEocdBlk,       //< we have the End of Central Directory record
        HaveZip64EocdlBlk, //< we have the ZIP64 End of Central Directory locator record
        HaveZip64EocdBlk,  //< we have the ZIP64 End of Central Directory record
        HaveCdRecords,     //< we have Central Directory records
        Done,              //< we are done parsing the Central Directory
        Error,             //< opening/parsing failed
        NotParsed          //< the ZIP archive has been opened but Central Directory is not parsed
      };

      //-----------------------------------------------------------------------
      //! LFH of a newly appended file (in case it needs to be overwritten)
      //-----------------------------------------------------------------------
      struct NewFile
      {
        NewFile( uint64_t offset, std::unique_ptr<LFH> lfh ) : offset( offset ),
                                                               lfh( std::move( lfh ) ),
                                                               overwrt( false )
        {
        }

        NewFile( NewFile && nf ) : offset( nf.offset ),
                                   lfh( std::move( nf.lfh ) ),
                                   overwrt( nf.overwrt )
        {
        }

        uint64_t             offset;  // the offset of the LFH of the file
        std::unique_ptr<LFH> lfh;     // LFH of the file
        bool                 overwrt; // if true the LFH needs to be overwritten on close
      };

      //-----------------------------------------------------------------------
      //! Type that maps file name to its cache
      //-----------------------------------------------------------------------
      typedef std::unordered_map<std::string, ZipCache> zipcache_t;
      typedef std::unordered_map<std::string, NewFile>  new_files_t;

      File                        archive;   //> File object for handling the ZIP archive
      uint64_t                    archsize;  //> size of the ZIP archive
      bool                        cdexists;  //> true if Central Directory exists, false otherwise
      bool                        updated;   //> true if the ZIP archive has been updated, false otherwise
      std::unique_ptr<char[]>     buffer;    //> buffer for keeping the data to be parsed or raw data
      std::unique_ptr<EOCD>       eocd;      //> End of Central Directory record
      cdvec_t                     cdvec;     //> vector of Central Directory File Headers
      cdmap_t                     cdmap;     //> mapping of file name to CDFH index
      uint64_t                    cdoff;     //> Central Directory offset
      uint32_t                    orgcdsz;   //> original CD size
      uint32_t                    orgcdcnt;  //> original number CDFH records
      buffer_t                    orgcdbuf;  //> buffer with the original CDFH records
      std::unique_ptr<ZIP64_EOCD> zip64eocd; //> ZIP64 End of Central Directory record
      OpenStages                  openstage; //> stage of opening / parsing a ZIP archive
      std::string                 openfn;    //> file name of opened file
      zipcache_t                  zipcache;  //> cache for inflating compressed data
      std::unique_ptr<LFH>        lfh;       //> Local File Header record for the newly appended file
      bool                        ckpinit;   //> a flag indicating whether a checkpoint has been initialized
      new_files_t                 newfiles;  //> all newly appended files
  };

} /* namespace XrdZip */

#endif /* SRC_XRDZIP_XRDZIPARCHIVE_HH_ */
