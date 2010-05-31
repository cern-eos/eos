//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   Class representing the file metadata
//------------------------------------------------------------------------------

#ifndef EOS_FILE_MD_HH
#define EOS_FILE_MD_HH

#include "Namespace/persistency/Buffer.hh"
#include "Namespace/ContainerMD.hh"

#include <stdint.h>
#include <cstring>
#include <string>
#include <set>

namespace eos
{
  //----------------------------------------------------------------------------
  //! Class holding the metadata information concerning a single file
  //----------------------------------------------------------------------------
  class FileMD
  {
    public:
      //------------------------------------------------------------------------
      // Type definitions
      //------------------------------------------------------------------------
      typedef uint64_t             id_t;
      typedef uint16_t             location_t;
      typedef std::set<location_t> LocationSet;

      //------------------------------------------------------------------------
      //! Constructor
      //------------------------------------------------------------------------
      FileMD( id_t id );

      //------------------------------------------------------------------------
      //! Get file id
      //------------------------------------------------------------------------
      id_t getId() const
      {
        return pId;
      }

      //------------------------------------------------------------------------
      //! Get creation time
      //------------------------------------------------------------------------
      uint64_t getCTime() const
      {
        return pCTime;
      }

      //------------------------------------------------------------------------
      //! Set creation time
      //------------------------------------------------------------------------
      void setCTime( uint64_t ctime )
      {
        pCTime = ctime;
      }

      //------------------------------------------------------------------------
      //! Get size
      //------------------------------------------------------------------------
      uint64_t getSize() const
      {
        return pSize;
      }

      //------------------------------------------------------------------------
      //! Set size
      //------------------------------------------------------------------------
      void setSize( uint64_t size )
      {
        pSize = size;
      }

      //------------------------------------------------------------------------
      //! Get tag
      //------------------------------------------------------------------------
      ContainerMD::id_t getContainerId() const
      {
        return pContainerId;
      }

      //------------------------------------------------------------------------
      //! Set tag
      //------------------------------------------------------------------------
      void setContainerId( ContainerMD::id_t containerId )
      {
        pContainerId = containerId;
      }

      //------------------------------------------------------------------------
      //! Get checksum
      //------------------------------------------------------------------------
      const Buffer &getChecksum() const
      {
        return pChecksum;
      }

      //------------------------------------------------------------------------
      //! Compare checksums
      //! WARNING: you have to supply enough bytes to compare with the checksum
      //! stored in the object!
      //------------------------------------------------------------------------
      bool checksumMatch( const void *checksum ) const
      {
        return !memcmp( checksum, pChecksum.getDataPtr(), pChecksum.getSize() );
      }

      //------------------------------------------------------------------------
      //! Set checksum
      //------------------------------------------------------------------------
      void setChecksum( const Buffer &checksum )
      {
        pChecksum = checksum;
      }

      //------------------------------------------------------------------------
      //! Set checksum
      //!
      //! @param checksum address of a memory location string the checksum
      //! @param size     size of the checksum in bytes
      //------------------------------------------------------------------------
      void setChecksum( const void *checksum, uint8_t size )
      {
        pChecksum.resize( size );
        memcpy( pChecksum.getDataPtr(), checksum, size );
      }

      //------------------------------------------------------------------------
      //! Get name
      //------------------------------------------------------------------------
      const std::string &getName() const
      {
        return pName;
      }

      //------------------------------------------------------------------------
      //! Set name
      //------------------------------------------------------------------------
      void setName( const std::string &name )
      {
        pName = name;
      }

      //------------------------------------------------------------------------
      //! Start iterator for locations
      //------------------------------------------------------------------------
      LocationSet::const_iterator locationsBegin() const
      {
        return pLocation.begin();
      }

      //------------------------------------------------------------------------
      //! End iterator for locations
      //------------------------------------------------------------------------
      LocationSet::const_iterator locationsEnd() const
      {
        return pLocation.end();
      }

      //------------------------------------------------------------------------
      //! Add location
      //------------------------------------------------------------------------
      void addLocation( location_t location )
      {
        pLocation.insert( location );
      }

      //------------------------------------------------------------------------
      //! Remove location
      //------------------------------------------------------------------------
      void removeLocation( location_t location )
      {
        pLocation.erase( location );
      }

      //------------------------------------------------------------------------
      //! Clear locations
      //------------------------------------------------------------------------
      void clearLocations()
      {
        pLocation.clear();
      }

      //------------------------------------------------------------------------
      //! Test the location
      //------------------------------------------------------------------------
      bool hasLocation( location_t location )
      {
        if( pLocation.find( location ) != pLocation.end() )
          return true;
        return false;
      }

      //------------------------------------------------------------------------
      //! Get number of location
      //------------------------------------------------------------------------
      size_t getNumLocation() const
      {
        return pLocation.size();
      }

      //------------------------------------------------------------------------
      //! Get uid
      //------------------------------------------------------------------------
      uid_t getUid() const
      {
        return pUid;
      }

      //------------------------------------------------------------------------
      //! Set uid
      //------------------------------------------------------------------------
      void setUid( uid_t uid )
      {
        pUid = uid;
      }

      //------------------------------------------------------------------------
      //! Get gid
      //------------------------------------------------------------------------
      gid_t getGid() const
      {
        return pGid;
      }

      //------------------------------------------------------------------------
      //! Set gid
      //------------------------------------------------------------------------
      void setGid( gid_t gid )
      {
        pGid = gid;
      }

      //------------------------------------------------------------------------
      //! Get name
      //------------------------------------------------------------------------
      uint32_t getLayoutId() const
      {
        return pLayoutId;
      }

      //------------------------------------------------------------------------
      //! Set name
      //------------------------------------------------------------------------
      void setLayoutId( uint32_t layoutId )
      {
        pLayoutId = layoutId;
      }

      //------------------------------------------------------------------------
      //! Serialize the object to a buffer
      //------------------------------------------------------------------------
      void serialize( Buffer &buffer ) throw( MDException );

      //------------------------------------------------------------------------
      //! Deserialize the class to a buffer
      //------------------------------------------------------------------------
      void deserialize( Buffer &buffer ) throw( MDException );

    protected:
      //------------------------------------------------------------------------
      // Data members
      //-----------------------------------------------------------------------0
      id_t              pId;
      uint64_t          pCTime;
      uint64_t          pSize;
      ContainerMD::id_t pContainerId;
      std::string       pName;
      LocationSet       pLocation;
      uid_t             pUid;
      gid_t             pGid;
      uint32_t          pLayoutId;
      Buffer            pChecksum;
  };
}

#endif // EOS_FILE_MD_HH
