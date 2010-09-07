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
#include <vector>

#include <sys/time.h>

namespace eos
{
  class IFileMDSvc;

  //----------------------------------------------------------------------------
  //! Class holding the metadata information concerning a single file
  //----------------------------------------------------------------------------
  class FileMD
  {
    public:
      //------------------------------------------------------------------------
      // Type definitions
      //------------------------------------------------------------------------
      typedef struct timespec      ctime_t;
      typedef uint64_t             id_t;
      typedef uint16_t             location_t;
      typedef std::vector<location_t> LocationVector;

      //------------------------------------------------------------------------
      //! Constructor
      //------------------------------------------------------------------------
      FileMD( id_t id, IFileMDSvc *fileMDSvc );

      //------------------------------------------------------------------------
      //! Copy constructor
      //------------------------------------------------------------------------
      FileMD( const FileMD &other );

      //------------------------------------------------------------------------
      //! Asignment operator
      //------------------------------------------------------------------------
      FileMD &operator = ( const FileMD &other );

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
      void getCTime( ctime_t &ctime ) const
      {
        ctime.tv_sec = pCTime.tv_sec;
	ctime.tv_nsec = pCTime.tv_nsec;
      }

      //------------------------------------------------------------------------
      //! Set creation time
      //------------------------------------------------------------------------
      void setCTime( ctime_t ctime )
      {
        pCTime.tv_sec = ctime.tv_sec;
	pCTime.tv_nsec = ctime.tv_nsec;
      }

      //------------------------------------------------------------------------
      //! Set creation time to now
      //------------------------------------------------------------------------
      void setCTimeNow()
      {
	clock_gettime(CLOCK_REALTIME, &pCTime);
      }


      //------------------------------------------------------------------------
      //! Get modification time
      //------------------------------------------------------------------------
      void getMTime( ctime_t &mtime ) const
      {
        mtime.tv_sec = pCTime.tv_sec;
	mtime.tv_nsec = pCTime.tv_nsec;
      }

      //------------------------------------------------------------------------
      //! Set modification time
      //------------------------------------------------------------------------
      void setMTime( ctime_t mtime )
      {
        pMTime.tv_sec = mtime.tv_sec;
	pMTime.tv_nsec = mtime.tv_nsec;
      }

      //------------------------------------------------------------------------
      //! Set modification time to now
      //------------------------------------------------------------------------
      void setMTimeNow()
      {
	clock_gettime(CLOCK_REALTIME, &pMTime);
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
        pChecksum.clear();
        pChecksum.putData( checksum, size );
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
      LocationVector::const_iterator locationsBegin() const
      {
	return pLocation.begin();
      }

      //------------------------------------------------------------------------
      //! End iterator for locations
      //------------------------------------------------------------------------
      LocationVector::const_iterator locationsEnd() const
      {
	return pLocation.end();
      }

      //------------------------------------------------------------------------
      //! Start iterator for unlinked locations 
      //------------------------------------------------------------------------ 
      LocationVector::const_iterator unlinkedLocationsBegin() const 
      { 
	return pUnlinkedLocation.begin(); 
      } 
    
      //------------------------------------------------------------------------ 
      //! End iterator for unlinked locations 
      //------------------------------------------------------------------------ 
      LocationVector::const_iterator unlinkedLocationsEnd() const 
      { 
	return pUnlinkedLocation.end(); 
      }

      //------------------------------------------------------------------------
      //! Add location
      //------------------------------------------------------------------------
      void addLocation( location_t location );

      //------------------------------------------------------------------------
      //! Get location
      //------------------------------------------------------------------------
      location_t getLocation( unsigned int index )
      {
	if (index < pLocation.size())
	  return pLocation[index];
	return 0; 
      }

      //------------------------------------------------------------------------
      //! replace location by index
      //------------------------------------------------------------------------
      void replaceLocation( unsigned int index, location_t newlocation );

      //------------------------------------------------------------------------
      //! Remove location that was previously unlinked
      //------------------------------------------------------------------------
      void removeLocation( location_t location );

      //------------------------------------------------------------------------
      //! Unlink location
      //------------------------------------------------------------------------
      void unlinkLocation( location_t location );

      //------------------------------------------------------------------------
      //! Unlink all locations
      //------------------------------------------------------------------------
      void unlinkAllLocations();

      //------------------------------------------------------------------------
      //! Get number of unlinked locations
      //------------------------------------------------------------------------
      size_t getNumUnlinkedLocation() const
      {
        return pUnlinkedLocation.size();
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
        for (unsigned int i=0; i< pLocation.size(); i++)
        {
          if( pLocation[i] == location )
            return true;
        }
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
      uid_t getCUid() const
      {
        return pCUid;
      }

      //------------------------------------------------------------------------
      //! Set uid
      //------------------------------------------------------------------------
      void setCUid( uid_t uid )
      {
        pCUid = uid;
      }

      //------------------------------------------------------------------------
      //! Get gid
      //------------------------------------------------------------------------
      gid_t getCGid() const
      {
        return pCGid;
      }

      //------------------------------------------------------------------------
      //! Set gid
      //------------------------------------------------------------------------
      void setCGid( gid_t gid )
      {
        pCGid = gid;
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
      //! Env Representation
      //------------------------------------------------------------------------
      void getEnv( std::string &env );

      //------------------------------------------------------------------------
      //! Serialize the object to a buffer
      //------------------------------------------------------------------------
      void serialize( Buffer &buffer ) throw( MDException );

      //------------------------------------------------------------------------
      //! Deserialize the class to a buffer
      //------------------------------------------------------------------------
      void deserialize( const Buffer &buffer ) throw( MDException );

    protected:
      //------------------------------------------------------------------------
      // Data members
      //-----------------------------------------------------------------------0
      id_t               pId;
      ctime_t            pCTime;
      ctime_t            pMTime;
      uint64_t           pSize;
      ContainerMD::id_t  pContainerId;
      std::string        pName;
      LocationVector     pLocation;
      LocationVector     pUnlinkedLocation;
      uid_t              pCUid;
      gid_t              pCGid;
      uint32_t           pLayoutId;
      Buffer             pChecksum;
      IFileMDSvc        *pFileMDSvc;
  };
}

#endif // EOS_FILE_MD_HH
