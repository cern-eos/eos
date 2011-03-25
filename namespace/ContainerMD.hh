//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   Class representing the container metadata
//------------------------------------------------------------------------------

#ifndef EOS_NS_CONTAINER_MD_HH
#define EOS_NS_CONTAINER_MD_HH

#include <stdint.h>
#include <unistd.h>
#include <cstring>
#include <string>
#include <vector>
#include <google/sparse_hash_map>
#include <map>
#include <sys/time.h>

#include "namespace/persistency/Buffer.hh"

namespace eos
{
  class FileMD;

  //----------------------------------------------------------------------------
  //! Class holding the metadata information concerning a single container
  //----------------------------------------------------------------------------
  class ContainerMD
  {
    public:
      typedef google::sparse_hash_map<std::string, ContainerMD*> ContainerMap;
      typedef google::sparse_hash_map<std::string, FileMD*>      FileMap;
      typedef std::map<std::string, std::string>                 XAttrMap;

      //------------------------------------------------------------------------
      // Type definitions
      //------------------------------------------------------------------------
      typedef uint64_t id_t;
      typedef struct timespec      ctime_t;

      //------------------------------------------------------------------------
      //! Constructor
      //------------------------------------------------------------------------
      ContainerMD( id_t id );

      //------------------------------------------------------------------------
      //! Copy constructor
      //------------------------------------------------------------------------
      ContainerMD( const ContainerMD &other );

      //------------------------------------------------------------------------
      //!
      //------------------------------------------------------------------------
      ContainerMD &operator = ( const ContainerMD &other );

      //------------------------------------------------------------------------
      //! Get container id
      //------------------------------------------------------------------------
      id_t getId() const
      {
        return pId;
      }

      //------------------------------------------------------------------------
      //! Get parent id
      //------------------------------------------------------------------------
      id_t getParentId() const
      {
        return pParentId;
      }

      //------------------------------------------------------------------------
      //! Set parent id
      //------------------------------------------------------------------------
      void setParentId( id_t parentId )
      {
        pParentId = parentId;
      }

      //------------------------------------------------------------------------
      //! Get the flags
      //------------------------------------------------------------------------
      uint16_t &getFlags()
      {
        return pFlags;
      }

      //------------------------------------------------------------------------
      //! Get the flags
      //------------------------------------------------------------------------
      uint16_t getFlags() const
      {
        return pFlags;
      }

      //------------------------------------------------------------------------
      //! Set creation time
      //------------------------------------------------------------------------
      void setCTime( ctime_t ctime)
      {
        pCTime.tv_sec = ctime.tv_sec;
        pCTime.tv_nsec = ctime.tv_nsec;
      }

      //------------------------------------------------------------------------
      //! Set creation time to now
      //------------------------------------------------------------------------
      void setCTimeNow()
      {
#ifdef __APPLE__
        struct timeval tv;
        gettimeofday(&tv, 0);
        pCTime.tv_sec = tv.tv_sec;
        pCTime.tv_nsec = tv.tv_usec * 1000;
#else
        clock_gettime(CLOCK_REALTIME, &pCTime);
#endif
      }

      //------------------------------------------------------------------------
      //! Get creation time
      //------------------------------------------------------------------------
      void getCTime( ctime_t &ctime) const
      {
        ctime.tv_sec = pCTime.tv_sec;
        ctime.tv_nsec = pCTime.tv_nsec;
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
      //! Get mode
      //------------------------------------------------------------------------
      mode_t getMode() const
      {
        return pMode;
      }

      //------------------------------------------------------------------------
      //! Set mode
      //------------------------------------------------------------------------
      void setMode( mode_t mode )
      {
        pMode = mode;
      }

      //------------------------------------------------------------------------
      //! Get ACL Id
      //------------------------------------------------------------------------
      uint16_t getACLId() const
      {
        return pACLId;
      }

      //------------------------------------------------------------------------
      //! Set ACL Id
      //------------------------------------------------------------------------
      void setACLId( uint16_t ACLId )
      {
        pACLId = ACLId;
      }

      //------------------------------------------------------------------------
      //! Serialize the object to a buffer
      //------------------------------------------------------------------------
      void serialize( Buffer &buffer ) throw( MDException );

      //------------------------------------------------------------------------
      //! Deserialize the class to a buffer
      //------------------------------------------------------------------------
      void deserialize( Buffer &buffer ) throw( MDException );

      //------------------------------------------------------------------------
      //! Find sub container
      //------------------------------------------------------------------------
      ContainerMD *findContainer( const std::string &name )
      {
        ContainerMap::iterator it = pSubContainers.find( name );
        if( it == pSubContainers.end() )
          return 0;
        return it->second;
      }

      //------------------------------------------------------------------------
      //! Remove container
      //------------------------------------------------------------------------
      void removeContainer( const std::string &name )
      {
        pSubContainers.erase( name );
      }

      //------------------------------------------------------------------------
      //! Add container
      //------------------------------------------------------------------------
      void addContainer( ContainerMD *container )
      {
        container->setParentId( pId );
        pSubContainers[container->getName()] = container;
      }

      //------------------------------------------------------------------------
      //! Get the start iterator to the container list
      //------------------------------------------------------------------------
      ContainerMap::iterator containersBegin()
      {
        return pSubContainers.begin();
      }

      //------------------------------------------------------------------------
      //! Get the end iterator of the contaienr list
      //------------------------------------------------------------------------
      ContainerMap::iterator containersEnd()
      {
        return pSubContainers.end();
      }

      //------------------------------------------------------------------------
      //! Get number of containers
      //------------------------------------------------------------------------
      size_t getNumContainers() const
      {
        return pSubContainers.size();
      }

      //------------------------------------------------------------------------
      //! Find file
      //------------------------------------------------------------------------
      FileMD *findFile( const std::string &name )
      {
        FileMap::iterator it = pFiles.find( name );
        if( it == pFiles.end() )
          return 0;
        return it->second;
      }

      //------------------------------------------------------------------------
      //! Remove file
      //------------------------------------------------------------------------
      void removeFile( const std::string &name )
      {
        pFiles.erase( name );
      }

      //------------------------------------------------------------------------
      //! Add file
      //------------------------------------------------------------------------
      void addFile( FileMD *file );

      //------------------------------------------------------------------------
      //! Get the start iterator to the file list
      //------------------------------------------------------------------------
      FileMap::iterator filesBegin()
      {
        return pFiles.begin();
      }

      //------------------------------------------------------------------------
      //! Get the end iterator of the contaienr list
      //------------------------------------------------------------------------
      FileMap::iterator filesEnd()
      {
        return pFiles.end();
      }

      //------------------------------------------------------------------------
      //! Get number of files
      //------------------------------------------------------------------------
      size_t getNumFiles() const
      {
        return pFiles.size();
      }

      //------------------------------------------------------------------------
      //! Add extended attribute
      //------------------------------------------------------------------------
      void setAttribute( const std::string &name, const std::string &value )
      {
        pXAttrs[name] = value;
      }

      //------------------------------------------------------------------------
      //! Remove attribute
      //------------------------------------------------------------------------
      void removeAttribute( const std::string &name )
      {
        XAttrMap::iterator it = pXAttrs.find( name );
        if( it != pXAttrs.end() )
          pXAttrs.erase( it );
      }

      //------------------------------------------------------------------------
      //! Check if the attribute exist
      //------------------------------------------------------------------------
      bool hasAttribute( const std::string &name ) const
      {
        return pXAttrs.find( name ) != pXAttrs.end();
      }

      //------------------------------------------------------------------------
      //! Return number of attributes
      //------------------------------------------------------------------------
      size_t numAttributes() const
      {
        return pXAttrs.size();
      }

      //------------------------------------------------------------------------
      // Get the attribute
      //------------------------------------------------------------------------
      std::string getAttribute( const std::string &name ) const
                                                            throw( MDException )
      {
        XAttrMap::const_iterator it = pXAttrs.find( name );
        if( it == pXAttrs.end() )
        {
          MDException e( ENOENT );
          e.getMessage() << "Attribute: " << name << " not found";
          throw e;
        }
        return it->second;
      }

      //------------------------------------------------------------------------
      //! Get attribute begin iterator
      //------------------------------------------------------------------------
      XAttrMap::iterator attributesBegin()
      {
        return pXAttrs.begin();
      }

      //------------------------------------------------------------------------
      //! Get the attribute end iterator
      //------------------------------------------------------------------------
      XAttrMap::iterator attributesEnd()
      {
        return pXAttrs.end();
      }

      //------------------------------------------------------------------------
      //! Check the access permissions
      //!
      //! @return true if all the requested rights are granted, false otherwise
      //------------------------------------------------------------------------
      bool access( uid_t uid, gid_t gid, int flags = 0 );

    protected:

      //------------------------------------------------------------------------
      // Data members
      //-----------------------------------------------------------------------0
      id_t         pId;
      id_t         pParentId;
      uint16_t     pFlags;
      ctime_t      pCTime;
      std::string  pName;
      uid_t        pCUid;
      gid_t        pCGid;
      mode_t       pMode;
      uint16_t     pACLId;
      XAttrMap     pXAttrs;
      ContainerMap pSubContainers;
      FileMap      pFiles;
  };
}

#endif // EOS_NS_CONTAINER_MD_HH
