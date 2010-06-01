//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   Class representing the container metadata
//------------------------------------------------------------------------------

#ifndef EOS_CONTAINER_MD_HH
#define EOS_CONTAINER_MD_HH

#include <stdint.h>
#include <cstring>
#include <string>
#include <vector>
#include <google/sparse_hash_map>
#include <sys/time.h>

#include "Namespace/persistency/Buffer.hh"

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
	clock_gettime(CLOCK_REALTIME, &pCTime);
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

    protected:

      //------------------------------------------------------------------------
      // Data members
      //-----------------------------------------------------------------------0
      id_t         pId;
      id_t         pParentId;
      ctime_t      pCTime;
      std::string  pName;
      uid_t        pCUid;
      gid_t        pCGid;
      mode_t       pMode;
      uint16_t     pACLId;
      ContainerMap pSubContainers;
      FileMap      pFiles;
  };
}

#endif // EOS_CONTAINER_MD_HH
