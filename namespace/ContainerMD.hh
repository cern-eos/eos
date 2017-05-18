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
#include <google/dense_hash_map>
#include <map>
#include <sys/time.h>
#include <features.h>

#if __GNUC_PREREQ(4,8)
#include <atomic>
#endif

#include "namespace/persistency/Buffer.hh"

namespace eos
{
  class FileMD;
  class IContainerMDSvc;
  
  //----------------------------------------------------------------------------
  //! Class holding the metadata information concerning a single container
  //----------------------------------------------------------------------------
  class ContainerMD
  {

    public:
      typedef google::dense_hash_map<std::string, ContainerMD*> ContainerMap;
      typedef google::dense_hash_map<std::string, FileMD*>      FileMap;
      typedef std::map<std::string, std::string>                XAttrMap;

      //------------------------------------------------------------------------
      // Type definitions
      //------------------------------------------------------------------------
      typedef uint64_t id_t;
      typedef struct timespec      ctime_t;
      typedef struct timespec      mtime_t;
      typedef struct timespec      tmtime_t;

#if __GNUC_PREREQ(4,8)
      struct tmtime_atomic_t {

	tmtime_atomic_t() {tv_sec.store(0); tv_nsec.store(0);}

	std::atomic_ulong tv_sec;
	std::atomic_ulong tv_nsec;

	tmtime_atomic_t &operator = ( const tmtime_atomic_t &other )
	{
	  tv_sec.store(other.tv_sec.load());
	  tv_nsec.store(other.tv_nsec.load());
	  return *this;
	}

	void load(tmtime_t &tmt)
	{
	  tmt.tv_sec = tv_sec.load();
	  tmt.tv_nsec = tv_nsec.load();
	}
	void store(const tmtime_t& tmt )
	{
	  tv_sec.store(tmt.tv_sec);
	  tv_nsec.store(tmt.tv_nsec);
	}
      };
#else
#endif
 
      //------------------------------------------------------------------------
      //! Constructor
      //------------------------------------------------------------------------
      ContainerMD( id_t id );

      //------------------------------------------------------------------------
      //! Copy constructor
      //------------------------------------------------------------------------
      ContainerMD( const ContainerMD &other);

      //------------------------------------------------------------------------
      //! Assignment operator
      //------------------------------------------------------------------------
      ContainerMD &operator = ( const ContainerMD &other );

      //------------------------------------------------------------------------
      //! Children inheritance
      //------------------------------------------------------------------------
      void InheritChildren( const ContainerMD &other);

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
      //! Set creation time
      //------------------------------------------------------------------------
      void setMTime( mtime_t mtime);
      
      //------------------------------------------------------------------------
      //! Set creation time to now
      //------------------------------------------------------------------------
      void setMTimeNow();


      //------------------------------------------------------------------------
      //! Trigger an mtime change event
      //------------------------------------------------------------------------
      void notifyMTimeChange( IContainerMDSvc *containerMDSvc );

      //------------------------------------------------------------------------
      //! Get creation time
      //------------------------------------------------------------------------
      void getMTime( mtime_t &mtime) const
      {
        mtime.tv_sec = pMTime.tv_sec;
        mtime.tv_nsec = pMTime.tv_nsec;
      }
    
      //------------------------------------------------------------------------
      //! Set propagated modification time (if newer)
      //------------------------------------------------------------------------
      bool setTMTime( tmtime_t tmtime)
      {
	while(1) {
#if __GNUC_PREREQ(4,8)
	  pTMTime_atomic.load(pTMTime);
#endif
	  if ( (tmtime.tv_sec > pTMTime.tv_sec ) || 
	       ( (tmtime.tv_sec == pTMTime.tv_sec) && 
		 (tmtime.tv_nsec > pTMTime.tv_nsec) ) )
	  {
#if __GNUC_PREREQ(4,8)
	    uint64_t ts = (uint64_t) pTMTime.tv_sec;
	    uint64_t tns = (uint64_t) pTMTime.tv_nsec;
	    bool retry1 = pTMTime_atomic.tv_sec.compare_exchange_weak(ts,  tmtime.tv_sec,
								      std::memory_order_relaxed,
								      std::memory_order_relaxed);
	    bool retry2 = pTMTime_atomic.tv_nsec.compare_exchange_weak(tns, tmtime.tv_nsec,
								       std::memory_order_relaxed,
								       std::memory_order_relaxed);
	    
	    if (!retry1 || !retry2)
	      continue;

	    pTMTime_atomic.load(pTMTime);
#else
	    pTMTime.tv_sec = tmtime.tv_sec;
	    pTMTime.tv_nsec = tmtime.tv_nsec;
#endif
	  }
	  return true;
	}
	return false;
      }
  

      //------------------------------------------------------------------------
      //! Set propagated modification time to now
      //------------------------------------------------------------------------
      void setTMTimeNow()
      {
        tmtime_t tmtime;
#ifdef __APPLE__
        struct timeval tv;
        gettimeofday(&tv, 0);
	tmtime..tv_sec = tv.tv_sec;
        tmtime.tv_nsec = tv.tv_usec * 1000;
#else
        clock_gettime(CLOCK_REALTIME, &tmtime);
#endif
	setTMTime(tmtime);
	return;
      }

      //------------------------------------------------------------------------
      //! Get creation time
      //------------------------------------------------------------------------
      void getTMTime( tmtime_t &tmtime) 
      {
#if __GNUC_PREREQ(4,8)
	pTMTime_atomic.load(pTMTime);
#endif
        tmtime.tv_sec = pTMTime.tv_sec;
        tmtime.tv_nsec = pTMTime.tv_nsec;
      }

      //------------------------------------------------------------------------
      //! Get tree size
      //------------------------------------------------------------------------
      uint64_t getTreeSize() const
      {
#if __GNUC_PREREQ(4,8)
	return pTreeSize.load();
#else
	return pTreeSize;
#endif
      }

      //------------------------------------------------------------------------
      //! Set tree size
      //------------------------------------------------------------------------
      void setTreeSize( uint64_t treesize)
      {
#if __GNUC_PREREQ(4,8)
	pTreeSize.store(treesize);
#else
	pTreeSize = treesize;
#endif
      }

      //------------------------------------------------------------------------
      //! Add to tree size
      //------------------------------------------------------------------------
      uint64_t addTreeSize( uint64_t addsize)
      {
	pTreeSize += addsize;
	return getTreeSize();
      }

      //------------------------------------------------------------------------
      //! Remove from tree size
      //------------------------------------------------------------------------
      uint64_t removeTreeSize( uint64_t removesize)
      {
	pTreeSize -= removesize;
	return getTreeSize();
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
      void removeFile( const std::string &name );

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
      //! Clear extended attribute
      //------------------------------------------------------------------------
      void clearAttributes()
      {
        pXAttrs.clear();
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
      // Get the attribute map
      //------------------------------------------------------------------------
      XAttrMap getAttributeMap() const 
      {
	return pXAttrs;
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
 
      // non presisted data members
      mtime_t      pMTime;
      tmtime_t     pTMTime;

#if __GNUC_PREREQ(4,8)
      // atomic (thread-safe) types
      std::atomic_ulong pTreeSize;
      tmtime_atomic_t pTMTime_atomic;
#else
      uint64_t     pTreeSize;
#endif
  };
}

#endif // EOS_NS_CONTAINER_MD_HH
