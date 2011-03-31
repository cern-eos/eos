//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   User quota accounting
//------------------------------------------------------------------------------

#ifndef EOS_NS_QUOTA_STATS_HH
#define EOS_NS_QUOTA_STATS_HH

#include "namespace/ContainerMD.hh"
#include "namespace/FileMD.hh"
#include <map>

namespace eos
{
  class QuotaStats;

  //----------------------------------------------------------------------------
  //! Placeholder for space occupancy statistics of an accounting node
  //----------------------------------------------------------------------------
  class QuotaNode
  {
    public:
      typedef std::map<uid_t, uint64_t> UserMap;
      typedef std::map<gid_t, uint64_t> GroupMap;

      //------------------------------------------------------------------------
      //! Constructor
      //------------------------------------------------------------------------
      QuotaNode( QuotaStats *quotaStats ): pQuotaStats( quotaStats ) {}

      //------------------------------------------------------------------------
      //! Get the amount of space occupied by the given user
      //------------------------------------------------------------------------
      uint64_t getOccupancyByUser( uid_t uid ) throw( MDException )
      {
        return pUserOccupancy[uid];
      }

      //------------------------------------------------------------------------
      //! Get the amount of space occupied by the given group
      //------------------------------------------------------------------------
      uint64_t getOccupancyByGroup( gid_t gid ) throw( MDException )
      {
        return pGroupOccupancy[gid];
      }

      //------------------------------------------------------------------------
      //! Change the amount of space occupied by the given user
      //------------------------------------------------------------------------
      void changeOccupancyUser( uid_t uid, int64_t delta ) throw( MDException )
      {
        pUserOccupancy[uid] += delta;
      }

      //------------------------------------------------------------------------
      //! Change the amount of space occpied by the given group
      //------------------------------------------------------------------------
      void changeOccupancyGroup( gid_t gid, int64_t delta ) throw( MDException )
      {
        pGroupOccupancy[gid] += delta;
      }

      //------------------------------------------------------------------------
      // Iterate over the usage of known users
      //------------------------------------------------------------------------
      UserMap::iterator userOccupancyBegin()
      {
        return pUserOccupancy.begin();
      }

      UserMap::iterator userOccupencyEnd()
      {
        return pUserOccupancy.end();
      }

      UserMap::const_iterator userOccupancyBegin() const
      {
        return pUserOccupancy.begin();
      }

      UserMap::const_iterator userOccupancyEnd() const
      {
        return pUserOccupancy.end();
      }

      //------------------------------------------------------------------------
      // Iterate over the usage of known groups
      //------------------------------------------------------------------------
      GroupMap::iterator groupOccupancyBegin()
      {
        return pGroupOccupancy.begin();
      }

      GroupMap::iterator groupOccupancyEnd()
      {
        return pGroupOccupancy.end();
      }

      const GroupMap::const_iterator groupOccupancyBegin() const
      {
        return pGroupOccupancy.begin();
      }

      const GroupMap::const_iterator groupOccupancyEnd() const
      {
        return pGroupOccupancy.end();
      }

      //------------------------------------------------------------------------
      //! Account a new file, adjust the size using the size mapping function
      //------------------------------------------------------------------------
      void addFile( const FileMD *file );

      //------------------------------------------------------------------------
      //! Remove a file, adjust the size using the size mapping function
      //------------------------------------------------------------------------
      void removeFile( const FileMD *file );

    private:
      UserMap     pUserOccupancy;
      GroupMap    pGroupOccupancy;
      QuotaStats *pQuotaStats;
  };

  //----------------------------------------------------------------------------
  //! Manager of the quota nodes
  //----------------------------------------------------------------------------
  class QuotaStats
  {
    public:
      //------------------------------------------------------------------------
      // Type definitions
      //------------------------------------------------------------------------
      typedef uint64_t (*SizeMapper)( const FileMD *file );
      typedef google::sparse_hash_map<ContainerMD::id_t, QuotaNode*> NodeMap;

      //------------------------------------------------------------------------
      //! Constructor
      //------------------------------------------------------------------------
      QuotaStats(): pSizeMapper( 0 ) {}

      //------------------------------------------------------------------------
      //! Destructor
      //------------------------------------------------------------------------
      virtual ~QuotaStats();

      //------------------------------------------------------------------------
      //! Get a quota node associated to the container id
      //------------------------------------------------------------------------
      QuotaNode *getQuotaNode( ContainerMD::id_t nodeId );

      //------------------------------------------------------------------------
      //! Register a new quota node
      //------------------------------------------------------------------------
      QuotaNode *registerNewNode( ContainerMD::id_t nodeId ) throw( MDException );

      //------------------------------------------------------------------------
      //! Register a mapping function used to calculate the physical
      //! space that the file occuppies (replicas, striping and so on)
      //------------------------------------------------------------------------
      void registerSizeMapper( SizeMapper sizeMapper )
      {
        pSizeMapper = sizeMapper;
      }

      //------------------------------------------------------------------------
      //! Calculate the physical size the file occupies
      //------------------------------------------------------------------------
      uint64_t getPhysicalSize( const FileMD *file ) throw( MDException )
      {
        if( !pSizeMapper )
        {
          MDException e;
          e.getMessage() << "No size mapping function registered" << std::endl;
          throw( e );
        }
        return (*pSizeMapper)( file );
      }

    private:
      SizeMapper pSizeMapper;
      NodeMap    pNodeMap;
  };
}

#endif // EOS_NS_QUOTA_STATS_HH
