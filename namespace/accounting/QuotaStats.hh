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
      struct UsageInfo
      {
        UsageInfo(): space(0), files(0) {}
        uint64_t space;
	uint64_t physical_space;
        uint64_t files;
      };
      typedef std::map<uid_t, UsageInfo> UserMap;
      typedef std::map<gid_t, UsageInfo> GroupMap;

      //------------------------------------------------------------------------
      //! Constructor
      //------------------------------------------------------------------------
      QuotaNode( QuotaStats *quotaStats ): pQuotaStats( quotaStats ) {}

      //------------------------------------------------------------------------
      //! Get the amount of space occupied by the given user
      //------------------------------------------------------------------------
      uint64_t getUsedSpaceByUser( uid_t uid ) throw( MDException )
      {
        return pUserUsage[uid].space;
      }

      //------------------------------------------------------------------------
      //! Get the amount of space occupied by the given group
      //------------------------------------------------------------------------
      uint64_t getUsedSpaceByGroup( gid_t gid ) throw( MDException )
      {
        return pGroupUsage[gid].space;
      }

      //------------------------------------------------------------------------
      //! Get the amount of space occupied by the given user
      //------------------------------------------------------------------------
      uint64_t getPhysicalSpaceByUser( uid_t uid ) throw( MDException )
      {
        return pUserUsage[uid].physical_space;
      }

      //------------------------------------------------------------------------
      //! Get the amount of space occupied by the given group
      //------------------------------------------------------------------------
      uint64_t getPhysicalSpaceByGroup( gid_t gid ) throw( MDException )
      {
        return pGroupUsage[gid].physical_space;
      }

      //------------------------------------------------------------------------
      //! Get the amount of space occupied by the given user
      //------------------------------------------------------------------------
      uint64_t getNumFilesByUser( uid_t uid ) throw( MDException )
      {
        return pUserUsage[uid].files;
      }

      //------------------------------------------------------------------------
      //! Get the amount of space occupied by the given group
      //------------------------------------------------------------------------
      uint64_t getNumFilesByGroup( gid_t gid ) throw( MDException )
      {
        return pGroupUsage[gid].files;
      }

      //------------------------------------------------------------------------
      //! Change the amount of space occupied by the given user
      //------------------------------------------------------------------------
      void changeSpaceUser( uid_t uid, int64_t delta ) throw( MDException )
      {
        pUserUsage[uid].space += delta;
      }

      //------------------------------------------------------------------------
      //! Change the amount of space occpied by the given group
      //------------------------------------------------------------------------
      void changeSpaceGroup( gid_t gid, int64_t delta ) throw( MDException )
      {
        pGroupUsage[gid].space += delta;
      }

      //------------------------------------------------------------------------
      //! Change the amount of space occupied by the given user
      //------------------------------------------------------------------------
      void changePhysicalSpaceUser( uid_t uid, int64_t delta ) throw( MDException )
      {
        pUserUsage[uid].physical_space += delta;
      }

      //------------------------------------------------------------------------
      //! Change the amount of space occpied by the given group
      //------------------------------------------------------------------------
      void changePhysicalSpaceGroup( gid_t gid, int64_t delta ) throw( MDException )
      {
        pGroupUsage[gid].physical_space += delta;
      }

      //------------------------------------------------------------------------
      // Iterate over the usage of known users
      //------------------------------------------------------------------------
      UserMap::iterator userUsageBegin()
      {
        return pUserUsage.begin();
      }

      UserMap::iterator userUsageEnd()
      {
        return pUserUsage.end();
      }

      UserMap::const_iterator userUsageBegin() const
      {
        return pUserUsage.begin();
      }

      UserMap::const_iterator userUsageEnd() const
      {
        return pUserUsage.end();
      }

      //------------------------------------------------------------------------
      // Iterate over the usage of known groups
      //------------------------------------------------------------------------
      GroupMap::iterator groupUsageBegin()
      {
        return pGroupUsage.begin();
      }

      GroupMap::iterator groupUsageEnd()
      {
        return pGroupUsage.end();
      }

      const GroupMap::const_iterator groupUsageBegin() const
      {
        return pGroupUsage.begin();
      }

      const GroupMap::const_iterator groupUsageEnd() const
      {
        return pGroupUsage.end();
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
      UserMap     pUserUsage;
      GroupMap    pGroupUsage;
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
      typedef std::map<ContainerMD::id_t, QuotaNode*> NodeMap;

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

      //------------------------------------------------------------------------
      // Iterate over the quota nodes
      //------------------------------------------------------------------------
      NodeMap::iterator nodesBegin()
      {
        return pNodeMap.begin();
      }

      NodeMap::iterator nodesEnd()
      {
        return pNodeMap.end();
      }

      const NodeMap::const_iterator nodesBegin() const
      {
        return pNodeMap.begin();
      }

      const NodeMap::const_iterator nodesEnd() const
      {
        return pNodeMap.end();
      }


    private:
      SizeMapper pSizeMapper;
      NodeMap    pNodeMap;
  };
}

#endif // EOS_NS_QUOTA_STATS_HH
