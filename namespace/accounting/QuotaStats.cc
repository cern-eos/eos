//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   User quota accounting
//------------------------------------------------------------------------------

#include "namespace/accounting/QuotaStats.hh"

namespace eos
{
  //----------------------------------------------------------------------------
  // Account a new file, adjust the size using the size mapping function
  //----------------------------------------------------------------------------
  void QuotaNode::addFile( const FileMD *file )
  {
    uint64_t size = pQuotaStats->getPhysicalSize( file );
    UsageInfo &user  = pUserUsage[file->getCUid()];
    UsageInfo &group = pGroupUsage[file->getCGid()];
    user.physicalSpace  += size;
    group.physicalSpace += size;
    user.space   += file->getSize();
    group.space  += file->getSize();
    user.files++;
    group.files++;
  }

  //----------------------------------------------------------------------------
  // Remove a file, adjust the size using the size mapping function
  //----------------------------------------------------------------------------
  void QuotaNode::removeFile( const FileMD *file )
  {
    uint64_t size = pQuotaStats->getPhysicalSize( file );
    UsageInfo &user  = pUserUsage[file->getCUid()];
    UsageInfo &group = pGroupUsage[file->getCGid()];
    user.physicalSpace  -= size;
    group.physicalSpace -= size;
    user.space   -= file->getSize();
    group.space  -= file->getSize();
    user.files--;
    group.files--;
  }

  //----------------------------------------------------------------------------
  // Meld in another quota node
  //----------------------------------------------------------------------------
  void QuotaNode::meld( const QuotaNode *node )
  {
    UserMap::const_iterator it1 = node->pUserUsage.begin();
    for( ; it1 != node->pUserUsage.end(); ++it1 )
      pUserUsage[it1->first] += it1->second;

    GroupMap::const_iterator it2 = node->pGroupUsage.begin();
    for( ; it2 != node->pGroupUsage.end(); ++it2 )
      pGroupUsage[it2->first] += it2->second;
  }

  //----------------------------------------------------------------------------
  // Get a quota node associated to the container id
  //----------------------------------------------------------------------------
  QuotaNode *QuotaStats::getQuotaNode( ContainerMD::id_t nodeId )
  {
    NodeMap::iterator it = pNodeMap.find( nodeId );
    if( it == pNodeMap.end() )
      return 0;
    return it->second;
  }

  //----------------------------------------------------------------------------
  // Destructor
  //----------------------------------------------------------------------------
  QuotaStats::~QuotaStats()
  {
    NodeMap::iterator it;
    for( it = pNodeMap.begin(); it != pNodeMap.end(); ++it )
      delete it->second;
  }

  //----------------------------------------------------------------------------
  // Register a new quota node
  //----------------------------------------------------------------------------
  QuotaNode *QuotaStats::registerNewNode( ContainerMD::id_t nodeId )
    throw( MDException )
  {
    if( pNodeMap.find( nodeId ) != pNodeMap.end() )
    {
      MDException e;
      e.getMessage() << "Quota node already exist: " << nodeId;
      throw e;
    }
    QuotaNode *node = new QuotaNode( this );
    pNodeMap[nodeId] = node;
    return node;
  }

  //----------------------------------------------------------------------------
  // Remove quota node
  //----------------------------------------------------------------------------
  void QuotaStats::removeNode( ContainerMD::id_t nodeId )
    throw( MDException )
  {
    NodeMap::iterator it = pNodeMap.find( nodeId );
    if( it == pNodeMap.end() )
    {
      MDException e;
      e.getMessage() << "Quota node does not exist: " << nodeId;
      throw e;
    }
    QuotaNode *node = new QuotaNode( this );
    pNodeMap[nodeId] = node;
  }

}
