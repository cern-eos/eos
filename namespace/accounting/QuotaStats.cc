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
    user.physical_space  += size;
    group.physical_space += size;
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
    user.physical_space  -= size;
    group.physical_space -= size;
    user.space   -= file->getSize();
    group.space  -= file->getSize();
    user.files--;
    group.files--;
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
}
