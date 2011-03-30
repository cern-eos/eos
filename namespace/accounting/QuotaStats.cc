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
    pUserOccupancy[file->getCUid()]  += size;
    pGroupOccupancy[file->getCGid()] += size;
  }

  //----------------------------------------------------------------------------
  // Remove a file, adjust the size using the size mapping function
  //----------------------------------------------------------------------------
  void QuotaNode::removeFile( const FileMD *file )
  {
    uint64_t size = pQuotaStats->getPhysicalSize( file );
    pUserOccupancy[file->getCUid()]  -= size;
    pGroupOccupancy[file->getCGid()] -= size;
  }

  //----------------------------------------------------------------------------
  // Get a quota node associated to the container id
  //----------------------------------------------------------------------------
  QuotaNode *QuotaStats::getQuotaNode( ContainerMD::id_t nodeId )
    throw( MDException )
  {
    NodeMap::iterator it = pNodeMap.find( nodeId );
    if( it == pNodeMap.end() )
    {
      MDException e;
      e.getMessage() << "Quota node does not exist: " << nodeId;
      throw e;
    }
    return it->second;
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
