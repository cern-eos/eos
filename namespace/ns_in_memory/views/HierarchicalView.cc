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
// desc:   Hierarchical view implementation
//------------------------------------------------------------------------------

#include "namespace/ns_in_memory/views/HierarchicalView.hh"
#include "namespace/utils/PathProcessor.hh"
#include "namespace/interface/IContainerMDSvc.hh"
#include "namespace/interface/IFileMDSvc.hh"
#include "namespace/Constants.hh"
#include <errno.h>

#include <ctime>

#ifdef __APPLE__
#define EBADFD 77
#endif

namespace eos
{
  //----------------------------------------------------------------------------
  // Configure the view
  //----------------------------------------------------------------------------
  void HierarchicalView::configure( std::map<std::string, std::string> &config )
  {
    if( !pContainerSvc )
    {
      MDException e( EINVAL );
      e.getMessage() << "Container MD Service was not set";
      throw e;
    }

    if( !pFileSvc )
    {
      MDException e( EINVAL );
      e.getMessage() << "File MD Service was not set";
      throw e;
    }
  }

  //----------------------------------------------------------------------------
  // Initialize the view
  //----------------------------------------------------------------------------

  void HierarchicalView::initialize() throw( MDException )
  {
    initialize1();
    initialize2();
    initialize3();
  }

  void HierarchicalView::initialize1() throw( MDException )
  {
    pContainerSvc->initialize();

    //--------------------------------------------------------------------------
    // Get root container
    //--------------------------------------------------------------------------
    try
    {
      pRoot = pContainerSvc->getContainerMD( 1 );
    }
    catch( MDException &e )
    {
      pRoot = pContainerSvc->createContainer();
      pRoot->setParentId( pRoot->getId() );
      if (!static_cast<ChangeLogContainerMDSvc*>(pContainerSvc)->getSlaveMode())
	pContainerSvc->updateStore( pRoot );
    }
  }

  void HierarchicalView::initialize2() throw( MDException )
  {
    pFileSvc->initialize();
  }

  void HierarchicalView::initialize3() throw( MDException )
  {
    //--------------------------------------------------------------------------
    // Scan all the files to reattach them to containers - THIS SHOULD NOT
    // BE DONE! THE INFO NEEDS TO BE STORED WITH CONTAINERS
    //--------------------------------------------------------------------------

    FileVisitor visitor( pContainerSvc, pQuotaStats, this );
    pFileSvc->visit( &visitor );
  }

  //----------------------------------------------------------------------------
  // Finalize the view
  //----------------------------------------------------------------------------
  void HierarchicalView::finalize() throw( MDException )
  {
    pContainerSvc->finalize();
    pFileSvc->finalize();
    delete pQuotaStats;
    pQuotaStats = new QuotaStats();
  }

  //----------------------------------------------------------------------------
  // Retrieve a file for given uri
  //----------------------------------------------------------------------------
  IFileMD*
  HierarchicalView::getFile(const std::string &uri) throw( MDException )
  {
    char uriBuffer[uri.length()+1];
    strcpy( uriBuffer, uri.c_str() );
    std::vector<char*> elements;

    if (uri == "/")
    {
      MDException e( ENOENT );
      e.getMessage() << " is not a file";
      throw e;
    }

    eos::PathProcessor::splitPath( elements, uriBuffer );
    size_t position;    
    IContainerMD *cont = findLastContainer(elements, elements.size()-1,
                                           position);

    if( position != elements.size()-1 )
    {
      MDException e( ENOENT );
      e.getMessage() << "Container does not exist";
      throw e;
    }

    IFileMD *file = cont->findFile( elements[position] );
    
    if( !file )
    {
      MDException e( ENOENT );
      e.getMessage() << "File does not exist";
      throw e;
    }
    
    return file;
  }

  //----------------------------------------------------------------------------
  // Create a file for given uri
  //----------------------------------------------------------------------------
  IFileMD*
  HierarchicalView::createFile( const std::string &uri, uid_t uid, gid_t gid )
      throw( MDException )
  {
    // Split the path and find the last container
    char uriBuffer[uri.length()+1];
    strcpy( uriBuffer, uri.c_str() );
    std::vector<char*> elements;
    eos::PathProcessor::splitPath( elements, uriBuffer );
    size_t position;
    IContainerMD *cont = findLastContainer(elements, elements.size()-1,
                                           position);

    if( position != elements.size()-1 )
    {
      MDException e( ENOENT );
      e.getMessage() << "Container does not exist";
      throw e;
    }

    // Check if the file of this name can be inserted
    if( cont->findContainer( elements[position] ) )
    {
      MDException e( EEXIST );
      e.getMessage() << "File exist";
      throw e;
    }

    if( cont->findFile( elements[position] ) )
    {
      MDException e( EEXIST );
      e.getMessage() << "File exist";
      throw e;
    }

    IFileMD *file = pFileSvc->createFile();
    
    if( !file )
    {
      MDException e( EIO );
      e.getMessage() << "File creation failed";
      throw e;
    }

    file->setName( elements[position] );
    file->setCUid( uid );
    file->setCGid( gid );
    file->setCTimeNow();
    file->setMTimeNow();
    file->clearChecksum(0);
    cont->addFile( file );
    pFileSvc->updateStore( file );

    return file;
  }

  //----------------------------------------------------------------------------
  // Unlink the file for given uri
  //----------------------------------------------------------------------------
  void HierarchicalView::unlinkFile( const std::string &uri )
    throw( MDException )
  {
    char uriBuffer[uri.length()+1];
    strcpy( uriBuffer, uri.c_str() );
    std::vector<char*> elements;
    eos::PathProcessor::splitPath( elements, uriBuffer );
    size_t position;
    IContainerMD *cont = findLastContainer(elements, elements.size()-1,
                                           position);

    if( position != elements.size()-1 )
    {
      MDException e( ENOENT );
      e.getMessage() << "Container does not exist";
      throw e;
    }

    IFileMD *file = cont->findFile( elements[position] );

    if( !file )
    {
      MDException e( ENOENT );
      e.getMessage() << "File does not exist";
      throw e;
    }

    cont->removeFile( file->getName() );
    file->setContainerId( 0 );
    file->unlinkAllLocations();
    pFileSvc->updateStore( file );
  }

  //----------------------------------------------------------------------------
  // Remove the file
  //----------------------------------------------------------------------------
  void HierarchicalView::removeFile( IFileMD *file ) throw( MDException )
  {
    //--------------------------------------------------------------------------
    // Check if the file can be removed
    //--------------------------------------------------------------------------
    if( file->getNumLocation() != 0 || file->getNumUnlinkedLocation() != 0 )
    {
      MDException ex( EBADFD );
      ex.getMessage() << "Cannot remove the record. Unlinked replicas still ";
      ex.getMessage() << "still exist";
      throw ex;
    }

    if( file->getContainerId() != 0 )
    {
      IContainerMD *cont = pContainerSvc->getContainerMD( file->getContainerId() );
      cont->removeFile( file->getName() );
    }
    pFileSvc->removeFile( file );
  }

  //----------------------------------------------------------------------------
  // Get a container (directory)
  //----------------------------------------------------------------------------
  IContainerMD *HierarchicalView::getContainer( const std::string &uri )
    throw( MDException )
  {
    if( uri == "/" )
      return pRoot;

    char uriBuffer[uri.length()+1];
    strcpy( uriBuffer, uri.c_str() );
    std::vector<char*> elements;
    eos::PathProcessor::splitPath( elements, uriBuffer );

    size_t position;
    IContainerMD *cont = findLastContainer( elements, elements.size(), position );

    if( position != elements.size() )
    {
      MDException e( ENOENT );
      e.getMessage() << uri << ": No such file or directory";
      throw e;
    }

    return cont;
  }

  //----------------------------------------------------------------------------
  // Create a container (directory) 
  //----------------------------------------------------------------------------
  IContainerMD *HierarchicalView::createContainer(const std::string &uri,
                                                  bool createParents)
    throw( MDException )
  {
    //--------------------------------------------------------------------------
    // Split the path
    //--------------------------------------------------------------------------
    if( uri == "/" )
    {
      MDException e( EEXIST );
      e.getMessage() << uri << ": File exist" << std::endl;
      throw e;
    }

    char uriBuffer[uri.length()+1];
    strcpy( uriBuffer, uri.c_str() );
    std::vector<char*> elements;
    eos::PathProcessor::splitPath( elements, uriBuffer );

    if( elements.size() == 0 )
    {
      MDException e( EEXIST );
      e.getMessage() << uri << ": File exist" << std::endl;
      throw e;
    }

    //--------------------------------------------------------------------------
    // Look for the last existing container
    //--------------------------------------------------------------------------
    size_t position;
    IContainerMD *lastContainer = findLastContainer( elements, elements.size(),
                                                    position );

    if( position == elements.size() )
    {
      MDException e( EEXIST );
      e.getMessage() << uri << ": File exist" << std::endl;
      throw e;
    }

    //--------------------------------------------------------------------------
    // One of the parent containers does not exist
    //--------------------------------------------------------------------------
    if( (!createParents) && (position < elements.size()-1) )
    {
      MDException e( ENOENT );
      e.getMessage() << uri << ": Parent does not exist" << std::endl;
      throw e;
    }

    if( lastContainer->findFile( elements[position] ) )
    {
      MDException e( EEXIST );
      e.getMessage() << "File exists" << std::endl;
      throw e;
    }

    //--------------------------------------------------------------------------
    // Create the container with all missing parent's if requires
    //--------------------------------------------------------------------------
    for( size_t i = position; i < elements.size(); ++i )
    {
      IContainerMD *newContainer = pContainerSvc->createContainer();
      newContainer->setName( elements[i] );
      newContainer->setCTimeNow();
      lastContainer->addContainer( newContainer );
      lastContainer = newContainer;
      pContainerSvc->updateStore( lastContainer );
    }

    return lastContainer;
  }

  //----------------------------------------------------------------------------
  // Remove a container (directory) 
  //----------------------------------------------------------------------------
  void HierarchicalView::removeContainer( const std::string &uri,
                                          bool recursive )
    throw( MDException )
  {
    //--------------------------------------------------------------------------
    // Find the container
    //--------------------------------------------------------------------------
    if( uri == "/" )
    {
      MDException e( EPERM );
      e.getMessage() << "Permission denied.";
      throw e;
    }

    char uriBuffer[uri.length()+1];
    strcpy( uriBuffer, uri.c_str() );
    std::vector<char*> elements;
    eos::PathProcessor::splitPath( elements, uriBuffer );

    size_t position;
    IContainerMD *parent = findLastContainer( elements, elements.size()-1, position );
    if( (position != (elements.size()-1)) )
    {
      MDException e( ENOENT );
      e.getMessage() << uri << ": No such file or directory";
      throw e;
    }

    //--------------------------------------------------------------------------
    // Check if the container exist and remove it
    //--------------------------------------------------------------------------
    IContainerMD *cont = parent->findContainer( elements[elements.size()-1] );
    if( !cont )
    {
      MDException e( ENOENT );
      e.getMessage() << uri << ": No such file or directory";
      throw e;
    }

    if( (cont->getNumContainers() != 0 || cont->getNumFiles() != 0) &&
        !recursive )
    {
      MDException e( ENOTEMPTY );
      e.getMessage() << uri << ": Container is not empty";
      throw e;
    }

    parent->removeContainer( cont->getName() );

    if( recursive )
      cleanUpContainer( cont );

    pContainerSvc->removeContainer( cont );

  }

  //----------------------------------------------------------------------------
  // Find the last existing container in the path
  //----------------------------------------------------------------------------
  IContainerMD *HierarchicalView::findLastContainer( std::vector<char*> &elements,
                                                    size_t end, size_t &index )
  {
    IContainerMD *current  = pRoot;
    IContainerMD *found    = 0;
    size_t       position = 0;

    while( position < end )
    {
      found = current->findContainer( elements[position] );
      if( !found )
      {
        index = position;
        return current;
      }
      current = found;
      ++position;
    }

    index = position;
    return current;
  }

  //----------------------------------------------------------------------------
  // Clean up the container's children
  //----------------------------------------------------------------------------
  void HierarchicalView::cleanUpContainer( IContainerMD *cont )
  {
    (void) cont->cleanUp(pContainerSvc, pFileSvc);
  }

  //----------------------------------------------------------------------------
  // Update quota
  //----------------------------------------------------------------------------
  void HierarchicalView::FileVisitor::visitFile( IFileMD *file )
  {
    if( file->getContainerId() == 0 )
      return;

    IContainerMD *cont = 0;
    try {
      cont = pContSvc->getContainerMD( file->getContainerId() ); }
    catch( MDException &e ) {}

    if( !cont )
      return;

    //--------------------------------------------------------------------------
    // Update quota stats
    //--------------------------------------------------------------------------
    IQuotaNode *node = pView->getQuotaNode( cont );
    if( node )
      node->addFile( file );
  }

  //----------------------------------------------------------------------------
  // Get uri for the container
  //----------------------------------------------------------------------------
  std::string HierarchicalView::getUri( const IContainerMD *container ) const
    throw( MDException )
  {
    //--------------------------------------------------------------------------
    // Check the input
    //--------------------------------------------------------------------------
    if( !container )
    {
      MDException ex;
      ex.getMessage() << "Invalid container (zero pointer)";
      throw ex;
    }

    //--------------------------------------------------------------------------
    // Gather the uri elements
    //--------------------------------------------------------------------------
    std::vector<std::string> elements;
    elements.reserve( 10 );
    const IContainerMD *cursor = container;
    while( cursor->getId() != 1 )
    {
      elements.push_back( cursor->getName() );
      cursor = pContainerSvc->getContainerMD( cursor->getParentId() );
    }

    //--------------------------------------------------------------------------
    // Assemble the uri
    //--------------------------------------------------------------------------
    std::string path = "/";
    std::vector<std::string>::reverse_iterator rit;
    for( rit = elements.rbegin(); rit != elements.rend(); ++rit )
    {
      path += *rit;
      path += "/";
    }
    return path;
  }

  //----------------------------------------------------------------------------
  // Get uri for the file
  //----------------------------------------------------------------------------
  std::string HierarchicalView::getUri( const IFileMD *file ) const
    throw( MDException )
  {
    //--------------------------------------------------------------------------
    // Check the input
    //--------------------------------------------------------------------------
    if( !file )
    {
      MDException ex;
      ex.getMessage() << "Invalid file (zero pointer)";
      throw ex;
    }

    //--------------------------------------------------------------------------
    // Get the uri
    //--------------------------------------------------------------------------
    std::string path = getUri( pContainerSvc->getContainerMD(
                                                    file->getContainerId() ) );
    return path+file->getName();
  }

  //----------------------------------------------------------------------------
  // Get quota node id concerning given container
  //----------------------------------------------------------------------------
  IQuotaNode *HierarchicalView::getQuotaNode( const IContainerMD *container,
                                              bool search )
    throw( MDException )
  {
    //--------------------------------------------------------------------------
    // Initial sanity check
    //--------------------------------------------------------------------------
    if( !container )
    {
      MDException ex;
      ex.getMessage() << "Invalid container (zero pointer)";
      throw ex;
    }

    if( !pQuotaStats )
    {
      MDException ex;
      ex.getMessage() << "No QuotaStats placeholder registered";
      throw ex;
    }

    //--------------------------------------------------------------------------
    // Search for the node
    //--------------------------------------------------------------------------
    const IContainerMD *current = container;

    if (search)
    {
      while( current != pRoot && (current->getFlags() & QUOTA_NODE_FLAG) == 0 )
        current = pContainerSvc->getContainerMD( current->getParentId() );
    }

    //--------------------------------------------------------------------------
    // We have either found a quota node or reached root without finding one
    // so we need to double check whether the current container has an
    // associated quota node
    //--------------------------------------------------------------------------
    if( (current->getFlags() & QUOTA_NODE_FLAG) == 0 )
      return 0;

    IQuotaNode *node = pQuotaStats->getQuotaNode( current->getId() );
    if( node )
      return node;

    return pQuotaStats->registerNewNode( current->getId() );
  }

  //----------------------------------------------------------------------------
  // Register the container to be a quota node
  //----------------------------------------------------------------------------
  IQuotaNode *HierarchicalView::registerQuotaNode( IContainerMD *container )
    throw( MDException )
  {
    //--------------------------------------------------------------------------
    // Initial sanity check
    //--------------------------------------------------------------------------
    if( !container )
    {
      MDException ex;
      ex.getMessage() << "Invalid container (zero pointer)";
      throw ex;
    }

    if( !pQuotaStats )
    {
      MDException ex;
      ex.getMessage() << "No QuotaStats placeholder registered";
      throw ex;
    }

    if( container->getFlags() & QUOTA_NODE_FLAG )
    {
      MDException ex;
      ex.getMessage() << "Already a quota node: " << container->getId();
      throw ex;
    }

    IQuotaNode *node = pQuotaStats->registerNewNode( container->getId() );
    container->getFlags() |= QUOTA_NODE_FLAG;
    updateContainerStore( container );

    return node;
  }

  //----------------------------------------------------------------------------
  // Remove the quota node
  //----------------------------------------------------------------------------
  void HierarchicalView::removeQuotaNode( IContainerMD *container )
    throw( MDException )
  {
    //--------------------------------------------------------------------------
    // Sanity checks
    //--------------------------------------------------------------------------
    if( !container )
    {
      MDException ex;
      ex.getMessage() << "Invalid container (zero pointer)";
      throw ex;
    }

    if( !pQuotaStats )
    {
      MDException ex;
      ex.getMessage() << "No QuotaStats placeholder registered";
      throw ex;
    }

    if( !(container->getFlags() & QUOTA_NODE_FLAG) )
    {
      MDException ex;
      ex.getMessage() << "Not a quota node: " << container->getId();
      throw ex;
    }

    //--------------------------------------------------------------------------
    // Get the quota node and meld it with the parent node if present
    //--------------------------------------------------------------------------
    IQuotaNode *node   = getQuotaNode( container );
    IQuotaNode *parent = 0;
    if( container != pRoot )
      parent = getQuotaNode( pContainerSvc->getContainerMD( container->getParentId() ),
                             true );

    container->getFlags() &= ~QUOTA_NODE_FLAG;
    updateContainerStore( container );
    if( parent )
      parent->meld( node );

    pQuotaStats->removeNode( container->getId() );
  }

  //----------------------------------------------------------------------------
  // Rename container
  //----------------------------------------------------------------------------
  void HierarchicalView::renameContainer( IContainerMD *container,
                                          const std::string &newName )
    throw( MDException )
  {
    if( !container )
    {
      MDException ex;
      ex.getMessage() << "Invalid container (zero pointer)";
      throw ex;
    }

    if( newName.empty() )
    {
      MDException ex;
      ex.getMessage() << "Invalid new name (empty)";
      throw ex;
    }

    if( newName.find( '/' ) != std::string::npos )
    {
      MDException ex;
      ex.getMessage() << "Name cannot contain slashes: " << newName;
      throw ex;
    }

    if( container->getId() == container->getParentId() )
    {
      MDException ex;
      ex.getMessage() << "Cannot rename /";
      throw ex;
    }

    IContainerMD *parent = pContainerSvc->getContainerMD( container->getParentId() );
    if( parent->findContainer( newName ) )
    {
      MDException ex;
      ex.getMessage() << "Container exists: " << newName;
      throw ex;
    }

    if( parent->findFile( newName ) )
    {
      MDException ex;
      ex.getMessage() << "File exists: " << newName;
      throw ex;
    }

    parent->removeContainer( container->getName() );
    container->setName( newName );
    parent->addContainer( container );
    updateContainerStore( container );
  }

  //----------------------------------------------------------------------------
  // Rename file
  //----------------------------------------------------------------------------
  void HierarchicalView::renameFile( IFileMD *file, const std::string &newName )
    throw( MDException )
  {
    if( !file )
    {
      MDException ex;
      ex.getMessage() << "Invalid file (zero pointer)";
      throw ex;
    }

    if( newName.empty() )
    {
      MDException ex;
      ex.getMessage() << "Invalid new name (empty)";
      throw ex;
    }

    if( newName.find( '/' ) != std::string::npos )
    {
      MDException ex;
      ex.getMessage() << "Name cannot contain slashes: " << newName;
      throw ex;
    }

    IContainerMD *parent = pContainerSvc->getContainerMD( file->getContainerId() );
    if( parent->findContainer( newName ) )
    {
      MDException ex;
      ex.getMessage() << "Container exists: " << newName;
      throw ex;
    }

    if( parent->findFile( newName ) )
    {
      MDException ex;
      ex.getMessage() << "File exists: " << newName;
      throw ex;
    }

    parent->removeFile( file->getName() );
    file->setName( newName );
    parent->addFile( file );
    updateFileStore( file );
  }
};
