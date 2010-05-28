//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   Hierarchical view implementation
//------------------------------------------------------------------------------

#include "Namespace/views/HierarchicalView.hh"
#include "Namespace/utils/PathProcessor.hh"
#include "Namespace/IContainerMDSvc.hh"
#include "Namespace/IFileMDSvc.hh"
#include <iostream>

namespace eos
{
  //----------------------------------------------------------------------------
  // Configure the view
  //----------------------------------------------------------------------------
  void HierarchicalView::configure( std::map<std::string, std::string> &config )
  {
    if( !pContainerSvc )
    {
      MDException e;
      e.getMessage() << "Container MD Service was not set";
      throw e;
    }

    if( !pFileSvc )
    {
      MDException e;
      e.getMessage() << "Container MD Service was not set";
      throw e;
    }
  }

  //----------------------------------------------------------------------------
  // Initialize the view
  //----------------------------------------------------------------------------
  void HierarchicalView::initialize() throw( MDException )
  {
    pContainerSvc->initialize();
    pFileSvc->initialize();

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
      pContainerSvc->updateStore( pRoot );
    }

    //--------------------------------------------------------------------------
    // Scan all the files to reattach them to containers - THIS SHOULD NOT
    // BE DONE! THE INFO NEEDS TO BE STORED WITH CONTAINERS
    //--------------------------------------------------------------------------
    FileVisitor visitor( pContainerSvc );
    pFileSvc->visit( &visitor );
  }

  //----------------------------------------------------------------------------
  // Finalize the view
  //----------------------------------------------------------------------------
  void HierarchicalView::finalize() throw( MDException )
  {
    pContainerSvc->finalize();
    pFileSvc->finalize();
  }

  //----------------------------------------------------------------------------
  // Retrieve a file for given uri
  //----------------------------------------------------------------------------
  FileMD *HierarchicalView::getFile( const std::string &uri )
                                                            throw( MDException )
  {
    char uriBuffer[uri.length()+1];
    strcpy( uriBuffer, uri.c_str() );
    std::vector<char*> elements;
    eos::PathProcessor::splitPath( elements, uriBuffer );
    size_t position;
    ContainerMD *cont = findLastContainer( elements, elements.size()-1,
                                           position );

    if( position != elements.size()-1 )
    {
      MDException e;
      e.getMessage() << "Container does not exist";
      throw e;
    }

    FileMD *file = cont->findFile( elements[position] );
    if( !file )
    {
      MDException e;
      e.getMessage() << "No such file or directory";
      throw e;
    }
    return file;
  }

  //----------------------------------------------------------------------------
  // Create a file for given uri
  //----------------------------------------------------------------------------
  FileMD *HierarchicalView::createFile( const std::string &uri )
                                                            throw( MDException )
  {
    //--------------------------------------------------------------------------
    // Split the path and find the last container
    //--------------------------------------------------------------------------
    char uriBuffer[uri.length()+1];
    strcpy( uriBuffer, uri.c_str() );
    std::vector<char*> elements;
    eos::PathProcessor::splitPath( elements, uriBuffer );
    size_t position;
    ContainerMD *cont = findLastContainer( elements, elements.size()-1,
                                           position );

    if( position != elements.size()-1 )
    {
      MDException e;
      e.getMessage() << "Container does not exist";
      throw e;
    }

    //--------------------------------------------------------------------------
    // Check it the file of this name can be inserted
    //--------------------------------------------------------------------------
    if( cont->findContainer( elements[position] ) )
    {
      MDException e;
      e.getMessage() << "File exist";
      throw e;
    }

    if( cont->findFile( elements[position] ) )
    {
      MDException e;
      e.getMessage() << "File exist";
      throw e;
    }

    FileMD *file = pFileSvc->createFile();
    file->setName( elements[position] );
    cont->addFile( file );
    pFileSvc->updateStore( file );
    
    return file;
  }

  //----------------------------------------------------------------------------
  // Remove the file for given uri
  //----------------------------------------------------------------------------
  void HierarchicalView::removeFile( const std::string &uri )
                                                            throw( MDException )
  {
    char uriBuffer[uri.length()+1];
    strcpy( uriBuffer, uri.c_str() );
    std::vector<char*> elements;
    eos::PathProcessor::splitPath( elements, uriBuffer );
    size_t position;
    ContainerMD *cont = findLastContainer( elements, elements.size()-1,
                                           position );

    if( position != elements.size()-1 )
    {
      MDException e;
      e.getMessage() << "Container does not exist";
      throw e;
    }

    FileMD *file = cont->findFile( elements[position] );
    cont->removeFile( file->getName() );
    pFileSvc->removeFile( file );
  }

  //----------------------------------------------------------------------------
  // Get a container (directory)
  //----------------------------------------------------------------------------
  ContainerMD *HierarchicalView::getContainer( const std::string &uri )
                                                            throw( MDException )
  {
    if( uri == "/" )
      return pRoot;

    char uriBuffer[uri.length()+1];
    strcpy( uriBuffer, uri.c_str() );
    std::vector<char*> elements;
    eos::PathProcessor::splitPath( elements, uriBuffer );

    size_t position;
    ContainerMD *cont = findLastContainer( elements, elements.size(), position );

    if( position != elements.size() )
    {
      MDException e;
      e.getMessage() << uri << ": No such file or directory";
      throw e;
    }

    return cont;
  }

  //----------------------------------------------------------------------------
  // Create a container (directory) 
  //----------------------------------------------------------------------------
  ContainerMD *HierarchicalView::createContainer( const std::string &uri,
                                                  bool createParents )
                                                            throw( MDException )
  {
    //--------------------------------------------------------------------------
    // Split the path
    //--------------------------------------------------------------------------
    if( uri == "/" )
    {
      MDException e;
      e.getMessage() << uri << ": File exist" << std::endl;
      throw e;
    }

    char uriBuffer[uri.length()+1];
    strcpy( uriBuffer, uri.c_str() );
    std::vector<char*> elements;
    eos::PathProcessor::splitPath( elements, uriBuffer );

    if( elements.size() == 0 )
    {
      MDException e;
      e.getMessage() << uri << ": File exist" << std::endl;
      throw e;
    }

    //--------------------------------------------------------------------------
    // Look for the last existing container
    //--------------------------------------------------------------------------
    size_t position;
    ContainerMD *lastContainer = findLastContainer( elements, elements.size(),
                                                    position );

    if( position == elements.size() )
    {
      MDException e;
      e.getMessage() << uri << ": File exist" << std::endl;
      throw e;
    }

    //--------------------------------------------------------------------------
    // One of the parent containers does not exist
    //--------------------------------------------------------------------------
    if( (!createParents) && (position < elements.size()-1) )
    {
      MDException e;
      e.getMessage() << uri << ": Parent does not exist" << std::endl;
      throw e;
    }

    if( lastContainer->findFile( elements[position] ) )
    {
      MDException e;
      e.getMessage() << "File exists" << std::endl;
      throw e;
    }

    //--------------------------------------------------------------------------
    // Create the container with all missing parent's if requires
    //--------------------------------------------------------------------------
    for( size_t i = position; i < elements.size(); ++i )
    {
      ContainerMD *newContainer = pContainerSvc->createContainer();
      newContainer->setName( elements[i] );
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
      MDException e;
      e.getMessage() << "Permission denied.";
      throw e;
    }

    char uriBuffer[uri.length()+1];
    strcpy( uriBuffer, uri.c_str() );
    std::vector<char*> elements;
    eos::PathProcessor::splitPath( elements, uriBuffer );

    size_t position;
    ContainerMD *parent = findLastContainer( elements, elements.size()-1, position );
    if( (position != (elements.size()-1)) )
    {
      MDException e;
      e.getMessage() << uri << ": No such file or directory";
      throw e;
    }

    //--------------------------------------------------------------------------
    // Check if the container exist and remove it
    //--------------------------------------------------------------------------
    ContainerMD *cont = parent->findContainer( elements[elements.size()-1] );
    if( !cont )
    {
      MDException e;
      e.getMessage() << uri << ": No such file or directory";
      throw e;
    }

    if( (cont->getNumContainers() != 0 || cont->getNumFiles() != 0) &&
        !recursive )
    {
      MDException e;
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
  ContainerMD *HierarchicalView::findLastContainer( std::vector<char*> &elements,
                                                    size_t end, size_t &index )
  {
    ContainerMD *current  = pRoot;
    ContainerMD *found    = 0;
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
  void HierarchicalView::cleanUpContainer( ContainerMD *cont )
  {
    ContainerMD::FileMap::iterator itF;
    for( itF = cont->filesBegin(); itF != cont->filesEnd(); ++itF )
      pFileSvc->removeFile( itF->second );

    ContainerMD::ContainerMap::iterator itC;
    for( itC = cont->containersBegin(); itC != cont->containersEnd(); ++itC )
    {
      cleanUpContainer( itC->second );
      pContainerSvc->removeContainer( itC->second );
    }
  }

  //----------------------------------------------------------------------------
  // Visit reconnect the files and containers on initialization
  //----------------------------------------------------------------------------
  void HierarchicalView::FileVisitor::visitFile( FileMD *file )
  {
    ContainerMD *cont = pContSvc->getContainerMD( file->getContainerId() );
    cont->addFile( file );
  }
};
