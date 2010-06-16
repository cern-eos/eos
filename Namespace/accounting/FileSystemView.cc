//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   The filesystem view over the stored files
//------------------------------------------------------------------------------

#include "Namespace/accounting/FileSystemView.hh"
#include <iostream>

namespace eos
{
  //----------------------------------------------------------------------------
  // Resize
  //----------------------------------------------------------------------------
  template<class Cont>
  static void resize( Cont &d, size_t size )
  {
    size_t oldSize = d.size();
    if( size <= oldSize )
      return;
    d.resize(size);
    for( size_t i = oldSize; i < size; ++i )
      d[i].set_deleted_key( 0 );
  }

  //----------------------------------------------------------------------------
  // Constructor
  //----------------------------------------------------------------------------
  FileSystemView::FileSystemView()
  {
  }

  //----------------------------------------------------------------------------
  // Notify the me about the changes in the main view
  //----------------------------------------------------------------------------
  void FileSystemView::fileMDChanged( IFileMDChangeListener::Event *e )
  {
    switch( e->action )
    {
      case IFileMDChangeListener::LocationAdded:
        resize( pFiles, e->location+1 );
        resize( pUnlinkedFiles, e->location+1 );
        pFiles[e->location].insert( e->file->getId() );
        break;

      case IFileMDChangeListener::LocationReplaced:
        if( e->oldLocation >= pFiles.size() )
          return; // incostency should probably crash here...

        if( e->location >= pFiles.size() )
        {
          resize( pFiles, e->location+1 );
          resize( pUnlinkedFiles, e->location+1 );
        }
        pFiles[e->oldLocation].erase( e->file->getId() );
        pFiles[e->location].insert( e->file->getId() );
        break;

      case IFileMDChangeListener::LocationRemoved:
        if( e->location >= pFiles.size() )
          return; // incostency should probably crash here...
        pFiles[e->location].erase( e->file->getId() );
        break;

      case IFileMDChangeListener::Updated:
      {
        if( e->file->getContainerId() != 0 )
          return;
        FileMD::LocationVector::const_iterator it;
        for( it = e->file->locationsBegin();
             it != e->file->locationsEnd(); ++it )
        {
          if( *it >= pUnlinkedFiles.size() )
          {
            resize( pFiles, *it+1 );
            resize( pUnlinkedFiles, *it+1 );
          }

          if( *it >= pFiles.size() )
            continue; // incostency should probably crash here...

          pFiles[*it].erase( e->file->getId() );
          pUnlinkedFiles[*it].insert( e->file->getId() );
        }
        break;
      }

      default:
        break;
    }
  }

  //----------------------------------------------------------------------------
  // Notify me about files when recovering from changelog
  //----------------------------------------------------------------------------
  void FileSystemView::fileMDRead( FileMD *obj )
  {
    std::deque<FileList> *coll;
    if( obj->getContainerId() == 0 )
      coll = &pUnlinkedFiles;
    else
      coll = &pFiles;

    FileMD::LocationVector::const_iterator it;
    for( it = obj->locationsBegin();
         it != obj->locationsEnd(); ++it )
    {
      if( *it >= coll->size() )
        coll->resize( *it+1 );
      (*coll)[*it].insert( obj->getId() );
    }
  }

  //----------------------------------------------------------------------------
  // Get a list of files registered in given fs
  //----------------------------------------------------------------------------
  std::pair<FileSystemView::FileIterator, FileSystemView::FileIterator>
                          FileSystemView::getFiles(
                            FileMD::location_t location ) throw( MDException )
  {
    if( pFiles.size() <= location )
    {
      MDException e( ENOENT );
      e.getMessage() << "Location does not exist" << std::endl;
      throw( e );
    }

    return std::make_pair( pFiles[location].begin(), pFiles[location].end() );
  }

  //----------------------------------------------------------------------------
  // Get a list of unlinked but not deleted files 
  //----------------------------------------------------------------------------
  std::pair<FileSystemView::FileIterator, FileSystemView::FileIterator>
                            FileSystemView::getUnlinkedFiles(
                              FileMD::location_t location ) throw( MDException )
  {
    if( pUnlinkedFiles.size() <= location )
    {
      MDException e( ENOENT );
      e.getMessage() << "Location does not exist" << std::endl;
      throw( e );
    }


    return std::make_pair( pUnlinkedFiles[location].begin(), pUnlinkedFiles[location].end() );
  }


  //----------------------------------------------------------------------------
  // Return reference to a list of files
  //----------------------------------------------------------------------------
  const FileSystemView::FileList &FileSystemView::getFileList(
                                                  FileMD::location_t location )
                                                          throw( MDException )
  {
    if( pFiles.size() <= location )
    {
      MDException e( ENOENT );
      e.getMessage() << "Location does not exist" << std::endl;
      throw( e );
    }
    return pFiles[location];
  }

  //----------------------------------------------------------------------------
  // Return reference to a list of unlinked files
  //----------------------------------------------------------------------------
  const FileSystemView::FileList &FileSystemView::getUnlinkedFileList(
                                                  FileMD::location_t location )
                                                          throw( MDException )
  {
    if( pUnlinkedFiles.size() <= location )
    {
      MDException e( ENOENT );
      e.getMessage() << "Location does not exist" << std::endl;
      throw( e );
    }

    return pUnlinkedFiles[location];
  }

  //----------------------------------------------------------------------------
  // Initialize
  //----------------------------------------------------------------------------
  void FileSystemView::initialize()
  {
  }

  //----------------------------------------------------------------------------
  // Finalize
  //----------------------------------------------------------------------------
  void FileSystemView::finalize()
  {
    pFiles.clear();
    pUnlinkedFiles.clear();
  }
}
