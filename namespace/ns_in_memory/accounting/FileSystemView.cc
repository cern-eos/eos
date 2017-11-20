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
// desc:   The filesystem view over the stored files
//------------------------------------------------------------------------------

#include "namespace/ns_in_memory/accounting/FileSystemView.hh"
#include <iostream>

EOSNSNAMESPACE_BEGIN

//----------------------------------------------------------------------------
// Resize
//----------------------------------------------------------------------------
template<class Cont>
static void resize(Cont& d, size_t size)
{
  size_t oldSize = d.size();

  if (size <= oldSize) {
    return;
  }

  d.resize(size);

  for (size_t i = oldSize; i < size; ++i) {
    d[i].set_deleted_key(0);
    d[i].set_empty_key(0xffffffffffffffffll);
  }
}

//----------------------------------------------------------------------------
// Constructor
//----------------------------------------------------------------------------
FileSystemView::FileSystemView()
{
  pNoReplicas.set_empty_key(0xffffffffffffffffll);
  pNoReplicas.set_deleted_key(0);
}

//----------------------------------------------------------------------------
// Notify the me about the changes in the main view
//----------------------------------------------------------------------------
void FileSystemView::fileMDChanged(IFileMDChangeListener::Event* e)
{
  switch (e->action) {
  //------------------------------------------------------------------------
  // New file has been created
  //------------------------------------------------------------------------
  case IFileMDChangeListener::Created:
    if (!e->file->isLink()) {
      pNoReplicas.insert(e->file->getId());
    }

    break;

  //------------------------------------------------------------------------
  // File has been deleted
  //------------------------------------------------------------------------
  case IFileMDChangeListener::Deleted:
    pNoReplicas.erase(e->file->getId());
    break;

  //------------------------------------------------------------------------
  // Add location
  //------------------------------------------------------------------------
  case IFileMDChangeListener::LocationAdded:
    resize(pFiles, e->location + 1);
    resize(pUnlinkedFiles, e->location + 1);
    pFiles[e->location].insert(e->file->getId());
    pNoReplicas.erase(e->file->getId());
    break;

  //------------------------------------------------------------------------
  // Replace location
  //------------------------------------------------------------------------
  case IFileMDChangeListener::LocationReplaced:
    if (e->oldLocation >= pFiles.size()) {
      return;  // incostency, we should probably crash here...
    }

    resize(pFiles, e->location + 1);
    resize(pUnlinkedFiles, e->location + 1);
    pFiles[e->oldLocation].erase(e->file->getId());
    pFiles[e->location].insert(e->file->getId());
    break;

  //------------------------------------------------------------------------
  // Remove location
  //------------------------------------------------------------------------
  case IFileMDChangeListener::LocationRemoved:
    if (e->location >= pUnlinkedFiles.size()) {
      return;  // incostency, we should probably crash here...
    }

    pUnlinkedFiles[e->location].erase(e->file->getId());

    if (!e->file->getNumUnlinkedLocation() && !e->file->getNumLocation()) {
      pNoReplicas.insert(e->file->getId());
    }

    break;

  //------------------------------------------------------------------------
  // Unlink location
  //------------------------------------------------------------------------
  case IFileMDChangeListener::LocationUnlinked:
    if (e->location >= pFiles.size()) {
      return;  // incostency, we should probably crash here...
    }

    pFiles[e->location].erase(e->file->getId());
    pUnlinkedFiles[e->location].insert(e->file->getId());
    break;

  default:
    break;
  }
}

//----------------------------------------------------------------------------
// Notify me about files when recovering from changelog
//----------------------------------------------------------------------------
void FileSystemView::fileMDRead(IFileMD* obj)
{
  IFileMD::LocationVector::const_iterator it;
  IFileMD::LocationVector loc_vect = obj->getLocations();

  for (it = loc_vect.begin(); it != loc_vect.end(); ++it) {
    resize(pFiles, *it + 1);
    resize(pUnlinkedFiles, *it + 1);
    pFiles[*it].insert(obj->getId());
  }

  IFileMD::LocationVector unlink_vect = obj->getUnlinkedLocations();

  for (it = unlink_vect.begin(); it != unlink_vect.end(); ++it) {
    resize(pFiles, *it + 1);
    resize(pUnlinkedFiles, *it + 1);
    pUnlinkedFiles[*it].insert(obj->getId());
  }

  if (obj->getNumLocation() == 0 && obj->getNumUnlinkedLocation() == 0) {
    pNoReplicas.insert(obj->getId());
  }
}

//----------------------------------------------------------------------------
// Get iterator to list of files on a particular file system
//----------------------------------------------------------------------------
std::shared_ptr<ICollectionIterator<IFileMD::id_t>>
    FileSystemView::getFileList(IFileMD::location_t location)
{
  if (pFiles.size() <= location) {
    return nullptr;
  }

  return std::shared_ptr<ICollectionIterator<IFileMD::id_t>>
         (new FileIterator(pFiles[location]));
}

//------------------------------------------------------------------------------
// Get iterator to list of unlinked files on a particular file system
//------------------------------------------------------------------------------
std::shared_ptr<ICollectionIterator<IFileMD::id_t>>
    FileSystemView::getUnlinkedFileList(IFileMD::location_t location)
{
  if (pUnlinkedFiles.size() <= location) {
    return nullptr;
  }

  return std::shared_ptr<ICollectionIterator<IFileMD::id_t>>
         (new FileIterator(pUnlinkedFiles[location]));
}

//------------------------------------------------------------------------------
// Clear unlinked files for filesystem
//------------------------------------------------------------------------------
bool
FileSystemView::clearUnlinkedFileList(IFileMD::location_t location)
{
  pUnlinkedFiles[location].clear();
  return true;
}

//------------------------------------------------------------------------------
// Get iterator to list of files without replicas
//------------------------------------------------------------------------------
std::shared_ptr<ICollectionIterator<IFileMD::id_t>>
    FileSystemView::getNoReplicasFileList()
{
  return std::shared_ptr<ICollectionIterator<IFileMD::id_t>>
         (new FileIterator(pNoReplicas));
}

//----------------------------------------------------------------------------
// Finalize
//----------------------------------------------------------------------------
void FileSystemView::finalize()
{
  pFiles.clear();
  pUnlinkedFiles.clear();
  pNoReplicas.clear();
}

//----------------------------------------------------------------------------
// Shrink
//----------------------------------------------------------------------------
void FileSystemView::shrink()
{
  for (size_t i = 0; i < pFiles.size(); ++i) {
    pFiles[i].resize(0);
  }

  for (size_t i = 0; i < pUnlinkedFiles.size(); ++i) {
    pUnlinkedFiles[i].resize(0);
  }
}

//------------------------------------------------------------------------------
// Get number of files on the given file system
//------------------------------------------------------------------------------
uint64_t
FileSystemView::getNumFilesOnFs(IFileMD::location_t fs_id)
{
  if (pFiles.size() <= fs_id) {
    return 0ull;
  }

  return pFiles[fs_id].size();
}


//------------------------------------------------------------------------------
// Get number of unlinked files on the given file system
//------------------------------------------------------------------------------
uint64_t
FileSystemView::getNumUnlinkedFilesOnFs(IFileMD::location_t fs_id)
{
  if (pUnlinkedFiles.size() <= fs_id) {
    return 0ull;
  }

  return pUnlinkedFiles[fs_id].size();
}

//------------------------------------------------------------------------------
// Check if file system has file id
//------------------------------------------------------------------------------
bool
FileSystemView::hasFileId(IFileMD::id_t fid, IFileMD::location_t fs_id) const
{
  if (pFiles.size() <= fs_id) {
    return false;
  }

  return (pFiles[fs_id].find(fid) != pFiles[fs_id].end());
}

EOSNSNAMESPACE_END
