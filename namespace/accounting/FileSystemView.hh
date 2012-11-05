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

#ifndef EOS_NS_FILESYSTEM_VIEW_HH
#define EOS_NS_FILESYSTEM_VIEW_HH

#include "namespace/IFileMDSvc.hh"
#include "namespace/FileMD.hh"
#include "namespace/MDException.hh"
#include <utility>
#include <list>
#include <deque>
#include <google/dense_hash_set>

namespace eos
{
  class FileSystemView: public IFileMDChangeListener
  {
    public:
      //------------------------------------------------------------------------
      // Google sparse table is used for much lower memory overhead per item
      // than a list and it's fragmented structure speeding up deletions.
      // The filelists we keep are quite big - a list would be faster
      // but more memory consuming, a vector would be slower but less
      // memory consuming. We changed to dense hash set since it is much faster
      // and the memory overhead is not visible in a million file namespace.
      //------------------------------------------------------------------------
      typedef google::dense_hash_set<FileMD::id_t> FileList;
      typedef FileList::iterator                   FileIterator;

      //------------------------------------------------------------------------
      //! Constructor
      //------------------------------------------------------------------------
      FileSystemView();

      //------------------------------------------------------------------------
      //! Destructor
      //------------------------------------------------------------------------
      virtual ~FileSystemView(){}

    
      //------------------------------------------------------------------------
      //! Notify me about the changes in the main view
      //------------------------------------------------------------------------
      virtual void fileMDChanged( IFileMDChangeListener::Event *e );

      //------------------------------------------------------------------------
      //! Notify me about files when recovering from changelog
      //------------------------------------------------------------------------
      virtual void fileMDRead( FileMD *obj );

      //------------------------------------------------------------------------
      //! Get a list of files registered in given fs
      //------------------------------------------------------------------------
      std::pair<FileIterator, FileIterator> getFiles(
                                               FileMD::location_t location )
        throw( MDException );

      //------------------------------------------------------------------------
      //! Get a list of unlinked but not deleted files
      //------------------------------------------------------------------------
      std::pair<FileIterator, FileIterator> getUnlinkedFiles(
                                                FileMD::location_t location )
        throw( MDException );

      //------------------------------------------------------------------------
      //! Get a list of unlinked but not deleted files
      //------------------------------------------------------------------------
      std::pair<FileIterator, FileIterator> getNoReplicaFiles()
        throw( MDException );

      //------------------------------------------------------------------------
      //! Return reference to a list of files
      //! BEWARE: any replica change may invalidate iterators
      //------------------------------------------------------------------------
      const FileList &getFileList( FileMD::location_t location )
        throw( MDException );

      //------------------------------------------------------------------------
      //! Return reference to a list of unlinked files
      //! BEWARE: any replica change may invalidate iterators
      //------------------------------------------------------------------------
      const FileList &getUnlinkedFileList( FileMD::location_t location )
        throw( MDException );

      //------------------------------------------------------------------------
      //! Get number of file systems
      //------------------------------------------------------------------------
      size_t getNumFileSystems() const
      {
        return pFiles.size();
      }

      //------------------------------------------------------------------------
      //! Get list of files without replicas
      //! BEWARE: any replica change may invalidate iterators
      //------------------------------------------------------------------------
      FileList &getNoReplicasFileList()
      {
        return pNoReplicas;
      }

      //------------------------------------------------------------------------
      //! Get list of files without replicas
      //! BEWARE: any replica change may invalidate iterators
      //------------------------------------------------------------------------
      const FileList &getNoReplicasFileList() const
      {
        return pNoReplicas;
      }

      //------------------------------------------------------------------------
      //! Initizalie
      //------------------------------------------------------------------------
      void initialize();

      //------------------------------------------------------------------------
      //! Finalize
      //------------------------------------------------------------------------
      void finalize();

    private:
      std::vector<FileList> pFiles;
      std::vector<FileList> pUnlinkedFiles;
      FileList              pNoReplicas;
  };
}

#endif // EOS_NS_FILESYSTEM_VIEW_HH
