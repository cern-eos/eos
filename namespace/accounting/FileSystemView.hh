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
#include <google/sparse_hash_set>

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
      // memory consuming
      //------------------------------------------------------------------------
      typedef google::sparse_hash_set<FileMD::id_t> FileList;
      typedef FileList::iterator                    FileIterator;

      //------------------------------------------------------------------------
      //! Constructor
      //------------------------------------------------------------------------
      FileSystemView();

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
                            FileMD::location_t location ) throw( MDException );

      //------------------------------------------------------------------------
      //! Get a list of unlinked but not deleted files 
      //------------------------------------------------------------------------
      std::pair<FileIterator, FileIterator> getUnlinkedFiles(
                              FileMD::location_t location ) throw( MDException );

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
      //! Initizalie
      //------------------------------------------------------------------------
      void initialize();

      //------------------------------------------------------------------------
      //! Finalize
      //------------------------------------------------------------------------
      void finalize();

    private:
      std::deque<FileList> pFiles;
      std::deque<FileList> pUnlinkedFiles;
  };
}

#endif // EOS_NS_FILESYSTEM_VIEW_HH
