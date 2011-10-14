//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   Change log based FileMD service
//------------------------------------------------------------------------------

#ifndef EOS_NS_CHANGE_LOG_FILE_MD_SVC_HH
#define EOS_NS_CHANGE_LOG_FILE_MD_SVC_HH

#include "namespace/FileMD.hh"
#include "namespace/MDException.hh"
#include "namespace/IFileMDSvc.hh"
#include "namespace/persistency/ChangeLogFile.hh"

#include <google/sparse_hash_map>
#include <list>

namespace eos
{
  //----------------------------------------------------------------------------
  //! Change log based FileMD service
  //----------------------------------------------------------------------------
  class ChangeLogFileMDSvc: public IFileMDSvc
  {
  public:
    //------------------------------------------------------------------------
    //! Constructor
    //------------------------------------------------------------------------
    ChangeLogFileMDSvc(): pFirstFreeId( 1 ), pChangeLog( 0 )
    {
      pIdMap.set_deleted_key( 0 );
      pChangeLog = new ChangeLogFile;
    }

    //------------------------------------------------------------------------
    //! Destructor
    //------------------------------------------------------------------------
    virtual ~ChangeLogFileMDSvc()
    {
      delete pChangeLog;
    }

    //------------------------------------------------------------------------
    //! Initizlize the file service
    //------------------------------------------------------------------------
    virtual void initialize() throw( MDException );

    //------------------------------------------------------------------------
    //! Configure the file service
    //------------------------------------------------------------------------
    virtual void configure( std::map<std::string, std::string> &config )
      throw( MDException );

    //------------------------------------------------------------------------
    //! Finalize the file service
    //------------------------------------------------------------------------
    virtual void finalize() throw( MDException );

    //------------------------------------------------------------------------
    //! Get the file metadata information for the given file ID
    //------------------------------------------------------------------------
    virtual FileMD *getFileMD( FileMD::id_t id ) throw( MDException );

    //------------------------------------------------------------------------
    //! Create new file metadata object with an assigned id
    //------------------------------------------------------------------------
    virtual FileMD *createFile() throw( MDException );

    //------------------------------------------------------------------------
    //! Update the file metadata in the backing store after the FileMD object
    //! has been changed
    //------------------------------------------------------------------------
    virtual void updateStore( FileMD *obj ) throw( MDException );

    //------------------------------------------------------------------------
    //! Remove object from the store
    //------------------------------------------------------------------------
    virtual void removeFile( FileMD *obj ) throw( MDException );

    //------------------------------------------------------------------------
    //! Remove object from the store
    //------------------------------------------------------------------------
    virtual void removeFile( FileMD::id_t fileId ) throw( MDException );

    //------------------------------------------------------------------------
    //! Get number of files
    //------------------------------------------------------------------------
    virtual uint64_t getNumFiles() const
    {
      return pIdMap.size();
    }

    //------------------------------------------------------------------------
    //! Add file listener that will be notified about all of the changes in
    //! the store
    //------------------------------------------------------------------------
    virtual void addChangeListener( IFileMDChangeListener *listener );

    //------------------------------------------------------------------------
    //! Visit all the files
    //------------------------------------------------------------------------
    virtual void visit( IFileVisitor *visitor );

    //------------------------------------------------------------------------
    //! Notify the listeners about the change
    //------------------------------------------------------------------------
    virtual void notifyListeners( IFileMDChangeListener::Event *event )
    {
      ListenerList::iterator it;
      for( it = pListeners.begin(); it != pListeners.end(); ++it )
        (*it)->fileMDChanged( event );
    }

  private:
    //------------------------------------------------------------------------
    // Placeholder for the record info
    //------------------------------------------------------------------------
    struct DataInfo
    {
      DataInfo(): logOffset(0), ptr(0) {} // for some reason needed by sparse_hash_map::erase
      DataInfo( uint64_t logOffset, FileMD *ptr )
      {
        this->logOffset = logOffset;
        this->ptr       = ptr;
      }
      uint64_t  logOffset;
      FileMD   *ptr;
    };

    typedef google::sparse_hash_map<FileMD::id_t, DataInfo> IdMap;
    typedef std::list<IFileMDChangeListener*>               ListenerList;

    //------------------------------------------------------------------------
    // Changelog record scanner
    //------------------------------------------------------------------------
    class FileMDScanner: public ILogRecordScanner
    {
    public:
      FileMDScanner( IdMap &idMap ): pIdMap( idMap ), pLargestId( 0 )
      {}
      virtual void processRecord( uint64_t offset, char type,
                                  const Buffer &buffer );
      uint64_t getLargestId() const
      {
        return pLargestId;
      }
    private:
      IdMap    &pIdMap;
      uint64_t  pLargestId;
    };

    //------------------------------------------------------------------------
    // 
    //------------------------------------------------------------------------
    FileMD::id_t       pFirstFreeId;
    std::string        pChangeLogPath;
    ChangeLogFile     *pChangeLog;
    IdMap              pIdMap;
    ListenerList       pListeners;
  };
}

#endif // EOS_NS_CHANGE_LOG_FILE_MD_SVC_HH
