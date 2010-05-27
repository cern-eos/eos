//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   FileMD interface
//------------------------------------------------------------------------------

#ifndef EOS_I_FILE_MD_SVC_HH
#define EOS_I_FILE_MD_SVC_HH

#include "FileMD.hh"
#include "MDException.hh"

#include <map>
#include <string>

namespace eos
{
  //----------------------------------------------------------------------------
  //! Interface for a listener that is notified about all of the
  //! actions performed in a IFileMDSvc
  //----------------------------------------------------------------------------
  class IFileMDChangeListener
  {
    public:
      enum Action
      {
        Updated = 0,
        Deleted,
        Created
      };

      virtual void fileMDChanged( FileMD *obj, Action type );
  };

  //----------------------------------------------------------------------------
  //! Interface for a file visitor
  //----------------------------------------------------------------------------
  class IFileVisitor
  {
    public:
      virtual void visitFile( FileMD *file ) = 0;
  };

  //----------------------------------------------------------------------------
  //! Interface for class responsible for managing the metadata information
  //! concerning files. It is responsible for assigning file IDs and managing
  //! storage of the metadata. Could be implemented as a change log or db based
  //! store or as an interface to memcached or some other caching system or
  //! key value store
  //----------------------------------------------------------------------------
  class IFileMDSvc
  {
    public:
      //------------------------------------------------------------------------
      //! Destructor
      //------------------------------------------------------------------------
      virtual ~IFileMDSvc() {}

      //------------------------------------------------------------------------
      //! Initialize the file service
      //------------------------------------------------------------------------
      virtual void initialize() throw( MDException ) = 0;

      //------------------------------------------------------------------------
      //! Configure the file service
      //------------------------------------------------------------------------
      virtual void configure( std::map<std::string, std::string> &config )
                                                      throw( MDException ) = 0;

      //------------------------------------------------------------------------
      //! Finalize the file service
      //------------------------------------------------------------------------
      virtual void finalize() throw( MDException ) = 0;

      //------------------------------------------------------------------------
      //! Get the file metadata information for the given file ID
      //------------------------------------------------------------------------
      virtual FileMD *getFileMD( FileMD::id_t id ) throw( MDException ) = 0;

      //------------------------------------------------------------------------
      //! Create new file metadata object with an assigned id, the user has
      //! to fill all the remaining fields
      //------------------------------------------------------------------------
      virtual FileMD *createFile() throw( MDException ) = 0;

      //------------------------------------------------------------------------
      //! Update the file metadata in the backing store after the FileMD object
      //! has been changed
      //------------------------------------------------------------------------
      virtual void updateStore( FileMD *obj ) throw( MDException ) = 0;

      //------------------------------------------------------------------------
      //! Remove object from the store
      //------------------------------------------------------------------------
      virtual void removeFile( FileMD *obj ) throw( MDException ) = 0;

      //------------------------------------------------------------------------
      //! Remove object from the store
      //------------------------------------------------------------------------
      virtual void removeFile( FileMD::id_t fileId ) throw( MDException ) = 0;

      //------------------------------------------------------------------------
      //! Visit all the files
      //------------------------------------------------------------------------
      virtual void visit( IFileVisitor *visitor ) = 0;

      //------------------------------------------------------------------------
      //! Add file listener that will be notified about all of the changes in
      //! the store
      //------------------------------------------------------------------------
      virtual void addChangeListener( IFileMDChangeListener *listener ) = 0;
  };
}

#endif // EOS_I_FILE_MD_SVC_HH
