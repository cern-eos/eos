//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   View service interface
//------------------------------------------------------------------------------

#ifndef EOS_I_VIEW_HH
#define EOS_I_VIEW_HH

#include <map>
#include <string>
#include "Namespace/IFileMDSvc.hh"
#include "Namespace/IContainerMDSvc.hh"
#include "Namespace/MDException.hh"
#include "Namespace/ContainerMD.hh"

namespace eos
{
  //----------------------------------------------------------------------------
  //! Interface for the component responsible for the namespace.
  //! A concrete implementation could handle a hierarchical namespace,
  //! lists of files in the fileservers, lists of files belonging
  //! to users, container based store etc.
  //----------------------------------------------------------------------------
  class IView
  {
    public:

      //------------------------------------------------------------------------
      //! Specify a pointer to the underlying container service
      //------------------------------------------------------------------------
      virtual void setContainerMDSvc( IContainerMDSvc *containerSvc ) = 0;

      //------------------------------------------------------------------------
      //! Get the container svc pointer
      //------------------------------------------------------------------------
      virtual IContainerMDSvc *getContainerMDSvc() = 0;

      //------------------------------------------------------------------------
      //! Specify a pointer to the underlying file service, that alocates the
      //! actual files
      //------------------------------------------------------------------------
      virtual void setFileMDSvc( IFileMDSvc *fileMDSvc ) = 0;

      //------------------------------------------------------------------------
      //! Get the FileMDSvc
      //------------------------------------------------------------------------
      virtual IFileMDSvc *getFileMDSvc() = 0;

      //------------------------------------------------------------------------
      //! Configure the view
      //------------------------------------------------------------------------
      virtual void configure( std::map<std::string, std::string> &config ) = 0;

      //------------------------------------------------------------------------
      //! Initialize the view
      //------------------------------------------------------------------------
      virtual void initialize() throw( MDException ) = 0;

      //------------------------------------------------------------------------
      //! Finalizelize the view
      //------------------------------------------------------------------------
      virtual void finalize() throw( MDException ) = 0;

      //------------------------------------------------------------------------
      //! Retrieve a file for given uri
      //------------------------------------------------------------------------
      virtual FileMD *getFile( const std::string &uri ) throw( MDException ) = 0;

      //------------------------------------------------------------------------
      //! Update file store
      //------------------------------------------------------------------------
      virtual void updateFileStore( FileMD *file ) throw( MDException ) = 0;

      //------------------------------------------------------------------------
      //! Create a file for given uri
      //------------------------------------------------------------------------
      virtual FileMD *createFile( const std::string &uri,
                                  uid_t uid = 0, gid_t gid = 0 )
                                                      throw( MDException ) = 0;

      //------------------------------------------------------------------------
      //! Remove the file for given uri
      //------------------------------------------------------------------------
      virtual void removeFile( const std::string &uri )
                                                      throw( MDException ) = 0;

      //------------------------------------------------------------------------
      //! Get a container (directory)
      //------------------------------------------------------------------------
      virtual ContainerMD *getContainer( const std::string &uri )
                                                        throw( MDException )= 0;

      //------------------------------------------------------------------------
      //! Create a container (directory) 
      //------------------------------------------------------------------------
      virtual ContainerMD *createContainer( const std::string &uri,
                                            bool createParents = false )
                                                        throw( MDException )= 0;

      //------------------------------------------------------------------------
      //! Update container store
      //------------------------------------------------------------------------
      virtual void updateContainerStore( ContainerMD *container )
                                                      throw( MDException ) = 0;

      //------------------------------------------------------------------------
      //! Remove a container (directory) 
      //------------------------------------------------------------------------
      virtual void removeContainer( const std::string &uri,
                                    bool recursive = false )
                                                        throw( MDException )= 0;
  };
};

#endif // EOS_I_VIEW_HH
