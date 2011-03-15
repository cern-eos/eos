//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   Hierarchical namespace implementation
//------------------------------------------------------------------------------

#ifndef EOS_NS_HIERARHICAL_VIEW_HH
#define EOS_NS_HIERARHICAL_VIEW_HH

#include "namespace/IView.hh"
#include "namespace/IContainerMDSvc.hh"
#include "namespace/IFileMDSvc.hh"
#include "namespace/persistency/ChangeLogContainerMDSvc.hh"

namespace eos
{
  //----------------------------------------------------------------------------
  //! Implementation of the hierarchical namespace
  //----------------------------------------------------------------------------
  class HierarchicalView: public IView
  {
    public:
      //------------------------------------------------------------------------
      //! Constructor
      //------------------------------------------------------------------------
      HierarchicalView(): pContainerSvc( 0 ), pFileSvc( 0 ), pRoot( 0 )
      {
      }

      //------------------------------------------------------------------------
      //! Specify a pointer to the underlying container service
      //------------------------------------------------------------------------
      virtual void setContainerMDSvc( IContainerMDSvc *containerSvc )
      {
        pContainerSvc = containerSvc;
      }

      //------------------------------------------------------------------------
      //! Get the container svc pointer
      //------------------------------------------------------------------------
      virtual IContainerMDSvc *getContainerMDSvc()
      {
        return pContainerSvc;
      }

      //------------------------------------------------------------------------
      //! Specify a pointer to the underlying file service that alocates the
      //! actual files
      //------------------------------------------------------------------------
      virtual void setFileMDSvc( IFileMDSvc *fileMDSvc )
      {
        pFileSvc = fileMDSvc;
      }

      //------------------------------------------------------------------------
      //! Get the FileMDSvc
      //------------------------------------------------------------------------
      virtual IFileMDSvc *getFileMDSvc()
      {
        return pFileSvc;
      }

      //------------------------------------------------------------------------
      //! Configure the view
      //------------------------------------------------------------------------
      virtual void configure( std::map<std::string, std::string> &config );

      //------------------------------------------------------------------------
      //! Initialize the view
      //------------------------------------------------------------------------
      virtual void initialize() throw( MDException );

      //------------------------------------------------------------------------
      //! Finalize the view
      //------------------------------------------------------------------------
      virtual void finalize() throw( MDException );

      //------------------------------------------------------------------------
      //! Retrieve a file for given uri
      //------------------------------------------------------------------------
      virtual FileMD *getFile( const std::string &uri ) throw( MDException );

      //------------------------------------------------------------------------
      //! Create a file for given uri
      //------------------------------------------------------------------------
      virtual FileMD *createFile( const std::string &uri,
                                  uid_t uid = 0, gid_t gid = 0 )
                                                           throw( MDException );

      //------------------------------------------------------------------------
      //! Update file store
      //------------------------------------------------------------------------
      virtual void updateFileStore( FileMD *file ) throw( MDException )
      {
        pFileSvc->updateStore( file );
      }

      //------------------------------------------------------------------------
      //! Unlink the file
      //------------------------------------------------------------------------
      virtual void unlinkFile( const std::string &uri )
        throw( MDException );

      //------------------------------------------------------------------------
      //! Remove the file
      //------------------------------------------------------------------------
      virtual void removeFile( FileMD *file ) throw( MDException );

      //------------------------------------------------------------------------
      //! Get a container (directory)
      //------------------------------------------------------------------------
      virtual ContainerMD *getContainer( const std::string &uri )
                                                        throw( MDException );

      //------------------------------------------------------------------------
      //! Create a container (directory) 
      //------------------------------------------------------------------------
      virtual ContainerMD *createContainer( const std::string &uri,
                                            bool createParents = false )
                                                        throw( MDException );

      //------------------------------------------------------------------------
      //! Update container store
      //------------------------------------------------------------------------
      virtual void updateContainerStore( ContainerMD *container )
                                                      throw( MDException )
      {
        pContainerSvc->updateStore( container );
      }

      //------------------------------------------------------------------------
      //! Remove a container (directory) 
      //------------------------------------------------------------------------
      virtual void removeContainer( const std::string &uri,
                                    bool recursive = false )
                                                        throw( MDException );

      //------------------------------------------------------------------------
      //! Get uri for the container
      //------------------------------------------------------------------------
      virtual std::string getUri( const ContainerMD *container ) const
        throw( MDException );

      //------------------------------------------------------------------------
      //! Get uri for the file
      //------------------------------------------------------------------------
      virtual std::string getUri( const FileMD *file ) const
        throw( MDException );

    private:
      ContainerMD *findLastContainer( std::vector<char*> &elements, size_t end,
                                      size_t &index );
      void cleanUpContainer( ContainerMD *cont );

      //------------------------------------------------------------------------
      // File visitor for reloading
      //------------------------------------------------------------------------
      class FileVisitor: public IFileVisitor
      {
        public:
          FileVisitor( IContainerMDSvc *contSvc ): pContSvc( contSvc ) {}
          virtual void visitFile( FileMD *file );
        private:
          IContainerMDSvc *pContSvc;
      };

      //------------------------------------------------------------------------
      // Data members
      //------------------------------------------------------------------------
      IContainerMDSvc *pContainerSvc;
      IFileMDSvc      *pFileSvc;
      ContainerMD     *pRoot;
  };
};

#endif // EOS_NS_HIERARCHICAL_VIEW_HH
