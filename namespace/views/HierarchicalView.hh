
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
// desc:   Hierarchical namespace implementation
//------------------------------------------------------------------------------

#ifndef EOS_NS_HIERARHICAL_VIEW_HH
#define EOS_NS_HIERARHICAL_VIEW_HH

#include "namespace/IView.hh"
#include "namespace/accounting/QuotaStats.hh"
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
        pQuotaStats = new QuotaStats();
      }

      //------------------------------------------------------------------------
      //! Destructor
      //------------------------------------------------------------------------
      virtual ~HierarchicalView()
      {
        delete pQuotaStats;
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

      virtual void initialize1() throw( MDException );// phase 1 - load & setup container
      virtual void initialize2() throw( MDException );// phase 2 - load files
      virtual void initialize3() throw( MDException );// phase 3 - register files in container

      //------------------------------------------------------------------------
      //! Finalize the view
      //------------------------------------------------------------------------
      virtual void finalize() throw( MDException );

      //------------------------------------------------------------------------
      //! Retrieve a file for given uri
      //------------------------------------------------------------------------
      virtual IFileMD *getFile( const std::string &uri ) throw( MDException );

      //------------------------------------------------------------------------
      //! Create a file for given uri
      //------------------------------------------------------------------------
      virtual IFileMD *createFile( const std::string &uri,
                                  uid_t uid = 0, gid_t gid = 0 )
        throw( MDException );

      //------------------------------------------------------------------------
      //! Update file store
      //------------------------------------------------------------------------
      virtual void updateFileStore( IFileMD *file ) throw( MDException )
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
      virtual void removeFile( IFileMD *file ) throw( MDException );

      //------------------------------------------------------------------------
      //! Get a container (directory)
      //------------------------------------------------------------------------
      virtual IContainerMD *getContainer( const std::string &uri )
        throw( MDException );

      //------------------------------------------------------------------------
      //! Create a container (directory)
      //------------------------------------------------------------------------
      virtual IContainerMD *createContainer( const std::string &uri,
                                            bool createParents = false )
        throw( MDException );

      //------------------------------------------------------------------------
      //! Update container store
      //------------------------------------------------------------------------
      virtual void updateContainerStore( IContainerMD *container )
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
      virtual std::string getUri( const IContainerMD *container ) const
        throw( MDException );

      //------------------------------------------------------------------------
      //! Get uri for the file
      //------------------------------------------------------------------------
      virtual std::string getUri( const IFileMD *file ) const
        throw( MDException );

      //------------------------------------------------------------------------
      //! Get quota node id concerning given container
      //------------------------------------------------------------------------
      virtual QuotaNode *getQuotaNode( const IContainerMD *container,
                                       bool               search = true )
        throw( MDException );

      //------------------------------------------------------------------------
      //! Register the container to be a quota node
      //------------------------------------------------------------------------
      virtual QuotaNode *registerQuotaNode( IContainerMD *container )
        throw( MDException );

      //------------------------------------------------------------------------
      //! Remove the quota node
      //------------------------------------------------------------------------
      virtual void removeQuotaNode( IContainerMD *container )
        throw( MDException );

      //------------------------------------------------------------------------
      //! Get the quota stats placeholder
      //------------------------------------------------------------------------
      virtual QuotaStats *getQuotaStats()
      {
        return pQuotaStats;
      }

      //------------------------------------------------------------------------
      //! Set the quota stats placeholder, currently associated object (if any)
      //! won't beX deleted.
      //------------------------------------------------------------------------
      virtual void setQuotaStats( QuotaStats *quotaStats )
      {
        pQuotaStats = quotaStats;
      }

      //------------------------------------------------------------------------
      //! Rename container
      //------------------------------------------------------------------------
      virtual void renameContainer( IContainerMD *container,
                                    const std::string &newName )
        throw( MDException );

      //------------------------------------------------------------------------
      //! Rename file
      //------------------------------------------------------------------------
      virtual void renameFile( IFileMD *file, const std::string &newName )
        throw( MDException );

    private:
      IContainerMD *findLastContainer( std::vector<char*> &elements, size_t end,
                                      size_t &index );
      void cleanUpContainer( IContainerMD *cont );

      //------------------------------------------------------------------------
      // File visitor for reloading
      //------------------------------------------------------------------------
      class FileVisitor: public IFileVisitor
      {
        public:
          FileVisitor(IContainerMDSvc *contSvc,
                      QuotaStats *quotaStats,
                      IView *view):
            pContSvc( contSvc ), pQuotaStats( quotaStats ), pView( view ) {}

          virtual void visitFile( IFileMD *file );
        
        private:
          IContainerMDSvc *pContSvc;
          QuotaStats      *pQuotaStats;
          IView           *pView;
      };

      //------------------------------------------------------------------------
      // Data members
      //------------------------------------------------------------------------
      IContainerMDSvc *pContainerSvc;
      IFileMDSvc      *pFileSvc;
      QuotaStats      *pQuotaStats;
      IContainerMD    *pRoot;
  };
};

#endif // EOS_NS_HIERARCHICAL_VIEW_HH
