//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   ContainerMD service interface
//------------------------------------------------------------------------------

#ifndef EOS_I_CONTAINER_MD_SVC_HH
#define EOS_I_CONTAINER_MD_SVC_HH

#include "ContainerMD.hh"
#include "MDException.hh"

#include <map>
#include <string>

namespace eos
{
  //----------------------------------------------------------------------------
  //! Interface for the listener that is notified about all of the
  //! actions performed in a IContainerMDSvc
  //----------------------------------------------------------------------------
  class IContainerMDChangeListener
  {
    public:
      enum Action
      {
        Updated = 0,
        Deleted,
        Created
      };

      virtual void containerMDChanged( ContainerMD *obj, Action type );
  };

  //----------------------------------------------------------------------------
  //! Interface for class responsible for managing the metadata information
  //! concerning containers. It is responsible for assigning container IDs and
  //! managing storage of the metadata. Could be implemented as a change log or
  //! db based store or as an interface to memcached or some other caching
  //! system or key value store
  //----------------------------------------------------------------------------
  class IContainerMDSvc
  {
    public:
      virtual ~IContainerMDSvc() {}

      //------------------------------------------------------------------------
      //! Initizlize the container service
      //------------------------------------------------------------------------
      virtual void initialize() throw( MDException ) = 0;

      //------------------------------------------------------------------------
      //! Configure the container service
      //------------------------------------------------------------------------
      virtual void configure( std::map<std::string, std::string> &config )
                                                      throw( MDException ) = 0;

      //------------------------------------------------------------------------
      //! Finalize the container service
      //------------------------------------------------------------------------
      virtual void finalize() throw( MDException ) = 0;

      //------------------------------------------------------------------------
      //! Get the file metadata information for the given file ID
      //------------------------------------------------------------------------
      virtual ContainerMD *getContainerMD( ContainerMD::id_t id )
                                                        throw( MDException ) = 0;

      //------------------------------------------------------------------------
      //! Create new container metadata object with an assigned id, the user has
      //! to fill all the remaining fields
      //------------------------------------------------------------------------
      virtual ContainerMD *createContainer() throw( MDException ) = 0;

      //------------------------------------------------------------------------
      //! Update the contaienr metadata in the backing store after the
      //! ContainerMD object has been changed
      //------------------------------------------------------------------------
      virtual void updateStore( ContainerMD *obj ) throw( MDException ) = 0;

      //------------------------------------------------------------------------
      //! Remove object from the store
      //------------------------------------------------------------------------
      virtual void removeContainer( ContainerMD *obj ) throw( MDException ) = 0;

      //------------------------------------------------------------------------
      //! Remove object from the store
      //------------------------------------------------------------------------
      virtual void removeContainer( ContainerMD::id_t containerId )
                                                      throw( MDException ) = 0;

      //------------------------------------------------------------------------
      //! Add file listener that will be notified about all of the changes in
      //! the store
      //------------------------------------------------------------------------
      virtual void addChangeListener( IContainerMDChangeListener *listener ) = 0;
  };
}

#endif // EOS_I_FILE_MD_SVC_HH
