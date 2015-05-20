#ifndef KINETICCLUSTERINTERFACE_HH
#define	KINETICCLUSTERINTERFACE_HH

#include <vector>
#include <string>
#include <memory>
#include <functional>
#include "kinetic/kinetic.h"

enum KeyOperationStatus{
  OK,
  VERSION_MISSMATCH,
  OFFLINE
};

//------------------------------------------------------------------------------
//! Interface to a kinetic cluster. Can be a drive, a simulator, or or a
//! whole cluster of either.
//------------------------------------------------------------------------------
class KineticClusterInterface{
public:
  //----------------------------------------------------------------------------
  //! Check the health of the Kinetic Cluster.
  //!
  //! @return true if the cluster is operational, false if KeyOperations cannot
  //!         be accepted.
  //----------------------------------------------------------------------------
  virtual bool ok() = 0;

  //----------------------------------------------------------------------------
  //! Check the maximum size of the Kinetic cluster.
  //!
  //! @return current size and capacity of the Kinetic cluster in bytes
  //----------------------------------------------------------------------------
  virtual kinetic::Capacity size() = 0;

  //----------------------------------------------------------------------------
  //! Obtain cluster limits, most importantly maximum key / value sizes.
  //! These limits may drastically differ from standard Kinetic drive limits,
  //! as for example the value might be written to multiple drives concurrently
  //! and some of the key-space might be reserved for cluster internal metadata.
  //! Limits remain constant during the cluster lifetime. 
  //!
  //! @return cluster limits
  //----------------------------------------------------------------------------
  virtual const kinetic::Limits& limits() = 0;

  //----------------------------------------------------------------------------
  //! Get the value and version associated with the supplied key.
  //
  //! @param key in: the key
  //! @param value out: stores the value upon success, not modified on erro
  //! @param version out: stores the version upon success, not modified on error
  //! @param in: skip_value if true only update the version, do not request the
  //!        value from the backend
  //! @return status of operation
  //----------------------------------------------------------------------------
  virtual kinetic::KineticStatus get(
                  const std::shared_ptr<const std::string>& key,
                  std::shared_ptr<const std::string>& version,
                  std::shared_ptr<std::string>& value,
                  bool skip_value) = 0;

  //----------------------------------------------------------------------------
  //! Write the supplied key-value pair to the Kinetic cluster.
  //!
  //! @param key in: the key
  //! @param value in: value to store
  //! @param version  in: existing version expected in the cluster
  //!                 out: newly created version on success
  //! @param force    if set to true, possibly existing version in the cluster
  //!                 will be overwritten without check
  //! @return status of operation
  //----------------------------------------------------------------------------
  virtual kinetic::KineticStatus put(
                    const std::shared_ptr<const std::string>& key,
                    std::shared_ptr<const std::string>& version,
                    const std::shared_ptr<const std::string>& value,
                    bool force) = 0;

  //----------------------------------------------------------------------------
  //! Delete the key on the cluster.
  //!
  //! @param key     the key
  //! @param version existing version expected in the cluster
  //! @param force   if set to true, possibly existing version in the cluster
  //!                will not be checked against supplied version
  //! @return status of operation
   //---------------------------------------------------------------------------
  virtual kinetic::KineticStatus remove(
                     const std::shared_ptr<const std::string>& key,
                     const std::shared_ptr<const std::string>& version,
                     bool force) = 0;

  //----------------------------------------------------------------------------
  //! Obtain keys in the supplied range [start,...,end]
  //!
  //! @param start  the start point of the requested key range, supplied key
  //!               is included in the range
  //! @param end    the end point of the requested key range, supplied key is
  //!               included in the range
  //! @param maxRequested the maximum number of keys requested (cannot be higher
  //!                     than limits allow)
  //! @param keys   on success, contains existing key names in supplied range
  //! @return status of operation
  //----------------------------------------------------------------------------
  virtual kinetic::KineticStatus range(
          const std::shared_ptr<const std::string>& start_keys,
          const std::shared_ptr<const std::string>& end_key,
          int maxRequested,
          std::unique_ptr< std::vector<std::string> >& keys) = 0;

  //----------------------------------------------------------------------------
  //! Destructor.
  //----------------------------------------------------------------------------
  virtual ~KineticClusterInterface(){};
};




#endif
