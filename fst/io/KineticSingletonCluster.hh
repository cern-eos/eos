#ifndef KINETICSPACESIMPLECPP_HH
#define	KINETICSPACESIMPLECPP_HH

#include "KineticClusterInterface.hh"
#include <chrono>

/* Implementing the interface for a single drive. */
class KineticSingletonCluster : public KineticClusterInterface {
public:
  //! See documentation in superclass.
  bool ok();
  //! See documentation in superclass.
  kinetic::Capacity size();
  //! See documentation in superclass.
  const kinetic::Limits& limits();
  //! See documentation in superclass.
  kinetic::KineticStatus get(const std::shared_ptr<const std::string>& key,
      std::shared_ptr<const std::string>& version,
      std::shared_ptr<std::string>& value,
      bool skip_value);
  //! See documentation in superclass.
  kinetic::KineticStatus put(const std::shared_ptr<const std::string>& key,
      std::shared_ptr<const std::string>& version,
      const std::shared_ptr<const std::string>& value,
      bool force);
  //! See documentation in superclass.
  kinetic::KineticStatus remove(const std::shared_ptr<const std::string>& key,
      const std::shared_ptr<const std::string>& version,
      bool force);
  //! See documentation in superclass.
  kinetic::KineticStatus range(
      const std::shared_ptr<const std::string>& start_keys,
      const std::shared_ptr<const std::string>& end_key,
      int maxRequested,
      std::unique_ptr< std::vector<std::string> >& keys);

  //--------------------------------------------------------------------------
  //! Constructor.
  //!
  //! @param connection_info host / port / key of target kinetic drive
  //--------------------------------------------------------------------------
  explicit KineticSingletonCluster(const kinetic::ConnectionOptions &connection_info);

  //--------------------------------------------------------------------------
  //! Destructor.
  //--------------------------------------------------------------------------
  ~KineticSingletonCluster();

private:
  //--------------------------------------------------------------------------
  //! Attempt to build a connection to a kinetic drive using the connection
  //! information that has been supplied to the constructor.
  //--------------------------------------------------------------------------
  kinetic::KineticStatus connect();

private:
  //! information required to build a connection
  kinetic::ConnectionOptions connection_info;

  //! connection to a kinetic target
  std::unique_ptr<kinetic::ThreadsafeBlockingKineticConnection> con;

  //! limits (primarily to key/value/version buffer sizes) for this cluster
  kinetic::Limits cluster_limits;

  //! current size + capacity of the cluster
  kinetic::Capacity cluster_size;

  //! time point when cluster_size was last verified to be correct
  std::chrono::system_clock::time_point size_timepoint;

  //! expiration time during which the cached size will be accepted as valid
  std::chrono::milliseconds size_expiration;
};



#endif	/* KINETICSPACESIMPLECPP_HH */

