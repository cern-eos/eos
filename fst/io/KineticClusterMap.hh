//------------------------------------------------------------------------------
//! @file KineticDriveMap.hh
//! @author Paul Hermann Lensing
//! @brief Supplying a fst wide cluster map. Threadsafe.
//------------------------------------------------------------------------------
#ifndef KINETICDRIVEMAP_HH
#define	KINETICDRIVEMAP_HH

/*----------------------------------------------------------------------------*/
#include <condition_variable>
#include <unordered_map>
#include <unordered_set>
#include <string>
#include <mutex>
#include <json-c/json.h>
#include "KineticClusterInterface.hh"
#include "common/Logging.hh"
/*----------------------------------------------------------------------------*/

//------------------------------------------------------------------------------
//! Supplying a fst wide cluster map. Threadsafe.
//------------------------------------------------------------------------------
class KineticClusterMap : public eos::common::LogId {

public:
  //--------------------------------------------------------------------------
  //! Obtain an input-output class for the supplied identifier.
  //!
  //! @param id the unique identifier for the cluster
  //! @param cluster contains the cluster on success
  //! @return 0 if successful, ENODEV / ENXIO otherwise
  //--------------------------------------------------------------------------
  int getCluster(const std::string & id,
          std::shared_ptr<KineticClusterInterface> & cluster);

  //--------------------------------------------------------------------------
  //! Obtain the number of entries in the map.
  //!
  //! @return the number of entries in the map
  //--------------------------------------------------------------------------
  int getSize();

  //--------------------------------------------------------------------------
  //! Constructor.
  //! Requires a json file listing kinetic drives to be stored at the location
  //! indicated by the KINETIC_DRIVE_LOCATION and KINETIC_DRIVE_SECURITY
  //! environment variables
  //--------------------------------------------------------------------------
  explicit KineticClusterMap();

  //--------------------------------------------------------------------------
  //! Destructor
  //--------------------------------------------------------------------------
  ~KineticClusterMap();

private:
  //--------------------------------------------------------------------------
  //! Utility function to grab a specific json entry.
  //!
  //! @param parent the json object to search in
  //! @param entry output
  //! @param name the name of the entry to search for
  //! @return 0 if successful, EINVAL if name entry not available
  //--------------------------------------------------------------------------
  int getJsonEntry(struct json_object *parent, struct json_object *& entry,
          const char * name);

  //--------------------------------------------------------------------------
  //! Creates a KineticDrive object in the drive map containing the ip and port
  //!
  //! @param drive json root of one drive description containing location data
  //! @return 0 if successful, EINVAL if name entry not available
  int parseDriveInfo(struct json_object *drive);

  //--------------------------------------------------------------------------
  //! Adds security attributes to drive description
  //!
  //! @param drive json root of one drive description containing security data
  //! @return 0 if successful, EINVAL if drive description incomplete or
  //          incorrect json,  ENODEV if drive id does not exist in map.
  int parseDriveSecurity(struct json_object *drive);

  //--------------------------------------------------------------------------
  //! Parse the supplied json file.
  //!
  //! @param filedata contents of a json file
  //! @param filetype specifies if filedata contains security or location
  //!        information.
  //! @return 0 if successful, EINVAL if drive description incomplete or
  //!         incorrect json.
  //--------------------------------------------------------------------------
  int parseJson(const std::string & filedata, int filetype);

private:
  //--------------------------------------------------------------------------
  //! This structure is only suited to store single-drive info for the
  //! SingletonCluster. Will have to be adjusted to allow for other cluster
  //! types.
  //--------------------------------------------------------------------------
  struct KineticClusterInfo{
      //! everything required to create the cluster
      kinetic::ConnectionOptions connection_options;

      //! the cluster object, shared among IO objects of a fst
      std::shared_ptr<KineticClusterInterface> cluster;
  };

  //! the cluster map id <-> cluster info
  std::unordered_map<std::string, KineticClusterInfo> map;

  //! concurrency control
  std::mutex mutex;
};


#endif	/* KINETICDRIVEMAP_HH */

