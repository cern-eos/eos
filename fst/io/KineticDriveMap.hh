//------------------------------------------------------------------------------
//! @file KineticDriveMap.hh
//! @author Paul Hermann Lensing
//! @brief Supplying a fst wide connection map. Threadsafe. 
//------------------------------------------------------------------------------
#ifndef KINETICDRIVEMAP_HH
#define	KINETICDRIVEMAP_HH

/*----------------------------------------------------------------------------*/
#include <condition_variable>
#include <unordered_map>
#include <unordered_set>
#include <string>
#include <mutex>
#include <kinetic/kinetic.h>
#include <json-c/json.h>
#include "common/Logging.hh"
/*----------------------------------------------------------------------------*/

//------------------------------------------------------------------------------
//! Supplying a fst wide connection map. Threadsafe. 
//------------------------------------------------------------------------------
class KineticDriveMap : public eos::common::LogId {

public:  
    //--------------------------------------------------------------------------
    //! Obtain a connection to the supplied drive identifier. 
    //!
    //! @param drive_id the unique identifier for the drive
    //! @param connection contains the connection on success 
    //! @return 0 if successful, ENODEV / ENXIO otherwise
    //--------------------------------------------------------------------------
    int getConnection(const std::string & drive_id, std::shared_ptr<kinetic::BlockingKineticConnectionInterface> & connection); 
    
    //--------------------------------------------------------------------------
    //! Invalidate a connection to the supplied drive identifier. The connection
    //! pointer is simply reset so that the connection will be build from 
    //! scratch the next time getConnection is called with the identifier. 
    //!
    //! @param drive_id the unique identifier for the drive
    //! @return 0 if successful, ENODEV if supplied drive id is invalid.
    //--------------------------------------------------------------------------
    int invalidateConnection(const std::string & drive_id);
    
    //--------------------------------------------------------------------------
    //! Obtain the number of drives in the drive map. A positive return value 
    //! shows that json parsing was concluded successful. 
    //!
    //! @return the number of drive in the map
    //--------------------------------------------------------------------------
    int getSize();
    
    //--------------------------------------------------------------------------
    //! Constructor. 
    //! Requires a json file listing kinetic drives to be stored at the location
    //! indicated by the KINETIC_DRIVE_LOCATION and KINETIC_DRIVE_SECURITY environment 
    //! variables 
    //-------------------------------------------------------------------------- 
    explicit KineticDriveMap();
    
    //--------------------------------------------------------------------------
    //! Destructor 
    //-------------------------------------------------------------------------- 
    ~KineticDriveMap();
    
private:
    //--------------------------------------------------------------------------
    //! Utility function to grab a specific json entry. 
    //! 
    //! @param parent the json object to search in 
    //! @param entry output
    //! @param name the name of the entry to search for 
    //! @return 0 if successful, EINVAL if name entry not available or incorrect json
    //--------------------------------------------------------------------------
    int getJsonEntry(struct json_object *parent, struct json_object *& entry, const char * name);
    
    //--------------------------------------------------------------------------
    //! Creates a KineticDrive object in the drive map containing the ip and port 
    //!
    //! @param drive json root of single drive description containing lcoation data
    //! @return 0 if successful, EINVAL if drive description incomplete or incorrect json. 
    int parseDriveInfo(struct json_object *drive); 
    
    //--------------------------------------------------------------------------
    //! Adds security attributes to drive description
    //!
    //! @param drive json root of single drive description containing security data
    //! @return 0 if successful, EINVAL if drive description incomplete or incorrect json, 
    //!           ENODEV if drive id does not exist in map . 
    int parseDriveSecurity(struct json_object *drive);

    //--------------------------------------------------------------------------
    //! Parse the supplied json file. 
    //!
    //! @param filedata contents of a json file
    //! @param filetype specifies if filedata contains security or location information.
    //! @return 0 if successful, EINVAL if drive description incomplete or incorrect json. 
    //--------------------------------------------------------------------------
    int parseJson(const std::string & filedata, int filetype);
    
private:
    struct KineticDrive{
        //! everything required to build a connection to the drive 
        kinetic::ConnectionOptions connection_options; 
        
        //! the connection, shared among IO objects of a fst 
        std::shared_ptr<kinetic::ThreadsafeBlockingKineticConnection> connection;
    };
    
    //! the drive map
    std::unordered_map<std::string, KineticDrive> drives;
    
    //! list of currently blocked drive ids (drives to where a connection is currently created)
    std::unordered_set<std::string> blocked_id; 
    
    //! gets triggered every time a connection is created 
    std::condition_variable unblocked;
    
    //! concurrency control for drive map 
    std::mutex mutex; ///< concurrency control 
};


#endif	/* KINETICDRIVEMAP_HH */

