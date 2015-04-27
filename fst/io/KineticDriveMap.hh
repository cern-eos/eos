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
    //! indicated by the EOS_FST_KINETIC_JSON environment variable. 
    //-------------------------------------------------------------------------- 
    explicit KineticDriveMap();
    
    //--------------------------------------------------------------------------
    //! Destructor 
    //-------------------------------------------------------------------------- 
    ~KineticDriveMap();
    
private:
    //--------------------------------------------------------------------------
    //! Build the drive map from the drive information stored in the supplied 
    //! json string. 
    //!
    //! @param jsonString contents of the json file stored at EOS_FST_KINETIC_JSON
    //! @return 0 if successful, EINVAL if drive description incomplete or incorrect json. 
    //--------------------------------------------------------------------------
    int parseJson(const std::string & jsonString);
    
    //--------------------------------------------------------------------------
    //! This should by all rights be a lambda function inside parseJson. But 
    //! to support gcc 4.4.7 (for SLC6) lambda functions are out the window. 
    //!
    //! @param parent the json object to search in 
    //! @param entry output
    //! @param name the name of the entry to search for 
    //! @return 0 if successful, EINVAL if name entry not available or incorrect json
    //--------------------------------------------------------------------------
    int getJsonEntry(struct json_object *parent, struct json_object *& entry, const char * name);
    
    
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

