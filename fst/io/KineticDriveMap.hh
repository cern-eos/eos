#ifndef KINETICDRIVEMAP_HH
#define	KINETICDRIVEMAP_HH

#include <kinetic/kinetic.h>
#include <unordered_map>
#include <unordered_set>
#include <string>
#include <condition_variable>
#include <mutex>
#include <json-c/json.h>
#include "common/Logging.hh"

class KineticDriveMap : public eos::common::LogId {

private:
    struct KineticDrive{
        kinetic::ConnectionOptions connection_options; 
        std::shared_ptr<kinetic::ThreadsafeBlockingKineticConnection> connection;     
    };
    
    std::unordered_set<std::string> blocked_wwn;
    std::condition_variable unblocked;
    std::mutex mutex;
    
    std::unordered_map<std::string, KineticDrive> drives;
    
private:
    int getJsonEntry(struct json_object *parent, struct json_object *& entry, const char * name);
    int parseJson(const std::string & path);
    
public:  
    int getConnection(const std::string & drive_wwn, std::shared_ptr<kinetic::BlockingKineticConnectionInterface> & connection); 
    int invalidateConnection(const std::string & drive_wwn);
    int getSize();
    
    /* Constructor requires a json file listing kinetic drives. The path can 
     * either be supplied directly or stored in the EOS_FST_KINETIC_JSON
     * environmental variable. */
    explicit KineticDriveMap(std::string path = "");
    ~KineticDriveMap();
};


#endif	/* KINETICDRIVEMAP_HH */

