#include "KineticDriveMap.hh"
#include <fstream>
#include <sstream>
#include <stdlib.h>
#include <glog/logging.h>


KineticDriveMap::KineticDriveMap()
{
    /* get file name */
    char* envstring = getenv("EOS_FST_KINETIC_JSON");
    if(!envstring){
        eos_err("EOS_FST_KINETIC_JSON not set.");
        return;
    }
    
    /* get file content */
    std::ifstream file(envstring);
    std::stringstream buffer;
    buffer << file.rdbuf(); 
    if(buffer.str().empty()){
        eos_err("Failed reading in json file: %s", envstring);
        return;
    }
    
    /* parse file */
    if(parseJson(buffer.str())){
        eos_err("Failed parsing json file: %s", envstring);
        drives.clear();
    }
}

KineticDriveMap::~KineticDriveMap()
{
    
}

int KineticDriveMap::getConnection(const std::string & drive_id, std::shared_ptr<kinetic::BlockingKineticConnectionInterface> & connection)
{
    if(!drives.count(drive_id)){
        eos_warning("Connection requested for nonexisting drive: %s",drive_id.c_str());
        return ENODEV;
    }     
    KineticDrive & d = drives.at(drive_id);    

    /* Avoid creating the same connection multiple times concurrently, but 
     * allow concurrent creation of different connections. Probably a bit 
     * over-the-top but can't hurt. */
    std::unique_lock<std::mutex> locker(mutex);
    while(blocked_id.count(drive_id))
        unblocked.wait(locker);
    
    if(!d.connection ){
        blocked_id.insert(drive_id);
        locker.unlock();
    
        kinetic::KineticConnectionFactory factory = kinetic::NewKineticConnectionFactory();
        kinetic::Status status = factory.NewThreadsafeBlockingConnection(d.connection_options, d.connection, 30);
         
        blocked_id.erase(drive_id);
        unblocked.notify_all();
        if(status.notOk()){
            eos_warning("Failed creating connection to drive: %s", drive_id.c_str());
            return ENXIO;
        }
    }
    connection = d.connection;
    return 0;
}

int KineticDriveMap::invalidateConnection(const std::string& drive_id)
{
    if(!drives.count(drive_id)){
        eos_warning("Connection invalidation requested for nonexisting drive: %s",drive_id.c_str());
        return ENODEV;
    }
    KineticDrive & d = drives.at(drive_id);
    
    std::unique_lock<std::mutex> locker(mutex);
    d.connection.reset();
    return 0;
}

int KineticDriveMap::getSize()
{    
    return drives.size();
}

int KineticDriveMap::getJsonEntry(json_object* parent, json_object*& entry, const char* name)
{
    entry = json_object_object_get(parent, name);
    if(!entry){
        eos_warning("Entry %s not found.", name);
        return EINVAL;
    }
    return 0;
}

int KineticDriveMap::parseJson(const std::string& jsonString)
{
    struct json_object *root = json_tokener_parse(jsonString.c_str());
    if(!root){
        eos_warning("File doesn't contain json root.");
        return EINVAL;
    }
      
    struct json_object *dlist = NULL;
    if(int err = getJsonEntry(root, dlist, "drives"))
        return err;
    int num_drives = json_object_array_length(dlist);
    
    struct json_object *d = NULL, *t = NULL;
    KineticDrive kd; 
    std::string id;

    for(int i=0; i<num_drives; i++){ 
        d = json_object_array_get_idx(dlist, i);
        
        /* We could go with wwn instead of serial number. Chosen SN since it is also unique
         * and is both shorter and contains no spaces (eos does not like spaces in the path name). */
        if(int err = getJsonEntry(d,t,"serialNumber")) 
            return err;
        id = json_object_get_string(t);
        
        if(int err = getJsonEntry(d,t,"user_id")) 
            return err;
        kd.connection_options.user_id = json_object_get_int(t);
        
        if(int err = getJsonEntry(d,t,"hmac_key")) 
            return err;
        kd.connection_options.hmac_key = json_object_get_string(t);
        
        if(int err = getJsonEntry(d,t,"host")) 
            return err;
        kd.connection_options.host = json_object_get_string(t);
        
         if(int err = getJsonEntry(d,t,"port")) 
            return err;
        kd.connection_options.port = json_object_get_int(t);
        
        kd.connection_options.use_ssl = false;
       
        drives.insert(std::make_pair(id, kd));
    }
    return 0;
}