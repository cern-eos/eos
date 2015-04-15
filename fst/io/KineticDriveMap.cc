#include "KineticDriveMap.hh"
#include <fstream>
#include <sstream>
#include <stdlib.h>
#include <glog/logging.h>


KineticDriveMap::KineticDriveMap(std::string path)
{
    google::InitGoogleLogging("");
    if(path.empty()){
        char* envstring = getenv ("EOS_FST_KINETIC_JSON");
        path = std::string(envstring);    
    }
    std::ifstream file(path);
    std::stringstream buffer;
    buffer << file.rdbuf(); 
    if(buffer.str().empty())
        eos_warning("Failed reading in json file.");
    else
    if(parseJson(buffer.str()))
        drives.clear();
}

KineticDriveMap::~KineticDriveMap()
{
    
}

int KineticDriveMap::getConnection(const std::string & drive_wwn, std::shared_ptr<kinetic::ThreadsafeBlockingKineticConnection> & connection)
{
    if(!drives.count(drive_wwn)){
        eos_warning("Connection requested for nonexisting wwn: %s",drive_wwn.c_str());
        return ENODEV;
    }
     
    KineticDrive d = drives.at(drive_wwn);    
    
    // Avoid trying to create the same connection multiple times concurrently. Probably a bit over-the-top,
    // but can't hurt.
    std::unique_lock<std::mutex> locker(mutex);
    
    while(blocked_wwn.count(drive_wwn))
        unblocked.wait(locker);
    
    if(!d.connection ){
        blocked_wwn.insert(drive_wwn);
        locker.unlock();
    
        kinetic::KineticConnectionFactory factory = kinetic::NewKineticConnectionFactory();
        kinetic::Status status = factory.NewThreadsafeBlockingConnection(d.connection_options, d.connection, 30);
         
        blocked_wwn.erase(drive_wwn);
        unblocked.notify_all();
        if(status.notOk()){
            eos_warning("Failed creating connection to drive %s",drive_wwn.c_str());
            return ENXIO;
        }
    }
    connection = d.connection;
    return 0;
}

int KineticDriveMap::getSize(){
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
    std::string wwn;

    for(int i=0; i<num_drives; i++){ 
        d = json_object_array_get_idx(dlist, i);
        
        if(int err = getJsonEntry(d,t,"wwn")) 
            return err;
        wwn = json_object_get_string(t);
        
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
       
        drives.insert(std::make_pair(wwn, kd));
    }
    return 0;
}