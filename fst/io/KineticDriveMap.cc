#include "KineticDriveMap.hh"
#include <fstream>
#include <sstream>
#include <stdlib.h>


/* Read file located at path into string buffer and return it. */
static std::string readfile(const char* path)
{
    std::ifstream file(path);
    std::stringstream buffer;
    buffer << file.rdbuf(); 
    return buffer.str();
}
enum filetype{location,security};

KineticDriveMap::KineticDriveMap()
{
    /* get file names */
    const char* location = getenv("KINETIC_DRIVE_LOCATION");
    const char* security = getenv("KINETIC_DRIVE_SECURITY");
    if(!location || !security){
        eos_err("KINETIC_DRIVE_LOCATION / KINETIC_DRIVE_SECURITY not set.");
        return;
    }     
        
    /* get file contents */
    std::string location_data = readfile(location);
    std::string security_data = readfile(security);
    if(location_data.empty() || security_data.empty()){
        eos_err("KINETIC_DRIVE_LOCATION / KINETIC_DRIVE_SECURITY not correct.");
        return;
    }
    
    /* parse files */
    if( parseJson(location_data, filetype::location) ||
        parseJson(security_data, filetype::security) ){
        eos_err("Error during json parsing.");
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

int KineticDriveMap::parseDriveInfo(struct json_object * drive)
{  
    struct json_object *tmp = NULL;
    KineticDrive kd; 
    
    /* We could go with wwn instead of serial number. Chosen SN since it is also unique
     * and is both shorter and contains no spaces (eos does not like spaces in the path name). */
    if(int err = getJsonEntry(drive,tmp,"serialNumber")) 
        return err;
    string id = json_object_get_string(tmp);

    if(int err = getJsonEntry(drive,tmp,"inet4")) 
        return err;
    tmp = json_object_array_get_idx(tmp, 0);
    kd.connection_options.host = json_object_get_string(tmp);

    if(int err = getJsonEntry(drive,tmp,"port")) 
       return err;
    kd.connection_options.port = json_object_get_int(tmp);

    kd.connection_options.use_ssl = false;

    drives.insert(std::make_pair(id, kd));
    return 0;
}

int KineticDriveMap::parseDriveSecurity(struct json_object * drive)
{
    struct json_object *tmp = NULL;    
    
    /* We could go with wwn instead of serial number. Chosen SN since it is also unique
     * and is both shorter and contains no spaces (eos does not like spaces in the path name). */
    if(int err = getJsonEntry(drive,tmp,"serialNumber")) 
        return err;
    string id = json_object_get_string(tmp);
    
    /* Require that drive info has been scanned already.*/
    if(!drives.count(id))
        return ENODEV; 
         
    KineticDrive & kd = drives.at(id);    
    
    if(int err = getJsonEntry(drive,tmp,"userId")) 
        return err;
    kd.connection_options.user_id = json_object_get_int(tmp);

    if(int err = getJsonEntry(drive,tmp,"key")) 
       return err;
    kd.connection_options.hmac_key = json_object_get_string(tmp);
    
    return 0;   
}


int KineticDriveMap::parseJson(const std::string& filedata, const int type)
{
    struct json_object *root = json_tokener_parse(filedata.c_str());
    if(!root){
        eos_warning("File doesn't contain json root.");
        return EINVAL;
    }
      
    struct json_object *dlist = NULL;
    if(int err = getJsonEntry(root, dlist, type == filetype::location ? "location" : "security"))
        return err;
    int num_drives = json_object_array_length(dlist);
    
    struct json_object *d = NULL;
    int err = 0; 
    
    for(int i=0; i<num_drives; i++){ 
        d = json_object_array_get_idx(dlist, i);
        if(type == filetype::location)
            err = parseDriveInfo(d);
        else if (type == filetype::security)
            err = parseDriveSecurity(d);
        if(err) return err; 
    }
    return 0;
}