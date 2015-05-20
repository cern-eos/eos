#include <stdlib.h>
#include <fstream>
#include <sstream>
#include "KineticClusterMap.hh"
#include "KineticSingletonCluster.hh"


/* Read file located at path into string buffer and return it. */
static std::string readfile(const char* path)
{
  std::ifstream file(path);
  std::stringstream buffer;
  buffer << file.rdbuf();
  return buffer.str();
}
enum filetype{location,security};

KineticClusterMap::KineticClusterMap()
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
  if(parseJson(location_data, filetype::location) ||
    parseJson(security_data, filetype::security) ){
    eos_err("Error during json parsing.");
    map.clear();
  }
}

KineticClusterMap::~KineticClusterMap()
{
}

int KineticClusterMap::getCluster(const std::string & id,
          std::shared_ptr<KineticClusterInterface> & cluster)
{
  std::unique_lock<std::mutex> locker(mutex);

  if(!map.count(id)){
      eos_warning("Connection requested for nonexisting ID: %s",id.c_str());
      return ENODEV;
  }
  KineticClusterInfo & ki = map.at(id);
  if(!ki.cluster)
    ki.cluster.reset(new KineticSingletonCluster(ki.connection_options));
  cluster = ki.cluster;
  return 0;
}

int KineticClusterMap::getSize()
{
  return map.size();
}

int KineticClusterMap::getJsonEntry(json_object* parent, json_object*& entry,
        const char* name)
{
  entry = json_object_object_get(parent, name);
  if(!entry){
    eos_warning("Entry %s not found.", name);
    return EINVAL;
  }
  return 0;
}

int KineticClusterMap::parseDriveInfo(struct json_object * drive)
{
  struct json_object *tmp = NULL;
  KineticClusterInfo ki;
  /* We could go with wwn instead of serial number. Chosen SN since it is also
   * unique and is both shorter and contains no spaces (eos does not like spaces
   * in the path name). */
  if(int err = getJsonEntry(drive,tmp,"serialNumber"))
    return err;
  string id = json_object_get_string(tmp);

  if(int err = getJsonEntry(drive,tmp,"inet4"))
    return err;
  tmp = json_object_array_get_idx(tmp, 0);
  ki.connection_options.host = json_object_get_string(tmp);

  if(int err = getJsonEntry(drive,tmp,"port"))
    return err;
  ki.connection_options.port = json_object_get_int(tmp);

  ki.connection_options.use_ssl = false;

  map.insert(std::make_pair(id, ki));
  return 0;
}

int KineticClusterMap::parseDriveSecurity(struct json_object * drive)
{
  struct json_object *tmp = NULL;
  /* We could go with wwn instead of serial number. Chosen SN since it is also
   * unique and is both shorter and contains no spaces (eos does not like spaces
   * in the path name). */
  if(int err = getJsonEntry(drive,tmp,"serialNumber"))
      return err;
  string id = json_object_get_string(tmp);

  /* Require that drive info has been scanned already.*/
  if(!map.count(id))
      return ENODEV;

  KineticClusterInfo & ki = map.at(id);

  if(int err = getJsonEntry(drive,tmp,"userId"))
    return err;
  ki.connection_options.user_id = json_object_get_int(tmp);

  if(int err = getJsonEntry(drive,tmp,"key"))
    return err;
  ki.connection_options.hmac_key = json_object_get_string(tmp);

  return 0;
}


int KineticClusterMap::parseJson(const std::string& filedata, const int type)
{
  struct json_object *root = json_tokener_parse(filedata.c_str());
  if(!root){
      eos_warning("File doesn't contain json root.");
      return EINVAL;
  }

  struct json_object *d = NULL;
  struct json_object *dlist = NULL;
  int err = getJsonEntry(root, dlist,
          type == filetype::location ? "location" : "security");
  if( err) return err;

  int num_drives = json_object_array_length(dlist);
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