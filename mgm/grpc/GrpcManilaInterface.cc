// ----------------------------------------------------------------------
// File: GrpcManilaInterface.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

/*----------------------------------------------------------------------------*/
#include "GrpcManilaInterface.hh"
/*----------------------------------------------------------------------------*/
#include "common/LayoutId.hh"
#include "mgm/Acl.hh"
#include "mgm/Quota.hh"
#include "namespace/Prefetcher.hh"
#include "namespace/MDException.hh"
#include "namespace/interface/IFileMD.hh"
#include "namespace/interface/IContainerMD.hh"
#include "namespace/interface/ContainerIterators.hh"

#include "mgm/XrdMgmOfs.hh"

/*----------------------------------------------------------------------------*/


EOSMGMNAMESPACE_BEGIN

#ifdef EOS_GRPC

grpc::Status 
GrpcManilaInterface::Process(
			     eos::common::VirtualIdentity& vid,
			     eos::rpc::ManilaResponse* reply,
			     const eos::rpc::ManilaRequest* request
			     )
{
  if (!vid.sudoer) {
    // block every one who is not a sudoer
    reply->set_code(EPERM);
    reply->set_msg("Ask an admin to map your auth key to a sudo'er account - permission denied");
    return grpc::Status::OK;
  }

  // Verify/Load the manila configuration
  eos::IContainerMD::XAttrMap config;
  if (LoadManilaConfig(vid, reply, config)) {
    return grpc::Status::OK;
  }

  switch (request->request_type()) {
  case eos::rpc::MANILA_REQUEST_TYPE::CREATE_SHARE :
    CreateShare(reply, request, config);
    break;
  case eos::rpc::MANILA_REQUEST_TYPE::DELETE_SHARE :
    DeleteShare(reply, request, config);
    break;
  case eos::rpc::MANILA_REQUEST_TYPE::EXTEND_SHARE :
    ExtendShare(reply, request, config);
    break;
  case eos::rpc::MANILA_REQUEST_TYPE::SHRINK_SHARE :
    ShrinkShare(reply, request, config);
    break;
  case eos::rpc::MANILA_REQUEST_TYPE::MANAGE_EXISTING :
    ManageShare(reply, request, config);
    break;
  case eos::rpc::MANILA_REQUEST_TYPE::UNMANAGE:
    UnmanageShare(reply, request, config);
    break;
  case eos::rpc::MANILA_REQUEST_TYPE::GET_CAPACITIES :
    GetCapacityShare(reply, request, config);
    break;
  default:
    reply->set_code(EINVAL);
    reply->set_msg("Invalid request");
  }
  return grpc::Status::OK;
}


int 
GrpcManilaInterface::LoadManilaConfig(
				      eos::common::VirtualIdentity& vid,
				      eos::rpc::ManilaResponse* reply, 
				      eos::IContainerMD::XAttrMap& config
				      ) {
  std::string openstackdir = gOFS->MgmProcPath.c_str();
  openstackdir += "/openstack";
  XrdOucErrInfo error;

  if (gOFS->_attr_ls(openstackdir.c_str(), error, vid, "", config)) {
    if (error.getErrInfo() == ENOENT) {
      reply->set_code(-ENODATA);
      std::string msg = "Incomplete Configuration: ask the administrator to create and configure ";;
      msg += openstackdir;
      reply->set_msg(msg);
    } else {
      reply->set_code(-error.getErrInfo());
      reply->set_msg(error.getErrText());
    }
    return -1;
  }
  
  if (!config.count("manila.prefix")) {
    reply->set_code(-ENODEV);
    std::string msg = "Incomplete Configuration: ask the administrator to define the extended attribute 'manila.prefix' on ";
    msg += openstackdir;
    reply->set_msg(msg);
    return -1;
  }

  if (config["manila.prefix"].back() != '/') {
    config["manila.prefix"] += "/";
  }


  reply->set_code(1);
  return 0;
}

int 
GrpcManilaInterface::LoadShareConfig(const std::string share_path,
				     eos::IContainerMD::XAttrMap& managed)
{
  XrdOucErrInfo error;
  eos::common::VirtualIdentity vid = eos::common::VirtualIdentity::Root();
  if (gOFS->_attr_ls(share_path.c_str(), error, vid, "", managed)) {
    return -1;
  } 
  return 0;
}


bool
GrpcManilaInterface::ValidateManilaDirectoryTree(const std::string& share_directory, eos::rpc::ManilaResponse* reply)
{
  struct stat buf;
  XrdOucErrInfo error;
  eos::common::VirtualIdentity vid = eos::common::VirtualIdentity::Root();

  if (share_directory == "/") {
    std::string msg = "Incomplete Configuration: the share directory '";
    msg += share_directory;
    msg += "' points to root - ask the administrator to configure it";
    reply->set_msg(msg);
    reply->set_code(-EFAULT);
    return false;
  }
  
  // check existance of the prefix share_directory
  if (gOFS->_stat(share_directory.c_str(), 
		  &buf, 
		  error, 
		  vid)) {
    if (errno == ENOENT) {
      std::string msg = "Incomplete Configuration: the share directory '";
      msg += share_directory;
      msg += "' does not exist - ask the administrator to create it";
      reply->set_msg(msg);
      reply->set_code(-errno);
    } else  {
      std::string msg = "Incomplete Configuration: the share directory '";
      msg += share_directory;
      msg += "' can not be accessed - ask the administrator to fix it";
      reply->set_msg(msg);
      reply->set_code(-errno);
    }
    return false;
  }
  
  return true;
}

bool 
GrpcManilaInterface::ValidateManilaRequest(const eos::rpc::ManilaRequest* request, eos::rpc::ManilaResponse* reply)
{
  // check presence of all required request attributes
  bool ok = true;
  std::string msg;
  if ( request->creator().empty() ) {
    msg = "Invalid argument: creator field is empty";
    ok = false;
  }
  if ( request->protocol().empty() ) {
    msg = "Invalid argument: protocol field is empty";
    ok = false;
  } 
  if ( request->share_name().empty() ) {
    msg = "Invalid argument: share name is empty";
    ok = false;
  }
  if ( request->share_id().empty() ) {
    msg = "Invalid argument: share id is empty";
    ok = false;
  } 
  if (! request->quota()) {
    msg = "Invalid argument: quota is 0";
    ok = false;
  }
  if (!ok) {
    reply->set_code(-EINVAL);
    reply->set_msg(msg);
  }
  return ok;
}


std::string 
GrpcManilaInterface::BuildShareDirectory(const eos::rpc::ManilaRequest* request, 
					 eos::IContainerMD::XAttrMap& config)
{
  std::string share_directory = config["manila.prefix"];

  // by default we build shares as .../a/ashare/
  if ( (!config.count("manila.letter.prefix")) ||
       (config["manila.letter.prefix"] == "1") ) {
    share_directory += request->creator().substr(0,1);
    share_directory += "/";
  }
  share_directory += request->creator();
  share_directory += "/";
  share_directory += request->share_name();
  return share_directory;
}


int
GrpcManilaInterface::HandleShare(eos::rpc::ManilaResponse* reply,
				 const eos::rpc::ManilaRequest* request,
				 eos::IContainerMD::XAttrMap& config, 
				 bool create, 
				 bool quota)
{
  // pre-checks
  if (!ValidateManilaRequest(request, reply)) {
    return 0;
  }

  eos::common::VirtualIdentity vid = eos::common::VirtualIdentity::Root();

  XrdOucErrInfo error;
  std::string share_directory = config["manila.prefix"];

  if (!ValidateManilaDirectoryTree(share_directory, reply)) {
    return 0;
  }

  // check validity of creator
  if ( (request->creator().find("..") != std::string::npos) ||
       (request->creator().find("/") != std::string::npos) ) {
    std::string msg = "Illegal Creator Mame: ";
    msg += request->creator();
    msg += " => '/' and '..' are not allowed!";
    reply->set_msg(msg);
    reply->set_code(-errno);
    return 0;
  }

  // check validity of share name
  if ( (request->share_name().find("..") != std::string::npos) ||
       (request->share_name().find("/") != std::string::npos) ) {
    std::string msg = "Illegal Share Name: ";
    msg += request->share_name();
    msg += " =>  '/' and '..' are not allowed!";
    reply->set_msg(msg);
    reply->set_code(-errno);
    return 0;
  }

  share_directory = BuildShareDirectory(request, config);

  eos_static_notice("%s manila share name='%s' path='%s' for creator='%s' quota=%lu GB",
		    create? "Creating" : "Managing",
		    request->share_name().c_str(), 
		    share_directory.c_str(),
		    request->creator().c_str(),
		    request->quota());

  if (create) {
    // create path
    ProcCommand cmd;
    XrdOucString info = "mgm.cmd=mkdir&mgm.option=p&mgm.path=";
    info += share_directory.c_str();
    cmd.open("/proc/user", info.c_str(), vid, &error);
    cmd.close();
    int rc = cmd.GetRetc();
    
    if (rc) {
      std::string msg = "Creation Failed: ";
      msg += cmd.GetStdErr();
      reply->set_code( (rc>0) ? -rc:rc);
      reply->set_msg(msg);
      return 0;
    }
  } else {
    struct stat buf;
    // stat path
    // check existance of the prefix share_directory
    if (gOFS->_stat(share_directory.c_str(), 
		    &buf, 
		    error, 
		    vid)) {
      std::string msg = "Invalid share: unable to manage the given share directory '";
      msg += share_directory;
      msg += "'";
      reply->set_code(-errno);
      reply->set_msg(msg);
      return 0;
    }
  }

  if (create) {
    // change owner
    ProcCommand cmd;
    std::string info = "mgm.cmd=chown&mgm.chown.owner=";
    info += request->creator().c_str();
    info += "&mgm.path=";
    info += share_directory.c_str();

    cmd.open("/proc/user", info.c_str(), vid, &error);
    cmd.close();
    int rc = cmd.GetRetc();

    if (rc) {
      std::string msg = "Chown Failed: ";
      msg += cmd.GetStdErr();
      reply->set_code( (rc>0) ? -rc:rc);
      reply->set_msg(msg);
      return 0;
    }
  }

  if (quota && !create) {
    eos::IContainerMD::XAttrMap managed;
    // check if this is manila managed
    LoadShareConfig(share_directory, managed);
    if (managed["manila.managed"]!="true") {
      std::string msg = "Share is not managed: ";
      msg += share_directory;
      reply->set_code( -ENODEV );
      reply->set_msg(msg);
      return 0;
    }
  }

  if (quota) {
    if (config.count("manila.max_quota")) {
      // check quota restrictions
      uint64_t max_quota = strtoull(config["manila.max_quota"].c_str(), 0, 10);
      if ((uint64_t) (request->quota()) > max_quota) {
	std::string msg = "Quota request exceeded: the maximum quota allowed is ";
	msg += max_quota;
	msg += " GB";
	reply->set_code( -EINVAL );
	reply->set_msg(msg);
	return 0;
      }
    }
  }

  if (create || quota) {
    // set quota
    ProcCommand cmd;
    std::string info = "mgm.cmd=quota&mgm.subcmd=set&mgm.quota.maxbytes=";
    info += std::to_string(request->quota() * 1000ll*1000ll*1000ll).c_str();
    if ( 
	(!config.count("manila.project"))  ||
	 (config["manila.project"] != "1") 
	 ) {
      // user quota
      info += "&mgm.quota.uid=";
      info += request->creator();
      info += "&mgm.quota.space=";
      info += config["manila.prefix"];
    } else {
      // project quota
      info += "&mgm.quota.gid=99";
      info += "&mgm.quota.space=";
      info += share_directory;
    }

    cmd.open("/proc/user", info.c_str(), vid, &error);
    cmd.close();
    int rc = cmd.GetRetc();

    if (rc) {
      std::string msg = "Quota configuration failed: ";
      msg += cmd.GetStdErr();
      reply->set_code( (rc>0) ? -rc:rc);
      reply->set_msg(msg);
      return 0;
    }
  }

  std::string acl;
  {
    // prepare acls
    if (!request->egroup().empty()) {
      acl += "egroup:";
      acl += request->egroup();
      acl += ":";
      if (!config.count("manila.egroup.acl")) {
	acl += "rwx";
      } else {
	acl += config["manila.egroup.acl"];
      }
    }
    if (!request->admin_egroup().empty()) {
      if (!request->egroup().empty()) {
	acl += ",";
      }
      acl += "egroup:";
      acl += request->admin_egroup();
      acl += ":";
      if (!config.count("manila.admin_egroup.acl")) {
	acl += "rwxq";
      } else {
	acl += config["manila.admin_egroup.acl"];
      }
    }
  }

  {
    // store share id and group id and description
    int rc = 
      gOFS->_attr_set(share_directory.c_str(), error, vid, "", "manila.id", request->share_id().c_str()) ||
      gOFS->_attr_set(share_directory.c_str(), error, vid, "", "manila.group_id", request->share_group_id().c_str()) ||
      gOFS->_attr_set(share_directory.c_str(), error, vid, "", "manila.description", request->description().c_str()) ||
      gOFS->_attr_set(share_directory.c_str(), error, vid, "", "manila.protocol", request->protocol().c_str()) ||
      gOFS->_attr_set(share_directory.c_str(), error, vid, "", "manila.location", request->share_location().c_str()) ||
      gOFS->_attr_set(share_directory.c_str(), error, vid, "", "manila.name", request->share_name().c_str()) ||
      gOFS->_attr_set(share_directory.c_str(), error, vid, "", "manila.creator", request->creator().c_str()) ||
      gOFS->_attr_set(share_directory.c_str(), error, vid, "", "manila.managed", "true") ||
      ( create ? gOFS->_attr_set(share_directory.c_str(), error, vid, "", "sys.acl", acl.c_str()) : 0) ||
      (  ((config.count("manila.owner.auth")) &&
	  (config["manila.owner.auth"] == "1")) ? gOFS->_attr_set(share_directory.c_str(), error, vid, "", "sys.owner.auth=","*") : 0 );
    
    if (rc) {
      std::string msg = "Unable to store manila/acl attributes: ";
      msg += error.getErrText();
      reply->set_code( -errno );
      reply->set_msg(msg);
      return 0;
    }
  }

  reply->set_new_share_quota(request->quota());
  reply->set_new_share_path(share_directory);
  reply->set_code(1);

  return 0;
}


int
GrpcManilaInterface::CreateShare(eos::rpc::ManilaResponse* reply,
				 const eos::rpc::ManilaRequest* request,
				 eos::IContainerMD::XAttrMap& config) {

  return HandleShare(reply, request, config, true, true);
}


int 
GrpcManilaInterface::DeleteShare(eos::rpc::ManilaResponse* reply,
				 const eos::rpc::ManilaRequest* request,
				 eos::IContainerMD::XAttrMap& config)
{
  eos::common::VirtualIdentity vid = eos::common::VirtualIdentity::Root();
  XrdOucErrInfo error;

  std::string share_directory = config["manila.prefix"];

  if (!ValidateManilaDirectoryTree(share_directory, reply)) {
    return 0;
  }

  // check validity of creator
  if ( (request->creator().find("..") != std::string::npos) ||
       (request->creator().find("/") != std::string::npos) ) {
    std::string msg = "Illegal Creator Mame: ";
    msg += request->creator();
    msg += " => '/' and '..' are not allowed!";
    reply->set_msg(msg);
    reply->set_code(-errno);
    return 0;
  }

  // check validity of share name
  if ( (request->share_name().find("..") != std::string::npos) ||
       (request->share_name().find("/") != std::string::npos) ) {
    std::string msg = "Illegal Share Name: ";
    msg += request->share_name();
    msg += " =>  '/' and '..' are not allowed!";
    reply->set_msg(msg);
    reply->set_code(-errno);
    return 0;
  }

  share_directory = BuildShareDirectory(request, config);

  if (!config.count("manila.deletion") && 
      config["manila.deletion"] != "1") {
    std::string msg = "Deletion is forbidden: ask the admin to configure 'manila.deletion=1'";
    reply->set_code(-EPERM);
    reply->set_msg(msg);
    return 0;
  }

  eos_static_notice("%s manila share name='%s' path='%s' for creator='%s' quota=%lu GB",
		    "Deleting", 
		    request->share_name().c_str(), 
		    share_directory.c_str(),
		    request->creator().c_str(),
		    request->quota());
  
  ProcCommand cmd;
  XrdOucString info = "mgm.cmd=rm&mgm.option=r&mgm.path=";
  info += share_directory.c_str();
  cmd.open("/proc/user", info.c_str(), vid, &error);
  cmd.close();
  int rc = cmd.GetRetc();
  
  if (rc) {
    std::string msg = "Deletion Failed: ";
    msg += cmd.GetStdErr();
    reply->set_code( (rc>0) ? -rc:rc);
    reply->set_msg(msg);
    return 0;
  }

  return 0;
}

int 
GrpcManilaInterface::ExtendShare(eos::rpc::ManilaResponse* reply,
				 const eos::rpc::ManilaRequest* request,
				 eos::IContainerMD::XAttrMap& config)
{
  return HandleShare(reply, request, config, false, true);
}

int 
GrpcManilaInterface::ShrinkShare(eos::rpc::ManilaResponse* reply,
				 const eos::rpc::ManilaRequest* request,
				 eos::IContainerMD::XAttrMap& config)
{
  return HandleShare(reply, request, config, false, true);
}

int 
GrpcManilaInterface::ManageShare(eos::rpc::ManilaResponse* reply,
				 const eos::rpc::ManilaRequest* request,
				 eos::IContainerMD::XAttrMap& config)
{
  return HandleShare(reply, request, config, false, false);
}

int 
GrpcManilaInterface::UnmanageShare(eos::rpc::ManilaResponse* reply,
				   const eos::rpc::ManilaRequest* request,
				   eos::IContainerMD::XAttrMap& config)
{
  eos::IContainerMD::XAttrMap managed;

  eos::common::VirtualIdentity vid = eos::common::VirtualIdentity::Root();
  XrdOucErrInfo error;

  std::string share_directory = config["manila.prefix"];

  if (!ValidateManilaDirectoryTree(share_directory, reply)) {
    return 0;
  }
  // check validity of creator
  if ( (request->creator().find("..") != std::string::npos) ||
       (request->creator().find("/") != std::string::npos) ) {
    std::string msg = "Illegal Creator Mame: ";
    msg += request->creator();
    msg += " => '/' and '..' are not allowed!";
    reply->set_msg(msg);
    reply->set_code(-errno);
    return 0;
  }

  // check validity of share name
  if ( (request->share_name().find("..") != std::string::npos) ||
       (request->share_name().find("/") != std::string::npos) ) {
    std::string msg = "Illegal Share Name: ";
    msg += request->share_name();
    msg += " =>  '/' and '..' are not allowed!";
    reply->set_msg(msg);
    reply->set_code(-errno);
    return 0;
  }

  share_directory = BuildShareDirectory(request, config);

  LoadShareConfig(share_directory, managed);
  
  if (managed["manila.managed"]=="true") {
    int rc = 
      gOFS->_attr_set(share_directory.c_str(), error, vid, "", "manila.managed", "false");
    
    if (rc) {
      std::string msg = "Unable to store manila managed attribute: ";
      msg += error.getErrText();
      reply->set_code( -errno );
      reply->set_msg(msg);
    }
  } else {
    std::string msg = "The referenced share is not managed by manila";
    reply->set_code( -EINVAL );
    reply->set_msg(msg);
  }

  return 0;
}

int 
GrpcManilaInterface::GetCapacityShare(eos::rpc::ManilaResponse* reply,
				      const eos::rpc::ManilaRequest* request,
				      eos::IContainerMD::XAttrMap& config)
{
  eos::common::VirtualIdentity vid = eos::common::Mapping::Someone(request->creator());
  long long  max_bytes, free_bytes, max_files,free_files;
  max_bytes = free_bytes = max_files = free_files = 0;

  std::string share_directory = config["manila.prefix"];

  // check validity of creator
  if ( (request->creator().find("..") != std::string::npos) ||
       (request->creator().find("/") != std::string::npos) ) {
    std::string msg = "Illegal Creator Mame: ";
    msg += request->creator();
    msg += " => '/' and '..' are not allowed!";
    reply->set_msg(msg);
    reply->set_code(-errno);
    return 0;
  }

  // check validity of share name
  if ( (request->share_name().find("..") != std::string::npos) ||
       (request->share_name().find("/") != std::string::npos) ) {
    std::string msg = "Illegal Share Name: ";
    msg += request->share_name();
    msg += " =>  '/' and '..' are not allowed!";
    reply->set_msg(msg);
    reply->set_code(-errno);
    return 0;
  }

  share_directory += request->creator().substr(0,1);
  share_directory += "/";
  share_directory += request->creator();
  share_directory += "/";
  share_directory += request->share_name();


  Quota::GetIndividualQuota(vid, 
			    share_directory, 
			    max_bytes, 
			    free_bytes, 
			    max_files, 
			    free_files, 
			    true);

  if (!max_bytes && config.count("manila.max_quota")) {
    max_bytes = config("manila.max_quota");
  }

  reply->set_total_used( (max_bytes - free_bytes)/ (1000ll*1000ll*1000ll) );
  reply->set_total_capacity(max_bytes / (1000ll*1000ll*1000ll));
  reply->set_code(1);

  return 0;
}


#endif

EOSMGMNAMESPACE_END
