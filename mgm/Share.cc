// ------------------------------------------------------------------------------
// File: Share.cc
// Author: Andreas-Joachim Peters - CERN
// ------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2021 CERN/Switzerland                                  *
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


#include "mgm/Acl.hh"
#include "mgm/Namespace.hh"
#include "mgm/Share.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/XrdMgmOfsDirectory.hh"
#include "namespace/interface/IView.hh"
#include "namespace/Resolver.hh"
#include "common/table_formatter/TableFormatterBase.hh"
#include "common/Path.hh"
#include "common/Mapping.hh"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <memory>

EOSMGMNAMESPACE_BEGIN

/* ------------------------------------------------------------------------- */
Share::Share()
{
}

/* ------------------------------------------------------------------------- */
Share::Share(const char* prefix)
{
  mProc.Init(prefix);
}

Share::~Share()
{

}

/* ------------------------------------------------------------------------- */
Share::Acl::Acl()
{
}


/* ------------------------------------------------------------------------- */
Share::Acl::~Acl()
{
}

/* ------------------------------------------------------------------------- */
Share::Cache::Cache()
{
}

/* ------------------------------------------------------------------------- */
Share::Cache::~Cache()
{
}

Share::Proc::Proc()
{
  mValid = false;
}

Share::Proc::Proc(const char* prefix)
{
  if (Init(prefix)) {
    mValid = false;
  } else {
    mValid = true;
  }
}


Share::Proc::~Proc()
{
}


int
Share::Proc::Init(const char* prefix)
{
  // create proc entry
  mProcPrefix = prefix;
  mProcPrefix += "shares/";

  return CreateDir(mProcPrefix);
}

int
Share::Proc::CreateDir(const std::string& path)
{
  std::shared_ptr<eos::IContainerMD> eosmd;

  try {
    eosmd = gOFS->eosView->getContainer(path);
  } catch ( const eos::MDException& e ) {
    eosmd = nullptr;
  }

  if (!eosmd) {
    try {
      eosmd = gOFS->eosView->createContainer(path, true);
      eosmd->setMode(S_IFDIR | S_IRWXU | S_IROTH | S_IXOTH | S_IRGRP | S_IWGRP | S_IXGRP);
      gOFS->eosView->updateContainerStore(eosmd.get());
    } catch ( eos::MDException& e) {
      eos_static_crit("msg=\"failed to create proc directory\" path=\"%s\" errc=%d errmsg=\"\"", path.c_str(), e.getErrno(), e.getMessage().str().c_str());
      return -1;
    }
  } else {
    errno = EEXIST;
    return -1;
  }
  errno = 0;
  return 0;
}

/* ------------------------------------------------------------------------- */
int
Share::Proc::SetShareRoot(const std::string& path, const std::string& share_root)
{
  XrdOucErrInfo error;
  eos::common::VirtualIdentity root_vid = eos::common::VirtualIdentity::Root();

  return gOFS->_attr_set(path.c_str(),
		   error,
		   root_vid,
		   "",
		   "sys.share.root",
		   share_root.c_str(),
		   true);
}

/* ------------------------------------------------------------------------- */
int
Share::Proc::GetShareRoot(const std::string& path, std::string& share_root)
{
  XrdOucErrInfo error;
  eos::common::VirtualIdentity root_vid = eos::common::VirtualIdentity::Root();
  XrdOucString value;

  int rc = gOFS->_attr_get(path.c_str(),
			 error,
			 root_vid,
			 "",
			 "sys.share.root",
			 value,
			 true);
  if (!rc) {
    share_root = value.c_str();
  }
  return rc;
}


/* ------------------------------------------------------------------------- */
int
Share::Proc::SetShareAcl(const std::string& path, const std::string& share_acl)
{
  XrdOucErrInfo error;
  eos::common::VirtualIdentity root_vid = eos::common::VirtualIdentity::Root();

  return gOFS->_attr_set(path.c_str(),
		   error,
		   root_vid,
		   "",
		   "sys.share.acl",
		   share_acl.c_str(),
		   true);
}


std::string
Share::Proc::GetShareReference(const char* path)
{
  std::string shareattr;
  std::shared_ptr<eos::IContainerMD> dmd;
  eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__, __FILE__);
  try {
    dmd = gOFS->eosView->getContainer(path);
    eos::ContainerIdentifier cmd_id = dmd->getIdentifier();
    shareattr = "pxid:";
    shareattr += eos::common::FileId::Fid2Hex(cmd_id.getUnderlyingUInt64());
  } catch (eos::MDException& e) {
    errno = e.getErrno();
    return "";
  }
  return shareattr;
}
/* ------------------------------------------------------------------------- */
int
Share::Proc::Create(eos::common::VirtualIdentity& vid,
		    const std::string& name,
		    const std::string& share_root,
		    const std::string& share_acl
		    )
{
  errno = 0 ;

  std::string shareattr;
  // create path
  std::string procpath = GetEntry(vid.uid, name);

  // create share entry
  int rc = CreateDir(procpath);
  if (rc) {
    return rc;
  }

  if (share_root.length()) {
    bool is_owner = false;
    {
      std::shared_ptr<eos::IContainerMD> dh;
      eos::common::RWMutexWriteLock viewLock(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__, __FILE__);

      try {
	dh = gOFS->eosView->getContainer(share_root);
	eos::common::Path pPath(gOFS->eosView->getUri(dh.get()).c_str());
      } catch (eos::MDException& e) {
	dh.reset();
	errno = e.getErrno();
	eos_static_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
		e.getErrno(), e.getMessage().str().c_str());
      }

      if (!dh) {
	errno = ENOENT;
	return -1;
      }

      if (dh->getCUid() == vid.uid) {
	is_owner = true;
      }

      if (vid.sudoer) {
	is_owner = true;
      }
    }

    XrdOucErrInfo error;
    eos::IContainerMD::XAttrMap attrmap;
    eos::mgm::Acl acl (share_root.c_str(), error, vid, attrmap, true, true);
    if (!acl.CanShare() && !is_owner) {
      errno = EACCES;
      return -1;
    } else {
      errno = 0;
      // retrieve shareattr like pxid:<cid>
      shareattr = GetShareReference(procpath.c_str());
      if (errno) {
	return -1;
      }
    }
  }


  // add share root
  if (!share_root.empty()) {
    rc |= SetShareRoot(procpath, share_root);
  }
  if (!share_acl.empty()) {
    rc |= SetShareAcl(procpath, share_acl);
  }

  if (share_root.length()) {
    // apply the new share
    rc |= ModifyShare(vid, shareattr, share_root, false);
  }
  return rc;
}


/* ------------------------------------------------------------------------- */
int
Share::Proc::Share(eos::common::VirtualIdentity& vid, const std::string& name, const std::string& share_root, const std::string& share_acl)
{
  int rc = 0;
  errno = 0;
  std::string procpath = GetEntry(vid.uid, name);
  std::string current_share;
  std::string shareattr;

  rc = GetShareRoot(procpath, current_share);
  if (!rc) {
    errno = EAGAIN;
    eos_static_err("share is already shared")
    return -1;
  } else {
    // reset rc to be good!
    rc = 0;
  }

  if (!share_root.length()) {
    errno = EINVAL;
    eos_static_err("no share root specified")
    return -1;
  }

  bool is_owner = false;
  {
    std::shared_ptr<eos::IContainerMD> dh;
    eos::common::RWMutexWriteLock viewLock(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__, __FILE__);

    try {
      dh = gOFS->eosView->getContainer(share_root);
      eos::common::Path pPath(gOFS->eosView->getUri(dh.get()).c_str());
    } catch (eos::MDException& e) {
      dh.reset();
      errno = e.getErrno();
      eos_static_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
		       e.getErrno(), e.getMessage().str().c_str());
    }

    if (!dh) {
	errno = ENOENT;
	eos_static_err("path'%s' does not exist errno=%d", share_root.c_str(), errno);
	return -1;
    }

    if (dh->getCUid() == vid.uid) {
	is_owner = true;
    }

    if (vid.sudoer) {
      is_owner = true;
    }
  }

  XrdOucErrInfo error;
  eos::IContainerMD::XAttrMap attrmap;
  eos::mgm::Acl acl (share_root.c_str(), error, vid, attrmap, true, true);
  if (!acl.CanShare() && !is_owner) {
    errno = EACCES;
    eos_static_err("no access");
    return -1;
  } else {
    errno = 0;
    // retrieve shareattr like pxid:<cid>
    shareattr = GetShareReference(procpath.c_str());
    if (errno) {
      eos_static_err("no share reference errno=%d", errno);
      return -1;
    }
  }

  rc |= SetShareRoot(procpath, share_root);
  rc |= SetShareAcl(procpath, share_acl);
  rc |= ModifyShare(vid, shareattr, share_root, false);
  return rc;
}

/* ------------------------------------------------------------------------- */
int
Share::Proc::UnShare(eos::common::VirtualIdentity& vid, const std::string& name, const std::string& share_root)
{
  return Delete(vid, name, true);
}

int
Share::Proc::Access(eos::common::VirtualIdentity& vid, const std::string& name, std::string& out, const std::string& user, const std::string& group, bool json)
{
  uid_t uid = atoi(user.c_str());
  gid_t gid = atoi(group.c_str());
  eos::common::VirtualIdentity access_vid;

  // construct the accessing vid
  if (uid && gid) {
    access_vid = eos::common::Mapping::Someone(uid,gid);
  } else {
    access_vid = eos::common::Mapping::Someone(user);
  }

  auto acl = getShareAclByName(vid,access_vid, name);
  if (!acl) {
    errno = ENOENT;
    return -1;
  }

  if (json) {
    eos::mgm::Acl::accessmap_t map;
    // return a JSON document
    acl->Out(false,&map);
    Json::Value json;
    for (auto it=map.begin(); it!=map.end(); ++it) {
      json["access"][it->first] = json[it->second];
    }
    std::stringstream r;
    r << json;
    out = r.str().c_str();
  } else {
    out = acl->Out(false);
  }
  return 0;
}

int
Share::Proc::Modify(eos::common::VirtualIdentity& vid, const std::string& name, const std::string& share_acl)
{
  int rc = 0;
  errno = 0;
  std::string procpath = GetEntry(vid.uid, name);
  std::string current_share;
  std::string shareattr;

  rc = GetShareRoot(procpath, current_share);
  if (rc) {
    eos_static_err("unable to get share")
    return -1;
  }

  rc |= SetShareAcl(procpath, share_acl);
  return rc;
}

/* ------------------------------------------------------------------------- */
int
Share::Proc::ModifyShare(const eos::common::VirtualIdentity& vid, std::string shareattr, const std::string& share_root, bool remove)
{
  // recursively add this share
  XrdMgmOfsDirectory subtree;
  eos::common::VirtualIdentity root_vid = eos::common::VirtualIdentity::Root();
  int rc = subtree._open(share_root.c_str(),
			 root_vid,
			 "ls.skip.files=1");
  if (!rc) {
    // modify the sharing on this directory
    rc |= ModifyShareAttr(share_root, shareattr,  remove);
    const char* item;
    while ( ( item = subtree.nextEntry() ) ) {
      std::string child = share_root;
      std::string sitem = item;
      if ( (sitem  == ".") || (sitem == "..") ) {
	continue;
      }
      child += "/";
      child += item;

      // propagate to children
      rc |= ModifyShare(vid,shareattr, child, remove);
    }
    subtree.close();
  }

  return rc;
}

int
Share::Proc::ModifyShareAttr(const std::string& path, const std::string& shareattr, bool remove)
{
  eos::common::VirtualIdentity root_vid = eos::common::VirtualIdentity::Root();
  XrdOucErrInfo error;
  XrdOucString value;
  int rc = gOFS->_attr_get(path.c_str(), error, root_vid, "", "sys.acl.share", value, true);

  eos_static_info("path='%s' shareattr='%s' acl='%s' remove=%d",
		  path.c_str(),
		  shareattr.c_str(),
		  value.c_str(),
		  remove);

  std::vector<std::string> rules;
  std::string delimiter = ",";
  std::string shareacl = value.c_str();

  eos::common::StringConversion::Tokenize(shareacl, rules, delimiter);
  std::string new_shareacl;
  bool add = true;
  for ( auto i : rules ) {
    eos_static_info("'%s' vs '%s'", i.c_str(), shareattr.c_str());
    if (remove) {
      add = false;
      if ( i == shareattr ) {
	continue;
      } else {
	new_shareacl += i;
	new_shareacl += ",";
      }
    } else {
      if ( i == shareattr ) {
	add = false;
      } else {
	new_shareacl += i;
	new_shareacl += ",";
      }
    }
  }
  if (add) {
    new_shareacl += shareattr;
    new_shareacl += ",";
  }
  if (new_shareacl.length()) {
    new_shareacl.pop_back();
  }

  eos_static_info("path='%s' new-share-acl='%s'", path.c_str(), new_shareacl.c_str());
  if (new_shareacl.empty()) {
    rc = gOFS->_attr_rem(path.c_str(), error, root_vid, "", "sys.acl.share", true);
  } else {
    rc = gOFS->_attr_set(path.c_str(), error, root_vid, "", "sys.acl.share", new_shareacl.c_str(), true);
  }
  return rc;
}

void
Share::AclList::Dump(std::string& out, bool monitoring, bool json, eos::mgm::Share::shareinfo_t* info)
{
  std::string format_s = !monitoring ? "s" : "os";
  TableFormatterBase table_all;

  if (json) {
    Json::Value json;
    for (auto it : mListing) {
      Json::Value jsonshares;
      jsonshares["uid"] = (long long) it->get_uid();
      jsonshares["name"] = it->get_plain_name();
      jsonshares["rule"] = it->get_rule();
      jsonshares["root"] = it->get_plain_root();
      jsonshares["shared"] = (long long)(mReshares[it->get_plain_root()]);
      json["share"].append(jsonshares);

      if (info) {
	std::map<std::string,std::string> s;
	s["uid"] = std::to_string((long long) it->get_uid());
	s["name"] = it->get_name();
	s["rule"] = it->get_rule();
	s["root"] = it->get_root();
	s["nshared"] = std::to_string((long long)(mReshares[it->get_plain_root()]));
	info->push_back(s);
      }
    }
    std::stringstream r;
    r << json;
    out = r.str().c_str();
  } else {
    if (!monitoring) {
      table_all.SetHeader({
	  std::make_tuple("uid", 8, format_s),
	    std::make_tuple("name", 32, format_s),
	    std::make_tuple("rule", 48, format_s),
	    std::make_tuple("root", 48, format_s),
	    std::make_tuple("shared", 8, format_s)
	    });
  } else {
      table_all.SetHeader({
	  std::make_tuple("uid", 0, format_s),
	    std::make_tuple("name", 0, format_s),
	    std::make_tuple("rule", 0, format_s),
	    std::make_tuple("root", 0, format_s),
	    std::make_tuple("shared", 0, format_s)
          });
    }

    for (auto it : mListing) {
      TableData table_data;
      table_data.emplace_back();
      table_data.back().push_back(TableCell(std::to_string((long long )it->get_uid()), format_s));
      table_data.back().push_back(TableCell(it->get_name(), format_s));
      table_data.back().push_back(TableCell(it->get_rule(), format_s));
      table_data.back().push_back(TableCell(it->get_root(), format_s));
      table_data.back().push_back(TableCell(std::to_string((long long)(mReshares[it->get_plain_root()])), format_s));
      table_all.AddRows(table_data);
    }
    out = table_all.GenerateTable(HEADER);
  }
}

/* ------------------------------------------------------------------------- */
Share::AclList
Share::Proc::List(eos::common::VirtualIdentity& vid, const std::string& name)
{
  Share::AclList acllist;
  Share::reshare_t reshares;

  std::set<uid_t> users;
  users.insert(vid.uid);

  if ( (vid.uid == 0) ||
       (vid.uid == 11) ) {
    users = GetShareUsers();
  }

  for ( auto it : users ) {
    std::string procpath = GetEntry(it, name);

    XrdMgmOfsDirectory directory;
    int listrc = directory.open(procpath.c_str(),
				vid,
				"");
    if (!listrc) {
      const char* val;

      while ((val = directory.nextEntry())) {
	if (std::string(val) == ".") continue;
	if (std::string(val) == "..") continue;
	std::string entry = procpath + val;
	XrdOucString acl;
	XrdOucString root;
	XrdOucErrInfo error;
	eos::common::VirtualIdentity root_vid = eos::common::VirtualIdentity::Root();

	if (!gOFS->_attr_get(entry.c_str(), error,
			     root_vid, "", "sys.share.acl", acl, true)) {
	  gOFS->_attr_get(entry.c_str(), error,
			  root_vid, "", "sys.share.root", root, true);
	  std::string sacl = acl.c_str();
	  std::string sroot = root.c_str();
	  acllist.Add(it, val, sacl ,sroot);
	  reshares[sroot]++;
	} else {
	  std::string none="-";
	  acllist.Add(it, val, none, none);
	}
      }
    }
  }
  acllist.SetReshare(reshares);
  return acllist;
}

/* ------------------------------------------------------------------------- */
int
Share::Proc::Delete(eos::common::VirtualIdentity& vid, const std::string& name, bool keep_share)
{
  std::string procpath = GetEntry(vid.uid, name);
  XrdOucString acl;
  XrdOucString share_root;
  std::string sshare_root;
  XrdOucErrInfo error;
  int rc=0;
  std::string shareattr;

  eos::common::VirtualIdentity root_vid = eos::common::VirtualIdentity::Root();

  XrdOucErrInfo attrerror;
  {
    if (!gOFS->_attr_get(procpath.c_str(), attrerror,
			 root_vid, "", "sys.share.acl", acl, true)) {
      gOFS->_attr_get(procpath.c_str(), attrerror,
		      root_vid, "", "sys.share.root", share_root, true);
    } else {
      struct stat buf;
      // check if there is an incomplete entry
      if (gOFS->_stat(procpath.c_str(), &buf, attrerror, vid)) {
	return -1;
      }
      // let's continue to wipe this entry
    }
  }

  if (share_root.length()) {
    sshare_root = share_root.c_str();

    bool is_owner = false;
    {
      std::shared_ptr<eos::IContainerMD> dh;
      eos::common::RWMutexWriteLock viewLock(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__, __FILE__);

      try {
	dh = gOFS->eosView->getContainer(share_root.c_str());
	eos::common::Path pPath(gOFS->eosView->getUri(dh.get()).c_str());
      } catch (eos::MDException& e) {
	dh.reset();
	errno = e.getErrno();
	eos_static_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
		e.getErrno(), e.getMessage().str().c_str());
      }

      if (!dh) {
	errno = ENOENT;
	return -1;
      }

      if (dh->getCUid() == vid.uid) {
	is_owner = true;
      }

      if (vid.sudoer) {
	is_owner = true;
      }
    }

    XrdOucErrInfo error;
    eos::IContainerMD::XAttrMap attrmap;
    eos::mgm::Acl acl (share_root.c_str(), error, vid, attrmap, true, true);
    if (!acl.CanShare() && !is_owner) {
      errno = EACCES;
      return -1;
    } else {
      errno = 0;
      // retrieve shareattr like pxid:<cid>
      shareattr = GetShareReference(procpath.c_str());
      if (errno) {
	return -1;
      }
    }

    // apply the new share
    rc = ModifyShare(vid, shareattr, sshare_root, true);
    if (rc) {
      return rc;
    }
  }

  if (keep_share) {
    // only remove the share root and acl
    rc |= gOFS->_attr_rem(procpath.c_str(), error, root_vid, "", "sys.share.root", true);
    rc |= gOFS->_attr_rem(procpath.c_str(), error, root_vid, "", "sys.share.acl", true);
    return rc;
  } else {
    return gOFS->_remdir(procpath.c_str(),
			 error,
			 root_vid,
			 "",
			 false);
  }
}

/* ------------------------------------------------------------------------- */
int
Share::Proc::Modify()
{
  return 0;
}

std::set<uid_t>
Share::Proc::GetShareUsers()
{
  std::set<uid_t> users;

  eos::common::VirtualIdentity root_vid = eos::common::VirtualIdentity::Root();
  XrdMgmOfsDirectory directory;
  int listrc = directory.open(mProcPrefix.c_str(),
			      root_vid,
			      "");
  if (!listrc) {
    const char* val;

    while ((val = directory.nextEntry())) {
      if (std::string(val) == ".") continue;
      if (std::string(val) == "..") continue;
      std::string entry =val;
      if (entry.substr(0,4) == "uid:") {
	users.insert(std::stoul(entry.substr(4)));
      }
    }
  }
  return users;
}

/* ------------------------------------------------------------------------- */
std::shared_ptr<eos::mgm::Acl>
Share::getShareAclById(const eos::common::VirtualIdentity& vid, const std::string& s_id)
{
  XrdOucString s = s_id.c_str();
  s.replace('p', 'f', 0, 1);

  uint64_t id = Resolver::retrieveFileIdentifier(s).getUnderlyingUInt64();

  eos_static_debug("id=%s:%llx\n", s.c_str(),id);

  if (!id) {
    return std::make_shared<eos::mgm::Acl>();
  }
  std::string share_path;
  std::string error_msg;
  int retc = IProcCommand::GetPathFromCid(share_path, id, error_msg, false);
  if (retc) {
    return std::make_shared<eos::mgm::Acl>();
  }
  XrdOucErrInfo error;
  eos::IContainerMD::XAttrMap attrmap;

  return std::make_shared<eos::mgm::Acl>(share_path.c_str(), error, vid, attrmap, false, true);
}

/* ------------------------------------------------------------------------- */
std::shared_ptr<eos::mgm::Acl>
Share::Proc::getShareAclByName(const eos::common::VirtualIdentity& vid, const eos::common::VirtualIdentity& access_vid, const std::string& name)
{
  std::string procpath = GetEntry(vid.uid, name);
  std::string shareattr;
  XrdOucErrInfo error;
  eos::IContainerMD::XAttrMap attrmap;
  eos::common::VirtualIdentity root_vid = eos::common::VirtualIdentity::Root();
  XrdOucString acl;

  if (!gOFS->_attr_get(procpath.c_str(), error,
		       root_vid, "", "sys.share.acl", acl, true)) {
    attrmap["sys.acl"] = acl.c_str();
    return std::make_shared<eos::mgm::Acl>((const char*)0, error, access_vid, attrmap, false, false);
  } else {
    return std::make_shared<eos::mgm::Acl>();
  }



}
EOSMGMNAMESPACE_END
