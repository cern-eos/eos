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
    shareattr += std::to_string(cmd_id.getUnderlyingUInt64());
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
  // create path
  std::string procpath = GetEntry(vid, name);
  // create share entry
  int rc = CreateDir(procpath);
  if (rc) {
    return rc;
  }

  std::string shareattr;

  if (share_root.length()) {
    XrdOucErrInfo error;
    eos::IContainerMD::XAttrMap attrmap;
    eos::mgm::Acl acl (share_root.c_str(), error, vid, attrmap, true, true);
    if (!acl.CanShare()) {
      errno = EACCES;
      return -1;
    } else {
      errno = 0;
      // retrieve shareattr like pxis:<cid>
      shareattr = GetShareReference(share_root.c_str());
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
  eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__, __FILE__);
  int rc = gOFS->_attr_get(path.c_str(), error, root_vid, "", "sys.acl.share", value, false);
  if (rc) {
    return rc;
  }

  std::vector<std::string> rules;
  std::string delimiter = ",";
  std::string shareacl = value.c_str();

  eos::common::StringConversion::Tokenize(shareacl, rules, delimiter);
  std::string new_shareacl;
  bool add = true;
  for ( auto i : rules ) {
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
  }
  if (new_shareacl.length()) {
    new_shareacl.pop_back();
  }

  rc = gOFS->_attr_set(path.c_str(), error, root_vid, "", "sys.acl.share", new_shareacl.c_str(), false);
  return rc;
}

void
Share::AclList::Dump(std::string& out)
{
  for (auto it : mListing) {
    char format[1024];
    snprintf(format, sizeof(format),"uid:%06d %32s %s\n", it->get_uid(), it->get_name().c_str(), it->get_rule().c_str());
    out += format;
  }
}

/* ------------------------------------------------------------------------- */
Share::AclList
Share::Proc::List(eos::common::VirtualIdentity& vid, const std::string& name)
{
  Share::AclList acllist;
  std::string procpath = GetEntry(vid, name);

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
      XrdOucErrInfo error;
      if (!gOFS->_attr_get(entry.c_str(), error,
			   vid, "", "sys.share.acl", acl, true)) {
	std::string sacl = acl.c_str();
	acllist.Add(vid.uid, val, sacl);
      }
    }
  }
  return acllist;
}

/* ------------------------------------------------------------------------- */
int
Share::Proc::Delete(eos::common::VirtualIdentity& vid, const std::string& name)
{
  std::string procpath = GetEntry(vid, name);

  XrdOucErrInfo error;
  eos::common::VirtualIdentity root_vid = eos::common::VirtualIdentity::Root();
  return gOFS->_remdir(procpath.c_str(),
		      error,
		      root_vid,
		      "",
		      false);
}


/* ------------------------------------------------------------------------- */
int
Share::Proc::Modify()
{
  return 0;
}

/* ------------------------------------------------------------------------- */
std::shared_ptr<eos::mgm::Acl>
Share::getShareAcl(const eos::common::VirtualIdentity& vid, const std::string& s_id)
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
EOSMGMNAMESPACE_END
