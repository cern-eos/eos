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
Share::Proc::Create(eos::common::VirtualIdentity& vid, const std::string& name, const std::string& share_root)
{
  // check if exists
  if (!Get(vid, name)) {
    return EEXIST;
  }
  // create path
  std::string procpath = GetEntry(vid, name);
  // create share entry
  int rc = CreateDir(procpath);
  // add share root
  rc |= SetShareRoot(procpath, share_root);
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
  // check if exists
  if (Get(vid, name)) {
    return ENOENT;
  }

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
Share::Proc::Get(eos::common::VirtualIdentity& vid, const std::string& name)
{
  return 0;
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
