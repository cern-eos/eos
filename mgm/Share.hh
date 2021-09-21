//------------------------------------------------------------------------------
//! @file Share.hh
//------------------------------------------------------------------------------

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


#pragma once
#include "mgm/Namespace.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Acl.hh"
#include "common/VirtualIdentity.hh"
#include <string>

EOSMGMNAMESPACE_BEGIN
class Share {
public:
  Share();
  Share(const char* prefix);
  ~Share();

  bool Valid() {
    return mProc.Valid();
  }


  class Acl {
  public:
    Acl();
    Acl(uid_t _uid, const std::string& _name, const std::string& _rule, const std::string& _root) : uid(_uid), name(_name), rule(_rule), root(_root) {}
    virtual ~Acl();
    uid_t get_uid() { return uid; }
    std::string get_name() { return std::string("\"") + name + std::string("\""); }
    std::string get_plain_name() { return name; }
    std::string get_rule() { return rule; }
    std::string get_root() { return std::string("\"") + root + std::string("\""); }
    std::string get_plain_root() { return root; }
  private:
    uid_t uid;
    std::string name;
    std::string rule;
    std::string root;
  };

  typedef std::map<std::string, size_t> reshare_t;
  typedef std::vector<std::map<std::string,std::string>> shareinfo_t;

  class AclList {
  public:
    AclList(){}
    virtual ~AclList(){}


    void Add(uid_t uid, const std::string& name, const std::string& acl, const std::string& root) {
      mListing.push_back( std::make_shared<Acl>(uid,name,acl, root) );
    }
    void Dump(std::string& out, bool monitoring=false, bool json=false, shareinfo_t* info = nullptr);
    size_t Size() { return mListing.size(); }
    void SetReshare(const reshare_t& reshares) { mReshares = reshares;
      for (auto it : reshares ) {
	mReshares[it.first]=it.second;
      }
    }

  private:
    std::vector<std::shared_ptr<Acl>> mListing;
    reshare_t mReshares;
  };

  class Cache {
  public:
    Cache();
    virtual ~Cache();
  };


  class Proc {
  public:
    Proc();
    Proc(const char* prefix);
    virtual ~Proc();

    int Init(const char* prefix);
    int Create(eos::common::VirtualIdentity& vid, const std::string& name, const std::string& share_root, const std::string& share_acl);

    int Share(eos::common::VirtualIdentity& vid, const std::string& name, const std::string& share_root, const std::string& share_acl);

    int UnShare(eos::common::VirtualIdentity& vid, const std::string& name, const std::string& share_root);

    int Access(eos::common::VirtualIdentity& vid, const std::string& name, std::string& out, const std::string& user, const std::string& group, bool json=false);

    int Modify(eos::common::VirtualIdentity& vid, const std::string& name, const std::string& share_acl);

    int ModifyShare(const eos::common::VirtualIdentity& vid, std::string shareattr, const std::string& share_root, bool remove=false);
    int ModifyShareAttr(const std::string& path, const std::string& shareattr, bool remove=false);

    std::set<uid_t> GetShareUsers();

    std::string GetEntry(uid_t uid, const std::string& name) {
      return mProcPrefix + "uid:" + std::to_string(uid) + std::string("/") + name;
    }

    int Delete(eos::common::VirtualIdentity& vid, const std::string& name, bool keep_share=false);
    AclList List(eos::common::VirtualIdentity& vid, const std::string& name);
    int Delete();
    int Modify();
    bool Valid() { return mValid; }
  private:
    int CreateDir(const std::string& path);
    int SetShareRoot(const std::string& path, const std::string& share_root);
    int GetShareRoot(const std::string& path, std::string& share_root);
    int SetShareAcl(const std::string& path, const std::string& share_acl);
    std::string GetShareReference(const char* path);
    std::shared_ptr<eos::mgm::Acl> getShareAclByName(const eos::common::VirtualIdentity& vid, const eos::common::VirtualIdentity& access_vid, const std::string& name);
    std::string mProcPrefix;
    bool mValid;
  };

  // return an ACL object for a given share
  static std::shared_ptr<eos::mgm::Acl> getShareAclById(const eos::common::VirtualIdentity& vid, const std::string& s_id);
  // return Proc object
  Proc& getProc() { return mProc; }
private:
  Proc mProc;
};


EOSMGMNAMESPACE_END
