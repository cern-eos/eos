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
    Acl(uid_t _uid, const std::string& _name, const std::string& _rule) : uid(_uid), name(_name), rule(_rule) {}
    virtual ~Acl();
    uid_t get_uid() { return uid; }
    std::string get_name() { return name; }
    std::string get_rule() { return rule; }
  private:
    uid_t uid;
    std::string name;
    std::string rule;
  };

  class AclList {
  public:
    AclList(){}
    virtual ~AclList(){}
    void Add(uid_t uid, const std::string& name, const std::string& acl) {
      mListing.push_back( std::make_shared<Acl>(uid,name,acl) );
    }
    void Dump(std::string& out);
    size_t Size() { return mListing.size(); }
  private:
    std::vector<std::shared_ptr<Acl>> mListing;
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
    int Create(eos::common::VirtualIdentity& vid, const std::string& name, const std::string& share_root);
    int Get(eos::common::VirtualIdentity& vid, const std::string& name);
    std::string GetEntry(eos::common::VirtualIdentity& vid, const std::string& name) {
      return mProcPrefix + "uid:" + std::to_string(vid.uid) + std::string("/") + name;
    }

    int Delete(eos::common::VirtualIdentity& vid, const std::string& name);
    AclList List(eos::common::VirtualIdentity& vid, const std::string& name);
    int Delete();
    int Modify();
    bool Valid() { return mValid; }
  private:
    int CreateDir(const std::string& path);
    int SetShareRoot(const std::string& path, const std::string& share_root);
    std::string mProcPrefix;
    bool mValid;
  };

  // return an ACL object for a given share_acl entry
  static std::shared_ptr<eos::mgm::Acl> getShareAcl(const eos::common::VirtualIdentity& vid, const std::string& s_id);

  // return Proc object
  Proc& getProc() { return mProc; }
private:
  Proc mProc;
};


EOSMGMNAMESPACE_END
