/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2022 CERN/Switzerland                                  *
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

/************************************************************************
 * @file  XattrLock.hh                                                  *
 * @brief Class to handle locks in extended attributes                  *
 ************************************************************************/

#pragma once

#include <set>
#include <string>
#include <iostream>

#include "mgm/Namespace.hh"
#include "common/Constants.hh"
#include "namespace/Prefetcher.hh"

EOSMGMNAMESPACE_BEGIN

struct XattrLock {
public:
  XattrLock() :
    valid(false), isfuseopen(false), isshared(false), expires(0)
  {}

  XattrLock(eos::IFileMD::XAttrMap& attr) :
    xattr(attr), valid(false), isfuseopen(false),
    isshared(false), expires(0)

  {
    auto l = xattr.find(eos::common::EOS_APP_LOCK_ATTR);

    if (l != xattr.end()) {
      Parse(l->second.c_str());
    }

    // check fuse commit state
    std::string fs = attr["sys.fusex.state"];

    if (!fs.empty()) {
      isfuseopen = fs.back() != '|';
    }
  }
  virtual ~XattrLock() {}

  void Parse(const char* l)
  {
    valid = false;
    std::map<std::string, std::string> kvmap;

    if (eos::common::StringConversion::GetKeyValueMap(l,
        kvmap)) {
      if (!kvmap.count("expires") ||
          !kvmap.count("type") ||
          !kvmap.count("owner")) {
        valid = false;
      } else {
        expires = atoi(kvmap["expires"].c_str());
        isshared = (kvmap["type"] == "shared");
        owner = (kvmap["owner"]);
        valid = true;
      }
    }
  }

  bool foreignLock(eos::common::VirtualIdentity& vid, bool isrw)
  {
    time_t now = time(NULL);

    if (!valid) {
      return false;
    }

    ssize_t lifetime = expires - now;

    if (isfuseopen) {
      // file is in open state
      return false;
    }

    if (lifetime > 0) {
      // check the lock is still within its lifetime
      if (lifetime > 604800) {
        // that is an illegal attribute and we ignore it
        return false;
      }

      if (!isrw && isshared) {
        // if this is read access on a shared lock, we let it pass
        return false;
      }

      // we can have full match or a wildcard ownership for the user or app name, both with wildcard does not make sense
      std::string lockowner   = std::string(vid.uid_string.c_str()) + std::string(":")
                                + std::string(vid.app.c_str());
      std::string lockownerwn = std::string("*") + std::string(":") + std::string(
                                  vid.app.c_str());
      std::string lockownerwa = std::string(vid.uid_string.c_str()) + std::string(":")
                                + std::string("*");

      if ((lockowner != owner) &&
          (lockownerwn != owner) &&
          (lockownerwa != owner)) {
        // this is not us - this is the only case we prevent access
        return true;
      }
    }

    return false;
  }

  bool Lock(const char* path, bool shrd, time_t lifetime,
            eos::common::VirtualIdentity& vid, bool userwildcard, bool appwildcard)
  {
    errno = 0;
    XrdOucErrInfo error;
    std::string value;
    eos::Prefetcher::prefetchFileMDAndWait(gOFS->eosView, path);
    eos::IFileMDPtr fmd = nullptr;
    eos::MDLocking::FileWriteLockPtr fdLock;

    try {
      fmd = gOFS->eosView->getFile(path);
      fdLock = eos::MDLocking::writeLock(fmd.get());
    } catch (eos::MDException& e) {
      errno = e.getErrno();
      eos_static_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),
                       e.getMessage().str().c_str());
      return false;
    }

    if (!gOFS->_attr_get(path, error, vid, "", eos::common::EOS_APP_LOCK_ATTR,
                         value)) {
      // parse the lock
      Parse(value.c_str());

      if (foreignLock(vid, true)) {
        errno = EBUSY;
        // we cannot lock this
        return false;
      }
    }

    if (lifetime > (604800)) {
      // application locks no longer than a week
      errno = EINVAL;
      return false;
    }

    if (userwildcard && appwildcard) {
      // cannot have both with wildcard
      errno = EINVAL;
      return false;
    }

    // define expires
    expires = time(NULL) + lifetime;
    // define the owner
    owner = (userwildcard ? std::string("*") : std::string(vid.uid_string.c_str()))
            + std::string(":") + (appwildcard ? std::string("*") : std::string(
                                    vid.app.c_str()));
    // define shared
    isshared = shrd;

    // store the lock attribute
    if (gOFS->_attr_set(path, error, vid, "", eos::common::EOS_APP_LOCK_ATTR,
                        Value().c_str())) {
      return false;
    }

    return true;
  }

  bool Unlock(const char* path, eos::common::VirtualIdentity& vid)
  {
    errno = 0;
    XrdOucErrInfo error;
    std::string value;
    eos::Prefetcher::prefetchFileMDAndWait(gOFS->eosView, path);
    eos::IFileMDPtr fmd = nullptr;
    eos::MDLocking::FileWriteLockPtr fdLock;

    try {
      fmd = gOFS->eosView->getFile(path);
      fdLock = eos::MDLocking::writeLock(fmd.get());
    } catch (eos::MDException& e) {
      errno = e.getErrno();
      eos_static_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),
                       e.getMessage().str().c_str());
      return false;
    }

    if (!gOFS->_attr_get(path, error, vid, "", eos::common::EOS_APP_LOCK_ATTR,
                         value)) {
      // parse the lock
      Parse(value.c_str());

      if (foreignLock(vid, true)) {
        errno = EBUSY;
        // we cannot unlock this
        return false;
      }
    }

    // remove the lock attribute
    if (gOFS->_attr_rem(path, error, vid, "", eos::common::EOS_APP_LOCK_ATTR)) {
      return false;
    }

    return true;
  }

  std::string Dump()
  {
    std::ostringstream ss;
    ss << "valid:" << valid << " expires:" << expires << " shared:" << isshared <<
       std::endl;
    return ss.str();
  }

  std::string Value()
  {
    std::ostringstream ss;
    ss << "expires:" << expires;

    if (isshared) {
      ss << ",type:" << "shared";
    } else {
      ss << ",type:" << "exclusive";
    }

    ss << ",owner:" << owner;
    return ss.str();
  }

private:
  eos::IFileMD::XAttrMap xattr;
  bool valid;
  bool isfuseopen;
  bool isshared;
  time_t expires;
  std::string owner;
};

EOSMGMNAMESPACE_END
