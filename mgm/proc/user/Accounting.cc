// ----------------------------------------------------------------------
// File: proc/user/Accounting.cc
// Author: Jozsef Makai - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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

#include "common/StringTokenizer.hh"
#include "common/ExpiryCache.hh"
#include "common/Logging.hh"
#include "mgm/proc/ProcInterface.hh"
#include "mgm/proc/ProcCommand.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Quota.hh"
#include "json/json.h"

EOSMGMNAMESPACE_BEGIN

int
ProcCommand::Accounting()
{
  static eos::common::ExpiryCache<std::string> accountingCache(
    std::chrono::seconds(600));
  static const auto generateAccountingJson = [](
  eos::common::VirtualIdentity & vid) {
    static const auto processAccountingAttribute = [](
    std::pair<std::string, std::string> attr, Json::Value & storageShare) {
      static auto accountingAttrPrefix = "sys.accounting";

      if (attr.first.find(accountingAttrPrefix) == 0) {
        auto objectPath =
          eos::common::StringTokenizer::split<std::vector<std::string>>(attr.first, '.');
        Json::Value* keyEndpointPtr = nullptr;

        // Creating a new json object following the path in this case
        if (objectPath.size() > 3) {
          // we keep a pointer to the previous json object along the path
          auto* prevObject = &(storageShare[objectPath[2]]);

          for (size_t i = 3; i < objectPath.size(); ++i) {
            try {
              auto number = std::stoul(objectPath[i]);
              prevObject = &((*prevObject)[Json::ArrayIndex(number)]);
            } catch (std::invalid_argument& err) {
              prevObject = &((*prevObject)[objectPath[i]]);
            }
          }

          // finally set the value of the attribute
          keyEndpointPtr = prevObject;
        } else {
          keyEndpointPtr = &(storageShare[objectPath[objectPath.size() - 1]]);
        }

        // This value is interpreted as a list of elements separated by comma
        if (attr.second.find(',') != std::string::npos) {
          auto attrs = eos::common::StringTokenizer::split<std::list<std::string>>
                       (attr.second, ',');

          for (auto && attrValue : attrs) {
            (*keyEndpointPtr).append(attrValue);
          }
        } else {
          *keyEndpointPtr = attr.second;
        }
      }
    };
    Json::Value root;
    Json::Value storageShare;
    eos::IContainerMD::XAttrMap attributes;
    XrdOucErrInfo errInfo;
    // start with extended attributes so they can't overwrite fields
    gOFS->_attr_ls(gOFS->MgmProcPath.c_str(), errInfo, vid, nullptr,
                   attributes);

    for (const auto& attr : attributes) {
      processAccountingAttribute(attr, storageShare);
    }

    for (const auto& member : storageShare.getMemberNames()) {
      root["storageservice"][member] = storageShare[member];
    }

    root["storageservice"]["name"] = gOFS->MgmOfsInstanceName.c_str();
    std::ostringstream version;
    version << VERSION << "-" << RELEASE;
    root["storageservice"]["implementation"] = "EOS";
    root["storageservice"]["implementationversion"] = version.str().c_str();
    root["storageservice"]["latestupdate"] = Json::Int64{std::time(nullptr)};
    auto capacityOnline = Json::UInt64{0};
    auto usedOnline = Json::UInt64{0};

    for (const auto& quota : Quota::GetAllGroupsLogicalQuotaValues()) {
      storageShare.clear();
      attributes.clear();
      errInfo.clear();
      gOFS->_attr_ls(quota.first.c_str(), errInfo, vid, nullptr, attributes);

      for (const auto& attr : attributes) {
        processAccountingAttribute(attr, storageShare);
      }

      auto usedSizeofShare = Json::UInt64{std::get<0>(quota.second)};
      auto totalSizeofShare = Json::UInt64{std::get<1>(quota.second)};
      capacityOnline += totalSizeofShare;
      usedOnline += usedSizeofShare;
      storageShare["path"].append(quota.first);
      storageShare["usedsize"] = usedSizeofShare;
      storageShare["totalsize"] = totalSizeofShare;
      storageShare["numberoffiles"] = Json::UInt64{std::get<2>(quota.second)};
      storageShare["timestamp"] = Json::Int64{std::time(nullptr)};
      root["storageservice"]["storageshares"].append(storageShare);
    }

    root["storageservice"]["storagecapacity"]["online"]["totalsize"] =
      capacityOnline;
    root["storageservice"]["storagecapacity"]["online"]["usedsize"] = usedOnline;
    root["storageservice"]["storagecapacity"]["offline"]["totalsize"] = Json::UInt64{0};
    root["storageservice"]["storagecapacity"]["offline"]["usedsize"] = Json::UInt64{0};
    return new std::string(SSTR(root));
  };
  retc = SFS_OK;

  if (mSubCmd == "config") {
    if (!pVid->sudoer) {
      stdErr += "error: only sudoers are allowed to change cache configuration";
      retc = EPERM;
      return retc;
    }

    if (pOpaque->Get("mgm.accounting.expired")) {
      try {
        auto minutes = std::stoi(pOpaque->Get("mgm.accounting.expired"));
        accountingCache.SetExpiredAfter(
          std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::minutes(std::max(minutes, 1))
          )
        );
        stdOut += "success: expired time frame set to ";
        stdOut += minutes;
        stdOut += "\n";
      } catch (std::invalid_argument& err) {
        stdErr += "error: provided number is not configurable";
        retc = EINVAL;
      }
    }

    if (pOpaque->Get("mgm.accounting.invalid")) {
      try {
        auto minutes = std::stoi(pOpaque->Get("mgm.accounting.invalid"));
        accountingCache.SetInvalidAfter(
          std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::minutes(std::max(minutes, 5))
          )
        );
        stdOut += "success: invalid time frame set to ";
        stdOut += minutes;
        stdOut += "\n";
      } catch (std::invalid_argument& err) {
        stdErr += "error: provided number is not configurable";
        retc = EINVAL;
      }
    }
  } else if (mSubCmd == "report") {
    auto options = std::string(pOpaque->Get("mgm.option") ?
                               pOpaque->Get("mgm.option") : "");
    bool forceUpdate = options.find('f') != std::string::npos;

    try {
      auto json = accountingCache.getCachedObject(forceUpdate, generateAccountingJson,
                  std::ref(*pVid));
      stdOut += json.c_str();
    } catch (eos::common::UpdateException& err) {
      stdErr += err.what();
      retc = EAGAIN;
    }
  } else {
    stdErr += "error: command is not supported";
    retc = ENOTSUP;
  }

  return retc;
}

EOSMGMNAMESPACE_END
