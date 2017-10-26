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
 * You should have received a copy of the AGNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include "mgm/proc/ProcInterface.hh"
#include "mgm/Quota.hh"
#include "common/StringTokenizer.hh"
#include "common/ExpiryCache.hh"

EOSMGMNAMESPACE_BEGIN

int
ProcCommand::Accounting()
{
  static eos::common::ExpiryCache<std::string> accountingCache(
    std::chrono::seconds(600));
  static const auto generateAccountingJson = [this](
  eos::common::Mapping::VirtualIdentity & vid) {
    static const auto processAccountingAttribute = [](
    std::pair<std::string, std::string> attr, Json::Value & storageShare) {
      static auto accountingAttrPrefix = "sys.accounting";

      if (attr.first.find(accountingAttrPrefix) == 0) {
        auto objectPath = eos::common::StringTokenizer::split(attr.first, '.');
        Json::Value* keyEndpointPtr = nullptr;

        // Creating a new json object following the path in this case
        if (objectPath.size() > 3) {
          // we keep a pointer to the previous json object along the path
          auto* prevObject = &(storageShare[objectPath[2]]);

          for (size_t i = 3; i < objectPath.size(); ++i) {
            try {
              auto number = std::stoul(objectPath[i].c_str());
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
          for (auto &&
               attrValue : eos::common::StringTokenizer::split(attr.second, ',')) {
            (*keyEndpointPtr).append(attrValue);
          }
        } else {
          *keyEndpointPtr = attr.second;
        }
      }
    };
    Json::Value root;
    root["storageservice"]["name"] = gOFS->MgmOfsInstanceName.c_str();
    std::ostringstream version;
    version << VERSION << "-" << RELEASE;
    root["storageservice"]["implementationversion"] = version.str().c_str();
    root["storageservice"]["latestupdate"] = Json::Int64{std::time(nullptr)};
    auto capacity = Json::UInt64{0};
    auto used = Json::UInt64{0};
    {
      eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);

      for (const auto& space : FsView::gFsView.mIdView) {
        capacity += space.second->GetLongLong("stat.statfs.capacity");
        used += space.second->GetLongLong("stat.statfs.usedbytes");
      }
    }
    root["storageservice"]["storagecapacity"]["online"]["totalsize"] = capacity;
    root["storageservice"]["storagecapacity"]["online"]["usedsize"] = used;
    Json::Value storageShare;
    eos::IContainerMD::XAttrMap attributes;
    XrdOucErrInfo errInfo;
    gOFS->_attr_ls(gOFS->MgmProcPath.c_str(), errInfo, vid, (const char*) 0,
                   attributes);

    for (const auto& attr : attributes) {
      processAccountingAttribute(attr, storageShare);
    }

    for (const auto& member : storageShare.getMemberNames()) {
      root["storageservice"][member] = storageShare[member];
    }

    for (const auto& quota : Quota::GetAllGroupsLogicalQuotaValues()) {
      storageShare.clear();
      attributes.clear();
      errInfo.clear();
      gOFS->_attr_ls(quota.first.c_str(), errInfo, vid, (const char*) 0, attributes);

      for (const auto& attr : attributes) {
        processAccountingAttribute(attr, storageShare);
      }

      storageShare["path"].append(quota.first);
      storageShare["usedsize"] = Json::UInt64{std::get<0>(quota.second)};
      storageShare["totalsize"] = Json::UInt64{std::get<1>(quota.second)};
      storageShare["numberoffiles"] = Json::UInt64{std::get<2>(quota.second)};
      storageShare["timestamp"] = Json::Int64{std::time(nullptr)};
      root["storageservice"]["storageshares"].append(storageShare);
    }

    Json::StyledWriter writer;
    return new std::string(writer.write(root));
  };

  if (mSubCmd == "config") {
    if (pOpaque->Get("mgm.accounting.expired")) {
      try {
        auto minutes = std::stoi(pOpaque->Get("mgm.accounting.expired"));
        accountingCache.SetExpiredAfter(
          std::chrono::duration_cast<std::chrono::seconds>(std::chrono::minutes(
                minutes)));
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
          std::chrono::duration_cast<std::chrono::seconds>(std::chrono::minutes(
                minutes)));
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

  return SFS_OK;
}

EOSMGMNAMESPACE_END
