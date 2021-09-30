// ----------------------------------------------------------------------
// File: QoSConfig.cc
// Author: Mihai Patrascoiu - CERN
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

#include "mgm/qos/QoSConfig.hh"
#include "mgm/qos/QoSClass.hh"
#include "namespace/interface/IFileMD.hh"

#include <iostream>
#include <json/json.h>
#include <fcntl.h>
#include <unistd.h>

EOSMGMNAMESPACE_BEGIN

//----------------------------------------------------------------------------
// Constructor
//----------------------------------------------------------------------------
QoSConfig::QoSConfig(const char* filename) : mFilename(filename)
{
  mFile.open(filename);
}

//----------------------------------------------------------------------------
// Load the config file into a map of QoS Classes
//----------------------------------------------------------------------------
std::map<std::string, eos::mgm::QoSClass>
QoSConfig::LoadConfig()
{
  std::map<std::string, eos::mgm::QoSClass> map;

  if (IsValid()) {
    Json::Reader reader;
    Json::Value root;

    if (reader.parse(mFile, root, false)) {
      for (auto& it: root) {
        // Create QoS class
        auto qosclass_ptr = QoSConfig::CreateQoSClass(it);

        if (qosclass_ptr) {
          map.emplace(qosclass_ptr->name, *qosclass_ptr.get());
        }
      }
    } else {
      eos_static_err("msg=\"failed parsing JSON config file\" emsg=\"%s\"",
                     reader.getFormattedErrorMessages().c_str());
    }
  }

  return map;
}

//----------------------------------------------------------------------------
// Build a QoS Class object from JSON
//----------------------------------------------------------------------------
std::shared_ptr<eos::mgm::QoSClass>
QoSConfig::CreateQoSClass(const Json::Value& qos_json)
{
  std::ostringstream info;

  std::vector<std::string> transitions;
  std::vector<std::string> locations;
  eos::IFileMD::QoSAttrMap attributes;
  int cdmi_redundancy = -1;
  int cdmi_latency = -1;
  std::string name;

  // Lambda function -- Check if member exists,
  // otherwise write it as missing to the to the output stream
  auto hasMember = [&info](const Json::Value& json, const char* key) -> bool {
    if (json.isMember(key)) {
      return true;
    }

    info << (info.tellp() != 0 ? " " : "") << key;
    return false;
  };

  try {
    // Extract name
    if (hasMember(qos_json, "name")) {
      name = qos_json["name"].asString();
    }

    // Extract transition list
    if (hasMember(qos_json, "transition")) {
      for (auto& it: qos_json["transition"]) {
        transitions.emplace_back(it.asString());
      }
    }

    // Extract metadata attributes
    if (hasMember(qos_json, "metadata")) {
      auto field = qos_json["metadata"];

      if (hasMember(field, CDMI_REDUNDANCY_TAG)) {
        cdmi_redundancy = field[CDMI_REDUNDANCY_TAG].asInt();
      }

      if (hasMember(field, CDMI_LATENCY_TAG)) {
        cdmi_latency = field[CDMI_LATENCY_TAG].asInt();
      }

      if (hasMember(field, CDMI_PLACEMENT_TAG)) {
        for (auto& it: field[CDMI_PLACEMENT_TAG]) {
          locations.emplace_back(it.asString());
        }
      }
    }

    // Extract class attributes
    if (hasMember(qos_json, "attributes")) {
      auto field = qos_json["attributes"];

      if (hasMember(field, "layout")) {
        attributes["layout"] = field["layout"].asString();
      }

      if (hasMember(field, "replica")) {
        attributes["replica"] = field["replica"].asString();
      }

      if (hasMember(field, "checksum")) {
        attributes["checksum"] = field["checksum"].asString();
      }

      if (hasMember(field, "placement")) {
        attributes["placement"] = field["placement"].asString();
      }
    }
  } catch (const Json::Exception& e) {
    eos_static_err("msg=\"json conversion exception\" emsg=\"%s\"",
                   e.what());
    return nullptr;
  }

  if (info.tellp() != 0) {
    eos_static_notice("msg=\"failed to construct QoS class\" "
                      "missing_fields=\"%s\"", info.str().c_str());
    return nullptr;
  }

  return shared_ptr<eos::mgm::QoSClass>(
    new QoSClass(name, cdmi_redundancy, cdmi_latency,
                 transitions, locations, attributes));
}

//----------------------------------------------------------------------------
//! Return string representation of a QoS class
//----------------------------------------------------------------------------
std::string
QoSConfig::QoSClassToString(const eos::mgm::QoSClass& qos)
{
  std::ostringstream out;

  auto arrayToString = [](auto& array) -> std::string {
    std::ostringstream out;
    out << "[";

    for (auto& it: array) {
      out << " " << it << ",";
    }

    out.seekp((array.size() ? -1 : 0), std::ios_base::end);
    out << " ]";
    return out.str();
  };

  out << "name=" << qos.name << std::endl
      << "transition=" << arrayToString(qos.transitions) << std::endl
      << CDMI_REDUNDANCY_TAG << "=" << qos.cdmi_redundancy << std::endl
      << CDMI_PLACEMENT_TAG << "=" << arrayToString(qos.locations) << std::endl
      << CDMI_LATENCY_TAG << "=" << qos.cdmi_latency << std::endl;

  for (auto& it: qos.attributes) {
    out << it.first << "=" << it.second << std::endl;
  }

  return out.str();
}

//----------------------------------------------------------------------------
//! Return JSON representation of a QoS class
//----------------------------------------------------------------------------
Json::Value
QoSConfig::QoSClassToJson(const eos::mgm::QoSClass& qos)
{
  Json::Value json;

  json["name"] = qos.name;
  json["transition"] = Json::arrayValue;
  for (auto& transition: qos.transitions) {
    json["transition"].append(transition);
  }

  json["metadata"] = Json::objectValue;
  json["metadata"][CDMI_REDUNDANCY_TAG] = (Json::UInt) qos.cdmi_redundancy;
  json["metadata"][CDMI_LATENCY_TAG] = (Json::UInt) qos.cdmi_latency;
  json["metadata"][CDMI_PLACEMENT_TAG] = Json::arrayValue;
  for (auto& location: qos.locations) {
    json["metadata"][CDMI_PLACEMENT_TAG].append(location);
  }

  json["attributes"] = Json::objectValue;
  for (auto& it: qos.attributes) {
    json["attributes"][it.first] = it.second;
  }

  return json;
}

EOSMGMNAMESPACE_END
