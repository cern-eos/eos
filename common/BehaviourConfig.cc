//------------------------------------------------------------------------------
//! @file BehaviourConfig.cc
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2024 CERN/Switzerland                                  *
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

#include "common/BehaviourConfig.hh"

EOSCOMMONNAMESPACE_BEGIN

//----------------------------------------------------------------------------
// Check the accepted configuration values per behaviour
//----------------------------------------------------------------------------
bool AcceptedValue(BehaviourType behaviour, const std::string& value)
{
  if (behaviour == BehaviourType::RainMinFsidEntry) {
    if ((value != "on") && (value != "off")) {
      return false;
    }
  }

  return true;
}

//----------------------------------------------------------------------------
// Convert string to behaviour type
//----------------------------------------------------------------------------
BehaviourType
BehaviourConfig::ConvertStringToBehaviour(const std::string& input)
{
  if (input == "rain_min_fsid_entry") {
    return BehaviourType::RainMinFsidEntry;
  } else if (input == "all") {
    return BehaviourType::All;
  } else {
    return BehaviourType::None;
  }
}

//----------------------------------------------------------------------------
//! Convert behaviour type to string
//----------------------------------------------------------------------------
std::string
BehaviourConfig::ConvertBehaviourToString(const BehaviourType& btype)
{
  if (btype == BehaviourType::RainMinFsidEntry) {
    return "rain_min_fsid_entry";
  } else {
    return "unknown";
  }
}

//------------------------------------------------------------------------------
// Set behaviour change
//------------------------------------------------------------------------------
bool
BehaviourConfig::Set(BehaviourType behaviour, const std::string& value)
{
  if (!AcceptedValue(behaviour, value)) {
    return false;
  }

  std::unique_lock<std::mutex> lock(mMutex);

  if (value == "off") {
    mMapBehaviours.erase(behaviour);
  } else {
    mMapBehaviours[behaviour] = value;
  }

  return true;
}

//------------------------------------------------------------------------------
// Get behaviour configuration value
//------------------------------------------------------------------------------
std::string
BehaviourConfig::Get(const BehaviourType& behaviour) const
{
  std::unique_lock<std::mutex> loc(mMutex);
  auto it = mMapBehaviours.find(behaviour);

  if (it != mMapBehaviours.end()) {
    return it->second;
  }

  return std::string();
}

//------------------------------------------------------------------------------
// Check if given behaviour exists in the map
//------------------------------------------------------------------------------
bool
BehaviourConfig::Exists(const BehaviourType& behaviour) const
{
  std::unique_lock<std::mutex> lock(mMutex);
  return (mMapBehaviours.find(behaviour) != mMapBehaviours.end());
}

//------------------------------------------------------------------------------
// List all configured behaviours
//------------------------------------------------------------------------------
std::map<std::string, std::string>
BehaviourConfig::List() const
{
  std::map<std::string, std::string> output;
  std::unique_lock<std::mutex> lock(mMutex);

  for (const auto& elem : mMapBehaviours) {
    output[ConvertBehaviourToString(elem.first)] = elem.second;
  }

  return output;
}

//------------------------------------------------------------------------------
// Clean the given behaviour type
//------------------------------------------------------------------------------
void
BehaviourConfig::Clear(const BehaviourType& behaviour)
{
  if (behaviour == BehaviourType::None) {
    return;
  }

  std::unique_lock<std::mutex> lock(mMutex);

  if (behaviour == BehaviourType::All) {
    mMapBehaviours.clear();
  } else {
    auto it = mMapBehaviours.find(behaviour);

    if (it != mMapBehaviours.end()) {
      mMapBehaviours.erase(it);
    }
  }
}

EOSCOMMONNAMESPACE_END
