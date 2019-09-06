// ----------------------------------------------------------------------
// File: SharedHashWrapper.cc
// Author: Georgios Bitzes - CERN
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

#include "SharedHashWrapper.hh"
#include "mq/XrdMqSharedObject.hh"
#include "common/ParseUtils.hh"

EOSMQNAMESPACE_BEGIN

XrdMqSharedObjectManager* SharedHashWrapper::mSom;

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
SharedHashWrapper::SharedHashWrapper(const common::SharedHashLocator &locator, bool takeLock, bool create)
: mLocator(locator) {

  if(takeLock) {
    mReadLock.Grab(mSom->HashMutex);
  }

  mHash = mSom->GetObject(mLocator.getConfigQueue().c_str(), "hash");

  if (!mHash && create) {
    //--------------------------------------------------------------------------
    // Shared hash does not exist, create
    //--------------------------------------------------------------------------
    mReadLock.Release();

    mSom->CreateSharedHash(mLocator.getConfigQueue().c_str(),
      mLocator.getBroadcastQueue().c_str(), mSom);

    mReadLock.Grab(mSom->HashMutex);
    mHash = mSom->GetObject(mLocator.getConfigQueue().c_str(), "hash");
  }
}

//------------------------------------------------------------------------------
// "Constructor" for global MGM hash
//------------------------------------------------------------------------------
SharedHashWrapper SharedHashWrapper::makeGlobalMgmHash() {
  return SharedHashWrapper(common::SharedHashLocator::makeForGlobalHash());
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
SharedHashWrapper::~SharedHashWrapper() {
  releaseLocks();
}

//------------------------------------------------------------------------------
// Release any interal locks - DO NOT use this object any further
//------------------------------------------------------------------------------
void SharedHashWrapper::releaseLocks() {
  mHash = nullptr;
  mReadLock.Release();
}

//------------------------------------------------------------------------------
// Set key-value pair
//------------------------------------------------------------------------------
bool SharedHashWrapper::set(const std::string &key, const std::string &value, bool broadcast) {
  if(!mHash) return false;
  return mHash->Set(key.c_str(), value.c_str(), broadcast);
}

//------------------------------------------------------------------------------
// Query the given key
//------------------------------------------------------------------------------
std::string SharedHashWrapper::get(const std::string &key) {
  if(!mHash) return "";
  return mHash->Get(key.c_str());
}

//------------------------------------------------------------------------------
// Query the given key - convert to long long automatically
//------------------------------------------------------------------------------
long long SharedHashWrapper::getLongLong(const std::string &key) {
  return eos::common::ParseLongLong(get(key));
}

//----------------------------------------------------------------------------
// Query the given key - convert to double automatically
//----------------------------------------------------------------------------
double SharedHashWrapper::getDouble(const std::string &key) {
  return eos::common::ParseDouble(get(key));
}

//------------------------------------------------------------------------------
// Query the given key, return if retrieval successful
//------------------------------------------------------------------------------
bool SharedHashWrapper::get(const std::string &key, std::string &value) {
  if(!mHash) return false;
  value = mHash->Get(key.c_str());
  return true;
}

//------------------------------------------------------------------------------
// Delete the given key
//------------------------------------------------------------------------------
bool SharedHashWrapper::del(const std::string &key, bool broadcast) {
  if(!mHash) return false;
  return mHash->Delete(key.c_str(), broadcast);
}

//------------------------------------------------------------------------------
// Get all keys in hash
//------------------------------------------------------------------------------
bool SharedHashWrapper::getKeys(std::vector<std::string> &out) {
  if(!mHash) return false;
  out = mHash->GetKeys();
  return true;
}

//------------------------------------------------------------------------------
// Initialize, set shared manager.
// Call this function before using any SharedHashWrapper!
//------------------------------------------------------------------------------
void SharedHashWrapper::initialize(XrdMqSharedObjectManager *som) {
  mSom = som;
}

//------------------------------------------------------------------------------
// Print contents onto a table: Compatibility function, originally
// implemented in XrdMqSharedHash.
//------------------------------------------------------------------------------
void SharedHashWrapper::print(TableHeader& table_mq_header,
  TableData& table_mq_data, std::string format, const std::string &filter) {

  using eos::common::StringConversion;
  std::vector<std::string> formattoken;
  StringConversion::Tokenize(format, formattoken, "|");
  table_mq_data.emplace_back();

  for (unsigned int i = 0; i < formattoken.size(); ++i) {
    std::vector<std::string> tagtoken;
    std::map<std::string, std::string> formattags;
    StringConversion::Tokenize(formattoken[i], tagtoken, ":");

    for (unsigned int j = 0; j < tagtoken.size(); ++j) {
      std::vector<std::string> keyval;
      StringConversion::Tokenize(tagtoken[j], keyval, "=");

      if (keyval.size() >= 2) {
        formattags[keyval[0]] = keyval[1];
      }
    }

    if (formattags.count("format")) {
      unsigned int width = atoi(formattags["width"].c_str());
      std::string format = formattags["format"];
      std::string unit = formattags["unit"];

      // Normal member printout
      if (formattags.count("key")) {
        if (format.find("s") != std::string::npos) {
          table_mq_data.back().push_back(
            TableCell(this->get(formattags["key"]), format));
        }

        if ((format.find("S")) != std::string::npos) {
          std::string shortstring = this->get(formattags["key"].c_str());
          const size_t pos = shortstring.find(".");

          if (pos != std::string::npos) {
            shortstring.erase(pos);
          }

          table_mq_data.back().push_back(TableCell(shortstring, format));
        }

        if ((format.find("l")) != std::string::npos) {
          table_mq_data.back().push_back(
            TableCell(this->getLongLong(formattags["key"].c_str()), format, unit));
        }

        if ((format.find("f")) != std::string::npos) {
          table_mq_data.back().push_back(
            TableCell(this->getDouble(formattags["key"].c_str()), format, unit));
        }

        XrdOucString name = formattags["key"].c_str();

        if (format.find("o") == std::string::npos) {  //only for table output
          name.replace("stat.", "");
          name.replace("stat.statfs.", "");

          if (formattags.count("tag")) {
            name = formattags["tag"].c_str();
          }
        }

        table_mq_header.push_back(std::make_tuple(name.c_str(), width, format));
      }
    }
  }

  //we check for filters
  bool toRemove = false;

  if (filter.find("d") != string::npos) {
    std::string drain = this->get("stat.drain");

    if (drain == "nodrain") {
      toRemove = true;
    }
  }

  if (filter.find("e") != string::npos) {
    int err = (int) this->getLongLong("stat.errc");

    if (err == 0) {
      toRemove = true;
    }
  }

  if (toRemove) {
    table_mq_data.pop_back();
  }
}

EOSMQNAMESPACE_END
