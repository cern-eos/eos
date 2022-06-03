/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2022 CERN/Switzerland                           *
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

#ifndef EOS_MEMCONFIGSTORE_HH
#define EOS_MEMCONFIGSTORE_HH

#include "common/config/ConfigStore.hh"

class MemConfigStore : public eos::common::ConfigStore {
public:
  bool save(const std::string& key, const std::string& val) override
  {
    kvstore[key] = val;
    return true;
  }

  std::string load(const std::string& key) override
  {
    if (auto kv = kvstore.find(key);
        kv != kvstore.end()) {
      return kv->second;
    }
    return {};
  }

private:
  std::map<std::string, std::string> kvstore;
};


#endif // EOS_MEMCONFIGSTORE_HH
