//------------------------------------------------------------------------------
// File: ConfigStore.hh
// Author: Abhishek Lekshmanan - CERN
//------------------------------------------------------------------------------

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

#pragma once
#include "common/StringUtils.hh"
#include "common/Logging.hh"

namespace eos::common
{

/*
 * A simple class to talk to various ways of talking to generic string key-value
 * store, the intent is that consumer classes can talk to for eg. a global config
 * store/ a per space config store or qdb etc. It is upto the subclasses to implement
 * how to save and load the key value from config. A couple of convenience functions
 * are added to retrieve numeric keys and defaults in case no keys exist.
 */
class ConfigStore: public eos::common::LogId
{
public:
  //! Save a key value to the underlying config store
  //! \param key the string key
  //! \param val the string value
  //! \return bool status of the save
  virtual bool save(const std::string& key, const std::string& val) = 0;

  //! Obtain the value corresponding to the key from the store
  //! \param key the string key
  //! \return the string value or empty string
  virtual std::string load(const std::string& key) = 0;
  virtual ~ConfigStore() = default;

  std::string get(const std::string& key, const std::string& default_val)
  {
    if (auto s = load(key);
        !s.empty()) {
      return s;
    }

    return default_val;
  }

  //! Get a numeric (int/float/etc) value from the key value store
  //! \tparam NumT deduced from default_val, needn't be supplied
  //! \param key the string key
  //! \param default_val a default val of desired numeric return type
  //! \return the value or default_val if no hits were found
  template <typename NumT>
  auto get(const std::string& key, NumT default_val) noexcept
  -> typename std::enable_if_t<std::is_arithmetic_v<NumT>, NumT> {
    NumT val;
    std::string log_msg;

    if (!StringToNumeric(load(key), val, default_val, &log_msg))
    {
      eos_err("msg=\"failed to load key from Configstore\" key=\"%s\" err=%s",
      key.c_str(), log_msg.c_str());
    }

    return val;
  }
};

} // eos::common
