// ----------------------------------------------------------------------
// File: JsonCppJsonifier.hh
// Author: Cedric Caffy - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2013 CERN/Switzerland                                  *
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

#ifndef EOS_JSONCPPJSONIFIER_HH
#define EOS_JSONCPPJSONIFIER_HH

#include "common/Namespace.hh"
#include "common/json/Jsonifier.hh"
#include <sstream>
#include <memory>
#include <json/json.h>

EOSCOMMONNAMESPACE_BEGIN

/**
 * Inherit this interface in order to implement
 * the way to generate the json representation of any object using the JsonCPP library
 * @tparam Obj the Object you want to generate the json representation from
 */
template <typename Obj>
class JsonCppJsonifier : public virtual common::Jsonifier<Obj> {
public:
  /**
   * Implement this method to generate the json representation of any object
   * @param object the object from which you want the json representation
   * @param ss the stream where this json representation will be pushed to
   */
  virtual void jsonify(const Obj * obj, std::stringstream & oss) = 0;
protected:
  inline virtual void initializeArray(Json::Value & value) {
    value = Json::Value(Json::arrayValue);
  }
};

EOSCOMMONNAMESPACE_END

#endif // EOS_JSONCPPJSONIFIER_HH
