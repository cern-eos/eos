// ----------------------------------------------------------------------
// File: Jsonifiable.hh
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

#ifndef EOS_JSONIFIABLE_HH
#define EOS_JSONIFIABLE_HH

#include "mgm/Namespace.hh"
#include <memory>
#include <sstream>
#include "common/json/Jsonifier.hh"

EOSCOMMONNAMESPACE_BEGIN


/**
 * Common class allowing any object inheriting from this class
 * can give its json representation
 * You can implement the jsonify() method by hand
 * or set a custom jsonifier before calling jsonify()
 * @tparam Object the object to give its json representation
 */
template<typename Object>
class Jsonifiable {
public:
  inline void setJsonifier(std::shared_ptr<common::Jsonifier<Object>> jsonifier) {
    mJsonifier = jsonifier;
  }
  virtual void jsonify(std::stringstream & ss) const {
    if(mJsonifier) {
      Object* thisptr = const_cast<Object*>(static_cast<const Object*>(this));
      mJsonifier->jsonify(thisptr,ss);
    }
  }
protected:
  std::shared_ptr<common::Jsonifier<Object>> mJsonifier;
};

EOSCOMMONNAMESPACE_END

#endif // EOS_JSONIFIABLE_HH
