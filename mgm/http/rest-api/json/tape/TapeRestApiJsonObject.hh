// ----------------------------------------------------------------------
// File: TapeRestApiJsonObject.hh
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
#ifndef EOS_TAPERESTAPIJSONOBJECT_HH
#define EOS_TAPERESTAPIJSONOBJECT_HH

#include "mgm/Namespace.hh"
#include "common/json/JsonCppObject.hh"
#include "mgm/http/rest-api/model/tape/ErrorModel.hh"
#include <sstream>

EOSMGMRESTNAMESPACE_BEGIN

template<typename Obj>
class TapeRestApiJsonObject : public common::JsonCppObject<Obj>{
public:
  template<class... Args>
  TapeRestApiJsonObject(Args... args): common::JsonCppObject<Obj>(args...){}
  inline virtual void jsonify(std::stringstream & ss) override { common::JsonCppObject<Obj>::jsonify(ss); }
};

template<>
inline void
TapeRestApiJsonObject<ErrorModel>::jsonify(std::stringstream& ss) {
  Json::Value root;
  root["type"] = mObject->getType();
  root["title"] = mObject->getTitle();
  root["status"] = mObject->getStatus();
  root["detail"] = mObject->getDetail() ? mObject->getDetail().value() : "";
  ss << root;
}

EOSMGMRESTNAMESPACE_END

#endif // EOS_TAPERESTAPIJSONOBJECT_HH
