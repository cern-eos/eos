/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2020 CERN/Switzerland                                  *
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

#include "namespace/ns_quarkdb/inspector/OutputSink.hh"
#include <sstream>
#include <json/json.h>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Print everything known about a ContainerMD
//------------------------------------------------------------------------------
void OutputSink::print(const eos::ns::ContainerMdProto &proto) {
  std::map<std::string, std::string> out;

  out["cid"] = std::to_string(proto.id());
  out["name"] = proto.name();

  print(out);
}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
StreamSink::StreamSink(std::ostream &out, std::ostream &err)
: mOut(out), mErr(err) {}

//------------------------------------------------------------------------------
// Print implementation
//------------------------------------------------------------------------------
void StreamSink::print(const std::map<std::string, std::string> &line) {
  for(auto it = line.begin(); it != line.end(); it++) {
    if(it != line.begin()) {
      mOut << " ";
    }

    mOut << it->first << "=" << it->second;
  }

  mOut << std::endl;
}

//------------------------------------------------------------------------------
// Debug output
//------------------------------------------------------------------------------
void StreamSink::err(const std::string &str) {
  mErr << str << std::endl;
}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
JsonStreamSink::JsonStreamSink(std::ostream &out, std::ostream &err)
: mOut(out), mErr(err), mFirst(true) {

  mOut << "[" << std::endl;
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
JsonStreamSink::~JsonStreamSink() {
  mOut << "]" << std::endl;
}

//------------------------------------------------------------------------------
// Print implementation
//------------------------------------------------------------------------------
void JsonStreamSink::print(const std::map<std::string, std::string> &line) {
  if(!mFirst) {
    mOut << "," << std::endl;
  }

  mFirst = false;

  Json::Value json;

  for(auto it = line.begin(); it != line.end(); it++) {
    json[it->first] = it->second;
  }

  mOut << json;
}

//------------------------------------------------------------------------------
// Debug output
//------------------------------------------------------------------------------
void JsonStreamSink::err(const std::string &str) {
  mErr << str << std::endl;
}

EOSNSNAMESPACE_END
