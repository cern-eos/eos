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
#include "namespace/ns_quarkdb/inspector/Printing.hh"
#include <sstream>
#include <json/json.h>

#define SSTR(message) static_cast<std::ostringstream&>(std::ostringstream().flush() << message).str()

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Convert to octal string
//------------------------------------------------------------------------------
std::string to_octal_string(uint32_t v) {
  std::ostringstream ss;
  ss << std::oct << v;
  return ss.str();
}

//------------------------------------------------------------------------------
//! Print everything known about a ContainerMD
//------------------------------------------------------------------------------
void OutputSink::print(const eos::ns::ContainerMdProto &proto, const ContainerPrintingOptions &opts) {
  std::map<std::string, std::string> out;

  if(opts.showId) {
    out["cid"] = std::to_string(proto.id());
  }

  if(opts.showParent) {
    out["parent_id"] = std::to_string(proto.parent_id());
  }

  if(opts.showUid) {
    out["uid"] = std::to_string(proto.uid());
  }

  if(opts.showGid) {
    out["gid"] = std::to_string(proto.gid());
  }

  if(opts.showTreeSize) {
    out["tree_size"] = std::to_string(proto.tree_size());
  }

  if(opts.showMode) {
    out["mode"] = to_octal_string(proto.mode());
  }

  if(opts.showMode) {
    out["flags"] = to_octal_string(proto.flags());
  }

  if(opts.showName) {
    out["name"] = proto.name();
  }

  if(opts.showCTime) {
    out["ctime"] = Printing::timespecToTimestamp(Printing::parseTimespec(proto.ctime()));
  }

  if(opts.showMTime) {
    out["mtime"] = Printing::timespecToTimestamp(Printing::parseTimespec(proto.mtime()));
  }

  if(opts.showSTime) {
    out["stime"] = Printing::timespecToTimestamp(Printing::parseTimespec(proto.stime()));
  }

  if(opts.showXAttr) {
    for(auto it = proto.xattrs().begin(); it != proto.xattrs().end(); it++) {
      out[SSTR("xattr." << it->first)] = it->second;
    }
  }

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
