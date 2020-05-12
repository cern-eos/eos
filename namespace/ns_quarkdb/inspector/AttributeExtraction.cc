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

#include "namespace/ns_quarkdb/inspector/AttributeExtraction.hh"
#include "namespace/ns_quarkdb/inspector/Printing.hh"
#include "namespace/utils/Checksum.hh"
#include "common/StringUtils.hh"
#include <sstream>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Convert to octal string
//------------------------------------------------------------------------------
static std::string to_octal_string(uint32_t v) {
  std::ostringstream ss;
  ss << std::oct << v;
  return ss.str();
}

//------------------------------------------------------------------------------
// Serialize locations vector
//------------------------------------------------------------------------------
template<typename T>
static std::string serializeLocations(const T& vec) {
  std::ostringstream stream;

  for(int i = 0; i < vec.size(); i++) {
    stream << vec[i];
    if(i != vec.size() - 1) {
      stream << ",";
    }
  }

  return stream.str();
}

//------------------------------------------------------------------------------
// Extract the given attribute as string
//------------------------------------------------------------------------------
bool AttributeExtraction::asString(const eos::ns::FileMdProto &proto,
    const std::string &attr, std::string &out) {

  out.clear();

  if(common::startsWith(attr, "xattr.")) {
    std::string chopped = std::string(attr.begin()+6, attr.end());

    auto it = proto.xattrs().find(chopped);
    if(it != proto.xattrs().end()) {
      out = it->second;
    }

    return true;
  }

  if(attr == "fid") {
    out = std::to_string(proto.id());
    return true;
  }

  if(attr == "pid") {
    out = std::to_string(proto.cont_id());
    return true;
  }

  if(attr == "uid") {
    out = std::to_string(proto.uid());
    return true;
  }

  if(attr == "gid") {
    out = std::to_string(proto.gid());
    return true;
  }

  if(attr == "size") {
    out = std::to_string(proto.size());
    return true;
  }

  if(attr == "layout_id") {
    out = std::to_string(proto.layout_id());
    return true;
  }

  if(attr == "flags") {
    out = to_octal_string(proto.flags());
    return true;
  }

  if(attr == "name") {
    out = proto.name();
    return true;
  }

  if(attr == "link_name") {
    out = proto.link_name();
    return true;
  }

  if(attr == "ctime") {
    out = Printing::timespecToTimestamp(Printing::parseTimespec(proto.ctime()));
    return true;
  }

  if(attr == "mtime") {
    out = Printing::timespecToTimestamp(Printing::parseTimespec(proto.mtime()));
    return true;
  }

  if(attr == "xs") {
    std::string xs;
    eos::appendChecksumOnStringProtobuf(proto, xs);
    out = xs;
    return true;
  }

  if(attr == "locations") {
    out = serializeLocations(proto.locations());
    return true;
  }

  if(attr == "unlink_locations") {
    out = serializeLocations(proto.unlink_locations());
    return true;
  }

  if(attr == "stime") {
    out = Printing::timespecToTimestamp(Printing::parseTimespec(proto.stime()));
    return true;
  }

  return false;
}

EOSNSNAMESPACE_END
