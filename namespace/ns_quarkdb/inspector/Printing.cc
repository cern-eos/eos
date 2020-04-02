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

#include "namespace/ns_quarkdb/inspector/Printing.hh"
#include "common/LayoutId.hh"
#include "namespace/utils/Checksum.hh"
#include "namespace/interface/IFileMD.hh"
#include "namespace/utils/Etag.hh"
#include "common/StringConversion.hh"
#include <sstream>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Escape non-printable string
//------------------------------------------------------------------------------
std::string Printing::escapeNonPrintable(const std::string &str) {
  std::stringstream ss;

  for(size_t i = 0; i < str.size(); i++) {
    if(isprint(str[i])) {
      ss << str[i];
    }
    else if(str[i] == '\0') {
      ss << "\\x00";
    }
    else {
      char buff[16];
      snprintf(buff, 16, "\\x%02X", (unsigned char) str[i]);
      ss << buff;
    }
  }
  
  return ss.str();
}

//------------------------------------------------------------------------------
// Serialize locations vector
//------------------------------------------------------------------------------
template<typename T>
static std::string serializeLocations(const T& vec) {
  std::ostringstream stream;
  stream << "[";

  for(int i = 0; i < vec.size(); i++) {
    stream << vec[i];
    if(i != vec.size() - 1) {
      stream << ", ";
    }
  }

  stream << "]";
  return stream.str();
}

//------------------------------------------------------------------------------
// timespec to fileinfo: Convert a timespec into
// "1447252711.38412918"
//------------------------------------------------------------------------------
std::string Printing::timespecToTimestamp(const struct timespec &val) {
  std::ostringstream ss;
  ss << val.tv_sec << "." << val.tv_nsec;
  return ss.str();
}

//------------------------------------------------------------------------------
// timespec to fileinfo: Convert a timespec into
// "Wed Nov 11 15:38:31 2015 Timestamp: 1447252711.38412918"
//------------------------------------------------------------------------------
void Printing::timespecToFileinfo(const struct timespec &val, std::ostream &stream) {
  time_t tv_sec = (time_t) val.tv_sec;
  char buffer[4096];

  stream << ctime_r(&tv_sec, buffer);
  stream.seekp(-1, std::ios_base::end);
  stream << " Timestamp: " << timespecToTimestamp(val);
}

std::string Printing::timespecToFileinfo(const struct timespec &val) {
  std::ostringstream ss;
  timespecToFileinfo(val, ss);
  return ss.str();
}

//------------------------------------------------------------------------------
// Serialize protobuf time
//------------------------------------------------------------------------------
template<typename T>
static std::string serializeTime(const T& bytes) {
  std::ostringstream ss;
  Printing::timespecToFileinfo(Printing::parseTimespec(bytes), ss);
  return ss.str();
}

//----------------------------------------------------------------------------
// Print the given ContainerMd protobuf using multiple lines, full information
//----------------------------------------------------------------------------
void Printing::printMultiline(const eos::ns::ContainerMdProto &proto, std::ostream &stream) {
  stream << "ID: " << proto.id() << std::endl;
  stream << "Parent ID: " << proto.parent_id() << std::endl;
  stream << "Name: " << proto.name() << std::endl;
  stream << "uid: " << proto.uid() << ", gid: " << proto.gid() << std::endl;
  stream << "ctime: " << serializeTime(proto.ctime()) << std::endl;
  stream << "mtime: " << serializeTime(proto.mtime()) << std::endl;
  stream << "stime: " << serializeTime(proto.stime()) << std::endl;
  stream << "Tree size: " << proto.tree_size() << std::endl;
  stream << "Mode: " << proto.mode() << std::endl;
  stream << "Flags: " << proto.flags() << std::endl;
}

//------------------------------------------------------------------------------
// Print the given FileMd protobuf using multiple lines, full information
//------------------------------------------------------------------------------
void Printing::printMultiline(const eos::ns::FileMdProto &proto, std::ostream &stream) {
  stream << "ID: " << proto.id() << std::endl;
  stream << "Name: " << proto.name() << std::endl;
  stream << "Link name: " << proto.link_name() << std::endl;
  stream << "Container ID: " << proto.cont_id() << std::endl;
  stream << "uid: " << proto.uid() << ", gid: " << proto.gid() << std::endl;
  stream << "Size: " << proto.size() << std::endl;
  stream << "Modify: " << serializeTime(proto.mtime()) << std::endl;
  stream << "Change: " << serializeTime(proto.ctime()) << std::endl;
  stream << "Flags: " << common::StringConversion::IntToOctal( (int) proto.flags(), 4) << std::endl;

  std::string checksum;
  appendChecksumOnStringProtobuf(proto, checksum);
  stream << "Checksum type: " << common::LayoutId::GetChecksumString(proto.layout_id()) << ", checksum bytes: " << checksum << std::endl;
  stream << "Expected number of replicas / stripes: " << eos::common::LayoutId::GetStripeNumber(proto.layout_id()) + 1 << std::endl;

  std::string etag;
  eos::calculateEtag(proto, etag);
  stream << "Etag: " << etag << std::endl;

  stream << "Locations: " << serializeLocations(proto.locations()) << std::endl;
  stream << "Unlinked locations: " << serializeLocations(proto.unlink_locations()) << std::endl;
}

std::string Printing::printMultiline(const eos::ns::FileMdProto &proto) {
  std::ostringstream ss;
  printMultiline(proto, ss);
  return ss.str();
}

EOSNSNAMESPACE_END
