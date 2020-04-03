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
#include "namespace/utils/Checksum.hh"
#include <sstream>
#include <json/json.h>

#define SSTR(message) static_cast<std::ostringstream&>(std::ostringstream().flush() << message).str()

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
//! Return full path, if possible, otherwise empty
//------------------------------------------------------------------------------
static std::string populateFullPath(const eos::ns::FileMdProto &proto, FileScanner::Item &item) {
  item.fullPath.wait();
  if(item.fullPath.hasException()) {
    return std::string();
  }

  std::string fullPath = std::move(item.fullPath).get();

  if(fullPath.empty()) {
    return std::string();
  }

  return SSTR(fullPath << proto.name());
}

//------------------------------------------------------------------------------
//! Return full path, if possible, otherwise empty
//------------------------------------------------------------------------------
static std::string populateFullPath(const eos::ns::ContainerMdProto &proto, ContainerScanner::Item &item) {
  item.fullPath.wait();
  if(item.fullPath.hasException()) {
    return std::string();
  }

  std::string fullPath = std::move(item.fullPath).get();

  if(fullPath.empty()) {
    return std::string();
  }

  return fullPath;
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
//! Populate map with container attributes
//------------------------------------------------------------------------------
static void populateMetadata(const eos::ns::ContainerMdProto &proto,
  const ContainerPrintingOptions &opts, std::map<std::string, std::string> &out) {

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
}

//------------------------------------------------------------------------------
//! Print everything known about a ContainerMD
//------------------------------------------------------------------------------
void OutputSink::print(const eos::ns::ContainerMdProto &proto, const ContainerPrintingOptions &opts) {
  std::map<std::string, std::string> out;
  populateMetadata(proto, opts, out);
  print(out);
}

//------------------------------------------------------------------------------
// Get count as string
//------------------------------------------------------------------------------
static std::string countAsString(folly::Future<uint64_t> &fut) {
  fut.wait();

  if(fut.hasException()) {
    return "N/A";
  }

  uint64_t val = std::move(fut).get();
  fut = val;

  return std::to_string(val);
}

//------------------------------------------------------------------------------
//! Print everything known about a ContainerMD, including full path if available
//------------------------------------------------------------------------------
void OutputSink::print(const eos::ns::ContainerMdProto &proto, const ContainerPrintingOptions &opts, ContainerScanner::Item &item, bool showCounts) {
  std::map<std::string, std::string> out;
  populateMetadata(proto, opts, out);

  std::string fullPath = populateFullPath(proto, item);
  if(!fullPath.empty()) {
    out["path"] = fullPath;
  }

  if(showCounts) {
    out["file-count"] = countAsString(item.fileCount);
    out["container-count"] = countAsString(item.containerCount);
  }

  print(out);
}

//------------------------------------------------------------------------------
//! Populate map with file attributes
//------------------------------------------------------------------------------
static void populateMetadata(const eos::ns::FileMdProto &proto,
  const FilePrintingOptions &opts, std::map<std::string, std::string> &out) {

  if(opts.showId) {
    out["fid"] = std::to_string(proto.id());
  }

  if(opts.showContId) {
    out["pid"] = std::to_string(proto.cont_id());
  }

  if(opts.showUid) {
    out["uid"] = std::to_string(proto.uid());
  }

  if(opts.showGid) {
    out["gid"] = std::to_string(proto.gid());
  }

  if(opts.showSize) {
    out["size"] = std::to_string(proto.size());
  }

  if(opts.showLayoutId) {
    out["layout_id"] = std::to_string(proto.layout_id());
  }

  if(opts.showFlags) {
    out["flags"] = to_octal_string(proto.flags());
  }

  if(opts.showName) {
    out["name"] = proto.name();
  }

  if(opts.showLinkName) {
    out["link_name"] = proto.link_name();
  }

  if(opts.showCTime) {
    out["ctime"] = Printing::timespecToTimestamp(Printing::parseTimespec(proto.ctime()));
  }

  if(opts.showMTime) {
    out["mtime"] = Printing::timespecToTimestamp(Printing::parseTimespec(proto.mtime()));
  }

  if(opts.showChecksum) {
    std::string xs;
    eos::appendChecksumOnStringProtobuf(proto, xs);
    out["xs"] = xs;
  }

  if(opts.showLocations) {
    out["locations"] = serializeLocations(proto.locations());
  }

  if(opts.showLocations) {
    out["unlink_locations"] = serializeLocations(proto.unlink_locations());
  }

  if(opts.showXAttr) {
    for(auto it = proto.xattrs().begin(); it != proto.xattrs().end(); it++) {
      out[SSTR("xattr." << it->first)] = it->second;
    }
  }

  if(opts.showSTime) {
    out["stime"] = Printing::timespecToTimestamp(Printing::parseTimespec(proto.stime()));
  }
}

//------------------------------------------------------------------------------
//! Print everything known about a FileMD
//------------------------------------------------------------------------------
void OutputSink::print(const eos::ns::FileMdProto &proto, const FilePrintingOptions &opts) {
  std::map<std::string, std::string> out;
  populateMetadata(proto, opts, out);
  print(out);
}

//------------------------------------------------------------------------------
//! Print everything known about a FileMD, including full path if available
//------------------------------------------------------------------------------
void OutputSink::print(const eos::ns::FileMdProto &proto, const FilePrintingOptions &opts, FileScanner::Item &item) {
  std::map<std::string, std::string> out;
  populateMetadata(proto, opts, out);

  std::string fullPath = populateFullPath(proto, item);
  if(!fullPath.empty()) {
    out["path"] = fullPath;
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

    mOut << Printing::escapeNonPrintable(it->first) << "=" << Printing::escapeNonPrintable(it->second);
  }

  mOut << std::endl;
}

//------------------------------------------------------------------------------
// Print interface, single string implementation
//------------------------------------------------------------------------------
void StreamSink::print(const std::string &out) {
  mOut << Printing::escapeNonPrintable(out) << std::endl;
}

//------------------------------------------------------------------------------
// Debug output
//------------------------------------------------------------------------------
void StreamSink::err(const std::string &str) {
  mErr << Printing::escapeNonPrintable(str) << std::endl;
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
// Print interface, single string implementation
//------------------------------------------------------------------------------
void JsonStreamSink::print(const std::string &out) {
  mOut << out << std::endl;
}

//------------------------------------------------------------------------------
// Debug output
//------------------------------------------------------------------------------
void JsonStreamSink::err(const std::string &str) {
  mErr << str << std::endl;
}

EOSNSNAMESPACE_END
