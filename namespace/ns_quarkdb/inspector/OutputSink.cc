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
#include "namespace/utils/Checksum.hh"
#include <sstream>
#include <json/json.h>

#define SSTR(message) static_cast<std::ostringstream&>(std::ostringstream().flush() << message).str()

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Convert to octal string
//------------------------------------------------------------------------------
static std::string to_octal_string(uint32_t v)
{
  // Spelled out rather than via ostringstream: constructing a stream per field
  // dominates the cost of a namespace scan.
  char buf[24];
  char* end = buf + sizeof(buf);
  char* p = end;

  do {
    *--p = static_cast<char>('0' + (v & 7));
    v >>= 3;
  } while (v != 0);

  return std::string(p, end - p);
}

//------------------------------------------------------------------------------
//! Prefix an extended attribute key with "xattr."
//------------------------------------------------------------------------------
static std::string xattrPrefixed(const std::string& key)
{
  std::string out;
  out.reserve(6 + key.size());
  out.append("xattr.", 6);
  out.append(key);
  return out;
}

//------------------------------------------------------------------------------
//! Return full path, if possible, otherwise empty
//------------------------------------------------------------------------------
static std::string populateFullPath(const eos::ns::FileMdProto& proto,
                                    FileScanner::Item& item)
{
  item.fullPath.wait();

  if (item.fullPath.hasException()) {
    return std::string();
  }

  std::string fullPath = std::move(item.fullPath).get();

  if (fullPath.empty()) {
    return std::string();
  }

  return SSTR(fullPath << proto.name());
}

//------------------------------------------------------------------------------
//! Return full path, if possible, otherwise empty
//------------------------------------------------------------------------------
static std::string populateFullPath(const eos::ns::ContainerMdProto& proto,
                                    ContainerScanner::Item& item)
{
  item.fullPath.wait();

  if (item.fullPath.hasException()) {
    return std::string();
  }

  std::string fullPath = std::move(item.fullPath).get();

  if (fullPath.empty()) {
    return std::string();
  }

  return fullPath;
}

//------------------------------------------------------------------------------
// Serialize locations vector
//------------------------------------------------------------------------------
template<typename T>
static std::string serializeLocations(const T& vec)
{
  // Two of these per file record; an ostringstream each time is pure overhead.
  std::string out;
  out.reserve(vec.size() * 8);

  for (int i = 0; i < vec.size(); i++) {
    if (i != 0) {
      out.push_back(',');
    }

    out.append(std::to_string(vec[i]));
  }

  return out;
}

//------------------------------------------------------------------------------
//! Populate map with container attributes
//------------------------------------------------------------------------------
static void populateMetadata(const eos::ns::ContainerMdProto& proto,
                             const ContainerPrintingOptions& opts, std::map<std::string, std::string>& out)
{
  if (opts.showId) {
    out["cid"] = std::to_string(proto.id());
  }

  if (opts.showParent) {
    out["parent_id"] = std::to_string(proto.parent_id());
  }

  if (opts.showUid) {
    out["uid"] = std::to_string(proto.uid());
  }

  if (opts.showGid) {
    out["gid"] = std::to_string(proto.gid());
  }

  if (opts.showTreeSize) {
    out["tree_size"] = std::to_string(proto.tree_size());
  }

  if (opts.showMode) {
    out["mode"] = to_octal_string(proto.mode());
  }

  if (opts.showMode) {
    out["flags"] = to_octal_string(proto.flags());
  }

  if (opts.showName) {
    out["name"] = proto.name();
  }

  if (opts.showCTime) {
    out["ctime"] = Printing::timespecToTimestamp(Printing::parseTimespec(
                     proto.ctime()));
  }

  if (opts.showMTime) {
    out["mtime"] = Printing::timespecToTimestamp(Printing::parseTimespec(
                     proto.mtime()));
  }

  if (opts.showSTime) {
    out["stime"] = Printing::timespecToTimestamp(Printing::parseTimespec(
                     proto.stime()));
  }

  if (opts.showXAttr) {
    for (auto it = proto.xattrs().begin(); it != proto.xattrs().end(); it++) {
      // "xattr." + key by concatenation: SSTR builds an ostringstream, and
      // this runs once per extended attribute of every record.
      out[xattrPrefixed(it->first)] = it->second;
    }
  }
}

//------------------------------------------------------------------------------
//! Print everything known about a ContainerMD
//------------------------------------------------------------------------------
void OutputSink::print(const eos::ns::ContainerMdProto& proto,
                       const ContainerPrintingOptions& opts)
{
  std::map<std::string, std::string> out;
  populateMetadata(proto, opts, out);
  print(out);
}

//----------------------------------------------------------------------------
//! Print everything known about a ContainerMD -- custom path
//----------------------------------------------------------------------------
void OutputSink::printWithCustomPath(const eos::ns::ContainerMdProto& proto,
                                     const ContainerPrintingOptions& opts,
                                     const std::string& customPath)
{
  std::map<std::string, std::string> out;
  out["path"] = customPath;
  populateMetadata(proto, opts, out);
  print(out);
}

//------------------------------------------------------------------------------
// Get count as string
//------------------------------------------------------------------------------
static std::string countAsString(folly::Future<uint64_t>& fut)
{
  fut.wait();

  if (fut.hasException()) {
    return "N/A";
  }

  uint64_t val = std::move(fut).get();
  fut = val;
  return std::to_string(val);
}

//------------------------------------------------------------------------------
//! Print everything known about a ContainerMD, including full path if available
//------------------------------------------------------------------------------
void OutputSink::print(const eos::ns::ContainerMdProto& proto,
                       const ContainerPrintingOptions& opts, ContainerScanner::Item& item,
                       bool showCounts)
{
  std::map<std::string, std::string> out;
  populateMetadata(proto, opts, out);
  std::string fullPath = populateFullPath(proto, item);

  if (!fullPath.empty()) {
    out["path"] = fullPath;
  }

  if (showCounts) {
    out["file-count"] = countAsString(item.fileCount);
    out["container-count"] = countAsString(item.containerCount);
  }

  print(out);
}

//------------------------------------------------------------------------------
//! Populate map with file attributes
//------------------------------------------------------------------------------
static void populateMetadata(const eos::ns::FileMdProto& proto,
                             const FilePrintingOptions& opts, std::map<std::string, std::string>& out)
{
  if (opts.showId) {
    out["fid"] = std::to_string(proto.id());
  }

  if (opts.showContId) {
    out["pid"] = std::to_string(proto.cont_id());
  }

  if (opts.showUid) {
    out["uid"] = std::to_string(proto.uid());
  }

  if (opts.showGid) {
    out["gid"] = std::to_string(proto.gid());
  }

  if (opts.showSize) {
    out["size"] = std::to_string(proto.size());
  }

  if (opts.showLayoutId) {
    out["layout_id"] = std::to_string(proto.layout_id());
  }

  if (opts.showFlags) {
    out["flags"] = to_octal_string(proto.flags());
  }

  if (opts.showName) {
    out["name"] = proto.name();
  }

  if (opts.showLinkName) {
    out["link_name"] = proto.link_name();
  }

  if (opts.showCTime) {
    out["ctime"] = Printing::timespecToTimestamp(Printing::parseTimespec(
                     proto.ctime()));
  }

  if (opts.showMTime) {
    out["mtime"] = Printing::timespecToTimestamp(Printing::parseTimespec(
                     proto.mtime()));
  }

  if (opts.showChecksum) {
    std::string xs;
    eos::appendChecksumOnStringProtobuf(proto, xs);
    out["xs"] = xs;
  }

  if (opts.showLocations) {
    out["locations"] = serializeLocations(proto.locations());
  }

  if (opts.showLocations) {
    out["unlink_locations"] = serializeLocations(proto.unlink_locations());
  }

  if (opts.showXAttr) {
    for (auto it = proto.xattrs().begin(); it != proto.xattrs().end(); it++) {
      // "xattr." + key by concatenation: SSTR builds an ostringstream, and
      // this runs once per extended attribute of every record.
      out[xattrPrefixed(it->first)] = it->second;
    }
  }

  if (opts.showSTime) {
    out["stime"] = Printing::timespecToTimestamp(Printing::parseTimespec(
                     proto.stime()));
  }

  if(opts.showATime) {
    out["atime"] = Printing::timespecToTimestamp(Printing::parseTimespec(
        proto.atime()));
  }
}

//------------------------------------------------------------------------------
//! Print everything known about a FileMD
//------------------------------------------------------------------------------
void OutputSink::print(const eos::ns::FileMdProto& proto,
                       const FilePrintingOptions& opts)
{
  std::map<std::string, std::string> out;
  populateMetadata(proto, opts, out);
  print(out);
}

//----------------------------------------------------------------------------
// Print everything known about a FileMD -- custom path
//----------------------------------------------------------------------------
void OutputSink::printWithCustomPath(const eos::ns::FileMdProto& proto,
                                     const FilePrintingOptions& opts,
                                     const std::string& customPath)
{
  std::map<std::string, std::string> out;
  out["path"] = customPath;
  populateMetadata(proto, opts, out);
  print(out);
}

//----------------------------------------------------------------------------
//! Print everything known about a FileMD -- Additional fields to add
//----------------------------------------------------------------------------
void OutputSink::printWithAdditionalFields(const eos::ns::FileMdProto& proto,
    const FilePrintingOptions& opts,
    std::map<std::string, std::string>& extension)
{
  populateMetadata(proto, opts, extension);
  print(extension);
}

//------------------------------------------------------------------------------
//! Print everything known about a FileMD, including full path if available
//------------------------------------------------------------------------------
void OutputSink::print(const eos::ns::FileMdProto& proto,
                       const FilePrintingOptions& opts, FileScanner::Item& item)
{
  std::map<std::string, std::string> out;
  populateMetadata(proto, opts, out);
  std::string fullPath = populateFullPath(proto, item);

  if (!fullPath.empty()) {
    out["path"] = fullPath;
  }

  print(out);
}

//------------------------------------------------------------------------------
// Class StreamSink
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
StreamSink::StreamSink(std::ostream& out, std::ostream& err)
  : OutputSink(out, err)
{}

//------------------------------------------------------------------------------
// Print implementation for map
//------------------------------------------------------------------------------
void StreamSink::print(const std::map<std::string, std::string>& line)
{
  for (auto it = line.begin(); it != line.end(); it++) {
    if (it != line.begin()) {
      mOut << " ";
    }

    mOut << Printing::escapeNonPrintable(it->first) << "=" <<
         Printing::escapeNonPrintable(it->second);
  }

  // '\n' rather than std::endl: flushing per record turns a streaming scan
  // into one write() syscall per namespace entry.
  mOut << '\n';
}


//------------------------------------------------------------------------------
// Class JsonStreamSink
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
JsonStreamSink::JsonStreamSink(std::ostream& out, std::ostream& err)
  : OutputSink(out, err), mFirst(true)
{
  mOut << "[" << std::endl;
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
JsonStreamSink::~JsonStreamSink()
{
  mOut << "]" << std::endl;
}

//------------------------------------------------------------------------------
// Print implementation
//------------------------------------------------------------------------------
void JsonStreamSink::print(const std::map<std::string, std::string>& line)
{
  if (!mFirst) {
    mOut << ",\n";
  }

  mFirst = false;
  Json::Value json;

  for (auto it = line.begin(); it != line.end(); it++) {
    json[it->first] = it->second;
  }

  mOut << json;
}


//------------------------------------------------------------------------------
// Class JsonLinedStreamSink
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
JsonLinedStreamSink::JsonLinedStreamSink(std::ostream& out, std::ostream& err)
  : OutputSink(out, err)
{
  mBuilder["indentation"] = "";  // or whatever you like
  mWriter.reset(mBuilder.newStreamWriter());
  mBuffer.reserve(4096);
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
JsonLinedStreamSink::~JsonLinedStreamSink()
{
  mOut.flush();
}

//------------------------------------------------------------------------------
// Append str to mBuffer as a quoted json string.
//
// Most fields are printable ASCII and can be copied between quotes as they
// are. The rest -- notably binary xattrs such as sys.fusex.state, which the
// writer renders as \uXXXX escapes -- are handed to jsoncpp's own quoting
// routine, the very one the writer calls, so the bytes cannot diverge.
//
// Returns false if str contains a NUL, which valueToQuotedString cannot see
// past; the caller then falls back to building a Json::Value.
//------------------------------------------------------------------------------
bool JsonLinedStreamSink::appendQuoted(const std::string& str)
{
  bool verbatim = true;

  for (unsigned char c : str) {
    if (c == '\0') {
      return false;
    }

    if (c < 0x20 || c > 0x7e || c == '"' || c == '\\') {
      verbatim = false;
    }
  }

  if (verbatim) {
    mBuffer.push_back('"');
    mBuffer.append(str);
    mBuffer.push_back('"');
    return true;
  }

  mBuffer.append(Json::valueToQuotedString(str.c_str()));
  return true;
}

//------------------------------------------------------------------------------
// Serialize a line into mBuffer, compact
//------------------------------------------------------------------------------
bool JsonLinedStreamSink::fastSerialize(const std::map<std::string, std::string>&
                                        line)
{
  // An empty Json::Value is null, not an empty object, so the writer emits
  // "null" for an empty line. Leave that case to it rather than print "{}".
  if (line.empty()) {
    return false;
  }

  // std::map iterates in key order, which is the order jsoncpp's object
  // members are written in too -- the two paths agree field by field.
  mBuffer.clear();
  mBuffer.push_back('{');

  for (auto it = line.begin(); it != line.end(); it++) {
    if (it != line.begin()) {
      mBuffer.push_back(',');
    }

    if (!appendQuoted(it->first)) {
      return false;
    }

    mBuffer.push_back(':');

    if (!appendQuoted(it->second)) {
      return false;
    }
  }

  mBuffer.append("}\n");
  return true;
}

//------------------------------------------------------------------------------
// Print implementation
//------------------------------------------------------------------------------
void JsonLinedStreamSink::print(const std::map<std::string, std::string>& line)
{
  // Building a Json::Value and walking it with the writer costs more than the
  // namespace lookup that produced the record: it copies every key and value
  // into the Value tree before the writer walks it back out. Serialize
  // straight into a buffer instead, keeping the Value path only for records
  // with an embedded NUL.
  if (fastSerialize(line)) {
    mOut.write(mBuffer.data(), mBuffer.size());
    return;
  }

  Json::Value json;

  for (auto it = line.begin(); it != line.end(); it++) {
    json[it->first] = it->second;
  }

  print(json);
}

//------------------------------------------------------------------------------
// Print JsonValue implementation
//------------------------------------------------------------------------------
void JsonLinedStreamSink::print(const Json::Value& jsonObj)
{
  mWriter->write(jsonObj, &mOut);
  // '\n' rather than std::endl: flushing per record turns a streaming scan
  // into one write() syscall per namespace entry.
  mOut << '\n';
}

EOSNSNAMESPACE_END
