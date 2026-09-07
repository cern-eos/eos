/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2026 CERN/Switzerland                                  *
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

//------------------------------------------------------------------------------
//! @brief Output sink tests: json array vs jsonlines formats
//------------------------------------------------------------------------------

#include "namespace/ns_quarkdb/inspector/OutputSink.hh"
#include "namespace/ns_quarkdb/inspector/Printing.hh"

#include <gtest/gtest.h>
#include <json/json.h>
#include <sstream>
#include <memory>
#include <vector>

using namespace eos;

namespace
{
//------------------------------------------------------------------------------
// Split a string into lines, dropping a trailing empty one
//------------------------------------------------------------------------------
std::vector<std::string> splitLines(const std::string& out)
{
  std::vector<std::string> lines;
  std::istringstream ss(out);
  std::string line;

  while (std::getline(ss, line)) {
    lines.push_back(line);
  }

  return lines;
}

//------------------------------------------------------------------------------
// Parse a JSON document, failing the test on malformed input
//------------------------------------------------------------------------------
Json::Value parseJson(const std::string& doc)
{
  Json::Value parsed;
  std::istringstream ss(doc);
  ss >> parsed;
  return parsed;
}
}

TEST(JsonLinedStreamSink, OneCompactObjectPerLine)
{
  std::ostringstream out, err;
  JsonLinedStreamSink sink(out, err);
  sink.print(std::map<std::string, std::string> {
    {"fid", "10"}, {"path", "/eos/user/g/gd/a file, with {braces}"}});
  sink.print(std::map<std::string, std::string> {
    {"cid", "3"}, {"path", "/eos/pilot/"}});
  std::vector<std::string> lines = splitLines(out.str());
  ASSERT_EQ(lines.size(), 2u);

  for (const std::string& line : lines) {
    // every line is a self-contained compact object: no array framing
    ASSERT_FALSE(line.empty());
    ASSERT_EQ(line.front(), '{');
    ASSERT_EQ(line.back(), '}');
  }

  Json::Value first = parseJson(lines[0]);
  ASSERT_EQ(first["fid"].asString(), "10");
  ASSERT_EQ(first["path"].asString(), "/eos/user/g/gd/a file, with {braces}");
  Json::Value second = parseJson(lines[1]);
  ASSERT_EQ(second["cid"].asString(), "3");
}

//------------------------------------------------------------------------------
// The sink serializes plain-ASCII records directly instead of going through
// jsoncpp. That is only safe if the two agree byte for byte, so pin it: write
// each record through the sink, and compare against what the jsoncpp writer
// produces for the same map.
//------------------------------------------------------------------------------
TEST(JsonLinedStreamSink, FastPathMatchesJsoncppByteForByte)
{
  const std::vector<std::map<std::string, std::string>> records = {
    // the ordinary case: plain ASCII, takes the fast path
    {{"fid", "10"}, {"name", "doc.txt"}, {"path", "/eos/user/g/gd/doc.txt"}},
    // keys are emitted in map order, which must match jsoncpp's member order
    {{"zzz", "last"}, {"aaa", "first"}, {"mmm", "middle"}},
    // empty values, and an empty map
    {{"link_name", ""}, {"locations", ""}},
    {},
    // everything below must fall back to jsoncpp rather than be emitted raw
    {{"path", "/eos/user/g/gd/quote\"inside"}},
    {{"path", "/eos/user/g/gd/back\\slash"}},
    {{"path", "/eos/user/g/gd/tab\there"}},
    {{"path", "/eos/user/g/gd/newline\nhere"}},
    {{"path", std::string("/eos/user/g/gd/nul") + '\0' + "byte"}},
    {{"path", "/eos/user/g/gd/accentué"}},
    {{"path", "/eos/user/g/gd/日本語"}},
    {{"path", "/eos/user/g/gd/emoji\xf0\x9f\x93\x81"}},
    // invalid utf-8: a filename is an arbitrary byte string on disk
    {{"path", "/eos/user/g/gd/invalid\xff\xfe"}},
    // escaping needed in the key, not the value
    {{"xattr.user.weird\"key", "value"}},
    // the real-world case: sys.fusex.state is a binary xattr carried by most
    // files on a fuse-backed instance, and is what actually exercises the
    // escaping path during a namespace scan
    {{"fid", "42"}, {"xattr.sys.fusex.state", std::string("\x01\x02\x80\xfe\xff\x7f\x00\x03", 8)}},
    {{"fid", "43"}, {"xattr.sys.fusex.state", std::string("\xc3\xa9\xe6\x97\xa5\xf0\x9f\x93\x81\x01\x1f", 11)}},
    // every byte value 1..255, to pin the escaping across the whole range
    // (0 is covered by the NUL case above, which takes the fallback)
    {{"all_bytes", [] {
        std::string s;
        for (int i = 1; i < 256; i++) {
          s.push_back(static_cast<char>(i));
        }
        return s;
      }()}},
  };

  for (const std::map<std::string, std::string>& record : records) {
    std::ostringstream out, err;
    {
      JsonLinedStreamSink sink(out, err);
      sink.print(record);
    }
    // reference: what the jsoncpp writer emits for the same object
    Json::Value expected;

    for (auto it = record.begin(); it != record.end(); it++) {
      expected[it->first] = it->second;
    }

    Json::StreamWriterBuilder builder;
    builder["indentation"] = "";
    std::ostringstream reference;
    std::unique_ptr<Json::StreamWriter> writer(builder.newStreamWriter());
    writer->write(expected, &reference);
    ASSERT_EQ(out.str(), reference.str() + "\n")
        << "fast path diverged from jsoncpp for record starting with key '"
        << (record.empty() ? std::string("<empty>") : record.begin()->first)
        << "'";
  }
}

//------------------------------------------------------------------------------
// Printing must not flush per record -- that is one write() syscall per
// namespace entry. Nothing should reach the stream's consumer until the sink
// is destroyed or the buffer fills.
//------------------------------------------------------------------------------
TEST(JsonLinedStreamSink, FlushesOnDestructionNotPerRecord)
{
  std::ostringstream out, err;
  {
    JsonLinedStreamSink sink(out, err);
    sink.print(std::map<std::string, std::string> {{"fid", "10"}});
    sink.print(std::map<std::string, std::string> {{"fid", "11"}});
    // ostringstream has no intermediate buffer to hold back, so this only
    // pins that the records are complete and newline-separated
    ASSERT_EQ(splitLines(out.str()).size(), 2u);
  }
  ASSERT_EQ(out.str(), "{\"fid\":\"10\"}\n{\"fid\":\"11\"}\n");
}

//------------------------------------------------------------------------------
// timespecToTimestamp is called four times per namespace record, so it formats
// by hand instead of via ostringstream. Pin it against the stream formulation
// it replaced.
//------------------------------------------------------------------------------
TEST(Printing, TimespecToTimestampMatchesStreamFormatting)
{
  const std::vector<std::pair<long, long>> cases = {
    {0, 0}, {1, 0}, {0, 1}, {1447252711, 38412918},
    {1700000000, 999999999}, {9, 000000001},
    {2147483647, 123}, {4102444800, 0},
    {-1, 0}, {0, -1}, {-1447252711, -38412918},
  };

  for (const std::pair<long, long>& c : cases) {
    struct timespec ts;
    ts.tv_sec = c.first;
    ts.tv_nsec = c.second;
    std::ostringstream reference;
    reference << ts.tv_sec << "." << ts.tv_nsec;
    ASSERT_EQ(eos::Printing::timespecToTimestamp(ts), reference.str())
        << "for {" << c.first << ", " << c.second << "}";
  }
}

TEST(JsonStreamSink, ProducesOneJsonArray)
{
  std::ostringstream out, err;
  {
    // scoped: the closing bracket is written by the destructor
    JsonStreamSink sink(out, err);
    sink.print(std::map<std::string, std::string> {{"fid", "10"}});
    sink.print(std::map<std::string, std::string> {{"fid", "11"}});
  }
  Json::Value parsed = parseJson(out.str());
  ASSERT_TRUE(parsed.isArray());
  ASSERT_EQ(parsed.size(), 2u);
  ASSERT_EQ(parsed[0]["fid"].asString(), "10");
  ASSERT_EQ(parsed[1]["fid"].asString(), "11");
  // spans multiple lines: not parseable line by line, unlike jsonlines
  ASSERT_GT(splitLines(out.str()).size(), 2u);
}
