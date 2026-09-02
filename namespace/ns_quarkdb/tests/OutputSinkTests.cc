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

#include <gtest/gtest.h>
#include <json/json.h>
#include <sstream>

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
