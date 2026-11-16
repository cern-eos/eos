//------------------------------------------------------------------------------
// File: DocumentIndexTests.cc
//------------------------------------------------------------------------------

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

#include "common/qdrant/DocumentIndex.hh"
#include "gtest/gtest.h"
#include <string>

namespace {

void
Append16(std::string& out, uint16_t value)
{
  out.push_back(static_cast<char>(value & 0xff));
  out.push_back(static_cast<char>((value >> 8) & 0xff));
}

void
Append32(std::string& out, uint32_t value)
{
  out.push_back(static_cast<char>(value & 0xff));
  out.push_back(static_cast<char>((value >> 8) & 0xff));
  out.push_back(static_cast<char>((value >> 16) & 0xff));
  out.push_back(static_cast<char>((value >> 24) & 0xff));
}

std::string
StoredZip(const std::string& name, const std::string& data)
{
  std::string local;
  local.append("PK\x03\x04", 4);
  Append16(local, 20);
  Append16(local, 0);
  Append16(local, 0);
  Append16(local, 0);
  Append16(local, 0);
  Append32(local, 0);
  Append32(local, static_cast<uint32_t>(data.size()));
  Append32(local, static_cast<uint32_t>(data.size()));
  Append16(local, static_cast<uint16_t>(name.size()));
  Append16(local, 0);
  local += name;
  local += data;

  std::string central;
  central.append("PK\x01\x02", 4);
  Append16(central, 20);
  Append16(central, 20);
  Append16(central, 0);
  Append16(central, 0);
  Append16(central, 0);
  Append16(central, 0);
  Append32(central, 0);
  Append32(central, static_cast<uint32_t>(data.size()));
  Append32(central, static_cast<uint32_t>(data.size()));
  Append16(central, static_cast<uint16_t>(name.size()));
  Append16(central, 0);
  Append16(central, 0);
  Append16(central, 0);
  Append16(central, 0);
  Append32(central, 0);
  Append32(central, 0);
  central += name;

  std::string eocd;
  eocd.append("PK\x05\x06", 4);
  Append16(eocd, 0);
  Append16(eocd, 0);
  Append16(eocd, 1);
  Append16(eocd, 1);
  Append32(eocd, static_cast<uint32_t>(central.size()));
  Append32(eocd, static_cast<uint32_t>(local.size()));
  Append16(eocd, 0);
  return local + central + eocd;
}

}

TEST(DocumentIndex, SuffixAllowList)
{
  EXPECT_TRUE(eos::common::IsIndexableDocument("/eos/project/Notes.PDF"));
  EXPECT_TRUE(eos::common::IsIndexableDocument("draft.DOCX"));
  EXPECT_TRUE(eos::common::IsIndexableDocument("/eos/user/a/note.md"));
  EXPECT_TRUE(eos::common::IsIndexableDocument("paper.TeX"));
  EXPECT_TRUE(eos::common::IsIndexableDocument("slides.pptx"));
  EXPECT_FALSE(eos::common::IsIndexableDocument("/eos/data/run.root"));
  EXPECT_FALSE(eos::common::IsIndexableDocument("archive.tar.gz"));
  EXPECT_FALSE(eos::common::IsIndexableDocument("Makefile"));
  EXPECT_FALSE(eos::common::IsIndexableDocument("table.csv"));
}

TEST(DocumentIndex, PlainTextAndBinaryRejection)
{
  EXPECT_EQ(eos::common::ExtractDocumentText("note.md", "# Title\n\nbody"),
            "# Title\n\nbody");
  EXPECT_EQ(eos::common::ExtractDocumentText("empty.txt", ""), "");
  const std::string binary("abc\0def", 7);
  EXPECT_TRUE(eos::common::ExtractDocumentText("notes.txt", binary).empty());
}

TEST(DocumentIndex, PdfLiteral)
{
  const std::string pdf = "%PDF-1.4\n1 0 obj\n<< /Length 12 >>\nstream\n(Hello) Tj\n"
                          "endstream\nendobj\n";
  const std::string text = eos::common::ExtractDocumentText("paper.pdf", pdf);
  EXPECT_NE(text.find("Hello"), std::string::npos);
  EXPECT_TRUE(eos::common::ExtractDocumentText("paper.pdf", "not a pdf").empty());
}

TEST(DocumentIndex, DocxXml)
{
  const std::string xml = "<w:p><w:t>Hello &amp; EOS</w:t></w:p>";
  const std::string zip = StoredZip("word/document.xml", xml);
  const std::string text = eos::common::ExtractDocumentText("letter.docx", zip);
  EXPECT_NE(text.find("Hello & EOS"), std::string::npos);
  EXPECT_TRUE(eos::common::ExtractDocumentText("letter.docx", "not a zip").empty());
}

TEST(DocumentIndex, Rtf)
{
  const std::string text = eos::common::ExtractDocumentText(
      "note.rtf", "{\\rtf1 Hello \\par world}");
  EXPECT_NE(text.find("Hello"), std::string::npos);
  EXPECT_NE(text.find("world"), std::string::npos);
}

TEST(DocumentIndex, ChunksOverlapAndCap)
{
  std::string text(5000, 'a');
  text.replace(1200, 1, "\n");
  const auto chunks = eos::common::ChunkDocumentText(text);
  EXPECT_FALSE(chunks.empty());
  EXPECT_LE(chunks.size(), eos::common::kQdrantMaxChunks);
  EXPECT_LE(chunks.front().size(), eos::common::kQdrantChunkChars);
  EXPECT_TRUE(eos::common::ChunkDocumentText("   \n\t").empty());
}
