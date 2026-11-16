//------------------------------------------------------------------------------
//! @file DocumentIndex.cc
//! @brief Document suffix gate and text extraction for the Qdrant index
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
#include <zlib.h>
#include <algorithm>
#include <cctype>
#include <cstring>

EOSCOMMONNAMESPACE_BEGIN

namespace {

constexpr const char* kSuffixes[] = {
  "pdf", "docx", "doc", "odt", "rtf", "pptx", "ppt", "odp",
  "md", "markdown", "txt", "text", "tex", "latex", "ltx",
  "html", "htm", "rst"
};

std::string
LowerSuffix(std::string_view path)
{
  const auto slash = path.find_last_of('/');
  if (slash != std::string_view::npos) {
    path.remove_prefix(slash + 1);
  }

  const auto dot = path.find_last_of('.');
  if (dot == std::string_view::npos || dot + 1 >= path.size()) {
    return {};
  }

  std::string suffix(path.substr(dot + 1));
  std::transform(suffix.begin(), suffix.end(), suffix.begin(),
                 [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  return suffix;
}

bool
IsTextSuffix(const std::string& suffix)
{
  return suffix == "md" || suffix == "markdown" || suffix == "txt" ||
         suffix == "text" || suffix == "tex" || suffix == "latex" ||
         suffix == "ltx" || suffix == "html" || suffix == "htm" || suffix == "rst";
}

bool
IsOfficeXmlSuffix(const std::string& suffix)
{
  return suffix == "docx" || suffix == "pptx" || suffix == "odt" || suffix == "odp";
}

bool
StartsWith(std::string_view bytes, std::string_view magic)
{
  return bytes.size() >= magic.size() && bytes.substr(0, magic.size()) == magic;
}

uint16_t
Read16(const unsigned char* p)
{
  return static_cast<uint16_t>(p[0] | (p[1] << 8));
}

uint32_t
Read32(const unsigned char* p)
{
  return static_cast<uint32_t>(p[0] | (p[1] << 8) | (p[2] << 16) | (p[3] << 24));
}

std::string
InflateWith(const unsigned char* data, size_t len, size_t cap, int windowBits)
{
  if (data == nullptr || len == 0 || cap == 0) {
    return {};
  }

  z_stream stream{};
  stream.next_in = const_cast<Bytef*>(reinterpret_cast<const Bytef*>(data));
  stream.avail_in = static_cast<uInt>(std::min<size_t>(len, 0xffffffffu));

  if (inflateInit2(&stream, windowBits) != Z_OK) {
    return {};
  }

  std::string out(std::min(cap, static_cast<size_t>(64 * 1024)), '\0');
  int rc = Z_OK;

  while (rc != Z_STREAM_END && stream.total_out < cap) {
    if (stream.total_out >= out.size()) {
      out.resize(std::min(cap, std::max(out.size() * 2, static_cast<size_t>(1))));
    }

    stream.next_out = reinterpret_cast<Bytef*>(&out[stream.total_out]);
    stream.avail_out = static_cast<uInt>(out.size() - stream.total_out);
    rc = inflate(&stream, Z_NO_FLUSH);

    if (rc != Z_OK && rc != Z_STREAM_END) {
      inflateEnd(&stream);
      return {};
    }
  }

  out.resize(static_cast<size_t>(stream.total_out));
  inflateEnd(&stream);
  return out;
}

std::string
Inflate(const unsigned char* data, size_t len, size_t cap)
{
  // OOXML and PDF Flate streams are raw deflate. Some producers wrap zlib.
  std::string raw = InflateWith(data, len, cap, -MAX_WBITS);

  if (!raw.empty()) {
    return raw;
  }

  return InflateWith(data, len, cap, MAX_WBITS);
}

void
AppendDecodedXml(std::string_view xml, std::string& out)
{
  for (size_t i = 0; i < xml.size();) {
    if (xml[i] == '<') {
      const auto end = xml.find('>', i + 1);
      if (end == std::string_view::npos) {
        break;
      }

      const auto tag = xml.substr(i, end - i);
      if (tag.find("w:p") != std::string_view::npos ||
          tag.find("w:br") != std::string_view::npos ||
          tag.find("/text:p") != std::string_view::npos ||
          tag.find("a:br") != std::string_view::npos) {
        out.push_back('\n');
      }

      i = end + 1;
      continue;
    }

    if (xml[i] == '&') {
      const auto end = xml.find(';', i + 1);
      if (end != std::string_view::npos && end - i < 12) {
        const auto entity = xml.substr(i + 1, end - i - 1);
        if (entity == "amp") {
          out.push_back('&');
        } else if (entity == "lt") {
          out.push_back('<');
        } else if (entity == "gt") {
          out.push_back('>');
        } else if (entity == "quot") {
          out.push_back('"');
        } else if (entity == "apos") {
          out.push_back('\'');
        } else if (!entity.empty() && entity[0] == '#' && entity.size() > 1) {
          unsigned long code = 0;
          try {
            code = entity[1] == 'x' || entity[1] == 'X'
                   ? std::stoul(std::string(entity.substr(2)), nullptr, 16)
                   : std::stoul(std::string(entity.substr(1)), nullptr, 10);
          } catch (...) {
            code = 0;
          }

          if (code >= 32 && code < 127) {
            out.push_back(static_cast<char>(code));
          } else if (code == 10 || code == 9) {
            out.push_back(static_cast<char>(code));
          }
        }

        i = end + 1;
        continue;
      }
    }

    out.push_back(xml[i]);
    ++i;
  }
}

bool
WantZipEntry(const std::string& suffix, std::string_view name)
{
  if (suffix == "docx") {
    return name == "word/document.xml";
  }

  if (suffix == "pptx") {
    return name.compare(0, 16, "ppt/slides/slide") == 0 &&
           name.size() > 4 && name.substr(name.size() - 4) == ".xml";
  }

  if (suffix == "odt" || suffix == "odp") {
    return name == "content.xml";
  }

  return false;
}

std::string
ExtractOfficeXml(const std::string& suffix, std::string_view bytes)
{
  if (bytes.size() < 22 || !StartsWith(bytes, "PK\x03\x04")) {
    return {};
  }

  const auto* raw = reinterpret_cast<const unsigned char*>(bytes.data());
  const size_t n = bytes.size();
  size_t eocd = n;

  for (size_t i = n - 22; i + 22 <= n; --i) {
    if (raw[i] == 'P' && raw[i + 1] == 'K' && raw[i + 2] == 5 && raw[i + 3] == 6) {
      eocd = i;
      break;
    }

    if (i == 0 || (n > 22 && i + 65557 < n)) {
      break;
    }
  }

  if (eocd + 22 > n) {
    return {};
  }

  const uint32_t cdSize = Read32(raw + eocd + 12);
  const uint32_t cdOff = Read32(raw + eocd + 16);

  if (cdOff > n || cdSize > n - cdOff) {
    return {};
  }

  std::string text;
  size_t pos = cdOff;
  const size_t cdEnd = cdOff + cdSize;

  while (pos + 46 <= cdEnd && raw[pos] == 'P' && raw[pos + 1] == 'K' &&
         raw[pos + 2] == 1 && raw[pos + 3] == 2) {
    const uint16_t method = Read16(raw + pos + 10);
    const uint32_t compSize = Read32(raw + pos + 20);
    const uint32_t nameLen = Read16(raw + pos + 28);
    const uint32_t extraLen = Read16(raw + pos + 30);
    const uint32_t commentLen = Read16(raw + pos + 32);
    const uint32_t localOff = Read32(raw + pos + 42);

    if (pos + 46 + nameLen + extraLen + commentLen > cdEnd) {
      break;
    }

    const std::string_view name(reinterpret_cast<const char*>(raw + pos + 46), nameLen);
    pos += 46 + nameLen + extraLen + commentLen;

    if (!WantZipEntry(suffix, name) || localOff + 30 > n) {
      continue;
    }

    const uint32_t localName = Read16(raw + localOff + 26);
    const uint32_t localExtra = Read16(raw + localOff + 28);
    const size_t dataOff = static_cast<size_t>(localOff) + 30 + localName + localExtra;

    if (dataOff > n || compSize > n - dataOff) {
      continue;
    }

    std::string xml;

    if (method == 0) {
      xml.assign(reinterpret_cast<const char*>(raw + dataOff), compSize);
    } else if (method == 8) {
      xml = Inflate(raw + dataOff, compSize, 8 * 1024 * 1024);
    }

    if (!xml.empty()) {
      AppendDecodedXml(xml, text);
      text.push_back('\n');
    }
  }

  return text;
}

std::string
PdfStringAt(std::string_view bytes, size_t& i)
{
  std::string value;

  if (i >= bytes.size() || bytes[i] != '(') {
    return value;
  }

  ++i;
  int depth = 1;

  while (i < bytes.size() && depth > 0) {
    const char c = bytes[i++];

    if (c == '\\' && i < bytes.size()) {
      const char e = bytes[i++];

      switch (e) {
      case 'n': value.push_back('\n'); break;
      case 'r': value.push_back('\r'); break;
      case 't': value.push_back('\t'); break;
      case '(':
      case ')':
      case '\\': value.push_back(e); break;
      default: value.push_back(e); break;
      }

      continue;
    }

    if (c == '(') {
      ++depth;
    } else if (c == ')') {
      --depth;
      if (depth == 0) {
        break;
      }
    }

    value.push_back(c);
  }

  return value;
}

bool
HasLetter(std::string_view text)
{
  return std::any_of(text.begin(), text.end(), [](unsigned char c) {
    return std::isalnum(c) != 0;
  });
}

void
CollectPdfOperators(std::string_view bytes, std::string& out)
{
  for (size_t i = 0; i < bytes.size(); ++i) {
    if (bytes[i] != '(') {
      continue;
    }

    size_t cursor = i;
    const std::string literal = PdfStringAt(bytes, cursor);

    if (!HasLetter(literal)) {
      continue;
    }

    size_t look = cursor;
    while (look < bytes.size() &&
           std::isspace(static_cast<unsigned char>(bytes[look]))) {
      ++look;
    }

    if (look + 1 < bytes.size() && bytes[look] == 'T' &&
        (bytes[look + 1] == 'j' || bytes[look + 1] == 'J')) {
      out.append(literal);
      out.push_back('\n');
      i = look;
    }
  }
}

std::string
ExtractPdf(std::string_view bytes)
{
  if (!StartsWith(bytes, "%PDF")) {
    return {};
  }

  std::string text;
  CollectPdfOperators(bytes, text);
  const auto* raw = reinterpret_cast<const unsigned char*>(bytes.data());

  for (size_t i = 0; i + 6 < bytes.size(); ++i) {
    if (std::memcmp(raw + i, "stream", 6) != 0) {
      continue;
    }

    size_t start = i + 6;
    if (start < bytes.size() && raw[start] == '\r') {
      ++start;
    }
    if (start < bytes.size() && raw[start] == '\n') {
      ++start;
    }

    const auto end = bytes.find("endstream", start);
    if (end == std::string_view::npos || end <= start) {
      continue;
    }

    const bool flate = bytes.substr(i > 80 ? i - 80 : 0, i).find("FlateDecode") !=
                       std::string_view::npos;
    if (!flate) {
      continue;
    }

    const std::string inflated = Inflate(raw + start, end - start, 1024 * 1024);
    if (!inflated.empty()) {
      CollectPdfOperators(inflated, text);
    }

    i = end;
  }

  return text;
}

std::string
ExtractRtf(std::string_view bytes)
{
  if (!StartsWith(bytes, "{\\rtf")) {
    return {};
  }

  std::string text;
  bool skipGroup = false;
  int skipDepth = 0;
  int depth = 0;

  for (size_t i = 0; i < bytes.size(); ++i) {
    const char c = bytes[i];

    if (c == '{') {
      ++depth;
      if (!skipGroup && i + 1 < bytes.size() && bytes[i + 1] == '\\') {
        const auto wordEnd = bytes.find_first_of(" \\}", i + 2);
        const auto word = bytes.substr(i + 2, (wordEnd == std::string_view::npos
                                                ? bytes.size()
                                                : wordEnd) - (i + 2));
        if (word == "fonttbl" || word == "colortbl" || word == "stylesheet" ||
            word == "info" || word == "pict") {
          skipGroup = true;
          skipDepth = depth;
        }
      }
      continue;
    }

    if (c == '}') {
      if (skipGroup && depth == skipDepth) {
        skipGroup = false;
      }
      if (depth > 0) {
        --depth;
      }
      continue;
    }

    if (skipGroup) {
      continue;
    }

    if (c == '\\' && i + 1 < bytes.size()) {
      const char n = bytes[++i];
      if (n == '\\' || n == '{' || n == '}') {
        text.push_back(n);
        continue;
      }

      if (n == '\'') {
        if (i + 2 < bytes.size() && std::isxdigit(static_cast<unsigned char>(bytes[i + 1])) &&
            std::isxdigit(static_cast<unsigned char>(bytes[i + 2]))) {
          const int hi = std::isdigit(static_cast<unsigned char>(bytes[i + 1]))
                         ? bytes[i + 1] - '0'
                         : std::tolower(static_cast<unsigned char>(bytes[i + 1])) - 'a' + 10;
          const int lo = std::isdigit(static_cast<unsigned char>(bytes[i + 2]))
                         ? bytes[i + 2] - '0'
                         : std::tolower(static_cast<unsigned char>(bytes[i + 2])) - 'a' + 10;
          text.push_back(static_cast<char>((hi << 4) | lo));
          i += 2;
        }
        continue;
      }

      const auto start = i;
      while (i < bytes.size() && std::isalpha(static_cast<unsigned char>(bytes[i]))) {
        ++i;
      }

      const auto word = bytes.substr(start, i - start);
      if (i < bytes.size() && bytes[i] == ' ') {
        // The control word's terminating space is not document text.
      } else if (i > 0) {
        --i;
      }

      if (word == "par" || word == "line") {
        text.push_back('\n');
      }
      continue;
    }

    if (c == '\r' || c == '\n') {
      continue;
    }

    text.push_back(c);
  }

  return text;
}

std::string
ExtractPrintable(std::string_view bytes)
{
  std::string text;
  std::string run;
  auto flush = [&]() {
    if (run.size() >= 8 && HasLetter(run)) {
      if (!text.empty()) {
        text.push_back('\n');
      }
      text.append(run);
    }
    run.clear();
  };

  for (unsigned char c : bytes) {
    if (c == 9 || c == 10 || c == 13 || (c >= 32 && c < 127)) {
      run.push_back(static_cast<char>(c == 13 ? 10 : c));
    } else {
      flush();
    }
  }

  flush();

  // OLE documents store a lot of their prose as UTF-16LE.
  std::string wide;
  for (size_t i = 0; i + 1 < bytes.size(); i += 2) {
    const unsigned char lo = static_cast<unsigned char>(bytes[i]);
    const unsigned char hi = static_cast<unsigned char>(bytes[i + 1]);
    if (hi == 0 && (lo == 9 || lo == 10 || lo == 13 || (lo >= 32 && lo < 127))) {
      wide.push_back(static_cast<char>(lo == 13 ? 10 : lo));
    } else {
      if (wide.size() >= 8 && HasLetter(wide)) {
        if (!text.empty()) {
          text.push_back('\n');
        }
        text.append(wide);
      }
      wide.clear();
      if (hi != 0) {
        // The pair was not a character. Step back one byte so odd alignments
        // still find the next run.
        --i;
      }
    }
  }

  if (wide.size() >= 8 && HasLetter(wide)) {
    if (!text.empty()) {
      text.push_back('\n');
    }
    text.append(wide);
  }

  return text;
}

std::string
ExtractPlain(std::string_view bytes)
{
  if (bytes.empty()) {
    return {};
  }

  const size_t probe = std::min<size_t>(bytes.size(), 4096);
  size_t nuls = 0;

  for (size_t i = 0; i < probe; ++i) {
    if (bytes[i] == '\0') {
      ++nuls;
    }
  }

  if (nuls > 0) {
    return {};
  }

  return std::string(bytes);
}

std::string
TrimCopy(std::string text)
{
  auto notSpace = [](unsigned char c) { return std::isspace(c) == 0; };
  text.erase(text.begin(), std::find_if(text.begin(), text.end(), notSpace));
  text.erase(std::find_if(text.rbegin(), text.rend(), notSpace).base(), text.end());
  return text;
}

}  // namespace

bool
IsIndexableDocument(std::string_view path)
{
  const std::string suffix = LowerSuffix(path);
  if (suffix.empty()) {
    return false;
  }

  for (const char* candidate : kSuffixes) {
    if (suffix == candidate) {
      return true;
    }
  }

  return false;
}

std::string
ExtractDocumentText(std::string_view path, std::string_view bytes)
{
  const std::string suffix = LowerSuffix(path);
  if (suffix.empty() || bytes.empty()) {
    return {};
  }

  std::string text;

  if (suffix == "pdf") {
    text = ExtractPdf(bytes);
  } else if (IsOfficeXmlSuffix(suffix)) {
    text = ExtractOfficeXml(suffix, bytes);
  } else if (suffix == "rtf") {
    text = ExtractRtf(bytes);
  } else if (suffix == "doc" || suffix == "ppt") {
    text = ExtractPrintable(bytes);
  } else if (IsTextSuffix(suffix)) {
    text = ExtractPlain(bytes);
  }

  return TrimCopy(std::move(text));
}

std::vector<std::string>
ChunkDocumentText(std::string_view text)
{
  std::vector<std::string> chunks;
  size_t pos = 0;

  while (pos < text.size() && chunks.size() < kQdrantMaxChunks) {
    while (pos < text.size() &&
           std::isspace(static_cast<unsigned char>(text[pos]))) {
      ++pos;
    }

    if (pos >= text.size()) {
      break;
    }

    size_t end = std::min(text.size(), pos + kQdrantChunkChars);
    if (end < text.size()) {
      const auto breakAt = text.rfind('\n', end);
      if (breakAt != std::string_view::npos && breakAt > pos + kQdrantChunkChars / 2) {
        end = breakAt;
      }
    }

    auto piece = TrimCopy(std::string(text.substr(pos, end - pos)));
    if (!piece.empty()) {
      chunks.emplace_back(std::move(piece));
    }

    if (end >= text.size()) {
      break;
    }

    const size_t next = end > kQdrantChunkOverlap ? end - kQdrantChunkOverlap : end;
    pos = next > pos ? next : end;
  }

  return chunks;
}

EOSCOMMONNAMESPACE_END
