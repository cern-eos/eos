// ----------------------------------------------------------------------
// File: StringConversion.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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

#include "StringConversion.hh"
#include "common/Logging.hh"
#include "common/Timing.hh"
#include <XrdOuc/XrdOucTokenizer.hh>
#include "curl/curl.h"
#include <pthread.h>
#include <regex>

EOSCOMMONNAMESPACE_BEGIN

static std::atomic<int> sCounter {0};

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
CurlGlobalInitializer::CurlGlobalInitializer()
{
  if (sCounter++ == 0) {
    curl_global_init(CURL_GLOBAL_DEFAULT);
  }
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
CurlGlobalInitializer::~CurlGlobalInitializer()
{
  if (--sCounter == 0) {
    curl_global_cleanup();
  }
}

char StringConversion::pAscii2HexLkup[256];
char StringConversion::pHex2AsciiLkup[16];

//------------------------------------------------------------------------------
// Tokenize a string
//------------------------------------------------------------------------------
void
StringConversion::Tokenize(const std::string& str,
                           std::vector<std::string>& tokens,
                           const std::string& delimiters)
{
  // Skip delimiters at the beginning
  std::string::size_type lastPos = str.find_first_not_of(delimiters, 0);
  // Find first "non-delimiter"
  std::string::size_type pos = str.find_first_of(delimiters, lastPos);

  while (std::string::npos != pos || std::string::npos != lastPos) {
    // Found a token, add it to the vector.
    tokens.push_back(str.substr(lastPos, pos - lastPos));
    // Skip delimiters.  Note the "not_of"
    lastPos = str.find_first_not_of(delimiters, pos);
    // Find next "non-delimiter"
    pos = str.find_first_of(delimiters, lastPos);
  }
}

//------------------------------------------------------------------------------
// Tokenize a string accepting also empty members e.g. a||b returns 3 fields
//------------------------------------------------------------------------------
void
StringConversion::EmptyTokenize(const std::string& str,
                                std::vector<std::string>& tokens,
                                const std::string& delimiters)
{
  // Skip delimiters at beginning.
  std::string::size_type lastPos = str.find_first_not_of(delimiters, 0);
  // Find first "non-delimiter".
  std::string::size_type pos = str.find_first_of(delimiters, lastPos);

  while (std::string::npos != pos || std::string::npos != lastPos) {
    // Found a token, add it to the vector.
    tokens.push_back(str.substr(lastPos, pos - lastPos));
    // Skip delimiters.  Note the "not_of"
    lastPos = str.find_first_of(delimiters, pos);

    if (lastPos != std::string::npos) {
      lastPos++;
    }

    // Find next "non-delimiter"
    pos = str.find_first_of(delimiters, lastPos);
  }
}

//------------------------------------------------------------------------------
// Convert a long long value into time s,m,h,d  scale
//------------------------------------------------------------------------------
const char*
StringConversion::GetReadableAgeString(XrdOucString& sizestring,
                                       unsigned long long age)
{
  char formsize[1024];

  if (age > 86400) {
    sprintf(formsize, "%llud", age / 86400);
  } else if (age > 3600) {
    sprintf(formsize, "%lluh", age / 3600);
  } else if (age > 60) {
    sprintf(formsize, "%llum", age / 60);
  } else {
    sprintf(formsize, "%llus", age);
  }

  sizestring = formsize;
  return sizestring.c_str();
}




std::string
StringConversion::GetFixedDouble(double value, size_t width, size_t precision)
{
  char dline[1024];
  char format[16];

  if (precision == 1) {
    snprintf(dline, sizeof(dline), "%.01f", value);
  } else if (precision == 2) {
    snprintf(dline, sizeof(dline), "%.02f", value);
  } else if (precision == 3) {
    snprintf(dline, sizeof(dline), "%.03f", value);
  } else if (precision == 4) {
    snprintf(dline, sizeof(dline), "%.04f", value);
  } else {
    snprintf(dline, sizeof(dline), "%f", value);
  }

  char wline[1024];
  snprintf(format, sizeof(format), "%%-%lus", width);
  snprintf(wline, sizeof(wline), format, dline);
  return std::string(wline);
}


//------------------------------------------------------------------------------
// Convert a long long value into K,M,G,T,P,E byte scale
//------------------------------------------------------------------------------
const char*
StringConversion::GetReadableSizeString(XrdOucString& sizestring,
                                        unsigned long long insize,
                                        const char* unit)
{
  char formsize[1024];

  if (insize >= 10000) {
    if (insize >= (1000 * 1000)) {
      if (insize >= (1000ll * 1000ll * 1000ll)) {
        if (insize >= (1000ll * 1000ll * 1000ll * 1000ll)) {
          if (insize >= (1000ll * 1000ll * 1000ll * 1000ll * 1000ll)) {
            if (insize >= (1000ll * 1000ll * 1000ll * 1000ll * 1000ll * 1000ll)) {
              // EB
              sprintf(formsize, "%.02f E%s",
                      insize * 1.0 / (1000ll * 1000ll * 1000ll * 1000ll * 1000ll * 1000ll), unit);
            } else {
              // PB
              sprintf(formsize, "%.02f P%s",
                      insize * 1.0 / (1000ll * 1000ll * 1000ll * 1000ll * 1000ll), unit);
            }
          } else {
            // TB
            sprintf(formsize, "%.02f T%s",
                    insize * 1.0 / (1000ll * 1000ll * 1000ll * 1000ll), unit);
          }
        } else {
          // GB
          sprintf(formsize, "%.02f G%s", insize * 1.0 / (1000ll * 1000ll * 1000ll), unit);
        }
      } else {
        // MB
        sprintf(formsize, "%.02f M%s", insize * 1.0 / (1000 * 1000), unit);
      }
    } else {
      sprintf(formsize, "%.02f k%s", insize * 1.0 / (1000), unit);
    }
  } else {
    if (strlen(unit)) {
      sprintf(formsize, "%llu %s", insize, unit);
    } else {
      sprintf(formsize, "%llu", insize);
    }
  }

  sizestring = formsize;
  return sizestring.c_str();
}

//------------------------------------------------------------------------------
// Convert a string containing binary characters to its hex representation
//------------------------------------------------------------------------------
std::string
StringConversion::string_to_hex(const std::string& input)
{
  static const char* const lut = "0123456789ABCDEF";
  size_t len = input.length();
  std::string output;
  output.reserve(2 * len);

  for (size_t i = 0; i < len; ++i) {
    const unsigned char c = input[i];
    output.push_back(lut[c >> 4]);
    output.push_back(lut[c & 15]);
  }

  return output;
}

//------------------------------------------------------------------------------
// Convert a char to its hex representation
//------------------------------------------------------------------------------
std::string
StringConversion::char_to_hex(const char input)
{
  static const char* const lut = "0123456789abcdef";
  std::string output;
  output.resize(2);
  const unsigned char c = input;
  output[0] = lut[c >> 4];
  output[1] = lut[c & 15];
  return output;
}

//------------------------------------------------------------------------------
// Convert binary string given as a char* and length to hex string representation
//------------------------------------------------------------------------------
std::string
StringConversion::BinData2HexString(const char* buf, const size_t buf_len,
                                    const size_t nominal_len,
                                    const char separator)
{
  std::string out;

  if (buf_len == 0) {
    return out;
  }

  char hb[4];

  for (size_t i = 0; i < nominal_len; ++i) {
    unsigned char target = 0x00;

    if (i < buf_len) {
      target = buf[i];
    }

    if ((separator != 0x00) && (i != (nominal_len - 1))) {
      sprintf(hb, "%02x%c", target, separator);
    } else {
      sprintf(hb, "%02x", target);
    }

    out += hb;
  }

  return out;
}

//------------------------------------------------------------------------------
// Convert checksum hex representation to binary string
//------------------------------------------------------------------------------
std::unique_ptr<char[]>
StringConversion::Hex2BinDataChar(const std::string& shex, size_t& out_size,
                                  const size_t nominal_len)
{
  out_size = 0;
  std::unique_ptr<char[]> buf {new char[nominal_len]};

  if ((buf == nullptr) || shex.empty()) {
    return nullptr;
  }

  memset(buf.get(), 0, nominal_len);
  char hex[3];

  for (size_t i = 0;
       ((i < shex.length() - 1) && (i / 2 < nominal_len)); i += 2) {
    hex[0] = shex.at(i);
    hex[1] = shex.at(i + 1);
    hex[2] = '\0';
    buf.get()[i / 2] = std::stol(hex, 0, 16);
    ++out_size;
  }

  return buf;
}

//----------------------------------------------------------------------------
//! Get size from the given string, return true if parsing was successful,
//! false otherwise
//----------------------------------------------------------------------------
bool
StringConversion::GetSizeFromString(const std::string& sizestring,
                                    uint64_t& out)
{
  out = GetSizeFromString(sizestring.c_str());
  return (errno == 0);
}

//------------------------------------------------------------------------------
// Convert a readable string into a number
//------------------------------------------------------------------------------
unsigned long long
StringConversion::GetSizeFromString(const char* instring)
{
  if (!instring) {
    errno = EINVAL;
    return 0;
  }

  XrdOucString sizestring = instring;
  unsigned long long convfactor = 1ll;
  errno = 0;

  if (!sizestring.length()) {
    errno = EINVAL;
    return 0;
  }

  if (sizestring.endswith("B") || sizestring.endswith("b")) {
    sizestring.erase(sizestring.length() - 1);
  }

  if (sizestring.endswith("E") || sizestring.endswith("e")) {
    convfactor = 1000ll * 1000ll * 1000ll * 1000ll * 1000ll * 1000ll;
  }

  if (sizestring.endswith("P") || sizestring.endswith("p")) {
    convfactor = 1000ll * 1000ll * 1000ll * 1000ll * 1000ll;
  }

  if (sizestring.endswith("T") || sizestring.endswith("t")) {
    convfactor = 1000ll * 1000ll * 1000ll * 1000ll;
  }

  if (sizestring.endswith("G") || sizestring.endswith("g")) {
    convfactor = 1000ll * 1000ll * 1000ll;
  }

  if (sizestring.endswith("M") || sizestring.endswith("m")) {
    convfactor = 1000ll * 1000ll;
  }

  if (sizestring.endswith("K") || sizestring.endswith("k")) {
    convfactor = 1000ll;
  }

  if (sizestring.endswith("S") || sizestring.endswith("s")) {
    convfactor = 1ll;
  }

  if ((sizestring.length() > 3) && (sizestring.endswith("MIN")
                                    || sizestring.endswith("min"))) {
    convfactor = 60ll;
  }

  if (sizestring.endswith("H") || sizestring.endswith("h")) {
    convfactor = 3600ll;
  }

  if (sizestring.endswith("D") || sizestring.endswith("d")) {
    convfactor = 86400ll;
  }

  if (sizestring.endswith("W") || sizestring.endswith("w")) {
    convfactor = 7 * 86400ll;
  }

  if ((sizestring.length() > 2) && (sizestring.endswith("MO")
                                    || sizestring.endswith("mo"))) {
    convfactor = 31 * 86400ll;
  }

  if (sizestring.endswith("Y") || sizestring.endswith("y")) {
    convfactor = 365 * 86400ll;
  }

  if (convfactor > 1) {
    sizestring.erase(sizestring.length() - 1);
  }

  if ((sizestring.find(".")) != STR_NPOS) {
    return ((unsigned long long)(strtod(sizestring.c_str(), NULL) * convfactor));
  } else {
    return (strtoll(sizestring.c_str(), 0, 10) * convfactor);
  }
}

//------------------------------------------------------------------------------
// Convert a readable string into a number
//------------------------------------------------------------------------------
unsigned long long
StringConversion::GetDataSizeFromString(const char* instring)
{
  if (!instring) {
    errno = EINVAL;
    return 0ll;
  }

  XrdOucString sizestring = instring;
  unsigned long long convfactor = 1ll;
  errno = 0;

  if (!sizestring.length()) {
    errno = EINVAL;
    return 0ll;
  } else {
    if (sizestring.endswith("B") || sizestring.endswith("b")) {
      sizestring.erase(sizestring.length() - 1);
    }

    if (sizestring.endswith("E") || sizestring.endswith("e")) {
      convfactor = 1000ll * 1000ll * 1000ll * 1000ll * 1000ll * 1000ll;
    } else if (sizestring.endswith("P") || sizestring.endswith("p")) {
      convfactor = 1000ll * 1000ll * 1000ll * 1000ll * 1000ll;
    } else if (sizestring.endswith("T") || sizestring.endswith("t")) {
      convfactor = 1000ll * 1000ll * 1000ll * 1000ll;
    } else if (sizestring.endswith("G") || sizestring.endswith("g")) {
      convfactor = 1000ll * 1000ll * 1000ll;
    } else if (sizestring.endswith("M") || sizestring.endswith("m")) {
      convfactor = 1000ll * 1000ll;
    } else if (sizestring.endswith("K") || sizestring.endswith("k")) {
      convfactor = 1000ll;
    }
  }

  if (convfactor > 1ll) {
    sizestring.erase(sizestring.length() - 1);
  }

  if ((sizestring.find(".")) != STR_NPOS) {
    return ((unsigned long long)(strtod(sizestring.c_str(), NULL) * convfactor));
  } else {
    return (strtoll(sizestring.c_str(), 0, 10) * convfactor);
  }
}

//------------------------------------------------------------------------------
// Convert a long long value into K,M,G,T,P,E byte scale
//------------------------------------------------------------------------------
const char*
StringConversion::GetReadableSizeString(std::string& sizestring,
                                        unsigned long long insize,
                                        const char* unit)
{
  XrdOucString oucsizestring = "";
  GetReadableSizeString(oucsizestring, insize, unit);
  sizestring = oucsizestring.c_str();
  return sizestring.c_str();
}

//------------------------------------------------------------------------------
// Convert a long long number into a std::string
//------------------------------------------------------------------------------
const char*
StringConversion::GetSizeString(std::string& sizestring,
                                unsigned long long insize)
{
  char buffer[1024];
  sprintf(buffer, "%llu", insize);
  sizestring = buffer;
  return sizestring.c_str();
}

//------------------------------------------------------------------------------
// Convert a number into a XrdOuc string
//------------------------------------------------------------------------------
const char*
StringConversion::GetSizeString(XrdOucString& sizestring,
                                unsigned long long insize)
{
  char buffer[1024];
  sprintf(buffer, "%llu", insize);
  sizestring = buffer;
  return sizestring.c_str();
}

//------------------------------------------------------------------------------
// Convert a floating point number into a string
//------------------------------------------------------------------------------
const char*
StringConversion::GetSizeString(XrdOucString& sizestring, double insize)
{
  char buffer[1024];
  sprintf(buffer, "%.02f", insize);
  sizestring = buffer;
  return sizestring.c_str();
}

//------------------------------------------------------------------------------
// Split a 'key:value' definition into key + value
//------------------------------------------------------------------------------
bool
StringConversion::SplitKeyValue(std::string keyval, std::string& key,
                                std::string& value, std::string split)
{
  auto equalpos = keyval.find(split);

  if (equalpos != std::string::npos) {
    key.assign(keyval, 0, equalpos);
    value.assign(keyval, equalpos + 1, keyval.length() - (equalpos + 1));
    return true;
  } else {
    key = value = "";
    return false;
  }
}

//------------------------------------------------------------------------------
// Split a 'key:value' definition into key + value
//------------------------------------------------------------------------------
bool
StringConversion::SplitKeyValue(XrdOucString keyval, XrdOucString& key,
                                XrdOucString& value, XrdOucString split)
{
  int equalpos = keyval.find(split.c_str());

  if (equalpos != STR_NPOS) {
    key.assign(keyval, 0, equalpos - 1);
    value.assign(keyval, equalpos + 1);
    return true;
  } else {
    key = value = "";
    return false;
  }
}

//------------------------------------------------------------------------------
// Split a comma separated key:val list and fill it into a map
//------------------------------------------------------------------------------
bool
StringConversion::GetKeyValueMap(const char* mapstring,
                                 std::map<std::string, std::string>& map,
                                 const char* split,
                                 const char* sdelimiter,
                                 std::vector<std::string>* keyvector)
{
  if (!mapstring) {
    return false;
  }

  std::string is = mapstring;
  std::string delimiter = sdelimiter;
  std::vector<std::string> slist;
  Tokenize(is, slist, delimiter);

  if (!slist.size()) {
    return false;
  }

  size_t keyvectorindex = 0;

  for (auto it = slist.begin(); it != slist.end(); it++) {
    std::string key;
    std::string val;

    if (SplitKeyValue(*it, key, val, split)) {
      if (keyvector && !map.count(key)) {
        if (std::find(keyvector->begin(), keyvector->end(), key) == keyvector->end()) {
          std::vector<std::string>::iterator it = keyvector->begin();
          std::advance(it, keyvectorindex);
          keyvector->insert(it, key);
        }
      }

      keyvectorindex++;
      map[key] = val;
    } else {
      return false;
    }
  }

  return true;
}

//------------------------------------------------------------------------------
// Specialized splitting function returning the host part out of a queue name
//------------------------------------------------------------------------------
XrdOucString
StringConversion::GetHostPortFromQueue(const char* queue)
{
  XrdOucString hostport = queue;
  int pos = hostport.find("/", 2);

  if (pos != STR_NPOS) {
    hostport.erase(0, pos + 1);
    pos = hostport.find("/");

    if (pos != STR_NPOS) {
      hostport.erase(pos);
    }
  }

  return hostport;
}

//------------------------------------------------------------------------------
// Specialized splitting function returning the host:port part out of a queue name
//------------------------------------------------------------------------------
std::string
StringConversion::GetStringHostPortFromQueue(const char* queue)
{
  std::string hostport = queue;
  int pos = hostport.find("/", 2);

  if (pos != STR_NPOS) {
    hostport.erase(0, pos + 1);
    pos = hostport.find("/");

    if (pos != STR_NPOS) {
      hostport.erase(pos);
    }
  }

  return hostport;
}

//------------------------------------------------------------------------------
// Split 'a.b' into a and b
//------------------------------------------------------------------------------
void
StringConversion::SplitByPoint(std::string in, std::string& pre,
                               std::string& post)
{
  std::string::size_type dpos = 0;
  pre = in;
  post = in;

  if ((dpos = in.find(".")) != std::string::npos) {
    std::string s = in;
    post.erase(0, dpos + 1);
    pre.erase(dpos);
  } else {
    post = "";
  }
}

//------------------------------------------------------------------------------
// Convert a string into a line-wise map
//------------------------------------------------------------------------------
void
StringConversion::StringToLineVector(char* in, std::vector<std::string>& out)
{
  char* pos = in;
  char* old_pos = in;
  int len = strlen(in);

  while ((pos = strchr(pos, '\n'))) {
    *pos = 0;
    out.push_back(old_pos);
    *pos = '\n';
    pos++;
    old_pos = pos;
    // check for the end of string

    if ((pos - in) >= len) {
      break;
    }
  }
}

//------------------------------------------------------------------------------
// Split a string of type '<string>@<int>[:<0xXXXXXXXX] into string,int,
// std::set<unsigned long long>'
//------------------------------------------------------------------------------
bool
StringConversion::ParseStringIdSet(char* in, std::string& tag,
                                   unsigned long& id,
                                   std::set<unsigned long long>& set)
{
  char* ptr = in;
  char* add = strchr(in, '@');

  if (!add) {
    return false;
  }

  char* colon = strchr(add, ':');

  if (!colon) {
    id = strtoul(add + 1, 0, 10);

    if (id) {
      return true;
    } else {
      return false;
    }
  } else {
    *colon = 0;
    id = strtoul(add + 1, 0, 10);
    *colon = ':';
  }

  *add = 0;
  tag = ptr;
  *add = '@';
  ptr = colon + 1;

  do {
    char* nextcolon = strchr(ptr, ':');

    // get a set member
    if (nextcolon) {
      *nextcolon = 0;
      unsigned long long n = strtoull(ptr, 0, 16);
      *nextcolon = ':';
      set.insert(n);
      ptr = nextcolon + 1;
    } else {
      unsigned long long n = strtoull(ptr, 0, 16);
      set.insert(n);
      return true;
    }
  } while (1);

  return false;
}

//------------------------------------------------------------------------------
// Load a text file <name> into a string
//------------------------------------------------------------------------------
const char*
StringConversion::LoadFileIntoString(const char* filename, std::string& out)
{
  std::ifstream load(filename);
  std::stringstream buffer;
  buffer << load.rdbuf();
  out = buffer.str();
  return out.c_str();
}

// ---------------------------------------------------------------------------
// Save a string into  a text file <name>
// ---------------------------------------------------------------------------
bool
StringConversion::SaveStringIntoFile(const char* filename,
                                     const std::string& in)
{
  std::ofstream save(filename);
  save.write(in.c_str(), in.size());
  return true;
}


//------------------------------------------------------------------------------
// Read a long long number as output of a shell command - this is not usefull
// in multi-threaded environments.
//------------------------------------------------------------------------------
long long
StringConversion::LongLongFromShellCmd(const char* shellcommand)
{
  FILE* fd = popen(shellcommand, "r");

  if (fd) {
    char buffer[1025];
    buffer[0] = 0;
    int nread = fread((void*) buffer, 1, 1024, fd);
    pclose(fd);

    if ((nread > 0) && (nread < 1024)) {
      buffer[nread] = 0;
      return strtoll(buffer, 0, 10);
    }
  }

  return LLONG_MAX;
}

//------------------------------------------------------------------------------
// Read a string as output of a shell command - this is not usefull in
// multi-threaded environments.
//------------------------------------------------------------------------------
std::string
StringConversion::StringFromShellCmd(const char* shellcommand)
{
  FILE* fd = popen(shellcommand, "r");
  std::string shellstring;

  if (fd) {
    char buffer[1025];
    buffer[0] = 0;
    int nread = 0;

    while ((nread = fread((void*) buffer, 1, 1024, fd)) > 0) {
      buffer[nread] = 0;
      shellstring += buffer;

      if (nread != 1024) {
        break;
      }
    }

    pclose(fd);
    return shellstring;
  }

  return "<none>";
}

//------------------------------------------------------------------------------
// Return the time as <seconds>.<nanoseconds> in a string
//------------------------------------------------------------------------------
const char*
StringConversion::TimeNowAsString(XrdOucString& stime)
{
  struct timespec ts;
  eos::common::Timing::GetTimeSpec(ts);
  char tb[128];
  snprintf(tb, sizeof(tb) - 1, "%lu.%lu", ts.tv_sec, ts.tv_nsec);
  stime = tb;
  return stime.c_str();
}

//------------------------------------------------------------------------------
// Mask a tag 'key=val' as 'key=<...>' in an opaque string
//------------------------------------------------------------------------------
const char*
StringConversion::MaskTag(XrdOucString& line, const char* tag)
{
  XrdOucString smask = tag;
  smask += "=";
  int spos = line.find(smask.c_str());
  int epos = line.find("&", spos + 1);

  if (spos != STR_NPOS) {
    if (epos != STR_NPOS) {
      line.erase(spos, epos - spos);
    } else {
      line.erase(spos);
    }

    smask += "<...>";
    line.insert(smask.c_str(), spos);
  }

  return line.c_str();
}

//------------------------------------------------------------------------------
// Parse a string as an URL (does not deal with opaque information)
//------------------------------------------------------------------------------
const char*
StringConversion::ParseUrl(const char* url, XrdOucString& protocol,
                           XrdOucString& hostport)
{
  protocol = url;
  hostport = url;
  int ppos = protocol.find(":/");

  if (ppos != STR_NPOS) {
    protocol.erase(ppos);
  } else {
    if (protocol.beginswith("as3:")) {
      protocol = "as3";
    } else {
      protocol = "file";
    }
  }

  if (protocol == "file") {
    if (hostport.beginswith("file:")) {
      hostport = "";
      return (url + 5);
    } else {
      hostport = "";
      return url;
    }
  }

  if (protocol == "root") {
    int spos = hostport.find("//", ppos + 2);

    if (spos == STR_NPOS) {
      return 0;
    }

    hostport.erase(spos);
    hostport.erase(0, 7);
    return (url + spos + 1);
  }

  if (protocol == "as3") {
    if (hostport.beginswith("as3://")) {
      // as3://<hostname>/<bucketname>/<filename> like in ROOT
      int spos = hostport.find("/", 6);

      if (spos != STR_NPOS) {
        hostport.erase(spos);
        hostport.erase(0, 6);
        return (url + spos + 1);
      } else {
        return 0;
      }
    } else {
      // as3:<bucketname>/<filename>
      hostport = "";
      return (url + 4);
    }
  }

  if (protocol == "http") {
    // http://<hostname><path>
    int spos = hostport.find("/", 7);

    if (spos == STR_NPOS) {
      return 0;
    }

    hostport.erase(spos);
    hostport.erase(0, 7);
    return (url + spos);
  }

  if (protocol == "https") {
    // https://<hostname><path>
    int spos = hostport.find("/", 8);

    if (spos == STR_NPOS) {
      return 0;
    }

    hostport.erase(spos);
    hostport.erase(0, 8);
    return (url + spos);
  }

  if (protocol == "gsiftp") {
    // gsiftp://<hostname><path>
    int spos = hostport.find("/", 9);

    if (spos == STR_NPOS) {
      return 0;
    }

    hostport.erase(spos);
    hostport.erase(0, 9);
    return (url + spos);
  }

  return 0;
}

//------------------------------------------------------------------------------
// Create an Url
//------------------------------------------------------------------------------
const char*
StringConversion::CreateUrl(const char* protocol, const char* hostport,
                            const char* path, XrdOucString& url)
{
  if (!strcmp(protocol, "file")) {
    url = path;
    return url.c_str();
  }

  if (!strcmp(protocol, "root")) {
    url = "root://";
    url += hostport;
    url += "/";
    url += path;
    return url.c_str();
  }

  if (!strcmp(protocol, "as3")) {
    if (hostport && strlen(hostport)) {
      url = "as3://";
      url += hostport;
      url += path;
      return url.c_str();
    } else {
      url = "as3:";
      url += path;
      return url.c_str();
    }
  }

  if (!strcmp(protocol, "http")) {
    url = "http://";
    url += hostport;
    url += path;
    return url.c_str();
  }

  if (!strcmp(protocol, "gsiftp")) {
    url = "gsiftp://";
    url += hostport;
    url += path;
    return url.c_str();
  }

  url = "";
  return 0;
}

//------------------------------------------------------------------------------
// Check if string is hex number
//------------------------------------------------------------------------------
bool
StringConversion::IsHexNumber(const char* hexstring, const char* format)
{
  if (!hexstring) {
    return false;
  }

  unsigned long long number = strtoull(hexstring, 0, 16);
  char controlstring[256];
  snprintf(controlstring, sizeof(controlstring) - 1, format, number);
  return !strcmp(hexstring, controlstring);
}

//------------------------------------------------------------------------------
// Convert numeric value to string in a pretty way using KB, MB etc. symbols
//------------------------------------------------------------------------------
std::string
StringConversion::GetPrettySize(float size)
{
  float fsize = 0;
  std::string ret_str;
  std::string size_unit;

  if ((fsize = size / EB) >= 1) {
    size_unit = "EB";
  } else if ((fsize = size / PB) >= 1) {
    size_unit = "PB";
  } else if ((fsize = size / TB) >= 1) {
    size_unit = "TB";
  } else if ((fsize = size / MB) >= 1) {
    size_unit = "MB";
  } else {
    fsize = size / KB;
    size_unit = "KB";
  }

  char msg[30];
  sprintf(msg, "%.1f %s", fsize, size_unit.c_str());
  ret_str = msg;
  return ret_str;
}

//------------------------------------------------------------------------------
// CURL init/cleanup using local storage
//------------------------------------------------------------------------------
void StringConversion::tlCurlFree(void* arg)
{
  eos_static_debug("destroying thread specific CURL session");
  // delete the buffer
  curl_easy_cleanup((CURL*)arg);
}

CURL* StringConversion::tlCurlInit()
{
  eos_static_debug("allocating thread specific CURL session");
  CURL* buf = curl_easy_init();

  if (!buf) {
    eos_static_crit("error initialising CURL easy session");
  }

  if (buf && pthread_setspecific(sPthreadKey, buf)) {
    eos_static_crit("error registering thread-local buffer located at %p for "
                    "cleaning up : memory will be leaked when thread is "
                    "terminated", buf);
  }

  return buf;
}

void StringConversion::tlInitThreadKey()
{
  pthread_key_create(&sPthreadKey, StringConversion::tlCurlFree);
}

thread_local CURL* StringConversion::curl = NULL;
pthread_key_t  StringConversion::sPthreadKey;
pthread_once_t StringConversion::sInit = PTHREAD_ONCE_INIT;

//------------------------------------------------------------------------------
// Escape string using CURL
//------------------------------------------------------------------------------
std::string
StringConversion::curl_default_escaped(const std::string& str)
{
  pthread_once(&sInit, tlInitThreadKey);
  std::string ret_str;

  // encode the key
  if (!curl) {
    curl = tlCurlInit();
  }

  if (curl) {
    char* output = curl_easy_escape(curl, str.c_str(), str.length());

    if (output) {
      ret_str = output;
      curl_free(output);
    }
  }

  return ret_str;
}

//------------------------------------------------------------------------------
// Unescape string using CURL
//------------------------------------------------------------------------------
std::string
StringConversion::curl_default_unescaped(const std::string& str)
{
  pthread_once(&sInit, tlInitThreadKey);
  std::string ret_str;

  // encode the key
  if (!curl) {
    curl = tlCurlInit();
  }

  if (curl) {
    char* output = curl_easy_unescape(curl, str.c_str(), str.length(), 0);

    if (output) {
      ret_str = output;
      curl_free(output);
    }
  }

  return ret_str;
}

//------------------------------------------------------------------------------
// Escape string using CURL and add speical /#curl# prefix to the string
//------------------------------------------------------------------------------
std::string
StringConversion::curl_escaped(const std::string& str)
{
  pthread_once(&sInit, tlInitThreadKey);
  std::string ret_str = "<no-encoding>";

  // encode the key
  if (!curl) {
    curl = tlCurlInit();
  }

  if (curl) {
    char* output = curl_easy_escape(curl, str.c_str(), str.length());

    if (output) {
      ret_str = output;
      curl_free(output);
      // don't escape '/'
      XrdOucString no_slash = ret_str.c_str();

      while (no_slash.replace("%2F", "/")) {}

      // this is a hack to avoid decoding a pathname twice
      no_slash.insert("/#curl#", 0);
      ret_str = no_slash.c_str();
    }
  }

  return ret_str;
}

//------------------------------------------------------------------------------
// Unescape string using CURL that contains special /#curl# prefix
//------------------------------------------------------------------------------
std::string
StringConversion::curl_unescaped(const std::string& str)
{
  pthread_once(&sInit, tlInitThreadKey);
  std::string ret_str = "<no-encoding>";

  // encode the key
  if (!curl) {
    curl = tlCurlInit();
  }

  if (curl) {
    if (strncmp(str.c_str(), "/#curl#", 7)) {
      // the string has already been decoded
      return str;
    }

    char* output = curl_easy_unescape(curl, str.c_str() + 7, str.length() - 7, 0);

    if (output) {
      ret_str = output;
      curl_free(output);
    }
  }

  return ret_str;
}

// ---------------------------------------------------------------------------
// Escape JSON string
// ---------------------------------------------------------------------------
std::string
StringConversion::json_encode(const std::string& s)
{
  std::string output;
  output.reserve(s.length());

  for (size_t i = 0; i != s.length(); i++) {
    char c = s.at(i);

    switch (c) {
    case '"':
      output += "\\\"";
      break;

    case '/':
      output += "\\/";
      break;

    case '\b':
      output += "\\b";
      break;

    case '\f':
      output += "\\f";
      break;

    case '\n':
      output += "\\n";
      break;

    case '\r':
      output += "\\r";
      break;

    case '\t':
      output += "\\t";
      break;

    case '\\':
      output += "\\\\";
      break;

    default:
      output += c;
      break;
    }
  }

  return output;
}


//------------------------------------------------------------------------------
// Create random uuid string
//------------------------------------------------------------------------------
std::string
StringConversion::random_uuidstring()
{
  char id[40];
  uuid_t uuid;
  uuid_generate_time(uuid);
  uuid_unparse(uuid, id);
  return id;
}

//------------------------------------------------------------------------------
// Create time-based uuid string
//------------------------------------------------------------------------------
std::string StringConversion::timebased_uuidstring() {
  uuid_t uuid;
  //The uuid_unparse function converts the supplied UUID from the binary representation into a 36-byte string + '\0' trailing
  char uuid_str[37];
  uuid_generate_time(uuid);
  uuid_unparse(uuid,uuid_str);
  return std::string(uuid_str);
}

//------------------------------------------------------------------------------
// Sort lines alphabetically in-place
//------------------------------------------------------------------------------
void
StringConversion::SortLines(XrdOucString& data)
{
  XrdOucString sorts = "";
  std::vector<std::string> vec;
  XrdOucTokenizer linizer((char*)data.c_str());
  char* val = 0;

  while ((val = linizer.GetLine())) {
    vec.push_back(val);
  }

  std::sort(vec.begin(), vec.end());

  for (unsigned int i = 0; i < vec.size(); ++i) {
    sorts += vec[i].c_str();
    sorts += "\n";
  }

  data = sorts;
}


//------------------------------------------------------------------------------
// Check if a string is valid UTF
//------------------------------------------------------------------------------

bool
StringConversion::Valid_UTF8(const string& string)
{
  int c, i, ix, n, j;

  for (i = 0, ix = string.length(); i < ix; i++) {
    c = (unsigned char) string[i];

    //if (c==0x09 || c==0x0a || c==0x0d || (0x20 <= c && c <= 0x7e) ) n = 0; // is_printable_ascii
    if (0x00 <= c && c <= 0x7f) {
      n = 0;  // 0bbbbbbb
    } else if ((c & 0xE0) == 0xC0) {
      n = 1;  // 110bbbbb
    } else if (c == 0xed && i < (ix - 1) &&
               ((unsigned char)string[i + 1] & 0xa0) == 0xa0) {
      return false;  //U+d800 to U+dfff
    } else if ((c & 0xF0) == 0xE0) {
      n = 2;  // 1110bbbb
    } else if ((c & 0xF8) == 0xF0) {
      n = 3;  // 11110bbb
    }
    //else if (($c & 0xFC) == 0xF8) n=4; // 111110bb //byte 5, unnecessary in 4 byte UTF-8
    //else if (($c & 0xFE) == 0xFC) n=5; // 1111110b //byte 6, unnecessary in 4 byte UTF-8
    else {
      return false;
    }

    for (j = 0; j < n &&
         i < ix; j++) { // n bytes matching 10bbbbbb follow ?
      if ((++i == ix) || (((unsigned char)string[i] & 0xC0) != 0x80)) {
        return false;
      }
    }
  }

  return true;
}

//------------------------------------------------------------------------------
// CURL encode invalid UTF strings with SHA256
//------------------------------------------------------------------------------

std::string
StringConversion::EncodeInvalidUTF8(const string& key)
{
  if (!Valid_UTF8(key)) {
    return curl_escaped(key);
  } else {
    return key;
  }
}

//------------------------------------------------------------------------------
// Seal opaque xrootd info. I.e. replace any & with #AND#
//------------------------------------------------------------------------------
std::string
StringConversion::SealXrdOpaque(const std::string& input)
{
  std::string sealed {
    input
  };
  ReplaceStringInPlace(sealed, "&", "#AND#");
  return sealed;
}

//------------------------------------------------------------------------------
// Unseal opaque xrootd info. I.e. replace any #AND# with &
//------------------------------------------------------------------------------
std::string
StringConversion::UnsealXrdOpaque(const std::string& input)
{
  std::string unsealed {input};
  ReplaceStringInPlace(unsealed, "#AND#", "&");
  return unsealed;
}

EOSCOMMONNAMESPACE_END
