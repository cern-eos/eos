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


/*----------------------------------------------------------------------------*/
#include "common/StringConversion.hh"
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN


// ---------------------------------------------------------------------------
/** 
 * Tokenize a string
 * 
 * @param str string to be tokenized
 * @param tokens  returned list of seperated string tokens
 * @param delimiters delimiter used for tokenizing
 */
// ---------------------------------------------------------------------------
void
StringConversion::Tokenize (const std::string& str,
                            std::vector<std::string>& tokens,
                            const std::string& delimiters)
{
  // Skip delimiters at beginning.
  std::string::size_type lastPos = str.find_first_not_of(delimiters, 0);
  // Find first "non-delimiter".
  std::string::size_type pos = str.find_first_of(delimiters, lastPos);

  while (std::string::npos != pos || std::string::npos != lastPos)
  {
    // Found a token, add it to the vector.
    tokens.push_back(str.substr(lastPos, pos - lastPos));
    // Skip delimiters.  Note the "not_of"
    lastPos = str.find_first_not_of(delimiters, pos);
    // Find next "non-delimiter"
    pos = str.find_first_of(delimiters, lastPos);
  }
}

// ---------------------------------------------------------------------------
/** 
 * Tokenize a string accepting also empty members e.g. a||b is returning 3 fields
 * 
 * @param str string to be tokenized
 * @param tokens  returned list of seperated string tokens
 * @param delimiters delimiter used for tokenizing
 */
// ---------------------------------------------------------------------------

void
StringConversion::EmptyTokenize (const std::string& str,
                                 std::vector<std::string>& tokens,
                                 const std::string& delimiters)
{
  // Skip delimiters at beginning.
  std::string::size_type lastPos = str.find_first_not_of(delimiters, 0);
  // Find first "non-delimiter".
  std::string::size_type pos = str.find_first_of(delimiters, lastPos);

  while (std::string::npos != pos || std::string::npos != lastPos)
  {
    // Found a token, add it to the vector.
    tokens.push_back(str.substr(lastPos, pos - lastPos));
    // Skip delimiters.  Note the "not_of"
    lastPos = str.find_first_of(delimiters, pos);
    if (lastPos != std::string::npos)
      lastPos++;
    // Find next "non-delimiter"
    pos = str.find_first_of(delimiters, lastPos);
  }
}

// ---------------------------------------------------------------------------
/** 
 * Convert a long long value into K,M,G,T,P,E byte scale
 * 
 * @param sizestring returned XrdOuc string representation
 * @param insize number to convert
 * @param unit unit to display e.g. B for bytes
 * 
 * @return sizestring.c_str()
 */
// ---------------------------------------------------------------------------

const char*
StringConversion::GetReadableSizeString (XrdOucString& sizestring, unsigned long long insize, const char* unit)
{
  char formsize[1024];
  if (insize >= 1000)
  {
    if (insize >= (1000 * 1000))
    {
      if (insize >= (1000ll * 1000ll * 1000ll))
      {
        if (insize >= (1000ll * 1000ll * 1000ll * 1000ll))
        {
          if (insize >= (1000ll * 1000ll * 1000ll * 1000ll * 1000ll))
          {
            if (insize >= (1000ll * 1000ll * 1000ll * 1000ll * 1000ll * 1000ll))
            {
              // EB
              sprintf(formsize, "%.02f E%s", insize * 1.0 / (1000ll * 1000ll * 1000ll * 1000ll * 1000ll * 1000ll), unit);
            }
            else
            {
              // PB
              sprintf(formsize, "%.02f P%s", insize * 1.0 / (1000ll * 1000ll * 1000ll * 1000ll * 1000ll), unit);
            }
          }
          else
          {
            // TB
            sprintf(formsize, "%.02f T%s", insize * 1.0 / (1000ll * 1000ll * 1000ll * 1000ll), unit);
          }
        }
        else
        {
          // GB
          sprintf(formsize, "%.02f G%s", insize * 1.0 / (1000ll * 1000ll * 1000ll), unit);
        }
      }
      else
      {
        // MB
        sprintf(formsize, "%.02f M%s", insize * 1.0 / (1000 * 1000), unit);
      }
    }
    else
    {
      // kB
      sprintf(formsize, "%.02f k%s", insize * 1.0 / (1000), unit);
    }
  }
  else
  {
    if (strlen(unit))
    {
      sprintf(formsize, "%.02f %s", insize * 1.0, unit);
    }
    else
    {
      sprintf(formsize, "%.02f", insize * 1.0);
    }
  }
  sizestring = formsize;

  return sizestring.c_str();
}

// ---------------------------------------------------------------------------
/** 
 * Convert a readable string into a number
 * 
 * @param sizestring readable string like 4 KB or 1000 GB
 * 
 * @return number
 */
// ---------------------------------------------------------------------------

unsigned long long
StringConversion::GetSizeFromString (XrdOucString sizestring)
{
  errno = 0;
  unsigned long long convfactor;
  convfactor = 1ll;
  if (!sizestring.length())
  {
    errno = EINVAL;
    return 0;
  }

  if (sizestring.endswith("B") || sizestring.endswith("b"))
  {
    sizestring.erase(sizestring.length() - 1);
  }

  if (sizestring.endswith("E") || sizestring.endswith("e"))
  {
    convfactor = 1000ll * 1000ll * 1000ll * 1000ll * 1000ll * 1000ll;
  }

  if (sizestring.endswith("P") || sizestring.endswith("p"))
  {
    convfactor = 1000ll * 1000ll * 1000ll * 1000ll * 1000ll;
  }

  if (sizestring.endswith("T") || sizestring.endswith("t"))
  {
    convfactor = 1000ll * 1000ll * 1000ll * 1000ll;
  }

  if (sizestring.endswith("G") || sizestring.endswith("g"))
  {
    convfactor = 1000ll * 1000ll * 1000ll;
  }

  if (sizestring.endswith("M") || sizestring.endswith("m"))
  {
    convfactor = 1000ll * 1000ll;
  }

  if (sizestring.endswith("K") || sizestring.endswith("k"))
  {
    convfactor = 1000ll;
  }

  if (sizestring.endswith("S") || sizestring.endswith("s"))
  {
    convfactor = 1ll;
  }

  if ((sizestring.length() > 3) && (sizestring.endswith("MIN") || sizestring.endswith("min")))
  {
    convfactor = 60ll;
  }

  if (sizestring.endswith("H") || sizestring.endswith("h"))
  {
    convfactor = 3600ll;
  }

  if (sizestring.endswith("D") || sizestring.endswith("d"))
  {
    convfactor = 86400ll;
  }

  if (sizestring.endswith("W") || sizestring.endswith("w"))
  {
    convfactor = 7 * 86400ll;
  }

  if ((sizestring.length() > 2) && (sizestring.endswith("MO") || sizestring.endswith("mo")))
  {
    convfactor = 31 * 86400ll;
  }

  if (sizestring.endswith("Y") || sizestring.endswith("y"))
  {
    convfactor = 365 * 86400ll;
  }

  if (convfactor > 1)
    sizestring.erase(sizestring.length() - 1);

  if ((sizestring.find(".")) != STR_NPOS)
  {
    return ((unsigned long long) (strtod(sizestring.c_str(), NULL) * convfactor));
  }
  else
  {
    return (strtoll(sizestring.c_str(), 0, 10) * convfactor);
  }
}


// ---------------------------------------------------------------------------
/** 
 * Convert a long long value into K,M,G,T,P,E byte scale
 * 
 * @param sizestring returned standard string representation
 * @param insize number to convert
 * @param unit unit to display e.g. B for bytes
 * 
 * @return sizestring.c_str()
 */
// ---------------------------------------------------------------------------

const char*
StringConversion::GetReadableSizeString (std::string& sizestring, unsigned long long insize, const char* unit)
{
  const char* ptr = 0;
  XrdOucString oucsizestring = "";
  ptr = GetReadableSizeString(oucsizestring, insize, unit);
  sizestring = oucsizestring.c_str();
  return ptr;
}

// ---------------------------------------------------------------------------
/** 
 * Convert a long long number into a std::string
 * 
 * @param sizestring returned string
 * @param insize number
 * 
 * @return sizestring.c_str()
 */
// ---------------------------------------------------------------------------

const char*
StringConversion::GetSizeString (std::string& sizestring, unsigned long long insize)
{
  char buffer[1024];
  sprintf(buffer, "%llu", insize);
  sizestring = buffer;
  return sizestring.c_str();
}

// ---------------------------------------------------------------------------
/** 
 * Convert a number into a XrdOuc string
 * 
 * @param sizestring returned XrdOuc string
 * @param insize number
 * 
 * @return sizestring.c_str()
 */
// ---------------------------------------------------------------------------

const char*
StringConversion::GetSizeString (XrdOucString& sizestring, unsigned long long insize)
{
  char buffer[1024];
  sprintf(buffer, "%llu", insize);
  sizestring = buffer;
  return sizestring.c_str();
}

// ---------------------------------------------------------------------------
/** 
 * Convert a floating point number into a string
 * 
 * @param sizestring returned string
 * @param insize floating point number
 * 
 * @return sizestring.c_str()
 */
// ---------------------------------------------------------------------------

const char*
StringConversion::GetSizeString (XrdOucString& sizestring, double insize)
{
  char buffer[1024];
  sprintf(buffer, "%.02f", insize);
  sizestring = buffer;
  return sizestring.c_str();
}

// ---------------------------------------------------------------------------
/** 
 * Split a 'key:value' definition into key + value
 * 
 * @param keyval key-val string 'key:value'
 * @param key returned key
 * @param value return value
 * 
 * @return true if parsing ok, false if wrong format
 */
// ---------------------------------------------------------------------------  

bool
StringConversion::SplitKeyValue (std::string keyval, std::string &key, std::string &value)
{
  int equalpos = keyval.find(":");
  if (equalpos != STR_NPOS)
  {
    key.assign(keyval, 0, equalpos - 1);
    value.assign(keyval, equalpos + 1, keyval.length()-(equalpos + 1));
    return true;
  }
  else
  {
    key = value = "";
    return false;
  }
}

// ---------------------------------------------------------------------------
/** 
 * Split a 'key:value' definition into key + value
 * 
 * @param keyval key-val string 'key:value'
 * @param key returned key
 * @param value return value
 * 
 * @return true if parsing ok, false if wrong format
 */
// ---------------------------------------------------------------------------  

bool
StringConversion::SplitKeyValue (XrdOucString keyval, XrdOucString &key, XrdOucString &value)
{
  int equalpos = keyval.find(":");
  if (equalpos != STR_NPOS)
  {
    key.assign(keyval, 0, equalpos - 1);
    value.assign(keyval, equalpos + 1);
    return true;
  }
  else
  {
    key = value = "";
    return false;
  }
}

// ---------------------------------------------------------------------------  
/** 
 * Specialized splitting function returning the host part out of a queue name
 * 
 * @param queue name of a queue e.g. /eos/host:port/role
 * 
 * @return string containing the host
 */
// ---------------------------------------------------------------------------  

XrdOucString
StringConversion::GetHostPortFromQueue (const char* queue)
{
  XrdOucString hostport = queue;
  int pos = hostport.find("/", 2);
  if (pos != STR_NPOS)
  {
    hostport.erase(0, pos + 1);
    pos = hostport.find("/");
    if (pos != STR_NPOS)
    {
      hostport.erase(pos);
    }
  }
  return hostport;
}

// ---------------------------------------------------------------------------
/** 
 * Specialized splitting function returning the host:port part out of a queue name
 * 
 * @param queue name of a queue e.g. /eos/host:port/role
 * 
 * @return string containing host:port
 */
// ---------------------------------------------------------------------------

std::string
StringConversion::GetStringHostPortFromQueue (const char* queue)
{
  std::string hostport = queue;
  int pos = hostport.find("/", 2);
  if (pos != STR_NPOS)
  {
    hostport.erase(0, pos + 1);
    pos = hostport.find("/");
    if (pos != STR_NPOS)
    {
      hostport.erase(pos);
    }
  }
  return hostport;
}

// ---------------------------------------------------------------------------
/** 
 * Split 'a.b' into a and b
 * 
 * @param in 'a.b'
 * @param pre string before .
 * @param post string after .
 */
// ---------------------------------------------------------------------------

void
StringConversion::SplitByPoint (std::string in, std::string &pre, std::string &post)
{
  std::string::size_type dpos = 0;
  pre = in;
  post = in;
  if ((dpos = in.find(".")) != std::string::npos)
  {
    std::string s = in;
    post.erase(0, dpos + 1);
    pre.erase(dpos);
  }
  else
  {
    post = "";
  }
}

// ---------------------------------------------------------------------------
/** 
 * Convert a string into a line-wise map
 * 
 * @param in char*
 * @param out vector with std::string lines
 */
// ---------------------------------------------------------------------------

void
StringConversion::StringToLineVector (char* in, std::vector<std::string> &out)
{
  char* pos = in;
  char* old_pos = in;
  int len = strlen(in);
  while ((pos = strchr(pos, '\n')))
  {
    *pos = 0;
    out.push_back(old_pos);
    *pos = '\n';
    pos++;
    old_pos = pos;
    // check for the end of string
    if ((pos - in) >= len)
      break;
  }
}

// ---------------------------------------------------------------------------
/** 
 * Split a string of type '<string>@<int>[:<0xXXXXXXXX] into string,int,std::set<unsigned long long>'
 * 
 * @param in char*
 * @param tag string 
 * @param id unsigned long
 * @param set std::set<unsigned long long>
 * @return true if parsed, false if format error
 */
// ---------------------------------------------------------------------------  

bool
StringConversion::ParseStringIdSet (char* in, std::string& tag, unsigned long& id, std::set<unsigned long long> &set)
{
  char* ptr = in;
  char* add = strchr(in, '@');
  if (!add)
    return false;

  char* colon = strchr(add, ':');

  if (!colon)
  {
    id = strtoul(add + 1, 0, 10);
    if (id)
    {
      return true;
    }
    else
    {
      return false;
    }
  }
  else
  {
    *colon = 0;
    id = strtoul(add + 1, 0, 10);
    *colon = ':';
  }

  *add = 0;
  tag = ptr;
  *add = '@';

  ptr = colon + 1;
  do
  {
    char* nextcolon = strchr(ptr, ':');
    // get a set member
    if (nextcolon)
    {
      *nextcolon = 0;
      unsigned long long n = strtoull(ptr, 0, 16);
      *nextcolon = ':';
      set.insert(n);
      ptr = nextcolon + 1;
    }
    else
    {
      unsigned long long n = strtoull(ptr, 0, 16);
      set.insert(n);
      return true;
    }
  }
  while (1);
  return false;
}

// ---------------------------------------------------------------------------
/** 
 * Load a text file <name> into a string
 * 
 * @param filename from where to load the contents
 * @param out string where to inject the file contents
 * @return (const char*) pointer to loaded string
 */
// ---------------------------------------------------------------------------  

const char*
StringConversion::LoadFileIntoString (const char* filename, std::string &out)
{
  std::ifstream load(filename);
  std::stringstream buffer;

  buffer << load.rdbuf();
  out = buffer.str();
  return out.c_str();
}


// ---------------------------------------------------------------------------
/** 
 * Read a long long number as output of a shell command - this is not usefull in multi-threaded environments
 * 
 * @param shellcommand to execute
 * @return long long value of converted shell output
 */
// ---------------------------------------------------------------------------  

long long
StringConversion::LongLongFromShellCmd (const char* shellcommand)
{
  FILE* fd = popen(shellcommand, "r");
  if (fd)
  {
    char buffer[1024];
    buffer[0] = 0;
    int nread = fread((void*) buffer, 1, 1024, fd);
    pclose(fd);
    if ((nread > 0) && (nread < 1024))
    {
      buffer[nread] = 0;
      return strtoll(buffer, 0, 10);
    }
  }
  return LLONG_MAX;
}

// ---------------------------------------------------------------------------
/** 
 * Read a string as output of a shell command - this is not usefull in multi-threaded environments
 * 
 * @param shellcommand to execute
 * @return XrdOucString
 */
// ---------------------------------------------------------------------------  

std::string
StringConversion::StringFromShellCmd (const char* shellcommand)
{
  FILE* fd = popen(shellcommand, "r");
  if (fd)
  {
    char buffer[1024];
    buffer[0] = 0;
    int nread = fread((void*) buffer, 1, 1024, fd);
    pclose(fd);
    if ((nread > 0) && (nread < 1024))
    {
      buffer[nread] = 0;
      return std::string(buffer);
    }
  }
  return "<none>";
}

// ---------------------------------------------------------------------------
/** 
 * Return the time as <seconds>.<nanoseconds> in a string
 * @param stime XrdOucString where to store the time as text
 * @return const char* to XrdOucString object passed
 */
// ---------------------------------------------------------------------------  

const char*
StringConversion::TimeNowAsString (XrdOucString& stime)
{
  struct timespec ts;
  eos::common::Timing::GetTimeSpec(ts);
  char tb[128];
  snprintf(tb, sizeof (tb) - 1, "%lu.%lu", ts.tv_sec, ts.tv_nsec);
  stime = tb;
  return stime.c_str();
}

// ---------------------------------------------------------------------------
/** 
 * Mask a tag 'key=val' as 'key=<...>' in an opaque string
 * @param XrdOucString where to mask
 * @return pointer to string where the masked string is stored
 */
// ---------------------------------------------------------------------------  

const char*
StringConversion::MaskTag (XrdOucString& line, const char* tag)
{
  XrdOucString smask = tag;
  smask += "=";
  int spos = line.find(smask.c_str());
  int epos = line.find("&", spos + 1);
  if (spos != STR_NPOS)
  {
    if (epos != STR_NPOS)
    {
      line.erase(spos, epos - spos);
    }
    else
    {
      line.erase(spos);
    }
    smask += "<...>";
    line.insert(smask.c_str(), spos);
  }
  return line.c_str();
}


// ---------------------------------------------------------------------------
/** 
 * Parse a string as an URL (does not deal with opaque information)
 * @param url string to parse
 * @param &protocol - return of the protocol identifier
 * @param &hostport - return of the host(port) identifier
 * @return pointer to file path inside the url
 */
// ---------------------------------------------------------------------------  

const char*
StringConversion::ParseUrl (const char* url, XrdOucString& protocol, XrdOucString& hostport)
{
  protocol = url;
  hostport = url;
  int ppos = protocol.find(":/");
  if (ppos != STR_NPOS)
  {
    protocol.erase(ppos);
  }
  else
  {
    if (protocol.beginswith("as3:"))
    {
      protocol = "as3";
    }
    else
    {
      protocol = "file";
    }
  }
  if (protocol == "file")
  {
    if (hostport.beginswith("file:"))
    {
      hostport = "";
      return (url + 5);
    }
    else
    {
      hostport = "";
      return url;
    }
  }
  if (protocol == "root")
  {
    int spos = hostport.find("//", ppos + 2);
    if (spos == STR_NPOS)
    {
      return 0;
    }
    hostport.erase(spos);
    hostport.erase(0, 7);
    return (url + spos + 1);
  }

  if (protocol == "as3")
  {
    if (hostport.beginswith("as3://"))
    {
      // as3://<hostname>/<bucketname>/<filename> like in ROOT
      int spos = hostport.find("/", 6);
      if (spos != STR_NPOS)
      {
        hostport.erase(spos);
        hostport.erase(0, 6);
        return (url + spos + 1);
      }
      else
      {
        return 0;
      }
    }
    else
    {
      // as3:<bucketname>/<filename>
      hostport = "";
      return (url + 4);
    }

  }
  if (protocol == "http")
  {
    // http://<hostname><path>
    int spos = hostport.find("/", 7);
    if (spos == STR_NPOS)
    {
      return 0;
    }
    hostport.erase(spos);
    hostport.erase(0, 7);
    return (url + spos);
  }
  if (protocol == "gsiftp")
  {
    // gsiftp://<hostname><path>
    int spos = hostport.find("/", 9);
    if (spos == STR_NPOS)
    {
      return 0;
    }
    hostport.erase(spos);
    hostport.erase(0, 9);
    return (url + spos);
  }
  return 0;
}

// ---------------------------------------------------------------------------
/** 
 * Create an Url 
 * @param protocol - name of the protocol
 * @param hostport - host[+port] Ï
 * @param path     - path name
 * @param @url     - returned URL string
 * @return char* to returned URL string
 */
// ---------------------------------------------------------------------------  

const char*
StringConversion::CreateUrl (const char* protocol, const char* hostport, const char* path, XrdOucString& url)
{
  if (!strcmp(protocol, "file"))
  {
    url = path;
    return url.c_str();
  }
  if (!strcmp(protocol, "root"))
  {
    url = "root://";
    url += hostport;
    url += "/";
    url += path;
    return url.c_str();
  }

  if (!strcmp(protocol, "as3"))
  {
    if (hostport && strlen(hostport))
    {
      url = "as3://";
      url += hostport;
      url += path;
      return url.c_str();
    }
    else
    {
      url = "as3:";
      url += path;
      return url.c_str();
    }
  }
  if (!strcmp(protocol, "http"))
  {
    url = "http://";
    url += hostport;
    url += path;
    return url.c_str();
  }
  if (!strcmp(protocol, "gsiftp"))
  {
    url = "gsiftp://";
    url += hostport;
    url += path;
    return url.c_str();
  }
  url = "";
  return 0;
}


// ---------------------------------------------------------------------------
// Convert numeric value to string in a pretty way using KB, MB etc. symbols
// ---------------------------------------------------------------------------  
std::string
StringConversion::GetPrettySize(float size)
{
  float fsize = 0;
  std::string ret_str;
  std::string size_unit;

  if ((fsize = size / EB) >= 1) size_unit = "EB";
  else if ((fsize = size / PB) >= 1) size_unit = "PB";
  else if ((fsize = size / TB) >= 1) size_unit = "TB";
  else if ((fsize = size / MB) >= 1) size_unit = "MB";
  else {
    fsize = size / KB;
    size_unit = "KB";
  }

  char msg[30];
  sprintf(msg, "%.1f %s", fsize, size_unit.c_str());

  ret_str = msg;
  return ret_str;  
}


// ---------------------------------------------------------------------------
//! Constructor
// ---------------------------------------------------------------------------

StringConversion::StringConversion () { };

// ---------------------------------------------------------------------------
//! Destructor
// ---------------------------------------------------------------------------

StringConversion::~StringConversion () { };

/*----------------------------------------------------------------------------*/
EOSCOMMONNAMESPACE_END

