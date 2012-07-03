// ----------------------------------------------------------------------
// File: StringConversion.hh
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

/**
 * @file   StringConversion.hh
 * 
 * @brief  Convenience class to deal with strings.
 * 
 * 
 */

#ifndef __EOSCOMMON_STRINGCONVERSION__
#define __EOSCOMMON_STRINGCONVERSION__

/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
/*----------------------------------------------------------------------------*/
#include <string>
#include <vector>
#include <set>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <fstream>
#include <sstream>
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
//! Static helper class with convenience functinons for string tokenizing, value2string and split functions
/*----------------------------------------------------------------------------*/
class StringConversion {
public:

  // ---------------------------------------------------------------------------
  /** 
   * Tokenize a string
   * 
   * @param str string to be tokenized
   * @param tokens  returned list of seperated string tokens
   * @param delimiters delimiter used for tokenizing
   */
  // ---------------------------------------------------------------------------
  static void Tokenize(const std::string& str,
                       std::vector<std::string>& tokens,
                       const std::string& delimiters = " ")
  {
    // Skip delimiters at beginning.
    std::string::size_type lastPos = str.find_first_not_of(delimiters, 0);
    // Find first "non-delimiter".
    std::string::size_type pos     = str.find_first_of(delimiters, lastPos);
    
    while (std::string::npos != pos || std::string::npos != lastPos) {
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
  static void EmptyTokenize(const std::string& str,
                       std::vector<std::string>& tokens,
                       const std::string& delimiters = " ")
  {
    // Skip delimiters at beginning.
    std::string::size_type lastPos = str.find_first_not_of(delimiters, 0);
    // Find first "non-delimiter".
    std::string::size_type pos     = str.find_first_of(delimiters, lastPos);
    
    while (std::string::npos != pos || std::string::npos != lastPos) {
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
  static const char* 
  GetReadableSizeString(XrdOucString& sizestring, unsigned long long insize, const char* unit) {
    char formsize[1024];
    if (insize >= 1000) {
      if (insize >= (1000*1000)) {
        if (insize >= (1000ll*1000ll*1000ll)) {
          if (insize >= (1000ll*1000ll*1000ll*1000ll)) {
            if (insize >= (1000ll*1000ll*1000ll*1000ll*1000ll)) {
              if (insize >= (1000ll*1000ll*1000ll*1000ll*1000ll*1000ll)) {
                // EB
                sprintf(formsize,"%.02f E%s",insize*1.0 / (1000ll*1000ll*1000ll*1000ll*1000ll*1000ll), unit);
              } else {
                // PB
                sprintf(formsize,"%.02f P%s",insize*1.0 / (1000ll*1000ll*1000ll*1000ll*1000ll), unit);
              }
            } else {
              // TB
              sprintf(formsize,"%.02f T%s",insize*1.0 / (1000ll*1000ll*1000ll*1000ll), unit);
            }
          } else {
            // GB
            sprintf(formsize,"%.02f G%s",insize*1.0 / (1000ll*1000ll*1000ll), unit);
          }
        } else {
          // MB
          sprintf(formsize,"%.02f M%s",insize*1.0 / (1000*1000),unit);
        }
      } else {
        // kB
        sprintf(formsize,"%.02f k%s",insize*1.0 / (1000),unit);
      }
    } else {
      if (strlen(unit)) {
        sprintf(formsize,"%.02f %s",insize*1.0, unit);
      } else {
        sprintf(formsize,"%.02f",insize*1.0);
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
  static unsigned long long 
  GetSizeFromString(XrdOucString sizestring) {
    errno = 0;
    unsigned long long convfactor;
    convfactor = 1ll;
    if (!sizestring.length()) {
      errno = EINVAL;
      return 0;
    }
    
    if (sizestring.endswith("B") || sizestring.endswith("b")) {
      sizestring.erase(sizestring.length()-1);
    }
 
    if (sizestring.endswith("E") || sizestring.endswith("e")) {
      convfactor = 1000ll*1000ll*1000ll*1000ll*1000ll*1000ll;
    }
   
    if (sizestring.endswith("P") || sizestring.endswith("p")) {
      convfactor = 1000ll*1000ll*1000ll*1000ll*1000ll;
    }

    if (sizestring.endswith("T") || sizestring.endswith("t")) {
      convfactor = 1000ll*1000ll*1000ll*1000ll;
    }
    
    if (sizestring.endswith("G") || sizestring.endswith("g")) {
      convfactor = 1000ll*1000ll*1000ll;
    }
    
    if (sizestring.endswith("M") || sizestring.endswith("m")) {
      convfactor = 1000ll*1000ll;
    }
    
    if (sizestring.endswith("K") || sizestring.endswith("k")) {
      convfactor = 1000ll;
    }
    if (convfactor >1)
      sizestring.erase(sizestring.length()-1);

    if ( (sizestring.find("."))!=STR_NPOS) {
      return ((unsigned long long) (strtod(sizestring.c_str(),NULL) * convfactor));
    } else {
      return (strtoll(sizestring.c_str(),0,10) * convfactor);
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
  static const char* 
  GetReadableSizeString(std::string& sizestring, unsigned long long insize, const char* unit) {
    const char* ptr=0;
    XrdOucString oucsizestring="";
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
  static const char*
  GetSizeString(std::string& sizestring, unsigned long long insize) {
    char buffer[1024];
    sprintf(buffer,"%llu", insize);
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
  static const char*
  GetSizeString(XrdOucString& sizestring, unsigned long long insize) {
    char buffer[1024];
    sprintf(buffer,"%llu", insize);
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
  static const char*
  GetSizeString(XrdOucString& sizestring, double insize) {
    char buffer[1024];
    sprintf(buffer,"%.02f", insize);
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
  static bool 
  SplitKeyValue(std::string keyval, std::string &key, std::string &value) {
    int equalpos = keyval.find(":");
    if (equalpos != STR_NPOS) {
      key.assign(keyval,0,equalpos-1);
      value.assign(keyval,equalpos+1, keyval.length()-(equalpos+1));
      return true;
    } else {
      key=value="";
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
  static bool 
  SplitKeyValue(XrdOucString keyval, XrdOucString &key, XrdOucString &value) {
    int equalpos = keyval.find(":");
    if (equalpos != STR_NPOS) {
      key.assign(keyval,0,equalpos-1);
      value.assign(keyval,equalpos+1);
      return true;
    } else {
      key=value="";
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
  static XrdOucString
  GetHostPortFromQueue(const char* queue) {
    XrdOucString hostport = queue;
    int pos = hostport.find("/",2);
    if (pos != STR_NPOS) {
      hostport.erase(0, pos+1);
      pos = hostport.find("/");
      if (pos != STR_NPOS) {
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
  static std::string
  GetStringHostPortFromQueue(const char* queue) {
    std::string hostport = queue;
    int pos = hostport.find("/",2);
    if (pos != STR_NPOS) {
      hostport.erase(0, pos+1);
      pos = hostport.find("/");
      if (pos != STR_NPOS) {
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
  static void
  SplitByPoint(std::string in, std::string &pre, std::string &post) {
    std::string::size_type dpos=0;
    pre = in;
    post = in;
    if ( ( dpos = in.find(".") ) != std::string::npos) {
      std::string s = in;
      post.erase(0,dpos+1);
      pre.erase(dpos);
    } else {
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
  
  static void
  StringToLineVector(char* in, std::vector<std::string> &out) {
    char* pos = in;
    char* old_pos = in;
    int len = strlen(in);
    while ( (pos = strchr(pos,'\n') ) ) {
      *pos = 0;
      out.push_back(old_pos);
      *pos = '\n';
      pos++;
      old_pos = pos;
      // check for the end of string
      if ((pos-in)>=len) 
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
  static bool
  ParseStringIdSet(char* in, std::string& tag, unsigned long& id, std::set<unsigned long long> &set)
  {
    char* ptr = in;
    char* add = strchr(in,'@');
    if (!add)
      return false;

    char* colon = strchr(add,':');

    if (!colon) {
      id = strtoul(add+1,0,10);
      if (id) {
	return true;
      } else {
	return false;
      }
    } else {
      *colon = 0;
      id = strtoul(add+1,0,10);
      *colon = ':';
    }

    *add = 0;
    tag = ptr;
    *add = '@';
    
    ptr = colon+1;
    do {
      char* nextcolon = strchr(ptr,':');
      // get a set member
      if (nextcolon) {
	*nextcolon=0;
	unsigned long long n = strtoull(ptr,0,16);
	*nextcolon=':';
	set.insert(n);
	ptr = nextcolon+1;
      } else {
	unsigned long long n = strtoull(ptr,0,16);
	set.insert(n);
	return true;
      }
    } while(1);
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
  static const char*
  LoadFileIntoString(const char* filename, std::string &out)
  {
    std::ifstream load(filename);
    std::stringstream buffer;

    buffer << load.rdbuf();
    out=buffer.str();
    return out.c_str();
  }

  // ---------------------------------------------------------------------------
  //! Constructor
  // ---------------------------------------------------------------------------
  StringConversion() {};

  // ---------------------------------------------------------------------------
  //! Destructor
  // ---------------------------------------------------------------------------
  ~StringConversion() {};
};

/*----------------------------------------------------------------------------*/
EOSCOMMONNAMESPACE_END

#endif
