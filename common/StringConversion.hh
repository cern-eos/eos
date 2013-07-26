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
#include "common/Timing.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
/*----------------------------------------------------------------------------*/
#include <string>
#include <vector>
#include <map>
#include <set>
#include <stdio.h>
#include <limits.h>
#include <errno.h>
#include <string.h>
#include <fstream>
#include <sstream>

/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

  //! Constants used throughout the code
  const uint64_t KB = 1024;
const uint64_t MB = 1024 * KB;
const uint64_t GB = 1024 * MB;
const uint64_t TB = 1024 * GB;
const uint64_t PB = 1024 * TB;
const uint64_t EB = 1024 * PB;

/*----------------------------------------------------------------------------*/
//! Static helper class with convenience functions for string tokenizing, value2string and split functions

/*----------------------------------------------------------------------------*/
class StringConversion
{
public:

  // ---------------------------------------------------------------------------
  /** 
   * Tokenize a string
   * 
   * @param str string to be tokenized
   * @param tokens  returned list of seperated string tokens
   * @param delimiters delimiter used for tokenizing
   */
  // ----------------------------------------------------------------------------
  static void Tokenize (const std::string& str,
                        std::vector<std::string>& tokens,
                        const std::string& delimiters = " ");


  // ---------------------------------------------------------------------------
  /** 
   * Tokenize a string accepting also empty members e.g. a||b is returning 3 fields
   * 
   * @param str string to be tokenized
   * @param tokens  returned list of seperated string tokens
   * @param delimiters delimiter used for tokenizing
   */
  // ---------------------------------------------------------------------------
  static void EmptyTokenize (const std::string& str,
                             std::vector<std::string>& tokens,
                             const std::string& delimiters = " ");

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
  GetReadableSizeString (XrdOucString& sizestring,
                         unsigned long long insize,
                         const char* unit);

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
  GetReadableSizeString (std::string& sizestring,
                         unsigned long long insize,
                         const char* unit);

  // ---------------------------------------------------------------------------
  /** 
   * Convert a readable string into a number
   * 
   * @param sizestring readable string like 4KB or 1000GB or 1s,1d,1y
   * 
   * @return number
   */
  // ----------------------------------------------------------------------------
  static unsigned long long
  GetSizeFromString (XrdOucString sizestring);

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
  GetSizeString (XrdOucString& sizestring, unsigned long long insize);

  // ----------------------------------------------------------------------------
  /**
   * Convert a long long number into a XrdOucString
   * 
   * @param sizestring returned string
   * @param insize number
   * 
   * @return sizestring.c_str()
   */
  // ---------------------------------------------------------------------------

  static const char*
  GetSizeString (std::string& sizestring, unsigned long long insize);

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
  GetSizeString (XrdOucString& sizestring, double insize);

  // ---------------------------------------------------------------------------
  /** 
   * Convert a floating point number into a std::string
   * 
   * @param sizestring returned string
   * @param insize number
   * 
   * @return sizestring.c_str()
   */
  // ---------------------------------------------------------------------------Ï
  static const char*
  GetSizeString (std::string& sizestring, double insize);

  // ---------------------------------------------------------------------------
  /** 
   * Split a 'key:value' definition into key + value
   * 
   * @param keyval key-val string 'key:value'
   * @param key returned key
   * @param split split character
   * @param value return value
   * 
   * @return true if parsing ok, false if wrong format
   */
  // ---------------------------------------------------------------------------  
  static bool
  SplitKeyValue (std::string keyval,
                 std::string &key,
                 std::string &value,
                 std::string split = ":");

  // ---------------------------------------------------------------------------
  /** 
   * Split a 'key:value' definition into key + value
   * 
   * @param keyval key-val string 'key:value'
   * @param key returned key
   * @param split split character
   * @param value return value
   * 
   * @return true if parsing ok, false if wrong format
   */
  // ---------------------------------------------------------------------------  
  static bool
  SplitKeyValue (XrdOucString keyval,
                 XrdOucString &key,
                 XrdOucString &value,
                 XrdOucString split = ":");

  // ---------------------------------------------------------------------------
  /**
   * Split a comma seperated key:val list and fill it into a map
   * 
   * @param mapstring map string to parse
   * @param map return map after parsing if ok
   * @param split seperator used to seperate key from value default ":"
   * @return true if format ok, otherwise false
   */
  // ---------------------------------------------------------------------------
  static bool
  GetKeyValueMap (const char* mapstring,
                  std::map<std::string, std::string> &map,
                  const char* split = ":");


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
  GetHostPortFromQueue (const char* queue);

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
  GetStringHostPortFromQueue (const char* queue);

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
  SplitByPoint (std::string in, std::string &pre, std::string &post);

  // ---------------------------------------------------------------------------
  /** 
   * Convert a string into a line-wise map
   * 
   * @param in char*
   * @param out vector with std::string lines
   */
  // ---------------------------------------------------------------------------

  static void
  StringToLineVector (char* in, std::vector<std::string> &out);

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
  ParseStringIdSet (char* in,
                    std::string& tag,
                    unsigned long& id,
                    std::set<unsigned long long> &set);

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
  LoadFileIntoString (const char* filename, std::string &out);

  // ---------------------------------------------------------------------------
  /** 
   * Read a long long number as output of a shell command - this is not usefull in multi-threaded environments
   * 
   * @param shellcommand to execute
   * @return long long value of converted shell output
   */
  // ---------------------------------------------------------------------------  
  static long long
  LongLongFromShellCmd (const char* shellcommand);

  // ---------------------------------------------------------------------------
  /** 
   * Read a string as output of a shell command - this is not usefull in multi-threaded environments
   * 
   * @param shellcommand to execute
   * @return XrdOucString
   */
  // ---------------------------------------------------------------------------  
  static std::string
  StringFromShellCmd (const char* shellcommand);


  // ---------------------------------------------------------------------------
  /** 
   * Return the time as <seconds>.<nanoseconds> in a string
   * @param stime XrdOucString where to store the time as text
   * @return const char* to XrdOucString object passed
   */
  // ---------------------------------------------------------------------------  
  static const char*
  TimeNowAsString (XrdOucString& stime);


  // ---------------------------------------------------------------------------
  /** 
   * Mask a tag 'key=val' as 'key=<...>' in an opaque string
   * @param XrdOucString where to mask
   * @return pointer to string where the masked string is stored
   */
  // ---------------------------------------------------------------------------  
  static const char*
  MaskTag (XrdOucString& line, const char* tag);



  // ---------------------------------------------------------------------------
  /** 
   * Parse a string as an URL (does not deal with opaque information)
   * @param url string to parse
   * @param &protocol - return of the protocol identifier
   * @param &hostport - return of the host(port) identifier
   * @return pointer to file path inside the url
   */
  // ---------------------------------------------------------------------------  
  static const char*
  ParseUrl (const char* url, XrdOucString& protocol, XrdOucString& hostport);


  // ---------------------------------------------------------------------------
  /** 
   * Convert numeric value to string in a pretty way using KB, MB or GB symbols
   * @param size size in KB to be processed
   *
   * @return string representation of the value in a pretty format
   */
  // ---------------------------------------------------------------------------  
  static std::string
  GetPrettySize (float size);


  // ---------------------------------------------------------------------------
  /** 
   * Create an Url 
   * @param protocol - name of the protocol
   * @param hostport - host[+port] 
   * @param path     - path name
   * @param @url     - returned URL string
   * @return char* to returned URL string
   */
  // ---------------------------------------------------------------------------  
  static const char*
  CreateUrl (const char* protocol, const char* hostport, const char* path, XrdOucString& url);

  // ---------------------------------------------------------------------------
  /**
   * Check if a string is a hexadecimal number
   * @param hexstring - hexadecimal string
   * @param format - format used for printing e.g. %08x 
   * @return true if it is a converted hex number otherwise false
   */
  // ---------------------------------------------------------------------------
  static bool
  IsHexNumber (const char* hexstring, const char* format = "%08x");

  // ---------------------------------------------------------------------------
  //! 
  // ---------------------------------------------------------------------------
  //! Constructor
  // ---------------------------------------------------------------------------
  StringConversion ();

  // ---------------------------------------------------------------------------
  //! Destructor
  // ---------------------------------------------------------------------------
  ~StringConversion ();
};

/*----------------------------------------------------------------------------*/
EOSCOMMONNAMESPACE_END

#endif
