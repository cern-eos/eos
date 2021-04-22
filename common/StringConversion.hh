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
 * @brief  Convenience class to deal with strings.
 */

#ifndef __EOSCOMMON_STRINGCONVERSION__
#define __EOSCOMMON_STRINGCONVERSION__

#include "common/Namespace.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "fmt/format.h"
#include <string>
#include <vector>
#include <algorithm>
#include <map>
#include <set>
#include <memory>
#include <stdio.h>
#include <limits.h>
#include <errno.h>
#include <string.h>
#include <fstream>
#include <sstream>
#include <openssl/sha.h>

typedef void CURL;

EOSCOMMONNAMESPACE_BEGIN

//! Constants used throughout the code
const uint64_t KB = 1024;
const uint64_t MB = 1024 * KB;
const uint64_t GB = 1024 * MB;
const uint64_t TB = 1024 * GB;
const uint64_t PB = 1024 * TB;
const uint64_t EB = 1024 * PB;

#define LC_STRING(x) eos::common::StringConversion::ToLower((x))

//------------------------------------------------------------------------------
//! Static helper class with convenience functions for string tokenizing,
//! value2string and split functions.
//------------------------------------------------------------------------------
class StringConversion
{
public:

  // ---------------------------------------------------------------------------
  /**
   * Tokenize a string
   *
   * @param str string to be tokenized
   * @param tokens  returned list of separated string tokens
   * @param delimiters delimiter used for tokenizing
   */
  // ----------------------------------------------------------------------------
  static void Tokenize(const std::string& str,
                       std::vector<std::string>& tokens,
                       const std::string& delimiters = " ");

  // ---------------------------------------------------------------------------
  /**
   * Tokenize a string accepting also empty members e.g. a||b returns 3 fields
   *
   * @param str string to be tokenized
   * @param tokens  returned list of separated string tokens
   * @param delimiters delimiter used for tokenizing
   */
  // ---------------------------------------------------------------------------
  static void EmptyTokenize(const std::string& str,
                            std::vector<std::string>& tokens,
                            const std::string& delimiters = " ");


  // ---------------------------------------------------------------------------
  /**
   * Convert a string buffer to a hex dump string
   *
   * @param string to dump
   *
   * @return hex dumped string
   */
  // ---------------------------------------------------------------------------
  static std::string string_to_hex(const std::string& input);
  static std::string char_to_hex(const char input);

  //----------------------------------------------------------------------------
  //! Convert binary string given as a char* and length to hex string
  //! representation
  //!
  //! @param buf buffer holding the checksum in binary format
  //! @param buf_len size of the checksum buffer
  //! @param nominal_len expected size of the checksum in bytes
  //! @param separator possible separator for the display of the converted
  //!        string
  //! @param return string holding the hex representation of the checksum or
  //!        empty strin if nothing is converted
  //----------------------------------------------------------------------------
  static std::string
  BinData2HexString(const char* buf, const size_t buf_len,
                    const size_t nominal_len = SHA_DIGEST_LENGTH,
                    const char separator = 0x00);


  //----------------------------------------------------------------------------
  //! Convert checksum hex representation to binary string
  //!
  //! @param shex string hex representation of checksum
  //! @param out_size buffer size of the binary data
  //!
  //! @return array of chars holding the binary data representaion of the
  //!         checksum or nullptr if there were any errors
  //----------------------------------------------------------------------------
  static std::unique_ptr<char[]>
  Hex2BinDataChar(const std::string& shex, size_t& out_size,
                  const size_t nominal_len = SHA_DIGEST_LENGTH);

  // ---------------------------------------------------------------------------
  /**
   * Convert a long long value into time s,m,h,d  scale
   *
   * @param sizestring returned XrdOuc string representation
   * @param seconds number to convert
   *
   * @return sizestring.c_str()
   */
  // ---------------------------------------------------------------------------
  static const char*
  GetReadableAgeString(XrdOucString& sizestring,
                       unsigned long long age);


  // ---------------------------------------------------------------------------
  /**
   * Get a string with a width and precision formatted double value
   * @param value - the double to print
   * @param width - the total width of the printout
   * @param percision - the precision to show after the .
   *
   * @return formated string showing double value
   **/

  static std::string
  GetFixedDouble(double value, size_t width, size_t precision);

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
  GetReadableSizeString(XrdOucString& sizestring,
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
  GetReadableSizeString(std::string& sizestring,
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
  GetSizeFromString(const char* sizestring);

  static unsigned long long
  GetSizeFromString(const XrdOucString& sizestring)
  {
    return GetSizeFromString(sizestring.c_str());
  }

  static unsigned long long
  GetSizeFromString(const std::string& sizestring)
  {
    return GetSizeFromString(sizestring.c_str());
  }

  //----------------------------------------------------------------------------
  //! Get size from the given string, return true if parsing was successful,
  //! false otherwise
  //----------------------------------------------------------------------------
  static bool GetSizeFromString(const std::string& sizestring, uint64_t& out);

  // ---------------------------------------------------------------------------
  /**
   * Convert a readable string into a number, only for data
   *
   * @param sizestring readable string like 4KB or 1000GB
   *
   * @return number
   */
  // ----------------------------------------------------------------------------
  static unsigned long long
  GetDataSizeFromString(const char* sizestring);

  static unsigned long long
  GetDataSizeFromString(const XrdOucString& sizestring)
  {
    return GetDataSizeFromString(sizestring.c_str());
  }

  static unsigned long long
  GetDataSizeFromString(const std::string& sizestring)
  {
    return GetDataSizeFromString(sizestring.c_str());
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
  GetSizeString(XrdOucString& sizestring, unsigned long long insize);

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
  GetSizeString(std::string& sizestring, unsigned long long insize);

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
  GetSizeString(XrdOucString& sizestring, double insize);

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
  GetSizeString(std::string& sizestring, double insize);

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
  SplitKeyValue(std::string keyval, std::string& key, std::string& value,
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
  SplitKeyValue(XrdOucString keyval, XrdOucString& key, XrdOucString& value,
                XrdOucString split = ":");

  // ---------------------------------------------------------------------------
  /**
   * Split a comma separated key:val list and fill it into a map
   *
   * @param mapstring map string to parse
   * @param map return map after parsing if ok
   * @param split separator used to separate key from value default ":"
   * @param delimiter separator used to separate individual key value pairs
   * @param keyvector returns optional the order of the keys in a vector
   * @return true if format ok, otherwise false
   */
  // ---------------------------------------------------------------------------
  static bool
  GetKeyValueMap(const char* mapstring,
                 std::map<std::string, std::string>& map,
                 const char* split = ":",
                 const char* delimiter = ",",
                 std::vector<std::string>* keyvector = 0);


  // ---------------------------------------------------------------------------
  /**
   * Replace a key in a string,string map
   *
   * @return true if replaced
   */
  // ---------------------------------------------------------------------------
  static bool
  ReplaceMapKey(std::map<std::string, std::string>& map, const char* oldk,
                const char* newk)
  {
    if (map.count(oldk)) {
      map[newk] = map[oldk];
      map.erase(oldk);
      return true;
    }

    return false;
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
  GetHostPortFromQueue(const char* queue);

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
  GetStringHostPortFromQueue(const char* queue);

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
  SplitByPoint(std::string in, std::string& pre, std::string& post);

  // ---------------------------------------------------------------------------
  /**
   * Convert a string into a line-wise map
   *
   * @param in char*
   * @param out vector with std::string lines
   */
  // ---------------------------------------------------------------------------
  static void
  StringToLineVector(char* in, std::vector<std::string>& out);

  // ---------------------------------------------------------------------------
  /**
   * Split a string of type '<string>@<int>[:<0xXXXXXXXX] into string,int,
   * std::set<unsigned long long>'.
   *
   * @param in char*
   * @param tag string
   * @param id unsigned long
   * @param set std::set<unsigned long long>
   * @return true if parsed, false if format error
   */
  // ---------------------------------------------------------------------------
  static bool
  ParseStringIdSet(char* in, std::string& tag, unsigned long& id,
                   std::set<unsigned long long>& set);

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
  LoadFileIntoString(const char* filename, std::string& out);

  // ---------------------------------------------------------------------------
  /**
   * Save a string into  a text file <name>
   *
   * @param filename where to save the contents
   * @param in string with the contents
   * @return (const char*) pointer to loaded string
   */
  // ---------------------------------------------------------------------------
  static bool
  SaveStringIntoFile(const char* filename, const std::string& in);

  // ---------------------------------------------------------------------------
  /**
   * Read a long long number as output of a shell command - this is not useful
   * in multi-threaded environments.
   *
   * @param shellcommand to execute
   * @return long long value of converted shell output
   */
  // ---------------------------------------------------------------------------
  static long long
  LongLongFromShellCmd(const char* shellcommand);

  // ---------------------------------------------------------------------------
  /**
   * Read a string as output of a shell command - this is not useful in
   * multi-threaded environments.
   *
   * @param shellcommand to execute
   * @return XrdOucString
   */
  // ---------------------------------------------------------------------------
  static std::string
  StringFromShellCmd(const char* shellcommand);

  // ---------------------------------------------------------------------------
  /**
   * Return the time as <seconds>.<nanoseconds> in a string
   *
   * @param stime XrdOucString where to store the time as text
   * @return const char* to XrdOucString object passed
   */
  // ---------------------------------------------------------------------------
  static const char*
  TimeNowAsString(XrdOucString& stime);

  // ---------------------------------------------------------------------------
  /**
   * Mask a tag 'key=val' as 'key=<...>' in an opaque string
   *
   * @param XrdOucString where to mask
   * @return pointer to string where the masked string is stored
   */
  // ---------------------------------------------------------------------------
  static const char*
  MaskTag(XrdOucString& line, const char* tag);

  // ---------------------------------------------------------------------------
  /**
   * Parse a string as an URL (does not deal with opaque information)
   *
   * @param url string to parse
   * @param &protocol - return of the protocol identifier
   * @param &hostport - return of the host(port) identifier
   * @return pointer to file path inside the url
   */
  // ---------------------------------------------------------------------------
  static const char*
  ParseUrl(const char* url, XrdOucString& protocol, XrdOucString& hostport);

  // ---------------------------------------------------------------------------
  /**
   * Convert numeric value to string in a pretty way using KB, MB or GB symbols
   *
   * @param size size in KB to be processed
   * @return string representation of the value in a pretty format
   */
  // ---------------------------------------------------------------------------
  static std::string
  GetPrettySize(float size);

  // ---------------------------------------------------------------------------
  /**
   * Create an URL
   *
   * @param protocol - name of the protocol
   * @param hostport - host[+port]
   * @param path     - path name
   * @param @url     - returned URL string
   * @return char* to returned URL string
   */
  // ---------------------------------------------------------------------------
  static const char*
  CreateUrl(const char* protocol, const char* hostport, const char* path,
            XrdOucString& url);

  // ---------------------------------------------------------------------------
  /**
   * Check if a string is a hexadecimal number
   *
   * @param hexstring - hexadecimal string
   * @param format - format used for printing e.g. %08x
   * @return true if it is a converted hex number otherwise false
   */
  // ---------------------------------------------------------------------------
  static bool
  IsHexNumber(const char* hexstring, const char* format = "%08x");

  // ---------------------------------------------------------------------------
  /**
   * Return a lower case string
   *
   * @param input - input string
   * @return lower case string
   */
  // ---------------------------------------------------------------------------
  static std::string
  ToLower(std::string is)
  {
    std::transform(is.begin(), is.end(), is.begin(), ::tolower);
    return is;
  }

  // ---------------------------------------------------------------------------
  /**
   * Return a lower case string
   *
   * @param input - input string
   * @return lower case string
   */
  // ---------------------------------------------------------------------------
  static std::string
  ToLower(const char* s_is)
  {
    std::string is = s_is;
    std::transform(is.begin(), is.end(), is.begin(), ::tolower);
    return is;
  }

  // ---------------------------------------------------------------------------
  /**
   * Return an octal string
   * @param number - integer
   * @param minimum format length
   * @return octal number as string
   */
  // ---------------------------------------------------------------------------
  static std::string
  IntToOctal(int number, int digits = 4)
  {
    char format[16];
    snprintf(format, sizeof(format), "%%0%do", digits);
    char octal[32];
    snprintf(octal, sizeof(octal), format, number);
    return std::string(octal);
  }

  static void InitLookupTables()
  {
    for (int i = 0; i < 10; i++) {
      pAscii2HexLkup['0' + i] = i;
      pHex2AsciiLkup[i] = '0' + i;
    }

    for (int i = 0; i < 6; i++) {
      pAscii2HexLkup['a' + i] = 10 + i;
      pHex2AsciiLkup[10 + i] = 'a' + i;
    }
  }

  // ---------------------------------------------------------------------------
  /**
   * @param u templated unsigned number to be converted in hexadecimal
   * @param s buffer to write the result to
   *
   * @return the address of the last character written in the buffer + 1
   */
  // ---------------------------------------------------------------------------
  template <typename UnsignedType> static char*
  FastUnsignedToAsciiHex(UnsignedType u, char* s)
  {
    if (!u) {
      *s = '0';
      return s + 1;
    }

    int nchar = 0;
    const int size = 2 * sizeof(UnsignedType);

    for (int j = 1; j <= size; j++) {
      int digit = (u >> ((size - j) << 2)) & 15;

      if (!nchar && !digit) {
        continue;
      }

      s[nchar++] = pHex2AsciiLkup[digit];
    }

    return s + nchar;
  }

  // ---------------------------------------------------------------------------
  /**
   * @param u templated unsigned number to be converted in hexadecimal
   *
   * @return the hex number as string
   */
  // ---------------------------------------------------------------------------
  template <typename UnsignedType> static std::string
  FastUnsignedToAsciiHex(UnsignedType u)
  {
    std::ostringstream oss;

    if (!u) {
      oss << '0';
      return oss.str();
    }

    const int size = 2 * sizeof(UnsignedType);
    bool hasChars = false;

    for (int j = 1; j <= size; j++) {
      int digit = (u >> ((size - j) << 2)) & 15;

      if (hasChars || digit != 0) {
        oss << pHex2AsciiLkup[digit];
        hasChars = true;
      }
    }

    return oss.str();
  }

  // ---------------------------------------------------------------------------
  /**
   * @param the buffer to read the ascii representation from.
   * @param templated unsigned pointer to write the result to
   * @param templated len to parse in the buffer (go until null character)
   */
  // ---------------------------------------------------------------------------
  template <typename UnsignedType> static void
  FastAsciiHexToUnsigned(char* s, UnsignedType* u, int len = -1)
  {
    *u = 0;

    for (int j = 0; s[j] != 0 && j != len; j++) {
      (*u) <<= 4;
      (*u) += pAscii2HexLkup[static_cast<int>(s[j])];
    }
  }

  //----------------------------------------------------------------------------
  //! Return an escaped URI
  //!
  //! @param str - uri to escape
  //! @return escaped URI string
  //----------------------------------------------------------------------------
  static std::string
  curl_default_escaped(const std::string& str);

  //----------------------------------------------------------------------------
  //! Return an unescaped URI
  //!
  //! @param str - uri to unescape
  //! @return unescaped URI string
  //----------------------------------------------------------------------------
  static std::string
  curl_default_unescaped(const std::string& str);

  // ---------------------------------------------------------------------------
  /**
   * Return an unescaped URI
   *
   * @param str - uri to unescape
   * @return unescaped URI string
   */
  // ---------------------------------------------------------------------------
  static std::string
  curl_unescaped(const std::string& str);

  // ---------------------------------------------------------------------------
  /**
   * Return an escaped URI
   *
   * @param str - uri to escape
   * @return escaped URI string
   */
  // ---------------------------------------------------------------------------
  static std::string
  curl_escaped(const std::string& str);

  // ---------------------------------------------------------------------------
  /**
   * Return an enocded json string
   * @param str - string to escape
   * @return escaped string
   */
  // ---------------------------------------------------------------------------

  static std::string
  json_encode(const std::string& str);

  // ---------------------------------------------------------------------------
  /**
   * Return a random generated uuid
   * @return uuid string
   */
  // ---------------------------------------------------------------------------
  static std::string
  random_uuidstring();

  //----------------------------------------------------------------------------
  /**
   * Returns a time-based generated uuid
   *
   * @return uuid string
   */
  //----------------------------------------------------------------------------
  static std::string timebased_uuidstring();

  // ---------------------------------------------------------------------------
  /**
   * Sort lines alphabetically in-place
   * @param data input data
   */
  // ---------------------------------------------------------------------------
  static void SortLines(XrdOucString& data);


  //------------------------------------------------------------------------------
  //! Fast convert element to string representation
  //!
  //! @param elem element to be converted
  //!
  //! @return string representation
  //------------------------------------------------------------------------------
  template <typename T>
  static std::string stringify(const T& elem)
  {
    return fmt::to_string(elem);
  }

  //------------------------------------------------------------------------------
  //! Replace a substring with another substring in a string
  //------------------------------------------------------------------------------
  static void ReplaceStringInPlace(std::string& subject,
                                   const std::string& search,
                                   const std::string& replace)
  {
    if (subject.empty() || search.empty() || replace.empty()) {
      return;
    }

    size_t pos = 0;

    while ((pos = subject.find(search)) != std::string::npos) {
      subject.replace(pos, search.length(), replace);
      pos += replace.length();
    }
  }


  //------------------------------------------------------------------------------
  //! Replace multiple characters in a string
  //------------------------------------------------------------------------------
  static void Replace(std::string& subject,
                      const char a,
                      const char b)
  {
    if (subject.empty()) {
      return;
    }

    for (size_t i = 0; i < subject.size(); ++i) {
      if (subject[i] == a) {
        subject[i] = b;
      }
    }
  }

  //----------------------------------------------------------------------------
  //! Check if a string is a valid UTF-8 string
  //----------------------------------------------------------------------------
  static bool Valid_UTF8(const string& string);

  //----------------------------------------------------------------------------
  //! CGI encode invalid UTF8 strings, valid just pass through
  //----------------------------------------------------------------------------
  static std::string EncodeInvalidUTF8(const string& string);

  //----------------------------------------------------------------------------
  //! CGI decode invalid UTF8 strings, valid just pass through
  //----------------------------------------------------------------------------
  static std::string DecodeInvalidUTF8(const string& string)
  {
    return curl_unescaped(string);
  }

  //----------------------------------------------------------------------------
  //! Seal opaque xrootd info i.e. replace any & with #AND#
  //!
  //! @param input string to be sealed
  //!
  //! @return newly sealed string
  //----------------------------------------------------------------------------
  static std::string SealXrdOpaque(const std::string& input);

  //----------------------------------------------------------------------------
  //! Unseal opaque xrootd inf i.e. replace any #AND# with &
  //!
  //! @param input string to be unsealed
  //!
  //! @return newly unsealed string
  //----------------------------------------------------------------------------
  static std::string UnsealXrdOpaque(const std::string& input);

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  StringConversion() {};

  //----------------------------------------------------------------------------
  //! Destructor
  // ---------------------------------------------------------------------------
  ~StringConversion() {};

private:
  //! Lookup Table for Hex Ascii Conversion
  static char pAscii2HexLkup[256];
  static char pHex2AsciiLkup[16];
  static thread_local CURL* curl;
  //! Thread-local storage management
  static pthread_key_t sPthreadKey;
  static pthread_once_t sInit;
  static void tlCurlFree(void* arg);
  static CURL* tlCurlInit();
  static void tlInitThreadKey();
};


//------------------------------------------------------------------------------
//! Static cURL global initializer
//------------------------------------------------------------------------------
static struct CurlGlobalInitializer {
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  CurlGlobalInitializer();

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~CurlGlobalInitializer();
} sCurlGlobalInit; ///< Static initializer for every translation unit

EOSCOMMONNAMESPACE_END
#endif
