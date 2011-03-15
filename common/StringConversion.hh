#ifndef __EOSCOMMON_STRINGCONVERSION__
#define __EOSCOMMON_STRINGCONVERSION__

/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
/*----------------------------------------------------------------------------*/
#include <string>
#include <vector>
#include <stdio.h>
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

class StringConversion {
public:

  /*----------------------------------------------------------------------------*/  
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
  
  /*----------------------------------------------------------------------------*/
  static const char* 
  GetReadableSizeString(XrdOucString& sizestring, unsigned long long insize, const char* unit) {
    char formsize[1024];
    if (insize >= 1000) {
      if (insize >= (1000*1000)) {
	if (insize >= (1000l*1000l*1000l)) {
	  if (insize >= (1000l*1000l*1000l*1000l)) {
	    // TB
	    sprintf(formsize,"%.02f T%s",insize*1.0 / (1000l*1000l*1000l*1000l), unit);
	  } else {
	    // GB
	    sprintf(formsize,"%.02f G%s",insize*1.0 / (1000l*1000l*1000l), unit);
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

/*----------------------------------------------------------------------------*/
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
    
    if (sizestring.endswith("T") || sizestring.endswith("t")) {
      convfactor = 1000l*1000l*1000l*1000l;
    }
    
    if (sizestring.endswith("G") || sizestring.endswith("g")) {
      convfactor = 1000l*1000l*1000l;
    }
    
    if (sizestring.endswith("M") || sizestring.endswith("m")) {
      convfactor = 1000l*1000l;
    }
    
    if (sizestring.endswith("K") || sizestring.endswith("k")) {
      convfactor = 1000l;
    }
    if (convfactor >1)
      sizestring.erase(sizestring.length()-1);
    return (strtoll(sizestring.c_str(),0,10) * convfactor);
  }
  
  /*----------------------------------------------------------------------------*/
  static const char*
  GetSizeString(XrdOucString& sizestring, unsigned long long insize) {
    char buffer[1024];
    sprintf(buffer,"%llu", insize);
    sizestring = buffer;
    return sizestring.c_str();
  }
  
  /*----------------------------------------------------------------------------*/
  static const char*
  GetSizeString(XrdOucString& sizestring, double insize) {
    char buffer[1024];
    sprintf(buffer,"%.02f", insize);
    sizestring = buffer;
    return sizestring.c_str();
  }
  
  /*----------------------------------------------------------------------------*/
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
  
  
  StringConversion() {};
  ~StringConversion() {};
};

EOSCOMMONNAMESPACE_END

#endif
