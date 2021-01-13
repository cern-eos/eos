// ----------------------------------------------------------------------
// File: Config.hh
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2021 CERN/Switzerland                                  *
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

#ifndef EOSCOMMON_CONFIG_HH
#define EOSCOMMON_CONFIG_HH

#include "common/Namespace.hh"
#include "common/StringConversion.hh"
#include <sstream>
#include <string>
#include <map>
#include <vector>

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// systemd style configuration file support
//------------------------------------------------------------------------------
class Config {
public:
  //----------------------------------------------------------------------------
  // Default constructor
  //----------------------------------------------------------------------------
  Config() : errcode(0) {hostname = eos::common::StringConversion::StringFromShellCmd("hostname -f");hostname.pop_back();}

  //----------------------------------------------------------------------------
  // Loader
  //----------------------------------------------------------------------------

  bool Load(const char* service, const char* name="default", bool reset=true);

  //----------------------------------------------------------------------------
  // Parse and possibly return a chapter entry
  //----------------------------------------------------------------------------
  std::string ParseChapter(const std::string& line);

  //----------------------------------------------------------------------------
  // Parse and possibly return a section entry
  //----------------------------------------------------------------------------
  std::string ParseSection(const std::string& line);

  //----------------------------------------------------------------------------
  // Is status ok?
  //----------------------------------------------------------------------------
  bool ok() const {
    return (errcode == 0);
  }

  //----------------------------------------------------------------------------
  // Get errorcode
  //----------------------------------------------------------------------------
  int getErrc() const {
    return errcode;
  }

  //----------------------------------------------------------------------------
  // Get error message
  //----------------------------------------------------------------------------
  std::string getMsg() const {
    return errorMessage;
  }

  //----------------------------------------------------------------------------
  // To string, including error code
  //----------------------------------------------------------------------------
  std::string toString() const {
    std::ostringstream ss;
    ss << "(" << errcode << "): " << errorMessage;
    return ss.str();
  }

  //----------------------------------------------------------------------------
  // Implicit conversion to boolean: Same value as ok()
  //----------------------------------------------------------------------------
  operator bool() const {
    return ok();
  }

  typedef std::vector<std::string> ConfigSection;
  typedef std::map<std::string, ConfigSection> ConfigChapter;

  //----------------------------------------------------------------------------
  // Overloaded [] operator
  //----------------------------------------------------------------------------
  ConfigSection &operator[](const char* i) {
    if (conf.count(i)) {
      return conf[i];
    } else {
      static ConfigSection none;
      return none;
    }
  }

  std::string ParseVariable(const std::string& s, size_t& start, size_t& stop) {
    size_t vstart = s.find("$");
    if (vstart == std::string::npos) {
      start = stop = 0;
      return "";
    }
    size_t vstop = 0;
    std::string v = s;
    if (s.at(vstart+1) == '{') {
      vstop = s.find("}");
      if (vstop != std::string::npos) {
	v.erase(vstop);
	v.erase(0,vstart+2);
	start = vstart;
	stop = vstop+1;
      } else {
	start = stop = 0;
	return "";
      }
    } else {
      v.erase(0,vstart+1);
      start = vstart;
      vstop = s.find(" ", vstart+1);
      if (vstop == std::string::npos) {
	stop = v.length()+1;
      } else {
	stop = vstop;
	v.erase(vstop-vstart-1);
      }
    }
    return v;
  }

  //----------------------------------------------------------------------------
  // Replae variables from a chapter until they are resolved
  //----------------------------------------------------------------------------
  void ReplaceFromChapter(std::string& s, const char* substitute_chapter) {
    if (Has(substitute_chapter)) {

      std::map<std::string, std::string> m = AsMap(substitute_chapter);
      // preset always the EOSHOST variable
      m["EOSHOST"] = hostname;

      std::string var;
      size_t p1,p2;
      while ( (var=ParseVariable(s,p1,p2)).length() ) {

	if (!p1 && !p2) {
	  break;
	}

	if (m.count(var)) {
	  s.erase(p1,p2-p1);
	  s.insert(p1,m[var]);
	} else {
	  break;}
      }
    } else {
      return;
    }
  }


  //----------------------------------------------------------------------------
  // Replace a string with a sysconfig definition
  //----------------------------------------------------------------------------
  std::string Substitute(const std::string& s, bool doit=false, const char* substitute_chapter="sysconfig") {

    if (doit) {
      std::string r = s;

      size_t p1,p2;
      ReplaceFromChapter(r, substitute_chapter);
      return r;
    }
    return s;
  }


  //----------------------------------------------------------------------------
  // Config Dumper
  //----------------------------------------------------------------------------

  std::string Dump(const char* chapter = 0, bool substitute = false, const char* substitute_chapter = "sysconfig") {
    std::string out;
    if (chapter) {
      if (conf.count(chapter)) {
	for ( auto it : conf[chapter] ) {
	  out += Substitute(it, substitute, substitute_chapter);
	  out += "\n";
	}
      }
    } else {
      for ( auto c : conf ) {
	out += "[";
	out += c.first;;
	out += "]\n";
	for ( auto it : c.second ) {
	  out += Substitute(it, substitute, substitute_chapter);
	  out += "\n";
	}
      }
    }
    return out;
  }

  //----------------------------------------------------------------------------
  // Test for configuration chapter
  //----------------------------------------------------------------------------
  bool Has(const char* chapter) const {
    return conf.count(chapter);
  }

  //----------------------------------------------------------------------------
  // Get <value> of line for a given key '<key> <value>'
  //----------------------------------------------------------------------------
  std::string GetValueByKey(const char* chapter, const char* key);


  //----------------------------------------------------------------------------
  // AsMap
  //----------------------------------------------------------------------------
  std::map<std::string, std::string> AsMap(const char* chapter);

  //----------------------------------------------------------------------------
  // Env
  // ---------------------------------------------------------------------------

  char** Env(const char* chapter);

private:
  int errcode;
  std::string errorMessage;
  std::string service;
  std::string name;
  std::string hostname;
  ConfigChapter conf;
  char* envv[1024];
};

EOSCOMMONNAMESPACE_END

#endif
