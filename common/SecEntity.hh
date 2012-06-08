// ----------------------------------------------------------------------
// File: SecEntity.hh
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

#ifndef __EOSCOMMON_SECENTITY__HH__
#define __EOSCOMMON_SECENTITY__HH__

/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
#include "common/StringConversion.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSec/XrdSecEntity.hh"
/*----------------------------------------------------------------------------*/
#include <map>
#include <string>
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

/**
 * @file   SecEntity.hh
 * 
 * @brief  Convenience Class to serialize/deserialize sec entity information
 * 
 * 
 */

class SecEntity {
public:
  
  // ---------------------------------------------------------------------------
  //! convert the fields of a sec entity into val1|val2|val3 ...
  // ---------------------------------------------------------------------------
  static std::string ToKey(const XrdSecEntity* entity, const char* app) {
    std::string s="";
    s+= (entity->prot)?entity->prot:"";
    s+= "|";
    s+= (entity->name)?entity->name:"";
    s+= "|";
    s+= (entity->host)?entity->host:"";
    s+= "|";
    s+= (entity->vorg)?entity->vorg:"";
    s+= "|";
    s+= (entity->grps)?entity->grps:"";
    s+= "|";
    s+= (entity->role)?entity->role:"";
    s+= "|";
    s+= (entity->moninfo)?entity->moninfo:"";
    s+= "|";
    s+= (app)?app:"";
    return s;
  }
  
  // ---------------------------------------------------------------------------
  //! convert val1|val2|val3... to a map with key/val pairs
  // ---------------------------------------------------------------------------
  static std::map<std::string,std::string> KeyToMap(std::string entitystring) {
    std::vector<std::string> tokens;
    eos::common::StringConversion::EmptyTokenize(entitystring,tokens,"|");;
    std::map<std::string,std::string> mp;
    mp["prot"]=tokens[0];
    mp["name"]=tokens[1];
    mp["host"]=tokens[2];
    mp["vorg"]=tokens[3];
    mp["grps"]=tokens[4];
    mp["role"]=tokens[5];
    mp["info"]=tokens[6];
    mp["app"] =tokens[7];
    return mp;
  }

  // ---------------------------------------------------------------------------
  //! convert val1|val2|val3... to an env string
  // ---------------------------------------------------------------------------
  static std::string ToEnv(const char* s) {
    if (!s) 
      return "";

    std::vector<std::string> tokens;
    std::string entitystring = s;
    eos::common::StringConversion::EmptyTokenize(entitystring,tokens,"|");
    std::string rs="prot=";

    if (tokens.size()>7) {
      rs+=tokens[0];
      rs+="&sec.name=";rs+= tokens[1];
      rs+="&sec.host=";rs+= tokens[2];
      rs+="&sec.vorg=";rs+= tokens[3];
      rs+="&sec.grps=";rs+= tokens[4];
      rs+="&sec.info=";rs+= tokens[5];
      rs+="&sec.info=";rs+= tokens[6];
      rs+="&sec.app=";rs+= tokens[7];
    } else {
      fprintf(stderr,"[eos::common::SecEntit::ToEnv] yerror: %s has illegal contents [%d]\n", s?s:"", (int)tokens.size());
    }
    return rs;
  }
};

EOSCOMMONNAMESPACE_END

#endif
