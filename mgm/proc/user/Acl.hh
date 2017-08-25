//------------------------------------------------------------------------------
//! @file Acl.hh
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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
 * You should have received a copy of the AGNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#pragma once

#include "common/Acl.pb.h"
#include <unordered_map>

typedef std::pair<std::string, unsigned short> Rule;
typedef std::unordered_map<std::string, unsigned short> RuleMap;

//------------------------------------------------------------------------------
//! Class Acl
//!
//! @description Implementing ACL command line tool. Tool is intended to be used
//!   as unix chmod tool for setting and removing ACL rights from eos directory.
//------------------------------------------------------------------------------
class Acl
{
public:
  //! Enumerator defining which bit represents which acl flag.
  enum ACLPos {
    R  = 1 << 0,   // 1    -  r
    W  = 1 << 1,   // 2    -  w
    X  = 1 << 2,   // 4    -  x
    M  = 1 << 3,   // 8    -  m
    nM = 1 << 4,   // 16   - !m
    nD = 1 << 5,   // 32   - !d
    pD = 1 << 6,   // 64   - +d
    nU = 1 << 7,   // 128  - !u
    pU = 1 << 8,   // 256  - +u
    Q  = 1 << 9,   // 512  -  q
    C  = 1 << 10   // 1024 -  c
  };

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  Acl(const char* comm) :
    m_rules(), m_add_rule(0), m_rm_rule(0), m_comm(const_cast<char*>(comm)),
    m_recursive(false), m_list(false), m_usr_acl(false), m_sys_acl(false),
    m_set(false)
  {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~Acl() = default;

  //----------------------------------------------------------------------------
  //! Returning error message
  //!
  //! @return string with error message
  //----------------------------------------------------------------------------
  inline std::string getErrorMessage()
  {
    return m_error_message;
  }

private:
  RuleMap m_rules; ///< Map containing current ACL rules
  std::string m_id; ///< Identifier for rule. Extracted from command line.
  ///< ACL rule bitmasks for adding and removing
  unsigned short m_add_rule, m_rm_rule;
  std::string m_path; ///< Path extracted from command line
  std::string m_rule; ///< Rule extracted from command line
  std::string m_error_message;
  // Loaded strings from mgm.
  std::string m_sys_acl_string;
  std::string m_usr_acl_string;
  char* m_comm; ///< pointer to original command
  eos::console::AclProto mAclProto; ///< Protobuf cmd representation

  // Flags
  bool m_recursive; ///< -R --recursive flag bool
  bool m_list; ///< -l --lists flag bool
  bool m_usr_acl; ///< --usr flag bool
  bool m_sys_acl; ///< --sys flag bool
  bool m_set; ///< Is rule set or not. (Containing =).

  //----------------------------------------------------------------------------
  //! Converting ACL bitmask to string representation
  //!
  //! @param in ACL bitmask
  //! @return std::string representation of ACL
  //----------------------------------------------------------------------------
  std::string AclShortToString(unsigned short in);

  //----------------------------------------------------------------------------
  //! Getting whole rule from string. Intended for extracting rules from acl
  //! string which MGM sent.
  //!
  //! @param in ACL bitmask
  //! @return std::pair containing id and ACL rule bitmask
  //----------------------------------------------------------------------------
  Rule AclRuleFromString(const std::string& in);

  //----------------------------------------------------------------------------
  //! Reading acl strings for given path from MGM
  //!
  //! @param path Path for which we are asking acl strings for.
  //! @return bool if operation is succesfull or not.
  //----------------------------------------------------------------------------
  bool GetAclStringsForPath(const std::string& path);

  //----------------------------------------------------------------------------
  //! Universal method for setting given acl string from mgm format.
  //!
  //! @param res String from MGM response containing acl_string
  //! @param which Reference to string where rules will be stored.
  //----------------------------------------------------------------------------
  void SetAclString(const std::string& res, std::string& which, const char* type);

  //----------------------------------------------------------------------------
  //! Generating rule map based on acl string
  //!
  //! @param acl_string string containing acl rules
  //! @param map Map which will be filled with acl rules
  //----------------------------------------------------------------------------
  void GenerateRuleMap(const std::string& acl_string, RuleMap& map);

  //----------------------------------------------------------------------------
  //! Check if id is in the correct format
  //!
  //! @param id string containing id
  //! @return bool if id is correct or not.
  //----------------------------------------------------------------------------
  bool CheckCorrectId(const std::string& id);

  //----------------------------------------------------------------------------
  //! Converting ACL string rule to bitmask
  //!
  //! @param rule string containing rule from command line
  //! @param set indicating if set mode is active or not
  //! @return bool if rule is correct or not.
  //----------------------------------------------------------------------------
  bool GetRuleInt(const std::string& rule, bool set = false);

  //----------------------------------------------------------------------------
  //! Applying current rule (which user give in command) to current rules loaded
  //----------------------------------------------------------------------------
  void ApplyRule();

  //----------------------------------------------------------------------------
  //! Convertin map with rules to string for storing within MGM
  //!
  //! @return string
  //----------------------------------------------------------------------------
  std::string MapToAclString();

  //----------------------------------------------------------------------------
  //! Parsing command line rule
  //!
  //! @param input string containing rule from command line.
  //! @return bool if rule is correct or not.
  //----------------------------------------------------------------------------
  bool ParseRule(const std::string& input);

  //----------------------------------------------------------------------------
  //! Converting ACL string rule to bitmask
  //!
  //! @param rule string containing rule from command line
  //! @param set indicating if set mode is active or not
  //! @return bool if rule is correct or not.
  //----------------------------------------------------------------------------
  bool ProcessCommand();

  //----------------------------------------------------------------------------
  //! Doing relevant action.
  //!
  //! @param apply True for apply action, false for list action
  //! @param path On which path to execute action.
  //----------------------------------------------------------------------------
  bool Action(bool apply, const std::string& path);
};
