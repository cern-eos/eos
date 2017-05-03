//------------------------------------------------------------------------------
//! @file AclCommand.hh
//! @author Stefan Isidorovic <stefan.isidorovic@comtrade.com>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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

#ifndef __ACL__COMMAND__HH__
#define __ACL__COMMAND__HH__

#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucString.hh"

#include "console/ConsoleMain.hh"

#include "common/StringTokenizer.hh"

#include <functional>
#include <iostream>
#include <memory>
#include <regex>
#include <string>
#include <unordered_map>
#include <utility>

typedef std::pair<std::string, unsigned short> Rule;
typedef std::unordered_map<std::string, unsigned short> RuleMap;

// Macro responsible for testing.
// Swapping MgmExecute with test part rather than with real one.
#ifdef CPPUNIT_FOUND
class AclCommandTest;
#include "console/tests/MgmExecuteTest.hh"
#else
//------------------------------------------------------------------------------
//! Class MgmExecute
//!
//! @description Class wrapper around communication with MGM node. Intended to
//!   be easily hotswapped in testing purposes of console commands.
//------------------------------------------------------------------------------
class MgmExecute
{
  std::string m_result;
  std::string m_error;
public:
  MgmExecute();

  bool ExecuteCommand ( const char* command );
  inline std::string& GetResult()
  {
    return this->m_result;
  }
  inline std::string& GetError()
  {
    return this->m_error;
  }
};

#endif


//------------------------------------------------------------------------------
//! Class AclCommand
//!
//! @description Implementing ACL command line tool. Tool is intended to be used
//!   as unix chmod tool for setting and removing ACL rights from eos directory.
//------------------------------------------------------------------------------

class AclCommand
{
// Making test class friend so can access to private parts for testing purposes
#ifdef CPPUNIT_FOUND
  //friend AclCommandTest;
  #define private public
#endif

private:
  RuleMap m_rules;  ///< Map containing current ACL rules

  MgmExecute m_mgm_execute;  ///< Object for executing mgm commands

  std::string m_id; ///< Identifier for rule. Extracted from command line.

  ///< ACL rule bitmasks for adding and removing
  unsigned short m_add_rule, m_rm_rule;

  std::string m_path;  ///< Path extracted from command line
  std::string m_rule;  ///< Rule extracted from command line

  std::string m_error_message;

  // Loaded strings from mgm.
  std::string m_sys_acl_string;
  std::string m_usr_acl_string;

  char* m_comm;  ///< pointer to original command

  // Flags

  bool m_recursive;  ///< -R --recursive flag bool
  bool m_list;  ///< -l --lists flag bool
  bool m_usr_acl;  ///< --usr flag bool
  bool m_sys_acl;  ///< --sys flag bool
  bool m_set;  ///< Is rule set or not. (Containing =).

  //----------------------------------------------------------------------------
  //! Converting ACL bitmask to string representation
  //!
  //! @param in ACL bitmask
  //! @return std::string representation of ACL
  //----------------------------------------------------------------------------
  std::string AclShortToString ( unsigned short in );

  //----------------------------------------------------------------------------
  //! Getting whole rule from string. Intended for extracting rules from acl
  //! string which MGM sent.
  //!
  //! @param in ACL bitmask
  //! @return std::pair containing id and ACL rule bitmask
  //----------------------------------------------------------------------------
  Rule AclRuleFromString ( const std::string& in );

  //----------------------------------------------------------------------------
  //! Reading acl strings for given path from MGM
  //!
  //! @param path Path for which we are asking acl strings for.
  //! @return bool if operation is succesfull or not.
  //----------------------------------------------------------------------------
  bool GetAclStringsForPath ( const std::string& path );

  //----------------------------------------------------------------------------
  //! Universal method for setting given acl string from mgm format.
  //!
  //! @param res String from MGM response containing acl_string
  //! @param which Reference to string where rules will be stored.
  //----------------------------------------------------------------------------
  void SetAclString ( const std::string& res, std::string& which, const char* type );

  //----------------------------------------------------------------------------
  //! Generating rule map based on acl string
  //!
  //! @param acl_string string containing acl rules
  //! @param map Map which will be filled with acl rules
  //----------------------------------------------------------------------------
  void GenerateRuleMap ( const std::string& acl_string, RuleMap& map );

  //----------------------------------------------------------------------------
  //! Checking if id is in correct format
  //!
  //! @param id string containing id
  //! @return bool if id is correct or not.
  //----------------------------------------------------------------------------
  bool CheckCorrectId ( const std::string& id );

  //----------------------------------------------------------------------------
  //! Converting ACL string rule to bitmask
  //!
  //! @param rule string containing rule from command line
  //! @param set indicating if set mode is active or not
  //! @return bool if rule is correct or not.
  //----------------------------------------------------------------------------
  bool GetRuleInt ( const std::string& rule, bool set = false );

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
  bool ParseRule ( const std::string& input );

  //----------------------------------------------------------------------------
  //! Applying changes to MGM
  //!
  //! @param path string containing path for which we are applying changes.
  //! @param set indicating if set mode is active or not
  //! @return bool indicating if we succesfully applied rules or not.
  //----------------------------------------------------------------------------
  bool MgmSet ( const std::string& path );

  //----------------------------------------------------------------------------
  //! Determining Acl type flags if there is no flag risen for acl type.
  //!
  //! @return bool indicating if flags are correctly set.
  //----------------------------------------------------------------------------
  bool SetDefaultAclRoleFlag();

  //----------------------------------------------------------------------------
  //! Converting ACL string rule to bitmask
  //!
  //! @param rule string containing rule from command line
  //! @param set indicating if set mode is active or not
  //! @return bool if rule is correct or not.
  //----------------------------------------------------------------------------
  bool ProcessCommand();

  //----------------------------------------------------------------------------
  //! Recursive calling for all directories in subtree from given directory
  //!
  //! @param action Action to be done for every directory
  //----------------------------------------------------------------------------
  void RecursiveCall ( bool apply );

  //----------------------------------------------------------------------------
  //! Doing relevant action.
  //!
  //! @param apply True for apply action, false for list action
  //! @param path On which path to execute action.
  //----------------------------------------------------------------------------
  bool Action ( bool apply, const std::string& path );

  //----------------------------------------------------------------------------
  //! Printing help
  //----------------------------------------------------------------------------
  void PrintHelp();

public:

  AclCommand ( const char* comm ) :
    m_rules(),
    m_comm ( const_cast<char*> ( comm ) ),
    m_recursive ( false ),
    m_list ( false ),
    m_usr_acl ( false ),
    m_sys_acl ( false ),
    m_set ( false )

  {}
  ///< Enumerator defining which bit represents which acl flag.
  enum ACLPos {
    R  = 1 << 0, // 1    -  r
    W  = 1 << 1, // 2    -  w
    X  = 1 << 2, // 4    -  x
    M  = 1 << 3, // 8    -  m
    nM = 1 << 4, // 16   - !m
    nD = 1 << 5, // 32   - !d
    pD = 1 << 6, // 64   - +d
    nU = 1 << 7, // 128  - !u
    pU = 1 << 8, // 256  - +u
    Q  = 1 << 9, // 512  -  q
    C  = 1 << 10 // 1024 -  c
  };

  //----------------------------------------------------------------------------
  //! Returning error message
  //!
  //! @return string with error message
  //----------------------------------------------------------------------------
  inline std::string getErrorMessage()
  {
    return this->m_error_message;
  }

  //----------------------------------------------------------------------------
  //! Executing command
  //----------------------------------------------------------------------------
  void Execute();
  ~AclCommand();
};

//----------------------------------------------------------------------------
//! Hiearchy of functor classes to replace original lambda implementation.
//----------------------------------------------------------------------------

class RuleParseActionBase
{
public:
  virtual void operator() ( const AclCommand::ACLPos &pos ) = 0;
};

class RuleParseActionAdd : public RuleParseActionBase
{
  unsigned short& add_ret;
  unsigned short& ret;
public:
  RuleParseActionAdd ( unsigned short& add_ret_, unsigned short& ret_ )
    : add_ret ( add_ret_ ), ret ( ret_ ) {}

  virtual void operator() ( const AclCommand::ACLPos &pos )
  {
    add_ret = add_ret | pos;
    ret = ret | pos;
  }
};

class RuleParseActionRm : public RuleParseActionBase
{
  unsigned short& rm_ret;
  unsigned short& ret;
public:
  RuleParseActionRm ( unsigned short& rm_ret_, unsigned short& ret_ )
    : rm_ret ( rm_ret_ ), ret ( ret_ ) {}

  virtual void operator() ( const AclCommand::ACLPos& pos )
  {
    rm_ret = rm_ret | pos;
    ret = ret & ( ~pos );
  }
};

#endif //__ACL__COMMAND__HH__
