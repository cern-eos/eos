//------------------------------------------------------------------------------
//! @file AclCmd.hh
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
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#pragma once
#include "mgm/Namespace.hh"
#include "mgm/proc/ProcCommand.hh"
#include "proto/Acl.pb.h"
#include <unordered_map>

EOSMGMNAMESPACE_BEGIN

typedef std::pair<std::string, unsigned short> Rule;
//typedef std::unordered_map<std::string, unsigned short> RuleMap;
typedef std::vector<Rule> RuleMap;

template <typename C, typename K, typename V>
void insert_or_assign(C& c, K&& k, V&& v)
{
  auto it = std::find_if(c.begin(),
                         c.end(),
                         [&k](const typename C::value_type& val) -> bool {
                           return k == val.first;
                         });
  if (it != c.end()) {
    it->second = v;
    return;
  }
  c.emplace_back(std::make_pair(std::forward<K>(k),
                                std::forward<V>(v)));
}

//------------------------------------------------------------------------------
//! Class AclCmd - class handling acl command from a client
//------------------------------------------------------------------------------
class AclCmd: public IProcCommand
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param req client ProtocolBuffer request
  //! @param vid client virtual identity
  //----------------------------------------------------------------------------
  explicit AclCmd(eos::console::RequestProto&& req,
                  eos::common::VirtualIdentity& vid):
    IProcCommand(std::move(req), vid, true), mId(), mAddRule(0),
    mRmRule(0), mSet(false)
  {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~AclCmd() = default;

  //----------------------------------------------------------------------------
  //! Method implementing the specific behavior of the command executed by the
  //! asynchronous thread
  //----------------------------------------------------------------------------
  eos::console::ReplyProto ProcessRequest() noexcept override;

  //----------------------------------------------------------------------------
  //! Generate rule map from the string representation of the acls. If there
  //! are no acls then the rmap will be empty.
  //!
  //! @note Public only for testing.
  //!
  //! @param acl_string string containing acl
  //! @param rmap map to be filled with acl rules
  //----------------------------------------------------------------------------
  static void
  GenerateRuleMap(const std::string& acl_string, RuleMap& rmap);

  //----------------------------------------------------------------------------
  //! Check if id has the correct format i.e u:user_id or g:group_id
  //!
  //! @param id string containing id
  //!
  //! @return bool true if correct, otherwise false
  //----------------------------------------------------------------------------
  bool CheckCorrectId(const std::string& id) const;

  //----------------------------------------------------------------------------
  //! Get ACL rule from string by creating a pair of identifier for the ACL and
  //! the bitmask representation.
  //!
  //! @param in ACL string
  //!
  //! @return std::pair containing ACL identifier (ie. u:user1 or g:group1)
  //! and the bitmask representation
  //----------------------------------------------------------------------------
  static Rule GetRuleFromString(const std::string& in);

  //----------------------------------------------------------------------------
  //! Convert acl modification command into bitmask rule format
  //!
  //! @param input string containing the modifications of the acls
  //! @param set if true "set" mode is active, otherwise false
  //!
  //! @return bool true if conversion successful, otherwise false
  //----------------------------------------------------------------------------
  bool GetRuleBitmask(const std::string& input, bool set = false);

  //----------------------------------------------------------------------------
  //! Return mAddRule result after GetRuleBitmask call.
  //----------------------------------------------------------------------------
  unsigned short GetAddRule()
  {
    return mAddRule;
  }

  //----------------------------------------------------------------------------
  //! Return mRmRule result after GetRuleBitmask call.
  //----------------------------------------------------------------------------
  unsigned short GetRmRule()
  {
    return mRmRule;
  }

private:
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
    C  = 1 << 10,  // 1024 -  c
    WO = 1 << 11   // 2048 - wo
  };

  std::string mId; ///< Rule identifier extracted from command line
  ///< ACL rule bitmasks for adding and removing
  unsigned short mAddRule, mRmRule;
  bool mSet; ///< Rule is set operations i.e contains =

  //----------------------------------------------------------------------------
  //! Get sys.acl and user.acl for a given path
  //!
  //! @param path path to get the ACLs for
  //! @param acls ACL VALUE
  //! @param is_sys if true return sys.acl, otherwise user.acl
  //! @param take_lock if true take namespace lock, otherwise don't
  //----------------------------------------------------------------------------
  void GetAcls(const std::string& path, std::string& acls, bool is_sys = false,
               bool take_lock = true);

  //----------------------------------------------------------------------------
  //! Modify the acls for a path
  //!
  //! @param acl acl ProtoBuf object
  //!
  //! @return 0 if successful, otherwise errno
  //----------------------------------------------------------------------------
  int ModifyAcls(const eos::console::AclProto& acl);

  //----------------------------------------------------------------------------
  //! Generate acl string representation from a rule map
  //!
  //! @param rmap map of rules to be used for conversion
  //!
  //! @return true if conversion successful, otherwise false
  //----------------------------------------------------------------------------
  static std::string GenerateAclString(const RuleMap& rmap);

  //----------------------------------------------------------------------------
  //! Parse command line (modification) rule given by the client. This specifies
  //! the modifications to be operated on the current acls of the dir(s).
  //!
  //! @param input string rule from command line
  //!
  //! @return bool true if rule is correct, otherwise false
  //----------------------------------------------------------------------------
  bool ParseRule(const std::string& input);

  //----------------------------------------------------------------------------
  //! Apply client modification rule(s) to the acls of the current entry
  //!
  //! @param rules map of acl rules for the current entry (directory)
  //----------------------------------------------------------------------------
  void ApplyRule(RuleMap& rules);

  //----------------------------------------------------------------------------
  //! Convert ACL bitmask to string representation
  //!
  //! @param in ACL bitmask
  //!
  //! @return std::string representation of ACL
  //----------------------------------------------------------------------------
  static std::string AclBitmaskToString(const unsigned short in);

  std::string mErr; ///< Command error output string

};

EOSMGMNAMESPACE_END
