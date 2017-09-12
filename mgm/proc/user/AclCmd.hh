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
 * You should have received a copy of the AGNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#pragma once
#include "mgm/Namespace.hh"
#include "mgm/proc/ProcCommand.hh"
#include "common/Acl.pb.h"
#include "common/ConsoleRequest.pb.h"
#include <unordered_map>

EOSMGMNAMESPACE_BEGIN

typedef std::pair<std::string, unsigned short> Rule;
typedef std::unordered_map<std::string, unsigned short> RuleMap;

//------------------------------------------------------------------------------
//! Class AclCmd - class hadling acl command from a client
//------------------------------------------------------------------------------
class AclCmd: public ProcCommand
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param req client ProtocolBuffer request
  //----------------------------------------------------------------------------
  AclCmd(eos::console::RequestProto&& req,
         eos::common::Mapping::VirtualIdentity& vid):
    ProcCommand(vid), mHasResponse(false), mReqProto(std::move(req))
  {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~AclCmd() = default;

  //----------------------------------------------------------------------------
  //! Open a proc command e.g. call the appropriate user or admin commmand and
  //! store the output in a resultstream of in case of find in temporary output
  //! files.
  //!
  //! @param inpath path indicating user or admin command
  //! @param info CGI describing the proc command
  //! @param vid_in virtual identity of the user requesting a command
  //! @param error object to store errors
  //!
  //! @return SFS_OK in any case
  //----------------------------------------------------------------------------
  virtual int open(const char* path, const char* info,
                   eos::common::Mapping::VirtualIdentity& vid,
                   XrdOucErrInfo* error) override;

  //----------------------------------------------------------------------------
  //! Read a part of the result stream created during open
  //!
  //! @param boff offset where to start
  //! @param buff buffer to store stream
  //! @param blen len to return
  //!
  //! @return number of bytes read
  //----------------------------------------------------------------------------
  virtual int read(XrdSfsFileOffset offset, char* buff,
                   XrdSfsXferSize blen) override;

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
    C  = 1 << 10   // 1024 -  c
  };

  std::string mTmpResp;
  bool mHasResponse; ///< Value indicating that the reseponse is ready
  std::promise<eos::console::ReplyProto> mPromise; ///< Promise reply
  std::future<eos::console::ReplyProto> mFuture; ///< Response future
  eos::console::RequestProto mReqProto; ///< Client request protobuf object
  RuleMap mRules; ///< Map containing current ACL rules

  //----------------------------------------------------------------------------
  //! Method executing the command and returning a future object
  //!
  //! @return future holding the reply object
  //----------------------------------------------------------------------------
  std::future<eos::console::ReplyProto> Execute();


  //----------------------------------------------------------------------------
  //! Process request - this is can be run asynchronously and needs to set the
  //! mPromise object which is storing the response.
  //----------------------------------------------------------------------------
  void ProcessRequest();

  //----------------------------------------------------------------------------
  //! Get sys.acl and user.acl for a given path
  //!
  //! @param path path to get the ACLs for
  //! @param acls ACL VALUE
  //! @param is_sys if true return sys.acl, otherwise user.acl
  //----------------------------------------------------------------------------
  void GetAcls(const std::string& path, std::string& acls, bool is_sys = false);












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

  /*

  //----------------------------------------------------------------------------
  //! Convert ACL bitmask to string representation
  //!
  //! @param in ACL bitmask
  //!
  //! @return std::string representation of ACL
  //----------------------------------------------------------------------------
  std::string AclBitmaskToString(unsigned short in) const;

  //----------------------------------------------------------------------------
  //! Get ACL rule from string by creating a pair of identifier for the ACL and
  //! the bitmask representation
  //!
  //! @param in ACL string
  //!
  //! @return std::pair containing ACL identifier (ie. u:user1 or g:group1)
  //! and the bitmask representation
  //----------------------------------------------------------------------------
  Rule AclRuleFromString(const std::string& in) const;

  //----------------------------------------------------------------------------
  //! Generating rule map based on acl string
  //!
  //! @param acl_string string containing acl rules
  //! @param map Map which will be filled with acl rules
  //----------------------------------------------------------------------------
  void GenerateRuleMap(const std::string& acl_string);

  //----------------------------------------------------------------------------
  //! Universal method for setting given acl string from mgm format.
  //!
  //! @param res String from MGM response containing acl_string
  //! @param which Reference to string where rules will be stored.
  //----------------------------------------------------------------------------
  void SetAclString(const std::string& res, std::string& which, const char* type);


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
  //! Doing relevant action.
  //!
  //! @param apply True for apply action, false for list action
  //! @param path On which path to execute action.
  //----------------------------------------------------------------------------
  bool Action(bool apply, const std::string& path);
  */
};

EOSMGMNAMESPACE_END
