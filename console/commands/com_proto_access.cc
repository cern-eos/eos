//------------------------------------------------------------------------------
// @file: com_proto_access.cc
// @author: Fabio Luchetti - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your token) any later version.                                   *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include "console/commands/ICmdHelper.hh"
#include "common/StringTokenizer.hh"
#include "console/ConsoleMain.hh"

extern int com_access(char*);
void com_access_help();

//------------------------------------------------------------------------------
//! Class AccessHelper
//------------------------------------------------------------------------------
class AccessHelper : public ICmdHelper
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param opts global options
  //----------------------------------------------------------------------------
  AccessHelper(const GlobalOptions& opts):
    ICmdHelper(opts)
  {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~AccessHelper() override = default;

  //----------------------------------------------------------------------------
  //! Parse command line input
  //!
  //! @param arg input
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool ParseCommand(const char* arg) override;
};

//------------------------------------------------------------------------------
// Parse command line input
//------------------------------------------------------------------------------
bool AccessHelper::ParseCommand(const char* arg)
{
  eos::console::AccessProto* access = mReq.mutable_access();
  eos::common::StringTokenizer tokenizer(arg);
  tokenizer.GetLine();
  std::string token;

  if (!tokenizer.NextToken(token)) {
    return false;
  }

  if (token == "ls") {
    eos::console::AccessProto_LsProto* ls = access->mutable_ls();

    while (tokenizer.NextToken(token)) {
      if (token == "-m") {
        ls->set_monitoring(true);
      } else if (token == "-n") {
        ls->set_id2name(true);
      } else {
        return false;
      }
    }
  } else if (token == "rm") {
    eos::console::AccessProto_RmProto* rm = access->mutable_rm();

    if (!tokenizer.NextToken(token)) {
      return false;
    }

    if (token == "redirect") {
      rm->set_rule(eos::console::AccessProto_RmProto::REDIRECT);

      if (tokenizer.NextToken(token)) {
        if (token == "r" || token == "w" || token == "ENOENT" || token == "ENONET" ||
            token == "ENETUNREACH") {
          rm->set_key(token);
        } else {
          return false;
        }
      }
    } else if (token == "stall") {
      rm->set_rule(eos::console::AccessProto_RmProto::STALL);

      if (tokenizer.NextToken(token)) {
        if (token == "r" || token == "w" || token == "ENOENT" || token == "ENONET" ||
            token == "ENETUNREACH") {
          rm->set_key(token);
        } else {
          return false;
        }
      }
    } else if (token == "limit") {
      rm->set_rule(eos::console::AccessProto_RmProto::LIMIT);

      if (!tokenizer.NextToken(token)) {
        return false;
      }


      if ((!token.find("threads:")) && (
					!(token.find("rate:user:") || token.find("rate:group:")) &&
					token.find(':', 11))) {
        return false;
      }

      rm->set_key(token);
    } else {
      return false;
    }
  } else if (token == "set") {
    eos::console::AccessProto_SetProto* set = access->mutable_set();

    if (!tokenizer.NextToken(token)) {
      return false;
    }

    if (token == "redirect") {
      set->set_rule(eos::console::AccessProto_SetProto::REDIRECT);

      if (!tokenizer.NextToken(token)) {
        return false;
      }

      set->set_target(token);

      if (tokenizer.NextToken(token)) {
        if (token == "r" || token == "w" || token == "ENOENT" || token == "ENONET" ||
            token == "ENETUNREACH") {
          set->set_key(token);
        } else {
          return false;
        }
      }
    } else if (token == "stall") {
      set->set_rule(eos::console::AccessProto_SetProto::STALL);

      if (!tokenizer.NextToken(token)) {
        return false;
      }

      set->set_target(token);

      if (tokenizer.NextToken(token)) {
        if (token == "r" || token == "w" || token == "ENOENT" || token == "ENONET" ||
            token == "ENETUNREACH") {
          set->set_key(token);
        } else {
          return false;
        }
      }
    } else if (token == "limit") {
      set->set_rule(eos::console::AccessProto_SetProto::LIMIT);

      if (!tokenizer.NextToken(token)) {
        return false;
      }

      set->set_target(token);

      if (!tokenizer.NextToken(token)) {
        return false;
      }

      if ((!token.find("threads:")) && (
				       !(token.find("rate:user:") || token.find("rate:group:")) &&
				       token.find(':', 11))) {
        return false;
      }

      fprintf(stderr,"settings %s\n", token.c_str());
      set->set_key(token);
    } else {
      return false;
    }
  } else if (token == "ban") {
    eos::console::AccessProto_BanProto* ban = access->mutable_ban();

    if (!tokenizer.NextToken(token)) {
      return false;
    }

    if (token == "user") {
      ban->set_idtype(eos::console::AccessProto_BanProto::USER);
    } else if (token == "group") {
      ban->set_idtype(eos::console::AccessProto_BanProto::GROUP);
    } else if (token == "host") {
      ban->set_idtype(eos::console::AccessProto_BanProto::HOST);
    } else if (token == "domain") {
      ban->set_idtype(eos::console::AccessProto_BanProto::DOMAINNAME);
    } else {
      return false;
    }

    if (!tokenizer.NextToken(token)) {
      return false;
    }

    ban->set_id(token);
  } else if (token == "unban") {
    eos::console::AccessProto_UnbanProto* unban = access->mutable_unban();

    if (!tokenizer.NextToken(token)) {
      return false;
    }

    if (token == "user") {
      unban->set_idtype(eos::console::AccessProto_UnbanProto::USER);
    } else if (token == "group") {
      unban->set_idtype(eos::console::AccessProto_UnbanProto::GROUP);
    } else if (token == "host") {
      unban->set_idtype(eos::console::AccessProto_UnbanProto::HOST);
    } else if (token == "domain") {
      unban->set_idtype(eos::console::AccessProto_UnbanProto::DOMAINNAME);
    } else {
      return false;
    }

    if (!tokenizer.NextToken(token)) {
      return false;
    }

    unban->set_id(token);
  } else if (token == "allow") {
    eos::console::AccessProto_AllowProto* allow = access->mutable_allow();

    if (!tokenizer.NextToken(token)) {
      return false;
    }

    if (token == "user") {
      allow->set_idtype(eos::console::AccessProto_AllowProto::USER);
    } else if (token == "group") {
      allow->set_idtype(eos::console::AccessProto_AllowProto::GROUP);
    } else if (token == "host") {
      allow->set_idtype(eos::console::AccessProto_AllowProto::HOST);
    } else if (token == "domain") {
      allow->set_idtype(eos::console::AccessProto_AllowProto::DOMAINNAME);
    } else {
      return false;
    }

    if (!tokenizer.NextToken(token)) {
      return false;
    }

    allow->set_id(token);
  } else if (token == "unallow") {
    eos::console::AccessProto_UnallowProto* unallow = access->mutable_unallow();

    if (!tokenizer.NextToken(token)) {
      return false;
    }

    if (token == "user") {
      unallow->set_idtype(eos::console::AccessProto_UnallowProto::USER);
    } else if (token == "group") {
      unallow->set_idtype(eos::console::AccessProto_UnallowProto::GROUP);
    } else if (token == "host") {
      unallow->set_idtype(eos::console::AccessProto_UnallowProto::HOST);
    } else if (token == "domain") {
      unallow->set_idtype(eos::console::AccessProto_UnallowProto::DOMAINNAME);
    } else {
      return false;
    }

    if (!tokenizer.NextToken(token)) {
      return false;
    }

    unallow->set_id(token);
  } else { // no proper subcommand
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Config command entry point
//------------------------------------------------------------------------------
int com_protoaccess(char* arg)
{
  if (wants_help(arg)) {
    com_access_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  AccessHelper access(gGlobalOpts);

  if (!access.ParseCommand(arg)) {
    com_access_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  global_retc = access.Execute();

  return global_retc;
}

//------------------------------------------------------------------------------
// Print help message
//------------------------------------------------------------------------------
void com_access_help()
{
  std::ostringstream oss;
  oss
      << " usage:\n"
      << "access ban|unban|allow|unallow|set|rm|ls [OPTIONS]\n"
      << "'[eos] access ..' provides the access interface of EOS to allow/disallow hosts/domains and/or users\n"
      << std::endl
      << "Subcommands:\n"
      << "access ban user|group|host|domain <identifier> : ban user,group or host,DOMAIN with identifier <identifier>\n"
      << "\t <identifier> : can be a user name, user id, group name, group id, hostname or IP or domainname\n"
      << std::endl
      << "access unban user|group|host|domain <identifier> : unban user,group or host,DOMAIN with identifier <identifier>\n"
      << "\t <identifier> : can be a user name, user id, group name, group id, hostname or IP or domainname\n"
      << std::endl
      << "access allow user|group|host|domain <identifier> : allows this user,group or host,domain access\n"
      << "\t <identifier> : can be a user name, user id, group name, group id, hostname or IP or domainname\n"
      << std::endl
      << "access unallow user|group|host|domain <identifier> : allows this user,group or host,domain access\n"
      << "\t <identifier> : can be a user name, user id, group name, group id, hostname or IP or domainname\n"
      << std::endl
      << "\t HINT: if you add any 'allow' the instance allows only the listed users. A banned identifier will still overrule an allowed identifier!\n"
      << std::endl
      << "access set redirect <target-host> [r|w|ENOENT|ENONET|ENETUNREACH] : allows to set a global redirection to <target-host>\n"
      << "\t <target-host>      : hostname to which all requests get redirected\n"
      << "\t         [r|w]      : optional set a redirect for read/write requests seperatly\n"
      << "\t      [ENOENT]      : optional set a redirect if a file is not existing\n"
      << "\t      [ENONET]      : optional set a redirect if a file is offline\n"
      << "\t      [ENETUNREACH] : optional set a redirect if @todo \n"
      << "\t                      <taget-hosts> can be structured like <host>:<port[:<delay-in-ms>] where <delay> holds each request for a given time before redirecting\n"
      << std::endl
      << "access set stall <stall-time> [r|w|ENOENT|ENONET|ENETUNREACH] : allows to set a global stall time\n"
      << "\t <stall-time> : time in seconds after which clients should rebounce\n"
      << "\t         [r|w]      : optional set stall time for read/write requests seperatly\n"
      << "\t      [ENOENT]      : optional set stall time if a file is not existing\n"
      << "\t      [ENONET]      : optional set stall time if a file is offline\n"
      << "\t      [ENETUNREACH] : optional set a stall time if @todo \n"
      << std::endl
      << "access set limit <frequency> rate:{user,group}:{name}:<counter>\n"
      << "\t rate:{user:group}:{name}:<counter> : stall the defined user group for 5s if the <counter> exceeds a frequency of <frequency> in a 5s interval\n"
      << "\t                                      - the instantaneous rate can exceed this value by 33%%\n"
      << "\t              rate:user:*:<counter> : apply to all users based on user counter\n"
      << "\t              rate:group:*:<counter>: apply to all groups based on group counter\n"
      << std::endl
      << "access set limit <frequency> threads:{*,max,<uid>}\n"
      << "\t                        threads:max : set the maximum number of threads running in parallel for <uid> > 3\n"
      << "\t                        threads:*   : set the default thread pool limit for all users with <uid> > 3\n"
      << "\t                      threads:<uid> : set a specific thread pool limit for user <uid>\n"
      << std::endl
      << "access set limit <nfiles> rate:user:{name}:FindFiles :\n\tset find query limit to <nfiles> for user {name}\n"
      << std::endl
      << "access set limit <ndirs> rate:user:{name}:FindDirs:\n\tset find query limit to <ndirs> for user {name}\n"
      << std::endl
      << "access set limit <nfiles> rate:group:{name}:FindFiles :\n\tset find query limit to <nfiles> for group {name}\n"
      << std::endl
      << "access set limit <ndirs> rate:group:{name}:FindDirs :\n\tset find query limit to <ndirss> for group {name}\n"
      << std::endl
      << "access set limit <nfiles> rate:user:*:FindFiles :\n\tset default find query limit to <nfiles> for everybody\n"
      << std::endl
      << "access set limit <ndirs> rate:user:*:FindDirs :\n\tset default find query limit to <ndirss> for everybody\n"
      << std::endl
      << "\t HINT : rule strength => user-limit >> group-limit >> wildcard-limit\n"
      << std::endl
      << "access rm redirect [r|w|ENOENT|ENONET|ENETUNREACH] : removes global redirection\n"
      << std::endl
      << "access rm stall [r|w|ENOENT|ENONET|ENETUNREACH] : removes global stall time\n"
      << std::endl
      << "access rm limit rate:{user,group}:{name}:<counter> : remove rate limitation\n"
      << std::endl
      << "access rm limit threads:{max,*,<uid} : remove thread pool limit\n"
      << std::endl
      << "access ls [-m] [-n] : print banned,unbanned user,group, hosts\n"
      << "\t -m : output in monitoring format with <key>=<value>\n"
      << "\t -n : don't translate uid/gids to names\n"
      << std::endl
      << "Examples:\n"
      << " access ban host foo                            : Ban host foo\n"
      << " access ban domain bar                          : Ban domain bar\n"
      << " access allow domain nobody@bar                 : Allows user nobody from domain bar\n"
      << " access allow domain -                          : use domain allow as whitelist - e.g. nobody@bar will additionally allow the nobody user from domain bar!\n"
      << " access allow domain bar                        : Allow only domain bar\n"
      << " access set redirect foo                        : Redirect all requests to host foo\n"
      << " access set redirect foo:1094:1000              : Redirect all requests to host foo:1094 and hold each reqeust for 1000ms\n"
      << " access rm redirect                             : Remove redirection to previously defined host foo\n"
      << " access set stall 60                            : Stall all clients by 60 seconds\n"
      << " access ls                                      : Print all defined access rules\n"
      << " access set limit 100  rate:user:*:OpenRead     : Limit the rate of open for read to a frequency of 100 Hz for all users\n"
      << " access set limit 2000 rate:group:zp:Stat       : Limit the stat rate for the zp group to 2kHz\n"
      << " access set limit 500 threads:*                 : Limit the thread pool usage to 500 threads per user\n"
      << " access rm limit rate:user:*:OpenRead           : Removes the defined limit\n"
      << " access rm limit threads:*                      : Removes the default per user thread pool limit\n";
  std::cerr << oss.str() << std::endl;
}
