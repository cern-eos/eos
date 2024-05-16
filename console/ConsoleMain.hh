//------------------------------------------------------------------------------
//! @file ConsoleMain.hh
//! @author Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

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

#pragma once
#include "console/GlobalOptions.hh"
#include "common/StringConversion.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdCl/XrdClFileSystem.hh"
#include "XrdCl/XrdClXRootDResponses.hh"
#include <vector>
#include <math.h>
#include <map>
#include <iostream>
#include <memory>

class XrdOucEnv;

extern const char* abspath(const char* in);
extern XrdOucString gPwd;
extern XrdOucString rstdout;
extern XrdOucString rstderr;
extern XrdOucString user_role;
extern XrdOucString group_role;
extern XrdOucString serveruri;
extern XrdOucString global_comment;
extern XrdOucString pwdfile;
extern void exit_handler(int a);
extern int global_retc;
extern bool global_highlighting;
extern bool global_debug;
extern bool interactive;
extern bool hasterminal;
extern bool silent;
extern bool timing;
extern bool pipemode;
extern bool runpipe;
extern bool ispipe;
extern bool json;
extern int output_result(XrdOucEnv* result, bool highlighting = true);
extern void command_result_stdout_to_vector(std::vector<std::string>&
    string_vector);
extern XrdOucEnv* CommandEnv;
// Structure used for the protobuf impelmentation to pass down the global
// options that are set before parsing the actual command
extern GlobalOptions gGlobalOpts;

using namespace XrdCl;

//------------------------------------------------------------------------------
//! Send client command to the MGM
//!
//! @param in command to be appended as opaque info to the XrdCl::File object
//! @param is_admin if true execute as an admin command, otherwise as an user
//!        command
//!
//! @return object containing the server response
//------------------------------------------------------------------------------
extern XrdOucEnv* client_command(XrdOucString& in, bool is_admin = false,
                                 std::string* reply = nullptr);

typedef int CFunction(char*);
//! Structure which contains information on the commands this program
//! understands.
typedef struct {
  char* name; /* User printable name of the function. */
  CFunction* func; /* Function to call to do the job. */
  char* doc; /* Documentation for this function.  */
} COMMAND;

//------------------------------------------------------------------------------
//! Help flag filter function.
//! Given a line containing command arguments, search for help keywords
//! to determine whether help message should be displayed.
//!
//! @param args_line the arguments line which to filter
//! @param no_h if true don't consider -h as requesting help e.g du -h
//!
//! @return true if help messaged should be displayed, false otherwise
//------------------------------------------------------------------------------
extern bool wants_help(const char* args_line, bool no_h = false);

extern COMMAND commands[];
extern int done;

char* stripwhite(char* string);
std::string parse_comment(const char* line, std::string& comment);
COMMAND* find_command(const char* command);
int execute_line(char* line);

int Run(int argc, char* argv[]);


//------------------------------------------------------------------------------
//! Given an input string, return the appropriate path identifier:
//!   - string begins with fid:|fxid:|cid:|cxid:|pid:|pxid: then leave untouched
//!   - otherwise -- return abspath of the given string
//!
//! @param in the string to process
//! @param escapeand whether to replace '&' symbols with '#AND#'
//!
//! @return processed path identifier
//------------------------------------------------------------------------------
std::string PathIdentifier(const char* in, bool escapeand = false);

//------------------------------------------------------------------------------
//! Check if input matches pattern and extract the file id if possible
//!
//! @param input input string which can also be a path
//! @param pattern regular expression fxid:<hex_id> | fid: <dec_id>
//!
//! @return true if input matches pattern, false otherwise
//------------------------------------------------------------------------------
bool RegWrapDenominator(XrdOucString& input, const std::string& key);

//------------------------------------------------------------------------------
//! Extract file id specifier if input is in one of the following formats:
//! fxid:<hex_id> | fid:<dec_id>
//!
//! @param input input following the above format or an actual path
//!
//! @return true if path is given as a file id specifier, otherwise false
//------------------------------------------------------------------------------
bool Path2FileDenominator(XrdOucString& path);

//------------------------------------------------------------------------------
//! Extract file id specifier if input is in one of the following formats:
//! fxid:<hex_id> | fid:<dec_id>
//!
//! @param input input following the above format or an actual path
//!
//! @param id result with the extracted id
//!
//! @return true if path is given as a file id specifier, otherwise false
//------------------------------------------------------------------------------
bool Path2FileDenominator(XrdOucString& path, unsigned long long& id);

//------------------------------------------------------------------------------
//! Extract container id specifier if input is in one of the following formats:
//! cxid:<hex_id> | cid:<dec_id>
//!
//! @param input input following the above format or an actual path
//!
//! @return true if path is given as a file id specifier, otherwise false
//------------------------------------------------------------------------------
bool Path2ContainerDenominator(XrdOucString& path);

//------------------------------------------------------------------------------
//! Extract container id specifier if input is in one of the following formats:
//! cxid:<hex_id> | cid:<dec_id>
//!
//! @param input input following the above format or an actual path
//!
//! @param id result with the extracted id
//!
//! @return true if path is given as a file id specifier, otherwise false
//------------------------------------------------------------------------------
bool Path2ContainerDenominator(XrdOucString& path, unsigned long long& id);

//------------------------------------------------------------------------------
//! Check whether the given command performs an MGM call
//!
//! @param name command name
//!
//! @param args arguments string
//!
//! @return true if remote command, false otherwise
//------------------------------------------------------------------------------
bool RequiresMgm(const std::string& name, const std::string& args);

//------------------------------------------------------------------------------
//! Check if MGM is online and reachable
//!
//! @param uri where to connect to the MGM
//!
//! @return true if MGM is online, otherwise false
//------------------------------------------------------------------------------
bool CheckMgmOnline(const std::string& uri);


//------------------------------------------------------------------------------
//! Compute a default home directory or take it from the environment.
//!
//! @return guessed home path
//------------------------------------------------------------------------------
std::string DefaultRoute();

//------------------------------------------------------------------------------
//! API to load the filesystem configuration of an instance
//------------------------------------------------------------------------------

class filesystems
{
public:
  filesystems() {}
  ~filesystems() {}

  typedef std::map<int, std::map<std::string, std::string>> filesystemmap_t;
  typedef std::map<int, std::shared_ptr<XrdCl::FileSystem>> clientmap_t;
  int Connect();
  int Load(bool verbose = false);

  filesystemmap_t& fs()
  {
    return fsmap;
  }
  clientmap_t& clients()
  {
    return clientmap;
  }
private:
  filesystemmap_t fsmap;
  clientmap_t clientmap;
};

class files
{
public:
  files() {}
  ~files() {}

  struct fileentry {
    int expired;
    int nrep;
    size_t size;
    std::string hexid;
    std::set<int> locations;
    std::string checksum;
    std::set<int> missing_locations;
    std::set<int> wrongsize_locations;
  };

  int Find(const char* path, bool verbose = false);
  int Lookup(filesystems& fsmap, bool verbose = false);
  size_t Size()
  {
    return filemap.size();
  }
  int Report(size_t expect_replica);

  class SyncResponseHandler: public ResponseHandler
  {
  public:
    SyncResponseHandler(const char* path, int fsid):
      pStatus(0),
      pResponse(0),
      pCondVar(0),
      pPath(path),
      pFsid(fsid)
    {
      pStartTime = time(NULL);
    }

    virtual ~SyncResponseHandler() {}

    virtual void HandleResponse(XRootDStatus* status,
                                AnyObject*    response)
    {
      XrdSysCondVarHelper scopedLock(pCondVar);
      pStatus = status;
      pResponse = response;
      pCondVar.Broadcast();
    }

    XRootDStatus* GetStatus()
    {
      return pStatus;
    }

    bool HasStatus()
    {
      XrdSysCondVarHelper scopedLock(pCondVar);

      if (pStatus) {
        return true;
      } else {
        return false;
      }
    }

    AnyObject* GetResponse()
    {
      return pResponse;
    }

    void WaitForResponse()
    {
      XrdSysCondVarHelper scopedLock(pCondVar);

      while (pStatus == 0) {
        pCondVar.Wait();
      }
    }

    const char* GetPath()
    {
      return pPath.c_str();
    }

    int         GetFsid()
    {
      return pFsid;
    }

    size_t      GetAge()
    {
      return time(NULL) - pStartTime;
    }

  private:
    SyncResponseHandler(const SyncResponseHandler& other);
    SyncResponseHandler& operator = (const SyncResponseHandler& other);

    XRootDStatus*    pStatus;
    AnyObject*       pResponse;
    XrdSysCondVar    pCondVar;
    std::string      pPath;
    int              pFsid;
    time_t           pStartTime;
  };

private:
  std::map<std::string, struct fileentry> filemap;
};

