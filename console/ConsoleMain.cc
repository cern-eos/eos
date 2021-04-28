//------------------------------------------------------------------------------
// File ConsoleMain.cc
// Author Andreas-Joachim Peters - CERN
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

#include "ConsoleMain.hh"
#include "ConsolePipe.hh"
#include "ConsoleCompletion.hh"
#include "console/RegexUtil.hh"
#include "XrdCl/XrdClDefaultEnv.hh"
#include "XrdCl/XrdClURL.hh"
#include "License"
#include "common/FileId.hh"
#include "common/Path.hh"
#include "common/IoPipe.hh"
#include "common/SymKeys.hh"
#include "common/StringTokenizer.hh"
#include "common/StringConversion.hh"
#include "common/StringUtils.hh"
#include "mq/XrdMqMessage.hh"
#include "mq/XrdMqTiming.hh"
#include "XrdOuc/XrdOucTokenizer.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdCl/XrdClFile.hh"
#include <zmq.hpp>
#include <iomanip>
#include <setjmp.h>
#include <readline/readline.h>
#include <readline/history.h>
#include <list>

#ifdef __APPLE__
#define ENONET 64
#endif

//------------------------------------------------------------------------------
// Implemented commands
//------------------------------------------------------------------------------
extern int com_protoaccess(char*);
extern int com_acl(char*);
extern int com_archive(char*);
extern int com_attr(char*);
extern int com_backup(char*);
extern int com_cd(char*);
extern int com_chmod(char*);
extern int com_chown(char*);
extern int com_clear(char*);
extern int com_protoconfig(char*);
extern int com_console(char*);
extern int com_convert(char*);
extern int com_cp(char*);
extern int com_protodebug(char*);
extern int com_file(char*);
extern int com_fileinfo(char*);
extern int com_find(char*);
extern int com_protonewfind(char*);
extern int com_protofs(char*);
extern int com_proto_fsck(char*);
extern int com_fuse(char*);
extern int com_fusex(char*);
extern int com_geosched(char*);
extern int com_protogroup(char*);
extern int com_health(char*);
extern int com_help(char*);
extern int com_info(char*);
extern int com_inspector(char*);
extern int com_protoio(char*);
extern int com_json(char*);
extern int com_license(char*);
extern int com_ln(char*);
extern int com_ls(char*);
extern int com_map(char*);
extern int com_member(char*);
extern int com_mkdir(char*);
extern int com_motd(char*);
extern int com_mv(char*);
extern int com_protonode(char*);
extern int com_ns(char*);
extern int com_pwd(char*);
extern int com_quit(char*);
extern int com_protoquota(char*);
extern int com_reconnect(char*);
extern int com_protorecycle(char*);
extern int com_rm(char*);
extern int com_route(char*);
extern int com_qos(char*);
extern int com_protorm(char*);
extern int com_rmdir(char*);
extern int com_role(char*);
extern int com_rtlog(char*);
extern int com_status(char*);
extern int com_silent(char*);
extern int com_protospace(char*);
extern int com_stagerrm(char*);
extern int com_stat(char*);
extern int com_squash(char*);
extern int com_test(char*);
extern int com_timing(char*);
extern int com_tracker(char*);
extern int com_transfer(char*);
extern int com_touch(char*);
extern int com_proto_token(char*);
extern int com_proto_share(char*);
extern int com_version(char*);
extern int com_vid(char*);
extern int com_whoami(char*);
extern int com_who(char*);
extern int com_accounting(char*);
extern int com_quota(char*);

//------------------------------------------------------------------------------
// Command mapping array
//------------------------------------------------------------------------------
COMMAND commands[] = {
  { (char*) "access", com_protoaccess, (char*) "Access Interface"},
  { (char*) "accounting", com_accounting, (char*) "Accounting Interface"},
  { (char*) "acl", com_acl, (char*) "Acl Interface"},
  { (char*) "archive", com_archive, (char*) "Archive Interface"},
  { (char*) "attr", com_attr, (char*) "Attribute Interface"},
  { (char*) "backup", com_backup, (char*) "Backup Interface"},
  { (char*) "clear", com_clear, (char*) "Clear the terminal"},
  { (char*) "cd", com_cd, (char*) "Change directory"},
  { (char*) "chmod", com_chmod, (char*) "Mode Interface"},
  { (char*) "chown", com_chown, (char*) "Chown Interface"},
  { (char*) "config", com_protoconfig, (char*) "Configuration System"},
  { (char*) "console", com_console, (char*) "Run Error Console"},
  { (char*) "convert", com_convert, (char*) "Convert Interface"},
  { (char*) "cp", com_cp, (char*) "Cp command"},
  { (char*) "debug", com_protodebug, (char*) "Set debug level"},
  { (char*) "exit", com_quit, (char*) "Exit from EOS console"},
  { (char*) "file", com_file, (char*) "File Handling"},
  { (char*) "fileinfo", com_fileinfo, (char*) "File Information"},
  { (char*) "find", com_find, (char*) "Find files/directories"},
  { (char*) "newfind", com_protonewfind, (char*) "Find files/directories (new implementation)"},
  { (char*) "fs", com_protofs, (char*) "File System configuration"},
  { (char*) "fsck", com_proto_fsck, (char*) "File System Consistency Checking"},
  { (char*) "fuse", com_fuse, (char*) "Fuse Mounting"},
  { (char*) "fusex", com_fusex, (char*) "Fuse(x) Administration"},
  { (char*) "geosched", com_geosched, (char*) "Geoscheduler Interface"},
  { (char*) "group", com_protogroup, (char*) "Group configuration"},
  { (char*) "health", com_health, (char*) "Health information about system"},
  { (char*) "help", com_help, (char*) "Display this text"},
  { (char*) "info", com_info, (char*) "Retrieve file or directory information"},
  { (char*) "inspector", com_inspector, (char*) "Interact with File Inspector"},
  { (char*) "io", com_protoio, (char*) "IO Interface"},
  { (char*) "json", com_json, (char*) "Toggle JSON output flag for stdout"},
  { (char*) "license", com_license, (char*) "Display Software License"},
  { (char*) "ls", com_ls, (char*) "List a directory"},
  { (char*) "ln", com_ln, (char*) "Create a symbolic link"},
  { (char*) "map", com_map, (char*) "Path mapping interface"},
  { (char*) "member", com_member, (char*) "Check Egroup membership"},
  { (char*) "mkdir", com_mkdir, (char*) "Create a directory"},
  { (char*) "motd", com_motd, (char*) "Message of the day"},
  { (char*) "mv", com_mv, (char*) "Rename file or directory"},
  { (char*) "node", com_protonode, (char*) "Node configuration"},
  { (char*) "ns", com_ns, (char*) "Namespace Interface"},
  { (char*) "pwd", com_pwd, (char*) "Print working directory"},
  { (char*) "qos", com_qos, (char*) "QoS configuration"},
  { (char*) "quit", com_quit, (char*) "Exit from EOS console"},
  { (char*) "quota", com_protoquota, (char*) "Quota System configuration"},
  { (char*) "reconnect", com_reconnect, (char*) "Forces a re-authentication of the shell"},
  { (char*) "recycle", com_protorecycle, (char*) "Recycle Bin Functionality"},
  { (char*) "rmdir", com_rmdir, (char*) "Remove a directory"},
  { (char*) "rm", com_protorm, (char*) "Remove a file"},
  { (char*) "role", com_role, (char*) "Set the client role"},
  { (char*) "route", com_route, (char*) "Routing interface"},
  { (char*) "rtlog", com_rtlog, (char*) "Get realtime log output from mgm & fst servers"},
  { (char*) "share", com_proto_share, (char*) "Share interface"},
  { (char*) "silent", com_silent, (char*) "Toggle silent flag for stdout"},
  { (char*) "space", com_protospace, (char*) "Space configuration"},
  { (char*) "stagerrm", com_stagerrm, (char*) "Remove disk replicas of a file if it has tape replicas"},
  { (char*) "stat", com_stat, (char*) "Run 'stat' on a file or directory"},
  { (char*) "status", com_status, (char*) "Display status information on an MGM"},
  { (char*) "squash", com_squash, (char*) "Run 'squashfs' utility function"},
  { (char*) "test", com_test, (char*) "Run performance test"},
  { (char*) "timing", com_timing, (char*) "Toggle timing flag for execution time measurement"},
  { (char*) "touch", com_touch, (char*) "Touch a file"},
  { (char*) "token", com_proto_token, (char*) "Token interface"},
  { (char*) "tracker", com_tracker, (char*) "Interact with File Tracker"},
  { (char*) "transfer", com_transfer, (char*) "Transfer Interface"},
  { (char*) "version", com_version, (char*) "Verbose client/server version"},
  { (char*) "vid", com_vid, (char*) "Virtual ID System Configuration"},
  { (char*) "whoami", com_whoami, (char*) "Determine how we are mapped on server side"},
  { (char*) "who", com_who, (char*) "Statistics about connected users"},
  { (char*) "?", com_help, (char*) "Synonym for 'help'"},
  { (char*) ".q", com_quit, (char*) "Exit from EOS console"},
  { (char*) 0, (int (*)(char*))0, (char*) 0}
};

//------------------------------------------------------------------------------
// Global variables
//------------------------------------------------------------------------------
XrdOucString serveruri = "";
XrdOucString historyfile = "";
XrdOucString pwdfile = "";
XrdOucString gPwd = "/";
XrdOucString rstdout;
XrdOucString rstderr;
XrdOucString rstdjson;
XrdOucString user_role = "";
XrdOucString group_role = "";
XrdOucString global_comment = "";

int global_retc = 0;
bool global_highlighting = true;
bool global_debug = false;
bool interactive = true;
bool hasterminal = true;
bool silent = false;
bool timing = false;
bool pipemode = false;
bool runpipe = false;
bool ispipe = false;
bool json = false;
GlobalOptions gGlobalOpts;

eos::common::IoPipe iopipe;
int retcfd = 0;
//! When non-zero, this global means the user is done using this program. */
int done;

// Pointer to the result of client_command. It gets invalid when the
// output_result function is called.
XrdOucEnv* CommandEnv = 0;
static sigjmp_buf sigjump_buf;

//------------------------------------------------------------------------------
// Exit handler
//------------------------------------------------------------------------------
void
exit_handler(int a)
{
  fprintf(stdout, "\n");
  fprintf(stderr, "<Control-C>\n");
  write_history(historyfile.c_str());

  if (ispipe) {
    iopipe.UnLockProducer();
  }

  exit(-1);
}

//------------------------------------------------------------------------------
// Jump handler
//------------------------------------------------------------------------------
void
jump_handler(int a)
{
  siglongjmp(sigjump_buf, 1);
}

//------------------------------------------------------------------------------
// Absolute path conversion function
//------------------------------------------------------------------------------
const char*
abspath(const char* in)
{
  static XrdOucString inpath;
  inpath = in;

  if (inpath.beginswith("/")) {
    return inpath.c_str();
  }

  if (gPwd == "/") {
    // check if we are in a /eos/ mountpoint
    char pwd[4096];

    if (getcwd(pwd, sizeof(pwd))) {
      XrdOucString lpwd = pwd;

      if (lpwd.beginswith("/eos")) {
        inpath = pwd;
        inpath += "/";
      } else {
        inpath = gPwd;
      }
    } else {
      inpath = gPwd;
    }
  } else {
    inpath = gPwd;
  }

  inpath += in;
  eos::common::Path cPath(inpath.c_str());
  inpath = cPath.GetPath();
  return inpath.c_str();
}

//------------------------------------------------------------------------------
// Help flag filter
//------------------------------------------------------------------------------
bool
wants_help(const char* args_line)
{
  XrdOucString allargs = " ";
  allargs += args_line;
  allargs += " ";

  if ((allargs.find(" help ") != STR_NPOS) ||
      (allargs.find("\"-h\"") != STR_NPOS) ||
      (allargs.find("\"--help\"") != STR_NPOS) ||
      (allargs.find(" -h ") != STR_NPOS) ||
      (allargs.find(" \"-h\" ") != STR_NPOS) ||
      (allargs.find(" --help ") != STR_NPOS) ||
      (allargs.find(" \"--help\" ") != STR_NPOS)) {
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Switches stdin, stdout, stderr to pipe mode where we are a persistent
// communication daemon for a the eospipe command forwarding commands.
//------------------------------------------------------------------------------
bool
startpipe()
{
  XrdOucString pipedir = "";
  XrdOucString stdinname = "";
  XrdOucString stdoutname = "";
  XrdOucString stderrname = "";
  XrdOucString retcname = "";
  ispipe = true;
  close(STDIN_FILENO);
  close(STDOUT_FILENO);
  close(STDERR_FILENO);

  if (!iopipe.Init()) {
    fprintf(stderr, "error: cannot set IoPipe\n");
    return false;
  }

  XrdSysLogger* logger = new XrdSysLogger();
  XrdSysError eDest(logger);
  int stdinfd = iopipe.AttachStdin(eDest);
  int stdoutfd = iopipe.AttachStdout(eDest);
  int stderrfd = iopipe.AttachStderr(eDest);
  retcfd = iopipe.AttachRetc(eDest);

  if ((stdinfd < 0) ||
      (stdoutfd < 0) ||
      (stderrfd < 0) ||
      (retcfd < 0)) {
    fprintf(stderr, "error: cannot attach to pipes\n");
    return false;
  }

  if (!iopipe.LockProducer()) {
    return false;
  }

  stdin = fdopen(stdinfd, "r");
  stdout = fdopen(stdoutfd, "w");
  stderr = fdopen(stderrfd, "w");
  return true;
}


/* **************************************************************** */
/*                                                                  */
/*                       EOSConsole Commands                        */
/*                                                                  */
/* **************************************************************** */
void
command_result_stdout_to_vector(std::vector<std::string>& string_vector)
{
  string_vector.clear();

  if (!CommandEnv) {
    fprintf(stderr, "error: command env is 0!\n");
    return;
  }

  rstdout = CommandEnv->Get("mgm.proc.stdout");

  if (!rstdout.length()) {
    return;
  }

  if (rstdout.beginswith("base64:")) {
    XrdOucString ub64out;
    eos::common::SymKey::DeBase64(rstdout, ub64out);
    rstdout = ub64out;
  } else {
    XrdMqMessage::UnSeal(rstdout);
  }

  XrdOucTokenizer subtokenizer((char*) rstdout.c_str());
  const char* nextline = 0;
  int i = 0;

  while ((nextline = subtokenizer.GetLine())) {
    if ((!strlen(nextline)) || (nextline[0] == '\n')) {
      continue;
    }

    string_vector.resize(i + 1);
    string_vector.push_back(nextline);
    i++;
  }
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
int
output_result(XrdOucEnv* result, bool highlighting)
{
  if (!result) {
    return EINVAL;
  }

  rstdout = result->Get("mgm.proc.stdout");
  rstderr = result->Get("mgm.proc.stderr");
  rstdjson = result->Get("mgm.proc.json");

  if (rstdout.beginswith("base64:")) {
    XrdOucString ub64out;
    eos::common::SymKey::DeBase64(rstdout, ub64out);
    rstdout = ub64out;
  } else {
    XrdMqMessage::UnSeal(rstdout);
  }

  if (rstderr.beginswith("base64:")) {
    XrdOucString ub64out;
    eos::common::SymKey::DeBase64(rstderr, ub64out);
    rstderr = ub64out;
  } else {
    XrdMqMessage::UnSeal(rstderr);
  }

  if (rstdjson.beginswith("base64:")) {
    XrdOucString ub64out;
    eos::common::SymKey::DeBase64(rstdjson, ub64out);
    rstdjson = ub64out;
  } else {
    XrdMqMessage::UnSeal(rstdjson);
  }

  if (highlighting && global_highlighting) {
    // color replacements
    rstdout.replace("[booted]", "\033[1m[booted]\033[0m");
    rstdout.replace("[down]", "\033[49;31m[down]\033[0m");
    rstdout.replace("[failed]", "\033[49;31m[failed]\033[0m");
    rstdout.replace("[booting]", "\033[49;32m[booting]\033[0m");
    rstdout.replace("[compacting]", "\033[49;34m[compacting]\033[0m");
    // replication highlighting
    rstdout.replace("master-rw", "\033[49;31mmaster-rw\033[0m");
    rstdout.replace("master-ro", "\033[49;34mmaster-ro\033[0m");
    rstdout.replace("slave-ro", "\033[1mslave-ro\033[0m");
    rstdout.replace("=ok", "=\033[49;32mok\033[0m");
    rstdout.replace("=compacting", "=\033[49;32mcompacting\033[0m");
    rstdout.replace("=off", "=\033[49;34moff\033[0m");
    rstdout.replace("=blocked", "=\033[49;34mblocked\033[0m");
    rstdout.replace("=wait", "=\033[49;34mwait\033[0m");
    rstdout.replace("=starting", "=\033[49;34mstarting\033[0m");
    rstdout.replace("=true", "=\033[49;32mtrue\033[0m");
    rstdout.replace("=false", "=\033[49;31mfalse\033[0m");
  }

  int retc = 0;

  if (result->Get("mgm.proc.retc")) {
    retc = atoi(result->Get("mgm.proc.retc"));
  }

  if (json) {
    if (rstdjson.length())
      if (!silent) {
        fprintf(stdout, "%s", rstdjson.c_str());

        if (rstdjson.endswith('\n')) {
          fprintf(stdout, "\n");
        }
      }
  } else {
    if (rstdout.length())
      if (!silent) {
        fprintf(stdout, "%s", rstdout.c_str());

        if (!rstdout.endswith('\n')) {
          fprintf(stdout, "\n");
        }
      }

    if (rstderr.length()) {
      fprintf(stderr, "%s (errc=%d) (%s)\n", rstderr.c_str(), retc, strerror(retc));
    }
  }

  fflush(stdout);
  fflush(stderr);
  CommandEnv = 0;
  delete result;
  return retc;
}

//------------------------------------------------------------------------------
// Execute user command
//------------------------------------------------------------------------------
XrdOucEnv*
client_command(XrdOucString& in, bool is_admin, std::string* reply)
{
  if (user_role.length()) {
    in += "&eos.ruid=";
  }

  in += user_role;

  if (group_role.length()) {
    in += "&eos.rgid=";
  }

  in += group_role;

  if (json) {
    in += "&mgm.format=json";
  }

  if (global_comment.length()) {
    in += "&mgm.comment=";
    in += global_comment;
    global_comment = "";
  }

  if (getenv("EOSAUTHZ")) {
    in += "&authz=";
    in += getenv("EOSAUTHZ");
  }


  XrdMqTiming mytiming("eos");
  TIMING("start", &mytiming);
  XrdOucString out = "";
  XrdOucString path = serveruri;

  if (is_admin) {
    path += "//proc/admin/";
  } else {
    path += "//proc/user/";
  }

  path += "?";
  path += in;

  if (global_debug) {
    printf("> %s\n", path.c_str());
  }

  if (path.beginswith("ipc://")) {
    // local ZMQ ipc connection
    zmq::context_t context(1);
    zmq::socket_t socket (context, ZMQ_REQ);
    path.erase(0,serveruri.length()+1);
    socket.connect(serveruri.c_str());
    zmq::message_t request(path.length());
    memcpy(request.data(), path.c_str(), path.length());
    socket.send(request);
    zmq::message_t response;
    socket.recv(&response);
    
    std::string sout;
    sout.assign((char*)response.data(), response.size());
    CommandEnv = new XrdOucEnv(sout.c_str());

    if (reply) {
      reply->assign(out.c_str());
    }
    
  } else {
    // xrootd based connection
    XrdCl::OpenFlags::Flags flags_xrdcl = XrdCl::OpenFlags::Read;
    std::unique_ptr<XrdCl::File> client {new XrdCl::File()};
    XrdCl::XRootDStatus status = client->Open(path.c_str(), flags_xrdcl);
    
    if (status.IsOK()) {
      off_t offset = 0;
      uint32_t nbytes = 0;
      char buffer[4096 + 1];
      status = client->Read(offset, 4096, buffer, nbytes);
      
      while (status.IsOK() && (nbytes > 0)) {
	buffer[nbytes] = 0;
	out += buffer;
	offset += nbytes;
	status = client->Read(offset, 4096, buffer, nbytes);
      }
      
      status = client->Close();
      TIMING("stop", &mytiming);
      
      if (timing) {
	mytiming.Print();
      }
      
      if (global_debug) {
	printf("> %s\n", out.c_str());
      }
      
      CommandEnv = new XrdOucEnv(out.c_str());
      
      // Save the reply string from the server
      if (reply) {
	reply->assign(out.c_str());
      }
    } else {
      std::string errmsg;
      std::ostringstream oss;
      int retc = status.GetShellCode();
      
      if (status.errNo) {
	retc = status.errNo;
      }
      
      oss << "mgm.proc.stdout=&"
	  << "mgm.proc.stderr=" << "error: errc=" << retc
	  << " msg=\"" << status.ToString() << "\"&"
	  << "mgm.proc.retc=" << retc;
      CommandEnv = new XrdOucEnv(oss.str().c_str());
      
      // Save the reply string from the server
      if (reply) {
	reply->assign(oss.str().c_str());
      }
    }
  }
  return CommandEnv;
}

//------------------------------------------------------------------------------
// Load and apply the last used directory
//------------------------------------------------------------------------------
void
read_pwdfile()
{
  std::string lpwd;
  eos::common::StringConversion::LoadFileIntoString(pwdfile.c_str(), lpwd);

  if (lpwd.length()) {
    com_cd((char*) lpwd.c_str());
  }
}

//------------------------------------------------------------------------------
// Colour definitions
//------------------------------------------------------------------------------
std::string textnormal("\001\033[0m\002");
std::string textblack("\001\033[49;30m\002");
std::string textred("\001\033[49;31m\002");
std::string textrederror("\001\033[47;31m\e[5m\002");
std::string textblueerror("\001\033[47;34m\e[5m\002");
std::string textgreen("\001\033[49;32m\002");
std::string textyellow("\001\033[49;33m\002");
std::string textblue("\001\033[49;34m\002");
std::string textbold("\001\033[1m\002");
std::string textunbold("\001\033[0m\002");

//------------------------------------------------------------------------------
// Usage Information
//------------------------------------------------------------------------------
void
usage()
{
  fprintf(stderr,
          "`eos' is the command line interface (CLI) of the EOS storage system.\n");
  fprintf(stderr,
          "Usage: eos [-r|--role <uid> <gid>] [-s] [-b|--batch] [-v|--version] [-p|--pipe] [-j|--json] [<mgm-url>] [<cmd> {<argN>}|<filename>.eosh]\n");
  fprintf(stderr,
          "            -r, --role <uid> <gid>              : select user role <uid> and group role <gid>\n");
  fprintf(stderr,
          "            -b, --batch                         : run in batch mode without colour and syntax highlighting and without pipe\n");
  fprintf(stderr,
          "            -j, --json                          : switch to json output format\n");
  fprintf(stderr,
          "            -p, --pipe                          : run stdin,stdout,stderr on local pipes and go to background\n");
  fprintf(stderr,
          "            -h, --help                          : print help text\n");
  fprintf(stderr,
          "            -v, --version                       : print version information\n");
  fprintf(stderr,
          "            -s                                  : run <status> command\n");
  fprintf(stderr,
          "            <mgm-url>                           : XRoot URL of the management server e.g. root://<hostname>[:<port>]\n");
  fprintf(stderr,
          "            <cmd>                               : eos shell command (use 'eos help' to see available commands)\n");
  fprintf(stderr,
          "            {<argN>}                            : single or list of arguments for the eos shell command <cmd>\n");
  fprintf(stderr,
          "            <filename>.eosh                     : eos script file name ending with .eosh suffix\n\n");
  fprintf(stderr, "Environment Variables: \n");
  fprintf(stderr,
          "            EOS_MGM_URL                         : sets the redirector URL - if ipc://[ipc-path] is used, it will talk via ZMQ messaging to a single dedicated thread in the MGM\n");
  fprintf(stderr,
          "            EOS_HISTORY_FILE                    : sets the command history file - by default '$HOME/.eos_history' is used\n\n");
  fprintf(stderr,
          "            EOS_PWD_FILE                        : sets the file where the last working directory is stored- by default '$HOME/.eos_pwd\n\n");
  fprintf(stderr,
          "            EOS_ENABLE_PIPEMODE                 : allows the EOS shell to split into a session and pipe executable to avoid useless re-authentication\n");
  fprintf(stderr, "Return Value: \n");
  fprintf(stderr,
          "            The return code of the last executed command is returned. 0 is returned in case of success otherwise <errno> (!=0).\n\n");
  fprintf(stderr, "Examples:\n");
  fprintf(stderr,
          "            eos                                 : start the interactive eos shell client connected to localhost or URL defined in environment variable EOS_MGM_URL\n");
  fprintf(stderr,
          "            eos -r 0 0                          : as before but take role root/root [only numeric IDs are supported]\n");
  fprintf(stderr,
          "            eos root://myeos                    : start the interactive eos shell connecting to MGM host 'myeos'\n");
  fprintf(stderr,
          "            eos -b whoami                       : run the eos shell command 'whoami' in batch mode without syntax highlighting\n");
  fprintf(stderr,
          "            eos space ls --io                   : run the eos shell command 'space' with arguments 'ls --io'\n");
  fprintf(stderr,
          "            eos --version                       : print version information\n");
  fprintf(stderr,
          "            eos -b eosscript.eosh               : run the eos shell script 'eosscript.eosh'. This script has to contain linewise commands which are understood by the eos interactive shell\n");
  fprintf(stderr,
	  "            eos -s                              : run <status> command\n");
  fprintf(stderr, "\n");
  fprintf(stderr,
          "You can leave the interactive shell with <Control-D>. <Control-C> cleans the current shell line or terminates the shell when a command is currently executed.\n");
  fprintf(stderr, "Report bugs to eos-dev@cern.ch\n");
}

//------------------------------------------------------------------------------
// Main executable
//------------------------------------------------------------------------------
int
Run(int argc, char* argv[])
{
  char* line, *s;
  serveruri = (char*) "root://localhost";
  // Enable fork handlers for XrdCl
  XrdCl::Env* env = XrdCl::DefaultEnv::GetEnv();
  env->PutInt("RunForkHandler", 1);

  if (getenv("EOS_MGM_URL")) {
    serveruri = getenv("EOS_MGM_URL");
    if (serveruri == "ipc://") {
      // set the default ipc pipe
      serveruri = "ipc:///var/eos/md/.admin_socket:1094";
    }
  }

  gGlobalOpts.mMgmUri = serveruri.c_str();
  XrdOucString urole = "";
  XrdOucString grole = "";
  bool selectedrole = false;
  int argindex = 1;
  int retc = system("test -t 0 && test -t 1");

  if (getenv("EOS_ENABLE_PIPEMODE")) {
    runpipe = true;
  } else {
    runpipe = false;
  }

  if (getenv("EOS_CONSOLE_DEBUG")) {
    global_debug = true;
    gGlobalOpts.mDebug = true;
  }

  if (!retc) {
    hasterminal = true;
    global_highlighting = true;
    interactive = true;
  } else {
    hasterminal = false;
    global_highlighting = false;
    interactive = false;
  }

  if (argc > 1) {
    XrdOucString in1 = argv[argindex];

    if (in1.beginswith("-")) {
      if ((in1 != "--help") &&
          (in1 != "--version") &&
          (in1 != "--batch") &&
          (in1 != "--pipe") &&
          (in1 != "--role") &&
          (in1 != "--json") &&
          (in1 != "-h") &&
          (in1 != "-b") &&
          (in1 != "-p") &&
          (in1 != "-v") &&
          (in1 != "-s") &&
          (in1 != "-j") &&
          (in1 != "-r")) {
        usage();
        exit(-1);
      }
    }

    if ((in1 == "--help") || (in1 == "-h")) {
      usage();
      exit(-1);
    }

    if ((in1 == "-s")) {
      return com_status(0);
    }

    if ((in1 == "--version") || (in1 == "-v")) {
      fprintf(stderr, "EOS %s (2020)\n\n", VERSION);
      fprintf(stderr, "Developed by the CERN IT storage group\n");
      exit(-1);
    }

    if ((in1 == "--batch") || (in1 == "-b")) {
      interactive = false;
      global_highlighting = false;
      runpipe = false;
      argindex++;
      in1 = argv[argindex];
    }

    if ((in1 == "--json") || (in1 == "-j")) {
      interactive = false;
      global_highlighting = false;
      json = true;
      runpipe = false;
      argindex++;
      in1 = argv[argindex];
      gGlobalOpts.mJsonFormat = true;
    }

    if ((in1 == "fuse")) {
      interactive = false;
      global_highlighting = false;
      runpipe = false;
    }

    if ((in1 == "--pipe") || (in1 == "-p")) {
      pipemode = true;
      argindex++;
      in1 = argv[argindex];

      if (!startpipe()) {
        fprintf(stderr, "error: unable to start the pipe - maybe there is "
                "already a process with 'eos -p' running?\n");
        exit(-1);
      }
    }

    if ((in1 == "--role") || (in1 == "-r")) {
      urole = argv[argindex + 1];
      grole = argv[argindex + 2];
      in1 = argv[argindex + 3];
      argindex += 3;
      // execute the role function
      XrdOucString cmdline = "role ";
      cmdline += urole;
      cmdline += " ";
      cmdline += grole;
      in1 = argv[argindex];

      if (in1.length()) {
        silent = true;
      }

      execute_line((char*) cmdline.c_str());

      if (in1.length()) {
        silent = false;
      }

      selectedrole = true;
    }

    if ((in1 == "--batch") || (in1 == "-b")) {
      interactive = false;
      argindex++;
      in1 = argv[argindex];
    }

    if ((in1 == "cp")) {
      interactive = false;
      global_highlighting = false;
      runpipe = false;
    }

    if ((in1 == "fuse")) {
      interactive = false;
    }

    if (in1.beginswith("root://")) {
      serveruri = argv[argindex];
      gGlobalOpts.mMgmUri = serveruri.c_str();
      argindex++;
      in1 = argv[argindex];
    }

    if (in1.beginswith("ipc://")) {
      serveruri = argv[argindex];
      if (serveruri == "ipc://") {
	// set the default ipc pipe
	serveruri = "ipc:///var/eos/md/.admin_socket:1094";
      }
      gGlobalOpts.mMgmUri = serveruri.c_str();
      argindex++;
      in1 = argv[argindex];
    }

    if (in1.length()) {
      // check if this is a file (workaround for XrdOucString bug
      if ((in1.length() > 5) && (in1.endswith(".eosh")) &&
          (!access(in1.c_str(), R_OK))) {
        // this is a script file
        char str[16384];
        fstream file_op(in1.c_str(), ios::in);

        while (!file_op.eof()) {
          file_op.getline(str, 16384);
          XrdOucString cmdline = "";
          cmdline = str;

          if (!cmdline.length()) {
            break;
          }

          while (cmdline.beginswith(" ")) {
            cmdline.erase(0, 1);
          }

          while (cmdline.endswith(" ")) {
            cmdline.erase(cmdline.length() - 1, 1);
          }

          execute_line((char*) cmdline.c_str());
        }

        file_op.close();
        exit(0);
      } else {
        XrdOucString cmdline = "";

        // enclose all arguments except first in quotes
        for (int i = argindex; i < argc; i++) {
          if (i == argindex) {
            cmdline += argv[i];
          } else {
            stringstream ss;
            ss << std::quoted(argv[i]);
            cmdline += " ";
            cmdline += ss.str().c_str();
          }
        }

        if ((!selectedrole) && (!getuid()) &&
            (serveruri.beginswith("root://localhost"))) {
          // we are root, we always select also the root role by default
          XrdOucString cmdline = "role 0 0 ";
          silent = true;
          execute_line((char*) cmdline.c_str());
          silent = false;
        }

        // strip leading and trailing white spaces
        while (cmdline.beginswith(" ")) {
          cmdline.erase(0, 1);
        }

        while (cmdline.endswith(" ")) {
          cmdline.erase(cmdline.length() - 1, 1);
        }

        // Here we can use the 'eospipe' mechanism if allowed
        if (runpipe) {
          cmdline += "\n";
          // put the eos daemon into batch mode
          interactive = false;
          global_highlighting = false;
          iopipe.Init(); // need to initialize for CheckProducer

          if (!iopipe.CheckProducer()) {
            // We need to run a pipe daemon, so we fork here and let the fork
            // run the code like 'eos -p'
            if (!fork()) {
              for (int i = 1; i < argc; i++) {
                for (size_t j = 0; j < strlen(argv[i]); j++) {
                  argv[i][j] = '*';
                }
              }

              // detach from the session id
              pid_t sid;

              if ((sid = setsid()) < 0) {
                fprintf(stderr, "ERROR: failed to create new session (setsid())\n");
                exit(-1);
              }

              startpipe();
              pipemode = true;
              // enters down the readline loop with modified stdin,stdout,stderr
            } else {
              // now we just deal with the pipes from the client end
              exit(pipe_command(cmdline.c_str()));
            }
          } else {
            // now we just deal with the pipes from the client end
            exit(pipe_command(cmdline.c_str()));
          }
        } else {
          execute_line((char*) cmdline.c_str());
          exit(global_retc);
        }
      }
    }
  }

  // By default select the root role if we are root@localhost
  if ((!selectedrole) && (!getuid()) &&
      (serveruri.beginswith("root://localhost"))) {
    // we are root, we always select also the root role by default
    XrdOucString cmdline = "role 0 0 ";
    silent = true;
    execute_line((char*) cmdline.c_str());
    silent = false;
  }

  /* install a shutdown handler */
  //signal(SIGINT, exit_handler);

  if (!interactive) {
    textnormal = "";
    textblack = "";
    textred = "";
    textrederror = "";
    textblueerror = "";
    textgreen = "";
    textyellow = "";
    textblue = "";
    textbold = "";
    textunbold = "";
  }

  if (interactive) {
    fprintf(stderr,
            "# ---------------------------------------------------------------------------\n");
    fprintf(stderr, "# EOS  Copyright (C) 2011-2020 CERN/Switzerland\n");
    fprintf(stderr,
            "# This program comes with ABSOLUTELY NO WARRANTY; for details type `license'.\n");
    fprintf(stderr,
            "# This is free software, and you are welcome to redistribute it \n");
    fprintf(stderr, "# under certain conditions; type `license' for details.\n");
    fprintf(stderr,
            "# ---------------------------------------------------------------------------\n");
    execute_line((char*) "motd");
    execute_line((char*) "version");
  }

  char prompt[4096];

  if (pipemode) {
    prompt[0] = 0;
  } else {
    sprintf(prompt, "%sEOS Console%s [%s%s%s] |> ", textbold.c_str(),
            textunbold.c_str(), textred.c_str(), serveruri.c_str(), textnormal.c_str());
  }

  // Bind our completer
  rl_readline_name = (char*) "EOS Console";
  rl_attempted_completion_function = eos_console_completion;
  rl_completer_quote_characters = (char*) "\"";
  rl_completion_append_character = '\0';

  if (getenv("EOS_HISTORY_FILE")) {
    historyfile = getenv("EOS_HISTORY_FILE");
  } else {
    if (getenv("HOME")) {
      historyfile = getenv("HOME");
      historyfile += "/.eos_history";
    }
  }

  if (getenv("EOS_PWD_FILE")) {
    pwdfile = getenv("EOS_PWD_FILE");
  } else {
    if (getenv("HOME")) {
      pwdfile = getenv("HOME");
      pwdfile += "/.eos_pwd";
    }
  }

  read_history(historyfile.c_str());
  // load the last used current working directory
  read_pwdfile();

  // Loop reading and executing lines until the user quits.
  for (; done == 0;) {
    char prompt[4096];

    if (pipemode) {
      prompt[0] = 0;
    } else {
      sprintf(prompt, "%sEOS Console%s [%s%s%s] |%s> ", textbold.c_str(),
              textunbold.c_str(), textred.c_str(), serveruri.c_str(), textnormal.c_str(),
              gPwd.c_str());
    }

    if (pipemode) {
      signal(SIGALRM, exit_handler);
      alarm(60);
    }

    signal(SIGINT, jump_handler);

    if (sigsetjmp(sigjump_buf, 1)) {
      signal(SIGINT, jump_handler);
      fprintf(stdout, "\n");
    }

    line = readline(prompt);
    signal(SIGINT, exit_handler);

    if (pipemode) {
      alarm(0);
    }

    if (!line) {
      fprintf(stdout, "\n");
      break;
    }

    // Remove leading and trailing whitespace from the line. Then, if there
    // is anything left, add it to the history list and execute it.
    s = stripwhite(line);

    if (*s) {
      add_history(s);
      // 20 minutes timeout for commands ... that is long !
      signal(SIGALRM, exit_handler);
      alarm(3600);
      execute_line(s);
      alarm(0);
      char newline = '\n';
      int n = 0;
      std::cout << std::flush;
      std::cerr << std::flush;
      fflush(stdout);
      fflush(stderr);

      if (pipemode) {
        n = write(retcfd, &global_retc, sizeof(global_retc));
        n = write(retcfd, &newline, sizeof(newline));

        if (n != 1) {
          fprintf(stderr, "error: unable to write retc to retc-socket\n");
          exit(-1);
        }

        // we send the stop sequence to the pipe thread listeners
        fprintf(stdout, "#__STOP__#\n");
        fprintf(stderr, "#__STOP__#\n");
        fflush(stdout);
        fflush(stderr);
      }
    }

    free(line);
  }

  write_history(historyfile.c_str());
  signal(SIGINT, SIG_IGN);
  exit(0);
}

//------------------------------------------------------------------------------
// Command line execution function
//------------------------------------------------------------------------------
int
execute_line(char* line)
{
  std::string comment;
  std::string line_without_comment = parse_comment(line, comment);

  if (line_without_comment.empty()) {
    fprintf(stderr, "error: syntax for comment is '<command> <args> "
            "--comment \"<comment>\"'\n");
    global_retc = -1;
    return (-1);
  }

  global_comment = comment.c_str();
  gGlobalOpts.mComment = comment.c_str();
  // Isolate the command word from the rest of the arguments
  std::list<std::string> tokens = eos::common::StringTokenizer::split
                                  <std::list<std::string>>(line_without_comment.c_str(), ' ');

  if (!tokens.size()) {
    global_retc = -1;
    return (-1);
  }

  COMMAND* command = find_command(tokens.begin()->c_str());

  if (!command) {
    fprintf(stderr, "%s: No such command for EOS Console.\n",
            tokens.begin()->c_str());
    global_retc = -1;
    return (-1);
  }

  // Extract arguments string from full command line
  line_without_comment = line_without_comment.substr(tokens.begin()->size());
  eos::common::trim(line_without_comment);
  std::string args = line_without_comment;

  // Check MGM availability
  if (RequiresMgm(command->name, args) &&
      !CheckMgmOnline(serveruri.c_str())) {
    std::cerr << "error: MGM " << serveruri.c_str()
              << " not online/reachable" << std::endl;
    exit(ENONET);
  }

  return ((*(command->func))((char*)args.c_str()));
}

//------------------------------------------------------------------------------
// Look up NAME as the name of a command, and return a pointer to that command.
// Return a 0 pointer if NAME isn't a command name.
//------------------------------------------------------------------------------
COMMAND*
find_command(const char* name)
{
  for (int i = 0; commands[i].name; ++i) {
    if (strcmp(name, commands[i].name) == 0) {
      return (&commands[i]);
    }
  }

  return ((COMMAND*) 0);
}

//------------------------------------------------------------------------------
// Strip whitespace from the start and end of STRING.  Return a pointer to
// STRING.
//------------------------------------------------------------------------------
char*
stripwhite(char* string)
{
  char* s, *t;

  for (s = string; (*s) == ' '; s++)
    ;

  if (*s == 0) {
    return (s);
  }

  t = s + strlen(s) - 1;

  while (t > s && ((*t) == ' ')) {
    t--;
  }

  *++t = '\0';
  return s;
}

//------------------------------------------------------------------------------
// Parse the command line, extracts the comment
// and returns the line without the comment in it
//------------------------------------------------------------------------------
std::string
parse_comment(const char* line, std::string& comment)
{
  std::string exec_line = line;
  // Commands issued from the EOS shell do not encase arguments in quotes
  // whereas commands issued from the terminal do
  size_t cbpos = exec_line.find("\"--comment\"");
  int size = 11;

  if (cbpos == std::string::npos) {
    cbpos = exec_line.find("--comment");
    size = 9;
  }

  if (cbpos != std::string::npos) {
    // Check that line doesn't end with comment flag
    if (cbpos + size == exec_line.length()) {
      return std::string();
    }

    // Check we found a complete word
    if (exec_line[cbpos + size] == ' ') {
      // Check we have comment text
      if (cbpos + size + 3 >= exec_line.length()) {
        return std::string();
      }

      // Comment text should always start with quotes: --comment "<comment>"
      if (exec_line[cbpos + size + 1] == '"') {
        size_t cepos = exec_line.find('"', cbpos + size + 2);

        // Comment text should always end with quotes: --comment "<comment>"
        if (cepos != std::string::npos) {
          comment = exec_line.substr(cbpos + size + 1, cepos -
                                     (cbpos + size)).c_str();
          exec_line.erase(cbpos, cepos - cbpos + 1);
        } else {
          return std::string();
        }
      } else {
        return std::string();
      }
    }
  }

  return exec_line;
}

//------------------------------------------------------------------------------
// Given an input string, return the appropriate path identifier.
//------------------------------------------------------------------------------
const char* path_identifier(const char* in, bool escapeand)
{
  static XrdOucString input;
  input = in;

  if ((input.beginswith("fid:")) || (input.beginswith("fxid:")) ||
      (input.beginswith("pid:")) || (input.beginswith("pxid:"))) {
    return in;
  }

  input = abspath(in);

  while (escapeand && input.replace("&", "#AND#")) {}

  return input.c_str();
}

//------------------------------------------------------------------------------
// Check if input matches pattern and extract the file id if possible
//------------------------------------------------------------------------------
bool RegWrapDenominator(XrdOucString& input, const std::string& key)
{
  try {
    RegexUtil reg;
    reg.SetRegex(key);
    reg.SetOrigin(input.c_str());
    reg.initTokenizerMode();
    std::string temp = reg.Match();
    auto pos = temp.find(':');
    temp = std::string(temp.begin() + pos + 1, temp.end());
    input = XrdOucString(temp.c_str());
    return true;
  } catch (std::string& e) {
    return false;
  }
}

//------------------------------------------------------------------------------
// Extract file id specifier if input is in one of the following formats:
// fxid:<hex_id> | fid:<dec_id>
//------------------------------------------------------------------------------
bool Path2FileDenominator(XrdOucString& input)
{
  if (RegWrapDenominator(input, "fxid:[a-fA-F0-9]+$")) {
    std::string temp = std::to_string(strtoull(input.c_str(), 0, 16));
    input = XrdOucString(temp.c_str());
    return true;
  }

  return RegWrapDenominator(input, "fid:[0-9]+$");
}

//------------------------------------------------------------------------------
// Extract file id specifier if input is in one of the following formats:
// fxid:<hex_id> | fid:<dec_id>
//------------------------------------------------------------------------------
bool Path2FileDenominator(XrdOucString& input, unsigned long long& id)
{
  if (RegWrapDenominator(input, "fxid:[a-fA-F0-9]+$")) {
    id = strtoull(input.c_str(), nullptr, 16);
    return true;
  } else if (RegWrapDenominator(input, "fid:[0-9]+$")) {
    id = strtoull(input.c_str(), nullptr, 10);
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Extract container id specifier if input is in one of the following formats:
// cxid:<hex_id> | cid:<dec_id>
//------------------------------------------------------------------------------
bool Path2ContainerDenominator(XrdOucString& input)
{
  if (RegWrapDenominator(input, "cxid:[a-fA-F0-9]+$")) {
    std::string temp = std::to_string(strtoull(input.c_str(), 0, 16));
    input = XrdOucString(temp.c_str());
    return true;
  }

  return RegWrapDenominator(input, "cid:[0-9]+$");
}

//------------------------------------------------------------------------------
// Extract container id specifier if input is in one of the following formats:
// cxid:<hex_id> | cid:<dec_id>
//------------------------------------------------------------------------------
bool Path2ContainerDenominator(XrdOucString& input, unsigned long long& id)
{
  if (RegWrapDenominator(input, "cxid:[a-fA-F0-9]+$")) {
    id = strtoull(input.c_str(), nullptr, 16);
    return true;
  } else if (RegWrapDenominator(input, "cid:[0-9]+$")) {
    id = strtoull(input.c_str(), nullptr, 10);
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Check whether the given command performs an MGM call
//------------------------------------------------------------------------------
bool RequiresMgm(const std::string& name, const std::string& args)
{
  if ((name == "clear") || (name == "console") || (name == "cp") ||
      (name == "exit") || (name == "help") || (name == "json") ||
      (name == "pwd") || (name == "quit") || (name == "role") ||
      (name == "silent") || (name == "timing") || (name == "?") ||
      (name == ".q")) {
    return false;
  }

  return !wants_help(args.c_str());
}

//------------------------------------------------------------------------------
// Check if MGM is online and reachable
//------------------------------------------------------------------------------
bool CheckMgmOnline(const std::string& uri)
{
  if (uri.substr(0,6) == "ipc://") {
    return true;
  }

  uint16_t timeout = 10;
  XrdCl::URL url(uri);

  if (!url.IsValid()) {
    std::cerr << "error: " << uri << " not a valid URL" << std::endl;
    return false;
  }

  XrdCl::FileSystem fs(url);
  XrdCl::XRootDStatus status = fs.Ping(timeout);
  return status.IsOK();
}

//------------------------------------------------------------------------------
// Guess a default 'route' e.g. home directory
//------------------------------------------------------------------------------
std::string DefaultRoute()
{
  std::string default_route = "";

  // add a default 'route' for the command
  if (getenv("EOSHOME")) {
    default_route = getenv("EOSHOME");
  } else {
    char default_home[4096];
    std::string username;

    if (getenv("EOSUSER")) {
      username = getenv("EOSUSER");
    }

    if (getenv("USER")) {
      username = getenv("USER");
    }

    if (username.length()) {
      snprintf(default_home, sizeof(default_home), "/eos/user/%s/%s/",
               username.substr(0, 1).c_str(), username.c_str());
      fprintf(stderr,
              "# pre-configuring default route to %s\n# -use $EOSHOME variable to override\n",
              default_home);
      default_route = default_home;
    }
  }

  return default_route;
}

//------------------------------------------------------------------------------
// Load current filesystems into a map
//------------------------------------------------------------------------------
int filesystems::Load(bool verbose)
{
  std::string cmd = "ls -m -s";
  struct stat buf;
  std::string cachefile = "/tmp/.eos.filesystems.";
  XrdOucString serverflat = serveruri;

  while (serverflat.replace("/", ":")) {}

  cachefile += serverflat.c_str();
  cachefile += std::to_string(geteuid());
  bool use_cache = false;

  if (!::stat(cachefile.c_str(), &buf)) {
    if ((buf.st_mtime + 3600) > time(NULL)) {
      use_cache = true;
    }
  }

  std::string cachefiletmp = cachefile + ".tmp";
  int retc = 0;

  if (use_cache) {
    std::string out;
    rstdout = eos::common::StringConversion::LoadFileIntoString(cachefile.c_str(),
              out);
  } else {
    retc = com_protofs((char*)cmd.c_str());
    std::string in = rstdout.c_str();
    eos::common::StringConversion::SaveStringIntoFile(cachefiletmp.c_str(), in);
    ::rename(cachefiletmp.c_str(), cachefile.c_str());
  }

  if (!retc) {
    std::istringstream f(rstdout.c_str());
    std::string line;

    while (std::getline(f, line)) {
      std::map<std::string, std::string> fs;
      eos::common::StringConversion::GetKeyValueMap(line.c_str(),
          fs, "=", " ");
      std::string hostport = fs["host"] + ":" + fs["port"];
      fs["hostport"] = hostport;
      fsmap[std::stoi(fs["id"])] = fs;

      if (verbose) {
        fprintf(stdout, "[fs] id=%06d %s\n", std::stoi(fs["id"]), hostport.c_str());
      }
    }

    fprintf(stdout, "# loaded %lu filesystems\n", fsmap.size());
    fflush(stdout);
  }

  return retc;
}

//------------------------------------------------------------------------------
// Connect current filesystems via XrdCl objects
//------------------------------------------------------------------------------

int filesystems::Connect()
{
  for (auto it = fsmap.begin(); it != fsmap.end() ; ++it) {
    if (!clientmap.count(it->first)) {
      XrdCl::URL url(std::string("root://") + it->second["id"] + "@" +
                     it->second["hostport"] + "//dummy");
      // make a new connection
      clientmap[it->first] = std::make_shared<XrdCl::FileSystem> (url);
    }
  }

  return 0;
}

//------------------------------------------------------------------------------
// Find files
//------------------------------------------------------------------------------

int files::Find(const char* path, bool verbose)
{
  fprintf(stdout, "# finding files ...\n");
  fflush(stdout);
  std::string cmd = "-f --nrep --fid --fs --checksum --size ";
  cmd += path;
  bool old_silent = silent;
  silent = true;
  int retc = com_find((char*)cmd.c_str());
  silent = old_silent;

  if (!retc) {
    std::istringstream f(rstdout.c_str());
    std::string line;

    while (std::getline(f, line)) {
      std::map<std::string, std::string> f;
      eos::common::StringConversion::GetKeyValueMap(line.c_str(),
          f, "=", " ");
      filemap[f["path"]].size = std::stol(f["size"]);
      filemap[f["path"]].nrep = std::stoi(f["nrep"]);
      filemap[f["path"]].checksum = f["checksum"];
      filemap[f["path"]].hexid = f["fid"];
      std::vector<std::string> tokens;
      eos::common::StringConversion::Tokenize(f["fsid"], tokens, ",");

      for (size_t i = 0 ; i < tokens.size(); ++i) {
        filemap[f["path"]].locations.insert(std::stoi(tokens[i]));
      }

      if (verbose) {
        fprintf(stdout,
                "[file] path=%s hexid=%s checksum=%s nrep=%d size=%lu locations=%lu\n",
                f["path"].c_str(),
                filemap[f["path"]].hexid.c_str(),
                filemap[f["path"]].checksum.c_str(),
                filemap[f["path"]].nrep,
                filemap[f["path"]].size,
                filemap[f["path"]].locations.size());
      }
    }
  }

  return retc;
}

//------------------------------------------------------------------------------
// Lookup locations
//------------------------------------------------------------------------------

int files::Lookup(filesystems& fsmap, bool verbose)
{
  size_t max_queue = 49512;
  size_t stat_timeout = 300;
  size_t n_timeouts = 0;
  size_t n_missing = 0;
  size_t n_other = 0;
  std::list< SyncResponseHandler*> callback_queue;
  std::map<uint64_t, SyncResponseHandler*> callbacks;
  size_t count = 0;

  for (auto it = filemap.begin(); it != filemap.end(); ++it) {
    count++;

    if (!(count % 100)) {
      fprintf(stdout,
              "# progress %.01f %% [ %lu/%lu ] [ unix:%lu ] [ cb:%lu ] [ to:%lu ] [ miss:%lu ] [ oth:%lu ] \n",
              100.0 * count / filemap.size(), count, filemap.size(), time(NULL),
              callbacks.size(), n_timeouts, n_missing, n_other);
      fflush(stdout);
    }

    for (auto loc = it->second.locations.begin(); loc != it->second.locations.end();
         ++loc) {
      if (!fsmap.fs().count(*loc)) {
        fprintf(stderr, "[ERROR] [SHADOWFS] fs=%d path=%s\n", *loc, it->first.c_str());
        continue;
      }

      std::string prefix = fsmap.fs()[*loc]["path"];
      std::string fullpath =
        eos::common::FileId::FidPrefix2FullPath(it->second.hexid.c_str(),
            prefix.c_str());

      if (verbose) {
        fprintf(stdout, "[file] path=%s loc=%d fstpath=%s\n", it->first.c_str(), *loc,
                fullpath.c_str());
      }

      bool cleaning = false;

      while (callbacks.size() > max_queue || (cleaning &&
                                              (callbacks.size() > max_queue / 2))) {
        //  fprintf(stderr,"waiting 1 %lu\n", callbacks.size());
        //  cleaning = true;
        for (auto it = callback_queue.begin(); it != callback_queue.end(); ++it) {
          if ((*it)->HasStatus()) {
            (*it)->WaitForResponse();

            if ((*it)->GetStatus()->IsOK()) {
              StatInfo* statinfo;
              (*it)->GetResponse()->Get(statinfo);

              //        fprintf(stderr,"path=%s size=%ld\n", (*it)->GetPath(), statinfo->GetSize());
              if (statinfo->GetSize() != filemap[(*it)->GetPath()].size) {
                filemap[(*it)->GetPath()].wrongsize_locations.insert((*it)->GetFsid());
              }

              delete statinfo;
            } else {
              if ((*it)->GetStatus()->code == XrdCl::errOperationExpired) {
                filemap[(*it)->GetPath()].expired = true;
                n_timeouts++;
              } else {
                if (((*it)->GetStatus()->code == XrdCl::errErrorResponse) &&
                    ((*it)->GetStatus()->errNo == kXR_NotFound)) {
                  filemap[(*it)->GetPath()].missing_locations.insert((*it)->GetFsid());
                  n_missing++;
                } else {
                  n_other++;
                }
              }
            }

            callbacks.erase((uint64_t)*it);
            delete *it;
            callback_queue.erase(it);
            break;
          } else {
            size_t age = (*it) -> GetAge();

            if (age > (stat_timeout + 60))  {
              fprintf(stderr, "pending request since %lu seconds - path=%s fsid=%d\n", age,
                      (*it)->GetPath(), (*it)->GetFsid());
            }
          }
        }

        //  fprintf(stderr,"waiting 2 %lu\n", callbacks.size());
      }

      SyncResponseHandler* cb = new SyncResponseHandler(it->first.c_str(), *loc);
      callback_queue.push_back(cb);
      callbacks[(uint64_t)cb] = cb;

      if (verbose) {
        fprintf(stdout, "sending to %d %s count=%lu\n", *loc,
                fsmap.fs()[*loc]["hostport"].c_str(),
                fsmap.clients().count(*loc));
      }

      XRootDStatus status = fsmap.clients() [*loc]->Stat(fullpath.c_str(), cb,
                            stat_timeout);

      if (!status.IsOK()) {
        fprintf(stderr, "error: failed to send path=%s to %d : %s\n", cb->GetPath(),
                cb->GetFsid(), status.ToString().c_str());
        callback_queue.pop_back();
        callbacks.erase((uint64_t)cb);
      }
    }
  }

  // wait for call-backs to be returned or time-out
  while (callbacks.size()) {
    for (auto it = callback_queue.begin(); it != callback_queue.end(); ++it) {
      if ((*it)->HasStatus()) {
        (*it)->WaitForResponse();

        if ((*it)->GetStatus()->IsOK()) {
          StatInfo* statinfo;
          (*it)->GetResponse()->Get(statinfo);

          // fprintf(stderr,"response=%llx path=%s size=%ld\n", (*it)->GetResponse(), (*it)->GetPath(), statinfo->GetSize());
          if (statinfo->GetSize() != filemap[(*it)->GetPath()].size) {
            filemap[(*it)->GetPath()].wrongsize_locations.insert((*it)->GetFsid());
          }

          delete statinfo;
        } else {
          if ((*it)->GetStatus()->code == XrdCl::errOperationExpired) {
            fprintf(stderr, "status=%s\n", (*it)->GetStatus()->ToString().c_str());
            filemap[(*it)->GetPath()].expired = true;
          } else {
            if (((*it)->GetStatus()->code == XrdCl::errErrorResponse) &&
                ((*it)->GetStatus()->errNo == kXR_NotFound)) {
              filemap[(*it)->GetPath()].missing_locations.insert((*it)->GetFsid());
              n_missing++;
            } else {
              n_other++;
            }
          }
        }

        if (verbose) {
          fprintf(stderr, "erasing callback %s %d\n", (*it)->GetPath(), (*it)->GetFsid());
        }

        callbacks.erase((uint64_t)*it);
        delete *it;
        callback_queue.erase(it);
        break;
      }

      size_t age = (*it) -> GetAge();

      if (age > (stat_timeout + 60))  {
        fprintf(stderr, "pending request since %lu seconds - path=%s fsid=%d\n", age,
                (*it)->GetPath(), (*it)->GetFsid());
      }
    }
  }

  fprintf(stdout,
          "# progress %.01f %% [ %lu/%lu ] [ unix:%lu ] [ cb:%lu ] [ to:%lu ] [ miss:%lu ] [ oth:%lu ] \n",
          100.0 * count / filemap.size(), count, filemap.size(), time(NULL),
          callbacks.size(), n_timeouts, n_missing, n_other);
  fflush(stdout);
  return 0;
}

//------------------------------------------------------------------------------
// Report files
//------------------------------------------------------------------------------
int files::Report(size_t expect_nrep)
{
  size_t n_missing = 0;
  size_t n_size = 0;
  size_t n_nrep = 0;
  size_t n_expired = 0;
  size_t n_lost = 0;

  for (auto it = filemap.begin(); it != filemap.end(); ++it) {
    if (it->second.expired) {
      fprintf(stderr, "[ERROR] [ EXPIRED ] path=%s nrep=%d \n", it->first.c_str(),
              it->second.nrep);
      n_expired++;
    } else {
      for (auto loc = it->second.missing_locations.begin();
           loc != it->second.missing_locations.end(); ++loc) {
        fprintf(stderr, "[ERROR] [ MISSING ] path=%s nrep=%d loc=%d \n",
                it->first.c_str(), it->second.nrep, *loc);
        n_missing++;
      }

      for (auto loc = it->second.wrongsize_locations.begin();
           loc != it->second.wrongsize_locations.end(); ++loc) {
        fprintf(stderr, "[ERROR] [ SIZE    ] path=%s loc=%d\n", it->first.c_str(),
                *loc);
        n_size++;
      }

      if (expect_nrep) {
        if (it->second.locations.size() != expect_nrep) {
          fprintf(stderr, "[ERROR] [ NREP    ] path=%s nrep=%lu expected=%lu\n",
                  it->first.c_str(), it->second.locations.size(), expect_nrep);
          n_nrep++;
        }
      }
    }

    if (it->second.missing_locations.size() == it->second.locations.size()) {
      fprintf(stderr, "[ERROR] [ LOST    ] path=%s nrep=%lu missing=%lu\n",
              it->first.c_str(), it->second.locations.size(),
              it->second.missing_locations.size());
      n_lost++;
    }
  }

  fprintf(stderr,
          "[SUMMARY] expired:%lu missing:%lu size:%lu nrep:%lu lost:%lu total:%lu\n",
          n_expired, n_missing, n_size, n_nrep, n_lost, filemap.size());
  return 0;
}
