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
#include "ConsoleCompletion.hh"
#include "CommandFramework.hh"
#include "console/RegexUtil.hh"
#include <XrdCl/XrdClDefaultEnv.hh>
#include <XrdCl/XrdClURL.hh>
#include "License"
#include "common/FileId.hh"
#include "common/Path.hh"
#include "common/SymKeys.hh"
#include "common/StringTokenizer.hh"
#include "common/StringConversion.hh"
#include "common/StringUtils.hh"
#include "common/mq/XrdMqTiming.hh"
#include <XrdOuc/XrdOucTokenizer.hh>
#include <XrdOuc/XrdOucEnv.hh>
#include <XrdCl/XrdClFile.hh>
#include <zmq.hpp>
#include <iomanip>
#include <setjmp.h>
#include <readline/readline.h>
#include <readline/history.h>
#include <list>
#include <sys/time.h>
#include <sys/resource.h>
#include <XrdCl/XrdClPostMaster.hh>

#ifdef __APPLE__
#define ENONET 64
#endif


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
XrdOucString app = "";

int global_retc = 0;
bool global_highlighting = true;
bool global_debug = false;
bool interactive = true;
bool hasterminal = true;
bool silent = false;
bool timing = false;
bool json = false;
GlobalOptions gGlobalOpts;
//! When non-zero, this global means the user is done using this program. */
int done;

// Pointer to the result of client_command. It gets invalid when the
// output_result function is called.
XrdOucEnv* CommandEnv = 0;
static sigjmp_buf sigjump_buf;

// Registry initialization helper
static bool registryInitialized = false;

static void EnsureRegistryInitialized()
{
  if (!registryInitialized) {
    RegisterNativeConsoleCommands();
    registryInitialized = true;
  }
}

// Convenience runner using the registered native commands
static int RunRegisteredCommand(const std::string& cmdName,
                                const std::vector<std::string>& argsVec)
{
  EnsureRegistryInitialized();
  IConsoleCommand* icmd = CommandRegistry::instance().find(cmdName);
  if (!icmd) {
    fprintf(stderr, "%s: No such command for EOS Console.\n", cmdName.c_str());
    return -1;
  }

  std::string rest;
  for (size_t i = 0; i < argsVec.size(); ++i) {
    if (i) rest.push_back(' ');
    rest += argsVec[i];
  }

  if (icmd->requiresMgm(rest) &&
      !CheckMgmOnline(serveruri.c_str())) {
    std::cerr << "error: MGM " << serveruri.c_str()
              << " not online/reachable" << std::endl;
    exit(ENONET);
  }

  CommandContext ctx;
  ctx.serverUri = serveruri.c_str();
  ctx.globalOpts = &gGlobalOpts;
  ctx.json = json;
  ctx.silent = silent;
  ctx.interactive = interactive;
  ctx.timing = timing;
  ctx.userRole = user_role.c_str();
  ctx.groupRole = group_role.c_str();
  ctx.clientCommand = &client_command;
  ctx.outputResult = &output_result;

  return icmd->run(argsVec, ctx);
}

//------------------------------------------------------------------------------
// Exit handler
//------------------------------------------------------------------------------
void
exit_handler(int a)
{
  fprintf(stdout, "\n");
  fprintf(stderr, "<Control-C>\n");
  write_history(historyfile.c_str());
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
wants_help(const char* args_line, bool no_h)
{
  std::string allargs = " ";
  allargs += args_line;
  allargs += " ";

  if ((allargs.find("\"--help\"") != std::string::npos) ||
      (allargs.find(" --help ") != std::string::npos) ||
      (allargs.find(" \"--help\" ") != std::string::npos)) {
    const char* ptr;
    std::string token;
    eos::common::StringTokenizer tokenizer(allargs);
    tokenizer.GetLine();

    while ((ptr = tokenizer.GetToken(false))) {
      token = ptr;

      if (token == "--help") {
        return true;
      }
    }
  }

  if (!no_h) {
    if ((allargs.find("\"-h\"") != std::string::npos) ||
        (allargs.find(" -h ") != std::string::npos) ||
        (allargs.find(" \"-h\" ") != std::string::npos)) {
      const char* ptr;
      std::string token;
      eos::common::StringTokenizer tokenizer(allargs);
      tokenizer.GetLine();

      while ((ptr = tokenizer.GetToken(false))) {
        token = ptr;

        if (token == "-h") {
          return true;
        }
      }
    }
  }

  return false;
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
    eos::common::StringConversion::UnSeal(rstdout);
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
  using eos::common::StringConversion;

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
    StringConversion::UnSeal(rstdout);
  }

  if (rstderr.beginswith("base64:")) {
    XrdOucString ub64out;
    eos::common::SymKey::DeBase64(rstderr, ub64out);
    rstderr = ub64out;
  } else {
    StringConversion::UnSeal(rstderr);
  }

  if (rstdjson.beginswith("base64:")) {
    XrdOucString ub64out;
    eos::common::SymKey::DeBase64(rstdjson, ub64out);
    rstdjson = ub64out;
  } else {
    StringConversion::UnSeal(rstdjson);
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
  if (app.length()) {
    in += "&eos.app=";
    in += app;
  }

  if (user_role.length()) {
    in += "&eos.ruid=";
    in += user_role;
  }

  if (group_role.length()) {
    in += "&eos.rgid=";
    in += group_role;
  } else {
    if (getenv("EOS_NEWGRP")) {
      if (getegid()) {
        // add the current effective group ID as a wish to the request, but not root!
        in += "&eos.rgid=";
        in += std::to_string(getegid()).c_str();
      }
    }
  }

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
    zmq::socket_t socket(context, ZMQ_REQ);
    path.erase(0, serveruri.length() + 1);
    socket.connect(serveruri.c_str());
    zmq::message_t request(path.length());
    memcpy(request.data(), path.c_str(), path.length());
    socket.send(request, zmq::send_flags::none);
    std::string sout;
    zmq::message_t response;
    zmq::recv_result_t ret_recv = socket.recv(response);

    if (ret_recv.has_value()) {
      sout.assign((char*)response.data(), response.size());
    }

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
    RunRegisteredCommand("cd", {lpwd});
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
          "Usage: eos [-r|--role <uid> <gid>] [-s] [-a|--app <app>] [-b|--batch] [-v|--version] [-j|--json] [<mgm-url>] [<cmd> {<argN>}|<filename>.eosh]\n");
  fprintf(stderr,
          "            -r, --role <uid> <gid>              : select user role <uid> and group role <gid>\n");
  fprintf(stderr,
          "            -a, --app <application>             : set the application name for the CLI\n");
  fprintf(stderr,
          "            -b, --batch                         : run in batch mode without colour and syntax highlighting\n");
  fprintf(stderr,
          "            -j, --json                          : switch to json output format\n");
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
          "            EOS_NEWGRP                          : requests for each command the group ID of the current shell\n");
  fprintf(stderr,
          "            EOS_PWD_FILE                        : sets the file where the last working directory is stored- by default '$HOME/.eos_pwd\n\n");
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
  // Ugly temporary hack for stopping the XRootD PostMaster environment no matter what happens (https://its.cern.ch/jira/browse/EOS-6282)
  auto stopPostMaster = [&](void*) {
    XrdCl::DefaultEnv::GetPostMaster()->Stop();
  };
  std::unique_ptr<void, decltype(stopPostMaster)> stopPostMasterDeleter((void *)1, stopPostMaster);

  // std::unique_ptr<void, void(*)(void *)> libShutDown(nullptr, [](void *){google::protobuf::ShutdownProtobufLibrary();});
  // std::unique_ptr<void, void(*)(void *)> stopPostMaster(nullptr, [](void *){XrdCl::DefaultEnv::GetPostMaster()->Stop();});
  atexit([]{google::protobuf::ShutdownProtobufLibrary();});
  // atexit([]{XrdCl::DefaultEnv::GetPostMaster()->Stop();});

  char* line, *s;
  serveruri = (char*) "root://localhost";
  // Enable fork handlers for XrdCl
  XrdCl::Env* env = XrdCl::DefaultEnv::GetEnv();
  env->PutInt("RunForkHandler", 1);
  env->PutInt("RequestTimeout", 900);
  env->PutInt("StreamTimeout", 1200);

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
          (in1 != "--role") &&
          (in1 != "--json") &&
          (in1 != "--app") &&
          (in1 != "-h") &&
          (in1 != "-b") &&
          (in1 != "-v") &&
          (in1 != "-s") &&
          (in1 != "-j") &&
          (in1 != "-a") &&
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
      return RunRegisteredCommand("status", {});
    }

    if ((in1 == "--version") || (in1 == "-v")) {
      fprintf(stderr, "EOS %s (2020)\n\n", VERSION);
      fprintf(stderr, "Developed by the CERN IT storage group\n");
      exit(0);
    }

    if ((in1 == "--batch") || (in1 == "-b")) {
      interactive = false;
      global_highlighting = false;
      argindex++;
      in1 = argv[argindex];
    }

    if ((in1 == "--json") || (in1 == "-j")) {
      interactive = false;
      global_highlighting = false;
      json = true;
      argindex++;
      in1 = argv[argindex];
      gGlobalOpts.mJsonFormat = true;
    }

    if ((in1 == "fuse")) {
      interactive = false;
      global_highlighting = false;
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

    if ((in1 == "--app") || (in1 == "-a")) {
      app = argv[argindex + 1];
      in1 = argv[argindex + 2];
      argindex += 2;
      setenv("EOSAPP", app.c_str(), 1);
    }

    app = getenv("EOSAPP");

    if ((in1 == "--batch") || (in1 == "-b")) {
      interactive = false;
      argindex++;
      in1 = argv[argindex];
    }

    if ((in1 == "cp")) {
      interactive = false;
      global_highlighting = false;
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
        std::fstream file_op(in1.c_str(), std::ios::in);

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
            std::stringstream ss;
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

        execute_line((char*) cmdline.c_str());
        exit(global_retc);
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
    fprintf(stderr, "# EOS  Copyright (C) 2011-2025 CERN/Switzerland\n");
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

// bump up the filedescriptor limits
  {
    int fdlimit = 4096;
    struct rlimit newrlimit;
    newrlimit.rlim_cur = fdlimit;
    newrlimit.rlim_max = fdlimit;

    if ((setrlimit(RLIMIT_NOFILE, &newrlimit) != 0) && (!geteuid())) {
      fprintf(stderr, "warning: unable to set fd limit to %d - errno %d\n",
              fdlimit, errno);
    }
  }
  char prompt[4096];

  sprintf(prompt, "%sEOS Console%s [%s%s%s] |> ", textbold.c_str(),
          textunbold.c_str(), textred.c_str(), serveruri.c_str(), textnormal.c_str());

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
  if (interactive) {
    read_pwdfile();
  }

  // Loop reading and executing lines until the user quits.
  for (; done == 0;) {
    char prompt[4096];

    sprintf(prompt, "%sEOS Console%s [%s%s%s] |%s> ", textbold.c_str(),
            textunbold.c_str(), textred.c_str(), serveruri.c_str(), textnormal.c_str(),
            gPwd.c_str());

    signal(SIGINT, jump_handler);

    if (sigsetjmp(sigjump_buf, 1)) {
      signal(SIGINT, jump_handler);
      fprintf(stdout, "\n");
    }

    line = readline(prompt);
    signal(SIGINT, exit_handler);

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
      std::cout << std::flush;
      std::cerr << std::flush;
      fflush(stdout);
      fflush(stderr);
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

  // Initialize registry on first use
  EnsureRegistryInitialized();

  std::string cmdName = *tokens.begin();
  IConsoleCommand* icmd = CommandRegistry::instance().find(cmdName);
  if (!icmd) {
    fprintf(stderr, "%s: No such command for EOS Console.\n", cmdName.c_str());
    global_retc = -1;
    return (-1);
  }

  // Extract arguments vector from full command line
  line_without_comment = line_without_comment.substr(tokens.begin()->size());
  eos::common::trim(line_without_comment);
  std::string rest = line_without_comment;
  // Quote-aware tokenization: preserve spaces within quoted strings, drop quotes
  std::vector<std::string> argsVec;
  {
    bool inD = false, inS = false;
    std::string cur;
    for (size_t i = 0; i < rest.size(); ++i) {
      char c = rest[i];
      if (c == '\\' && i + 1 < rest.size()) {
        char next = rest[i + 1];
        if (next == '"' || next == '\'') {
          // Preserve escaped quotes as literals and avoid toggling state.
          cur.push_back(c);
          cur.push_back(next);
          ++i;
          continue;
        }
      }
      if (c == '"' && !inS) {
        inD = !inD;
        continue;
      }
      if (c == '\'' && !inD) {
        inS = !inS;
        continue;
      }
      if (c == ' ' && !inD && !inS) {
        if (!cur.empty()) {
          argsVec.push_back(cur);
          cur.clear();
        }
        continue;
      }
      cur.push_back(c);
    }
    if (!cur.empty()) {
      argsVec.push_back(cur);
    }
  }

  // Check MGM availability
  if (icmd->requiresMgm(rest) &&
      !CheckMgmOnline(serveruri.c_str())) {
    std::cerr << "error: MGM " << serveruri.c_str()
              << " not online/reachable" << std::endl;
    exit(ENONET);
  }

  CommandContext ctx;
  ctx.serverUri = serveruri.c_str();
  ctx.globalOpts = &gGlobalOpts;
  ctx.json = json;
  ctx.silent = silent;
  ctx.interactive = interactive;
  ctx.timing = timing;
  ctx.userRole = user_role.c_str();
  ctx.groupRole = group_role.c_str();
  ctx.clientCommand = &client_command;
  ctx.outputResult = &output_result;

  return icmd->run(argsVec, ctx);
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
std::string PathIdentifier(const char* in, bool escapeand)
{
  std::string input;

  if (in == nullptr) {
    return input;
  }

  input = in;

  if ((input.find("fid:") == 0) || (input.find("fxid:") == 0) ||
      (input.find("cid:") == 0) || (input.find("cxid:") == 0) ||
      (input.find("pid:") == 0) || (input.find("pxid:") == 0)) {
    return in;
  }

  input = abspath(in);

  if (escapeand) {
    input = eos::common::StringConversion::SealXrdPath(input);
  }

  return input;
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
      (name == ".q") || (name == "daemon") || (name == "scitoken")) {
    return false;
  }

  return !wants_help(args.c_str());
}

//------------------------------------------------------------------------------
// Check if MGM is online and reachable
//------------------------------------------------------------------------------
bool CheckMgmOnline(const std::string& uri)
{
  if (uri.substr(0, 6) == "ipc://") {
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
      // @note route warning is no longer displayed
      // fprintf(stderr,
      //         "# pre-configuring default route to %s\n# -use $EOSHOME variable to override\n",
      //         default_home);
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
    retc = RunRegisteredCommand("fs", {"-m", "-s"});
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
  int retc = RunRegisteredCommand("find",
                                  {"-f", "--nrep", "--fid", "--fs", "--checksum",
                                   "--size", path});
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
