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
#include "CommandFramework.hh"
#include "ConsoleColor.hh"
#include "ConsoleCompletion.hh"
#include "ConsoleHelp.hh"
#include "ConsoleMetadata.hh"
#include "ConsoleOptions.hh"
#include "License"
#include "common/FileId.hh"
#include "common/Path.hh"
#include "common/StringConversion.hh"
#include "common/StringTokenizer.hh"
#include "common/StringUtils.hh"
#include "common/SymKeys.hh"
#include "common/mq/XrdMqTiming.hh"
#include "console/commands/helpers/PathIdentifier.hh"
#include <XrdCl/XrdClDefaultEnv.hh>
#include <XrdCl/XrdClFile.hh>
#include <XrdCl/XrdClPostMaster.hh>
#include <XrdCl/XrdClURL.hh>
#include <XrdOuc/XrdOucEnv.hh>
#include <XrdOuc/XrdOucTokenizer.hh>
#include <algorithm>
#include <arpa/inet.h>
#include <iomanip>
#include <list>
#include <netdb.h>
#include <netinet/in.h>
#include <readline/history.h>
#include <readline/readline.h>
#include <setjmp.h>
#include <string_view>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <zmq.hpp>

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

// Convenience runner using the registered native commands
static int RunRegisteredCommand(const std::string& cmdName,
                                const std::vector<std::string>& argsVec)
{
  EnsureNativeCommandRegistryInitialized();
  IConsoleCommand* icmd = CommandRegistry::instance().find(cmdName);
  if (!icmd) {
    fprintf(stderr, "%s: No such command for EOS Console.\n", cmdName.c_str());
    global_retc = -1;
    return -1;
  }

  if (icmd->requiresMgmArguments(argsVec) && !CheckMgmOnline(serveruri.c_str())) {
    std::cerr << eos::console::Colorize("error:", eos::console::TextStyle::Error,
                                        STDERR_FILENO)
              << " MGM " << serveruri.c_str() << " not online/reachable" << std::endl;
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

  global_retc = 0;
  return icmd->run(argsVec, ctx);
}

//------------------------------------------------------------------------------
// Exit handler
//------------------------------------------------------------------------------
void
exit_handler(int /*sig*/)
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
jump_handler(int /*sig*/)
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

  if (inpath.beginswith("fxid:") || inpath.beginswith("fid:") ||
      inpath.beginswith("cxid:") || inpath.beginswith("cid:") ||
      inpath.beginswith("pxid:") || inpath.beginswith("pid:")) {
    return inpath.c_str();
  }

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
  std::vector<std::string> arguments;
  if (!eos::console::TokenizeArguments(args_line, arguments)) {
    return false;
  }
  for (const auto& argument : arguments) {
    if (argument == "--") {
      break;
    }
    if (argument == "--help" || (!no_h && argument == "-h")) {
      return true;
    }
  }
  return false;
}

//------------------------------------------------------------------------------
// Check if the MGM URI refers to a loopback address.
//------------------------------------------------------------------------------
bool
IsLocalhostUri(const XrdOucString& uri)
{
  XrdCl::URL url(uri.c_str());
  if (!url.IsValid()) {
    return false;
  }

  std::string host = url.GetHostName();
  struct addrinfo hints{}, *res = nullptr;
  hints.ai_family = AF_UNSPEC;

  // Strip IPv6 bracket notation if present
  if (host.size() >= 2 && host.front() == '[' && host.back() == ']') {
    host = host.substr(1, host.size() - 2);
  }

  if (getaddrinfo(host.c_str(), nullptr, &hints, &res) != 0) {
    return false;
  }

  bool loopback = true;

  for (auto* p = res; p && loopback; p = p->ai_next) {
    if (p->ai_family == AF_INET) {
      auto* addr = &((struct sockaddr_in*)p->ai_addr)->sin_addr;
      loopback = (ntohl(addr->s_addr) == INADDR_LOOPBACK);
    } else if (p->ai_family == AF_INET6) {
      auto* addr = &((struct sockaddr_in6*)p->ai_addr)->sin6_addr;
      loopback = IN6_IS_ADDR_LOOPBACK(addr);
    } else {
      loopback = false;
    }
  }

  loopback = loopback && (res != nullptr);

  freeaddrinfo(res);
  return loopback;
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
    fprintf(
        stderr, "%s command env is 0!\n",
        eos::console::Colorize("error:", eos::console::TextStyle::Error, STDERR_FILENO)
            .c_str());
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

  // Server text is kept intact; style only semantic client-owned output.
  (void)highlighting;

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
      fprintf(stderr, "%s (errc=%d) (%s)\n",
              eos::console::Colorize(rstderr.c_str(), eos::console::TextStyle::Error,
                                     STDERR_FILENO)
                  .c_str(),
              retc, strerror(retc));
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
  if (!eos::console::IsSafeCgiMetadata(std::string_view(app.c_str(), app.length())) ||
      !eos::console::IsSafeCgiMetadata(
          std::string_view(user_role.c_str(), user_role.length())) ||
      !eos::console::IsSafeCgiMetadata(
          std::string_view(group_role.c_str(), group_role.length()))) {
    fprintf(
        stderr, "%s app and role values cannot contain '&' or control characters\n",
        eos::console::Colorize("error:", eos::console::TextStyle::Error, STDERR_FILENO)
            .c_str());
    global_retc = EINVAL;
    return nullptr;
  }
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
    in += eos::common::StringConversion::curl_escaped(global_comment.c_str()).c_str();
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
        reply->assign(oss.str());
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

  if (!lpwd.empty()) {
    RunRegisteredCommand("cd", {lpwd});
  }
}

//------------------------------------------------------------------------------
// Usage Information
//------------------------------------------------------------------------------
void
usage()
{
  using namespace eos::console;
  HelpPage page;
  page.blocks = {
      {HelpBlockKind::Paragraph, "EOS storage system command line interface", {}},
      {HelpBlockKind::Section, "USAGE", {}},
      {HelpBlockKind::Usage,
       HelpCommandText("eos").append(
           HelpSyntax(" [global-options] [<mgm-url>] <command> [<arguments>...]")),
       {}},
      {HelpBlockKind::Usage,
       HelpCommandText("eos").append(HelpSyntax(" [global-options] [<mgm-url>]")),
       {}},
      {HelpBlockKind::Usage,
       HelpCommandText("eos").append(
           HelpSyntax(" [global-options] [<mgm-url>] <file>.eosh")),
       {}},
      {HelpBlockKind::Paragraph,
       "Run a command, open an interactive shell, or execute a script containing one "
       "EOS command per line. Global options precede the command; options after its "
       "name belong to that command.",
       {}},
      {HelpBlockKind::Section, "FIND COMMAND HELP", {}},
      {HelpBlockKind::Definition, HelpCommandText("eos help"),
       "List every command with its description."},
      {HelpBlockKind::Definition,
       HelpCommandText("eos help").append(HelpSyntax(" <command> [<operation>...]")),
       "Read help for a command or a nested operation."},
      {HelpBlockKind::Definition,
       HelpCommandText("eos fs ls").append(HelpSyntax(" --help")),
       "Show only the filesystem listing options, without contacting an MGM."},
      {HelpBlockKind::Definition,
       HelpCommandText("eos fs").append(HelpSyntax(" --help-all")),
       "Read the complete filesystem command manual."},
      {HelpBlockKind::Section, "COMMANDS BY TASK", {}}};

  EnsureNativeCommandRegistryInitialized();
  const std::vector<std::pair<std::string, std::vector<std::string>>> groups{
      {"Browse and manage files",
       {"ls", "cd", "pwd", "stat", "find", "newfind", "oldfind", "du", "mkdir", "rmdir",
        "cp", "mv", "ln", "cat", "touch", "rm", "file", "info", "fileinfo"}},
      {"Metadata and access",
       {"attr", "acl", "chmod", "chown", "quota", "whoami", "who", "member", "role",
        "token", "scitoken"}},
      {"Storage and namespace",
       {"fs", "df", "node", "space", "group", "ns", "io", "fsck", "sched", "geosched",
        "route", "convert", "recycle", "archive", "backup", "evict", "register"}},
      {"Monitor and administer",
       {"health", "status", "config", "access", "accounting", "devices", "inspector",
        "monit", "tracker", "report", "rtlog", "debug", "vid", "map", "daemon", "test"}},
      {"Clients and tools", {"fuse", "fusex", "rclone", "squash", "tui", "reconnect"}},
      {"Shell and information",
       {"help", "?", "clear", "motd", "json", "silent", "timing", "version", "license",
        "quit", "exit", ".q"}}};
  std::vector<std::string> listed;
  for (const auto& [label, names] : groups) {
    HelpText commands;
    for (const auto& name : names) {
      if (CommandRegistry::instance().find(name)) {
        if (!commands.text.empty()) {
          commands.append("  ");
        }
        commands.append(HelpCommandText(name));
        listed.push_back(name);
      }
    }
    page.blocks.push_back({HelpBlockKind::Definition, label, std::move(commands)});
  }
  HelpText other;
  for (const auto* command : CommandRegistry::instance().all()) {
    if (std::find(listed.begin(), listed.end(), command->name()) == listed.end()) {
      if (!other.text.empty()) {
        other.append("  ");
      }
      other.append(HelpCommandText(command->name()));
    }
  }
  if (!other.text.empty()) {
    page.blocks.push_back({HelpBlockKind::Definition, "More commands", std::move(other)});
  }

  const std::vector<HelpBlock> details{
      {HelpBlockKind::Section, "CONNECTION AND IDENTITY", {}},
      {HelpBlockKind::Definition, HelpSyntax("<mgm-url>"),
       HelpText("Select an MGM, for example ")
           .append("root://eos.example.org:1094", TextStyle::Path)
           .append(" or ")
           .append("ipc:///path/to/mgm.sock", TextStyle::Path)
           .append(". Uses EOS_MGM_URL when omitted, otherwise root://localhost.")},
      {HelpBlockKind::Definition, HelpSyntax("-r, --role <user> <group>"),
       "Select user and group roles by numeric ID or configured mapping name."},
      {HelpBlockKind::Definition, HelpSyntax("-a, --app <application>"),
       "Set the client application name; overrides EOSAPP."},
      {HelpBlockKind::Definition, HelpSyntax("--comment <text>"),
       "Attach one nonempty audit comment to the command. Quote comments containing "
       "spaces."},
      {HelpBlockKind::Section, "OUTPUT AND EXECUTION", {}},
      {HelpBlockKind::Definition, HelpSyntax("-b, --batch"),
       "Run without interactive terminal formatting."},
      {HelpBlockKind::Definition, HelpSyntax("-j, --json"),
       "Request JSON output from commands that support it; implies noninteractive, plain "
       "output."},
      {HelpBlockKind::Definition,
       HelpSyntax("--color <auto|always|never>", {"auto", "always", "never"}),
       "Control color in human output (default: auto). Batch, JSON and completion output "
       "remain plain."},
      {HelpBlockKind::Definition, HelpSyntax("-s"),
       "Run the status command. Cannot be combined with another command."},
      {HelpBlockKind::Definition, HelpSyntax("-h, --help"),
       "Show this help and exit successfully."},
      {HelpBlockKind::Definition, HelpSyntax("-v, --version"),
       "Print version information."},
      {HelpBlockKind::Definition, HelpSyntax("--"),
       "End global option parsing. Within a command, use its -- delimiter for literal "
       "names beginning with '-'."},
      {HelpBlockKind::Section, "EXAMPLES", {}},
      {HelpBlockKind::Paragraph, "List a directory on a selected MGM:", {}},
      {HelpBlockKind::Example,
       HelpCommandText("eos")
           .append(" ")
           .append("root://myeos", TextStyle::Path)
           .append(" ")
           .append(HelpCommandText("ls"))
           .append(" ")
           .append("/eos/", TextStyle::Path),
       {}},
      {HelpBlockKind::Paragraph, "Inspect a command before running it:", {}},
      {HelpBlockKind::Example,
       HelpCommandText("eos fs config").append(HelpSyntax(" --help")),
       {}},
      {HelpBlockKind::Paragraph, "Request machine-readable namespace statistics:", {}},
      {HelpBlockKind::Example,
       HelpCommandText("eos")
           .append(HelpSyntax(" -j "))
           .append(HelpCommandText("ns stat")),
       {}},
      {HelpBlockKind::Paragraph, "Run an EOS script in batch mode:", {}},
      {HelpBlockKind::Example,
       HelpCommandText("eos")
           .append(HelpSyntax(" -b "))
           .append("commands.eosh", TextStyle::Path),
       {}},
      {HelpBlockKind::Section, "ENVIRONMENT", {}},
      {HelpBlockKind::Definition, StyledHelpText("EOS_MGM_URL", TextStyle::Identifier),
       "Default root:// or ipc:// management endpoint."},
      {HelpBlockKind::Definition, StyledHelpText("EOSAPP", TextStyle::Identifier),
       "Default client application name."},
      {HelpBlockKind::Definition, StyledHelpText("NO_COLOR", TextStyle::Identifier),
       "Disable automatic color when nonempty. --color=always overrides this "
       "preference."},
      {HelpBlockKind::Definition,
       StyledHelpText("EOS_HISTORY_FILE", TextStyle::Identifier),
       "Interactive history file (default: $HOME/.eos_history)."},
      {HelpBlockKind::Definition, StyledHelpText("EOS_PWD_FILE", TextStyle::Identifier),
       "Saved working directory file (default: $HOME/.eos_pwd)."},
      {HelpBlockKind::Definition, StyledHelpText("EOS_NEWGRP", TextStyle::Identifier),
       "Request the current shell's group ID for each command."},
      {HelpBlockKind::Section, "INTERACTIVE SHELL AND EXIT STATUS", {}},
      {HelpBlockKind::Paragraph,
       "With no command, EOS opens an interactive shell. Tab completes commands, options "
       "and paths. "
       "Control-D exits; Control-C clears the current line or interrupts a command. "
       "Scripts use EOS command syntax, without shell expansion, and accept no "
       "positional arguments.",
       {}},
      {HelpBlockKind::Paragraph,
       "Successful commands and help return 0. Failures return the command's error "
       "status. "
       "Report bugs to eos-dev@cern.ch.",
       {}}};
  page.blocks.insert(page.blocks.end(), details.begin(), details.end());
  PrintHelpPage(page);
}

namespace {
int
RunShellCompletion(int argc, char* argv[], int argindex)
{
  std::vector<std::string> words;

  for (int i = argindex; i < argc; ++i) {
    words.emplace_back(argv[i] ? argv[i] : "");
  }

  std::string currentWord;

  if (!words.empty()) {
    currentWord = words.back();
    words.pop_back();
  }

  const auto candidates = eos_shell_completion_candidates(words, currentWord);

  for (const auto& candidate : candidates) {
    fprintf(stdout, "%s\n", candidate.c_str());
  }

  return 0;
}

std::string
PromptDisplayValue(const XrdOucString& input, size_t budget)
{
  std::string value(input.c_str(), input.length());
  for (char& character : value) {
    const auto byte = static_cast<unsigned char>(character);
    if (byte < 0x20 || byte == 0x7f) {
      character = '?';
    }
  }
  if (value.size() <= budget) {
    return value;
  }
  if (budget < 3) {
    return std::string(budget, '.');
  }
  size_t prefix = (budget - 3) / 2;
  size_t suffix = value.size() - (budget - 3 - prefix);
  while (prefix && (static_cast<unsigned char>(value[prefix]) & 0xc0) == 0x80) {
    --prefix;
  }
  while (suffix < value.size() &&
         (static_cast<unsigned char>(value[suffix]) & 0xc0) == 0x80) {
    ++suffix;
  }
  return value.substr(0, prefix) + "..." + value.substr(suffix);
}
} // namespace

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
  serveruri = "root://localhost";
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
  bool selectedrole = false;
  const bool terminalInput = isatty(STDIN_FILENO) && isatty(STDOUT_FILENO);

  if (getenv("EOS_CONSOLE_DEBUG")) {
    global_debug = true;
    gGlobalOpts.mDebug = true;
  }

  if (terminalInput) {
    hasterminal = true;
    interactive = true;
  } else {
    hasterminal = false;
    interactive = false;
  }

  if (argc > 1 && std::string(argv[1]) == "__complete") {
    eos::console::ConfigureColor(eos::console::ColorMode::Never, true, false);
    return RunShellCompletion(argc, argv, 2);
  }

  std::vector<std::string> arguments;
  for (int i = 1; i < argc; ++i) {
    arguments.emplace_back(argv[i] ? argv[i] : "");
  }
  std::string comment, parse_error;
  eos::console::ConsoleOptions options;
  if (!eos::console::ExtractConsoleComment(arguments, comment, parse_error) ||
      !eos::console::ParseConsoleOptions(arguments, options, parse_error)) {
    // A failed global parse has not accepted a color/output policy yet.
    eos::console::ConfigureColor(eos::console::ColorMode::Never, false, false);
    fprintf(
        stderr, "%s %s\n",
        eos::console::Colorize("error:", eos::console::TextStyle::Error, STDERR_FILENO)
            .c_str(),
        parse_error.c_str());
    usage();
    return EINVAL;
  }
  eos::console::ConfigureColor(options.color, options.batch, options.json);
  eos::console::ConfigureHelp(options.batch, options.json);
  if (options.help) {
    usage();
    return 0;
  }
  if (options.version) {
    fprintf(stderr, "EOS %s (2026)\n\n", VERSION);
    fprintf(stderr, "Developed by the CERN IT Storage Group\n");
    return 0;
  }
  if (!options.endpoint.empty()) {
    serveruri = options.endpoint.c_str();
    if (serveruri == "ipc://") {
      serveruri = "ipc:///var/eos/md/.admin_socket:1094";
    }
    gGlobalOpts.mMgmUri = serveruri.c_str();
  }
  if (options.batch || options.json) {
    interactive = false;
  }
  json = options.json;
  gGlobalOpts.mJsonFormat = options.json;
  if (!options.application.empty()) {
    setenv("EOSAPP", options.application.c_str(), 1);
  }
  const char* configured_app = getenv("EOSAPP");
  if (configured_app && !eos::console::IsSafeCgiMetadata(configured_app)) {
    fprintf(
        stderr, "%s EOSAPP cannot contain '&' or control characters\n",
        eos::console::Colorize("error:", eos::console::TextStyle::Error, STDERR_FILENO)
            .c_str());
    return EINVAL;
  }
  app = configured_app ? configured_app : "";
  if (options.status) {
    options.command = {"status"};
  }
  if (!options.role.empty()) {
    const bool was_silent = silent;
    silent = !options.command.empty();
    RunRegisteredCommand("role", options.role);
    silent = was_silent;
    selectedrole = true;
  }
  if (!selectedrole && !getuid() && IsLocalhostUri(serveruri)) {
    const bool was_silent = silent;
    silent = true;
    RunRegisteredCommand("role", {"0", "0"});
    silent = was_silent;
  }
  global_comment = comment.c_str();
  gGlobalOpts.mComment = comment;

  if (!options.command.empty()) {
    const auto& command = options.command.front();
    if (command == "cp" || command == "rclone" || command == "fuse") {
      interactive = false;
    }
    if (command.size() > 5 && command.compare(command.size() - 5, 5, ".eosh") == 0 &&
        access(command.c_str(), R_OK) == 0) {
      if (options.command.size() != 1) {
        fprintf(stderr, "%s EOS scripts do not accept positional arguments\n",
                eos::console::Colorize("error:", eos::console::TextStyle::Error,
                                       STDERR_FILENO)
                    .c_str());
        return EINVAL;
      }
      std::ifstream script(command);
      if (!script) {
        fprintf(stderr, "%s cannot open EOS script '%s'\n",
                eos::console::Colorize("error:", eos::console::TextStyle::Error,
                                       STDERR_FILENO)
                    .c_str(),
                command.c_str());
        return EIO;
      }
      std::string script_line;
      while (std::getline(script, script_line)) {
        if (script_line.find('\0') != std::string::npos) {
          fprintf(stderr, "%s embedded NUL byte in EOS script '%s'\n",
                  eos::console::Colorize("error:", eos::console::TextStyle::Error,
                                         STDERR_FILENO)
                      .c_str(),
                  command.c_str());
          return EINVAL;
        }
        if (script_line.find_first_not_of(" \t\r\n") != std::string::npos) {
          execute_line(script_line.data());
        }
      }
      if (script.bad()) {
        fprintf(stderr, "%s cannot read EOS script '%s'\n",
                eos::console::Colorize("error:", eos::console::TextStyle::Error,
                                       STDERR_FILENO)
                    .c_str(),
                command.c_str());
        return EIO;
      }
      return global_retc;
    }
    const std::vector<std::string> command_arguments(options.command.begin() + 1,
                                                     options.command.end());
    RunRegisteredCommand(command, command_arguments);
    return global_retc;
  }

  /* install a shutdown handler */
  //signal(SIGINT, exit_handler);


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
    execute_line("motd");
    execute_line("version");
  }

// bump up the filedescriptor limits
  {
    int fdlimit = 4096;
    struct rlimit newrlimit;
    newrlimit.rlim_cur = fdlimit;
    newrlimit.rlim_max = fdlimit;

    if ((setrlimit(RLIMIT_NOFILE, &newrlimit) != 0) && (!geteuid())) {
      const int error = errno;
      fprintf(stderr, "%s unable to set fd limit to %d - errno %d\n",
              eos::console::Colorize("warning:", eos::console::TextStyle::Warning,
                                     STDERR_FILENO)
                  .c_str(),
              fdlimit, error);
    }
  }
  // Bind our completer
  static char readline_name[] = "EOS Console";
  static char completer_quote_characters[] = "\"'";
  static char word_break_characters[] = " \t\n\r\v\f\"'";
  static char filename_quote_characters[] = " \t\n\r\v\f\\\"'";
  rl_readline_name = readline_name;
  rl_attempted_completion_function = eos_console_completion;
  rl_completer_quote_characters = completer_quote_characters;
  rl_completer_word_break_characters = word_break_characters;
  rl_filename_quote_characters = filename_quote_characters;
  rl_filename_quoting_function = eos_completion_quote;
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

  rl_initialize();
  // Loop reading and executing lines until the user quits.
  for (; done == 0;) {
    int screenRows = 0, screenColumns = 0;
    rl_get_screen_size(&screenRows, &screenColumns);
    // Readline also has fixed redisplay arrays: bound wrapped prompt lines.
    const size_t promptBudget =
        static_cast<size_t>(std::clamp(screenColumns, 1, 128)) * 32;
    constexpr size_t fixedPromptBytes = sizeof("EOS Console [] |> ") - 1;
    const size_t valueBudget = (promptBudget - fixedPromptBytes) / 2;
    const auto label = eos::console::ColorPrompt(
        "EOS Console", eos::console::TextStyle::Heading, STDOUT_FILENO);
    const auto endpoint =
        eos::console::ColorPrompt(PromptDisplayValue(serveruri, valueBudget),
                                  eos::console::TextStyle::Command, STDOUT_FILENO);
    const auto directory =
        eos::console::ColorPrompt(PromptDisplayValue(gPwd, valueBudget),
                                  eos::console::TextStyle::Argument, STDOUT_FILENO);
    const std::string prompt = label + " [" + endpoint + "] |" + directory + "> ";

    signal(SIGINT, jump_handler);

    if (sigsetjmp(sigjump_buf, 1)) {
      signal(SIGINT, jump_handler);
      fprintf(stdout, "\n");
    }

    line = readline(prompt.c_str());
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
execute_line(const char* line)
{
  std::vector<std::string> arguments;
  std::string comment, error;
  global_comment = "";
  gGlobalOpts.mComment.clear();
  if (!eos::console::TokenizeArguments(line, arguments, &error) ||
      !eos::console::ExtractConsoleComment(arguments, comment, error)) {
    fprintf(
        stderr, "%s %s\n",
        eos::console::Colorize("error:", eos::console::TextStyle::Error, STDERR_FILENO)
            .c_str(),
        error.c_str());
    global_retc = -1;
    return -1;
  }
  if (arguments.empty() || arguments.front().empty()) {
    global_retc = -1;
    return -1;
  }
  global_comment = comment.c_str();
  gGlobalOpts.mComment = comment;
  const auto command = arguments.front();
  arguments.erase(arguments.begin());
  return RunRegisteredCommand(command, arguments);
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
// Extract only complete, in-range identifiers. Literal paths remain unchanged.
//------------------------------------------------------------------------------
bool
Path2FileDenominator(const XrdOucString& input, unsigned long long& id)
{
  const auto parsed = eos::console::ParsePathIdentifier(input.c_str());
  if (parsed.type != eos::console::IdentifierType::File) {
    return false;
  }
  id = parsed.value;
  return true;
}

bool
Path2ContainerDenominator(const XrdOucString& input, unsigned long long& id)
{
  const std::string_view text(input.c_str());
  if (text.compare(0, 4, "cid:") != 0 && text.compare(0, 5, "cxid:") != 0) {
    return false;
  }
  const auto parsed = eos::console::ParsePathIdentifier(input.c_str());
  if (parsed.type != eos::console::IdentifierType::Container) {
    return false;
  }
  id = parsed.value;
  return true;
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
    std::cerr << eos::console::Colorize("error:", eos::console::TextStyle::Error,
                                        STDERR_FILENO)
              << " " << uri << " not a valid URL" << std::endl;
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
        fprintf(stderr, "%s failed to send path=%s to %d : %s\n",
                eos::console::Colorize("error:", eos::console::TextStyle::Error,
                                       STDERR_FILENO)
                    .c_str(),
                cb->GetPath(), cb->GetFsid(), status.ToString().c_str());
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
