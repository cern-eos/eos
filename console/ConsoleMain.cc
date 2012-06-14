// ----------------------------------------------------------------------
// File: ConsoleMain.cc
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

/*----------------------------------------------------------------------------*/
#include "ConsoleMain.hh"
#include "ConsolePipe.hh"
#include "../License"

#include "common/Path.hh"
#include "common/IoPipe.hh"
/*----------------------------------------------------------------------------*/
#include "XrdNet/XrdNetOpts.hh"
#include "XrdNet/XrdNetSocket.hh"
#include "XrdSys/XrdSysLogger.hh"
#include "XrdPosix/XrdPosixXrootd.hh"
XrdPosixXrootd posixsingleton;
/*----------------------------------------------------------------------------*/

// ----------------------------------------------------------------------------
// - Implemented Commands                                                     -
// ----------------------------------------------------------------------------
extern int com_access (char*);
extern int com_attr (char*);
extern int com_cd (char*);
extern int com_chmod (char*);
extern int com_chown (char*);
extern int com_clear (char*);
extern int com_config (char*);
extern int com_console (char*);
extern int com_cp (char*);
extern int com_debug (char*);
extern int com_dropbox (char*);
extern int com_file (char*);
extern int com_fileinfo (char*);
extern int com_find (char*);
extern int com_fs   (char*);
extern int com_fsck   (char*);
extern int com_fuse (char*);
extern int com_group (char*);
extern int com_help (char *);
extern int com_io (char *);
extern int com_json (char *);
extern int com_license (char*);
extern int com_ls (char*);
extern int com_map (char*);
extern int com_mkdir (char*);
extern int com_motd (char*);
extern int com_node (char*);
extern int com_ns (char*);
extern int com_pwd (char*);
extern int com_quit (char *);
extern int com_quota (char*);
extern int com_reconnect (char*);
extern int com_restart (char*);
extern int com_rm (char*);
extern int com_rmdir (char*);
extern int com_role (char*);
extern int com_rtlog (char*);
extern int com_silent (char*);
extern int com_space (char*);
extern int com_stat (char*);
extern int com_test (char*);
extern int com_timing (char*);
extern int com_transfer (char*);
extern int com_verify (char*);
extern int com_version (char*);
extern int com_vid (char*);
extern int com_whoami (char*);
extern int com_who (char*);

// ----------------------------------------------------------------------------
// - Global Variables                                                         -
// ----------------------------------------------------------------------------
XrdOucString serveruri="";
XrdOucString historyfile="";
XrdOucString pwd="/";
XrdOucString rstdout;
XrdOucString rstderr;
XrdOucString rstdjson;
XrdOucString user_role="";
XrdOucString group_role="";
XrdOucString global_comment="";

int  global_retc         = 0;
bool global_highlighting = true;
bool interactive         = true;
bool silent              = false;
bool timing              = false;
bool debug               = false;
bool pipemode            = false;
bool runpipe             = false;
bool ispipe              = false;
bool json                = false;

eos::common::IoPipe iopipe;
int  retcfd = 0;

XrdOucEnv* CommandEnv=0; // this is a pointer to the result of client_admin... or client_user.... = it get's invalid when the output_result function is called

eos::common::ClientAdminManager CommonClientAdminManager;

// for static linking needed
/*int
  XrdOucUtils::makePath(char*, unsigned int) {
  return 0;
  }*/

// ----------------------------------------------------------------------------
// - Exit handler
// ----------------------------------------------------------------------------
void exit_handler (int a) {
  fprintf(stdout,"\n");
  fprintf(stderr,"<Control-C>\n");
  write_history(historyfile.c_str());
  if (ispipe) {
    iopipe.UnLockProducer();
  }
  exit(-1);
}


// ----------------------------------------------------------------------------
// - Absolut Path Conversion Function
// ----------------------------------------------------------------------------
const char* abspath(const char* in) {
  static XrdOucString inpath;
  inpath = in;
  if (inpath.beginswith("/"))
    return inpath.c_str();
  inpath = pwd; inpath += in;
  return inpath.c_str();
}

// ----------------------------------------------------------------------------
// - 'help' filter
// ----------------------------------------------------------------------------
int wants_help(const char* arg1) {
  XrdOucString allargs = " "; allargs += arg1;
  allargs += " ";
  if ( (allargs.find(" help ")!=STR_NPOS) ||
       (allargs.find(" -h ")!=STR_NPOS)   ||
       (allargs.find(" --help ") != STR_NPOS) )
    return 1;
  return 0;
}


// ----------------------------------------------------------------------------
// - Command Mapping Array
// ----------------------------------------------------------------------------
COMMAND commands[] = {
  { (char*)"access",   com_access,   (char*)"Access Interface" },
  { (char*)"attr",     com_attr,     (char*)"Attribute Interface" },
  { (char*)"clear",    com_clear,    (char*)"Clear the terminal" },
  { (char*)"cd",       com_cd,       (char*)"Change directory" },
  { (char*)"chmod",    com_chmod,    (char*)"Mode Interface" },
  { (char*)"chown",    com_chown,    (char*)"Chown Interface" },
  { (char*)"config",   com_config,   (char*)"Configuration System"},
  { (char*)"console",  com_console,  (char*)"Run Error Console"},
  { (char*)"cp",       com_cp,       (char*)"Cp command"},
  { (char*)"debug",    com_debug,    (char*)"Set debug level"},
  { (char*)"dropbox",  com_dropbox,  (char*)"Drop box"},
  { (char*)"exit",     com_quit,     (char*)"Exit from EOS console" },
  { (char*)"file",     com_file,     (char*)"File Handling" },
  { (char*)"fileinfo", com_fileinfo, (char*)"File Information" },
  { (char*)"find",     com_find,     (char*)"Find files/directories" },
  { (char*)"fs",       com_fs,       (char*)"File System configuration"},
  { (char*)"fsck",     com_fsck,      (char*)"File System Consistency Checking"},
  { (char*)"fuse",     com_fuse,     (char*)"Fuse Mounting"},
  { (char*)"group",    com_group,    (char*)"Group configuration" },
  { (char*)"help",     com_help,     (char*)"Display this text" },
  { (char*)"io",       com_io,       (char*)"IO Interface" }, 
  { (char*)"json",     com_json,     (char*)"Toggle JSON output flag for stdout" }, 
  { (char*)"license",  com_license,  (char*)"Display Software License" },
  { (char*)"ls",       com_ls,       (char*)"List a directory" },
  { (char*)"map",      com_map,      (char*)"Path mapping interface" },
  { (char*)"mkdir",    com_mkdir,    (char*)"Create a directory" },
  { (char*)"motd",     com_motd,     (char*)"Message of the day" },
  { (char*)"node",     com_node,     (char*)"Node configuration" },
  { (char*)"ns",       com_ns,       (char*)"Namespace Interface" },
  { (char*)"vid",      com_vid,      (char*)"Virtual ID System Configuration" },
  { (char*)"pwd",      com_pwd,      (char*)"Print working directory" },
  { (char*)"quit",     com_quit,     (char*)"Exit from EOS console" },
  { (char*)"quota",    com_quota,    (char*)"Quota System configuration"},
  { (char*)"reconnect",com_reconnect,(char*)"Forces a re-authentication of the shell"},
  { (char*)"restart",  com_restart,  (char*)"Restart System"},
  { (char*)"rmdir",    com_rmdir,    (char*)"Remove a directory" },
  { (char*)"rm",       com_rm,       (char*)"Remove a file" },
  { (char*)"role",     com_role,     (char*)"Set the client role" },
  { (char*)"rtlog",    com_rtlog,    (char*)"Get realtime log output from mgm & fst servers" },
  { (char*)"silent",   com_silent,   (char*)"Toggle silent flag for stdout" },
  { (char*)"space",    com_space,    (char*)"Space configuration" },
  { (char*)"stat",     com_stat,     (char*)"Run 'stat' on a file or directory" },
  { (char*)"test",     com_test,     (char*)"Run performance test" },
  { (char*)"timing",   com_timing,   (char*)"Toggle timing flag for execution time measurement" },
  { (char*)"transfer" ,com_transfer ,(char*)"Transfer Interface"},
  { (char*)"verify",   com_verify,   (char*)"Verify Interface"},
  { (char*)"version",  com_version,  (char*)"Verbose client/server version"},
  { (char*)"whoami",   com_whoami,   (char*)"Determine how we are mapped on server side" },
  { (char*)"who",      com_who,      (char*)"Statistics about connected users"},
  { (char*)"?",        com_help,     (char*)"Synonym for `help'" },
  { (char*)".q",       com_quit,    (char*)"Exit from EOS console" },
  { (char *)0, (int (*)(char*))0,(char *)0 }
};

// ----------------------------------------------------------------------------
// - Forward Declarations
// ----------------------------------------------------------------------------
char *stripwhite (char *string);
COMMAND *find_command (char *command);
char **EOSConsole_completion (const char *text, int start, int intend);
char *command_generator (const char *text, int state);
char *dir_generator (const char *text, int state);
char *filedir_generator (const char *text, int state);
int valid_argument (char *caller, char *arg);
int execute_line (char *line);

/* The name of this program, as taken from argv[0]. */
char *progname;

/* When non-zero, this global means the user is done using this program. */
int done;

char *
dupstr (char *s){
  char *r;

  r = (char*) malloc (strlen (s) + 1);
  strcpy (r, s);
  return (r);
}


/* Switches stdin,stdout,stderr to pipe mode where we are a persistant communication daemon for a the eospipe command forwarding commands */
bool
startpipe() {
  XrdOucString pipedir="";
  XrdOucString stdinname = "";
  XrdOucString stdoutname = "";
  XrdOucString stderrname = "";
  XrdOucString retcname = "";

  ispipe = true;

  close(STDIN_FILENO);
  close(STDOUT_FILENO);
  close(STDERR_FILENO);

  if (!iopipe.Init()) {
    fprintf(stderr,"error: cannot set IoPipe\n");
    return false;
  }

  XrdSysLogger* logger = new XrdSysLogger();
  XrdSysError eDest(logger);


  int stdinfd  = iopipe.AttachStdin (eDest);
  int stdoutfd = iopipe.AttachStdout(eDest);
  int stderrfd = iopipe.AttachStderr(eDest);
  retcfd       = iopipe.AttachRetc  (eDest);

  if ( (stdinfd <0) || 
       (stdoutfd < 0) ||
       (stderrfd < 0) ||
       (retcfd   < 0) ) {
    fprintf(stderr,"error: cannot attach to pipes\n");
    return false;
  }

  if (!iopipe.LockProducer()) {
    return false;
  }
    
  stdin =  fdopen(stdinfd ,"r");
  stdout = fdopen(stdoutfd,"w");
  stderr = fdopen(stderrfd,"w");

  return true;
}


// ----------------------------------------------------------------------------
// - Interface to Readline Completion
// ----------------------------------------------------------------------------

/* Tell the GNU Readline library how to complete.  We want to try to complete
   on command names if this is the first word in the line, or on filenames
   if not. */
void initialize_readline ()
{
  /* Allow conditional parsing of the ~/.inputrc file. */
  rl_readline_name = (char*) "EOS Console";

  /* Tell the completer that we want a crack first. */
  rl_attempted_completion_function = EOSConsole_completion;

  rl_completion_append_character = '\0';
}

/* Attempt to complete on the contents of TEXT.  START and END bound the
   region of rl_line_buffer that contains the word to complete.  TEXT is
   the word to complete.  We can use the entire contents of rl_line_buffer
   in case we want to do some simple parsing.  Return the array of matches,
   or 0 if there aren't any. */
char **
EOSConsole_completion (const char *text, int start, int intend) {
  char **matches;

  matches = (char **)0;

  /* If this word is at the start of the line, then it is a command
     to complete.  Otherwise it is the name of a file in the current
     directory. */
  if (start == 0) {
    rl_completion_append_character = ' ';
    matches = rl_completion_matches (text, command_generator);
  }

  XrdOucString cmd = rl_line_buffer;
  if ( cmd.beginswith("mkdir ") ||
       cmd.beginswith("rmdir ") ||
       cmd.beginswith("find ") ||
       cmd.beginswith("cd ") ||
       cmd.beginswith("chown ") ||
       cmd.beginswith("chmod ") ||
       cmd.beginswith("attr ") ) {
    // dir completion
    rl_completion_append_character = '\0';
    matches = rl_completion_matches (text, dir_generator);
  }

  if ( cmd.beginswith("rm ") ||
       cmd.beginswith("ls ") ||
       cmd.beginswith("fileinfo ") ) {
    // dir/file completion
    rl_completion_append_character = '\0';
    matches = rl_completion_matches (text, filedir_generator);
  }
  
  return (matches);
}

char *
dir_generator (const char *text, int state) {
  static int list_index, len;

  /* If this is a new word to complete, initialize now.  This includes
     saving the length of TEXT for efficiency, and initializing the index
     variable to 0. */
  if (!state)
    {
      list_index = 0;
      len = strlen (text);
    }

  /* Return the next name which partially matches from the command list. */
  // create a dirlist
  std::vector<std::string> dirs;
  dirs.resize(0);
  bool oldsilent=silent;
  silent = true;
  XrdOucString inarg = text;

  bool absolute = false;
  if (inarg.beginswith("/")) {
    absolute = true;
    // absolute pathnames
    if (inarg.endswith("/")) {
      // that's ok
    } else {
      int rpos = inarg.rfind("/");
      if ( (rpos != STR_NPOS) )
        inarg.erase(rpos+1);
    }
  } else {
    // relative pathnames
    if ( (!inarg.length()) || (!inarg.endswith("/"))) {
      inarg = pwd.c_str();
    } else {
      inarg = pwd.c_str(); 
      inarg += text;
    }
  }
  

  //  while (inarg.replace("//","/")) {};

  XrdOucString comarg = "-F "; 
  comarg += inarg;

  char buffer[4096];
  sprintf(buffer,"%s",comarg.c_str());
  com_ls((char*)buffer);

  silent = oldsilent;

  XrdOucTokenizer subtokenizer((char*)rstdout.c_str());
  do {
    subtokenizer.GetLine();
    XrdOucString entry = subtokenizer.GetToken();
    if (entry.endswith('\n')) 
      entry.erase(entry.length()-1);
    if (!entry.endswith("/"))
      continue;

    if (entry.length()) {
      dirs.push_back(entry.c_str());
    } else {
      break;
    }
  } while (1);


  for (unsigned int i=list_index; i< dirs.size(); i++) {
    list_index++;
    XrdOucString compare="";
    if (absolute) {
      compare = inarg;
      compare += dirs[i].c_str();
    } else {
      compare = dirs[i].c_str();
    }
    //    fprintf(stderr,"%s %s %d\n", compare.c_str(), text, len);
    if (strncmp (compare.c_str(), text, len) == 0) {
      return (dupstr((char*)compare.c_str()));
    }
  }

  /* If no names matched, then return 0. */
  return ((char *)0);
}

char *
filedir_generator (const char *text, int state) {
  static int list_index, len;

  /* If this is a new word to complete, initialize now.  This includes
     saving the length of TEXT for efficiency, and initializing the index
     variable to 0. */
  if (!state)
    {
      list_index = 0;
      len = strlen (text);
    }

  /* Return the next name which partially matches from the command list. */
  // create a dirlist
  std::vector<std::string> dirs;
  dirs.resize(0);
  bool oldsilent=silent;
  silent = true;
  XrdOucString inarg = text;

  bool absolute = false;
  if (inarg.beginswith("/")) {
    absolute = true;
    // absolute pathnames
    if (inarg.endswith("/")) {
      // that's ok
    } else {
      int rpos = inarg.rfind("/");
      if ( (rpos != STR_NPOS) )
        inarg.erase(rpos+1);
    }
  } else {
    // relative pathnames
    if ( (!inarg.length()) || (!inarg.endswith("/"))) {
      if (!inarg.length())
        inarg = "";
      else {
        int rpos = inarg.rfind("/");
        if ( (rpos != STR_NPOS) )
          inarg.erase(rpos+1);
        else 
          inarg ="";
      }
    } else {
      inarg = pwd.c_str(); 
      inarg += text;
    }
  }
  

  //  while (inarg.replace("//","/")) {};

  XrdOucString comarg = "-F "; 
  comarg += inarg;

  char buffer[4096];
  sprintf(buffer,"%s",comarg.c_str());
  com_ls((char*)buffer);

  silent = oldsilent;

  XrdOucTokenizer subtokenizer((char*)rstdout.c_str());
  do {
    subtokenizer.GetLine();
    XrdOucString entry = subtokenizer.GetToken();
    if (entry.endswith('\n')) 
      entry.erase(entry.length()-1);

    if (entry.length()) {
      dirs.push_back(entry.c_str());
    } else {
      break;
    }
  } while (1);


  for (unsigned int i=list_index; i< dirs.size(); i++) {
    list_index++;
    XrdOucString compare="";
    if (absolute) {
      compare = inarg;
      compare += dirs[i].c_str();
    } else {
      compare = inarg;
      compare += dirs[i].c_str();
    }
    //    fprintf(stderr,"%s %s %d\n", compare.c_str(), text, len);
    if (strncmp (compare.c_str(), text, len) == 0) {
      return (dupstr((char*)compare.c_str()));
    }
  }
  
  /* If no names matched, then return 0. */
  return ((char *)0);
}


/* Generator function for command completion.  STATE lets us know whether
   to start from scratch; without any state (i.e. STATE == 0), then we
   start at the top of the list. */
char *
command_generator (const char *text, int state) {
  static int list_index, len;
  char *name;

  /* If this is a new word to complete, initialize now.  This includes
     saving the length of TEXT for efficiency, and initializing the index
     variable to 0. */
  if (!state)
    {
      list_index = 0;
      len = strlen (text);
    }

  /* Return the next name which partially matches from the command list. */
  while ((name = commands[list_index].name))
    {
      list_index++;

      if (strncmp (name, text, len) == 0) {
        XrdOucString withspace = name;
        return (dupstr((char*)withspace.c_str()));
      }
    }

  /* If no names matched, then return 0. */
  return ((char *)0);
}

/* **************************************************************** */
/*                                                                  */
/*                       EOSConsole Commands                        */
/*                                                                  */
/* **************************************************************** */

void 
command_result_stdout_to_vector(std::vector<std::string> &string_vector)
{
  string_vector.clear();
  if (!CommandEnv) {
    fprintf(stderr,"error: command env is 0!\n");
    return;
  }

  rstdout = CommandEnv->Get("mgm.proc.stdout");
  
  if (!rstdout.length())
    return;

  XrdMqMessage::UnSeal(rstdout);

  XrdOucTokenizer subtokenizer((char*)rstdout.c_str());
  const char* nextline=0;
  int i=0;
  while( (nextline = subtokenizer.GetLine())) {
    if ((!strlen(nextline)) || (nextline[0] =='\n'))
      continue;
    string_vector.resize(i+1);
    string_vector.push_back(nextline);
    i++;
  }
}

int
output_result(XrdOucEnv* result, bool highlighting) {
  if (!result)
    return EINVAL;

  rstdout = result->Get("mgm.proc.stdout");
  rstderr = result->Get("mgm.proc.stderr");
  rstdjson = result->Get("mgm.proc.json");

  XrdMqMessage::UnSeal(rstdout);
  XrdMqMessage::UnSeal(rstderr);
  XrdMqMessage::UnSeal(rstdjson);

  if (highlighting && global_highlighting) {
    // color replacements
    rstdout.replace("online","\033[1monline\033[0m");
    rstdout.replace("offline","\033[47;31m\e[5moffline\033[0m");
    rstdout.replace("unknown","\033[47;31m\e[5munknown\033[0m");
    
    rstdout.replace(" ok","\033[49;32m ok\033[0m");
    rstdout.replace("warning","\033[49;33mwarning\033[0m");
    rstdout.replace("exceeded","\033[49;31mexceeded\033[0m");
  }

  int retc = EFAULT;
  if (result->Get("mgm.proc.retc")) {
    retc = atoi(result->Get("mgm.proc.retc"));
  }
  if (json) {
    if (rstdjson.length()) 
      if (!silent)fprintf(stdout,"%s\n",rstdjson.c_str());
  } else {
    if (rstdout.length()) 
      if (!silent)fprintf(stdout,"%s\n",rstdout.c_str());
    if (rstderr.length()) {
      fprintf(stderr,"%s (errc=%d) (%s)\n",rstderr.c_str(), retc, strerror(retc));
    }
  }
  fflush(stdout);
  fflush(stderr);
  CommandEnv=0;
  delete result;
  return retc;
}


XrdOucEnv* 
client_admin_command(XrdOucString &in) {
  if (user_role.length())
    in += "&eos.ruid="; in += user_role;
  if (group_role.length())
    in += "&eos.rgid="; in += group_role;
  if (json) {
    in += "&mgm.format=json";
  }
  if (global_comment.length()) {
    in += "&mgm.comment=";
    in += global_comment;
    global_comment="";
  }
  XrdMqTiming mytiming("eos");
  TIMING("start", &mytiming);
  XrdOucString out="";
  XrdOucString path = serveruri;
  path += "//proc/admin/";
  path += "?";
  path += in;

  if (debug) 
    printf("debug: %s\n", path.c_str());

  XrdClient client(path.c_str());
  if (client.Open(kXR_async,0,0)) {
    off_t offset = 0;
    int nbytes=0;
    char buffer[4096+1];
    while ((nbytes = client.Read(buffer,offset, 4096)) >0) {
      buffer[nbytes]=0;
      out += buffer;
      offset += nbytes;
    }
    client.Close();
    TIMING("stop", &mytiming);
    if (timing) 
      mytiming.Print();

    CommandEnv = new XrdOucEnv(out.c_str());      
    return CommandEnv;
  }
  return 0;
}

XrdOucEnv* 
client_user_command(XrdOucString &in) {
  if (user_role.length())
    in += "&eos.ruid="; in += user_role;
  if (group_role.length())
    in += "&eos.rgid="; in += group_role;
  if (json) {
    in += "&mgm.format=json";
  }
  if (global_comment.length()) {
    in += "&mgm.comment=";
    in += global_comment;
    global_comment="";
  }
  XrdMqTiming mytiming("eos");
  TIMING("start", &mytiming);
  XrdOucString out="";
  XrdOucString path = serveruri;
  path += "//proc/user/";
  path += "?";
  path += in;

  XrdClient client(path.c_str());
  if (client.Open(kXR_async,0,0)) {
    off_t offset = 0;
    int nbytes=0;
    char buffer[4096+1];
    while ((nbytes = client.Read(buffer,offset, 4096)) >0) {
      buffer[nbytes]=0;
      out += buffer;
      offset += nbytes;
    }
    client.Close();
    XrdMqMessage::UnSeal(out);
    TIMING("stop", &mytiming);
    if (timing) 
      mytiming.Print();

    CommandEnv = new XrdOucEnv(out.c_str());
    return CommandEnv;
  }
  return 0;
}

/* Return non-zero if ARG is a valid argument for CALLER, else print
   an error message and return zero. */
int
valid_argument (char *caller, char *arg) {
  if (!arg || !*arg)
    {
      //fprintf (stderr, "%s: Argument required.\n", caller);
      return (0);
    }

  return (1);
}

// ----------------------------------------------------------------------------
// - Colour Definitions
// ----------------------------------------------------------------------------

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

// ----------------------------------------------------------------------------
// - Usage Information
// ----------------------------------------------------------------------------

void usage() {
  fprintf(stderr,"`eos' is the command line interface (CLI) of the EOS storage system.\n");
  fprintf(stderr,"Usage: eos [-r|--role <uid> <gid>] [-b|--batch] [-v|--version] [-p|--pipe] [-j||--json] [<mgm-url>] [<cmd> {<argN>}|<filename>.eosh]\n");
  fprintf(stderr,"            -r, --role <uid> <gid>              : select user role <uid> and group role <gid>\n");
  fprintf(stderr,"            -b, --batch                         : run in batch mode without colour and syntax highlighting and without pipe\n");
  fprintf(stderr,"            -j, --json                          : switch to json output format\n");
  fprintf(stderr,"            -p, --pipe                          : run stdin,stdout,stderr on local pipes and go to background\n");
  fprintf(stderr,"            -h, --help                          : print help text\n");
  fprintf(stderr,"            -v, --version                       : print version information\n");
  fprintf(stderr,"            <mgm-url>                           : xroot URL of the management server e.g. root://<hostname>[:<port>]\n");
  fprintf(stderr,"            <cmd>                               : eos shell command (use 'eos help' to see available commands)\n");
  fprintf(stderr,"            {<argN>}                            : single or list of arguments for the eos shell command <cmd>\n");
  fprintf(stderr,"            <filename>.eosh                     : eos script file name ending with .eosh suffix\n\n");
  fprintf(stderr,"Environment Variables: \n");
  fprintf(stderr,"            EOS_MGM_URL                         : set's the redirector URL\n");
  fprintf(stderr,"            EOS_HISTORY_FILE                    : set's the command history file - by default '$HOME/.eos_history' is used\n\n");
  fprintf(stderr,"            EOS_SOCKS4_HOST                     : set's the SOCKS4 proxy host name\n");
  fprintf(stderr,"            EOS_SOCKS4_PORT                     : set's the SOCKS4 proxy port\n");
  fprintf(stderr,"            EOS_DISABLE_PIPEMODE                : forbids the EOS shell to split into a session and pipe executable to avoid useless re-authentication\n");
  fprintf(stderr,"Return Value: \n");
  fprintf(stderr,"            The return code of the last executed command is returned. 0 is returned in case of success otherwise <errno> (!=0).\n\n");
  fprintf(stderr, "Examples:\n");
  fprintf(stderr,"            eos                                 : start the interactive eos shell client connected to localhost or URL defined in environment variabel EOS_MGM_URL\n");
  fprintf(stderr,"            eos -r 0 0                          : as before but take role root/root [only numeric IDs are supported]\n");
  fprintf(stderr,"            eos root://myeos                    : start the interactive eos shell connecting to MGM host 'myeos'\n");
  fprintf(stderr,"            eos -b whoami                       : run the eos shell command 'whoami' in batch mode without syntax highlighting\n");
  fprintf(stderr,"            eos space ls --io                   : run the eos shell command 'space' with arguments 'ls --io'\n");
  fprintf(stderr,"            eos --version                       : print version information\n");
  fprintf(stderr,"            eos -b eosscript.eosh               : run the eos shell script 'eosscript.eosh'. This script has to contain linewise commands which are understood by the eos interactive shell.\n");
  

  fprintf(stderr,"Report bugs to eos-dev@cern.ch\n");
}

// ----------------------------------------------------------------------------
// - Main Executable
// ----------------------------------------------------------------------------

int main (int argc, char* argv[]) {
  char *line, *s;
  serveruri = (char*)"root://";
  XrdOucString HostName      = XrdSysDNS::getHostName();
  // serveruri += HostName;
  // serveruri += ":1094";
  serveruri += "localhost";


  if (getenv("EOS_MGM_URL")) {
    serveruri = getenv("EOS_MGM_URL");
  }

  if (getenv("EOS_SOCKS4_HOST") && getenv("EOS_SOCKS4_PORT")) {
    EnvPutString( NAME_SOCKS4HOST, getenv("EOS_SOCKS4_HOST"));
    EnvPutString( NAME_SOCKS4PORT, getenv("EOS_SOCKS4_PORT"));
  }

  XrdOucString urole="";
  XrdOucString grole="";
  bool selectedrole= false;
  int argindex=1;

  int retc = system("test -t 0 && test -t 1");

  if (!getenv("EOS_DISABLE_PIPEMODE")) {
    runpipe = true;
  }

  if (!retc) {
    global_highlighting = true;
    interactive = true;
  } else {
    global_highlighting = false;
    interactive = false;
  }

  if (argc>1) {
    XrdOucString in1 = argv[argindex];

    if (in1.beginswith("-")) {
      if ( (in1 != "--help")    &&
           (in1 != "--version") &&
           (in1 != "--batch")   &&
	   (in1 != "--pipe")    &&
           (in1 != "--role")    &&
           (in1 != "--json")    &&
           (in1 != "-h")        &&
           (in1 != "-b")        &&
           (in1 != "-p")        &&
           (in1 != "-v")        &&
           (in1 != "-j")        &&
           (in1 != "-r")) {
        usage();
        exit(-1);
      }
    }
    if ( (in1 == "--help") || (in1 == "-h") ) {
      usage();
      exit(-1);
    }
    
    if ( (in1 == "--version") || (in1 == "-v") ) {
      fprintf(stderr,"EOS %s (CERN)\n\n", VERSION);
      fprintf(stderr,"%s\n", license);
      fprintf(stderr,"Written by CERN-IT-DSS (Andreas-Joachim Peters, Lukasz Janyst & Elvin Sindrilaru)\n");
      exit(-1);
    }
    
    if ( (in1 == "--batch") || (in1 == "-b") ) {
      interactive = false;
      global_highlighting = false;
      runpipe = false;
      argindex++;
      in1 = argv[argindex];
    }
    
    if ( (in1 == "--json") || (in1 == "-j") ) {
      interactive = false;
      global_highlighting = false;
      json = true;
      runpipe = false;
      argindex++;
      in1 = argv[argindex];
    }
    
    if ( (in1 == "cp" ) ) {
      interactive = false;
      global_highlighting = false;
      runpipe = false;
    }

    if ( (in1 == "fuse" ) ) {
      interactive = false;
      global_highlighting = false;
      runpipe = false;
    }

    if ( (in1 == "--pipe") || (in1 == "-p") ) {
      pipemode = true;
      argindex++;
      in1 = argv[argindex];

      if (!startpipe()) {
	fprintf(stderr,"error: unable to start the pipe - maybe there is already a process with 'eos -p' running?\n");
	exit(-1);
      }
    }

    if ( (in1 == "--role") || (in1 == "-r") ) {
      urole = argv[argindex+1];
      grole = argv[argindex+2];
      in1 = argv[argindex+3];
      argindex+=3;
      // execute the role function
      XrdOucString cmdline="role ";
      cmdline += urole; cmdline += " ";
      cmdline += grole;

      in1 = argv[argindex];
      if (in1.length()) {
	silent = true;
      }
      execute_line ((char*)cmdline.c_str());
      if (in1.length()) {
	silent = false;
      }
      selectedrole = true;
    } 

    if ( (in1 == "--batch") || (in1 == "-b") ) {
      interactive = false;
      argindex++;
      in1 = argv[argindex];
    }

    if ( (in1 == "cp" ) ) {
      interactive = false;
    }

    if ( (in1 == "fuse" ) ) {
      interactive = false;
    }

    if (in1.beginswith("root://")) {
      serveruri = argv[argindex];
      argindex++;
      in1 = argv[argindex];
    }

    if (in1.length()) {
      // check if this is a file
      if ((in1.endswith("eosh")) && (!access(in1.c_str(), R_OK))) {
        // this is a script file
        char str[16384];
        fstream file_op(in1.c_str(),ios::in);
        while(!file_op.eof()) {
          file_op.getline(str,16384);
          XrdOucString cmdline="";
          cmdline = str;
          if (!cmdline.length())
            break;
          while (cmdline.beginswith(" ")) {cmdline.erase(0,1);}
          while (cmdline.endswith(" "))   {cmdline.erase(cmdline.length()-1,1);}
          execute_line ((char*)cmdline.c_str());
        }         
        file_op.close();
        exit(0);
      } else {
        XrdOucString cmdline="";
        // this are commands
        for (int i=argindex; i<argc; i++) {
          cmdline += argv[i];
          cmdline += " ";
        }
        if ( (!selectedrole) && (!getuid()) && (serveruri.beginswith("root://localhost"))) {
          // we are root, we always select also the root role by default
          XrdOucString cmdline="role 0 0 ";
          if (!interactive || (runpipe))silent = true;
          execute_line ((char*)cmdline.c_str());
          if (!interactive || (runpipe))silent = false;
        }

        // strip leading and trailing white spaces
        while (cmdline.beginswith(" ")) {cmdline.erase(0,1);}
        while (cmdline.endswith(" ")) {cmdline.erase(cmdline.length()-1,1);}
	
	// here we can use the 'eospipe' mechanism if allowed

	if (runpipe) {
	  cmdline += "\n";
	  // put the eos daemon into batch mode
	  interactive = false;
	  global_highlighting = false;
	  iopipe.Init(); // need to initialize for Checkproducer

	  if (!iopipe.CheckProducer()) {
	    // we need to run a pipe daemon, so we fork here and let the fork run the code like 'eos -p'
	    if (!fork()) {
	      for (int i=1; i< argc; i++) {
		for (size_t j=0; j< strlen(argv[i]); j++) {
		  argv[i][j] = '*';
		}
	      }
	      // detach from the session id
	      pid_t sid;
	      if((sid=setsid()) < 0) {
		fprintf(stderr,"ERROR: failed to create new session (setsid())\n");
		exit(-1);
	      }
	      startpipe();
	      pipemode=true;
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
	  execute_line ((char*)cmdline.c_str());
	  exit(global_retc);
	}
      }
    }
  }
  

  /* by default select the root role if we are root@localhost */

  if ( (!selectedrole) && (!getuid()) && (serveruri.beginswith("root://localhost"))) {
    // we are root, we always select also the root role by default
    XrdOucString cmdline="role 0 0 ";
    if (!interactive)silent = true;
    execute_line ((char*)cmdline.c_str());
    if (!interactive)silent = false;
  }

  /* configure looging */
  eos::common::Logging::Init();
  eos::common::Logging::SetUnit("eos");
  eos::common::Logging::SetLogPriority(LOG_NOTICE);
  

  /* install a shutdown handler */
  signal (SIGINT,  exit_handler);

  if (!interactive) {
    textnormal    = "";
    textblack     = "";
    textred       = "";
    textrederror  = "";
    textblueerror = "";
    textgreen     = "";
    textyellow    = "";
    textblue      = "";
    textbold      = "";
    textunbold    = "";
  } 

  if (interactive) {
    fprintf(stderr,"# ---------------------------------------------------------------------------\n");
    fprintf(stderr,"# EOS  Copyright (C) 2011 CERN/Switzerland\n");
    fprintf(stderr,"# This program comes with ABSOLUTELY NO WARRANTY; for details type `license'.\n");
    fprintf(stderr,"# This is free software, and you are welcome to redistribute it \n");
    fprintf(stderr,"# under certain conditions; type `license' for details.\n");
    fprintf(stderr,"# ---------------------------------------------------------------------------\n");

    execute_line((char*)"motd");
    execute_line((char*)"version");
  }


  char prompt[4096];
  if (pipemode) {
    prompt[0] = 0;
  } else {
    sprintf(prompt,"%sEOS Console%s [%s%s%s] |> ", textbold.c_str(),textunbold.c_str(),textred.c_str(),serveruri.c_str(),textnormal.c_str());
  }

  progname = argv[0];

  initialize_readline ();       /* Bind our completer. */

  if (getenv("EOS_HISTORY_FILE")) {
    historyfile = getenv("EOS_HISTORY_FILE");
  } else {
    if (getenv("HOME")) {
      historyfile = getenv("HOME");
      historyfile += "/.eos_history";
    }
  }
  read_history(historyfile.c_str());
  /* Loop reading and executing lines until the user quits. */
  for ( ; done == 0; )
    {
      char prompt[4096];
      if (pipemode) {
	prompt[0]=0;
      } else {
	sprintf(prompt,"%sEOS Console%s [%s%s%s] |%s> ", textbold.c_str(),textunbold.c_str(),textred.c_str(),serveruri.c_str(),textnormal.c_str(),pwd.c_str());
      }
      if (pipemode) {
	signal (SIGALRM,  exit_handler);
	alarm(60);
      }
      line = readline (prompt);
      if (pipemode)
	alarm(0);

      if (!line)
        break;

      /* Remove leading and trailing whitespace from the line.
         Then, if there is anything left, add it to the history list
         and execute it. */
      s = stripwhite (line);

      if (*s)
        {
          add_history (s);
	  // 20 minutes timeout for commands ... that is long !
	  signal (SIGALRM,  exit_handler);
	  alarm(1200);
          execute_line (s);
	  alarm(0);
	  char newline = '\n';
	  int n = 0;
	  std::cout << std::flush;
	  std::cerr << std::flush;
	  fflush(stdout);
	  fflush(stderr);

	  if (pipemode) {
	    n = write(retcfd,&global_retc,1);
	    n = write(retcfd,&newline,1);
	    if (n!=1) {
	      fprintf(stderr,"error: unable to write retc to retc-socket\n");
	      exit(-1);
	    }
	    // we send the stop sequence to the pipe thread listeners
	    fprintf(stdout,"#__STOP__#\n");
	    fprintf(stderr,"#__STOP__#\n");
	    fflush(stdout);
	    fflush(stderr);
	  }
        }

      free (line);
    }

  write_history(historyfile.c_str());
  exit (0);
}


// ----------------------------------------------------------------------------
// - Command Line Execution Function
// ----------------------------------------------------------------------------

int
execute_line (char *line) {
  register int i;
  COMMAND *command;
  char *word;

  std::string sline = line;
  if (sline.substr(0,9) == "comment:\"") {
    size_t pos = sline.find("\"",10);
    if (pos == std::string::npos) {
      fprintf(stderr,"error: syntax for comment is 'comment \" my comment \" <command> <args> ...'\n");
      return 0;
    } else {
      line += pos+1;
    }
    global_comment = sline.substr(8,pos+1-8).c_str();
  }
      

  /* Isolate the command word. */
  i = 0;
  while (line[i] && (line[i]==' '))
    i++;
  word = line + i;

  while (line[i] && ((line[i]!= ' ')))
    i++;

  if (line[i])
    line[i++] = '\0';

  command = find_command (word);

  if (!command)
    {
      fprintf (stderr, "%s: No such command for EOS Console.\n", word);
      global_retc=-1;
      return (-1);
    }

  /* Get argument to command, if any. */
  while (line[i] == ' ')
    i++;

  word = line + i;

  /* Call the function. */
  return ((*(command->func)) (word));
}

/* Look up NAME as the name of a command, and return a pointer to that
   command.  Return a 0 pointer if NAME isn't a command name. */
COMMAND *
find_command (char *name) {
  register int i;

  for (i = 0; commands[i].name; i++)
    if (strcmp (name, commands[i].name) == 0)
      return (&commands[i]);

  return ((COMMAND *)0);
}

/* Strip whitespace from the start and end of STRING.  Return a pointer
   into STRING. */
char*
stripwhite (char *string) {
  register char *s, *t;

  for (s = string; (*s) == ' '; s++)
    ;

  if (*s == 0)
    return (s);

  t = s + strlen (s) - 1;
  while (t > s && ((*t)== ' '))
    t--;
  *++t = '\0';

  return s;
}

