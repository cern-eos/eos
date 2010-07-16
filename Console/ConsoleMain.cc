/*----------------------------------------------------------------------------*/
#include "XrdMqOfs/XrdMqMessage.hh"
#include "XrdMqOfs/XrdMqTiming.hh"
/*----------------------------------------------------------------------------*/
#include "XrdClient/XrdClient.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucTrace.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucTokenizer.hh"
#include "XrdNet/XrdNetDNS.hh"
#include "XrdOuc/XrdOucUtils.hh"

/*----------------------------------------------------------------------------*/
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>
#include <errno.h>
#include <signal.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <readline/readline.h>
#include <readline/history.h>
#include <string>
#include <iostream>
#include <vector>
#include <fstream>
/*----------------------------------------------------------------------------*/
XrdOucString serveruri="";
XrdOucString historyfile="";
XrdOucString pwd="/";
XrdOucString rstdout;
XrdOucString rstderr;
XrdOucString user_role="";
XrdOucString group_role="";

int global_retc=0;
bool silent=false;
bool timing=false;
bool debug=false;

// for static linking needed
/*int
XrdOucUtils::makePath(char*, unsigned int) {
  return 0;
  }*/

/*----------------------------------------------------------------------------*/

void exit_handler (int a) {
  fprintf(stdout,"\n");
  fprintf(stderr,"<Control-C>\n");
  write_history(historyfile.c_str());
  exit(-1);
}

/* The names of functions that actually do the manipulation. */

int com_help PARAMS((char *));
int com_quit PARAMS((char *));
int com_attr PARAMS((char*));
int com_debug PARAMS((char*));
int com_cd PARAMS((char*));
int com_clear PARAMS((char*));
int com_chmod PARAMS((char*));
int com_config PARAMS((char*));
int com_file PARAMS((char*));
int com_fileinfo PARAMS((char*));
int com_find PARAMS((char*));
int com_fs   PARAMS((char*));
int com_ls PARAMS((char*));
int com_mkdir PARAMS((char*));
int com_ns PARAMS((char*));
int com_role PARAMS((char*));
int com_rmdir PARAMS((char*));
int com_rm PARAMS((char*));
int com_vid PARAMS((char*));
int com_pwd PARAMS((char*));
int com_quota PARAMS((char*));
int com_restart PARAMS((char*));
int com_rtlog PARAMS((char*));
int com_test PARAMS((char*));
int com_silent PARAMS((char*));
int com_timing PARAMS((char*));
int com_whoami PARAMS((char*));

const char* abspath(const char* in) {
  static XrdOucString inpath;
  inpath = in;
  if (inpath.beginswith("/"))
    return inpath.c_str();
  inpath = pwd; inpath += in;
  return inpath.c_str();
}

/* A structure which contains information on the commands this program
   can understand. */

typedef struct {
  char *name;			/* User printable name of the function. */
  rl_icpfunc_t *func;		/* Function to call to do the job. */
  char *doc;			/* Documentation for this function.  */
} COMMAND;

COMMAND commands[] = {
  { (char*)"attr", com_attr, (char*)"Attribute Interface" },
  { (char*)"clear", com_clear, (char*)"Clear the terminal" },
  { (char*)"cd", com_cd, (char*)"Change directory" },
  { (char*)"chmod", com_chmod, (char*)"Mode Interface" },
  { (char*)"config",com_config,(char*)"Configuration System"},
  { (char*)"debug", com_debug,(char*)"Set debug level"},
  { (char*)"exit",  com_quit, (char*)"Exit from EOS console" },
  { (char*)"file", com_file, (char*)"File Handling" },
  { (char*)"fileinfo", com_fileinfo, (char*)"File Information" },
  { (char*)"find",  com_find, (char*)"Find files/directories" },
  { (char*)"fs",    com_fs,   (char*)"File System configuration"},
  { (char*)"help",  com_help, (char*)"Display this text" },
  { (char*)"ls", com_ls, (char*)"List a directory" },
  { (char*)"mkdir", com_mkdir, (char*)"Create a directory" },
  { (char*)"ns", com_ns, (char*)"Namespace Interface" },
  { (char*)"vid", com_vid, (char*) "Virtual ID System Configuration" },
  { (char*)"pwd", com_pwd, (char*)"Print working directory" },
  { (char*)"quit",  com_quit, (char*)"Exit from EOS console" },
  { (char*)"quota", com_quota,(char*)"Quota System configuration"},
  { (char*)"restart",com_restart,(char*)"Restart System"},
  { (char*)"rmdir", com_rmdir, (char*)"Remove a directory" },
  { (char*)"rm", com_rm, (char*)"Remove a file" },
  { (char*)"role", com_role, (char*) "Set the client role" },
  { (char*)"rtlog", com_rtlog, (char*)"Get realtime log output from mgm & fst servers" },
  { (char*)"silent", com_silent, (char*)"Toggle silent flag for stdout" },
  { (char*)"test", com_test, (char*)"Run performance test" },
  { (char*)"timing", com_timing, (char*)"Toggle timing flag for execution time measurement" },
  { (char*)"whoami", com_whoami, (char*)"Determine how we are mapped on server side" },
  { (char*)"?",     com_help, (char*)"Synonym for `help'" },
  { (char*)".q",    com_quit, (char*)"Exit from EOS console" },
  { (char *)0, (rl_icpfunc_t *)0, (char *)0 }
};

/* Forward declarations. */
char *stripwhite (char *string);
COMMAND *find_command (char *command);
char **EOSConsole_completion (const char *text, int start, int intend);
char *command_generator (const char *text, int state);
char *dir_generator (const char *text, int state);
char *filedir_generator (const char *text, int state);
int valid_argument (char *caller, char *arg);
void too_dangerous (char *caller);
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

/* **************************************************************** */
/*                                                                  */
/*                  Interface to Readline Completion                */
/*                                                                  */
/* **************************************************************** */

/* Tell the GNU Readline library how to complete.  We want to try to complete
   on command names if this is the first word in the line, or on filenames
   if not. */
void initialize_readline ()
{
  /* Allow conditional parsing of the ~/.inputrc file. */
  rl_readline_name = "EOS Console";

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
       cmd.beginswith("cd ") ) {
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

int
output_result(XrdOucEnv* result) {
  if (!result)
    return EINVAL;

  rstdout = result->Get("mgm.proc.stdout");
  rstderr = result->Get("mgm.proc.stderr");

  XrdMqMessage::UnSeal(rstdout);
  XrdMqMessage::UnSeal(rstderr);

  // color replacements
  rstdout.replace("online","\033[1monline\033[0m");
  rstdout.replace("offline","\033[47;31m\e[5moffline\033[0m");

  rstdout.replace("OK","\033[49;32mOK\033[0m");
  rstdout.replace("WARNING","\033[49;33mWARNING\033[0m");
  rstdout.replace("EXCEEDED","\033[49;31mEXCEEDED\033[0m");
  int retc = EFAULT;
  if (result->Get("mgm.proc.retc")) {
    retc = atoi(result->Get("mgm.proc.retc"));
  }
  if (rstdout.length()) 
    if (!silent)fprintf(stdout,"%s\n",rstdout.c_str());
  if (rstderr.length()) {
    fprintf(stderr,"%s (errc=%d) (%s)\n",rstderr.c_str(), retc, strerror(retc));
  }
  
  delete result;
  return retc;
}


XrdOucEnv* 
client_admin_command(XrdOucString &in) {
  if (user_role.length())
    in += "&eos.ruid="; in += user_role;
  if (group_role.length())
    in += "&eos.rgid="; in += group_role;

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
      
    return new XrdOucEnv(out.c_str());
  }
  return 0;
}

XrdOucEnv* 
client_user_command(XrdOucString &in) {
  if (user_role.length())
    in += "&eos.ruid="; in += user_role;
  if (group_role.length())
    in += "&eos.rgid="; in += group_role;

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

    return new XrdOucEnv(out.c_str());
  }
  return 0;
}


/* Set the client user and group role */
int
com_role (char *arg) {
  XrdOucTokenizer subtokenizer(arg);
  subtokenizer.GetLine();
  user_role = subtokenizer.GetToken();
  group_role = subtokenizer.GetToken();
  printf("=> selected user role ruid=<%s> and group role rgid=<%s>\n", user_role.c_str(), group_role.c_str());

  if (user_role.beginswith("-"))
    goto com_role_usage;

  return (0);
 com_role_usage:
  printf("usage: role <user-role> [<group-role>]                       : select user role <user-role> [and group role <group-role>]\n");
  
  printf("            <user-role> can be a virtual user ID (unsigned int) or a user mapping alias\n");
  printf("            <group-role> can be a virtual group ID (unsigned int) or a group mapping alias\n");
  return (0);
}


/* Determine the mapping on server side */
int
com_whoami (char *arg) {
  XrdOucString in = "mgm.cmd=whoami"; 
  global_retc = output_result(client_user_command(in));
  return (0);
}


/* Print out help for ARG, or for all of the commands if ARG is
   not present. */
int
com_help (char *arg) {
  register int i;
  int printed = 0;

  for (i = 0; commands[i].name; i++)
    {
      if (!*arg || (strcmp (arg, commands[i].name) == 0))
        {
          printf ("%s\t\t%s.\n", commands[i].name, commands[i].doc);
          printed++;
        }
    }

  if (!printed)
    {
      printf ("No commands match `%s'.  Possibilties are:\n", arg);

      for (i = 0; commands[i].name; i++)
        {
          /* Print in six columns. */
          if (printed == 6)
            {
              printed = 0;
              printf ("\n");
            }

          printf ("%s\t", commands[i].name);
          printed++;
        }

      if (printed)
        printf ("\n");
    }
  return (0);
}

/* Clear the terminal screen */
int
com_clear (char *arg) {
  system("clear");
  return (0);
}

/* Change working directory &*/
int
com_cd (char *arg) {
  XrdOucString newpath=abspath(arg);
  XrdOucString oldpwd = pwd;

  pwd = newpath;

  if (!pwd.endswith("/")) 
    pwd += "/";

  // filter "/./";
  while (pwd.replace("/./","/")) {}
  // filter "..";
  int dppos=0;
  while ( (dppos=pwd.find("/../")) != STR_NPOS) {
    if (dppos==0) {
      pwd = oldpwd;
      break;
    }
    int rpos = pwd.rfind("/",dppos-1);
    //    printf("%s %d %d\n", pwd.c_str(), dppos, rpos);
    if (rpos != STR_NPOS) {
      //      printf("erasing %d %d", rpos, dppos-rpos+3);
      pwd.erase(rpos, dppos-rpos+3);
    } else {
      pwd = oldpwd;
      break;
    }
  }

  if (!pwd.endswith("/")) 
    pwd += "/";

  return (0);
}

/* Print working directory &*/
int
com_pwd (char *arg) {
  fprintf(stdout,"%s\n",pwd.c_str());
  return (0);
}


/* The user wishes to quit using this program.  Just set DONE non-zero. */
int
com_quit (char *arg) {
  done = 1;
  return (0);
}


/* Attribute ls, get, set rm */
int 
com_attr (char* arg1) {
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString subcommand = subtokenizer.GetToken();
  XrdOucString option="";
  XrdOucString in = "mgm.cmd=attr";
  XrdOucString arg = "";

  if (subcommand.beginswith("-")) {
    option = subcommand;
    option.erase(0,1);
    subcommand = subtokenizer.GetToken();
    arg = subtokenizer.GetToken();
    in += "&mgm.option=";
    in += option;
  } else {
    arg = subtokenizer.GetToken();
  }
    
  if ((!subcommand.length()) || (!arg.length()) ||
      ( (subcommand != "ls") && (subcommand != "set") && (subcommand != "get") && (subcommand != "rm") ))
    goto com_attr_usage;

  if ( subcommand == "ls") {
    XrdOucString path  = arg;
    if (!path.length())
      goto com_attr_usage;
    in += "&mgm.subcmd=ls";
    in += "&mgm.path=";in += path;
  }

  if ( subcommand == "set") {
    XrdOucString key   = arg;
    XrdOucString value = subtokenizer.GetToken();
    
    if (value.beginswith("\"")) {
      if (!value.endswith("\"")) {
	do {
	  XrdOucString morevalue = subtokenizer.GetToken();
	  
	  if (morevalue.endswith("\"")) {
	    value += " ";
	    value += morevalue;
	    break;
	  }
	  if (!morevalue.length()) {
	    goto com_attr_usage;
	  }
	  value += " ";
	  value += morevalue;
	} while (1);
      }
    }

    XrdOucString path  = subtokenizer.GetToken();
    if (!key.length() || !value.length() || !path.length()) 
      goto com_attr_usage;
    in += "&mgm.subcmd=set&mgm.attr.key="; in += key;
    in += "&mgm.attr.value="; in += value;
    in += "&mgm.path="; in += path;
  }

  if ( subcommand == "get") {
    XrdOucString key   = arg;
    XrdOucString path  = subtokenizer.GetToken();
    if (!key.length() || !path.length())
      goto com_attr_usage;
    in += "&mgm.subcmd=get&mgm.attr.key="; in += key;
    in += "&mgm.path="; in += path;
  }

  if ( subcommand == "rm") {
    XrdOucString key   = arg;
    XrdOucString path  = subtokenizer.GetToken();
    if (!key.length() || !path.length())
      goto com_attr_usage;
    in += "&mgm.subcmd=rm&mgm.attr.key="; in += key;
    in += "&mgm.path="; in += path;
  }
 
  global_retc = output_result(client_user_command(in));
  return (0); 

 com_attr_usage:
  printf("usage: attr [-r] ls <path>                                  : list attributes of path (-r recursive)\n");  
  printf("usage: attr [-r] set <key> <value> <path>                   : set attributes of path (-r recursive)\n");
  printf("usage: attr [-r] get <key> <path>                           : get attributes of path (-r recursive)\n");
  printf("usage: attr [-r] rm  <key> <path>                           : delete attributes of path (-r recursive)\n\n");
  printf("Help:    If <key> starts with 'sys.' you have to be member of the sudoer to see this attributes or modify\n");

  printf("         Administrator Variables:\n");
  printf("         -----------------------\n");
  printf("         attr: sys.forced.space=<space>         = enforces to use <space>    [configuration dependend]\n");
  printf("         attr: sys.forced.layout=<layout>       = enforces to use <layout>   [<layout>=(plain,replica,raid5)\n");
  printf("         attr: sys.forced.checksum=<checksum>   = enforces to use <checksum> [<checksum>=(adler,crc32,md5,sha)\n");
  printf("         attr: sys.forced.nstripes=<n>          = enforces to use <n> stripes[<n>= 1..16]\n");
  printf("         attr: sys.forced.stripewidth=<w>       = enforces to use a stripe width of <w> kb\n");
  printf("         attr: sys.forced.nouserlayout=1        = disables the user settings with user.forced.<xxx>\n");
  printf("         attr: sys.forced.nofsselection=1       = disables user defined filesystem selection with environment variables for reads\n");
  printf("         attr: sys.stall.unavailable=<sec>      = stall clients for <sec> seconds if a needed file system is unavailable\n");
  printf("         User Variables:\n");
  printf("         -----------------------\n");
  printf("         attr: user.forced.space=<space>        = s.a.\n");
  printf("         attr: user.forced.layout=<layout>      = s.a.\n");
  printf("         attr: user.forced.checksum=<checksum>  = s.a.\n");
  printf("         attr: user.forced.nstripes=<n>         = s.a.\n");
  printf("         attr: user.forced.stripewidth=<n>      = s.a.\n");
  printf("         attr: user.forced.nouserlayout=1       = s.a.\n");
  printf("         attr: user.forced.nofsselection=1      = s.a.\n");
  printf("         attr: user.stall.unavailable=<sec>     = s.a.\n");
  printf("         attr: user.tag=<tag>                   = - tag to group files for scheduling and flat file distribution\n");
  printf("                                                  - use this tag to define datasets (if <tag> contains space use tag with quotes)\n");
  return (0);
}

/* Mode Interface */
int 
com_chmod (char* arg1) {
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString mode = subtokenizer.GetToken();
  XrdOucString option="";
  XrdOucString in = "mgm.cmd=chmod";
  XrdOucString arg = "";

  if (mode.beginswith("-")) {
    option = mode;
    option.erase(0,1);
    mode = subtokenizer.GetToken();
    in += "&mgm.option=";
    in += option;
  }

  XrdOucString path = subtokenizer.GetToken();
  if ( !path.length() || !mode.length() ) 
    goto com_chmod_usage;
  in += "&mgm.path="; in += path;
  in += "&mgm.chmod.mode="; in += mode;

  global_retc = output_result(client_user_command(in));
  return (0);

 com_chmod_usage:
  printf("usage: chmod [-r] <mode> <path>                             : set mode for <path> (-r recursive)\n");  
  printf("                 <mode> can only numerical like 755, 644, 700\n");
  printf("                 <mode> to switch on attribute inheritance use 2755, 2644, 2700 ...\n");
  return (0);
}


/* Filesystem listing, configuration, manipulation */
int
com_fs (char* arg1) {
  // split subcommands
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString subcommand = subtokenizer.GetToken();
  if ( subcommand == "ls" ) {
    XrdOucString in ="mgm.cmd=fs&mgm.subcmd=ls";
    global_retc = output_result(client_admin_command(in));
    return (0);
  }
  if ( subcommand == "set" ) {
    XrdOucString fsname = subtokenizer.GetToken();
    XrdOucString fsid   = subtokenizer.GetToken();
    if (fsname.length() && fsid.length()) {
      XrdOucString in = "mgm.cmd=fs&mgm.subcmd=set&mgm.fsid=";
      in += fsid;
      in += "&mgm.fsname=";
      in += fsname;
      XrdOucString arg = subtokenizer.GetToken();
      
      do {
	if (arg == "-sched") {
	  XrdOucString sched = subtokenizer.GetToken();
	  if (!sched.length()) 
	    goto com_fs_usage;
	  
	  in += "&mgm.fsschedgroup=";
	  in += sched;
	  arg = subtokenizer.GetToken();
	} else {
	  if (arg == "-force") {
	    in += "mgm.fsforce=1";
	  }
	  arg = subtokenizer.GetToken();
	} 
      } while (arg.length());

      global_retc = output_result(client_admin_command(in));
      // boot by fsid
      return (0);
    }
  }

  if ( subcommand == "rm" ) {
    XrdOucString arg = subtokenizer.GetToken();
    XrdOucString in = "mgm.cmd=fs&mgm.subcmd=rm";
    int fsid = atoi(arg.c_str());
    char r1fsid[128]; sprintf(r1fsid,"%d", fsid);
    char r2fsid[128]; sprintf(r2fsid,"%04d", fsid);
    if ( (arg == r1fsid) || (arg == r2fsid) ) {
      // boot by fsid
      in += "&mgm.fsid=";
    } else {
      if (arg.endswith("/fst"))
	in += "&mgm.nodename=";
      else 
	in += "&mgm.fsname=";
    }

    in += arg;
    global_retc = output_result(client_admin_command(in));
    return (0);
    return (0);
  }

  if ( subcommand == "boot" ) {
    XrdOucString arg = subtokenizer.GetToken();
    XrdOucString in = "mgm.cmd=fs&mgm.subcmd=boot";
    if (!arg.length()) 
      goto com_fs_usage;
    int fsid = atoi(arg.c_str());
    char r1fsid[128]; sprintf(r1fsid,"%d", fsid);
    char r2fsid[128]; sprintf(r2fsid,"%04d", fsid);
    if ( (arg == r1fsid) || (arg == r2fsid) ) {
      // boot by fsid
      in += "&mgm.fsid=";
    } else {
      in += "&mgm.nodename=";
    }

    in += arg;
    global_retc = output_result(client_admin_command(in));
    return (0);
  }

  if ( subcommand == "config" ) {
    XrdOucString arg;
    arg = subtokenizer.GetToken();
    XrdOucString sched;
    sched ="";
    if (!arg.length())
      goto com_fs_usage;

    XrdOucString in = "mgm.cmd=fs&mgm.subcmd=config";
    int fsid = atoi(arg.c_str());
    char r1fsid[128]; sprintf(r1fsid,"%d", fsid);
    char r2fsid[128]; sprintf(r2fsid,"%04d", fsid);
    if ( (arg == r1fsid) || (arg == r2fsid) ) {
      // config by fsid
      in += "&mgm.fsid=";
    } else {
      if (arg.endswith("/fst"))
	in += "&mgm.nodename=";
      else 
	in += "&mgm.fsname=";
    }
    
    in += arg;

    arg = subtokenizer.GetToken();

    if (arg == "-sched") {
      sched = subtokenizer.GetToken();
      arg                = subtokenizer.GetToken();
      if (!sched.length() || !arg.length()) 
	goto com_fs_usage;
    }
    
    sched = subtokenizer.GetToken();
    if (sched == "-sched") {
      sched = subtokenizer.GetToken();
      if (!sched.length())
	goto com_fs_usage;
    }
    
    if (sched.length()) {
      in += "&mgm.fsschedgroup=";
      in += sched;
    }

    if (!arg.length())     
      goto com_fs_usage;

    in += "&mgm.fsconfig=";
    in += arg;
    global_retc = output_result(client_admin_command(in));
    return (0);
  }


  if ( subcommand == "clone" ) {
    XrdOucString sourceid;
    sourceid = subtokenizer.GetToken();    
    XrdOucString targetid;
    targetid = subtokenizer.GetToken();
    if (!sourceid.length() || !targetid.length()) 
      goto com_fs_usage;

    XrdOucString in = "mgm.cmd=fs&mgm.subcmd=clone";

    in += "&mgm.fsidsource="; in += sourceid;
    in += "&mgm.fsidtarget="; in += targetid;

    global_retc = output_result(client_admin_command(in));
    return (0);
  }

  if ( subcommand == "compare" ) {
    XrdOucString sourceid;
    sourceid = subtokenizer.GetToken();    
    XrdOucString targetid;
    targetid = subtokenizer.GetToken();
    if (!sourceid.length() || !targetid.length()) 
      goto com_fs_usage;

    XrdOucString in = "mgm.cmd=fs&mgm.subcmd=compare";

    in += "&mgm.fsidsource="; in += sourceid;
    in += "&mgm.fsidtarget="; in += targetid;

    global_retc = output_result(client_admin_command(in));
    return (0);
  }

  if ( subcommand == "dropfiles" ) {
    XrdOucString id;
    id = subtokenizer.GetToken();    

    if (!id.length()) 
      goto com_fs_usage;

    XrdOucString in = "mgm.cmd=fs&mgm.subcmd=dropfiles";

    in += "&mgm.fside="; in += id;

    global_retc = output_result(client_admin_command(in));
    return (0);
  }
   
  if ( subcommand == "dissolve" ) {
    XrdOucString sourceid;
    sourceid = subtokenizer.GetToken();    
    XrdOucString targetid;
    targetid = subtokenizer.GetToken();
    if (!sourceid.length() || !targetid.length()) 
      goto com_fs_usage;

    XrdOucString in = "mgm.cmd=fs&mgm.subcmd=dissolve";

    in += "&mgm.fsidsource="; in += sourceid;
    in += "&mgm.fsidtarget="; in += targetid;

    global_retc = output_result(client_admin_command(in));
    return (0);
  } 


  if ( subcommand == "flatten" ) {
    XrdOucString sourceid;
    sourceid = subtokenizer.GetToken();    
    XrdOucString tag;
    tag = subtokenizer.GetToken();
    if (!sourceid.length())
      goto com_fs_usage;

    XrdOucString in = "mgm.cmd=fs&mgm.subcmd=flatten";

    in += "&mgm.fsidsource="; in += sourceid;
    in += "&mgm.fstag"; in += tag;

    global_retc = output_result(client_admin_command(in));
    return (0);
  } 

  com_fs_usage:

  printf("usage: fs ls                                                    : list configured filesystems (or by name or id match\n");
  printf("       fs set   <fs-name> <fs-id> [-sched <group> ] [-force]    : configure filesystem with name and id\n");
  printf("       fs rm    <fs-name>|<fs-id>                               : remove filesystem configuration by name or id\n");
  printf("       fs boot  <fs-id>|<node-queue>                            : boot filesystem/node ['fs boot *' to boot all]  \n");
  printf("       fs config <fs-id>|<node-queue> <status> [-sched <group>] : set filesystem configuration status\n");
  printf("                    <status> can be := rw                       : filesystem is in read write mode\n");
  printf("                                    := wo                       : filesystem is in write-once mode\n");
  printf("                                    := ro                       : filesystem is in read-only mode\n");
  printf("                                    := drain                    : filesystem is in drain mode\n");
  printf("                                    := off                      : filesystem is disabled\n"); 
  printf("                    -sched <group>                              : allows to change the scheduling group\n");
  printf("       fs clone <fs-id-src> <fs-id-dst>                         : allows to clone the contents of <fs-id-src> to <fs-id-dst>\n");
  printf("       fs compare <fs-id-src> <fs-id-dst>|<space>               : does a comparison of <fs-id-src> with <fs-id-dst>|<space>\n");
  printf("       fs dropfiles  <fs-id>                                    : allows to drop all files on <fs-id>\n");
  printf("       fs dissolve   <fs-id> <space>                            : allows to create a new replica of all files on <fs-id> in <space>\n");
  printf("       fs flatten    <space> [<tag>]                            : allows to flatten the file distribution in <space> for files with tag <tag>\n");
  return (0);
}

/* Quota System listing, configuration, manipulation */
int
com_quota (char* arg1) {
  // split subcommands
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString subcommand = subtokenizer.GetToken();
  XrdOucString arg = subtokenizer.GetToken();
  
  if ( subcommand == "ls" ) {
    XrdOucString in ="mgm.cmd=quota&mgm.subcmd=ls";
    if (arg.length())
      do {
	if (arg == "-uid") {
	  XrdOucString uid = subtokenizer.GetToken();
	  if (!uid.length()) 
	    goto com_quota_usage;
	  in += "&mgm.quota.uid=";
	  in += uid;
	  arg = subtokenizer.GetToken();
	} else 
	  if (arg == "-gid") {
	    XrdOucString gid = subtokenizer.GetToken();
	    if (!gid.length()) 
	      goto com_quota_usage;
	    in += "&mgm.quota.gid=";
	    in += gid;
	    arg = subtokenizer.GetToken();
	  } else 
	    
	    if (arg.c_str()) {
	      in += "&mgm.quota.space=";
	      in += arg;
	    } else 
	      goto com_quota_usage;
      } while (arg.length());
    
    
    global_retc = output_result(client_admin_command(in));
    return (0);
  }
  
  if ( subcommand == "set" ) {
    XrdOucString in ="mgm.cmd=quota&mgm.subcmd=set";
    XrdOucString space ="default";
    do {
      if (arg == "-uid") {
	XrdOucString uid = subtokenizer.GetToken();
	if (!uid.length()) 
	  goto com_quota_usage;
	in += "&mgm.quota.uid=";
	in += uid;
	arg = subtokenizer.GetToken();
      } else
	if (arg == "-gid") {
	  XrdOucString gid = subtokenizer.GetToken();
	  if (!gid.length()) 
	    goto com_quota_usage;
	  in += "&mgm.quota.gid=";
	  in += gid;
	  arg = subtokenizer.GetToken();
	} else
	  if (arg == "-space") {
	     space = subtokenizer.GetToken();
	     if (!space.length()) 
	       goto com_quota_usage;
	     
	     in += "&mgm.quota.space=";
	     in += space;
	     arg = subtokenizer.GetToken();
	   } else
	     if (arg == "-size") {
	       XrdOucString bytes = subtokenizer.GetToken();
	       if (!bytes.length()) 
		 goto com_quota_usage;
	       in += "&mgm.quota.maxbytes=";
	       in += bytes;
	       arg = subtokenizer.GetToken();
	     } else
	       if (arg == "-inodes") {
		 XrdOucString inodes = subtokenizer.GetToken();
		 if (!inodes.length()) 
		   goto com_quota_usage;
		 in += "&mgm.quota.maxinodes=";
		 in += inodes;
		 arg = subtokenizer.GetToken();
	       } else 
		 goto com_quota_usage;
     } while (arg.length());

     global_retc = output_result(client_admin_command(in));
     return (0);
   }

  if ( subcommand == "rm" ) {
    XrdOucString in ="mgm.cmd=quota&mgm.subcmd=rm";
    do {
      if (arg == "-uid") {
	XrdOucString uid = subtokenizer.GetToken();
	if (!uid.length()) 
	  goto com_quota_usage;
	in += "&mgm.quota.uid=";
	in += uid;
	arg = subtokenizer.GetToken();
      } else 
	if (arg == "-gid") {
	  XrdOucString gid = subtokenizer.GetToken();
	  if (!gid.length()) 
	    goto com_quota_usage;
	  in += "&mgm.quota.gid=";
	  in += gid;
	  arg = subtokenizer.GetToken();
	} else 
	  
	  if (arg.c_str()) {
	    in += "&mgm.quota.space=";
	    in += arg;
	  } else 
	    goto com_quota_usage;
    } while (arg.length());
    
    
    global_retc = output_result(client_admin_command(in));
    return (0);
  }
  
   com_quota_usage:
  printf("usage: quota ls [-uid <uid>] [ -gid <gid> ] [-space {<space>}                                          : list configured quota and used space\n");
  printf("usage: quota set [-uid <uid>] [ -gid <gid> ] -space {<space>} [-size <bytes>] [ -inodes <inodes>]      : set volume and/or inode quota by uid or gid \n");
  printf("usage: quota rm [-uid <uid>] [ -gid <gid> ] -space {<space>}                                           : remove configured quota for uid/gid in space\n");
  printf("                                                  -uid <uid>       : print information only for uid <uid>\n");
  printf("                                                  -gid <gid>       : print information only for gid <gid>\n");
  printf("                                                  -space {<space>} : print information only for space <space>\n");
  printf("                                                  -size <bytes>    : set the space quota to <bytes>\n");
  printf("                                                  -inodes <inodes> : limit the inodes quota to <inodes>\n");
  printf("     => you have to specify either the user or the group id\n");
  printf("     => the space argument is by default assumed as 'default'\n");
  printf("     => you have to sepecify at least a size or an inode limit to set quota\n");

  return (0);
}

/* VID System listing, configuration, manipulation */
int
com_vid (char* arg1) {
  // split subcommands
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString subcommand = subtokenizer.GetToken();
  
  if ( subcommand == "ls" ) {
    XrdOucString in ="mgm.cmd=vid&mgm.subcmd=ls";
    XrdOucString soption="";
    XrdOucString option="";
    do {
      option = subtokenizer.GetToken();
      if (option.beginswith("-")) {
	option.erase(0,1);
	soption += option;
      }
    } while (option.length());
    
    if (soption.length()) {
      in += "&mgm.vid.option="; in += soption;
    }
    global_retc = output_result(client_admin_command(in));
    return (0);
  }
  
  if ( subcommand == "set" ) {
    XrdOucString in    = "mgm.cmd=vid&mgm.subcmd=set";
    XrdOucString key   = subtokenizer.GetToken();
    if (!key.length()) 
      goto com_vid_usage;

    XrdOucString vidkey="";
    if (key == "membership") {

      XrdOucString uid  = subtokenizer.GetToken();

      if (!uid.length()) 
	goto com_vid_usage;

      vidkey += uid;

      XrdOucString type = subtokenizer.GetToken();

      if (!type.length())
	goto com_vid_usage;

      in += "&mgm.vid.cmd=membership";
      in += "&mgm.vid.source.uid="; in += uid;

      XrdOucString list = "";
      if ( (type == "-uids") ) {
	vidkey += ":uids";
	list = subtokenizer.GetToken();
	in += "&mgm.vid.key="; in += vidkey;
	in += "&mgm.vid.target.uid="; in += list;
      }

      if ( (type == "-gids") ) {
	vidkey += ":gids";
	list = subtokenizer.GetToken();
	in += "&mgm.vid.key="; in += vidkey;
	in += "&mgm.vid.target.gid="; in += list;
      }
      
      if ( (type == "+sudo") ) {
	vidkey += ":root";
	list = " "; // fake
	in += "&mgm.vid.key="; in += vidkey;
	in += "&mgm.vid.target.sudo=true";
      }

      if ( (type == "-sudo") ) {
	vidkey += ":root";
	list = " "; // fake
	in += "&mgm.vid.key="; in += vidkey;
	in += "&mgm.vid.target.sudo=false";
      }
      if (!list.length()) {
	goto com_vid_usage;
      }
      global_retc = output_result(client_admin_command(in));
      return (0);
    }
    
    if (key == "map") {
      in += "&mgm.vid.cmd=map";
      XrdOucString type = subtokenizer.GetToken();

      if (!type.length())
	goto com_vid_usage;

      bool hastype=false;
      if ( (type == "-krb5") )  {     
	in += "&mgm.vid.auth=krb5";
	hastype=true;
      }
      if ( (type == "-ssl") ) {
	in += "&mgm.vid.auth=ssl";
	hastype=true;
      }
      if ( (type == "-sss") ) {
	in += "&mgm.vid.auth=sss";
	hastype=true;
      }
      if ( (type == "-unix") ) {
	in += "&mgm.vid.auth=unix";
	hastype=true;
      }
      if ( (type == "-tident") ) {
	in += "&mgm.vid.auth=tident";
	hastype=true;
      }

      if (!hastype) 
	goto com_vid_usage;
      

      XrdOucString pattern = subtokenizer.GetToken();
      // deal with patterns containing spaces but inside ""
      if (pattern.beginswith("\"")) {
	if (!pattern.endswith("\""))
	  do {
	    XrdOucString morepattern = subtokenizer.GetToken();

	    if (morepattern.endswith("\"")) {
	      pattern += " ";
	      pattern += morepattern;
	      break;
	    }
	    if (!morepattern.length()) {
	      goto com_vid_usage;
	    }
	    pattern += " ";
	    pattern += morepattern;
	  } while (1);
      }
      if (!pattern.length()) 
	goto com_vid_usage;

      in += "&mgm.vid.pattern="; in += pattern;

      XrdOucString vid= subtokenizer.GetToken();
      if (!vid.length())
	goto com_vid_usage;

      if (vid.beginswith("vuid:")) {
	vid.replace("vuid:","");
	in += "&mgm.vid.uid=";in += vid;	
	
	XrdOucString vid = subtokenizer.GetToken();
	if (vid.length()) {
	  fprintf(stderr,"Got %s\n", vid.c_str());
	  if (vid.beginswith("vgid:")) {
	    vid.replace("vgid:","");
	    in += "&mgm.vid.gid=";in += vid;
	  } else {
	    goto com_vid_usage;
	  }
	}
      } else {
	if (vid.beginswith("vgid:")) {
	  vid.replace("vgid:","");
	  in += "&mgm.vid.gid=";in += vid;
	} else {
	  goto com_vid_usage;
	}
      }

      in += "&mgm.vid.key="; in += "<key>";

      global_retc = output_result(client_admin_command(in));
      return (0);
    }
  }

  if ( subcommand == "rm" ) {
    XrdOucString in ="mgm.cmd=quota&mgm.subcmd=rm";
    XrdOucString key   = subtokenizer.GetToken();
    if ( (!key.length()) ) 
      goto com_vid_usage;
    in += "&mgm.vid.key="; in += key;
    
    global_retc = output_result(client_admin_command(in));
    return (0);
  }
  
 com_vid_usage:
  printf("usage: vid ls [-u] [-g] [s] [-U] [-G]                                                               : list configured policies\n");
  printf("                                        -u : show only user role mappings\n");
  printf("                                        -g : show only group role mappings\n");
  printf("                                        -s : show list of sudoers\n");
  printf("                                        -U : show user alias mapping\n");
  printf("                                        -G : show groupalias mapping\n");
  printf("usage: vid set membership <uid> -uids [<uid1>,<uid2>,...]\n");
  printf("       vid set membership <uid> -gids [<gid1>,<gid2>,...]\n");
  printf("       vid set membership <uid> [+|-]sudo \n");
  printf("       vid set map -krb5|-ssl|-sss|-unix|-tident <pattern> [vuid:<uid>] [vgid:<gid>] \n");
  printf("usage: vid rm <key>                                                                                 : remove configured vid with name key - hint: use config dump to see the key names of vid rules\n");

  return (0);
}

/* Configuration System listing, configuration, manipulation */
int
com_config (char* arg1) {
  // split subcommands
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString subcommand = subtokenizer.GetToken();
  XrdOucString arg = subtokenizer.GetToken();
  
  if ( subcommand == "dump" ) {
    XrdOucString in ="mgm.cmd=config&mgm.subcmd=dump";
    if (arg.length()) { 
      do {
	if (arg == "-fs") {
	  in += "&mgm.config.fs=1";
	  arg = subtokenizer.GetToken();
	} else 
	  if (arg == "-vid") {
	    in += "&mgm.config.vid=1";
	    arg = subtokenizer.GetToken();
	  } else 
	    if (arg == "-quota") {
	      in += "&mgm.config.quota=1";
	      arg = subtokenizer.GetToken();
	    } else 
	      if (arg == "-comment") {
		in += "&mgm.config.comment=1";
		arg = subtokenizer.GetToken();
	      } else 
		if (arg == "-policy") {
		  in += "&mgm.config.policy=1";
		  arg = subtokenizer.GetToken();
		} else 
		  if (!arg.beginswith("-")) {
		    in += "&mgm.config.file=";
		    in += arg;
		    arg = subtokenizer.GetToken();
		  }
      } while (arg.length());
    }      
    
    global_retc = output_result(client_admin_command(in));
    return (0);
  }

  
  
  if ( subcommand == "ls" ) {
    XrdOucString in ="mgm.cmd=config&mgm.subcmd=ls";
    if (arg == "-backup") {
      in += "&mgm.config.showbackup=1";
    }
    global_retc = output_result(client_admin_command(in));
    return (0);
  }
  
  if ( subcommand == "load") {
    XrdOucString in ="mgm.cmd=config&mgm.subcmd=load&mgm.config.file=";
    if (!arg.length()) 
      goto com_config_usage;
    
    in += arg;
    global_retc = output_result(client_admin_command(in));
    return (0);
  }

  if ( subcommand == "reset") {
    XrdOucString in ="mgm.cmd=config&mgm.subcmd=reset";
    global_retc = output_result(client_admin_command(in));
    return (0);
  }

  if ( subcommand == "save") {
    XrdOucString in ="mgm.cmd=config&mgm.subcmd=save";
    bool hasfile =false;
    printf("arg is %s\n", arg.c_str());
    do {
      if (arg == "-f") {
	in += "&mgm.config.force=1";
	arg = subtokenizer.GetToken();
      } else 
	if (arg == "-comment") {
	  in += "&mgm.config.comment=";
	  arg = subtokenizer.GetToken();
	  if (arg.beginswith("\"")) {
	    in += arg;
	    arg = subtokenizer.GetToken();
	    if (arg.length()) {
	      do {
		in += " ";
		in += arg;
		arg = subtokenizer.GetToken();
	      } while (arg.length() && (!arg.endswith("\"")));
	      if (arg.endswith("\"")) {
		in += " ";
		in += arg;
		arg = subtokenizer.GetToken();
	      }
	    }
	  }
	} else {
	  if (!arg.beginswith("-")) {
	    in += "&mgm.config.file=";
	    in += arg;
	    hasfile = true;
	    arg = subtokenizer.GetToken();
	  } else {
	    goto com_config_usage;
	  }
	}
    } while (arg.length());
    
    if (!hasfile) goto com_config_usage;
    global_retc = output_result(client_admin_command(in));
    return (0);
  }

  if ( subcommand == "diff") {
    XrdOucString in ="mgm.cmd=config&mgm.subcmd=diff";
    arg = subtokenizer.GetToken();
    if (arg.length()) 
      goto com_config_usage;
    
    global_retc = output_result(client_admin_command(in));
    return (0);
  }


  if ( subcommand == "changelog") {
    XrdOucString in ="mgm.cmd=config&mgm.subcmd=changelog";
    if (arg.length()) {
      if (arg.beginswith("-")) {
	// allow -100 and 100 
	arg.erase(0,1);
      }
      in += "&mgm.config.lines="; in+= arg;
    }

    arg = subtokenizer.GetToken();
    if (arg.length()) 
      goto com_config_usage;

    global_retc = output_result(client_admin_command(in));
    return (0);
  }
  
 com_config_usage:
  printf("usage: config ls   [-backup]                                             :  list existing configurations\n");
  printf("usage: config dump [-fs] [-vid] [-quota] [-policy] [-comment] [<name>]   :  dump current configuration or configuration with name <name>\n");

  printf("usage: config save [-comment \"<comment>\"] [-f] [<name>]                :  save config (optionally under name)\n");
  printf("usage: config load [-comment \"<comment>\"] [-f] [<name>]                :  load config (optionally with name)\n");
  printf("usage: config diff                                                       :  show changes since last load/save operation\n");
  printf("usage: config changelog [-#lines]                                        :  show the last <#> lines from the changelog - default is -10 \n");
  printf("usage: config reset                                                      :  reset all configuration to empty state\n");

  return (0);
}

/* Debug Level Setting */
int
com_debug (char* arg1) {
  // split subcommands
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString level     = subtokenizer.GetToken();
  XrdOucString nodequeue = subtokenizer.GetToken();
  XrdOucString filterlist="";

  if (level == "this") {
    printf("info: toggling shell debugmode to debug=%d\n",debug);
    debug = !debug;
    return (0);
  }
  if ( level.length() ) {
    XrdOucString in = "mgm.cmd=debug&mgm.debuglevel="; in += level; 

    if (nodequeue.length()) {
      if (nodequeue == "-filter") {
	filterlist = subtokenizer.GetToken();
	in += "&mgm.filter="; in += filterlist;
      } else {
	in += "&mgm.nodename="; in += nodequeue;
	nodequeue = subtokenizer.GetToken();
	if (nodequeue == "-filter") {
	  filterlist = subtokenizer.GetToken();
	  in += "&mgm.filter="; in += filterlist;
	}
      }
    } 
    


    global_retc = output_result(client_admin_command(in));
    return (0);
  }

  printf("       debug  <level> [-filter <unitlist>]                : set the mgm where this console is connected to into debug level <level>\n");
  printf("       debug  <node-queue> <level> [-filter <unitlist>]   : set the <node-queue> into debug level <level>\n");
  printf("               <unitlist> is a string list of units which should be filtered out in the message log !");
  printf("               Examples: > debug info *\n");
  printf("                         > debug info /eos/*/fst\n");
  printf("                         > debug info /eos/*/mgm\n");
  printf("                         > debug debug -filter MgmOfsMessage\n");
  printf("       debug  this                                        : toggle the debug flag for the shell itself\n");
  return (0);
}

/* Restart System */
int
com_restart (char* arg1) {
  // split subcommands
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString nodes = subtokenizer.GetToken();
  XrdOucString selection = subtokenizer.GetToken();

  XrdOucString in = "mgm.cmd=restart&mgm.subcmd="; 
  if (nodes.length()) {
    in += nodes;
    if (selection.length()) {
      in += "&mgm.nodename=";
      in += selection;
    }
    
    global_retc = output_result(client_admin_command(in));
    return (0);
  }
  
  printf("       restart fst [*]                         : restart all services on fst nodes !\n");
  return (0);
}

/* File handling */
int 
com_file (char* arg1) {
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString cmd = subtokenizer.GetToken();
  XrdOucString path = subtokenizer.GetToken();
  XrdOucString fsid1 = subtokenizer.GetToken();
  XrdOucString fsid2 = subtokenizer.GetToken();

  path = abspath(path.c_str());

  XrdOucString in = "mgm.cmd=file";
  if ( ( cmd != "drop") && ( cmd != "move") && ( cmd != "replicate" ) ) {
    goto com_file_usage;
  }

  if (cmd == "drop") {
    if ( !path.length() || !fsid1.length()) 
      goto com_file_usage;
    in += "&mgm.subcmd=drop";
    in += "&mgm.path="; in += path;
    in += "&mgm.file.fsid="; in += fsid1;
  }
  
  if (cmd == "move") {
    if ( !path.length() || !fsid1.length() || !fsid2.length() )
      goto com_file_usage;
    in += "&mgm.subcmd=move";
    in += "&mgm.path="; in += path;
    in += "&mgm.file.sourcefsid="; in += fsid1;
    in += "&mgm.file.targetfsid="; in += fsid2;
  }

  if (cmd == "replicate") {
    if ( !path.length() || !fsid1.length() || !fsid2.length() )
      goto com_file_usage;
    in += "&mgm.subcmd=replicate";
    in += "&mgm.path="; in += path;
    in += "&mgm.file.sourcefsid="; in += fsid1;
    in += "&mgm.file.targetfsid="; in += fsid2;
  }
  
  global_retc = output_result(client_user_command(in));
  return (0);

 com_file_usage:
  printf("usage: file drop <path> <fsid>                                       :  drop the file <path> part on <fsid>\n");
  printf("       file move <path> <fsid1> <fsid2>                              :  move the file <path> part on <fsid1> to <fsid2>\n");
  printf("       file replicate <path> <fsid1> <fsid2>                         :  replicate file <path> part on <fsid1> to <fsid2>\n");
  return (0);
}




/* Get file information */
int
com_fileinfo (char* arg1) {
  // split subcommands
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString path = subtokenizer.GetToken();
  XrdOucString selection = subtokenizer.GetToken();

  XrdOucString in = "mgm.cmd=fileinfo&"; 
  if (!path.length()) {
    goto com_fileinfo_usage;
    
  } else {
    path = abspath(path.c_str());
    in += "mgm.path=";
    in += path;
    
    global_retc = output_result(client_user_command(in));
    return (0);
  }

 com_fileinfo_usage:
  printf("usage: fileinfo <path>                                                   :  print file information for <path>\n");
  return (0);

}

/* Create a directory */
int
com_mkdir (char* arg1) {
  // split subcommands
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString path = subtokenizer.GetToken();
  XrdOucString in = "mgm.cmd=mkdir"; 

  if (path == "-p") {
    path = subtokenizer.GetToken();
    in += "&mgm.option=p";
  }
  if (!path.length()) {
    goto com_mkdir_usage;
    
  } else {
    path = abspath(path.c_str());
    in += "&mgm.path=";
    in += path;
    
    global_retc = output_result(client_user_command(in));
    return (0);
  }

 com_mkdir_usage:
  printf("usage: mkdir -p <path>                                                :  create directory <path>\n");
  return (0);

}

/* Remove a directory */
int
com_rmdir (char* arg1) {
  // split subcommands
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString path = subtokenizer.GetToken();
  XrdOucString selection = subtokenizer.GetToken();

  XrdOucString in = "mgm.cmd=rmdir&"; 
  if (!path.length()) {
    goto com_rmdir_usage;
    
  } else {
    path = abspath(path.c_str());
    in += "mgm.path=";
    in += path;
    
    global_retc = output_result(client_user_command(in));
    return (0);
  }

 com_rmdir_usage:
  printf("usage: rmdir <path>                                                   :  remote directory <path>\n");
  return (0);

}

/* Retrieve realtime log output */

int 
com_rtlog (char* arg1) {
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString queue = subtokenizer.GetToken();
  XrdOucString lines = subtokenizer.GetToken();
  XrdOucString tag   = subtokenizer.GetToken();
  XrdOucString filter= subtokenizer.GetToken();
  XrdOucString in = "mgm.cmd=rtlog&mgm.rtlog.queue=";

  if (!queue.length())
    goto com_rtlog_usage;

  if ( (queue!=".") && (queue!="*") && (!queue.beginswith("/eos/"))) {
    // there is no queue argument and means to talk with the mgm directly
    filter = tag;
    tag = lines;
    lines =queue;
    queue =".";
  }

  if (queue.length()) {
    in += queue;
    if (!lines.length())
      in += "&mgm.rtlog.lines=10";
    else 
      in += "&mgm.rtlog.lines="; in += lines;
    if (!tag.length()) 
      in += "&mgm.rtlog.tag=err";
    else 
      in += "&mgm.rtlog.tag="; in += tag;

    if (filter.length()) 
      in += "&mgm.rtlog.filter="; in += filter;

    global_retc = output_result(client_admin_command(in));
    return (0);
  }
  
 com_rtlog_usage:
  printf("usage: rtlog [<queue>|*|.] [<sec in the past>=3600] [<debug>=err] [filter-word]\n");
  printf("                     - '*' means to query all nodes\n");
  printf("                     - '.' means to query only the connected mgm\n");
  printf("                     - if the first argument is ommitted '.' is assumed\n");
  return (0);
}

/* List a directory */
int
com_ls (char* arg1) {
  // split subcommands
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString param="";
  XrdOucString option="";
  XrdOucString path="";
  XrdOucString in = "mgm.cmd=ls"; 

  do {
    param = subtokenizer.GetToken();
    if (!param.length())
      break;
    if (param.beginswith("-")) {
      option+= param;
      if ( (option.find("&")) != STR_NPOS) {
	goto com_ls_usage;
      }
    } else {
      path = param;
      break;
    }
  } while(1);

  if (!path.length()) {
    path = pwd;
  } 

  path = abspath(path.c_str());

  in += "&mgm.path=";
  in += path;
  in += "&mgm.option=";
  in += option;
  global_retc = output_result(client_user_command(in));
  return (0);

 com_ls_usage:
  printf("usage: ls <path>                                                       :  list directory <path>\n");
  return (0);
}

/* Remove a file */
int
com_rm (char* arg1) {
  // split subcommands
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString s1 = subtokenizer.GetToken();
  XrdOucString s2 = subtokenizer.GetToken();
  XrdOucString path;
  XrdOucString option;

  if (s1 == "-r") {
    option ="r";
    path = s2;
  } else {
    option ="";
    path = s1;
  }
  
  XrdOucString in = "mgm.cmd=rm&"; 
  if (!path.length()) {
    goto com_rm_usage;
    
  } else {
    path = abspath(path.c_str());
    in += "mgm.path=";
    in += path;
    in += "&mgm.option=";
    in += option;

    global_retc = output_result(client_user_command(in));
    return (0);
  }

 com_rm_usage:
  printf("usage: rm [-r] <path>                                                  :  remove file <path>\n");
  return (0);
}


/* Find files/directories */
int
com_find (char* arg1) {
  // split subcommands
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString s1;
  XrdOucString path;
  XrdOucString option="";
  XrdOucString attribute="";
  XrdOucString printkey="";
  XrdOucString in = "mgm.cmd=find&"; 
  while ( (s1 = subtokenizer.GetToken()).length() && (s1.beginswith("-")) ) {
    if (s1 == "-d") {
      option +="d";
    }
    
    if (s1 == "-f") {
      option +="f";
    }
    
    if (s1.beginswith( "-h" )) {
      goto com_find_usage;
    }

    if (s1 == "-x") {
      option += "x";

      attribute = subtokenizer.GetToken();

      if (!attribute.length())
	goto com_find_usage;

      if ((attribute.find("&")) != STR_NPOS)
	goto com_find_usage;
    }

    if (s1 == "-p") {
      option += "p";
      
      printkey = subtokenizer.GetToken();
      
      if (!printkey.length()) 
	goto com_find_usage;
    }
  }
  
  if (s1.length()) {
    path = s1;
  }
  
  path = abspath(path.c_str());
  in += "mgm.path=";
  in += path;
  in += "&mgm.option=";
  in += option;
  if (attribute.length()) {
    in += "&mgm.find.attribute=";
    in += attribute;
  }
  if (printkey.length()) {
    in += "&mgm.find.printkey=";
    in += printkey;
  }

  global_retc = output_result(client_user_command(in));
  return (0);

 com_find_usage:
  printf("usage: find [-d] [-f] [-x <key=<val>] [-p <key>] <path>                       :  find files(-f) or directories (-d) in <path>\n");
  printf("                                                                              :  find entries(-x) with <key>=<val>\n");
  printf("                                                                              :  additionally print (-p) the value of <key> for each entry\n");
  printf("                                                                      default :  find files and directories\n");
  return (0);
}

/* Namespace Interface */
int 
com_ns (char* arg1) {
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString cmd = subtokenizer.GetToken();
  XrdOucString option = subtokenizer.GetToken();

  XrdOucString in ="";
  if ( ( cmd != "stat") ) {
    goto com_ns_usage;
  }
  
  in = "mgm.cmd=ns&";
  if (cmd == "stat") {
    in += "mgm.subcmd=stat";
  }

  if (option == "-a") {
    in += "&mgm.option=a";
  }
  
  global_retc = output_result(client_admin_command(in));
  return (0);

 com_ns_usage:
  printf("usage: ns stat [-a]                                                  :  print namespace statistics\n");
  printf("                -a                                                   -  break down by uid/gid\n");
  return (0);
}


/* Test Interface */
int
com_test (char* arg1) {
  // split subcommands
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
 
  do {
    XrdOucString tag  = subtokenizer.GetToken();
    if (! tag.length()) 
      break;

    XrdOucString sn = subtokenizer.GetToken();
    if (! sn.length()) {
      goto com_test_usage;
    }

    int n = atoi(sn.c_str());
    printf("info: doing directory test with loop <n>=%d", n);

    if (tag == "mkdir") {
      XrdMqTiming timing("mkdir");
      
      TIMING("start",&timing);

      for (int i=0; i< 10; i++) {
	char dname[1024];
	sprintf(dname,"/test/%02d", i);
	XrdOucString cmd = ""; cmd += dname;
	//	printf("===> %s\n", cmd.c_str());
	com_mkdir((char*)cmd.c_str());

	for (int j=0; j< n/10; j++) {
	  sprintf(dname,"/test/%02d/%05d", i,j);
	  XrdOucString cmd = ""; cmd += dname;
	  //	  printf("===> %s\n", cmd.c_str());
	  com_mkdir((char*)cmd.c_str());
	}
      }
      TIMING("stop",&timing);
      timing.Print();
    }

    if (tag == "rmdir") {
      XrdMqTiming timing("mkdir");   
      TIMING("start",&timing);

      for (int i=0; i< 10; i++) {
	char dname[1024];
	sprintf(dname,"/test/%02d", i);
	XrdOucString cmd = ""; cmd += dname;
	//printf("===> %s\n", cmd.c_str());

	for (int j=0; j< n/10; j++) {
	  sprintf(dname,"/test/%02d/%05d", i,j);
	  XrdOucString cmd = ""; cmd += dname;
	  //printf("===> %s\n", cmd.c_str());
	  com_rmdir((char*)cmd.c_str());
	}
	com_rmdir((char*)cmd.c_str());
      }
      TIMING("stop",&timing);
      timing.Print();
    }

    if (tag == "ls") {
      XrdMqTiming timing("ls");   
      TIMING("start",&timing);

      for (int i=0; i< 10; i++) {
	char dname[1024];
	sprintf(dname,"/test/%02d", i);
	XrdOucString cmd = ""; cmd += dname;
	com_ls((char*)cmd.c_str());
      }
      TIMING("stop",&timing);
      timing.Print();
    }

    if (tag == "lsla") {
      XrdMqTiming timing("ls");   
      TIMING("start",&timing);

      for (int i=0; i< 10; i++) {
	char dname[1024];
	sprintf(dname,"/test/%02d", i);
	XrdOucString cmd = "-la "; cmd += dname;
	com_ls((char*)cmd.c_str());
      }
      TIMING("stop",&timing);
      timing.Print();
    }
  } while (1);

  return (0);
 com_test_usage:
  printf("usage: test [mkdir|rmdir|ls|lsla <N> ]                                             :  run performance test\n");
  return (0);

}


int com_silent PARAMS((char*)) {
  silent = (!silent);
  return (0);
}

int com_timing PARAMS((char*)) {
  timing = (!timing);
  return (0);
}


/* Function which tells you that you can't do this. */
void too_dangerous (char *caller) {
  fprintf (stderr,
           "%s: Too dangerous for me to distribute.  Write it yourself.\n",
           caller);
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

std::string textnormal("\033[0m");
std::string textblack("\033[49;30m");
std::string textred("\033[49;31m");
std::string textrederror("\033[47;31m\e[5m");
std::string textblueerror("\033[47;34m\e[5m");
std::string textgreen("\033[49;32m");
std::string textyellow("\033[49;33m");
std::string textblue("\033[49;34m");
std::string textbold("\033[1m");
std::string textunbold("\033[0m");

int main (int argc, char* argv[]) {
  char *line, *s;
  serveruri = (char*)"root://";
  XrdOucString HostName      = XrdNetDNS::getHostName();
  serveruri += HostName;
  serveruri += ":1094";

  int argindex=1;
  if (argc>1) {
    XrdOucString in1 = argv[1];
    if (in1.beginswith("root://")) {
      serveruri = argv[1];
      in1 = argv[2];
      argindex = 2;
    } 
    if (in1.length()) {
      // check if this is a file
      if (!access(in1.c_str(), R_OK)) {
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
	// strip leading and trailing white spaces
	while (cmdline.beginswith(" ")) {cmdline.erase(0,1);}
	while (cmdline.endswith(" ")) {cmdline.erase(cmdline.length()-1,1);}
	execute_line ((char*)cmdline.c_str());
	exit(0);
      }
    }
  }


  /* install a shutdown handler */
  signal (SIGINT,  exit_handler);

  char prompt[4096];
  sprintf(prompt,"%sEOS Console%s [%s%s%s] |> ", textbold.c_str(),textunbold.c_str(),textred.c_str(),serveruri.c_str(),textnormal.c_str());

  progname = argv[0];

  initialize_readline ();	/* Bind our completer. */

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
      sprintf(prompt,"%sEOS Console%s [%s%s%s] |%s> ", textbold.c_str(),textunbold.c_str(),textred.c_str(),serveruri.c_str(),textnormal.c_str(),pwd.c_str());
      line = readline (prompt);

      if (!line)
        break;

      /* Remove leading and trailing whitespace from the line.
         Then, if there is anything left, add it to the history list
         and execute it. */
      s = stripwhite (line);

      if (*s)
        {
          add_history (s);
          execute_line (s);
        }

      free (line);
    }

  write_history(historyfile.c_str());
  exit (0);
}

/* Execute a command line. */
int
execute_line (char *line) {
  register int i;
  COMMAND *command;
  char *word;

  /* Isolate the command word. */
  i = 0;
  while (line[i] && whitespace (line[i]))
    i++;
  word = line + i;

  while (line[i] && !whitespace (line[i]))
    i++;

  if (line[i])
    line[i++] = '\0';

  command = find_command (word);

  if (!command)
    {
      fprintf (stderr, "%s: No such command for EOS Console.\n", word);
      return (-1);
    }

  /* Get argument to command, if any. */
  while (whitespace (line[i]))
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

  for (s = string; whitespace (*s); s++)
    ;

  if (*s == 0)
    return (s);

  t = s + strlen (s) - 1;
  while (t > s && whitespace (*t))
    t--;
  *++t = '\0';

  return s;
}

