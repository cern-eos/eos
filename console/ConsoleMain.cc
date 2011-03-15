/*----------------------------------------------------------------------------*/
#include "ConsoleMain.hh"
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

XrdOucEnv* CommandEnv=0; // this is a pointer to the result of client_admin... or client_user.... = it get's invalid when the output_result function is called

XrdCommonClientAdminManager CommonClientAdminManager;

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
int com_transfers PARAMS((char*));
int com_verify PARAMS((char*));
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
  { (char*)"transfers",com_transfers,(char*)"Transfer Interface"},
  { (char*)"verify",com_verify,(char*)"Verify Interface"},
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
       cmd.beginswith("cd ") ||
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

/*----------------------------------------------------------------------------*/

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

void usage() {
  fprintf(stderr,"usage: eos [-role <uid> <gid>] <mgm-url> <cmd> [<argN>]\n");
  fprintf(stderr,"usage: eos [-role <uid> <gid>] <mgm-url> <filename>\n");
}

int main (int argc, char* argv[]) {
  char *line, *s;
  serveruri = (char*)"root://";
  XrdOucString HostName      = XrdNetDNS::getHostName();
  serveruri += HostName;
  serveruri += ":1094";
  
  XrdOucString urole="";
  XrdOucString grole="";

  int argindex=1;
  if (argc>1) {
    XrdOucString in1 = argv[1];
    if (in1.beginswith("root://")) {
      serveruri = argv[1];
      in1 = argv[2];
      argindex = 2;
    } else {
      if (argc > 4) {
	if (in1 == "-role") {
	  urole = argv[2];
	  grole = argv[3];
	  in1 = argv[4];

	  // execute the role function
	  XrdOucString cmdline="role ";
	  cmdline += urole; cmdline += " ";
	  cmdline += grole;
	  execute_line ((char*)cmdline.c_str());
	} 
	if (in1.beginswith("root://")) {
	  serveruri = argv[4];
	  in1 = argv[5];
	  argindex = 5;
	}
      } else {
	usage();
	exit(-1);
      }
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


  /* configure looging */
  XrdCommonLogging::Init();
  XrdCommonLogging::SetUnit("eos");
  XrdCommonLogging::SetLogPriority(LOG_NOTICE);
  

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

