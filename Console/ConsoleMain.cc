/*----------------------------------------------------------------------------*/
#include "XrdMqOfs/XrdMqMessage.hh"
/*----------------------------------------------------------------------------*/
#include "XrdClient/XrdClient.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucTokenizer.hh"
#include "XrdNet/XrdNetDNS.hh"
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
/*----------------------------------------------------------------------------*/
XrdOucString serveruri="";
int global_retc=0;
/*----------------------------------------------------------------------------*/

void exit_handler (int a) {
  fprintf(stdout,"\n");
  fprintf(stderr,"<Control-C>\n");
  exit(-1);
}

/* The names of functions that actually do the manipulation. */

int com_help PARAMS((char *));
int com_quit PARAMS((char *));
int com_fs   PARAMS((char*));
int com_debug PARAMS((char*));
int com_clear PARAMS((char*));

/* A structure which contains information on the commands this program
   can understand. */

typedef struct {
  char *name;			/* User printable name of the function. */
  rl_icpfunc_t *func;		/* Function to call to do the job. */
  char *doc;			/* Documentation for this function.  */
} COMMAND;

COMMAND commands[] = {
  { (char*)"help", com_help, (char*)"Display this text" },
  { (char*)"?",    com_help, (char*)"Synonym for `help'" },
  { (char*)"fs",   com_fs,   (char*)"Filsystem configuration"},
  { (char*)"debug",com_debug,(char*)"Set debug level"},
  { (char*)"quit", com_quit, (char*)"Exit from EOS console" },
  { (char*)"exit", com_quit, (char*)"Exit from EOS console" },
  { (char*)"clear", com_clear, (char*)"Clear the terminal" },
  { (char*)".q",   com_quit, (char*)"Exit from EOS console" },
  { (char *)0, (rl_icpfunc_t *)0, (char *)0 }
};

/* Forward declarations. */
char *stripwhite (char *string);
COMMAND *find_command (char *command);
char **EOSConsole_completion (const char *text, int start, int intend);
char *command_generator (const char *text, int state);
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
  if (start == 0)
    matches = rl_completion_matches (text, command_generator);

  return (matches);
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

      if (strncmp (name, text, len) == 0)
        return (dupstr(name));
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

  XrdOucString rstdout = result->Get("mgm.proc.stdout");
  XrdOucString rstderr = result->Get("mgm.proc.stderr");


  // color replacements
  rstdout.replace("online","\033[1monline\033[0m");
  rstdout.replace("offline","\033[47;31m\e[5moffline\033[0m");

  int retc = EFAULT;
  if (result->Get("mgm.proc.retc")) {
    retc = atoi(result->Get("mgm.proc.retc"));
  }
  if (rstdout.length()) 
    fprintf(stdout,"%s\n",rstdout.c_str());
  if (rstderr.length())
    fprintf(stderr,"%s\n",rstderr.c_str());
  
  delete result;
  return retc;
}


XrdOucEnv* 
client_admin_command(XrdOucString &in) {
  XrdOucString out="";
  XrdOucString path = serveruri;
  path += "//proc/admin/";
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
    return new XrdOucEnv(out.c_str());
  }
  return 0;
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

int
com_clear (char *arg) {
  system("clear");
  return (0);
}

/* The user wishes to quit using this program.  Just set DONE non-zero. */
int
com_quit (char *arg) {
  done = 1;
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

  printf("usage: fs ls                                   : list configured filesystems (or by name or id match\n");
  printf("       fs set   <fs-name> <fs-id>              : configure filesystem with name and id\n");
  printf("       fs rm    <fs-name>|<fs-id>              : remove filesystem configuration by name or id\n");
  printf("       fs boot  <fs-id>|<node-queue>           : boot filesystem/node ['fs boot *' to boot all]  \n");
  return (0);
}

/* Filesystem listing, configuration, manipulation */
int
com_debug (char* arg1) {
  // split subcommands
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString level     = subtokenizer.GetToken();
  XrdOucString nodequeue = subtokenizer.GetToken();


  if ( level.length() ) {
    XrdOucString in = "mgm.cmd=debug&mgm.debuglevel="; in += level; 
    if (nodequeue.length()) {
      in += "&mgm.nodename="; in += nodequeue;
    }
    global_retc = output_result(client_admin_command(in));
    return (0);
  }

  printf("       debug  <node-queue> <level>: set <node-queue> into debug level <level>\n");

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

  if (argc>1) serveruri = argv[1];

  /* install a shutdown handler */
  signal (SIGINT,  exit_handler);

  char prompt[4096];
  sprintf(prompt,"%sEOS Console%s [%s%s%s] |> ", textbold.c_str(),textunbold.c_str(),textred.c_str(),serveruri.c_str(),textnormal.c_str());

  progname = argv[0];

  initialize_readline ();	/* Bind our completer. */

  /* Loop reading and executing lines until the user quits. */
  for ( ; done == 0; )
    {
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

