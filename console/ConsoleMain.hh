/*----------------------------------------------------------------------------*/
#include "common/LayoutId.hh"
#include "common/Fmd.hh"
#include "common/Logging.hh"
#include "common/ClientAdmin.hh"
#include "common/FileSystem.hh"
#include "mq/XrdMqMessage.hh"
#include "mq/XrdMqTiming.hh"
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
#include <list>
#include <map>
#include <google/sparsehash/sparsehashtable.h>
#include <google/sparse_hash_set>
#include <math.h>
/*----------------------------------------------------------------------------*/

extern const char* abspath(const char* in);

extern XrdOucString pwd;
extern XrdOucString rstdout;
extern XrdOucString rstderr;
extern XrdOucString user_role;
extern XrdOucString group_role;

extern int global_retc;
extern bool silent;
extern bool timing;
extern bool debug;

extern XrdOucEnv* client_user_command(XrdOucString &in);
extern XrdOucEnv* client_admin_command(XrdOucString &in);
extern int output_result(XrdOucEnv* result);
extern eos::common::ClientAdminManager CommonClientAdminManager;
extern void command_result_stdout_to_vector(std::vector<std::string> &string_vector);
extern XrdOucEnv* CommandEnv;

/* A structure which contains information on the commands this program
   can understand. */

typedef int       CFunction(char *);

typedef struct {
  char *name;			/* User printable name of the function. */
  CFunction* func;;	        /* Function to call to do the job. */
  char *doc;			/* Documentation for this function.  */
} COMMAND;

extern COMMAND commands[];

extern int done;

extern int com_help (char *);
extern int com_quit (char *);
extern int com_attr (char*);
extern int com_debug (char*);
extern int com_cd (char*);
extern int com_clear (char*);
extern int com_chmod (char*);
extern int com_config (char*);
extern int com_file (char*);
extern int com_fileinfo (char*);
extern int com_find (char*);
extern int com_fs   (char*);
extern int com_ls (char*);
extern int com_mkdir (char*);
extern int com_ns (char*);
extern int com_role (char*);
extern int com_rmdir (char*);
extern int com_rm (char*);
extern int com_vid (char*);
extern int com_pwd (char*);
extern int com_quota (char*);
extern int com_restart (char*);
extern int com_rtlog (char*);
extern int com_test (char*);
extern int com_transfers (char*);
extern int com_verify (char*);
extern int com_silent (char*);
extern int com_timing (char*);
extern int com_whoami (char*);
