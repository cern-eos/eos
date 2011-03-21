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
extern bool global_highlighting;
extern bool interactive;
extern bool silent;
extern bool timing;
extern bool debug;

extern XrdOucEnv* client_user_command(XrdOucString &in);
extern XrdOucEnv* client_admin_command(XrdOucString &in);
extern int output_result(XrdOucEnv* result, bool highlighting=true);
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
