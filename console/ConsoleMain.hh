/*----------------------------------------------------------------------------*/
#include "XrdCommon/XrdCommonLayoutId.hh"
#include "XrdCommon/XrdCommonFmd.hh"
#include "XrdCommon/XrdCommonClientAdmin.hh"
#include "XrdCommon/XrdCommonFileSystem.hh"
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
extern XrdCommonClientAdminManager CommonClientAdminManager;
extern void command_result_stdout_to_vector(std::vector<std::string> &string_vector);
extern XrdOucEnv* CommandEnv;

/* A structure which contains information on the commands this program
   can understand. */

typedef struct {
  char *name;			/* User printable name of the function. */
  rl_icpfunc_t *func;		/* Function to call to do the job. */
  char *doc;			/* Documentation for this function.  */
} COMMAND;

extern COMMAND commands[];

extern int done;

extern int com_help PARAMS((char *));
extern int com_quit PARAMS((char *));
extern int com_attr PARAMS((char*));
extern int com_debug PARAMS((char*));
extern int com_cd PARAMS((char*));
extern int com_clear PARAMS((char*));
extern int com_chmod PARAMS((char*));
extern int com_config PARAMS((char*));
extern int com_file PARAMS((char*));
extern int com_fileinfo PARAMS((char*));
extern int com_find PARAMS((char*));
extern int com_fs   PARAMS((char*));
extern int com_ls PARAMS((char*));
extern int com_mkdir PARAMS((char*));
extern int com_ns PARAMS((char*));
extern int com_role PARAMS((char*));
extern int com_rmdir PARAMS((char*));
extern int com_rm PARAMS((char*));
extern int com_vid PARAMS((char*));
extern int com_pwd PARAMS((char*));
extern int com_quota PARAMS((char*));
extern int com_restart PARAMS((char*));
extern int com_rtlog PARAMS((char*));
extern int com_test PARAMS((char*));
extern int com_transfers PARAMS((char*));
extern int com_verify PARAMS((char*));
extern int com_silent PARAMS((char*));
extern int com_timing PARAMS((char*));
extern int com_whoami PARAMS((char*));
