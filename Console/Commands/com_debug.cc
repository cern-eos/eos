/*----------------------------------------------------------------------------*/
#include "ConsoleMain.hh"
/*----------------------------------------------------------------------------*/

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
    if (debug) {
      XrdCommonLogging::SetLogPriority(LOG_DEBUG);
    } else {
      XrdCommonLogging::SetLogPriority(LOG_NOTICE);
    }
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
