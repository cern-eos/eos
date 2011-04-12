/*----------------------------------------------------------------------------*/
#include "console/ConsoleMain.hh"
/*----------------------------------------------------------------------------*/

/* access (deny/bounce/redirect) -  Interface */
int 
com_access (char* arg1) {
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString option="";
  XrdOucString options="";
  bool ok=false;
  XrdOucString in ="";
  in = "mgm.cmd=access";

  XrdOucString subcmd = subtokenizer.GetToken();

  if (subcmd == "ban") {
    ok = true;
    in += "&mgm.subcmd=ban";
  } 

  if (subcmd == "unban") {
    in += "&mgm.subcmd=unban";
    ok = true;
  }

  if (subcmd == "allow") {
    in += "&mgm.subcmd=allow";
    ok = true;
  }

  if (subcmd == "unallow") {
    in += "&mgm.subcmd=unallow";
    ok = true;
  }

  if (subcmd == "ls") {
    in += "&mgm.subcmd=ls";
    ok = true;
  }

  if (ok) {
    ok = false;
    XrdOucString type = "";
    XrdOucString maybeoption = subtokenizer.GetToken();
    while (maybeoption.beginswith("-")) {
      if ( (subcmd == "ls") && (maybeoption != "-m") && (maybeoption != "-n") )
	goto com_access_usage;
      if ( (subcmd != "ls") ) 
	goto com_access_usage;

      maybeoption.replace("-","");
      option += maybeoption;
      maybeoption = subtokenizer.GetToken();
    } 
    
    if (subcmd == "ls") {
      ok = true;
    }

    if ( (subcmd == "ban") || (subcmd == "unban") || (subcmd == "allow") || (subcmd == "unallow")) {
      type = maybeoption;
      XrdOucString id   = subtokenizer.GetToken();
      if ((!type.length()) || (!id.length())) 
	goto com_access_usage;
      
      if (type == "host") {
	in += "&mgm.access.host="; in += id;
	ok = true;
      }
      if (type == "user") {
	in += "&mgm.access.user="; in += id;
	ok = true;
      }
      if (type == "group") {
	in += "&mgm.access.group="; in += id;
	ok = true;
      }
    }
    if (!ok) 
      goto com_access_usage;
  } else {
    goto com_access_usage;
  }

  in += "&mgm.access.option="; in += option;


  global_retc = output_result(client_admin_command(in));
  return (0);

 com_access_usage:
  printf("usage: access ban user|group|host <identifier>                      : ban user,group or host with identifier <identifier>\n");
  printf("       access unban user|group|host <identifier>                    : unban user,group or host with identifier <identifier>\n");
  printf("\n");
  printf("       access allow user|group|host <identifier>                    : allows this user,group or host access\n");
  printf("       access unallow user|group|host <identifier>                  : unallows this user,group or host access\n");
  printf("hint:  if you add any 'allow' the instance allows only the listed users. A banned identifier will still overrule an allowed identifier!\n");
  printf("\n");
  printf("       access ls [-m] [-n]                                          : print banned,unbanned user,group, hosts\n");
  printf("                                                                      -m : output in monitoring format with <key>=<value>\n");
  printf("                                                                      -n : don't translate uid/gids to names\n");
  return (0);
}
