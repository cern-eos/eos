/*----------------------------------------------------------------------------*/
#include "console/ConsoleMain.hh"
/*----------------------------------------------------------------------------*/

/* Namespace Interface */
int 
com_fsck (char* arg1) {
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString cmd = subtokenizer.GetToken();
  XrdOucString option;
  XrdOucString options="";
  XrdOucString path="";
  XrdOucString in ="";

  if ( ( cmd != "stat") && ( cmd != "enable" ) && ( cmd != "disable") && ( cmd != "report" ) && ( cmd != "repair" ) ) {
    goto com_fsck_usage;
  }
  
  in = "mgm.cmd=fsck&";

  if (cmd == "enable") {
    in += "mgm.subcmd=enable";
  }
  if (cmd == "disable") {
    in += "mgm.subcmd=disable";
  }

  if (cmd == "stat") {
    in += "mgm.subcmd=stat";
  }

  if (cmd == "report") {
    in += "mgm.subcmd=report";
    do {
      option = subtokenizer.GetToken();
      if (option.length()) {
	XrdOucString tag="";
	if (option == "--error") {
	  tag = subtokenizer.GetToken();
	  if (!tag.length()) {
	    goto com_fsck_usage;
	  } else {
	    in += "&mgm.fsck.selection=";
	    in += tag;
	    continue;
	  }
	}
	
	while (option.replace("-","")) {}
	options += option;
      }
    } while (option.length());
  }
  
  if (cmd == "repair") {
    in += "mgm.subcmd=repair";
    option = subtokenizer.GetToken();
    if ( (! option.length()) || 
	 ( (option != "--checksum") &&
	   (option != "--unlink-unregistered") &&
	   (option != "--unlink-orphans") &&
	   (option != "--adjust-replicas") &&
	   (option != "--drop-missing-replicas") ) )
      goto com_fsck_usage;
    option.replace("--","");
    in += "&mgm.option=";
    in += option;
  }
  

  if (options.length()) {
    in += "&mgm.option="; in += options;
  }  

  global_retc = output_result(client_admin_command(in));
  return (0);

 com_fsck_usage:
  printf("usage: fsck stat                                                  :  print status of consistency check\n");
  printf("       fsck enable                                                :  enable fsck\n");
  printf("       fsck disable                                               :  disable fsck\n");
  printf("       fsck report [-h] [-g] [-m] [-a] [-i] [-l] [--error <tag>]  :  report consistency check results");
  printf("                                                               -g :  report global counters\n");
  printf("                                                               -m :  select monitoring output format\n");
  printf("                                                               -a :  break down statistics per filesystem\n");
  printf("                                                               -i :  print concerned file ids\n");
  printf("                                                               -l :  print concerned logical names\n");
  printf("                                               --error <tag>      :  select only errors with name <tag> in the printout\n");
  printf("                                                                     you get the names by doing 'fsck report -g'\n");
  printf("                                                               -h :  print help explaining the individual tags!\n");

  printf("       fsck repair --checksum\n");
  printf("                                                                  :  issues a 'verify' operation on all files with checksum errors\n");
  printf("       fsck repair --unlink-unregistered\n");
  printf("                                                                  :  unlink replicas which are not connected/registered to their logical name\n");
  printf("       fsck repair --unlink-orphans\n");
  printf("                                                                  :  unlink replicas which don't belong to any logical name\n");
  printf("       fsck repair --adjust-replicas\n");
  printf("                                                                  :  try to fix all replica inconsistencies\n");
  printf("       fsck repair --drop-missing-replicas\n");
  printf("                                                                  :  just drop replicas from the namespace if they cannot be found on disk\n");

  

	 
  return (0);
}
