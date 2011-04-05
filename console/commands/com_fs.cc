/*----------------------------------------------------------------------------*/
#include "console/ConsoleMain.hh"
#include "common/StringConversion.hh"
/*----------------------------------------------------------------------------*/

extern int com_file (char*);
extern int com_fs   (char*);
extern int com_find (char*);

using namespace eos::common;

/*----------------------------------------------------------------------------*/

/* Filesystem listing, configuration, manipulation */
int
com_fs (char* arg1) {
  // split subcommands
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString subcommand = subtokenizer.GetToken();
  
  if ( subcommand == "add" ) {
    XrdOucString in ="mgm.cmd=fs&mgm.subcmd=add";
    XrdOucString uuid        = subtokenizer.GetToken();
    XrdOucString hostport    = subtokenizer.GetToken();
    XrdOucString mountpoint  = subtokenizer.GetToken();
    XrdOucString space       = subtokenizer.GetToken();
    XrdOucString configstatus = subtokenizer.GetToken();
    
    if (!uuid.length())
      goto com_fs_usage;
    
    if (!hostport.length())
      goto com_fs_usage;
    
    if (!hostport.beginswith("/eos/")) {
      hostport.insert("/eos/",0);
      hostport.append("/fst");
    }
    if (!mountpoint.length()) 
      goto com_fs_usage;
    
    if (!space.length())
      space = "default";
    
    if (!configstatus.length()) 
      configstatus = "off";
    
    in += "&mgm.fs.uuid=";        in += uuid;
    in += "&mgm.fs.node=";        in += hostport;
    in += "&mgm.fs.mountpoint=";  in += mountpoint;
    in += "&mgm.fs.space=";       in += space;
    in += "&mgm.fs.configstatus=";in += configstatus;
    
    XrdOucEnv* result = client_admin_command(in);
    global_retc = output_result(result);
    
    return (0);
  }
  
  
  if ( subcommand == "ls" ) {
    XrdOucString in ="mgm.cmd=fs&mgm.subcmd=ls";
    XrdOucString option="";
    bool highlighting=true;
    bool ok;

    do {
      subtokenizer.GetLine();
      option = subtokenizer.GetToken();
      if (option.length()) {
	if (option == "-m") {
	  in += "&mgm.outformat=m";
	  ok=true;
	  highlighting=false;
	} 
	if (option == "-l") {
	  in += "&mgm.outformat=l";
	  ok=true;
	}
	if (option == "-s") {
	  silent=true;
	  ok=true;
	}
	if (option == "-e") {
	  in += "&mgm.outformat=e";
	  ok=true;
	}
	
      } else {
	ok=true;
      }
    } while(option.length());

    if (!ok) 
      goto com_fs_usage;

    XrdOucEnv* result = client_admin_command(in);
    if (!silent) {
      global_retc = output_result(result, highlighting);
    } else {
      if (result) {
        global_retc = 0;
      } else {
        global_retc = EINVAL;
      }
    }
    return (0);
  }
  
  if ( subcommand == "config" ) {
    XrdOucString identifier = subtokenizer.GetToken();
    XrdOucString keyval   = subtokenizer.GetToken();

    if ( (!identifier.length()) || (!keyval.length()) ) {
      goto com_fs_usage;
    }
    
    if ( (keyval.find("=")) == STR_NPOS) {
      // not like <key>=<val>
      goto com_fs_usage;
    }

    std::string is = keyval.c_str();
    std::vector<std::string> token;
    std::string delimiter = "=";
    eos::common::StringConversion::Tokenize(is, token, delimiter);
    
    if (token.size() != 2) 
      goto com_fs_usage;
    
    XrdOucString in = "mgm.cmd=fs&mgm.subcmd=config&mgm.fs.identifier=";
    in += identifier;
    in += "&mgm.fs.key="; in += token[0].c_str();
    in += "&mgm.fs.value="; in += token[1].c_str();
    
    global_retc = output_result(client_admin_command(in));
    return (0);
  }


  if ( subcommand == "rm" ) {
    XrdOucString arg = subtokenizer.GetToken();
    if (!arg.c_str()) {
      goto com_fs_usage;
    }
    XrdOucString in = "mgm.cmd=fs&mgm.subcmd=rm";
    int fsid = atoi(arg.c_str());
    char r1fsid[128]; sprintf(r1fsid,"%d", fsid);
    char r2fsid[128]; sprintf(r2fsid,"%04d", fsid);
    if ( (arg == r1fsid) || (arg == r2fsid) ) {
      // boot by fsid
      in += "&mgm.fs.id=";
      in += arg;
    } else {
      XrdOucString mountpoint = subtokenizer.GetToken();
      in += "&mgm.fs.hostport=";
      in += arg;
      if (!(arg.find(":")!= STR_NPOS)) {
	in += ":1095";
      }
      in += "&mgm.fs.mountpoint=";
      if (!mountpoint.length())
	goto com_fs_usage;
      in += mountpoint;
    }

    global_retc = output_result(client_admin_command(in));
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
      in += "&mgm.fs.id=";
    } else {
      in += "&mgm.fs.node=";
    }

    in += arg;
    global_retc = output_result(client_admin_command(in));
    return (0);
  }

  if ( subcommand == "clone" ) {
    XrdOucString sourceid;
    sourceid = subtokenizer.GetToken();    
    XrdOucString targetid;
    targetid = subtokenizer.GetToken();
    if ( (!sourceid.length()) || (!targetid.length()))
      goto com_fs_usage;

    XrdOucString subcmd="dumpmd -s "; subcmd += sourceid; subcmd += " -path";

    com_fs((char*)subcmd.c_str());

    std::vector<std::string> files_found;
    files_found.clear();
    command_result_stdout_to_vector(files_found);
    std::vector<std::string>::const_iterator it;
    if (!files_found.size()) {
      output_result(CommandEnv);
    } else {
      if (CommandEnv) {
        delete CommandEnv; CommandEnv = 0;
      }

      for (unsigned int i=0; i< files_found.size(); i++) {
        if (!files_found[i].length())
          continue;
        XrdOucString line = files_found[i].c_str();
        if (line.beginswith("path=")) {
          line.replace("path=","");
          fprintf(stdout,"%06d: %s\n", i, line.c_str());
          // call the replication command here
          subcmd = "replicate "; subcmd += line; subcmd += " ";subcmd += sourceid; subcmd += " "; subcmd += targetid;
          com_file( (char*) subcmd.c_str());
        }
      }
    }
    
    return (0);
  }

  if ( subcommand == "compare" ) {
    XrdOucString sourceid;
    sourceid = subtokenizer.GetToken();    
    XrdOucString targetid;
    targetid = subtokenizer.GetToken();
    if (!sourceid.length() || !targetid.length()) 
      goto com_fs_usage;

    XrdOucString subcmd1="dumpmd -s "; subcmd1 += sourceid; subcmd1 += " -path";

    com_fs((char*)subcmd1.c_str());

    std::vector<std::string> files_found1;
    std::vector<std::string> files_found2;
    std::vector<std::string> files_miss1;

    files_found1.clear();
    files_found2.clear();
    files_miss1.clear();

    command_result_stdout_to_vector(files_found1);

    if (CommandEnv) {
      delete CommandEnv; CommandEnv = 0;
    }
    

    XrdOucString subcmd2="dumpmd -s "; subcmd2 += targetid; subcmd2 += " -path";

    com_fs((char*)subcmd2.c_str());

    command_result_stdout_to_vector(files_found2);

    if ( (!files_found1.size()) && (!files_found2.size())) {
      output_result(CommandEnv);
    }

    if (CommandEnv) {
      delete CommandEnv; CommandEnv = 0;
    }

    for (unsigned int i = 0 ; i < files_found1.size(); i++) {
      bool found=false;
      std::vector<std::string>::iterator it;
      for (it = files_found2.begin(); it != files_found2.end(); ++it) {
        if (files_found1[i] == *it) {
          files_found2.erase(it);
          found = true;
          break;
        }
      }
      if (!found) {
        files_miss1.push_back(files_found1[i]);
      }
    }
    // files_miss1 contains the missing files in 2
    // files_found2 contains the missing files in 1
      
    for (unsigned int i=0; i< files_miss1.size(); i++) {
      if (files_miss1[i].length())
        fprintf(stderr,"error: %s => found in %s - missing in %s\n", files_miss1[i].c_str(), sourceid.c_str(), targetid.c_str());
    }
    
    for (unsigned int i=0; i< files_found2.size(); i++) {
      if (files_found2[i].length())
        fprintf(stderr,"error: %s => found in %s - missing in %s\n", files_found2[i].c_str(), targetid.c_str(), sourceid.c_str());
    }
    
    return (0);
  }

  if ( subcommand == "dropfiles" ) {
    XrdOucString id;
    XrdOucString option;
    id = subtokenizer.GetToken();    
    option = subtokenizer.GetToken();

    if (!id.length()) 
      goto com_fs_usage;

    if (option.length() && (option != "-f")) {
      goto com_fs_usage;
    }

    XrdOucString subcmd="dumpmd -s "; subcmd += id; subcmd += " -path";

    com_fs((char*)subcmd.c_str());

    std::vector<std::string> files_found;
    files_found.clear();
    command_result_stdout_to_vector(files_found);
    std::vector<std::string>::const_iterator it;
    if (!files_found.size()) {
      output_result(CommandEnv);
    } else {
      if (CommandEnv) {
        delete CommandEnv; CommandEnv = 0;
      }

      string s;
      printf("Do you really want to delete ALL %u replica's from filesystem %s ?\n" , (int)files_found.size(), id.c_str());
      printf("Confirm the deletion by typing => ");
      XrdOucString confirmation="";
      for (int i=0; i<10; i++) {
        confirmation += (int) (9.0 * rand()/RAND_MAX);
      }
      printf("%s\n", confirmation.c_str());
      printf("                               => ");
      getline( std::cin, s );
      std::string sconfirmation = confirmation.c_str();
      if ( s == sconfirmation) {
        printf("\nDeletion confirmed\n");
        for (unsigned int i=0; i< files_found.size(); i++) {
          if (!files_found[i].length())
            continue;
          XrdOucString line = files_found[i].c_str();
          if (line.beginswith("path=")) {
            line.replace("path=","");
            fprintf(stdout,"%06d: %s\n", i, line.c_str());
            // call the replication command here
            subcmd = "drop "; subcmd += line; subcmd += " ";subcmd += id; 
            if (option.length()) { 
              subcmd += " "; subcmd += option; 
            }
            com_file( (char*) subcmd.c_str());
          }
        }
        printf("=> Deleted %u replicas from filesystem %s\n", (unsigned int) files_found.size(), id.c_str());
      } else {
        printf("\nDeletion aborted!\n");
      }
    }

   

    return (0);
  }
   
  if ( subcommand == "verify" ) {
    XrdOucString id;
    XrdOucString option="";
    id = subtokenizer.GetToken();
    XrdOucString options[5];
    
    options[0] = subtokenizer.GetToken();
    options[1] = subtokenizer.GetToken();
    options[2] = subtokenizer.GetToken();
    options[3] = subtokenizer.GetToken();
    options[4] = subtokenizer.GetToken();

    if (!id.length()) 
      goto com_fs_usage;

    for (int i=0; i< 5; i++) {
      if (options[i].length() && 
          ( options[i] != "-checksum") && ( options[i] != "-commitchecksum") && (options[i] != "-commitsize") && (options[i] != "-rate")) {
        goto com_fs_usage;
      }
      option += options[i]; option += " ";
      if (options[i] == "-rate") {
        option += options[i+1]; option += " ";
        i++;
      }
    }

    XrdOucString subcmd="dumpmd -s "; subcmd += id; subcmd += " -path";

    com_fs((char*)subcmd.c_str());

    std::vector<std::string> files_found;
    files_found.clear();
    command_result_stdout_to_vector(files_found);
    std::vector<std::string>::const_iterator it;
    if (!files_found.size()) {
      output_result(CommandEnv);
    } else {
      if (CommandEnv) {
        delete CommandEnv; CommandEnv = 0;
      }

      for (unsigned int i=0; i< files_found.size(); i++) {
        if (!files_found[i].length())
          continue;
        XrdOucString line = files_found[i].c_str();
        if (line.beginswith("path=")) {
          line.replace("path=","");
          fprintf(stdout,"%06d: %s\n", i, line.c_str());
          // call the replication command here
          subcmd = "verify "; subcmd += line; subcmd += " "; subcmd += id; subcmd += " ";
          if (option.length()) { 
            subcmd += option; 
          }
          com_file( (char*) subcmd.c_str());
        }
      }
    }
    return (0);
  }

  if ( subcommand == "dumpmd" ) {
    bool silentcommand=false;

    XrdOucString arg = subtokenizer.GetToken();
    if (arg == "-s") {
      silentcommand=true;
      arg = subtokenizer.GetToken();
    }


    XrdOucString option1 = subtokenizer.GetToken();
    XrdOucString option2 = subtokenizer.GetToken();
    XrdOucString option3 = subtokenizer.GetToken();

    XrdOucString in = "mgm.cmd=fs&mgm.subcmd=dumpmd";
    if (!arg.length()) 
      goto com_fs_usage;
    
    int fsid = atoi(arg.c_str());
    in += "&mgm.fsid=";
    in += (int) fsid;
    
    if ( (option1 == "-path") || (option2 == "-path") || (option3 == "-path") ) {
      in += "&mgm.dumpmd.path=1";
    } 
      
    if ( (option1 == "-fid") || (option2 == "-fid") || (option3 == "-fid") ) {
      in += "&mgm.dumpmd.fid=1";
    } 

    if ( (option1 == "-size") || (option2 == "-size") || (option3 == "-size") ) {
      in += "&mgm.dumpmd.size=1";
    } 
    
    if ( option1.length() && (option1 != "-path") && (option1 != "-fid") && (option1 != "-size")) 
      goto com_fs_usage;
    
    if ( option2.length() && (option2 != "-path") && (option2 != "-fid") && (option2 != "-size"))
      goto com_fs_usage;

    if ( option3.length() && (option3 != "-path") && (option3 != "-fid") && (option3 != "-size"))
      goto com_fs_usage;

    XrdOucEnv* result = client_admin_command(in);
    if (!silentcommand) {
      global_retc = output_result(result);
    } else {
      if (result) {
        global_retc = 0;
      } else {
        global_retc = EINVAL;
      }
    }

    return (0);
  }

 com_fs_usage:

  printf("usage: fs ls  [-m] [-l] [-e] -s                                 : list configured filesystems (or by name or id match\n");
  printf("                                                                  -m : display monitoring format <key>=<value>\n");
  printf("                                                                  -l : display long format\n");
  printf("                                                                  -e : display format with error information\n");
  printf("       fs add <uuid> <node-queue>|<host:port> <mountpoint> [<schedgroup>] [<status]\n");
  printf("                                                                : add a filesystem and dynamically assign a filesystem id based on the unique identifier for the disk <uuid>\n");
  printf("       fs config <host>:<port><path>|<fsid>|<uuid> <key>=<value>: configure filesystem parameter for a single filesystem identified by host:port/path, filesystem id or filesystem UUID.\n");
  printf("         => fs config <...> configstatus=rw|wo|ro|drain|off\n");
  printf("                    <status> can be := rw                       : filesystem is in read write mode\n");
  printf("                                    := wo                       : filesystem is in write-once mode\n");
  printf("                                    := ro                       : filesystem is in read-only mode\n");
  printf("                                    := drain                    : filesystem is in drain mode\n");
  printf("                                    := off                      : filesystem is disabled\n"); 

  printf("       fs rm    <fs-name>|<fs-id>                               : remove filesystem configuration by name or id\n");
  printf("       fs boot  <fs-id>|<node-queue>                            : boot filesystem/node ['fs boot *' to boot all]  \n");
  printf("                    -sched <group>                              : allows to change the scheduling group\n");
  printf("       fs clone <fs-id-src> <fs-id-dst>                         : allows to clone the contents of <fs-id-src> to <fs-id-dst>\n");
  printf("       fs compare <fs-id-src> <fs-id-dst>|<space>               : does a comparison of <fs-id-src> with <fs-id-dst>|<space>\n");
  printf("       fs dropfiles <fs-id> [-f]                                : allows to drop all files on <fs-id> - force (-f) unlinks/removes files at the time from the NS (you have to cleanup or remove the files from disk) \n");
  printf("       fs dumpmd [-s] <fs-id> [-fid] [-path]                    : dump all file meta data on this filesystem in query format\n");
  printf("                                                                  -s    : don't printout keep an internal reference\n");
  printf("                                                                  -fid  : dump only a list of file id's stored on this filesystem\n");
  printf("                                                                  -path : dump only a list of file names stored on this filesystem\n");
  printf("       fs verify <fs-name>|<fs-id> [-checksum] [-commitchecksum] [-commitsize] [-rate <rate>]\n");
  printf("                                                                : schedule asynchronous replication [with checksumming] on a filesystem\n");
  printf("                                                      -checksum : trigger the checksum calculation during the verification process\n");
  printf("                                                -commitchecksum : commit the computed checksum to the MGM\n");
  printf("                                                -commitsize     : commit the file size to the MGM\n");
  printf("                                                -rate <rate>    : restrict the verification speed to <rate> per node\n");

  return (0);
}
