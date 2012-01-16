// ----------------------------------------------------------------------
// File: com_fs.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

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
  bool temp_silent=false;

  if ( subcommand == "add" ) {
    XrdOucString in ="mgm.cmd=fs&mgm.subcmd=add";
    XrdOucString uuid="";
    XrdOucString manual      = subtokenizer.GetToken();
    XrdOucString fsid        = "";
    if ( (manual == "-m") || (manual == "--manual") ) {
      fsid = subtokenizer.GetToken();
      if (fsid.length()) {
        int ifsid= atoi(fsid.c_str());  
        if (ifsid == 0)
          goto com_fs_usage;
        uuid = subtokenizer.GetToken();
      } else {
        goto com_fs_usage;
      }
    } else {
      uuid = manual;
    }

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
    
    if (fsid.length()) {
      in += "&mgm.fs.fsid=";
      in += fsid;
    }

    in += "&mgm.fs.uuid=";        in += uuid;
    in += "&mgm.fs.node=";        in += hostport;
    in += "&mgm.fs.mountpoint=";  in += mountpoint;
    in += "&mgm.fs.space=";       in += space;
    in += "&mgm.fs.configstatus=";in += configstatus;
    
    XrdOucEnv* result = client_admin_command(in);
    global_retc = output_result(result);
    
    return (0);
  }

  if ( subcommand == "mv" ) {
    XrdOucString in ="mgm.cmd=fs&mgm.subcmd=mv";
    XrdOucString fsid        = subtokenizer.GetToken();
    XrdOucString space       = subtokenizer.GetToken();
    if ( (!fsid.length()) || (!space.length()) )
      goto com_fs_usage;
    in += "&mgm.fs.id="; in += fsid;
    in += "&mgm.space="; in += space;

    XrdOucEnv* result = client_admin_command(in);
    global_retc = output_result(result);
    return (0);
  }  
  
  if ( subcommand == "ls" ) {
    XrdOucString in ="mgm.cmd=fs&mgm.subcmd=ls";
    XrdOucString option="";
    bool highlighting=true;
    bool ok;
    bool sel=false;

    do {
      ok=false;
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
        if (option == "--io") {
          in += "&mgm.outformat=io";
          ok=true;
        }
        if ( (option == "--drain") || (option == "-d")) {
          in += "&mgm.outformat=d";
          ok=true;
        }
        if (option == "-s") {
          temp_silent=true;
          ok=true;
        }
        if (option == "-e") {
          in += "&mgm.outformat=e";
          ok=true;
        }
        if (!option.beginswith("-")) {
          in += "&mgm.selection=";
          in += option;
          if (!sel)
            ok=true;
          sel=true;
        }
      } else {
        ok=true;
      }
      
      if (!ok) 
        goto com_fs_usage;

    } while(option.length());

    XrdOucEnv* result = client_admin_command(in);
    if (!silent && (!temp_silent)) {
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
      XrdOucString mp = subtokenizer.GetToken();
      XrdOucString hostport = arg;

      if (!mp.length()) {
        mp = arg;
        hostport = XrdSysDNS::getHostName();
      }
      if (!(hostport.find(":")!= STR_NPOS)) {
        in += ":1095";
        hostport += ":1095";
      }

      if (!hostport.beginswith("/eos/")) {
        hostport.insert("/eos/",0);
        hostport.append("/fst");
      }
      in += "&mgm.fs.node=";
      in += hostport;

      in += "&mgm.fs.mountpoint=";
      if (!mp.length())
        goto com_fs_usage;

      while(mp.endswith("/")) {mp.erase(mp.length()-1);}
      in += mp;

      if (access(mp.c_str(), R_OK | X_OK)) {
        goto com_fs_usage;
      }
    }

    global_retc = output_result(client_admin_command(in));
    return (0);
  }


  if ( subcommand == "dropdeletion" ) {
    XrdOucString arg = subtokenizer.GetToken();
    if (!arg.c_str()) {
      goto com_fs_usage;
    }
    XrdOucString in = "mgm.cmd=fs&mgm.subcmd=dropdeletion";
    int fsid = atoi(arg.c_str());
    char r1fsid[128]; sprintf(r1fsid,"%d", fsid);
    char r2fsid[128]; sprintf(r2fsid,"%04d", fsid);
    if ( (arg == r1fsid) || (arg == r2fsid) ) {
      // boot by fsid
      in += "&mgm.fs.id=";
      in += arg;
    } else {
      goto com_fs_usage;
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

  if ( subcommand == "status" ) {
    XrdOucString arg = subtokenizer.GetToken();
    XrdOucString in = "mgm.cmd=fs&mgm.subcmd=status";
    if (!arg.length()) 
      goto com_fs_usage;
    int fsid = atoi(arg.c_str());
    char r1fsid[128]; sprintf(r1fsid,"%d", fsid);
    char r2fsid[128]; sprintf(r2fsid,"%04d", fsid);
    if ( (arg == r1fsid) || (arg == r2fsid) ) {
      // status by fsid
      in += "&mgm.fs.id=";
      in += arg;
    } else {
      XrdOucString arg2 = subtokenizer.GetToken();
      XrdOucString mp = arg;
      if (!arg2.length()) {
        // status by mount point
        char* HostName = XrdSysDNS::getHostName();
        in += "&mgm.fs.node=";
        in += HostName;
        in += "&mgm.fs.mountpoint=";
        in += arg;
        mp = arg;
      } else {
        in += "&mgm.fs.node=";
        in += arg;
        in += "&mgm.fs.mountpoint=";
        in += arg2;
        mp = arg2;
      }

      while(mp.endswith("/")) {mp.erase(mp.length()-1);}
      if (access(mp.c_str(), R_OK | X_OK)) {
        goto com_fs_usage;
      }
    }

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


  printf("'[eos] fs ..' provides the filesystem interface of EOS.\n");
  printf("Usage: fs add|boot|config|dropfiles|dumpmd|mv|ls|rm|status [OPTIONS]\n");
  printf("Options:\n");
  printf("fs ls [-m] [-l] [-e] [--io] [-d|--drain] [-s]   :\n"); 
  printf("                                                  list all filesystems in default output format\n");
  printf("            -m                                  : list all filesystem parameters in monitoring format\n");
  printf("            -l                                  : display all filesystem parameters in long format\n");
  printf("            -e                                  : display all filesystems in error state\n");
  printf("            --io                                : display all filesystems in IO output format\n");
  printf("            -d,--drain                          : display all filesystems in drain or draindead status with drain progress and statistics\n");

  printf("fs add [-m|--manual <fsid>] <uuid> <node-queue>|<host>[:<port>] <mountpoint> [<schedgroup>] [<status] :\n");
  printf("                                                  add a filesystem and dynamically assign a filesystem id based on the unique identifier for the disk <uuid>\n");
  printf("            -m,--manual <fsid>                  : add with user specified <fsid> and <schedgroup> - no automatic assignment\n");
  printf("            <fsid>                              : numeric filesystem id 1..65535\n");
  printf("            <uuid>                              : arbitrary string unique to this particular filesystem\n");
  printf("            <node-queue>                        : internal EOS identifier for a node,port,mountpoint description ... /eos/<host>:<port>/fst e.g. /eos/myhost.cern.ch:1095/fst [you should prefer the host:port syntax]\n");
  printf("            <host>                              : fully qualified hostname where the filesystem is mounted\n");
  printf("            <port>                              : port where xrootd is running on the FST [normally 1095]\n");
  printf("            <mountpoint>                        : local path of the mounted filesystem e.g. /data\n");
  printf("            <schedgroup>                        : scheduling group where the filesystem should be inserted ... default is 'default'\n");
  printf("            <status>                            : file system status after the insert ... default is 'off', in most cases should be 'rw'\n");

  printf("fs mv <src-fsid|src-space> <dst-schedgroup|dst-space> :\n");
  printf("                                                  move a filesystem into a different scheduling group\n");
  printf("            <src-fsid>                          : source filesystem id\n");
  printf("            <src-space>                         : source space\n");
  printf("            <dst-schedgroup>                    : destination scheduling group\n");
  printf("            <dst-space>                         : destination space\n");
  printf("If the source is a <space> a filesystem will be chosen to fit into the destionation group or space.\n");
  printf("If the target is a <space> : a scheduling group is auto-selected where the filesystem can be placed.\n\n");


  printf("fs config <host>:<port><path>|<fsid>|<uuid> <key>=<value> :\n");
  printf("                                                   configure filesystem parameter for a single filesystem identified by host:port/path, filesystem id or filesystem UUID.\n");

  printf("fs config <fsid> configstatus=rw|wo|ro|drain|off :\n");
  printf("                    <status> can be \n");
  printf("                                    rw          : filesystem set in read write mode\n");
  printf("                                    wo          : filesystem set in write-once mode\n");
  printf("                                    ro          : filesystem set in read-only mode\n");
  printf("                                    drain       : filesystem set in drain mode\n");
  printf("                                    off         : filesystem set disabled\n"); 
  printf("                                    empty       : filesystem is set to empty - possible only if there are no files stored anymore"); 
  printf("fs config <fsid> headroom=<size>\n");
  printf("                    <size> can be (>0)[BMGT]    : the headroom to keep per filesystem (e.g. you can write '1G' for 1 GB)\n");
  printf("fs config <fsid> scaninterval=<seconds>: \n");
  printf("                                                  configures a scanner thread on each FST to recheck the file & block checksums of all stored files every <seconds> seconds. 0 disables the scanning.\n\n");
  printf("fs config <fsid> graceperiod=<seconds> :\n");
  printf("                                                  grace period before a filesystem with an operation error get's automatically drained\n");
  printf("fs config <fsid> drainperiod=<seconds> : \n");
  printf("                                                  drain period a drain job is waiting to finish the drain procedure\n");
  printf("\n");
  printf("fs rm    <fs-id>|<node-queue>|<mount-point>|<hostname> <mountpoint> :\n");
  printf("                                                  remove filesystem configuration by various identifiers\n");
  printf("\n");
  printf("fs boot  <fs-id>|<node-queue>|* :\n");
  printf("                                                  boot filesystem with ID <fs-id> or name <node-queue> or all (*)\n");
  printf("\n");
  printf("fs dropfiles <fs-id> [-f] :\n");
  printf("                                                  allows to drop all files on <fs-id> - force\n");
  printf("                                                                  -f    : unlinks/removes files at the time from the NS (you have to cleanup or remove the files from disk) \n");
  printf("\n");
  printf("fs dumpmd [-s] <fs-id> [-fid] [-path] :\n");
  printf("                                                  dump all file meta data on this filesystem in query format\n");

  printf("                                                                  -s    : don't printout keep an internal reference\n");
  printf("                                                                  -fid  : dump only a list of file id's stored on this filesystem\n");
  printf("                                                                  -path : dump only a list of file names stored on this filesystem\n");


  printf("\n");
  printf("fs status <fs-id> :\n");
  printf("                                                  returns all status variables of a filesystem and calculates the risk of data loss if this filesystem get's removed\n"); 
  printf("fs status <mount-point> :\n");
  printf("                                                  as before but accepts the mount point as input parameters and set's host=<this host>\n");

  printf("fs status <host> <mount-point> :\n");
  printf("                                                  as before but accepts the mount point and hostname as input parameters\n");
  printf("Examples:\n");
  printf("  fs ls --io             List all filesystems with IO statistics\n\n");
  printf("  fs boot *              Send boot request to all filesystems\n\n");
  printf("  fs dumpmd 100 -path    Dump all logical path names on filesystem 100\n\n");
  printf("  fs mv spare default    Move one filesystem from the sapre space into the default space. If default has subgroups the smallest subgroup is selected.\n\n");
  printf("  fs mv 100 default.0    Move filesystem 100 into scheduling group default.0\n\n");
  printf("Report bugs to eos-dev@cern.ch.\n");
  return (0);
}
