/*----------------------------------------------------------------------------*/
#include "ConsoleMain.hh"
/*----------------------------------------------------------------------------*/


/* Find files/directories */
int
com_find (char* arg1) {
  // split subcommands
  XrdOucString oarg=arg1;

  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString s1;
  XrdOucString path;
  XrdOucString option="";
  XrdOucString attribute="";
  XrdOucString printkey="";
  XrdOucString filter="";
  XrdOucString stripes="";

  XrdOucString in = "mgm.cmd=find&"; 
  while ( (s1 = subtokenizer.GetToken()).length() && (s1.beginswith("-")) ) {
    if (s1 == "-s") {
      option +="s";
    }
    
    if (s1 == "-d") {
      option +="d";
    }
    
    if (s1 == "-f") {
      option +="f";
    }
    
    if (s1 == "-0") {
      option +="f0";
    }

    if (s1 == "-m") {
      option += "fM";
    }

    if (s1 == "--size") {
      option += "S";
    }


    if (s1 == "--fs") {
      option += "L";
    }

    if (s1 == "--checksum") {
      option += "X";
    }


    if (s1 == "--ctime") {
      option += "C";
    }

    if (s1 == "--mtime") {
      option += "M";
    }

    if (s1 == "--fid") {
      option += "F";
    }

    if (s1 == "--nrep") {
      option += "R";
    }

    if (s1 == "--nunlink") {
      option += "U";
    }

    if (s1 == "--stripediff") {
      option += "D";
    }

    if (s1 == "-1") {
      option += "1";
    }

    if (s1.beginswith( "-h" )) {
      goto com_find_usage;
    }

    if (s1 == "-x") {
      option += "x";

      attribute = subtokenizer.GetToken();

      if (!attribute.length())
	goto com_find_usage;

      if ((attribute.find("&")) != STR_NPOS)
	goto com_find_usage;
    }

    if (s1 == "-c") {

      option += "c";
      
      filter = subtokenizer.GetToken();
      if (!filter.length()) 
	goto com_find_usage;
      
      if ((filter.find("%%")) != STR_NPOS) {
	goto com_find_usage;
      }
    }

    if (s1 == "-layoutstripes") {
      stripes = subtokenizer.GetToken();
      if (!stripes.length()) 
	goto com_find_usage;
    }

    if (s1 == "-p") {
      option += "p";
      
      printkey = subtokenizer.GetToken();
      
      if (!printkey.length()) 
	goto com_find_usage;
    }

    if (s1 == "-b") {
      option += "b";
    }
  }
  
  if (s1.length()) {
    path = s1;
  }

  // the find to change a layout
  if ( (stripes.length()) ) {
    XrdOucString subfind = oarg;
    XrdOucString repstripes= " "; repstripes += stripes; repstripes += " ";
    subfind.replace("-layoutstripes","");
    subfind.replace(repstripes," -f -s ");
    int rc = com_find((char*)subfind.c_str());
    std::vector<std::string> files_found;
    files_found.clear();
    command_result_stdout_to_vector(files_found);
    std::vector<std::string>::const_iterator it;
    unsigned long long cnt=0;
    unsigned long long goodentries=0;
    unsigned long long badentries=0;
    for (unsigned int i=0; i< files_found.size(); i++) {
      if (!files_found[i].length())
	continue;

      XrdOucString cline="layout "; 
      cline += files_found[i].c_str();
      cline += " -stripes "; 
      cline += stripes;
      rc = com_file((char*)cline.c_str());
      if (rc) {
	badentries++;
      } else {
	goodentries++;
      }
      cnt++;
    }
    rc = 0;
    if (!silent) {
      fprintf(stderr,"nentries=%llu good=%llu bad=%llu\n", cnt, goodentries,badentries);
    }
    return 0;
  }

  // the find with consistency check 
  if ( (option.find("c")) != STR_NPOS ) {
    XrdOucString subfind = oarg;
    subfind.replace("-c","-s -f");
    subfind.replace(filter,"");
    int rc = com_find((char*)subfind.c_str());
    std::vector<std::string> files_found;
    files_found.clear();
    command_result_stdout_to_vector(files_found);
    std::vector<std::string>::const_iterator it;
    unsigned long long cnt=0;
    unsigned long long goodentries=0;
    unsigned long long badentries=0;
    for (unsigned int i=0; i< files_found.size(); i++) {
      if (!files_found[i].length())
	continue;

      XrdOucString cline="check "; 
      cline += files_found[i].c_str();
      cline += " "; 
      cline += filter;
      rc = com_file((char*)cline.c_str());
      if (rc) {
	badentries++;
      } else {
	goodentries++;
      }
      cnt++;
    }
    rc = 0;
    if (!silent) {
      fprintf(stderr,"nentries=%llu good=%llu bad=%llu\n", cnt, goodentries,badentries);
    }
    return 0;
  }


  path = abspath(path.c_str());
  in += "mgm.path=";
  in += path;
  in += "&mgm.option=";
  in += option;
  if (attribute.length()) {
    in += "&mgm.find.attribute=";
    in += attribute;
  }
  if (printkey.length()) {
    in += "&mgm.find.printkey=";
    in += printkey;
  }

  XrdOucEnv* result;
  result = client_user_command(in);
  if ( ( option.find("s") ) == STR_NPOS) {
    global_retc = output_result(result);
  } else {
    if (result) {
      global_retc = 0;
    } else {
      global_retc = EINVAL;
    }
  }
  return (0);

 com_find_usage:
  printf("usage: find [-s] [-d] [-f] [-0] [-m] [-x <key>=<val>] [-p <key>] [-b] [-c %%tags] [-layoutstripes <n>] <path>\n");
  printf("                                                                        -f -d :  find files(-f) or directories (-d) in <path>\n");
  printf("                                                               -x <key>=<val> :  find entries with <key>=<val>\n");
  printf("                                                                           -0 :  find 0-size files \n");
  printf("                                                                           -m :  find files with mixed scheduling groups\n");
  printf("                                                                     -p <key> :  additionally print the value of <key> for each entry\n");
  printf("                                                                           -b :  query the server balance of the files found\n");
  printf("                                                                    -c %%tags  :  find all files with inconsistencies defined by %%tags [ see help of 'file check' command]\n");
  printf("                                                                           -s :  run as a subcommand (in silent mode)\n");
  printf("                                                           -layoutstripes <n> :  apply new layout with <n> stripes to all files found\n");
  printf("                                                                           -1 :  find files which are atleast 1 hour old\n");
  printf("                                                                  -stripediff :  find files which have not the nominal number of stripes(replicas)\n");
  printf("                                                                      default :  find files and directories\n");
  printf("       find [--nrep] [--nunlink] [--size] [--fid] [--fs] [--checksum] [--ctime] [--mtime] <path>   :  find files and print out the requested meta data as key value pairs\n");              
  return (0);
}
