// ----------------------------------------------------------------------
// File: com_rm.cc
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
#include "common/Path.hh"
#include "curses.h"
/*----------------------------------------------------------------------------*/

extern int com_find (char*);

/* Remove a file */
int
com_rm (char* arg1) {
  // split subcommands
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString s1 = subtokenizer.GetToken();
  XrdOucString s2 = subtokenizer.GetToken();
  XrdOucString path;
  XrdOucString option;
  eos::common::Path* cPath = 0;
  XrdOucString in = "mgm.cmd=rm&"; 

  if ( (s1 == "--help")  || (s1 == "-h") ) {
    goto com_rm_usage;
  }

  if (s1 == "-r") {
    option ="r";
    path = s2;
  } else {
    if (s1.beginswith("-")) {
      goto com_rm_usage;
    }
    option ="";
    path = s1;
  }
  

  do {
    XrdOucString param=subtokenizer.GetToken();
    if (param.length()) {
      path += " ";
      path += param;
    } else {
      break;
    }
  } while (1);

  // remove escaped blanks
  while (path.replace("\\ "," ")) {}

  if (!path.length()) {
    goto com_rm_usage;
    
  } else {
    path = abspath(path.c_str());
    in += "mgm.path=";
    in += path;
    in += "&mgm.option=";
    in += option;

    cPath = new eos::common::Path(path.c_str());

    if (cPath->GetSubPathSize() < 2) {
      string s;
      fprintf(stdout,"Do you really want to delete ALL files starting at %s ?\n" , path.c_str());
      fprintf(stdout,"Confirm the deletion by typing => ");
      XrdOucString confirmation="";
      for (int i=0; i<10; i++) {
	confirmation += (int) (9.0 * rand()/RAND_MAX);
      }
      fprintf(stdout,"%s\n", confirmation.c_str());
      fprintf(stdout,"                               => ");
      getline( std::cin, s );
      std::string sconfirmation = confirmation.c_str();
      if ( s == sconfirmation) {
	fprintf(stdout,"\nDeletion confirmed\n");
	in += "&mgm.deletion=deep";
	delete cPath;
      } else {
	fprintf(stdout,"\nDeletion aborted\n");
	global_retc = EINTR;
	delete cPath;
	return (0);
      }
    } else {
      std::string kv;
      XrdOucString subfind = "";
      std::vector<std::string> q1;
      std::vector<std::string> q2;
      std::vector<std::string> q3;

      subfind = "-s -b "; subfind += path;
      int rc = com_find((char*)subfind.c_str());
      command_result_stdout_to_vector(q1);	
      if (CommandEnv) {
	delete CommandEnv; CommandEnv=0;
      }
      
      subfind = "-s --count "; subfind += path;
      rc = com_find((char*)subfind.c_str());
      command_result_stdout_to_vector(q2);
      
      if (CommandEnv) {
	delete CommandEnv; CommandEnv=0;
      }
      
      q3=q1;
      q3.insert(q3.end(), q2.begin(), q2.end());
      
      std::vector<std::string>::const_iterator it;
      
      for (unsigned int i=0; i< q3.size(); i++) {
	XrdOucString line = q3[i].c_str();
	while (line.replace("=",":")) {}
	if ( (line.beginswith("nfiles:")) ) {
	  kv += " ";
	  kv += line.c_str();
	}
	if (line.beginswith("space:")) {
	  kv += " ";
	  int pos = line.find("nbytes:");
	  line.erase(0,pos);
	  kv += line.c_str();
	}
      }
      std::vector<std::string> keyval;
      eos::common::StringConversion::Tokenize(kv,keyval," ");
      std::map<std::string, std::string> key2val;
      std::string key;
      std::string value;
      for (size_t i=0; i< keyval.size(); i++) {
	if (eos::common::StringConversion::SplitKeyValue(keyval[i], key, value)) {
	  key2val[key]=value;
	}
      }
      
      XrdOucString sizestring;
      
      if (isatty(0) && isatty(1) ) {
	
	string s;
	// we ask an annoying security code for more than 10 GB or more than 1000 files ... otherwise just yes/no
	if ( (strtoull(key2val["nfiles"].c_str(),0,10) > 100 ) ||
	     (strtoull(key2val["nbytes"].c_str(),0,10) > (1000ll*1000ll*1000ll*10) ) ) { 
	  fprintf(stderr,"warning: I found %s files and %s directories starting at %s ... are you sure you want to try to delete %s ?\n", key2val["nfiles"].c_str(), key2val["ndirectories"].c_str(), path.c_str(), eos::common::StringConversion::GetReadableSizeString(sizestring, strtoull(key2val["nbytes"].c_str(),0,10), "B"));

	  fprintf(stdout,"Confirm the deletion by typing => ");
	  XrdOucString confirmation="";
	  for (int i=0; i<10; i++) {
	    confirmation += (int) (9.0 * rand()/RAND_MAX);
	  }
	  fprintf(stdout,"%s\n", confirmation.c_str());
	  fprintf(stdout,"                               => ");
	  getline( std::cin, s );
	  std::string sconfirmation = confirmation.c_str();
	  if ( s == sconfirmation) {
	    fprintf(stdout,"\nDeletion confirmed\n");
	    delete cPath;
	  } else {
	    fprintf(stdout,"\nDeletion aborted\n");
	    global_retc = EINTR;
	    delete cPath;
	    return (0);
	  }
	} else {
	  // for the moment accept without question ...
	}
      } else {
	if ( (strtoull(key2val["nfiles"].c_str(),0,10) > 100 ) ||
	     (strtoull(key2val["nbytes"].c_str(),0,10) > (1000ll*1000ll*1000ll*10) ) ) {
	  fprintf(stderr,"error: deletion canceled for safety reasons - you want to delete more than 100 files or 10GB and you are not running on a terminal!\n");
	  global_retc = EPERM;
	  delete cPath;
	  return (0);
	}
      }
    }

    global_retc = output_result(client_user_command(in));
    return (0);
  }

 com_rm_usage:
  fprintf(stdout,"usage: rm [-r] <path>                                                  :  remove file <path>\n");
  fprintf(stdout,"                                                                    -r :  remove recursively on a terminal requiring a security code confirmation\n");
  fprintf(stdout,"                                                                    -r :  remove recursively if not on a terminal upto 100 files and 10 GB, otherwise the command fails\n");
  return (0);
}
