/*----------------------------------------------------------------------------*/
#include "console/ConsoleMain.hh"
/*----------------------------------------------------------------------------*/


using namespace eos::common;

/* File handling */

/* Get file information */
int
com_fileinfo (char* arg1) {
  // split subcommands
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString path = subtokenizer.GetToken();
  XrdOucString option = "";
  do {
    XrdOucString newoption = subtokenizer.GetToken();
    if (!newoption.length()) {
      break;
    } else {
      option+= newoption;
    }
  } while(1);

  XrdOucString in = "mgm.cmd=fileinfo&"; 
  if (!path.length()) {
    goto com_fileinfo_usage;
    
  } else {
    if (path.beginswith("-")) {
      goto com_fileinfo_usage;
    }
    if ((!path.beginswith("fid:"))&&(!path.beginswith("fxid:")))
      path = abspath(path.c_str());
    in += "mgm.path=";
    in += path;
    if (option.length()) {
      in += "&mgm.file.info.option=";
      in += option;
    }

    global_retc = output_result(client_user_command(in));
    return (0);
  }

 com_fileinfo_usage:
  printf("usage: fileinfo <path> [--path] [--fxid] [--fid] [--size] [--checksum] [--fullpath] [-m]   :  print file information for <path>\n");
  printf("       fileinfo fxid:<fid-hex>                                           :  print file information for fid <fid-hex>\n");
  printf("       fileinfo fid:<fid-dec>                                            :  print file information for fid <fid-dec>\n");
  printf("                                                                 --path  :  selects to add the path information to the output\n");
  printf("                                                                 --fxid  :  selects to add the hex file id information to the output\n");
  printf("                                                                 --fid   :  selects to add the base10 file id information to the output\n");
  printf("                                                                 --size  :  selects to add the size information to the output\n");
  printf("                                                              --checksum :  selects to add the checksum information to the output\n");
  printf("                                                              --fullpath :  selects to add the full path information to each replica\n");
  printf("                                                                  -m     :  print single line in monitoring format\n");
  return (0);

}

int 
com_file (char* arg1) {
  XrdOucString arg = arg1;

  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString cmd = subtokenizer.GetToken();
  XrdOucString path = subtokenizer.GetToken();
  XrdOucString fsid1 = subtokenizer.GetToken();
  XrdOucString fsid2 = subtokenizer.GetToken();

  path = abspath(path.c_str());

  XrdOucString in = "mgm.cmd=file";
  if ( ( cmd != "drop") && ( cmd != "move") && ( cmd != "replicate" ) && (cmd != "check") && ( cmd != "adjustreplica" ) && ( cmd != "info" ) && (cmd != "layout") && (cmd != "verify")) {
    goto com_file_usage;
  }

  // convenience function
  if (cmd == "info") {
    arg.replace("info ","");
    return com_fileinfo((char*) arg.c_str());
  }

  if (cmd == "drop") {
    if ( !path.length() || !fsid1.length()) 
      goto com_file_usage;

    in += "&mgm.subcmd=drop";
    in += "&mgm.path="; in += path;
    in += "&mgm.file.fsid="; in += fsid1;

    if (fsid2 == "-f") {
      in += "&mgm.file.force=1";
    } else {
      if (fsid2.length()) 
        goto com_file_usage;
    }
  }
  
  if (cmd == "move") {
    if ( !path.length() || !fsid1.length() || !fsid2.length() )
      goto com_file_usage;
    in += "&mgm.subcmd=move";
    in += "&mgm.path="; in += path;
    in += "&mgm.file.sourcefsid="; in += fsid1;
    in += "&mgm.file.targetfsid="; in += fsid2;
  }

  if (cmd == "replicate") {
    if ( !path.length() || !fsid1.length() || !fsid2.length() )
      goto com_file_usage;
    in += "&mgm.subcmd=replicate";
    in += "&mgm.path="; in += path;
    in += "&mgm.file.sourcefsid="; in += fsid1;
    in += "&mgm.file.targetfsid="; in += fsid2;
  }

  if (cmd == "adjustreplica") { 
    if (!path.length())
      goto com_file_usage;

    in += "&mgm.subcmd=adjustreplica";
    in += "&mgm.path="; in += path;
    if (fsid1.length()) {
      in += "&mgm.file.desiredspace="; in += fsid1;
      if (fsid2.length()) {
        in += "&mgm.file.desiredsubgroup="; in += fsid2;
      }
    }
  }

  if (cmd == "layout") {
    if (!path.length())
      goto com_file_usage;

    in += "&mgm.subcmd=layout";
    in += "&mgm.path="; in += path;
    if (fsid1 != "-stripes")
      goto com_file_usage;
    if (!fsid2.length()) 
      goto com_file_usage;

    in += "&mgm.file.layout.stripes=";
    in += fsid2;
  }

  if (cmd == "verify") {
    if (!path.length())
      goto com_file_usage;

    XrdOucString option[6];

    in += "&mgm.subcmd=verify";
    in += "&mgm.path="; in += path;
    if (fsid1.length()) {
      if  ( (fsid1 != "-checksum") && (fsid1 != "-commitchecksum") && (fsid1 != "-commitsize") && (fsid1 != "-commitfmd") && (fsid1 != "-rate")) {
        if (fsid1.beginswith("-"))
          goto com_file_usage;

        in += "&mgm.file.verify.filterid="; in += fsid1;
        if (fsid2.length()) {
          option[0] = fsid2;
          option[1] = subtokenizer.GetToken();
          option[2] = subtokenizer.GetToken();
          option[3] = subtokenizer.GetToken();
          option[4] = subtokenizer.GetToken();
	  option[5] = subtokenizer.GetToken();
        }
      } else {
        option[0] = fsid1;
        option[1] = fsid2;
        option[2] = subtokenizer.GetToken();
        option[3] = subtokenizer.GetToken();
        option[4] = subtokenizer.GetToken();
        option[5] = subtokenizer.GetToken();
      }
    }

    for (int i=0; i< 6; i++) {
      if (option[i].length()) {
        if (option[i] == "-checksum") {
          in += "&mgm.file.compute.checksum=1";
        } else {
          if (option[i] == "-commitchecksum") {
            in += "&mgm.file.commit.checksum=1";
          } else {
            if (option[i] == "-commitsize") {
              in += "&mgm.file.commit.size=1";
            } else {
	      if (option[i] == "-commitfmd") {
		in += "&mgm.file.commit.fmd=1";
	      } else {
		if (option[i] == "-rate") {
		  in += "&mgm.file.verify.rate=";
		  if ( (i==5) || (!option[i+1].length()))
		    goto com_file_usage;
		  in += option[i+1];
		  i++;
		  continue;
		} else {
		  goto com_file_usage;
		}
	      }
            }
          }
        }
      }
    }
  }

  if (cmd == "check") { 
    if (!path.length())
      goto com_file_usage;

    in += "&mgm.subcmd=getmdlocation";
    in += "&mgm.path="; in += path;

    XrdOucString option = fsid1;

    XrdOucEnv* result = client_user_command(in);

    if (!result) {
      fprintf(stderr,"error: getmdlocation query failed\n");
      return EINVAL;
    }    
    int envlen=0;

    XrdOucEnv* newresult = new XrdOucEnv(result->Env(envlen));

    delete result;

    bool consistencyerror=false;
    bool down=false;
    if (!newresult->Get("mgm.proc.stderr")) {

      XrdOucString checksumtype = newresult->Get("mgm.checksumtype");
      XrdOucString checksum =  newresult->Get("mgm.checksum");
      XrdOucString size = newresult->Get("mgm.size");
      
      if ( (option.find("%silent") == STR_NPOS) && (!silent) ) {
        printf("path=%-32s fid=%s size=%s nrep=%s checksumtype=%s checksum=%s\n", path.c_str(), newresult->Get("mgm.fid0"), size.c_str(), newresult->Get("mgm.nrep"), checksumtype.c_str(), newresult->Get("mgm.checksum"));
      }

      int i=0;
      XrdOucString inconsistencylable ="";
      int nreplicaonline = 0;
          
      for (i=0; i< LayoutId::kSixteenStripe; i++) {
        XrdOucString repurl  = "mgm.replica.url"; repurl += i;
        XrdOucString repfid  = "mgm.fid"; repfid += i;
        XrdOucString repfsid = "mgm.fsid"; repfsid += i;
        XrdOucString repbootstat = "mgm.fsbootstat"; repbootstat += i;

        if ( newresult->Get(repurl.c_str()) ) {
          // Query 
          ClientAdmin* admin = CommonClientAdminManager.GetAdmin(newresult->Get(repurl.c_str()));
          XrdOucString bs = newresult->Get(repbootstat.c_str());
          if (bs != "booted") {
            down = true;
          }  else {
            down = false;
          }

          if (!admin) {
            fprintf(stderr,"error: unable to get admin\n");
            return ECOMM;
          }
          struct Fmd::FMD fmd;
          int retc=0;

          int oldsilent=silent;

          if ( (option.find("%silent"))!= STR_NPOS ) {
            silent = true;
          }

          if ( down && ((option.find("%force"))== STR_NPOS ))  {
            consistencyerror = true;
            if (!silent) fprintf(stderr,"error: unable to retrieve file meta data from %s [ status=%s ]\n",  newresult->Get(repurl.c_str()),bs.c_str());
            inconsistencylable ="DOWN";
          } else {
            //      fprintf(stderr,"%s %s %s\n",newresult->Get(repurl.c_str()), newresult->Get(repfid.c_str()),newresult->Get(repfsid.c_str()));
            if ((retc=gFmdHandler.GetRemoteFmd(admin, newresult->Get(repurl.c_str()), newresult->Get(repfid.c_str()),newresult->Get(repfsid.c_str()), fmd))) {
              if (!silent)fprintf(stderr,"error: unable to retrieve file meta data from %s [%d]\n",  newresult->Get(repurl.c_str()),retc);
	      consistencyerror = true;
	      inconsistencylable="NOFMD";
            } else {
              XrdOucString cx="";
              for (unsigned int k=0; k< SHA_DIGEST_LENGTH; k++) {
                // the adler and crc32 functions are not bytewise but derived from int byte order
                if ( ((checksumtype == "adler") || (checksumtype == "crc32") || (checksumtype == "crc32c"))  && (k<4) ) {
                  char hb[3]; sprintf(hb,"%02x", (unsigned char) (fmd.checksum[3-k]));
                  cx += hb;
                } else {
                  char hb[3]; sprintf(hb,"%02x", (unsigned char) (fmd.checksum[k]));
                  cx += hb;
                }
              }
              
              if ( (option.find("%size"))!= STR_NPOS ) {
                char ss[1024]; sprintf(ss,"%llu", fmd.size);
                XrdOucString sss = ss;
                if (sss != size) {
                  consistencyerror = true;
                  inconsistencylable ="SIZE";
                }
              }
              
              if ( (option.find("%checksum")) != STR_NPOS ) {
                if (cx != checksum) { 
                  consistencyerror = true;
                  inconsistencylable ="CHECKSUM";
                }
              }
              nreplicaonline++;
                      
              if (!silent)printf("nrep=%02d fsid=%lu size=%llu checksum=%s\n", i, fmd.fsid, fmd.size, cx.c_str());
            }
          }

          if ( (option.find("%silent"))!= STR_NPOS ) {
            silent = oldsilent;
          }
        } else {
          break;
        }
      }

      if ( (option.find("%nrep")) != STR_NPOS ) {
        int nrep = 0; 
        int stripes = 0;
        if (newresult->Get("mgm.stripes")) { stripes = atoi (newresult->Get("mgm.stripes"));}
        if (newresult->Get("mgm.nrep"))    { nrep = atoi (newresult->Get("mgm.nrep"));}
        if (nrep != stripes) {
          consistencyerror = true;
	  if (inconsistencylable != "NOFMD") {
	    inconsistencylable ="REPLICA";
	  }
        }
      }
      
      if ( (option.find("%output"))!= STR_NPOS ) {
        if (consistencyerror)
          printf("INCONSISTENCY %s path=%-32s fid=%s size=%s stripes=%s nrep=%s nrepstored=%d nreponline=%d checksumtype=%s checksum=%s\n", inconsistencylable.c_str(), path.c_str(), newresult->Get("mgm.fid0"), size.c_str(), newresult->Get("mgm.stripes"), newresult->Get("mgm.nrep"), i, nreplicaonline, checksumtype.c_str(), newresult->Get("mgm.checksum"));
      }

      delete newresult;
    } else {
      fprintf(stderr,"error: %s",newresult->Get("mgm.proc.stderr"));
    }
    return (consistencyerror);
  }

  global_retc = output_result(client_user_command(in));
  return (0);

 com_file_usage:
  printf("Usage: file drop|move|replicate|adjustreplica|check|info|layout|verify ...\n");
  printf("'[eos] file ..' provides the file management interface of EOS.\n");
  printf("Options:\n");
  printf("file drop <path> <fsid> [-f] :\n");
  printf("                                                  drop the file <path> from <fsid> - force removes replica without trigger/wait for deletion (used to retire a filesystem) \n");
  printf("file move <path> <fsid1> <fsid2> :\n");
  printf("                                                  move the file <path> from  <fsid1> to <fsid2>\n");
  printf("file replicate <path> <fsid1> <fsid2> :\n");
  printf("                                                  replicate file <path> part on <fsid1> to <fsid2>\n");
  printf("file adjustreplica <path>|fid:<fid-dec>|fxid:<fid-hex> [space [subgroup]] :\n");
  printf("                                                  tries to bring a files with replica layouts to the nominal replica level [ need to be root ]\n");
  printf("file check <path> [%%size%%checksum%%nrep%%force%%output%%silent] :\n");
  printf("                                                  retrieves stat information from the physical replicas and verifies the correctness\n");
  printf("       - %%size                                                       :  return with an error code if there is a mismatch between the size meta data information\n");
  printf("       - %%checksum                                                   :  return with an error code if there is a mismatch between the checksum meta data information\n");
  printf("       - %%nrep                                                       :  return with an error code if there is a mismatch between the layout number of replicas and the existing replicas\n");
  printf("       - %%silent                                                     :  suppresses all information for each replic to be printed\n");
  printf("       - %%force                                                      :  forces to get the MD even if the node is down\n");
  printf("       - %%output                                                     :  prints lines with inconsitency information\n");
  printf("file info <path> :\n");
  printf("                                                  convenience function aliasing to 'fileinfo' command\n");
  printf("file layout <path>|fid:<fid-dec>|fxid:<fid-hex>  -stripes <n> :\n");
  printf("                                                  change the number of stripes of a file with replica layout to <n>\n");
  printf("file verify <path>|fid:<fid-dec>|fxid:<fid-hex> [<fsid>] [-checksum] [-commitchecksum] [-commitsize] [-rate <rate>] : \n");
  printf("                                                  verify a file against the disk images\n");
  printf("       <fsid>          : verifies only the replica on <fsid>\n");
  printf("       -checksum       : trigger the checksum calculation during the verification process\n");
  printf("       -commitchecksum : commit the computed checksum to the MGM\n");
  printf("       -commitsize     : commit the file size to the MGM\n");
  printf("       -rate <rate>    : restrict the verification speed to <rate> per node\n");
  return (0);
}

