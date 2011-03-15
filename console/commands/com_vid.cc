/*----------------------------------------------------------------------------*/
#include "console/ConsoleMain.hh"
/*----------------------------------------------------------------------------*/

/* VID System listing, configuration, manipulation */
int
com_vid (char* arg1) {
  // split subcommands
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString subcommand = subtokenizer.GetToken();
  
  if ( subcommand == "ls" ) {
    XrdOucString in ="mgm.cmd=vid&mgm.subcmd=ls";
    XrdOucString soption="";
    XrdOucString option="";
    do {
      option = subtokenizer.GetToken();
      if (option.beginswith("-")) {
        option.erase(0,1);
        soption += option;
      }
    } while (option.length());
    
    if (soption.length()) {
      in += "&mgm.vid.option="; in += soption;
    }
    global_retc = output_result(client_admin_command(in));
    return (0);
  }
  
  if ( subcommand == "set" ) {
    XrdOucString in    = "mgm.cmd=vid&mgm.subcmd=set";
    XrdOucString key   = subtokenizer.GetToken();
    if (!key.length()) 
      goto com_vid_usage;

    XrdOucString vidkey="";
    if (key == "membership") {

      XrdOucString uid  = subtokenizer.GetToken();

      if (!uid.length()) 
        goto com_vid_usage;

      vidkey += uid;

      XrdOucString type = subtokenizer.GetToken();

      if (!type.length())
        goto com_vid_usage;

      in += "&mgm.vid.cmd=membership";
      in += "&mgm.vid.source.uid="; in += uid;

      XrdOucString list = "";
      if ( (type == "-uids") ) {
        vidkey += ":uids";
        list = subtokenizer.GetToken();
        in += "&mgm.vid.key="; in += vidkey;
        in += "&mgm.vid.target.uid="; in += list;
      }

      if ( (type == "-gids") ) {
        vidkey += ":gids";
        list = subtokenizer.GetToken();
        in += "&mgm.vid.key="; in += vidkey;
        in += "&mgm.vid.target.gid="; in += list;
      }
      
      if ( (type == "+sudo") ) {
        vidkey += ":root";
        list = " "; // fake
        in += "&mgm.vid.key="; in += vidkey;
        in += "&mgm.vid.target.sudo=true";
      }

      if ( (type == "-sudo") ) {
        vidkey += ":root";
        list = " "; // fake
        in += "&mgm.vid.key="; in += vidkey;
        in += "&mgm.vid.target.sudo=false";
      }
      if (!list.length()) {
        goto com_vid_usage;
      }
      global_retc = output_result(client_admin_command(in));
      return (0);
    }
    
    if (key == "map") {
      in += "&mgm.vid.cmd=map";
      XrdOucString type = subtokenizer.GetToken();

      if (!type.length())
        goto com_vid_usage;

      bool hastype=false;
      if ( (type == "-krb5") )  {     
        in += "&mgm.vid.auth=krb5";
        hastype=true;
      }
      if ( (type == "-ssl") ) {
        in += "&mgm.vid.auth=ssl";
        hastype=true;
      }
      if ( (type == "-sss") ) {
        in += "&mgm.vid.auth=sss";
        hastype=true;
      }
      if ( (type == "-unix") ) {
        in += "&mgm.vid.auth=unix";
        hastype=true;
      }
      if ( (type == "-tident") ) {
        in += "&mgm.vid.auth=tident";
        hastype=true;
      }

      if (!hastype) 
        goto com_vid_usage;
      

      XrdOucString pattern = subtokenizer.GetToken();
      // deal with patterns containing spaces but inside ""
      if (pattern.beginswith("\"")) {
        if (!pattern.endswith("\""))
          do {
            XrdOucString morepattern = subtokenizer.GetToken();

            if (morepattern.endswith("\"")) {
              pattern += " ";
              pattern += morepattern;
              break;
            }
            if (!morepattern.length()) {
              goto com_vid_usage;
            }
            pattern += " ";
            pattern += morepattern;
          } while (1);
      }
      if (!pattern.length()) 
        goto com_vid_usage;

      in += "&mgm.vid.pattern="; in += pattern;

      XrdOucString vid= subtokenizer.GetToken();
      if (!vid.length())
        goto com_vid_usage;

      if (vid.beginswith("vuid:")) {
        vid.replace("vuid:","");
        in += "&mgm.vid.uid=";in += vid;        
        
        XrdOucString vid = subtokenizer.GetToken();
        if (vid.length()) {
          fprintf(stderr,"Got %s\n", vid.c_str());
          if (vid.beginswith("vgid:")) {
            vid.replace("vgid:","");
            in += "&mgm.vid.gid=";in += vid;
          } else {
            goto com_vid_usage;
          }
        }
      } else {
        if (vid.beginswith("vgid:")) {
          vid.replace("vgid:","");
          in += "&mgm.vid.gid=";in += vid;
        } else {
          goto com_vid_usage;
        }
      }

      in += "&mgm.vid.key="; in += "<key>";

      global_retc = output_result(client_admin_command(in));
      return (0);
    }
  }

  if ( subcommand == "rm" ) {
    XrdOucString in ="mgm.cmd=quota&mgm.subcmd=rm";
    XrdOucString key   = subtokenizer.GetToken();
    if ( (!key.length()) ) 
      goto com_vid_usage;
    in += "&mgm.vid.key="; in += key;
    
    global_retc = output_result(client_admin_command(in));
    return (0);
  }
  
 com_vid_usage:
  printf("usage: vid ls [-u] [-g] [s] [-U] [-G]                                                               : list configured policies\n");
  printf("                                        -u : show only user role mappings\n");
  printf("                                        -g : show only group role mappings\n");
  printf("                                        -s : show list of sudoers\n");
  printf("                                        -U : show user alias mapping\n");
  printf("                                        -G : show groupalias mapping\n");
  printf("usage: vid set membership <uid> -uids [<uid1>,<uid2>,.com_attr..]\n");
  printf("       vid set membership <uid> -gids [<gid1>,<gid2>,...]\n");
  printf("       vid set membership <uid> [+|-]sudo \n");
  printf("       vid set map -krb5|-ssl|-sss|-unix|-tident <pattern> [vuid:<uid>] [vgid:<gid>] \n");
  printf("usage: vid rm <key>                                                                                 : remove configured vid with name key - hint: use config dump to see the key names of vid rules\n");

  return (0);
}
