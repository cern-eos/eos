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
      if ( (type == "-gsi") ) {
        in += "&mgm.vid.auth=gsi";
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

  if ( (subcommand == "enable") || (subcommand == "disable") ) {
    XrdOucString in    = "mgm.cmd=vid&mgm.subcmd=set&mgm.vid.cmd=map";
    XrdOucString disableu ="mgm.cmd=vid&mgm.subcmd=rm&mgm.vid.cmd=unmap&mgm.vid.key=";
    XrdOucString disableg ="mgm.cmd=vid&mgm.subcmd=rm&mgm.vid.cmd=unmap&mgm.vid.key=";
    XrdOucString type   = subtokenizer.GetToken();
    if (!type.length()) 
      goto com_vid_usage;

    bool hastype=false;
    if ( (type == "krb5") )  {     
      in += "&mgm.vid.auth=krb5";
      disableu +="krb5:\"<pwd>\":uid";
      disableg +="krb5:\"<pwd>\":gid";
      hastype=true;
    }
    if ( (type == "ssl") ) {
      in += "&mgm.vid.auth=ssl";
      disableu +="ssl:\"<pwd>\":uid";
      disableg +="ssl:\"<pwd>\":gid";
      hastype=true;
    }
    if ( (type == "sss") ) {
      in += "&mgm.vid.auth=sss";
      disableu +="sss:\"<pwd>\":uid";
      disableg +="sss:\"<pwd>\":guid";
      hastype=true;
    }
    if ( (type == "gsi") ) {
      in += "&mgm.vid.auth=gsi";
      disableu +="gsi:\"<pwd>\":uid";
      disableg +="gsi:\"<pwd>\":gid";
      hastype=true;
    }
    if ( (type == "unix") ) {
      in += "&mgm.vid.auth=unix";
      disableu +="unix\"<pwd>\":uid";
      disableg +="unix\"<pwd>\":gid";
      hastype=true;
    }
    if ( (type == "tident") ) {
      in += "&mgm.vid.auth=tident";
      disableu +="tident\"<pwd>\":uid";
      disableg +="tident\"<pwd>\":gid";
      hastype=true;
    }
    if (!hastype) 
      goto com_vid_usage;

   in += "&mgm.vid.pattern=<pwd>";
    if (type != "unix") {
      in += "&mgm.vid.uid=0";
      in += "&mgm.vid.gid=0";
    } else {
      in += "&mgm.vid.uid=99";
      in += "&mgm.vid.gid=99";
    }

    in += "&mgm.vid.key="; in += "<key>";
    
    if ( (subcommand == "enable") )
      global_retc = output_result(client_admin_command(in));
    
    if ( (subcommand == "disable") ) {
      global_retc = output_result(client_admin_command(disableu));
      global_retc |= output_result(client_admin_command(disableg));
    }
    return (0);    
  }

  if ( (subcommand == "add") || (subcommand == "remove") ) {
    XrdOucString gw   = subtokenizer.GetToken();
    if (gw != "gateway") 
      goto com_vid_usage;
    XrdOucString host = subtokenizer.GetToken();
    if (!host.length()) 
      goto com_vid_usage;

    XrdOucString in    = "mgm.cmd=vid&mgm.subcmd=set&mgm.vid.cmd=map";
    XrdOucString disableu ="mgm.cmd=vid&mgm.subcmd=rm&mgm.vid.cmd=unmap&mgm.vid.key=";
    XrdOucString disableg ="mgm.cmd=vid&mgm.subcmd=rm&mgm.vid.cmd=unmap&mgm.vid.key=";
    
    in += "&mgm.vid.auth=tident";
    in += "&mgm.vid.pattern=\"*@";in += host; in += "\"";
    in += "&mgm.vid.uid=0";
    in += "&mgm.vid.gid=0";
    disableu +="tident:\"*@";disableu += host; disableu += "\":uid";
    disableg +="tident:\"*@";disableg += host; disableg += "\":gid";

    in += "&mgm.vid.key="; in += "<key>";    

    if ( (subcommand == "add") )
      global_retc = output_result(client_admin_command(in));
    
    if ( (subcommand == "remove") ) {
      global_retc = output_result(client_admin_command(disableu));
      global_retc |= output_result(client_admin_command(disableg));
    }
    return (0);
  }

  if ( subcommand == "rm" ) {
    XrdOucString in ="mgm.cmd=vid&mgm.subcmd=rm";
    XrdOucString key   = subtokenizer.GetToken();
    if ( (!key.length()) ) 
      goto com_vid_usage;
    in += "&mgm.vid.key="; in += key;
    
    global_retc = output_result(client_admin_command(in));
    return (0);
  }
  
 com_vid_usage:
  printf("usage: vid ls [-u] [-g] [-s] [-U] [-G] [-g] [-a]                                                    : list configured policies\n");
  printf("                                        -u : show only user role mappings\n");
  printf("                                        -g : show only group role mappings\n");
  printf("                                        -s : show list of sudoers\n");
  printf("                                        -U : show user  alias mapping\n");
  printf("                                        -G : show group alias mapping\n");
  printf("                                        -y : show configured gateways\n");
  printf("                                        -a : show authentication\n");
  printf("\n");
  printf("       vid set membership <uid> -uids [<uid1>,<uid2>,.com_attr..]\n");
  printf("       vid set membership <uid> -gids [<gid1>,<gid2>,...]\n");
  printf("       vid set membership <uid> [+|-]sudo \n");
  printf("       vid set map -krb5|-gsi|-ssl|-sss|-unix|-tident <pattern> [vuid:<uid>] [vgid:<gid>] \n");
  printf("\n");
  printf("       vid rm <key>                                                                                 : remove configured vid with name key - hint: use config dump to see the key names of vid rules\n");
  printf("\n");
  printf("       vid enable|disable krb5|gsi|ssl|sss|unix\n");
  printf("                                           : enable/disables the default mapping via password database\n");
  printf("\n");
  printf("       vid add|remove gateway <hostname>\n");
  printf("                                             adds/removes a host as a (fuse) gateway with 'su' priviledges\n");


  return (0);
}
