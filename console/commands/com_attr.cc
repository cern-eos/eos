/*----------------------------------------------------------------------------*/
#include "console/ConsoleMain.hh"
/*----------------------------------------------------------------------------*/

/* Attribute ls, get, set rm */
int 
com_attr (char* arg1) {
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString subcommand = subtokenizer.GetToken();
  XrdOucString option="";
  XrdOucString in = "mgm.cmd=attr";
  XrdOucString arg = "";

  if (subcommand.beginswith("-")) {
    option = subcommand;
    option.erase(0,1);
    subcommand = subtokenizer.GetToken();
    arg = subtokenizer.GetToken();
    in += "&mgm.option=";
    in += option;
  } else {
    arg = subtokenizer.GetToken();
  }
    
  if ((!subcommand.length()) || (!arg.length()) ||
      ( (subcommand != "ls") && (subcommand != "set") && (subcommand != "get") && (subcommand != "rm") ))
    goto com_attr_usage;

  if ( subcommand == "ls") {
    XrdOucString path  = arg;
    if (!path.length())
      goto com_attr_usage;

    path = abspath(path.c_str());

    in += "&mgm.subcmd=ls";
    in += "&mgm.path=";in += path;
  }

  if ( subcommand == "set") {
    XrdOucString key   = arg;
    XrdOucString value = subtokenizer.GetToken();
    
    if (value.beginswith("\"")) {
      if (!value.endswith("\"")) {
        do {
          XrdOucString morevalue = subtokenizer.GetToken();
          
          if (morevalue.endswith("\"")) {
            value += " ";
            value += morevalue;
            break;
          }
          if (!morevalue.length()) {
            goto com_attr_usage;
          }
          value += " ";
          value += morevalue;
        } while (1);
      }
    }

    XrdOucString path  = subtokenizer.GetToken();
    if (!key.length() || !value.length() || !path.length()) 
      goto com_attr_usage;

    path = abspath(path.c_str());

    in += "&mgm.subcmd=set&mgm.attr.key="; in += key;
    in += "&mgm.attr.value="; in += value;
    in += "&mgm.path="; in += path;
  }

  if ( subcommand == "get") {
    XrdOucString key   = arg;
    XrdOucString path  = subtokenizer.GetToken();
    if (!key.length() || !path.length())
      goto com_attr_usage;
    path = abspath(path.c_str());
    in += "&mgm.subcmd=get&mgm.attr.key="; in += key;
    in += "&mgm.path="; in += path;
  }

  if ( subcommand == "rm") {
    XrdOucString key   = arg;
    XrdOucString path  = subtokenizer.GetToken();
    if (!key.length() || !path.length())
      goto com_attr_usage;
    path = abspath(path.c_str());
    in += "&mgm.subcmd=rm&mgm.attr.key="; in += key;
    in += "&mgm.path="; in += path;
  }
 
  global_retc = output_result(client_user_command(in));
  return (0); 

 com_attr_usage:
  printf("usage: attr [-r] ls <path>                                  : list attributes of path (-r recursive)\n");  
  printf("usage: attr [-r] set <key> <value> <path>                   : set attributes of path (-r recursive)\n");
  printf("usage: attr [-r] get <key> <path>                           : get attributes of path (-r recursive)\n");
  printf("usage: attr [-r] rm  <key> <path>                           : delete attributes of path (-r recursive)\n\n");
  printf("Help:    If <key> starts with 'sys.' you have to be member of the sudoer to see this attributes or modify\n");

  printf("         Administrator Variables:\n");
  printf("         -----------------------\n");
  printf("         attr: sys.forced.space=<space>            = enforces to use <space>    [configuration dependend]\n");
  printf("         attr: sys.forced.layout=<layout>          = enforces to use <layout>   [<layout>=(plain,replica,raid5)\n");
  printf("         attr: sys.forced.checksum=<checksum>      = enforces to use file-level  checksum <checksum> [<checksum>=(adler,crc32,crc32c,md5,sha)\n");
  printf("         attr: sys.forced.blockchecksum=<checksum> = enforces to use block0level checksum <checksum> [<checksum>=(adler,crc32,crc32c,md5,sha)\n");
  printf("         attr: sys.forced.nstripes=<n>             = enforces to use <n> stripes[<n>= 1..16]\n");
  printf("         attr: sys.forced.blocksize=<w>            = enforces to use a blocksize of <w> - <w> can be 4k,64k,128k,256k or 1M \n");
  printf("         attr: sys.forced.nouserlayout=1           = disables the user settings with user.forced.<xxx>\n");
  printf("         attr: sys.forced.nofsselection=1          = disables user defined filesystem selection with environment variables for reads\n");
  printf("         attr: sys.forced.bookingsize=<bytes>      = set's the number of bytes which get for each new created replica\n");
  printf("         attr: sys.stall.unavailable=<sec>         = stall clients for <sec> seconds if a needed file system is unavailable\n");
  printf("         attr: sys.heal.unavailable=<tries>        = try to heal an unavailable file for atleast <tries> times - must be >= 3 !!\n");
  printf("                                                     - the product <heal-tries> * <stall-time> should be bigger than the expect replication time for a given filesize!\n");
  printf("         User Variables:\n");
  printf("         -----------------------\n");
  printf("         attr: user.forced.space=<space>           = s.a.\n");
  printf("         attr: user.forced.layout=<layout>         = s.a.\n");
  printf("         attr: user.forced.checksum=<checksum>     = s.a.\n");
  printf("         attr: user.forced.blockchecksum=<checksum>= s.a.\n");
  printf("         attr: user.forced.nstripes=<n>            = s.a.\n");
  printf("         attr: user.forced.blocksize=<w>           = s.a.\n");
  printf("         attr: user.forced.nouserlayout=1          = s.a.\n");
  printf("         attr: user.forced.nofsselection=1         = s.a.\n");
  printf("         attr: user.stall.unavailable=<sec>        = s.a.\n");
  printf("         attr: user.tag=<tag>                      = - tag to group files for scheduling and flat file distribution\n");
  printf("                                                     - use this tag to define datasets (if <tag> contains space use tag with quotes)\n");
  return (0);
}
