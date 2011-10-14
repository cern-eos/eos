/*----------------------------------------------------------------------------*/
#include "console/ConsoleMain.hh"
#include "common/SymKeys.hh"
/*----------------------------------------------------------------------------*/

/* Get the server version*/
int
com_motd (char *arg) {
  XrdOucString in = "mgm.cmd=motd"; 
  XrdOucString motdfile = arg;
  if (motdfile.length()) {
    int fd = open(motdfile.c_str(),O_RDONLY);
    if (fd >0) {
      char maxmotd[1024];
      memset(maxmotd,0,sizeof(maxmotd));
      size_t nread = read(fd,maxmotd,sizeof(maxmotd));
      maxmotd[1023]=0;
      XrdOucString b64out;
      if (nread>0) {
        eos::common::SymKey::Base64Encode(maxmotd, strlen(maxmotd)+1, b64out);
      }
      in += "&mgm.motd=";
      in += b64out.c_str();
    }
  }
  
  global_retc = output_result(client_user_command(in));
  return (0);
}
