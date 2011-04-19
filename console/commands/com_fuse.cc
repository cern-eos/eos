/*----------------------------------------------------------------------------*/
#include "console/ConsoleMain.hh"
/*----------------------------------------------------------------------------*/

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

extern XrdOucString serveruri;

/* mount/umount via fuse */
int
com_fuse (char* arg1) {
  // split subcommands
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString cmd = subtokenizer.GetToken();
  XrdOucString mountpoint=subtokenizer.GetToken();
  XrdOucString option="";
  XrdOucString params="kernel_cache,attr_timeout=30,entry_timeout=30,max_readahead=131072,max_write=4194304";
  XrdOucString logfile="";
  
  if ( (cmd != "mount") && (cmd != "umount") )
    goto com_fuse_usage;
  
  if (!mountpoint.length()) 
    goto com_fuse_usage;
  
  do {
    option = subtokenizer.GetToken();
    if (!option.length())
      break;
    if (option.beginswith("-o")) {
      params = subtokenizer.GetToken();
      if (!params.length())
        goto com_fuse_usage;
    }
    if (option.beginswith("-l")) {
      logfile = subtokenizer.GetToken();
      if (!logfile.length()) 
        goto com_fuse_usage;
    }
  } while(1);

  if (cmd == "mount") {
    struct stat buf;
    struct stat buf2;
    if (stat(mountpoint.c_str(),&buf)) {
      XrdOucString createdir="mkdir -p ";
      createdir += mountpoint;
      createdir += " >& /dev/null";
      system(createdir.c_str());
      fprintf(stderr,".... trying to create ... %s\n",mountpoint.c_str());
    }
    
    if (stat(mountpoint.c_str(),&buf)) {
      fprintf(stderr,"\nerror: cannot create mountpoint %s !\n", mountpoint.c_str());
      exit(-1);
    } else {
      fprintf(stderr,"OK\n");
    }
    
    if (params.length()) {
      params += ",url="; params += serveruri.c_str(); params += "//eos/";
    } else {
      params += "url="; params += serveruri.c_str(); params += "//eos/";
    }

    fprintf(stderr,"===> Mountpoint   : %s\n", mountpoint.c_str());
    fprintf(stderr,"===> Fuse-Options : %s\n", params.c_str());
    if (logfile.length()) {
      fprintf(stderr,"===> Log File     : %s\n", logfile.c_str());
    }
    
    XrdOucString mount="eosfsd "; mount += mountpoint.c_str(); mount += " -o"; mount += params;
    if (logfile.length()) {
      mount += " -d >& "; mount += logfile;
    } else {
      mount += " -s >& /dev/null";
    }

    system(mount.c_str());

    if ((stat(mountpoint.c_str(),&buf2) || (buf2.st_ino == buf.st_ino))) {
      fprintf(stderr,"error: mount failed at %s\n", mountpoint.c_str());
      exit(-1);
    }
    
  }

  if (cmd == "umount") {
    struct stat buf;
    struct stat buf2;

    if ((stat(mountpoint.c_str(),&buf)|| (buf.st_ino !=1))) {
      fprintf(stderr,"error: there is no eos mount at %s\n", mountpoint.c_str());
      exit(-1);
    }
    
    XrdOucString umount = "fusermount -z -u "; umount += mountpoint.c_str(); umount += ">& /dev/null";
    system(umount.c_str());
    if ((stat(mountpoint.c_str(),&buf2))) {
      fprintf(stderr,"error: mount directoy disappeared from %s\n", mountpoint.c_str());
      exit(-1);
    }
    
    if (buf.st_ino == buf2.st_ino) {
      fprintf(stderr,"error: umount didn't work\n");
      exit(-1);
    }
  }
  
  exit(0);
  
  com_fuse_usage:
  printf("usage: fuse mount  <mount-point> [-o <fuseparamaterlist>] [-l <logfile>] : mount connected eos pool on <mount-point>\n");
  printf("       fuse umount <mount-point>                                         : unmount eos pool from <mount-point>\n");
  exit(-1);
  }
