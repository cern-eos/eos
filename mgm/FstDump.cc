// -----------------------------------------------------------------
// ! this is a tiny program dumping the contents of an FST directory
// -----------------------------------------------------------------

#include <XrdPosix/XrdPosixXrootd.hh>
#include <XrdOuc/XrdOucString.hh>
#include <stdio.h>
#include <errno.h>

int main(int argc, char* argv[])
{
  setenv("XrdSecPROTOCOL","sss",1);
  // argv[1] = 'root://<host>/<datadir>
  XrdOucString url = argv[1];
  if ((argc!=2) || (!url.beginswith("root://"))) {
    fprintf(stderr,"usage: eos-fst-dump root://<host>/<datadir>\n");
    exit(-EINVAL);
  }
  DIR* dir = XrdPosixXrootd::Opendir(url.c_str());
  if (dir) {
    static struct dirent*  dentry;

    while ( (dentry = XrdPosixXrootd::Readdir(dir)) ) {
      fprintf(stderr,"%s\n",dentry->d_name);
    }
    XrdPosixXrootd::Closedir(dir);
    exit(0);
  } else {
    exit(-1);
  }
}
