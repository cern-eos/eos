/*----------------------------------------------------------------------------*/
#include "fst/ScanDir.hh"
#include "common/Attr.hh"
#include "fst/checksum/ChecksumPlugins.hh"
#include "common/LayoutId.hh"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
/*----------------------------------------------------------------------------*/

int main (int argc, char *argv[]){
  bool setxs=false;
  if ( (argc <2) || (argc >3) ) {
    fprintf(stderr,"usage: eos-scan-fs <directory> [--setxs]\n");
    exit(-1);
  }

  if (argc == 3) {
    XrdOucString set = argv[2];
    if (set != "--setxs") {
      fprintf(stderr,"usage: eos-scan-fs <directory> [--setxs]\n");
      exit(-1);
    }
    setxs=true;
  }
  srand((unsigned int) time(NULL));

  eos::fst::Load fstLoad(1);
  fstLoad.Monitor();

  XrdOucString dirName = argv[1];
  eos::fst::ScanDir* sd = new eos::fst::ScanDir(dirName.c_str(), &fstLoad, false, 10,100,setxs);
  if (sd) {
    eos::fst::ScanDir::StaticThreadProc( (void*)sd);
    delete sd;
  }
}
