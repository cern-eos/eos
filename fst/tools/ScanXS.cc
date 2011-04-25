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
  if (argc !=2) {
    fprintf(stderr,"usage: eos-scan-fs <directory>\n");
    exit(-1);
  }
  
  eos::fst::Load fstLoad(1);
  fstLoad.Monitor();

  XrdOucString dirName = argv[1];
  eos::fst::ScanDir* sd = new eos::fst::ScanDir(dirName.c_str(), &fstLoad, false, 20,100);
  if (sd) {
    eos::fst::ScanDir::StaticThreadProc( (void*)sd);
    delete sd;
  }
}
