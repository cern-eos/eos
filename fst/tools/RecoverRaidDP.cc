/*----------------------------------------------------------------------------*/
#include <cstdio>
#include <cstdlib>
#include <fst/RaidDPScan.hh>
#include "XrdOuc/XrdOucString.hh"
/*----------------------------------------------------------------------------*/

int main (int argc, char *argv[]){
  if (argc != 2){
    fprintf(stderr, "usage: eos-raiddp-scan <file_name>\n");
    exit(-1);
  }

  XrdOucString fileName = argv[1];
  eos::fst::RaidDPScan* rds = new eos::fst::RaidDPScan(fileName.c_str(), false);
  if (rds){
    eos::fst::RaidDPScan::StaticThreadProc((void*) rds);
    delete rds;
  }
}
