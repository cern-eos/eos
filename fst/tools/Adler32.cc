#include <stdio.h>
#include <stdlib.h>
#include "fst/checksum/ChecksumPlugins.hh"

int main(int argc, char* argv[]) {
  if (argc!=2) {
    fprintf(stderr,"error: you have to provide a path name\n");
    exit(-1);
  }
  
  eos::fst::CheckSum *normalXS;
  normalXS = eos::fst::ChecksumPlugins::GetChecksumObject(eos::common::LayoutId::kAdler);
  if (normalXS) {
    unsigned long long scansize;
    float scantime;
    normalXS->ScanFile(argv[1],scansize,scantime);
    fprintf(stdout,"path=%s size=%llu time=%.02f adler32=%s\n", argv[1],scansize,scantime,normalXS->GetHexChecksum());
    exit(0);
  }
  fprintf(stderr,"error: failed to get checksum object\n");
  exit(-1);
}
