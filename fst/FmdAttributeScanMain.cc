#include "FmdAttributeHandler.hh"

using namespace eos::fst;

int main(int argc, char* argv[]) {
  if(argc < 2) {
    cerr << "Usage: eos-fst-fmd-dbattr-convert <md dictionary path> <file1> <file2> ..." << endl;
    return -1;
  }

  eos::common::ZStandard fmdCompressor;
  fmdCompressor.setDicts(argv[1]);
  FmdAttributeHandler fmdAttributeHandler{&fmdCompressor, &gFmdClient};

  for(int i = 2; i < argc; i++){
    try {
      Fmd fmd = fmdAttributeHandler.FmdAttrGet(argv[i]);
      cout << argv[i] << ":" << endl << fmd.DebugString() << endl;
    } catch (eos::MDException& error) {
      cout << error.what() << endl << endl;
    }
  }

  return 0;
}