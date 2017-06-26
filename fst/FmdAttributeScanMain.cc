#include "FmdAttributeHandler.hh"
#include <iomanip>

using namespace eos::fst;

int main(int argc, char* argv[]) {
  for(int i = 1; i < argc; i++){
    try {
      Fmd fmd = gFmdAttributeHandler.FmdAttrGet(argv[i]);
      cout << argv[i] << ":" << endl << fmd.DebugString() << endl;
    } catch (fmd_attribute_error& error) {
      cout << error.what() << endl << endl;
    }
  }

  return 0;
}