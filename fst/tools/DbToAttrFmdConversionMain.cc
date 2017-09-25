#include "fst/FmdDbMap.hh"
#include "fst/FmdAttributeHandler.hh"

#include <future>

using namespace eos;
using namespace eos::fst;

int main(int argc, char* argv[]) {
  if(argc < 3) {
    cerr << "Usage: eos-fst-fmd-dbattr-convert <md dictionary path> <db directory>" << endl;
    return -1;
  }

  const auto dictPath = argv[1];
  const auto dbPath = argv[2];

  eos::common::ZStandard fmdCompressor;
  fmdCompressor.setDicts(dictPath);
  FmdAttributeHandler fmdAttributeHandler{&fmdCompressor, &gFmdClient};

  std::vector<std::future<void>> futures;
  for(const auto& fsid : FmdDbMapHandler::GetFsidInMetaDir(dbPath)) {
    auto future = std::async(
      std::launch::async,
      [&fmdAttributeHandler, &dbPath, fsid]() {
        XrdOucString dbfilename;
        FmdDbMapHandler fmdDbMapHandler;
        fmdDbMapHandler.CreateDBFileName(dbPath, dbfilename);
        fmdDbMapHandler.SetDBFile(dbfilename.c_str(), fsid);
        for(const auto& fmd : fmdDbMapHandler.RetrieveAllFmd()) {
          fmdAttributeHandler.FmdAttrSet(fmd, fmd.fid(), fmd.fsid(), nullptr);
        }
      }
    );
    futures.emplace_back(std::move(future));
  }

  for(auto&& future : futures) {
    future.get();
  }

  return 0;
}