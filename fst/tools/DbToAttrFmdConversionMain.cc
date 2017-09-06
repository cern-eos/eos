#include "fst/FmdDbMap.hh"
#include "fst/FmdAttributeHandler.hh"

#include <future>

using namespace eos;
using namespace eos::fst;

int main(int argc, char* argv[]) {
  if(argc < 2) {
    cerr << "Usage: eos-fst-fmd-dbattr-convert <db directory>" << endl;
    return -1;
  }

  const auto dbPath = argv[1];

  std::vector<std::future<void>> futures;
  for(const auto& fsid : FmdDbMapHandler::GetFsidInMetaDir(dbPath)) {
    auto future = std::async(
      std::launch::async,
      [&dbPath, fsid]() {
        XrdOucString dbfilename;
        FmdDbMapHandler fmdDbMapHandler;
        fmdDbMapHandler.CreateDBFileName(dbPath, dbfilename);
        fmdDbMapHandler.SetDBFile(dbfilename.c_str(), fsid);
        for(const auto& fmd : fmdDbMapHandler.RetrieveAllFmd()) {
          gFmdAttributeHandler.FmdAttrSet(fmd, fmd.fid(), fmd.fsid(), nullptr);
        }
      }
    );
    futures.push_back(std::move(future));
  }

  for(auto&& future : futures) {
    future.get();
  }

  cout << futures.size() << endl;
  cout << FmdDbMapHandler::GetFsidInMetaDir("/home/jmakai/md").size() << endl;

  return 0;
}