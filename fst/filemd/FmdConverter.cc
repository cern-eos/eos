#include "fst/filemd/FmdConverter.hh"
#include "fst/utils/StdFSWalkTree.hh"

#include "common/Logging.hh"
#include "fst/utils/FSPathHandler.hh"
#include "fst/utils/StdFSWalkTree.hh"
#include <folly/executors/IOThreadPoolExecutor.h>

namespace eos::fst
{


FmdConverter::FmdConverter(FmdHandler* src_handler,
                           FmdHandler* tgt_handler,
                           size_t per_disk_pool) :
  mSrcFmdHandler(src_handler), mTgtFmdHandler(tgt_handler),
  mExecutor(std::make_unique<folly::IOThreadPoolExecutor>(per_disk_pool))
{}


folly::Future<bool>
FmdConverter::Convert(std::string_view path, uint64_t count)
{
  auto fsid = FSPathHandler::GetFsid(path);
  auto fid = eos::common::FileId::PathToFid(path.data());

  if (!fsid) {
    return false;
  }

  if (!mSrcFmdHandler->ConvertFrom(fsid, fid, mTgtFmdHandler, false)) {
    return false;
  }

  return true;
}

void
FmdConverter::ConvertFS(std::string_view fspath)
{
  std::error_code ec;
  stdfs::WalkFSTree(fspath, [this](std::string path, uint64_t count) {
    this->Convert(path, count)
    .via(mExecutor.get())
    .thenValue([&path](bool status) {
      eos_static_info("msg=\"Conversion status\" file=%s, status =%d",
                      path.c_str(),
                      status);
    }
              );
  }, ec);
}

} // namespace eos::fst
