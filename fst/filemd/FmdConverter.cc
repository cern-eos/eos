#include "fst/filemd/FmdConverter.hh"
#include "fst/utils/StdFSWalkTree.hh"
#include "common/StringSplit.hh"
#include "common/StringUtils.hh"
#include "common/StringConversion.hh"
#include "common/Logging.hh"
#include "fst/storage/Storage.hh"
#include "fst/XrdFstOfs.hh"
#include "fst/utils/StdFSWalkTree.hh"
#include <folly/executors/IOThreadPoolExecutor.h>

namespace eos::fst {

eos::common::FileSystem::fsid_t GetFsid(std::string_view path)
{
  eos::common::FileSystem::fsid_t fsid;
  std::string err_msg;
  std::string fsidpath = eos::common::GetRootPath(path);
  fsidpath += "/.eosfsid";
  std::string sfsid;
  eos::common::StringConversion::LoadFileIntoString(fsidpath.c_str(), sfsid);

  if (eos::common::StringToNumeric(sfsid, fsid, (uint32_t)0, &err_msg)) {
    eos_static_crit("msg=\"Unable to obtain FSID from path=\"%s",
                    path.data());
    // TODO: this is exceptional, throw an error!
  }

  return fsid;
}

std::string XrdOfsPathInfo::GetPath(eos::common::FileSystem::fsid_t fsid)
{
  return gOFS.Storage->GetStoragePath(fsid);
}

FmdConverter::FmdConverter(FmdHandler* src_handler,
                           FmdHandler* tgt_handler,
                           size_t per_disk_pool) :
    mSrcFmdHandler(src_handler), mTgtFmdHandler(tgt_handler),
    mExecutor(std::make_unique<folly::IOThreadPoolExecutor>(per_disk_pool))
{}


folly::Future<bool>
FmdConverter::Convert(std::string_view path, uint64_t count)
{

  auto fsid = FsidPathInfo::GetFsid(path);
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
