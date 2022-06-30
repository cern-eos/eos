#include "fst/filemd/FmdConverter.hh"
#include "fst/utils/StdFSWalkTree.hh"

#include "common/Logging.hh"
#include "fst/utils/FSPathHandler.hh"
#include "fst/utils/StdFSWalkTree.hh"
#include <folly/executors/IOThreadPoolExecutor.h>
#include "common/StringUtils.hh"

namespace eos::fst
{


FmdConverter::FmdConverter(FmdHandler* src_handler,
                           FmdHandler* tgt_handler,
                           size_t per_disk_pool) :
  mSrcFmdHandler(src_handler), mTgtFmdHandler(tgt_handler),
  mExecutor(std::make_unique<folly::IOThreadPoolExecutor>(std::clamp(per_disk_pool, MIN_FMDCONVERTER_THREADS, MAX_FMDCONVERTER_THREADS))),
  mDoneHandler(std::make_unique<FileFSConversionDoneHandler>(ATTR_CONVERSION_DONE_FILE))
{}


folly::Future<bool>
FmdConverter::Convert(eos::common::FileSystem::fsid_t fsid,
                      std::string_view path)
{
  auto fid = eos::common::FileId::PathToFid(path.data());

  if (!fsid || !fid) {
    return false;
  }

  return mTgtFmdHandler->ConvertFrom(fid, fsid, mSrcFmdHandler, true);
}

void
FmdConverter::ConvertFS(std::string_view fspath,
                        eos::common::FileSystem::fsid_t fsid)
{
  std::error_code ec;
  if (mDoneHandler->isFSConverted(fspath)) {
    return;
  }

  if (!mSrcFmdHandler || !mTgtFmdHandler) {
    eos_static_crit("%s", "Empty handlers for FST filemd! Cannot Convert!");
    return;
  }

  eos_static_info("msg=\"Starting Conversion for fsid=%u\"", fsid);

  auto count = stdfs::WalkFSTree(fspath, [this, fsid](std::string path) {
    this->Convert(fsid, path)
    .via(mExecutor.get())
    .thenValue([path = std::move(path)](bool status) {
      eos_static_info("msg=\"Conversion status\" file=%s, status=%d",
                      path.c_str(),
                      status);
    });
  }, ec);

  if (ec) {
    eos_static_err("msg=\"Walking FS tree ran into errors!\" err=%s",
                   ec.message().c_str());
  }

  eos_static_info("msg=\"Finished converting %llu successfully. Setting done marker\"",
                  count);
  mDoneHandler->markFSConverted(fspath);
}

void
FmdConverter::ConvertFS(std::string_view fspath)
{
  ConvertFS(fspath, FSPathHandler::GetFsid(fspath));
}


bool
FileFSConversionDoneHandler::isFSConverted(std::string_view fstpath)
{
  return std::filesystem::exists(getDoneFilePath(fstpath));
}

bool
FileFSConversionDoneHandler::markFSConverted(std::string_view fstpath)
{
  std::ofstream f(getDoneFilePath(fstpath));
  return f.good();
}

std::string
FileFSConversionDoneHandler::getDoneFilePath(std::string_view fstpath)
{
  std::string path {fstpath};
  if (!eos::common::endsWith(fstpath,"/")) {
    path += "/";
  }
  path += mConversionDoneFile;
  return path;
}

} // namespace eos::fst
