#include "fst/filemd/FmdConverter.hh"
#include "fst/utils/StdFSWalkTree.hh"

#include "common/Logging.hh"
#include "fst/utils/FSPathHandler.hh"
#include "fst/utils/StdFSWalkTree.hh"
#include "fst/utils/FTSWalkTree.hh"
#include <folly/executors/IOThreadPoolExecutor.h>
#include "common/StringUtils.hh"

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Check if file system was already converted
//------------------------------------------------------------------------------
bool
FileFSConversionDoneHandler::isFSConverted(std::string_view fstpath)
{
  return std::filesystem::exists(getDoneFilePath(fstpath));
}

//------------------------------------------------------------------------------
// Mark file system as converted
//------------------------------------------------------------------------------
bool
FileFSConversionDoneHandler::markFSConverted(std::string_view fstpath)
{
  std::ofstream f(getDoneFilePath(fstpath));
  return f.good();
}

//------------------------------------------------------------------------------
// Construct conversion done file path based on the mountpoint
//------------------------------------------------------------------------------
std::string
FileFSConversionDoneHandler::getDoneFilePath(std::string_view fstpath)
{
  std::string path {fstpath};

  if (!eos::common::endsWith(fstpath, "/")) {
    path += "/";
  }

  path += mConversionDoneFile;
  return path;
}


//------------------------------------------------------------------------------
// Class FmdConverter
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
FmdConverter::FmdConverter(FmdHandler* src_handler,
                           FmdHandler* tgt_handler,
                           uint16_t per_disk_pool) :
  mSrcFmdHandler(src_handler), mTgtFmdHandler(tgt_handler),
  mExecutor(std::make_shared<folly::IOThreadPoolExecutor>
            (std::clamp(per_disk_pool, MIN_FMDCONVERTER_THREADS,
                        MAX_FMDCONVERTER_THREADS))),
  mDoneHandler(std::make_unique<FileFSConversionDoneHandler>
               (ATTR_CONVERSION_DONE_FILE))
{}

FmdConverter::FmdConverter(FmdHandler* src_handler,
                           FmdHandler* tgt_handler,
                           std::shared_ptr<folly::Executor> _executor) :
  mSrcFmdHandler(src_handler), mTgtFmdHandler(tgt_handler),
  mExecutor(_executor),
  mDoneHandler(std::make_unique<FileFSConversionDoneHandler>
               (ATTR_CONVERSION_DONE_FILE))
{}


//------------------------------------------------------------------------------
// Conversion method
//------------------------------------------------------------------------------
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

//------------------------------------------------------------------------------
// Method converting files on a given file system
//------------------------------------------------------------------------------
void
FmdConverter::ConvertFS(std::string_view fspath,
                        eos::common::FileSystem::fsid_t fsid)
{
  if (mDoneHandler->isFSConverted(fspath)) {
    return;
  }

  if (!mSrcFmdHandler || !mTgtFmdHandler) {
    eos_static_crit("%s", "msg=\"failed fs conversion due to null handers\"");
    return;
  }

  eos_static_info("msg=\"starting file system conversion\" fsid=%u", fsid);
  std::error_code ec;
  auto count = stdfs::WalkFSTree(std::string(fspath), [this,
  fsid](std::string path) {
    this->Convert(fsid, path)
    .via(mExecutor.get())
    .thenValue([path = std::move(path)](bool status) {
      eos_static_info("msg=\"conversion status\" file=%s, status=%d",
                      path.c_str(),
                      status);
    });
  }, ec);

  if (ec) {
    eos_static_err("msg=\"walking fs tree ran into errors!\" err=%s",
                   ec.message().c_str());
  }

  eos_static_info("msg=\"conversion successful, set done marker\" count=%llu",
                  count);
  mDoneHandler->markFSConverted(fspath);
}

//------------------------------------------------------------------------------
// Helper method for file system conversion
//------------------------------------------------------------------------------
void
FmdConverter::ConvertFS(std::string_view fspath)
{
  ConvertFS(fspath, FSPathHandler::GetFsid(fspath));
}

EOSFSTNAMESPACE_END
