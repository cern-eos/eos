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
                           size_t per_disk_pool) :
  mSrcFmdHandler(src_handler), mTgtFmdHandler(tgt_handler),
  mExecutor(std::make_unique<folly::IOThreadPoolExecutor>
            (std::clamp(per_disk_pool, MIN_FMDCONVERTER_THREADS,
                        MAX_FMDCONVERTER_THREADS))),
  mDoneHandler(std::make_unique<FileFSConversionDoneHandler>
               (ATTR_CONVERSION_DONE_FILE))
{}

FmdConverter::~FmdConverter()
{
  eos_static_info("%s", "msg=\"calling FmdConverter destructor\"");

  // drain all pending tasks
  if (auto executor = dynamic_cast<folly::IOThreadPoolExecutor*>(mExecutor.get())) {
    executor->join();
  }
  mExecutor.reset();
  eos_static_info("%s", "msg=\"calling FmdConverter destructor done\"");
}

//------------------------------------------------------------------------------
// Conversion method
//------------------------------------------------------------------------------
bool
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

  /*using convert_ret_type = std::result_of_t<decltype(&FmdConverter::Convert)
                           (FmdConverter, eos::common::FileSystem::fsid_t,
                            std::string_view)>;*/
  using future_vector = std::vector<folly::Future<bool>>;
  future_vector futures;
  eos_static_info("msg=\"starting file system conversion\" fsid=%u", fsid);
  std::error_code ec;
  auto count = stdfs::WalkFSTree(std::string(fspath), [this,
  fsid, &futures](std::string path) {
        auto fut = folly::makeFuture().via(mExecutor.get()).
            thenValue([this, fsid, path](auto&&){
          return folly::Future<bool>(this->Convert(fsid, path)); }).
            thenValue([path = std::move(path)](bool status) {
          eos_static_info("msg=\"conversion status\" file=%s, status=%d",
                          path.c_str(), status); return status; });


        static_assert(std::is_same_v<folly::Future<bool>, decltype(fut)>);
        futures.emplace_back(std::move(fut));
  }, ec);

  size_t success_count = 0;
  for (auto&& fut : futures) {
    try {
      success_count += std::move(fut).get();
    } catch (const std::exception& e) {
      eos_static_crit("msg=\"exception during conversion\" err=\"%s\"",
                      e.what());
    }
  }

  if (ec) {
    eos_static_err("msg=\"walking fs tree ran into errors!\" err=%s",
                   ec.message().c_str());
    return;
  }

  eos_static_info("msg=\"conversion successful, set done marker\" count=%llu success_count=%llu",
                  count, success_count);
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
