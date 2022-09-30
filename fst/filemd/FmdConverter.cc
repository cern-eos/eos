#include "fst/filemd/FmdConverter.hh"
#include "fst/utils/StdFSWalkTree.hh"

#include "common/async/OpaqueFuture.hh"
#include "common/async/ExecutorMgr.hh"
#include "common/Logging.hh"
#include "common/StringUtils.hh"
#include "fst/utils/FSPathHandler.hh"
#include "fst/utils/FTSWalkTree.hh"
#include "fst/utils/StdFSWalkTree.hh"

EOSFSTNAMESPACE_BEGIN

using eos::common::ExecutorType;
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
  mExecutorMgr(std::make_shared<common::ExecutorMgr>(ExecutorType::kThreadPool,
               per_disk_pool)),
  mDoneHandler(std::make_unique<FileFSConversionDoneHandler>
               (ATTR_CONVERSION_DONE_FILE))
{}

FmdConverter::FmdConverter(FmdHandler* src_handler, FmdHandler* tgt_handler,
                           size_t per_disk_pool, std::string_view executor_type):
  mSrcFmdHandler(src_handler), mTgtFmdHandler(tgt_handler),
  mExecutorMgr(std::make_shared<common::ExecutorMgr>(executor_type,
               per_disk_pool)),
  mDoneHandler(std::make_unique<FileFSConversionDoneHandler>
               (ATTR_CONVERSION_DONE_FILE))
{
}

FmdConverter::FmdConverter(FmdHandler* src_handler, FmdHandler* tgt_handler,
                           std::shared_ptr<common::ExecutorMgr> executor_mgr):
  mSrcFmdHandler(src_handler), mTgtFmdHandler(tgt_handler),
  mExecutorMgr(executor_mgr),
  mDoneHandler(std::make_unique<FileFSConversionDoneHandler>
               (ATTR_CONVERSION_DONE_FILE))
{
}

FmdConverter::~FmdConverter()
{
  eos_static_info("%s", "msg=\"calling FmdConverter destructor\"");
  // drain all pending tasks
  //mExecutor.reset();
  eos_static_info("%s", "msg=\"calling FmdConverter destructor done\"");
}

//------------------------------------------------------------------------------
// Conversion method
//------------------------------------------------------------------------------
bool
FmdConverter::Convert(eos::common::FileSystem::fsid_t fsid, std::string path)
{
  auto fid = eos::common::FileId::PathToFid(path.c_str());

  if (!fsid || !fid) {
    eos_static_info("msg=\"conversion failed invalid fid\" file=%s, fid=%lu",
                    path.c_str(), fid);
    return false;
  }

  bool status = mTgtFmdHandler->ConvertFrom(fid, fsid, mSrcFmdHandler, true);
  eos_static_info("msg=\"conversion done\" file=%s, fid=%lu, status=%d",
                  path.data(), fid, status);
  return status;
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

  using future_vector = std::vector<eos::common::OpaqueFuture<bool>>;
  future_vector futures;
  eos_static_info("msg=\"starting file system conversion\" fsid=%u", fsid);
  std::error_code ec;
  auto count = stdfs::WalkFSTree(std::string(fspath), [this,
  fsid, &futures](std::string path) {
    try {
      auto fut = mExecutorMgr->PushTask([this, fsid, path = std::move(path)]() {
        return this->Convert(fsid, path);
      });
      static_assert(std::is_same_v<eos::common::OpaqueFuture<bool>, decltype(fut)>);
      futures.emplace_back(std::move(fut));
    } catch (const std::exception& e) {
      eos_static_crit("msg=\"failed to push task\" path=%s err=%s", path.c_str(),
                      e.what());
    }
  }, ec);
  size_t success_count = 0;

  try {
    for (auto && fut : futures) {
      success_count += fut.getValue();
    }
  } catch (const std::exception& e) {
    eos_static_crit("msg=\"failed to convert file system\" fsid=%u, err=%s",
                    fsid, e.what());
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
