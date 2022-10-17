#include "fst/filemd/FmdConverter.hh"
#include "fst/utils/StdFSWalkTree.hh"

#include "common/async/OpaqueFuture.hh"
#include "common/async/ExecutorMgr.hh"
#include "common/Logging.hh"
#include "common/StringUtils.hh"
#include "common/utils/XrdUtils.hh"
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
  return Convert(fsid, fid);
}

bool
FmdConverter::Convert(eos::common::FileSystem::fsid_t fsid,
                      eos::common::FileId::fileid_t fid)
{
  if (!fsid || !fid) {
    eos_static_info("msg=\"conversion failed invalid fid\" fid=%lu fsid=%u",
                    fid, fsid);
    return false;
  }

  bool status = mTgtFmdHandler->ConvertFrom(fid, fsid, mSrcFmdHandler, true);
  eos_static_info("msg=\"conversion done\" fsid=%u, fid=%lu, status=%d",
                  fsid, fid, status);
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

  LoadConfigFromEnv(fsid);

  using future_vector = std::vector<eos::common::OpaqueFuture<bool>>;
  future_vector futures;
  futures.reserve(mPerDiskQueueSize);

  eos_static_info("msg=\"starting file system conversion\" fsid=%u", fsid);
  std::error_code ec;
  size_t success_count = 0;
  auto count = stdfs::WalkFSTree(std::string(fspath), [this,
  fsid, &futures, &success_count](const std::string& path) {
    auto fid = eos::common::FileId::PathToFid(path.c_str());
    try {
      // Anything inside the PushTask must be captured by value as this executes
      // much later in a different thread
      auto fut = mExecutorMgr->PushTask([this, fsid, fid]() {
        return this->Convert(fsid, fid);
      });
      futures.emplace_back(std::move(fut));
      success_count += DrainFutures(futures, fsid);
    } catch (const std::exception& e) {
      eos_static_crit("msg=\"failed to push task\" fsid=%u, fid=%lu err=%s",
                      fsid, fid, e.what());
    }
  }, ec);

  success_count += DrainFutures(futures, fsid, true);
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

uint64_t
FmdConverter::DrainFutures(std::vector<common::OpaqueFuture<bool>>& futures,
                           eos::common::FileSystem::fsid_t fsid,
                           bool force)
{
  uint64_t success_count = 0;

  if (force ||
      futures.size() > mPerDiskQueueSize) {
      for (auto && fut : futures) {
        try {
          success_count += fut.getValue();
        } catch (const std::exception& e) {
           eos_static_crit("msg=\"failed to get value\" err=%s, fsid=%u", e.what(), fsid);
        }
      }
     futures.clear();
     unsigned int wait_ctr {0};
     while (mExecutorMgr->GetQueueSize() > mGlobalQueueSize) {
       eos_static_info("msg=\"waiting for FmdConverter queue to drain\" "
                       "fsid=%u wait_ctr=%u", fsid, ++wait_ctr);
       std::this_thread::sleep_for(std::chrono::milliseconds(500));
     }
   }
  return success_count;
}

void
FmdConverter::LoadConfigFromEnv(eos::common::FileSystem::fsid_t fsid)
{
  mPerDiskQueueSize = common::XrdUtils::GetEnv("EOS_FMD_PER_FS_QUEUE_SIZE",
                                       FMD_PER_FS_QUEUE_SIZE);
  mGlobalQueueSize = common::XrdUtils::GetEnv("EOS_FMD_GLOBAL_QUEUE_SIZE",
                                      FMD_GLOBAL_QUEUE_SIZE);
  eos_static_info("msg=\"loading FmdConverter config:\" "
                  "fsid=%u per_disk_queue_size=%lu global_queue_size=%lu",
                  fsid, mPerDiskQueueSize, mGlobalQueueSize);
}

EOSFSTNAMESPACE_END
