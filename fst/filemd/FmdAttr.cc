#include "FmdAttr.hh"
#include "fst/io/local/FsIo.hh"
#include "fst/XrdFstOfs.hh"
#include "fst/utils/FTSWalkTree.hh"
#include "fst/utils/FSPathHandler.hh"
#include "FmdHandler.hh"
#include <functional>

namespace eos::fst
{


FmdAttrHandler::FmdAttrHandler(std::unique_ptr<FSPathHandler>&& _FSPathHandler) :
    mFSPathHandler(std::move(_FSPathHandler))
{
}


std::pair<bool, eos::common::FmdHelper>
FmdAttrHandler::LocalRetrieveFmd(const std::string& path)
{
  FsIo localIo {path};
  std::string attrval;
  int result = localIo.attrGet(gFmdAttrName, attrval);

  if (result != 0) {
    eos_err("Failed to Retrieve Fmd Attribute at path:%s, errno=%d", path.c_str(),
            errno);
    return {false, eos::common::FmdHelper{}};
  }

  eos::common::FmdHelper fmd;
  bool status = fmd.mProtoFmd.ParsePartialFromString(attrval);

  if (!status) {
    eos_err("msg=\"Failed Parsing attrval\" attrval_sz=%lu", attrval.size());
  }

  return {status, std::move(fmd)};
}

int
FmdAttrHandler::CreateFile(FileIo* fio)
{
  if (fio->fileExists() == 0) {
    return 0;
  }

  FsIo fsio {fio->GetPath()};
  int rc = fsio.fileOpen(O_CREAT | O_RDWR | O_APPEND,
                         S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);

  if (rc != 0) {
    eos_err("Failed to open file rc=%d", errno);
  }

  fsio.fileClose();
  return rc;
}

bool
FmdAttrHandler::LocalPutFmd(const std::string& path,
                            const eos::common::FmdHelper& fmd)
{
  FsIo localio {path};
  int rc;
  rc = CreateFile(&localio);

  if (rc != 0) {
    // TODO: do we need to set kMissing when create
    eos_err("Failed to open file for fmd attr path:%s, rc=%d", path.c_str(), rc);
    return rc != 0;
  }

  std::string attrval;
  fmd.mProtoFmd.SerializePartialToString(&attrval);
  rc = localio.attrSet(gFmdAttrName, attrval.c_str(), attrval.length());

  if (rc != 0) {
    eos_err("Failed to Set Fmd Attribute at path:%s, errno=%d", path.c_str(),
            errno);
  }

  return rc == 0;
}

void
FmdAttrHandler::LocalDeleteFmd(const std::string& path, bool drop_file)
{
  FsIo localio {path};

  if (drop_file) {
    int rc = localio.fileRemove();

    if (rc && errno != ENOENT) {
      eos_err("Failed to drop file at path=%s, errno=%d", path.c_str(), errno)
    }

    return;
  }

  if (int rc = localio.attrDelete(gFmdAttrName);
      rc != 0) {
    if (errno == ENOATTR || errno == ENOENT) {
      return;
    }

    eos_err("Failed to Delete Fmd Attribute at path:%s, rc=%d", path.c_str(),
            errno);
  }
}

std::pair<bool, eos::common::FmdHelper>
FmdAttrHandler::LocalRetrieveFmd(eos::common::FileId::fileid_t fid,
                                 eos::common::FileSystem::fsid_t fsid)
{
  return LocalRetrieveFmd(mFSPathHandler->GetPath(fid, fsid));
}

bool
FmdAttrHandler::LocalPutFmd(eos::common::FileId::fileid_t fid,
                            eos::common::FileSystem::fsid_t fsid,
                            const eos::common::FmdHelper& fmd)
{
  return LocalPutFmd(mFSPathHandler->GetPath(fid, fsid), fmd);
}

void
FmdAttrHandler::LocalDeleteFmd(eos::common::FileId::fileid_t fid,
                               eos::common::FileSystem::fsid_t fsid,
                               bool drop_file)
{
  return LocalDeleteFmd(mFSPathHandler->GetPath(fid, fsid), drop_file);
}

bool
FmdAttrHandler::Commit(eos::common::FmdHelper* fmd, bool)
{
  struct timeval tv;
  struct timezone tz;
  gettimeofday(&tv, &tz);
  fmd->mProtoFmd.set_mtime(tv.tv_sec);
  fmd->mProtoFmd.set_atime(tv.tv_sec);
  fmd->mProtoFmd.set_mtime_ns(tv.tv_usec * 1000);
  fmd->mProtoFmd.set_atime_ns(tv.tv_usec * 1000);
  return LocalPutFmd(fmd->mProtoFmd.fid(), fmd->mProtoFmd.fsid(),
                     *fmd);
}

std::unique_ptr<eos::common::FmdHelper>
FmdAttrHandler::LocalGetFmd(eos::common::FileId::fileid_t fid,
                            eos::common::FileSystem::fsid_t fsid,
                            bool force_retrieve, bool do_create,
                            uid_t uid, gid_t gid,
                            eos::common::LayoutId::layoutid_t layoutid)
{
  auto [status, _fmd] = LocalRetrieveFmd(fid, fsid);

  if (!status && !do_create) {
    eos_warning("msg=\"no fmd record found\" fid=%08llx fsid=%lu", fid, fsid);
    return nullptr;
  }

  // Check the various conditions if we have a fmd attr already
  if (status) {
    auto fmd = std::make_unique<eos::common::FmdHelper>(std::move(_fmd.mProtoFmd));

    if ((fmd->mProtoFmd.fid() != fid) || (fmd->mProtoFmd.fsid() != fsid)) {
      eos_crit("msg=\"mismatch between requested fid/fsid and retrieved ones\" "
               "fid=%08llx retrieved_fid=%08llx fsid=%lu retrieved_fsid=%lu",
               fid, fmd->mProtoFmd.fid(), fsid, fmd->mProtoFmd.fsid());
      return nullptr;
    }

    if (force_retrieve) {
      return fmd;
    }

    if (!eos::common::LayoutId::IsRain(fmd->mProtoFmd.lid())) {
      if (!do_create &&
          ((fmd->mProtoFmd.disksize() &&
            (fmd->mProtoFmd.disksize() != eos::common::FmdHelper::UNDEF) &&
            (fmd->mProtoFmd.disksize() != fmd->mProtoFmd.size())) ||
           (fmd->mProtoFmd.mgmsize() &&
            (fmd->mProtoFmd.mgmsize() != eos::common::FmdHelper::UNDEF) &&
            (fmd->mProtoFmd.mgmsize() != fmd->mProtoFmd.size())))) {
        eos_crit("msg=\"size mismatch disk/mgm vs memory\" fxid=%08llx "
                 "fsid=%lu size=%llu disksize=%llu mgmsize=%llu",
                 fid, (unsigned long) fsid, fmd->mProtoFmd.size(),
                 fmd->mProtoFmd.disksize(), fmd->mProtoFmd.mgmsize());
        return nullptr;
      }

      if (!do_create &&
          ((fmd->mProtoFmd.filecxerror() == 1) ||
           (fmd->mProtoFmd.mgmchecksum().length() &&
            (fmd->mProtoFmd.mgmchecksum() != fmd->mProtoFmd.checksum())))) {
        eos_crit("msg=\"checksum error flagged/detected\" fxid=%08llx "
                 "fsid=%lu checksum=%s diskchecksum=%s mgmchecksum=%s "
                 "filecxerror=%d blockcxerror=%d", fid,
                 (unsigned long) fsid, fmd->mProtoFmd.checksum().c_str(),
                 fmd->mProtoFmd.diskchecksum().c_str(),
                 fmd->mProtoFmd.mgmchecksum().c_str(),
                 fmd->mProtoFmd.filecxerror(),
                 fmd->mProtoFmd.blockcxerror());
      }
    } else {//Non Rain
      if (fmd->mProtoFmd.blockcxerror() == 1) {
        eos_crit("msg=\"blockxs error detected\" fxid=%08llx fsid=%lu",
                 fid, fsid);
        return nullptr;
      }
    }

    return fmd;
  } // status || force_retrieve

  auto fmd = eos::fst::FmdHandler::make_fmd_helper(fid, fsid, uid, gid,
             layoutid);

  if (Commit(fmd.get(), false)) {
    eos_debug("msg=\"return fmd object\" fid=%08llx fsid=%lu", fid, fsid);
    return fmd;
  }

  eos_crit("msg=\"failed to commit fmd to storage\" fid=%08llx fsid=%lu",
           fid, fsid);
  return nullptr;
}



bool
FmdAttrHandler::GetInconsistencyStatistics(
  eos::common::FileSystem::fsid_t fsid,
  std::map<std::string, size_t>& statistics,
  std::map<std::string, std::set<eos::common::FileId::fileid_t>>& fidset)
{
  std::error_code ec;
  uint64_t count {0};
  auto ret = WalkFSTree(mFSPathHandler->GetFSPath(fsid),
                        [this, &statistics, &fidset, &count ](const char* path) {
    eos_debug("msg=\"Accessing file=\"%s", path);

    if (++count % 10000 == 0) {
      eos_info("msg=\"synced files so far\" nfiles=%llu",
               count);
    }

    this->UpdateInconsistencyStat(path, statistics, fidset);
  },
  ec);
  statistics["mem_n"] += ret;

  if (ec) {
    eos_err("msg=\"Failed to walk FST Tree\" error=%s", ec.message());
  }

  return true;
}

bool
FmdAttrHandler::UpdateInconsistencyStat(
  const std::string& path, std::map<std::string, size_t>& statistics,
  std::map<std::string, std::set<eos::common::FileId::fileid_t>>& fidset)
{
  auto&& [status, fmd] = LocalRetrieveFmd(path);

  if (!status) {
    return status;
  }

  CollectInconsistencies(fmd, statistics, fidset);
  return true;
}


} // namespace eos::fst
