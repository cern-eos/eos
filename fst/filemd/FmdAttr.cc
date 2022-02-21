#include "FmdAttr.hh"
#include "fst/io/local/LocalIo.hh"
#include "fst/XrdFstOfs.hh"
#include "FmdHandler.hh"

namespace eos::fst {

std::string FmdAttrHandler::GetPath(eos::common::FileId::fileid_t fid,
                                    eos::common::FileSystem::fsid_t fsid)
{
  // TODO: make this common fn take a couple of strings
  return eos::common::FileId::FidPrefix2FullPath(eos::common::FileId::Fid2Hex(fid).c_str(),
                                                 gOFS.Storage->GetStoragePath(fsid).c_str());
}

std::pair<bool,eos::common::FmdHelper>
FmdAttrHandler::LocalRetrieveFmd(const std::string& path)
{
  LocalIo localIo {path};
  std::string attrval;
  int result = localIo.attrGet(gFmdAttrName, attrval);
  if (result != 0) {
    eos_err("Failed to Retrieve Fmd Attribute at path:%s, errno=%d", path.c_str(), errno);
    return {false, eos::common::FmdHelper{}};
  }

  eos::common::FmdHelper fmd;
  bool status = fmd.mProtoFmd.ParsePartialFromString(attrval);
  return {status, std::move(fmd)};
}

int
FmdAttrHandler::CreateFile(FileIo* fio)
{
  if (fio->fileExists() == 0) {
    return 0;
  }
  FsIo fsio {fio->GetPath()};
  int rc = fsio.fileOpen(O_CREAT | O_RDWR | O_APPEND);
  if (rc != 0)
  {
    eos_err("Failed to open file rc=%d", errno);
  }
  fsio.fileClose();

  return rc;
}

bool
FmdAttrHandler::LocalPutFmd(const std::string& path, const eos::common::FmdHelper& fmd)
{
  LocalIo localio {path};
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
FmdAttrHandler::LocalDeleteFmd(const std::string& path)
{
  LocalIo localio {path};
  if (int rc = localio.attrDelete(gFmdAttrName);
      rc != 0) {
    if (errno != ENOATTR)
      eos_err("Failed to Delete Fmd Attribute at path:%s, rc=%d", path.c_str(), errno);
  }
}

std::pair<bool,eos::common::FmdHelper>
FmdAttrHandler::LocalRetrieveFmd(eos::common::FileId::fileid_t fid,
                                 eos::common::FileSystem::fsid_t fsid)
{
  return LocalRetrieveFmd(FmdAttrHandler::GetPath(fid, fsid));
}

bool
FmdAttrHandler::LocalPutFmd(eos::common::FileId::fileid_t fid,
                            eos::common::FileSystem::fsid_t fsid,
                            const eos::common::FmdHelper& fmd)
{
  return LocalPutFmd(FmdAttrHandler::GetPath(fid, fsid), fmd);
}

void
FmdAttrHandler::LocalDeleteFmd(eos::common::FileId::fileid_t fid,
                               eos::common::FileSystem::fsid_t fsid)
{
  return LocalDeleteFmd(FmdAttrHandler::GetPath(fid, fsid));
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

  // Check the various conditions
  if (!do_create) {
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
      if ((fmd->mProtoFmd.disksize() &&
          (fmd->mProtoFmd.disksize() != eos::common::FmdHelper::UNDEF) &&
          (fmd->mProtoFmd.disksize() != fmd->mProtoFmd.size())) ||
         (fmd->mProtoFmd.mgmsize() &&
          (fmd->mProtoFmd.mgmsize() != eos::common::FmdHelper::UNDEF) &&
          (fmd->mProtoFmd.mgmsize() != fmd->mProtoFmd.size()))) {
        eos_crit("msg=\"size mismatch disk/mgm vs memory\" fxid=%08llx "
                 "fsid=%lu size=%llu disksize=%llu mgmsize=%llu",
                 fid, (unsigned long) fsid, fmd->mProtoFmd.size(),
                 fmd->mProtoFmd.disksize(), fmd->mProtoFmd.mgmsize());
        return nullptr;
      }

      if ((fmd->mProtoFmd.filecxerror() == 1) ||
          (fmd->mProtoFmd.mgmchecksum().length() &&
           (fmd->mProtoFmd.mgmchecksum() != fmd->mProtoFmd.checksum()))) {
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
  } // !do_create
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

} // namespace eos::fst
