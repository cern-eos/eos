#include "FmdAttr.hh"
#include "fst/io/local/LocalIo.hh"
#include "fst/XrdFstOfs.hh"

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
  int result = localIo.attrGet(gFmdAttrName.c_str(), attrval);
  if (result != 0) {
    eos_err("Failed to Fmd Attribute at path:%s, errno=%d", path.c_str(), errno);
    return {false, eos::common::FmdHelper{}};
  }

  eos::common::FmdHelper fmd;
  bool status = fmd.mProtoFmd.ParsePartialFromString(attrval);
  return {status, std::move(fmd)};
}

bool
FmdAttrHandler::LocalPutFmd(const std::string& path, const eos::common::FmdHelper& fmd)
{
  LocalIo localio {path};
  std::string attrval;
  fmd.mProtoFmd.SerializePartialToString(&attrval);

  int result = localio.attrSet(gFmdAttrName, attrval);
  bool err_status = result != 0;
  if (err_status) {
    eos_err("Failed to Set Fmd Attribute at path:%s", path.c_str());
  }
  return err_status;
}

void
FmdAttrHandler::LocalDeleteFmd(const std::string& path)
{
  LocalIo localio {path};
  if (int rc = localio.attrDelete(gFmdAttrName.c_str());
      rc != 0 && rc != ENOATTR) {
    eos_err("Failed to Delete Fmd Attribute at path:%s, rc=%d", path.c_str(), rc)
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
} // namespace eos::fst
