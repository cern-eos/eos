#include "FmdHandler.hh"

namespace eos::fst {

class FileIo;

class FmdAttrHandler final: public FmdHandler
{
public:
  // We don't maintain any local other than the mutexes held by the parent class
  ~FmdAttrHandler() = default;

  fmd_handler_t get_type() override {
    return fmd_handler_t::ATTR;
  }


  void LocalDeleteFmd(eos::common::FileId::fileid_t fid,
                      eos::common::FileSystem::fsid_t fsid) override;

  bool Commit(eos::common::FmdHelper* fmd, bool lockit = true) override;

  virtual std::unique_ptr<eos::common::FmdHelper>
  LocalGetFmd(eos::common::FileId::fileid_t fid,
              eos::common::FileSystem::fsid_t fsid,
              bool force_retrieve = false, bool do_create = false,
              uid_t uid = 0, gid_t gid = 0,
              eos::common::LayoutId::layoutid_t layoutid = 0) override;

  std::unique_ptr<eos::common::FmdHelper>
  LocalGetFmd(std::string filepath,
              bool force_retrieve = false, bool do_create = false,
              uid_t uid = 0, gid_t gid = 0,
              eos::common::LayoutId::layoutid_t layoutid = 0);

  //TODO: Move this to a more appropriate location somewhere higher, this is
  //nothing related to AttrHandler as such
  static std::string GetPath(eos::common::FileId::fileid_t fid,
                             eos::common::FileSystem::fsid_t fsid);
private:
  bool LocalPutFmd(eos::common::FileId::fileid_t fid,
                   eos::common::FileSystem::fsid_t fsid,
                   const eos::common::FmdHelper& fmd) override;

  int CreateFile(FileIo* fio);

  std::pair<bool,eos::common::FmdHelper>
  LocalRetrieveFmd(eos::common::FileId::fileid_t fid,
                   eos::common::FileSystem::fsid_t fsid) override;

  bool LocalPutFmd(const std::string& path,
                   const eos::common::FmdHelper& fmd);

  std::pair<bool,eos::common::FmdHelper>
  LocalRetrieveFmd(const std::string& path);

  void LocalDeleteFmd(const std::string& path);

  bool ResetDiskInformation(eos::common::FileSystem::fsid_t fsid) override { return true; };
  bool ResetMgmInformation(eos::common::FileSystem::fsid_t fsid) override { return true; };

  void SetSyncStatus(eos::common::FileSystem::fsid_t, bool) {}
};

static constexpr auto gFmdAttrName = "user.eos.fmd";

} // namespace eos::fst
