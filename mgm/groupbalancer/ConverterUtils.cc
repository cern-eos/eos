#include "ConverterUtils.hh"

#include "common/StringUtils.hh"
#include "common/Logging.hh"
#include "common/LayoutId.hh"
#include "namespace/Prefetcher.hh"
#include "mgm/XrdMgmOfs.hh"
#include "namespace/interface/IView.hh"
#include <XrdOuc/XrdOucString.hh>

namespace eos::mgm::group_balancer
{
bool PrefixFilter::operator()(std::string_view path) {
    return eos::common::startsWith(path, prefix);
}

std::string
getFileProcTransferNameAndSize(eos::common::FileId::fileid_t fid,
                               const std::string& target_group, uint64_t* size,
                               const SkipFileFn& skip_file_fn)
{
  char fileName[1024];
  std::shared_ptr<eos::IFileMD> fmd;
  eos::common::LayoutId::layoutid_t layoutid = 0;
  eos::common::FileId::fileid_t fileid = 0;
  {
    eos::Prefetcher::prefetchFileMDAndWait(gOFS->eosView, fid);

    try {
      fmd = gOFS->eosFileService->getFileMD(fid);
      std::string fmdUri = gOFS->eosView->getUri(fmd.get());
      auto fmdLock = eos::MDLocking::readLock(fmd.get());
      layoutid = fmd->getLayoutId();
      fileid = fmd->getId();

      if (fmd->getContainerId() == 0) {
        return std::string("");
      }

      if (skip_file_fn && skip_file_fn(fmdUri)) {
        return std::string("");
      }

      if (size) {
        *size = fmd->getSize();
      }

      eos_static_debug("msg=\"found file for transfering\" fid=\"%08llx\"",
                       fileid);
    } catch (eos::MDException& e) {
      eos_static_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),
                       e.getMessage().str().c_str());
      return std::string("");
    }
  }
  snprintf(fileName, 1024, "%s/%016llx:%s#%08lx",
           gOFS->MgmProcConversionPath.c_str(), fileid, target_group.c_str(),
           (unsigned long)layoutid);
  return std::string(fileName);
}

} // eos::mgm::group_balancer
