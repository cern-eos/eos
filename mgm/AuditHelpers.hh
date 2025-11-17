#ifndef EOS_MGM_AUDIT_HELPERS_HH
#define EOS_MGM_AUDIT_HELPERS_HH

#include "proto/Audit.pb.h"
#include "namespace/interface/IFileMD.hh"
#include "namespace/interface/IContainerMD.hh"
#include "namespace/utils/Checksum.hh"
#include <string>
#include <cstdio>

namespace eos {
namespace mgm {
namespace auditutil {

static inline void buildStatFromFileMD(const std::shared_ptr<eos::IFileMD>& fmd,
                                       eos::audit::Stat& out,
                                       bool includeSize = true,
                                       bool includeChecksum = false,
                                       bool includeNs = true)
{
  if (!fmd) return;
  eos::IFileMD::ctime_t cts, mts;
  fmd->getCTime(cts);
  fmd->getMTime(mts);
  out.set_ctime(cts.tv_sec);
  out.set_mtime(mts.tv_sec);
  if (includeNs) {
    char cbuf[64], mbuf[64];
    std::snprintf(cbuf, sizeof(cbuf), "%ld.%09ld", (long)cts.tv_sec, (long)cts.tv_nsec);
    std::snprintf(mbuf, sizeof(mbuf), "%ld.%09ld", (long)mts.tv_sec, (long)mts.tv_nsec);
    out.set_ctime_ns(cbuf);
    out.set_mtime_ns(mbuf);
  }
  out.set_uid(fmd->getCUid());
  out.set_gid(fmd->getCGid());
  uint32_t am = (fmd->getFlags() & 07777);
  out.set_mode(am);
  char amo[8];
  std::snprintf(amo, sizeof(amo), "0%04o", am);
  out.set_mode_octal(amo);
  if (includeSize) out.set_size(fmd->getSize());
  if (includeChecksum) {
    std::string hex;
    eos::appendChecksumOnStringAsHex(fmd.get(), hex);
    if (!hex.empty()) out.set_checksum(hex);
  }
}

static inline void buildStatFromContainerMD(const std::shared_ptr<eos::IContainerMD>& cmd,
                                            eos::audit::Stat& out,
                                            bool includeNs = true)
{
  if (!cmd) return;
  eos::IFileMD::ctime_t cts, mts;
  cmd->getCTime(cts);
  cmd->getMTime(mts);
  out.set_ctime(cts.tv_sec);
  out.set_mtime(mts.tv_sec);
  if (includeNs) {
    char cbuf[64], mbuf[64];
    std::snprintf(cbuf, sizeof(cbuf), "%ld.%09ld", (long)cts.tv_sec, (long)cts.tv_nsec);
    std::snprintf(mbuf, sizeof(mbuf), "%ld.%09ld", (long)mts.tv_sec, (long)mts.tv_nsec);
    out.set_ctime_ns(cbuf);
    out.set_mtime_ns(mbuf);
  }
  out.set_uid(cmd->getCUid());
  out.set_gid(cmd->getCGid());
  uint32_t am = (cmd->getMode() & 07777);
  out.set_mode(am);
  char amo[8];
  std::snprintf(amo, sizeof(amo), "0%04o", am);
  out.set_mode_octal(amo);
}

} // namespace auditutil
} // namespace mgm
} // namespace eos

#endif
