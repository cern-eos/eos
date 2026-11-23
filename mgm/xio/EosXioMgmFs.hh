//------------------------------------------------------------------------------
// MGM backend for an Xio server.
//
// Namespace, mkdir, delete, rename, and open-for-redirect go through the
// existing EosMgmOfs methods. The Xio server never sees XrdOucErrInfo or
// XrdSecEntity; this file is the translation.
//------------------------------------------------------------------------------
#ifndef EOS_MGM_XIOFS_HH
#define EOS_MGM_XIOFS_HH

#include "common/xio/XioFs.hh"

namespace eos {
namespace mgm {

class EosXioMgmFs : public XioFS::Fs
{
public:
  void handle(const XioFS::FsRequest& req, XioFS::FsResult& out) override;
};

} // namespace mgm
} // namespace eos

#endif
