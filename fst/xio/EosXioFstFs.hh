//------------------------------------------------------------------------------
// FST backend for an Xio server.
//
// Range GET, PATCH, and close go through EosFstOfsFile and the layouts.
// The Xio lane calls this. XrdOucErrInfo stays inside this translation.
//------------------------------------------------------------------------------
#ifndef EOS_FST_XIOFS_HH
#define EOS_FST_XIOFS_HH

#include "common/xio/XioFs.hh"
#include "fst/EosFstOfsFile.hh"
#include <XrdSec/XrdSecEntity.hh>
#include <cstdint>
#include <ctime>
#include <map>
#include <memory>
#include <mutex>
#include <string>

namespace eos {
namespace fst {

class EosXioFstFs : public XioFS::Fs
{
public:
  ~EosXioFstFs() override;
  void handle(const XioFS::FsRequest& req, XioFS::FsResult& out) override;

private:
  // One EosFstOfsFile for a client session. Layouts and TPC issue many
  // reads and writes against a single open, and close is what commits.
  struct HeldFile {
    std::string name;
    XrdSecEntity ent;
    std::unique_ptr<EosFstOfsFile> file;
    time_t created{0};
    HeldFile() : ent("xio") {}
  };
  // True when the request carries x-xio-open or x-xio-handle.
  bool session(const XioFS::FsRequest& req, XioFS::FsResult& out,
               const std::string& method);
  std::mutex held_mu_;
  // unique_ptr so the XrdSecEntity address stays put. XrdOucErrInfo and the
  // layout keep that pointer for the life of the open.
  std::map<std::string, std::unique_ptr<HeldFile>> held_;
  uint64_t next_id_{1};
};

} // namespace fst
} // namespace eos

#endif
