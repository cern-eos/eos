//------------------------------------------------------------------------------
// FileIo over XioFS::Client (HTTP/2). URLs are xiohttp:// and xiohttps://.
//------------------------------------------------------------------------------
#ifndef EOS_FST_XIOIO_HH
#define EOS_FST_XIOIO_HH

#include "fst/io/FileIo.hh"

namespace eos {
namespace fst {

class XioIo : public FileIo
{
public:
  explicit XioIo(std::string path);
  ~XioIo() override;

  // Use the process login (bearer, Kerberos, certificate, or uid/gid)
  // instead of the daemon token.
  void useProcessAuth() { user_auth_ = true; }
  int lastHttpStatus() const { return last_http_; }

  // Empty when in is not root:// or the HTTP/2 port is unset.
  static std::string UserUrl(const std::string& root_url);
  // root://host:xrootport/path becomes xiohttp://host:EOS_FST_XIO_PORT/path.
  // Empty when the FST HTTP/2 port is unset. The daemon token authorizes this.
  static std::string ServiceUrl(const std::string& root_url);
  static int UserOpaque(const std::string& endpoint, const std::string& request,
                        std::string& body, int& http);
  static int UserStat(const std::string& root_url, struct stat* buf, int& http);
  // One request with the process login. http is 0 when the listener is not
  // reached. hdr_name/hdr_value are an optional extra request header.
  static int UserExchange(const std::string& method, const std::string& root_url,
                          std::string& body, int& http,
                          const char* hdr_name = nullptr,
                          const char* hdr_value = nullptr);

  int fileOpen(XrdSfsFileOpenMode flags, mode_t mode = 0,
               const std::string& opaque = "", uint16_t timeout = 0) override;
  std::future<XrdCl::XRootDStatus>
  fileOpenAsync(XrdSfsFileOpenMode flags, mode_t mode = 0,
                const std::string& opaque = "", uint16_t timeout = 0) override;
  int64_t fileRead(XrdSfsFileOffset offset, char* buffer,
                   XrdSfsXferSize length, uint16_t timeout = 0) override;
  int64_t fileReadPrefetch(XrdSfsFileOffset offset, char* buffer,
                           XrdSfsXferSize length, uint16_t timeout = 0) override;
  int64_t fileReadAsync(XrdSfsFileOffset offset, char* buffer,
                        XrdSfsXferSize length, uint16_t timeout = 0) override;
  int64_t fileReadV(XrdCl::ChunkList& chunkList, uint16_t timeout = 0) override;
  int64_t fileReadVAsync(XrdCl::ChunkList& chunkList,
                         uint16_t timeout = 0) override;
  int64_t fileWrite(XrdSfsFileOffset offset, const char* buffer,
                    XrdSfsXferSize length, uint16_t timeout = 0) override;
  int64_t fileWriteAsync(XrdSfsFileOffset offset, const char* buffer,
                         XrdSfsXferSize length, uint16_t timeout = 0) override;
  std::future<XrdCl::XRootDStatus>
  fileWriteAsync(const char* buffer, XrdSfsFileOffset offset,
                 XrdSfsXferSize length) override;
  int fileTruncate(XrdSfsFileOffset offset, uint16_t timeout = 0) override;
  std::future<XrdCl::XRootDStatus>
  fileTruncateAsync(XrdSfsFileOffset offset, uint16_t timeout = 0) override;
  int fileFallocate(XrdSfsFileOffset length) override;
  int fileFdeallocate(XrdSfsFileOffset fromOffset,
                      XrdSfsFileOffset toOffset) override;
  int fileRemove(uint16_t timeout = 0) override;
  int fileSync(uint16_t timeout = 0) override;
  void* fileGetAsyncHandler() override;
  int fileExists() override;
  int fileClose(uint16_t timeout = 0) override;
  int fileStat(struct stat* buf, uint16_t timeout = 0) override;
  int fileFctl(const std::string& cmd, uint16_t timeout = 0) override;
  int attrSet(const char* name, const char* value, size_t len) override;
  int attrSet(std::string name, std::string value) override;
  int attrGet(const char* name, char* value, size_t& size) override;
  int attrGet(std::string name, std::string& value) override;
  int attrDelete(const char* name) override;
  int attrList(std::vector<std::string>& list) override;
  FileIo::FtsHandle* ftsOpen(int options = 0) override;
  std::string ftsRead(FtsHandle* handle) override;
  int ftsClose(FtsHandle* handle) override;
  int Statfs(struct statfs* statFs) override;

private:
  struct Session;
  std::unique_ptr<Session> session_;
  std::string rel_;
  bool user_auth_{false};
  int last_http_{0};
  int fail(int err, const std::string& msg);
};

FileIo* MakeXioIo(const std::string& path);

} // namespace fst
} // namespace eos

#endif
