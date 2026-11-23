//------------------------------------------------------------------------------
// Filesystem operations for the Xio server.
//
// This is the same header as src/XrdXioFS/include/XioFs.hh in the XRootD
// XrdHttp2 tree. Keep the two copies identical: EOS backends and the Xio
// server share this vtable.
//
// The server thread pool calls Fs::handle. Results are HTTP responses.
// This header has no XRootD protocol, bridge, or SFS types.
//
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#ifndef XIOFS_FS_HH
#define XIOFS_FS_HH

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace XioFS {

struct Credential {
  uint32_t uid{static_cast<uint32_t>(-1)};
  uint32_t gid{static_cast<uint32_t>(-1)};
  std::string name;
  // User bearer. Set only when it is not the internal daemon token.
  // The backend verifies it. A client-supplied uid is not accepted.
  std::string token;
  // krb5, gsi, or sss after the listener has checked that credential.
  std::string prot;
  std::string endorsement;
};

// One HTTP request after nghttp2 has assembled it.
struct FsRequest {
  std::string method;
  std::string path;
  std::string query;
  std::vector<std::pair<std::string, std::string>> headers;
  std::string body;
  Credential cred;
  // Set when the TLS handshake verified a client certificate.
  std::string peer;
  bool tls{false};

  const std::string *header(const char *name) const;
};

// What the lane worker hands back to the connection thread.
struct FsResult {
  // errno-style. 0 means success. Ignored when http_status is set.
  int err{0};
  // 0 means derive a status from err and the method.
  int http_status{0};
  std::string body;
  std::string content_type{"application/octet-stream"};
  // Absolute URL. The server answers 307 Location.
  std::string redirect;
  // Seconds. The server answers 503 Retry-After.
  int stall_sec{0};
  std::vector<std::pair<std::string, std::string>> headers;
};

// Backend implemented by a POSIX export, or by EOS, or by anything else.
class Fs {
public:
  virtual ~Fs() = default;
  virtual void handle(const FsRequest &req, FsResult &out) = 0;
};

// Map an errno to an HTTP status. 0 becomes 200.
int errnoToHttp(int err);

} // namespace XioFS

#endif
