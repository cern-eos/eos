//------------------------------------------------------------------------------
// HTTP/2 listener in front of EosXioFstFs.
//------------------------------------------------------------------------------
#include "fst/xio/EosXioFstServer.hh"
#include "common/Logging.hh"

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <string>
#include <strings.h>

#if defined(EOS_HAVE_XIO)
#include "fst/xio/EosXioFstFs.hh"
#include "XioServer.hh"
#include <memory>
#endif

namespace eos {
namespace fst {

#if defined(EOS_HAVE_XIO)

namespace {
std::unique_ptr<XioFS::Server> gXio;
}

int StartXioServer()
{
  const char* port_env = getenv("EOS_FST_XIO_PORT");
  if (!port_env || !port_env[0]) {
    return 0;
  }
  int port = std::atoi(port_env);
  if (port <= 0) {
    eos_static_err("msg=\"EOS_FST_XIO_PORT is not a port\" value=\"%s\"",
                   port_env);
    return EINVAL;
  }
  if (gXio) {
    return 0;
  }
  XioFS::Server::Options opt;
  opt.port = port;
  opt.workers = 8;
  if (const char* bind = getenv("EOS_FST_XIO_BIND")) {
    opt.bind_host = bind;
  } else {
    opt.bind_host = "0.0.0.0";
  }
  if (const char* cert = getenv("EOS_FST_XIO_CERT")) {
    opt.tls_cert = cert;
  }
  if (const char* key = getenv("EOS_FST_XIO_KEY")) {
    opt.tls_key = key;
  }
  gXio.reset(new XioFS::Server(std::make_shared<EosXioFstFs>(), opt));
  gXio->setAuthHook([](const XioFS::FsRequest& req, XioFS::Credential& cred) {
    const std::string* auth = req.header("authorization");
    if (!auth || auth->size() < 8 ||
        strncasecmp(auth->c_str(), "Bearer ", 7) != 0) {
      return 0;
    }
    const std::string presented = auth->substr(7);
    const char* token = getenv("EOS_XIO_INTERNAL_TOKEN");
    if (token && token[0] && presented == token) {
      cred.name = "daemon";
      cred.uid = 2;
      cred.gid = 2;
      return 0;
    }
    if (!presented.empty()) {
      cred.token = presented;
    }
    return 0;
  });
  std::string err;
  int rc = gXio->start(err);
  if (rc) {
    eos_static_err("msg=\"HTTP/2 listener failed\" port=%d err=\"%s\"",
                   port, err.c_str());
    gXio.reset();
    return rc < 0 ? -rc : rc;
  }
  eos_static_notice("msg=\"HTTP/2 listener\" port=%d tls=%d",
                    gXio->port(), gXio->tls() ? 1 : 0);
  return 0;
}

void StopXioServer()
{
  if (gXio) {
    gXio->stop();
    gXio.reset();
  }
}

int XioPort()
{
  return gXio ? gXio->port() : 0;
}

#else

int XioPort()
{
  return 0;
}

int StartXioServer()
{
  if (getenv("EOS_FST_XIO_PORT")) {
    eos_static_err("%s", "msg=\"EOS_FST_XIO_PORT is set but this build has no Xio library\"");
    return ENOTSUP;
  }
  return 0;
}

void StopXioServer() {}

#endif

} // namespace fst
} // namespace eos
