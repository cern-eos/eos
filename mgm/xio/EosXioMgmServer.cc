//------------------------------------------------------------------------------
// HTTP/2 listener in front of EosXioMgmFs.
//------------------------------------------------------------------------------
#include "mgm/xio/EosXioMgmServer.hh"
#include "common/Logging.hh"

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <string>
#include <strings.h>
#include <gssapi/gssapi.h>

#if defined(EOS_HAVE_XIO)
#include "mgm/xio/EosXioMgmFs.hh"
#include "XioServer.hh"
#include <memory>
#endif

namespace eos {
namespace mgm {

#if defined(EOS_HAVE_XIO)

namespace {

std::unique_ptr<XioFS::Server> gXio;

bool b64dec(const std::string& in, std::string& out)
{
  static const int8_t kDec[256] = {
    -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
    -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
    -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,62,-1,-1,-1,63,
    52,53,54,55,56,57,58,59,60,61,-1,-1,-1,-1,-1,-1,
    -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,10,11,12,13,14,
    15,16,17,18,19,20,21,22,23,24,25,-1,-1,-1,-1,-1,
    -1,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,
    41,42,43,44,45,46,47,48,49,50,51,-1,-1,-1,-1,-1
  };
  out.clear();
  int val = 0;
  int valb = -8;
  for (unsigned char c : in) {
    if (c == '=' || c == '\n' || c == '\r') {
      break;
    }
    const int d = kDec[c];
    if (d < 0) {
      return false;
    }
    val = (val << 6) + d;
    valb += 6;
    if (valb >= 0) {
      out.push_back(static_cast<char>((val >> valb) & 0xff));
      valb -= 8;
    }
  }
  return !out.empty();
}

std::string schemeValue(const std::string& header, const char* scheme)
{
  const size_t n = std::strlen(scheme);
  if (header.size() <= n || header[n] != ' ' ||
      strncasecmp(header.c_str(), scheme, n) != 0) {
    return {};
  }
  return header.substr(n + 1);
}

std::string acceptNegotiate(const std::string& b64)
{
  std::string raw;
  if (!b64dec(b64, raw)) {
    return {};
  }
  gss_ctx_id_t ctx = GSS_C_NO_CONTEXT;
  gss_name_t src = GSS_C_NO_NAME;
  gss_buffer_desc inbuf;
  inbuf.length = raw.size();
  inbuf.value = raw.data();
  gss_buffer_desc outbuf = GSS_C_EMPTY_BUFFER;
  OM_uint32 minor = 0;
  OM_uint32 major = gss_accept_sec_context(
                      &minor, &ctx, GSS_C_NO_CREDENTIAL, &inbuf,
                      GSS_C_NO_CHANNEL_BINDINGS, &src, nullptr, &outbuf,
                      nullptr, nullptr, nullptr);
  if (outbuf.length) {
    gss_release_buffer(&minor, &outbuf);
  }
  if (GSS_ERROR(major) || src == GSS_C_NO_NAME) {
    if (src != GSS_C_NO_NAME) {
      gss_release_name(&minor, &src);
    }
    if (ctx != GSS_C_NO_CONTEXT) {
      gss_delete_sec_context(&minor, &ctx, GSS_C_NO_BUFFER);
    }
    return {};
  }
  gss_buffer_desc displayed = GSS_C_EMPTY_BUFFER;
  major = gss_display_name(&minor, src, &displayed, nullptr);
  std::string principal;
  if (!GSS_ERROR(major) && displayed.value && displayed.length) {
    principal.assign(static_cast<const char*>(displayed.value), displayed.length);
  }
  if (displayed.length) {
    gss_release_buffer(&minor, &displayed);
  }
  gss_release_name(&minor, &src);
  if (ctx != GSS_C_NO_CONTEXT) {
    gss_delete_sec_context(&minor, &ctx, GSS_C_NO_BUFFER);
  }
  return principal;
}

}

int StartXioServer()
{
  const char* port_env = getenv("EOS_MGM_XIO_PORT");
  if (!port_env || !port_env[0]) {
    return 0;
  }
  int port = std::atoi(port_env);
  if (port <= 0) {
    eos_static_err("msg=\"EOS_MGM_XIO_PORT is not a port\" value=\"%s\"",
                   port_env);
    return EINVAL;
  }
  if (gXio) {
    return 0;
  }
  XioFS::Server::Options opt;
  opt.port = port;
  opt.workers = 8;
  if (const char* bind = getenv("EOS_MGM_XIO_BIND")) {
    opt.bind_host = bind;
  } else {
    opt.bind_host = "0.0.0.0";
  }
  if (const char* cert = getenv("EOS_MGM_XIO_CERT")) {
    opt.tls_cert = cert;
  }
  if (const char* key = getenv("EOS_MGM_XIO_KEY")) {
    opt.tls_key = key;
  }
  if (const char* ca = getenv("EOS_MGM_XIO_CLIENT_CA")) {
    opt.tls_client_ca = ca;
  }
  gXio.reset(new XioFS::Server(std::make_shared<EosXioMgmFs>(), opt));
  gXio->setAuthHook([](const XioFS::FsRequest& req, XioFS::Credential& cred) {
    const std::string* auth = req.header("authorization");
    if (auth) {
      const std::string neg = schemeValue(*auth, "Negotiate");
      if (!neg.empty()) {
        const std::string principal = acceptNegotiate(neg);
        if (principal.empty()) {
          return EACCES;
        }
        cred.prot = "krb5";
        cred.name = principal;
        return 0;
      }
      const std::string presented = schemeValue(*auth, "Bearer");
      if (!presented.empty()) {
        const char* token = getenv("EOS_XIO_INTERNAL_TOKEN");
        if (token && token[0] && presented == token) {
          cred.name = "daemon";
          cred.uid = 2;
          cred.gid = 2;
          return 0;
        }
        cred.token = presented;
        return 0;
      }
      if (req.tls) {
        const std::string sss = schemeValue(*auth, "SSS");
        if (!sss.empty()) {
          const auto sp = sss.find(' ');
          const std::string name = sp == std::string::npos ? sss : sss.substr(0, sp);
          if (!name.empty() && name != "daemon") {
            cred.prot = "sss";
            cred.name = name;
            if (sp != std::string::npos) {
              cred.endorsement = sss.substr(sp + 1);
            }
            return 0;
          }
        }
      }
    }
    if (!req.peer.empty()) {
      cred.prot = "gsi";
      cred.name = req.peer;
      return 0;
    }
    if (req.tls) {
      const std::string* uid_h = req.header("x-eos-uid");
      const std::string* gid_h = req.header("x-eos-gid");
      if (uid_h && gid_h && !uid_h->empty() && !gid_h->empty()) {
        char* uend = nullptr;
        char* gend = nullptr;
        errno = 0;
        unsigned long uid = std::strtoul(uid_h->c_str(), &uend, 10);
        unsigned long gid = std::strtoul(gid_h->c_str(), &gend, 10);
        if (uend && *uend == '\0' && gend && *gend == '\0' && errno == 0 &&
            uid <= 0xfffffffful && gid <= 0xfffffffful) {
          cred.prot = "unix";
          cred.uid = static_cast<uint32_t>(uid);
          cred.gid = static_cast<uint32_t>(gid);
        }
      }
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

#else

int StartXioServer()
{
  if (getenv("EOS_MGM_XIO_PORT")) {
    eos_static_err("%s", "msg=\"EOS_MGM_XIO_PORT is set but this build has no Xio library\"");
    return ENOTSUP;
  }
  return 0;
}

void StopXioServer() {}

#endif

} // namespace mgm
} // namespace eos
