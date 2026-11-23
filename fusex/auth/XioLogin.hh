//------------------------------------------------------------------------------
// Build an HTTP/2 user call from the fusex credential cache.
//------------------------------------------------------------------------------
#ifndef FUSE_XIO_LOGIN_HH_
#define FUSE_XIO_LOGIN_HH_

#if defined(EOS_HAVE_XIO)

#include "common/StringConversion.hh"
#include "common/xio/XioUser.hh"
#include "eosfuse.hh"
#include "fusex/misc/fusexrdlogin.hh"

#include <cstdlib>
#include <string>

namespace eos {
namespace fusex {

inline bool FillXioCall(uid_t uid, gid_t gid, pid_t pid, eos::common::XioCall& call,
                        std::string& scheme)
{
  scheme = "http";
  if (!fusexrdlogin::processCache) {
    return false;
  }
  ProcessSnapshot snapshot = fusexrdlogin::processCache->retrieve(
                               pid, uid, gid, false);
  if (snapshot && snapshot->getBoundIdentity() &&
      snapshot->getBoundIdentity()->getCreds()) {
    const UserCredentials& uc = snapshot->getBoundIdentity()->getCreds()->getUC();
    if (uc.type == CredentialType::KRB5 || uc.type == CredentialType::KRK5 ||
        uc.type == CredentialType::KCM) {
      call.krb5 = true;
      if (uc.type == CredentialType::KRB5) {
        call.krb5cc = uc.fname;
      } else if (uc.type == CredentialType::KRK5) {
        call.krb5cc = uc.keyring;
      } else {
        call.krb5cc = uc.kcm;
      }
    } else if (uc.type == CredentialType::X509) {
      call.cert = uc.fname;
      call.key = uc.fname;
      scheme = "https";
    } else if (uc.type == CredentialType::ZTN ||
               uc.type == CredentialType::OAUTH2) {
      if (!uc.endorsement.empty() &&
          !eos::common::XioIsInternalToken(uc.endorsement)) {
        call.bearer = uc.endorsement;
      } else {
        std::string token;
        std::string terr;
        if (XioFS::loadBearerToken(uid, token, terr) &&
            !eos::common::XioIsInternalToken(token)) {
          call.bearer = std::move(token);
        } else if (!uc.fname.empty() &&
                   eos::common::StringConversion::LoadFileIntoString(
                     uc.fname.c_str(), token) &&
                   !eos::common::XioIsInternalToken(token)) {
          call.bearer = std::move(token);
        }
      }
      if (std::getenv("EOS_MGM_XIO_TLS")) {
        scheme = "https";
      }
    } else if (uc.type == CredentialType::SSS) {
      scheme = "https";
      call.sss = uc.username.empty() ? std::to_string(uid) : uc.username;
      if (!uc.endorsement.empty()) {
        call.sss += " ";
        call.sss += uc.endorsement;
      }
    }
  }
  if (call.bearer.empty() && !call.krb5 && call.cert.empty() && call.sss.empty()) {
    const std::string bearer = eos::common::XioUserBearer(uid);
    if (!bearer.empty()) {
      call.bearer = bearer;
    }
  }
  if (call.bearer.empty() && !call.krb5 && call.cert.empty() && call.sss.empty()) {
    const bool unix_ok = EosFuse::Instance().Config().auth.use_user_unix &&
                         (uid || EosFuse::Instance().Config().auth.use_root_unix);
    if (!unix_ok) {
      return false;
    }
    scheme = "https";
    call.unix_id = true;
    call.uid = uid;
    call.gid = gid;
  }
  if (const char* ca = std::getenv("EOS_MGM_XIO_CA")) {
    call.cacert = ca;
    call.verify_peer = true;
    if (call.cert.empty() && call.sss.empty() && !call.unix_id &&
        std::getenv("EOS_MGM_XIO_TLS")) {
      scheme = "https";
    }
  }
  return true;
}

} // namespace fusex
} // namespace eos

#endif
#endif
