#include "mgm/utils/AttrHelper.hh"
#include "mgm/misc/Constants.hh"
#include "common/Logging.hh"
#include "common/StringUtils.hh"

namespace eos::mgm::attr
{

bool
checkDirOwner(const eos::IContainerMD::XAttrMap& attrmap, uid_t d_uid,
              gid_t d_gid, eos::common::VirtualIdentity& vid,
              bool& sticky_owner, const char* path)
{
  // -------------------------------------------------------------------------
  // Check for sys.ownerauth entries, which let people operate as the owner of
  // the directory
  // -------------------------------------------------------------------------
  sticky_owner = false;

  if (auto kv = attrmap.find(SYS_OWNER_AUTH);
      kv != attrmap.end()) {
    if (kv->second == "*") {
      sticky_owner = true;
    } else {
      std::string ownerkey = vid.prot.c_str();
      ownerkey += ":";

      if (vid.prot == "gsi") {
        ownerkey += vid.dn;
      } else {
        ownerkey += vid.uid_string;
      }

      if (kv->second.find(ownerkey) != std::string::npos) {
        eos_static_info("msg=\"client authenticated as directory owner\" path=\"%s\"uid=\"%u=>%u\" gid=\"%u=>%u\"",
                        path, vid.uid, vid.gid, d_uid, d_gid);
        // yes the client can operate as the owner, we rewrite the virtual
        // identity to the directory uid/gid pair
        vid.uid = d_uid;
        vid.gid = d_gid;
        return true;
      }
    }
  }

  return sticky_owner;
}

bool checkAtomicUpload(const eos::IContainerMD::XAttrMap& attrmap,
                       const char* atomic_cgi)
{
  int isAtomic(0);

  if (const auto& kv = attrmap.find(SYS_FORCED_ATOMIC);
      kv != attrmap.end()) {
    // This should return true for values > 0
    eos::common::StringToNumeric(kv->second, isAtomic);
  } else if (const auto& kv = attrmap.find(USER_FORCED_ATOMIC);
             kv != attrmap.end()) {
    eos::common::StringToNumeric(kv->second, isAtomic);
  } else if (atomic_cgi) {
    return true;
  }

  return isAtomic;
}

int getVersioning(const eos::IContainerMD::XAttrMap& attrmap,
                  std::string_view versioning_cgi)
{
  int versioning {0};

  if (versioning_cgi.length()) {
    eos::common::StringToNumeric(versioning_cgi, versioning);
    return versioning;
  }

  if (const auto& kv = attrmap.find(SYS_VERSIONING);
      kv != attrmap.end()) {
    eos::common::StringToNumeric(kv->second, versioning, (int)0);
  } else if (const auto& kv = attrmap.find(USER_VERSIONING);
             kv != attrmap.end()) {
    eos::common::StringToNumeric(kv->second, versioning, (int)0);
  }

  return versioning;
}

bool getValue(const eos::IContainerMD::XAttrMap &attrmap,
              const std::string &key, std::string &out)
{
  if (auto kv = attrmap.find(key);
      kv != attrmap.end()) {
    out = kv->second;
    return true;
  }
  return false;
}


} // namespace eos::mgm::attr
