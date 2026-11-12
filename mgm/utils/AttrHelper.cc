#include "mgm/utils/AttrHelper.hh"
#include "common/Logging.hh"
#include "common/StringUtils.hh"
#include "mgm/misc/Constants.hh"
#include <iomanip>
#include <sstream>

namespace eos::mgm::attr
{

std::string
formatQuotedValue(const std::string& value, bool json)
{
  if (!json) {
    return '"' + value + '"';
  }
  // TokenizeQuoted decodes escapes before ConvertOutputToJsonFormat runs.
  std::ostringstream output;
  output << std::quoted(value);
  return output.str();
}

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
      eos_static_info("msg=\"client authenticated as directory owner (sticky '*')\" "
                      "path=\"%s\" uid=\"%u=>%u\" gid=\"%u=>%u\"",
                      path, vid.uid, vid.gid, d_uid, d_gid);
      sticky_owner = true;
    } else {
      // Anchor the match with ',' separators so that a partial prefix in the
      // comma-separated list cannot match a longer key (e.g. attribute value
      // 'krb5:alice123' must NOT match an ownerkey 'krb5:alice').
      std::string haystack;
      haystack.reserve(kv->second.size() + 2);
      haystack.append(1, ',');
      haystack.append(kv->second);
      haystack.append(1, ',');

      auto listed = [&](const std::string& id) {
        if (id.empty()) {
          return false;
        }

        const std::string needle = "," + std::string(vid.prot.c_str()) + ":" + id + ",";
        return haystack.find(needle) != std::string::npos;
      };

      // GSI is matched by DN. Other protocols match the mapped account and
      // the authenticated client name: an SSS login "daemon" stays uid
      // nobody when no vid map exists, but sys.owner.auth=sss:daemon must
      // still select that principal.
      bool matched = false;

      if (vid.prot == "gsi") {
        matched = listed(vid.dn);
      } else {
        matched = listed(vid.uid_string);

        if (!matched && vid.name.length()) {
          matched = listed(std::string(vid.name.c_str()));
        }
      }

      if (matched) {
        eos_static_info("msg=\"client authenticated as directory owner\" path=\"%s\" uid=\"%u=>%u\" gid=\"%u=>%u\"",
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
