#include "mgm/utils/AttrHelper.hh"
#include "common/Constants.hh"
#include "common/Logging.hh"
#include "common/StringUtils.hh"
#include "mgm/misc/Constants.hh"
#include "namespace/utils/Attributes.hh"
#include <algorithm>
#include <array>
#include <iomanip>
#include <set>
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
      std::string ownerkey = vid.prot.c_str();
      ownerkey += ":";

      if (vid.prot == "gsi") {
        ownerkey += vid.dn;
      } else {
        ownerkey += vid.uid_string;
      }

      // Anchor the match with ',' separators so that a partial prefix in the
      // comma-separated list cannot match a longer key (e.g. attribute value
      // 'krb5:alice123' must NOT match an ownerkey 'krb5:alice').
      std::string haystack;
      haystack.reserve(kv->second.size() + 2);
      haystack.append(1, ',');
      haystack.append(kv->second);
      haystack.append(1, ',');
      std::string needle;
      needle.reserve(ownerkey.size() + 2);
      needle.append(1, ',');
      needle.append(ownerkey);
      needle.append(1, ',');

      if (haystack.find(needle) != std::string::npos) {
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

eos::IFileMD::XAttrMap
getCarryOverXattrs(const eos::IFileMD::XAttrMap& old_xattrs)
{
  // Attributes describing the previous instance of the file
  static const std::set<std::string> skip_keys{
      eos::common::EOS_FS_TRACKING_ATTR, eos::common::EOS_DTRACE_ATTR,
      eos::common::EOS_VTRACE_ATTR, eos::common::EOS_UTRACE_ATTR};
  // Attributes EOS manages itself (e.g. btime, hard link count), temporary
  // ones (e.g. forced ETag), the FUSE bookkeeping of the previous file and the
  // tape metadata of its contents - CTA gives the new file its own at creation
  static constexpr std::array skip_prefixes{
      eos::common::EOS_INTERNAL_ATTR_PREFIX, eos::common::EOS_TMP_ATTR_PREFIX,
      eos::common::FUSEX_ATTR_PREFIX,        eos::common::ARCHIVE_ATTR_PREFIX,
      eos::common::CTA_ATTR_PREFIX,          eos::common::RETRIEVE_ATTR_PREFIX};
  const auto has_skip_prefix = [](const std::string& key) {
    return std::any_of(
        skip_prefixes.begin(), skip_prefixes.end(),
        [&key](const char* prefix) { return eos::common::startsWith(key, prefix); });
  };
  const auto& enc_keys = eos::kAttrEncryptionKeys;
  eos::IFileMD::XAttrMap out;

  for (const auto& [key, val] : old_xattrs) {
    // The new file keeps the encryption keys it is written with
    if (!skip_keys.count(key) && !has_skip_prefix(key) &&
        std::find(enc_keys.begin(), enc_keys.end(), key) == enc_keys.end()) {
      out.emplace(key, val);
    }
  }

  return out;
}

} // namespace eos::mgm::attr
