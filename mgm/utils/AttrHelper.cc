#include "mgm/utils/AttrHelper.hh"
#include "mgm/Constants.hh"
#include "common/Logging.hh"

namespace eos::mgm::attr {

bool
checkStickyDirOwner(const eos::IContainerMD::XAttrMap& attrmap,
            uid_t d_uid,
            gid_t d_gid,
            eos::common::VirtualIdentity& vid,
            const char* path)
{
  // -------------------------------------------------------------------------
  // Check for sys.ownerauth entries, which let people operate as the owner of
  // the directory
  // -------------------------------------------------------------------------
  bool sticky_owner = false;

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
      }
    }
  }
  return sticky_owner;

}


} // namespace eos::mgm::attr
