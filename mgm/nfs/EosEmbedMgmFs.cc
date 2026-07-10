// EosEmbedMgmFS: in-process cern-nfs VFS for the EOS MGM (namespace / MDS).
// Compiled into XrdEosMgm, not libcernnfs — EOS headers (folly, namespace, …)
// are only available in the MGM link context.

#include <cernnfs/ds_fh.hpp>
#include <cernnfs/vfs.hpp>

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <XrdOuc/XrdOucEnv.hh>
#include <XrdOuc/XrdOucErrInfo.hh>
#include <XrdOuc/XrdOucString.hh>
#include <XrdSfs/XrdSfsInterface.hh>
#include <XrdSfs/XrdSfsFlags.hh>

#include "common/Definitions.hh"
#include "common/FileId.hh"
#include "common/Constants.hh"
#include "common/LayoutId.hh"
#include "common/Logging.hh"
#include "common/Mapping.hh"
#include "common/Path.hh"
#include "common/RWMutex.hh"
#include "common/SecEntity.hh"
#include "common/StringConversion.hh"
#include "common/SymKeys.hh"
#include "common/VirtualIdentity.hh"
#include "mgm/fsview/FsView.hh"
#include "mgm/ofs/XrdMgmOfs.hh"
#include "mgm/ofs/XrdMgmOfsDirectory.hh"
#include "mgm/ofs/XrdMgmOfsFile.hh"
#include "namespace/MDException.hh"
#include "namespace/Prefetcher.hh"
#include "namespace/interface/IContainerMD.hh"
#include "namespace/interface/IFileMDSvc.hh"

#include <XrdCl/XrdClFile.hh>
#include <XrdSec/XrdSecEntity.hh>

#include <atomic>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include <uuid/uuid.h>

#include <arpa/inet.h>
#include <netdb.h>

namespace cernnfs {

namespace {

std::string mgm_normalize_path(const std::string& in)
{
  if (in.empty()) {
    return std::string("/");
  }

  std::string out;
  out.reserve(in.size() + 1);
  out.push_back('/');
  size_t i = 0;

  while (i < in.size()) {
    while (i < in.size() && in[i] == '/') {
      i++;
    }

    size_t start = i;

    while (i < in.size() && in[i] != '/') {
      i++;
    }

    size_t len = i - start;

    if (len == 0) {
      break;
    }

    std::string_view seg(in.c_str() + start, len);

    if (seg == ".") {
      continue;
    }

    if (seg == "..") {
      if (out.size() > 1) {
        size_t slash = out.find_last_of('/');

        if (slash > 0) {
          out.erase(slash);
        } else {
          out.erase(1);
        }
      }

      continue;
    }

    if (out.size() > 1) {
      out.push_back('/');
    }

    out.append(seg.data(), seg.size());
  }

  return out;
}

XrdMgmOfs* g_mgm_ofs = nullptr;
std::string g_export_mount_path;
std::unique_ptr<ds_fh::Keytab> g_ds_keytab;
std::mutex g_keytab_mu;
std::atomic<std::uint64_t> g_layout_epoch{1};
std::uint8_t g_active_key_id = 0;

thread_local std::vector<eos::common::VirtualIdentity> g_vid_stack;
// AUTH_SYS uid/gid from the active NFS RPC (before EOS IdMap remapping).
thread_local uid_t g_nfs_auth_uid = static_cast<uid_t>(-1);
thread_local gid_t g_nfs_auth_gid = static_cast<gid_t>(-1);

XrdMgmOfs*
mgm_ofs()
{
  return g_mgm_ofs ? g_mgm_ofs : gOFS;
}

eos::common::VirtualIdentity&
current_vid()
{
  if (!g_vid_stack.empty()) {
    return g_vid_stack.back();
  }

  static thread_local eos::common::VirtualIdentity s_root =
    eos::common::VirtualIdentity::Root();
  return s_root;
}

int
errno_from_errinfo(XrdOucErrInfo& err)
{
  const int code = err.getErrInfo();
  return code > 0 ? code : EIO;
}

template<typename T>
Result<T>
result_from_errinfo(XrdOucErrInfo& err)
{
  return Result<T>::make_error(err.getErrText(), errno_from_errinfo(err));
}

FileMetadata
stat_to_metadata(const struct stat& st)
{
  FileMetadata m{};
  m.size = static_cast<uint64_t>(st.st_size);
  // st_ino/st_dev come from MGM _stat: file inodes via FileId::FidToInode,
  // container inodes as container ids — never raw file fids.
  m.file_id = static_cast<uint64_t>(st.st_ino);
  m.device_id = static_cast<uint64_t>(st.st_dev);
  m.mode = static_cast<uint32_t>(st.st_mode & 07777);
  m.is_directory = S_ISDIR(st.st_mode);
  m.is_regular_file = S_ISREG(st.st_mode);
  m.is_symlink = S_ISLNK(st.st_mode);
  m.num_links = static_cast<uint32_t>(st.st_nlink);
  // MGM overloads st_nlink with replica redundancy; metadata-only files (#Rep=0)
  // stat as nlink=0. Linux NFS clients treat that as deleted and omit READDIR
  // entries — always report at least 1 for objects that exist in the namespace.
  if (m.num_links == 0) {
    m.num_links = 1;
  }
  // Empty directories should report nlink >= 2 ( "." and ".." ). MGM _stat
  // sometimes returns 1 for container dirs; Linux mount/getattr rejects that.
  if (m.is_directory && m.num_links < 2) {
    m.num_links = 2;
  }
  m.owner_uid = st.st_uid;
  m.owner_gid = st.st_gid;
  auto to_timepoint = [](time_t sec, long nsec) -> SystemTime {
    using namespace std::chrono;
    return system_clock::time_point(seconds(sec)) + nanoseconds(nsec);
  };
#if defined(__linux__)
  m.access_time = to_timepoint(st.st_atim.tv_sec, st.st_atim.tv_nsec);
  m.modify_time = to_timepoint(st.st_mtim.tv_sec, st.st_mtim.tv_nsec);
  m.create_time = to_timepoint(st.st_ctim.tv_sec, st.st_ctim.tv_nsec);
#else
  m.access_time = to_timepoint(st.st_atime, 0);
  m.modify_time = to_timepoint(st.st_mtime, 0);
  m.create_time = to_timepoint(st.st_ctime, 0);
#endif
  return m;
}

Result<FileMetadata>
mgm_stat_path(const std::string& path, bool follow = true)
{
  XrdMgmOfs* ofs = mgm_ofs();

  if (!ofs) {
    return Result<FileMetadata>::make_error("eos-embed-mgm: MGM not configured",
                                            ENXIO);
  }

  struct stat st {};
  XrdOucErrInfo err;
  auto& vid = current_vid();

  if (ofs->_stat(path.c_str(), &st, err, vid, nullptr, nullptr, follow)) {
    return result_from_errinfo<FileMetadata>(err);
  }

  return Result<FileMetadata>(stat_to_metadata(st));
}

const EosEmbedMgmFS*
as_mgm_fs(const std::shared_ptr<VfsPath>& path)
{
  return dynamic_cast<const EosEmbedMgmFS*>(path.get());
}

std::string
keytab_path_from_env()
{
  const char* env = std::getenv("EOS_EMBED_NFS_KEYTAB");
  return (env && *env) ? env : "/etc/eos.nfs.keytab";
}

bool
ensure_keytab_loaded()
{
  std::lock_guard<std::mutex> lock(g_keytab_mu);

  if (g_ds_keytab && g_ds_keytab->size() > 0) {
    return true;
  }

  auto loaded = ds_fh::load_keytab_file(keytab_path_from_env());

  if (loaded.is_err()) {
    return false;
  }

  g_ds_keytab = std::make_unique<ds_fh::Keytab>(std::move(loaded.value()));

  for (std::uint8_t kid = 0; kid < 255; ++kid) {
    if (g_ds_keytab->lookup(kid)) {
      g_active_key_id = kid;
      return true;
    }
  }

  return false;
}

int
fst_nfs_port()
{
  const char* env = std::getenv("EOS_FST_NFSPORT");

  if (env && *env) {
    char* end = nullptr;
    const long port = std::strtol(env, &end, 10);

    if (end != env && *end == '\0' && port > 0 && port <= 65535) {
      return static_cast<int>(port);
    }
  }

  // Default distinct from EOS_MGM_NFSPORT (2094) for co-located MGM+FST nodes.
  return 2095;
}

std::string
nfs_embed_tident(const std::string& username, const std::string& host)
{
  // Dummy pid:fd (unknown for NFS). ReduceTident() strips ".0:0" for display
  // and builds "*@<host>" wildcards used by gateway / tident VID rules.
  return username + ".0:0@" + host;
}

std::string
normalize_nfs_client_host(std::string host)
{
  if (host.empty()) {
    return "localhost";
  }

  if (host == "127.0.0.1") {
    return "localhost";
  }

  return host;
}

std::string
make_universal_addr(const std::string& host, int port)
{
  // RFC 5665 universal address = "a.b.c.d.p1.p2". Linux pNFS parses this
  // with rpc_uaddr2sockaddr(), which expects a dotted IPv4 quad — not a
  // hostname with extra dots (e.g. vl-ns-04.cern.ch.8.47).
  std::string ip = host;
  if (!host.empty()) {
    struct in_addr v4{};
    if (inet_pton(AF_INET, host.c_str(), &v4) != 1) {
      addrinfo hints{};
      hints.ai_family = AF_INET;
      hints.ai_socktype = SOCK_STREAM;
      addrinfo* res = nullptr;
      if (getaddrinfo(host.c_str(), nullptr, &hints, &res) == 0 && res) {
        char ipbuf[INET_ADDRSTRLEN]{};
        auto* sa = reinterpret_cast<sockaddr_in*>(res->ai_addr);
        if (inet_ntop(AF_INET, &sa->sin_addr, ipbuf, sizeof(ipbuf))) {
          ip = ipbuf;
        }
        freeaddrinfo(res);
      }
    }
  }

  std::string uaddr = ip;
  uaddr += '.';
  uaddr += std::to_string((port >> 8) & 0xff);
  uaddr += '.';
  uaddr += std::to_string(port & 0xff);
  return uaddr;
}

XrdSecEntity*
embed_mgm_client(const eos::common::VirtualIdentity& vid)
{
  static thread_local XrdSecEntity client{};
  static thread_local char name_buf[256];
  static thread_local char host_buf[256];
  static thread_local char tident_buf[512];

  const std::string& username =
    vid.uid_string.empty() ? eos::common::Mapping::UidAsString(vid.uid)
                           : vid.uid_string;
  const std::string host =
    normalize_nfs_client_host(vid.host.empty() ? "localhost" : vid.host);
  const std::string tident =
    vid.tident.length() ? std::string(vid.tident.c_str())
                        : nfs_embed_tident(username, host);

  std::strncpy(client.prot, "unix", sizeof(client.prot) - 1);
  client.prot[sizeof(client.prot) - 1] = '\0';
  std::strncpy(name_buf, username.c_str(), sizeof(name_buf) - 1);
  name_buf[sizeof(name_buf) - 1] = '\0';
  std::strncpy(host_buf, host.c_str(), sizeof(host_buf) - 1);
  host_buf[sizeof(host_buf) - 1] = '\0';
  std::strncpy(tident_buf, tident.c_str(), sizeof(tident_buf) - 1);
  tident_buf[sizeof(tident_buf) - 1] = '\0';
  client.name = name_buf;
  client.host = host_buf;
  client.tident = tident_buf;

  return &client;
}

Result<OpenResult>
build_pnfs_layout(const std::vector<unsigned int>& replica_fsids,
                  const std::shared_ptr<eos::IFileMD>& fmd,
                  uid_t uid,
                  gid_t gid,
                  bool write_io)
{
  if (!ensure_keytab_loaded()) {
    return Result<OpenResult>::make_error(
      "eos-embed-mgm: DS keytab not loaded (set EOS_EMBED_NFS_KEYTAB)", ENOENT);
  }

  FlexFileLayout layout;
  layout.no_layoutcommit = true;
  // Writes must go to DS; reads may still use MDS until DS I/O is verified.
  layout.no_io_thru_mds = write_io;
  layout.ff_uid = uid;
  layout.ff_gid = gid;
  const int nfs_port = fst_nfs_port();
  const std::uint64_t epoch = g_layout_epoch.load();

  {
    eos::common::RWMutexReadLock fs_lock(eos::mgm::FsView::gFsView.ViewMutex);

    for (const auto fsid : replica_fsids) {
      eos::mgm::FileSystem* fs =
        eos::mgm::FsView::gFsView.mIdView.lookupByID(fsid);

      if (!fs) {
        continue;
      }

      ds_fh::Header hdr;
      hdr.inode = eos::common::FileId::FidToInode(
        static_cast<unsigned long long>(fmd->getId()));
      hdr.flags = static_cast<std::uint16_t>(fsid);
      hdr.generation = static_cast<std::uint64_t>(fmd->getCloneId());
      hdr.layout_epoch = epoch;
      hdr.key_id = g_active_key_id;

      auto enc = ds_fh::encode(hdr, *g_ds_keytab);

      if (enc.is_err()) {
        return Result<OpenResult>::make_error(enc.error(), enc.error_code());
      }

      DataServer ds;
      ds.netid = "tcp";
      ds.universal_addr = make_universal_addr(fs->GetHost(), nfs_port);
      ds.ds_fh = std::move(enc.value());
      layout.mirrors.push_back(std::move(ds));
    }
  }

  if (layout.mirrors.empty()) {
    return Result<OpenResult>::make_error("eos-embed-mgm: no data servers for file",
                                          ENXIO);
  }

  OpenResult out;
  out.layout = std::move(layout);
  return Result<OpenResult>(std::move(out));
}

std::string
full_namespace_path(const std::string& path)
{
  if (path.rfind("/eos/", 0) == 0) {
    return path;
  }

  if (g_export_mount_path.empty()) {
    return path;
  }

  if (path == "/" || path.empty()) {
    return g_export_mount_path;
  }

  return g_export_mount_path + path;
}

void
invalidate_embed_dir_listings()
{
  XrdMgmOfsDirectory::invalidateListingCache();
}

void
touch_parent_directory_mtime(const std::string& child_path)
{
  XrdMgmOfs* ofs = mgm_ofs();

  if (!ofs) {
    return;
  }

  eos::common::Path cpath(full_namespace_path(child_path));
  const std::string parent_path = cpath.GetParentPath();

  if (parent_path.empty()) {
    return;
  }

  try {
    eos::common::RWMutexWriteLock lock(ofs->eosViewRWMutex);
    auto pcmd = ofs->eosView->getContainer(parent_path);
    pcmd->setMTimeNow();
    // NFS GETATTR CHANGE (attr 3) is derived from ctime; Linux clients use
    // it to invalidate cached directory listings, not just mtime/verf.
    pcmd->setCTimeNow();
    pcmd->notifyMTimeChange(ofs->eosDirectoryService);
    ofs->eosView->updateContainerStore(pcmd.get());
  } catch (eos::MDException& e) {
    eos_static_debug("msg=\"touch parent mtime failed\" ec=%d emsg=\"%s\"",
                     e.getErrno(), e.getMessage().str().c_str());
  }
}

std::string
make_embed_log_id()
{
  char buf[40];
  uuid_t uuid;
  uuid_generate_time(uuid);
  uuid_unparse(uuid, buf);
  return std::string(buf);
}

struct EmbedReplicaInfo {
  std::string fst_host;
  int fst_port{0};
  unsigned fsid{0};
  size_t replica_count{1};
  std::shared_ptr<eos::IFileMD> fmd;
  std::string log_id;
};

bool
is_local_host(const std::string& host)
{
  if (host.empty() || host == "127.0.0.1" || host == "localhost") {
    return true;
  }

  char hostname[256];

  if (gethostname(hostname, sizeof(hostname)) == 0 && host == hostname) {
    return true;
  }

  const char* dot = std::strchr(hostname, '.');

  if (dot && host == std::string(hostname, dot - hostname)) {
    return true;
  }

  XrdMgmOfs* ofs = mgm_ofs();

  if (!ofs || !ofs->ManagerId.length()) {
    return false;
  }

  const std::string manager = ofs->ManagerId.c_str();
  const auto scheme = manager.find("://");

  if (scheme == std::string::npos) {
    return host == manager;
  }

  const auto host_start = scheme + 3;
  const auto host_end = manager.find(':', host_start);
  const std::string manager_host =
    (host_end == std::string::npos)
      ? manager.substr(host_start)
      : manager.substr(host_start, host_end - host_start);

  if (host == manager_host) {
    return true;
  }

  const auto manager_dot = manager_host.find('.');

  if (manager_dot != std::string::npos
      && host == manager_host.substr(0, manager_dot)) {
    return true;
  }

  return false;
}

Result<EmbedReplicaInfo>
resolve_embed_replica(const std::string& vfs_path)
{
  if (!mgm_ofs()) {
    return Result<EmbedReplicaInfo>::make_error("eos-embed-mgm: MGM not configured",
                                                ENXIO);
  }

  eos::common::VirtualIdentity vid = current_vid();
  XrdMgmOfsFile mgmFile;
  const int rc = mgmFile.open(
    &vid,
    vfs_path.c_str(),
    SFS_O_RDONLY,
    0644,
    embed_mgm_client(vid),
    "eos.embed=1&eos.app=NFS");

  if (rc != SFS_OK) {
    return result_from_errinfo<EmbedReplicaInfo>(mgmFile.error);
  }

  if (!mgmFile.isEmbedOpen()) {
    return Result<EmbedReplicaInfo>::make_error("eos-embed-mgm: embed open not active",
                                                  EINVAL);
  }

  if (!mgmFile.embedFileMd()) {
    return Result<EmbedReplicaInfo>::make_error("eos-embed-mgm: file metadata missing",
                                                 ENOENT);
  }

  const auto& fsids = mgmFile.embedReplicaFsids();

  if (fsids.empty()) {
    return Result<EmbedReplicaInfo>::make_error("eos-embed-mgm: no replica for read",
                                               ENXIO);
  }

  eos::common::RWMutexReadLock fs_lock(eos::mgm::FsView::gFsView.ViewMutex);
  eos::mgm::FileSystem* fs =
    eos::mgm::FsView::gFsView.mIdView.lookupByID(fsids.front());

  if (!fs) {
    return Result<EmbedReplicaInfo>::make_error("eos-embed-mgm: FST not found", ENXIO);
  }

  EmbedReplicaInfo info;
  info.fst_host = fs->GetString("host");
  info.fst_port = std::atoi(fs->GetString("port").c_str());
  const std::string host_alias = fs->GetString("stat.alias.host");
  const std::string port_alias = fs->GetString("stat.alias.port");

  if (!host_alias.empty()) {
    info.fst_host = host_alias;

    if (!port_alias.empty()) {
      info.fst_port = std::atoi(port_alias.c_str());
    }
  }

  info.fsid = fsids.front();
  info.replica_count = fsids.size();
  info.fmd = mgmFile.embedFileMd();
  info.log_id = make_embed_log_id();
  return Result<EmbedReplicaInfo>(std::move(info));
}

Result<std::string>
build_signed_fst_read_url(const EmbedReplicaInfo& rep,
                          const std::string& eos_path,
                          const eos::common::VirtualIdentity& vid)
{
  XrdMgmOfs* ofs = mgm_ofs();

  if (!ofs) {
    return Result<std::string>::make_error("eos-embed-mgm: MGM not configured", ENXIO);
  }

  XrdOucString capability;

  if (ofs->mTapeEnabled) {
    capability += "&tapeenabled=1";
  }

  capability += "&mgm.access=read";
  capability += "&mgm.ruid=";
  capability += static_cast<int>(vid.uid);
  capability += "&mgm.rgid=";
  capability += static_cast<int>(vid.gid);
  capability += "&mgm.uid=99";
  capability += "&mgm.gid=99";

  XrdOucString safepath = eos_path.c_str();
  eos::common::StringConversion::SealXrdPath(safepath);
  capability += "&mgm.path=";
  capability += safepath;
  capability += "&mgm.manager=";
  capability += ofs->ManagerId.c_str();

  const std::string hex_fid =
    eos::common::FileId::Fid2Hex(rep.fmd->getId());
  capability += "&mgm.fid=";
  capability += hex_fid.c_str();

  XrdOucString cid_str;
  capability += "&mgm.cid=";
  capability += eos::common::StringConversion::GetSizeString(
    cid_str, rep.fmd->getContainerId());
  capability += "&mgm.sec=";
  capability += eos::common::SecEntity::ToKey(embed_mgm_client(vid), "NFS").c_str();

  const unsigned long layoutId = rep.fmd->getLayoutId();
  const int stripe_count =
    static_cast<int>(std::max<size_t>(1, rep.replica_count));
  unsigned long new_lid = eos::common::LayoutId::GetId(
    eos::common::LayoutId::GetLayoutType(layoutId),
    eos::common::LayoutId::GetChecksum(layoutId),
    stripe_count,
    eos::common::LayoutId::GetBlocksizeType(layoutId),
    eos::common::LayoutId::GetBlockChecksum(layoutId));

  if (eos::common::LayoutId::IsRain(layoutId)) {
    eos::common::LayoutId::SetStripeNumber(
      new_lid, eos::common::LayoutId::GetStripeNumber(layoutId));
  }

  capability += "&mgm.lid=";
  capability += static_cast<int>(new_lid);

  XrdOucString bookingsize_str;
  capability += "&mgm.bookingsize=";
  capability += eos::common::StringConversion::GetSizeString(
    bookingsize_str, rep.fmd->getSize());

  capability += "&mgm.fsid=";
  capability += static_cast<int>(rep.fsid);

  capability += "&mgm.url0=root://";
  capability += rep.fst_host.c_str();
  capability += ":";
  capability += std::to_string(rep.fst_port).c_str();
  capability += "//";
  capability += "&mgm.fsid0=";
  capability += static_cast<int>(rep.fsid);

  XrdOucEnv incapability(capability.c_str());
  eos::common::SymKey* symkey = eos::common::gSymKeyStore.GetCurrentKey();

  if (!symkey) {
    return Result<std::string>::make_error("eos-embed-mgm: symkey missing", ENOENT);
  }

  XrdOucEnv* capabilityenv_raw = nullptr;
  const int caprc = eos::common::SymKey::CreateCapability(
    &incapability, capabilityenv_raw, symkey, ofs->mCapabilityValidity);

  if (caprc) {
    return Result<std::string>::make_error("eos-embed-mgm: sign capability failed",
                                          caprc > 0 ? caprc : EIO);
  }

  std::unique_ptr<XrdOucEnv> capabilityenv(capabilityenv_raw);
  int caplen = 0;
  XrdOucString query = capabilityenv->Env(caplen);
  query += "&mgm.logid=";
  query += rep.log_id.c_str();
  query += "&mgm.replicaindex=0";
  query += "&mgm.replicahead=0";
  query += "&mgm.id=";
  query += hex_fid.c_str();

  if (eos::common::LayoutId::GetLayoutType(rep.fmd->getLayoutId())
      == eos::common::LayoutId::kReplica) {
    query += "&mgm.blockchecksum=ignore";
  }

  try {
    eos::IFileMD::ctime_t mtime;
    rep.fmd->getMTime(mtime);
    query += "&mgm.mtime=";
    query += std::to_string(mtime.tv_sec).c_str();
  } catch (...) {
  }

  query += "&xrd.wantprot=unix";

  const std::string connect_host =
    is_local_host(rep.fst_host) ? "127.0.0.1" : rep.fst_host;

  std::ostringstream url;
  url << "root://" << connect_host << ":" << rep.fst_port;

  if (!eos_path.empty() && eos_path[0] == '/') {
    url << "//" << eos_path.substr(1);
  } else {
    url << "//" << eos_path;
  }

  url << "?" << query.c_str();
  return Result<std::string>(url.str());
}

Result<std::string>
read_file_bytes_via_fst(const std::string& vfs_path)
{
  const std::string eos_path = full_namespace_path(vfs_path);
  const eos::common::VirtualIdentity& vid = current_vid();
  auto rep = resolve_embed_replica(vfs_path);

  if (rep.is_err()) {
    return Result<std::string>::make_error(rep.error(), rep.error_code());
  }

  auto url = build_signed_fst_read_url(rep.value(), eos_path, vid);

  if (url.is_err()) {
    return Result<std::string>::make_error(url.error(), url.error_code());
  }

  XrdOucString log_url = url.value().c_str();
  eos::common::StringConversion::MaskTag(log_url, "cap.msg");
  eos::common::StringConversion::MaskTag(log_url, "cap.sym");
  eos_static_info("eos-embed-mgm: MDS read via FST url=%s", log_url.c_str());

  XrdCl::File file;
  XrdCl::XRootDStatus st = file.Open(url.value(), XrdCl::OpenFlags::Read);

  if (!st.IsOK()) {
    const std::string detail =
      std::string("eos-embed-mgm: XRootD open failed: ") + st.ToStr();
    eos_static_warning("%s path=%s", detail.c_str(), eos_path.c_str());
    return Result<std::string>::make_error(
      detail, st.errNo ? static_cast<int>(st.errNo) : EIO);
  }

  XrdCl::StatInfo* info_raw = new XrdCl::StatInfo();
  std::unique_ptr<XrdCl::StatInfo> info(info_raw);
  st = file.Stat(false, info_raw);
  constexpr size_t kMaxMdsRead = 16 * 1024 * 1024;
  size_t to_read = kMaxMdsRead;

  if (st.IsOK()) {
    const uint64_t sz = info_raw->GetSize();

    if (sz > kMaxMdsRead) {
      return Result<std::string>::make_error(
        "eos-embed-mgm: file too large for MDS read", EFBIG);
    }

    if (sz > 0) {
      to_read = static_cast<size_t>(sz);
    }
  }

  std::string data(to_read, '\0');
  uint32_t got = 0;
  st = file.Read(0, to_read, data.data(), got);

  if (!st.IsOK()) {
    eos_static_warning("eos-embed-mgm: XRootD read failed path=%s err=%s",
                eos_path.c_str(), st.ToStr().c_str());
    return Result<std::string>::make_error("eos-embed-mgm: XRootD read failed", EIO);
  }

  data.resize(got);
  return Result<std::string>(std::move(data));
}

} // namespace

bool
EosEmbedMgmFS::configure_mgm(void* mgm_ofs)
{
  g_mgm_ofs = static_cast<XrdMgmOfs*>(mgm_ofs);

  if (!g_mgm_ofs) {
    g_mgm_ofs = gOFS;
  }

  if (!g_mgm_ofs) {
    return false;
  }

  if (g_mgm_ofs->mNamespaceState.load() != NamespaceState::kBooted) {
    return false;
  }

  if (!ensure_keytab_loaded()) {
    return false;
  }

  return true;
}

EosEmbedMgmFS::IdentityScope::IdentityScope(const Subject& subject)
{
  const gid_t pgid = subject.gids.empty()
                       ? 0
                       : static_cast<gid_t>(subject.gids.back());
  g_nfs_auth_uid = static_cast<uid_t>(subject.uid);
  g_nfs_auth_gid = pgid;

  std::string host = normalize_nfs_client_host(subject.client_host());
  const bool krb5 =
    (subject.auth_prot == "krb5") && !subject.auth_principal.empty();

  static thread_local char name_buf[256];
  static thread_local char host_buf[256];
  static thread_local char tident_buf[512];

  const char* prot = "unix";
  std::string name;
  std::string tident;

  if (krb5) {
    prot = "krb5";
    name = subject.auth_principal;
    tident = subject.auth_principal + "@" + host;
  } else {
    int errc = 0;
    name = eos::common::Mapping::UidToUserName(subject.uid, errc);

    if (errc || name.empty()) {
      name = eos::common::Mapping::UidAsString(subject.uid);
    }

    tident = nfs_embed_tident(name, host);
  }

  std::strncpy(name_buf, name.c_str(), sizeof(name_buf) - 1);
  name_buf[sizeof(name_buf) - 1] = '\0';
  std::strncpy(host_buf, host.c_str(), sizeof(host_buf) - 1);
  host_buf[sizeof(host_buf) - 1] = '\0';
  std::strncpy(tident_buf, tident.c_str(), sizeof(tident_buf) - 1);
  tident_buf[sizeof(tident_buf) - 1] = '\0';

  XrdSecEntity client{};
  std::strncpy(client.prot, prot, sizeof(client.prot) - 1);
  client.prot[sizeof(client.prot) - 1] = '\0';
  client.name = name_buf;
  client.host = host_buf;
  client.tident = tident_buf;

  eos::common::VirtualIdentity vid;
  eos::common::Mapping::IdMap(&client, "eos.app=NFS", tident_buf, vid, nullptr,
                              AOP_Stat, "", false);
  g_vid_stack.push_back(std::move(vid));
}

EosEmbedMgmFS::IdentityScope::~IdentityScope()
{
  if (!g_vid_stack.empty()) {
    g_vid_stack.pop_back();
  }

  g_nfs_auth_uid = static_cast<uid_t>(-1);
  g_nfs_auth_gid = static_cast<gid_t>(-1);
}

std::shared_ptr<EosEmbedMgmFS>
EosEmbedMgmFS::open_root(const std::string& mount_path)
{
  std::string normalized = mgm_normalize_path(mount_path);
  auto st = mgm_stat_path(normalized, true);

  if (st.is_err() || !st.value().is_directory) {
    return nullptr;
  }

  g_export_mount_path = normalized;
  return std::shared_ptr<EosEmbedMgmFS>(new EosEmbedMgmFS(std::move(normalized)));
}

const std::string&
EosEmbedMgmFS::export_mount_path()
{
  return g_export_mount_path;
}

std::string
EosEmbedMgmFS::to_export_relative(const std::string& vfs_path)
{
  if (g_export_mount_path.empty()) {
    return vfs_path;
  }

  if (vfs_path == g_export_mount_path) {
    return std::string("/");
  }

  if (vfs_path.size() > g_export_mount_path.size() &&
      vfs_path.compare(0, g_export_mount_path.size(), g_export_mount_path) == 0 &&
      vfs_path[g_export_mount_path.size()] == '/') {
    return vfs_path.substr(g_export_mount_path.size());
  }

  return vfs_path;
}

std::string
EosEmbedMgmFS::normalize_export_path(const std::string& path)
{
  if (path.empty()) {
    return std::string("/");
  }

  return to_export_relative(path);
}

std::string
EosEmbedMgmFS::nfs_normalize_export_path(const std::string& path) const
{
  return normalize_export_path(path);
}

std::string
EosEmbedMgmFS::nfs_to_export_relative(const std::string& vfs_path) const
{
  return to_export_relative(vfs_path);
}

std::unique_ptr<VfsPath::NfsRequestIdentityScope>
EosEmbedMgmFS::nfs_push_request_identity(const Subject& subject) const
{
  return std::make_unique<IdentityScope>(subject);
}

std::optional<FlexFileLayout>
EosEmbedMgmFS::nfs_pnfs_device_template() const
{
  if (!mgm_ofs()) {
    return std::nullopt;
  }

  FlexFileLayout layout;
  layout.no_layoutcommit = true;
  const int nfs_port = fst_nfs_port();

  eos::common::RWMutexReadLock fs_lock(eos::mgm::FsView::gFsView.ViewMutex);

  for (auto it = eos::mgm::FsView::gFsView.mIdView.begin();
       it != eos::mgm::FsView::gFsView.mIdView.end(); ++it) {
    eos::mgm::FileSystem* fs = it->second;

    if (!fs) {
      continue;
    }

    const auto fsid = fs->GetId();

    if (fsid == 0 || fsid == eos::common::TAPE_FS_ID) {
      continue;
    }

    DataServer ds;
    ds.netid = "tcp";
    ds.universal_addr = make_universal_addr(fs->GetHost(), nfs_port);
    layout.mirrors.push_back(std::move(ds));
  }

  if (layout.mirrors.empty()) {
    return std::nullopt;
  }

  return layout;
}

EosEmbedMgmFS::EosEmbedMgmFS(std::string path)
    : path_(std::move(path))
{
}

std::string
EosEmbedMgmFS::filename() const
{
  if (path_ == "/" || path_.empty()) {
    return std::string();
  }

  auto pos = path_.find_last_of('/');
  return pos == std::string::npos ? path_ : path_.substr(pos + 1);
}

std::shared_ptr<VfsPath>
EosEmbedMgmFS::parent() const
{
  if (path_ == "/" || path_.empty()) {
    return nullptr;
  }

  auto pos = path_.find_last_of('/');
  std::string parent_path = (pos == 0 || pos == std::string::npos)
                              ? std::string("/")
                              : path_.substr(0, pos);
  return std::shared_ptr<EosEmbedMgmFS>(new EosEmbedMgmFS(std::move(parent_path)));
}

std::shared_ptr<VfsPath>
EosEmbedMgmFS::join(const std::string& path) const
{
  std::string joined;

  if (!path.empty() && path.front() == '/') {
    joined = path;
  } else {
    joined = path_;

    if (joined.empty() || joined.back() != '/') {
      joined.push_back('/');
    }

    joined += path;
  }

  return std::shared_ptr<EosEmbedMgmFS>(
    new EosEmbedMgmFS(mgm_normalize_path(joined)));
}

std::string
EosEmbedMgmFS::as_str() const
{
  return path_;
}

bool
EosEmbedMgmFS::supports_pnfs() const
{
  return true;
}

Result<OpenResult>
EosEmbedMgmFS::open_for_io(uint32_t rw_mask)
{
  if (!mgm_ofs()) {
    return Result<OpenResult>::make_error("eos-embed-mgm: MGM not configured", ENXIO);
  }

  auto md = metadata();

  if (md.is_err()) {
    return Result<OpenResult>::make_error(md.error(), md.error_code());
  }

  if (!md.value().is_regular_file) {
    return Result<OpenResult>::make_error("eos-embed-mgm: not a regular file", EINVAL);
  }

  eos::common::VirtualIdentity vid = current_vid();
  const bool is_write = (rw_mask & 0x2u) != 0;
  XrdMgmOfsFile mgmFile;
  const XrdSfsFileOpenMode open_mode = is_write ? SFS_O_RDWR : SFS_O_RDONLY;
  const int rc = mgmFile.open(
    &vid,
    path_.c_str(),
    open_mode,
    0644,
    embed_mgm_client(vid),
    "eos.embed=1&eos.app=NFS");

  if (rc != SFS_OK) {
    return result_from_errinfo<OpenResult>(mgmFile.error);
  }

  if (!mgmFile.isEmbedOpen()) {
    return Result<OpenResult>::make_error("eos-embed-mgm: embed open not active",
                                          EINVAL);
  }

  if (!mgmFile.embedFileMd()) {
    return Result<OpenResult>::make_error("eos-embed-mgm: file metadata missing",
                                          ENOENT);
  }

  const uid_t layout_uid =
    g_nfs_auth_uid != static_cast<uid_t>(-1) ? g_nfs_auth_uid : vid.uid;
  const gid_t layout_gid =
    g_nfs_auth_gid != static_cast<gid_t>(-1) ? g_nfs_auth_gid : vid.gid;
  auto layout = build_pnfs_layout(mgmFile.embedReplicaFsids(), mgmFile.embedFileMd(),
                                  layout_uid, layout_gid, is_write);

  if (layout.is_err()) {
    return layout;
  }

  OpenResult out;
  out.layout = std::move(layout.value().layout);

  // pNFS reads go to DS; don't block OPEN on an XRootD pull that the client
  // won't use when NO_IO_THRU_MDS is set. For read-only opens with a layout,
  // return immediately so LAYOUTGET / DS connect can proceed.
  const bool pnfs_read =
    !is_write && out.layout.has_value() && !out.layout->mirrors.empty();
  if (!is_write && !pnfs_read) {
    auto bytes = read_file_bytes_via_fst(path_);

    if (bytes.is_ok()) {
      auto ss = std::make_shared<std::stringstream>(
        std::ios::in | std::ios::out | std::ios::binary);
      ss->write(bytes.value().data(),
                static_cast<std::streamsize>(bytes.value().size()));
      ss->seekg(0);
      out.stream = ss;
    } else {
      eos_static_warning("eos-embed-mgm: open_for_io MDS read cache failed path=%s err=%s",
                  path_.c_str(), bytes.error().c_str());
    }
  }

  return Result<OpenResult>(std::move(out));
}

Result<FileMetadata>
EosEmbedMgmFS::metadata() const
{
  // NFS LOOKUP/GETATTR/READDIR need lstat semantics: report the symlink inode
  // itself, not the (possibly missing) target.
  return mgm_stat_path(path_, false);
}

Result<FilesystemSpace>
EosEmbedMgmFS::nfs_filesystem_space() const
{
  FilesystemSpace sp{};

  eos::common::RWMutexReadLock fs_lock(eos::mgm::FsView::gFsView.ViewMutex);
  auto it = eos::mgm::FsView::gFsView.mSpaceView.find("default");

  if (it == eos::mgm::FsView::gFsView.mSpaceView.end() || !it->second) {
    return Result<FilesystemSpace>(sp);
  }

  auto* space = it->second;
  sp.space_avail = static_cast<uint64_t>(
    space->SumLongLong("stat.statfs.freebytes", false));
  sp.files_avail = static_cast<uint64_t>(
    space->SumLongLong("stat.statfs.ffree", false));
  sp.space_total = static_cast<uint64_t>(
    space->SumLongLong("stat.statfs.capacity", false));
  sp.files_total = static_cast<uint64_t>(
    space->SumLongLong("stat.statfs.files", false));

  const long long used =
    space->SumLongLong("stat.statfs.usedbytes", false);

  if (sp.space_total > static_cast<uint64_t>(used)) {
    sp.space_free = sp.space_total - static_cast<uint64_t>(used);
  } else {
    sp.space_free = sp.space_avail;
  }

  sp.files_free = sp.files_avail;
  return Result<FilesystemSpace>(sp);
}

Result<bool>
EosEmbedMgmFS::exists() const
{
  auto md = mgm_stat_path(path_, false);

  if (md.is_ok()) {
    return Result<bool>(true);
  }

  if (md.error_code() == ENOENT) {
    return Result<bool>(false);
  }

  return Result<bool>::make_error(md.error(), md.error_code());
}

Result<std::vector<uint8_t>>
EosEmbedMgmFS::read() const
{
  return Result<std::vector<uint8_t>>::make_error("eos-embed-mgm: read: not implemented",
                                                  ENOTSUP);
}

Result<void>
EosEmbedMgmFS::write(const std::vector<uint8_t>& /*data*/)
{
  return Result<void>::make_error("eos-embed-mgm: write: not implemented", ENOTSUP);
}

Result<void>
EosEmbedMgmFS::create_file()
{
  XrdMgmOfs* ofs = mgm_ofs();

  if (!ofs) {
    return Result<void>::make_error("eos-embed-mgm: MGM not configured", ENXIO);
  }

  XrdOucErrInfo err;
  auto& vid = current_vid();

  // Metadata-only touch (useLayout=false) leaves #Rep=0 and st_nlink=0, which
  // makes NFS clients drop the dentry. Match normal EOS clients: schedule a
  // disk replica on create (eos touch / xrdcp open path).
  if (ofs->_touch(path_.c_str(), err, vid,
                  "eos.bookingsize=0&eos.app=NFS",
                  true,
                  true)) {
    return result_from_errinfo<void>(err);
  }

  // NFS clients use the parent directory mtime (READDIR cookie verifier /
  // cache invalidation). Namespace create alone is not enough — bump the
  // parent explicitly so listings refresh after touch/create.
  touch_parent_directory_mtime(path_);
  invalidate_embed_dir_listings();
  return Result<void>();
}

Result<void>
EosEmbedMgmFS::create_dir()
{
  XrdMgmOfs* ofs = mgm_ofs();

  if (!ofs) {
    return Result<void>::make_error("eos-embed-mgm: MGM not configured", ENXIO);
  }

  XrdOucErrInfo err;
  auto& vid = current_vid();
  const mode_t mode = S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH;

  if (ofs->_mkdir(path_.c_str(), mode, err, vid)) {
    return result_from_errinfo<void>(err);
  }

  invalidate_embed_dir_listings();
  return Result<void>();
}

Result<void>
EosEmbedMgmFS::create_dir_all()
{
  XrdMgmOfs* ofs = mgm_ofs();

  if (!ofs) {
    return Result<void>::make_error("eos-embed-mgm: MGM not configured", ENXIO);
  }

  XrdOucErrInfo err;
  auto& vid = current_vid();
  const mode_t mode = S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH | SFS_O_MKPTH;

  if (ofs->_mkdir(path_.c_str(), mode, err, vid)) {
    return result_from_errinfo<void>(err);
  }

  invalidate_embed_dir_listings();
  return Result<void>();
}

Result<void>
EosEmbedMgmFS::remove_file()
{
  XrdMgmOfs* ofs = mgm_ofs();

  if (!ofs) {
    return Result<void>::make_error("eos-embed-mgm: MGM not configured", ENXIO);
  }

  XrdOucErrInfo err;
  auto& vid = current_vid();

  if (ofs->_rem(path_.c_str(), err, vid)) {
    return result_from_errinfo<void>(err);
  }

  touch_parent_directory_mtime(path_);
  invalidate_embed_dir_listings();
  return Result<void>();
}

Result<void>
EosEmbedMgmFS::remove_dir()
{
  XrdMgmOfs* ofs = mgm_ofs();

  if (!ofs) {
    return Result<void>::make_error("eos-embed-mgm: MGM not configured", ENXIO);
  }

  XrdOucErrInfo err;
  auto& vid = current_vid();

  if (ofs->_remdir(path_.c_str(), err, vid)) {
    return result_from_errinfo<void>(err);
  }

  invalidate_embed_dir_listings();
  return Result<void>();
}

Result<std::vector<DirEntry>>
EosEmbedMgmFS::read_dir() const
{
  XrdMgmOfs* ofs = mgm_ofs();

  if (!ofs) {
    return Result<std::vector<DirEntry>>::make_error(
      "eos-embed-mgm: MGM not configured", ENXIO);
  }

  auto self_md = mgm_stat_path(path_, true);

  if (self_md.is_err()) {
    return Result<std::vector<DirEntry>>::make_error(self_md.error(),
                                                     self_md.error_code());
  }

  if (!self_md.value().is_directory) {
    return Result<std::vector<DirEntry>>::make_error("eos-embed-mgm: not a directory",
                                                     ENOTDIR);
  }

  auto* dir = dynamic_cast<XrdMgmOfsDirectory*>(ofs->newDir());

  if (!dir) {
    return Result<std::vector<DirEntry>>::make_error("eos-embed-mgm: newDir failed",
                                                    ENOMEM);
  }

  std::unique_ptr<XrdSfsDirectory> dir_guard(dir);
  auto& vid = current_vid();

  if (mgm_ofs()) {
    eos::Prefetcher::prefetchContainerMDWithChildrenAndWait(mgm_ofs()->eosView,
                                                            path_.c_str());
  }

  if (dir->open(path_.c_str(), vid, "eos.embed.nocache=1") != SFS_OK) {
    return Result<std::vector<DirEntry>>::make_error(
      "eos-embed-mgm: readdir open failed", EIO);
  }

  std::vector<DirEntry> entries;

  while (const char* name = dir->nextEntry()) {
    if (!name || !*name || !std::strcmp(name, ".") || !std::strcmp(name, "..")) {
      continue;
    }

    DirEntry entry;
    entry.name = name;

    auto child = join(name);
    auto child_md = child->metadata();

    if (child_md.is_ok()) {
      entry.file_id = child_md.value().file_id;
      entry.device_id = child_md.value().device_id;
      entry.is_directory = child_md.value().is_directory;
      entry.is_symlink = child_md.value().is_symlink;
      entry.has_metadata = true;
      entry.metadata = child_md.value();
    }

    entries.push_back(std::move(entry));
  }

  dir->close();
  return Result<std::vector<DirEntry>>(std::move(entries));
}

Result<std::shared_ptr<std::istream>>
EosEmbedMgmFS::open_read() const
{
  auto bytes = read_file_bytes_via_fst(path_);

  if (bytes.is_err()) {
    return Result<std::shared_ptr<std::istream>>::make_error(bytes.error(),
                                                            bytes.error_code());
  }

  auto ss = std::make_shared<std::istringstream>(std::move(bytes.value()));
  return Result<std::shared_ptr<std::istream>>(
    std::static_pointer_cast<std::istream>(ss));
}

Result<std::shared_ptr<std::ostream>>
EosEmbedMgmFS::open_write()
{
  return Result<std::shared_ptr<std::ostream>>::make_error(
    "eos-embed-mgm: open_write: not implemented", ENOTSUP);
}

Result<std::shared_ptr<std::iostream>>
EosEmbedMgmFS::open_read_write()
{
  return Result<std::shared_ptr<std::iostream>>::make_error(
    "eos-embed-mgm: open_read_write: not implemented", ENOTSUP);
}

Result<void>
EosEmbedMgmFS::rename_to(std::shared_ptr<VfsPath> new_parent,
                         const std::string& new_name)
{
  const auto* parent = as_mgm_fs(new_parent);

  if (!parent) {
    return Result<void>::make_error("eos-embed-mgm: rename parent type mismatch",
                                    EINVAL);
  }

  XrdMgmOfs* ofs = mgm_ofs();

  if (!ofs) {
    return Result<void>::make_error("eos-embed-mgm: MGM not configured", ENXIO);
  }

  const std::string dst = parent->join(new_name)->as_str();
  XrdOucErrInfo err;
  auto& vid = current_vid();

  if (ofs->_rename(path_.c_str(), dst.c_str(), err, vid, nullptr, nullptr, false,
                   false, true)) {
    return result_from_errinfo<void>(err);
  }

  invalidate_embed_dir_listings();
  return Result<void>();
}

Result<void>
EosEmbedMgmFS::set_size(uint64_t new_size)
{
  XrdMgmOfs* ofs = mgm_ofs();

  if (!ofs) {
    return Result<void>::make_error("eos-embed-mgm: MGM not configured", ENXIO);
  }

  XrdOucErrInfo err;
  auto& vid = current_vid();

  if (ofs->_touch(path_.c_str(), err, vid, nullptr, true, false, true, new_size)) {
    return result_from_errinfo<void>(err);
  }

  return Result<void>();
}

Result<void>
EosEmbedMgmFS::set_mode(uint32_t mode)
{
  XrdMgmOfs* ofs = mgm_ofs();

  if (!ofs) {
    return Result<void>::make_error("eos-embed-mgm: MGM not configured", ENXIO);
  }

  XrdOucErrInfo err;
  auto& vid = current_vid();
  XrdSfsMode mode_sfs = static_cast<XrdSfsMode>(mode);

  if (ofs->_chmod(path_.c_str(), mode_sfs, err, vid)) {
    return result_from_errinfo<void>(err);
  }

  return Result<void>();
}

Result<void>
EosEmbedMgmFS::set_times(const SystemTime* atime, const SystemTime* mtime)
{
  XrdMgmOfs* ofs = mgm_ofs();

  if (!ofs) {
    return Result<void>::make_error("eos-embed-mgm: MGM not configured", ENXIO);
  }

  struct timespec tv[2] {};
  const auto to_timespec = [](const SystemTime* tp, struct timespec& out) {
    if (!tp) {
      out.tv_sec = UTIME_OMIT;
      out.tv_nsec = UTIME_OMIT;
      return;
    }

    using namespace std::chrono;
    const auto sec = time_point_cast<seconds>(*tp);
    out.tv_sec = sec.time_since_epoch().count();
    out.tv_nsec =
      static_cast<long>(duration_cast<nanoseconds>(*tp - sec).count());
  };
  to_timespec(atime, tv[0]);
  to_timespec(mtime, tv[1]);

  XrdOucErrInfo err;
  auto& vid = current_vid();

  if (ofs->_utimes(path_.c_str(), tv, err, vid)) {
    return result_from_errinfo<void>(err);
  }

  return Result<void>();
}

Result<void>
EosEmbedMgmFS::set_owner(uid_t uid, gid_t gid)
{
  XrdMgmOfs* ofs = mgm_ofs();

  if (!ofs) {
    return Result<void>::make_error("eos-embed-mgm: MGM not configured", ENXIO);
  }

  XrdOucErrInfo err;
  auto& vid = current_vid();

  if (ofs->_chown(path_.c_str(), uid, gid, err, vid)) {
    return result_from_errinfo<void>(err);
  }

  return Result<void>();
}

Result<uint32_t>
EosEmbedMgmFS::access(uint32_t requested) const
{
  XrdMgmOfs* ofs = mgm_ofs();

  if (!ofs) {
    return Result<uint32_t>::make_error("eos-embed-mgm: MGM not configured", ENXIO);
  }

  uint32_t allowed = 0;

  // Match MGM _access() / open permission checks (parent sys.acl, write-once,
  // root bypass, AccessChecker) instead of evaluating Acl on the file path only.
  auto eos_allows = [&](int mode) -> bool {
    XrdOucErrInfo err;
    eos::common::VirtualIdentity vid = current_vid();
    return !ofs->_access(path_.c_str(), mode, err, vid, nullptr);
  };

  if ((requested & AccessMask::Read) && eos_allows(R_OK)) {
    allowed |= AccessMask::Read;
  }

  if ((requested & AccessMask::Lookup) && eos_allows(X_OK)) {
    allowed |= AccessMask::Lookup;
  }

  if ((requested & AccessMask::Execute) && eos_allows(X_OK)) {
    allowed |= AccessMask::Execute;
  }

  if ((requested & AccessMask::Modify) && eos_allows(W_OK)) {
    allowed |= AccessMask::Modify;
  }

  if ((requested & AccessMask::Extend) && eos_allows(W_OK)) {
    allowed |= AccessMask::Extend;
  }

  if ((requested & AccessMask::Delete) && eos_allows(D_OK)) {
    allowed |= AccessMask::Delete;
  }

  return Result<uint32_t>(allowed);
}

Result<void>
EosEmbedMgmFS::link_from(std::shared_ptr<VfsPath> source,
                          const std::string& new_name)
{
  const auto* src = as_mgm_fs(source);

  if (!src) {
    return Result<void>::make_error("eos-embed-mgm: link source type mismatch", EINVAL);
  }

  XrdMgmOfs* ofs = mgm_ofs();

  if (!ofs) {
    return Result<void>::make_error("eos-embed-mgm: MGM not configured", ENXIO);
  }

  const std::string dst = join(new_name)->as_str();
  XrdOucErrInfo err;
  auto& vid = current_vid();

  if (ofs->_touch(dst.c_str(), err, vid, nullptr, true, false, false, 0, false,
                  src->path_.c_str())) {
    return result_from_errinfo<void>(err);
  }

  return Result<void>();
}

Result<void>
EosEmbedMgmFS::create_symlink(const std::string& new_name,
                              const std::string& target)
{
  XrdMgmOfs* ofs = mgm_ofs();

  if (!ofs) {
    return Result<void>::make_error("eos-embed-mgm: MGM not configured", ENXIO);
  }

  const std::string link_path = join(new_name)->as_str();
  XrdOucErrInfo err;
  auto& vid = current_vid();

  if (ofs->_symlink(target.c_str(), link_path.c_str(), err, vid)) {
    return result_from_errinfo<void>(err);
  }

  invalidate_embed_dir_listings();
  return Result<void>();
}

Result<std::string>
EosEmbedMgmFS::read_symlink_target() const
{
  XrdMgmOfs* ofs = mgm_ofs();

  if (!ofs) {
    return Result<std::string>::make_error("eos-embed-mgm: MGM not configured", ENXIO);
  }

  XrdOucErrInfo err;
  XrdOucString target;
  auto& vid = current_vid();

  if (ofs->_readlink(path_.c_str(), err, vid, target)) {
    return result_from_errinfo<std::string>(err);
  }

  return Result<std::string>(target.c_str());
}

Result<std::vector<uint8_t>>
EosEmbedMgmFS::get_acl() const
{
  XrdMgmOfs* ofs = mgm_ofs();

  if (!ofs) {
    return Result<std::vector<uint8_t>>::make_error(
      "eos-embed-mgm: MGM not configured", ENXIO);
  }

  XrdOucErrInfo err;
  auto& vid = current_vid();
  std::string value;

  if (ofs->_attr_get(path_.c_str(), err, vid, nullptr, "user.acl", value)) {
    return result_from_errinfo<std::vector<uint8_t>>(err);
  }

  return Result<std::vector<uint8_t>>(
    std::vector<uint8_t>(value.begin(), value.end()));
}

Result<void>
EosEmbedMgmFS::set_acl(const std::vector<uint8_t>& acl_blob)
{
  XrdMgmOfs* ofs = mgm_ofs();

  if (!ofs) {
    return Result<void>::make_error("eos-embed-mgm: MGM not configured", ENXIO);
  }

  const std::string value(acl_blob.begin(), acl_blob.end());
  XrdOucErrInfo err;
  auto& vid = current_vid();

  if (ofs->_attr_set(path_.c_str(), err, vid, nullptr, "user.acl", value.c_str())) {
    return result_from_errinfo<void>(err);
  }

  return Result<void>();
}

Result<KernelFh>
EosEmbedMgmFS::kernel_handle() const
{
  return Result<KernelFh>::make_error("eos-embed-mgm: kernel_handle: not implemented",
                                        ENOTSUP);
}

Result<FileMetadata>
EosEmbedMgmFS::metadata_from_kernel_handle(uint32_t /*type*/,
                                           const std::vector<uint8_t>& /*handle*/) const
{
  return Result<FileMetadata>::make_error(
    "eos-embed-mgm: metadata_from_kernel_handle: not implemented", ENOTSUP);
}

Result<std::string>
EosEmbedMgmFS::readlink_from_kernel_handle(uint32_t /*type*/,
                                           const std::vector<uint8_t>& /*handle*/) const
{
  return Result<std::string>::make_error(
    "eos-embed-mgm: readlink_from_kernel_handle: not implemented", ENOTSUP);
}

Result<std::string>
EosEmbedMgmFS::path_from_kernel_handle(uint32_t /*type*/,
                                       const std::vector<uint8_t>& /*handle*/) const
{
  return Result<std::string>::make_error(
    "eos-embed-mgm: path_from_kernel_handle: not implemented", ENOTSUP);
}

} // namespace cernnfs
