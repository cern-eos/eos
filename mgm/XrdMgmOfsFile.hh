// ----------------------------------------------------------------------
// File: XrdMgmOfsFile.hh
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

/*
 * ═══════════════════════════════════════════════════════════════════════════════════════════
 * EOS MGM FILE INTERFACE - HEADER DOCUMENTATION
 * ═══════════════════════════════════════════════════════════════════════════════════════════
 *
 * @file   XrdMgmOfsFile.hh
 * @brief  XRootD OFS plugin class implementing file metadata handling in EOS
 * @author Andreas-Joachim Peters - CERN
 *
 * OVERVIEW:
 * =========
 * This header defines the XrdMgmOfsFile class, which serves as the core file interface
 * for the EOS (CERN Disk Storage System) Manager (MGM). The MGM is responsible for
 * metadata management, access control, and coordinating file operations across a
 * distributed storage infrastructure.
 *
 * ARCHITECTURAL ROLE:
 * ===================
 * The MGM acts as a central coordinator that:
 * - Manages file and directory metadata through the EOS namespace
 * - Handles authentication, authorization, and access control
 * - Schedules file placement across storage nodes (FSTs)
 * - Generates capability tokens for secure client-FST communication
 * - Coordinates advanced features like versioning, workflows, and reconstruction
 *
 * SUPPORTED OPERATIONS:
 * =====================
 * 1. FILE ACCESS OPERATIONS
 *    - Standard POSIX operations (open, read, write, close, stat, etc.)
 *    - Advanced operations (versioning, atomic uploads, reconstruction)
 *    - Special interfaces (/proc/ commands, workflow triggers)
 *
 * 2. ACCESS CONTROL MECHANISMS
 *    - Unix-style permissions (user, group, other)
 *    - Extended ACLs with fine-grained control
 *    - Virtual identity mapping and sudo capabilities
 *    - Quota enforcement and space management
 *
 * 3. STORAGE ORCHESTRATION
 *    - Intelligent file placement across storage nodes
 *    - Data redundancy and fault tolerance (RAID, erasure coding)
 *    - Load balancing and performance optimization
 *    - Network topology awareness
 *
 * CGI PARAMETERS:
 * ===============
 * The file interface accepts various CGI parameters for operation control:
 *
 * IDENTITY & SECURITY:
 * - "eos.ruid"          - Override user ID for the operation
 * - "eos.rgid"          - Override group ID for the operation
 * - "eos.app"           - Application name for monitoring/auditing
 *
 * STORAGE & PLACEMENT:
 * - "eos.space"         - Target storage space for file placement
 * - "eos.bookingsize"   - Pre-reserve space for file upload
 * - "eos.targetsize"    - Expected final size of uploaded file
 * - "eos.checksum"      - Expected file checksum for verification
 *
 * SPECIAL OPERATIONS:
 * - "eos.lfn"           - Use logical filename instead of path parameter
 * - "eos.cli.access=pio" - Request parallel I/O access for RAIN layouts
 * - "eos.blockchecksum=ignore" - Disable block-level checksum verification
 * - "eos.atomic"        - Enable atomic upload semantics
 * - "eos.injection"     - Enable injection mode for data migration
 * - "eos.reconstruction" - Access file for reconstruction/repair
 *
 * WORKFLOW & AUTOMATION:
 * - "eos.workflow"      - Specify workflow type for processing
 * - "eos.event"         - Trigger specific workflow events
 *
 * ERROR HANDLING:
 * ===============
 * The interface provides comprehensive error handling with:
 * - Detailed error codes and messages
 * - Retry mechanisms with exponential backoff
 * - Graceful degradation for partial failures
 * - Client redirection for load balancing
 * - Stalling mechanisms for temporary unavailability
 *
 * PERFORMANCE CONSIDERATIONS:
 * ===========================
 * - Asynchronous operations where possible
 * - Intelligent caching of metadata and capabilities
 * - Network topology optimization for client redirection
 * - Bulk operations for efficiency
 * - Resource pooling and connection reuse
 *
 * THREAD SAFETY:
 * ==============
 * - All operations are thread-safe through namespace locking
 * - Read operations use shared locks for concurrency
 * - Write operations use exclusive locks for consistency
 * - Lock ordering prevents deadlocks in complex operations
 *
 * RECENT REFACTORING:
 * ===================
 * The implementation has been significantly refactored from a monolithic
 * design to a modular architecture with specialized helper functions:
 * - Improved code organization and maintainability
 * - Enhanced testability and debugging capabilities
 * - Better error handling and recovery mechanisms
 * - Clearer separation of concerns and responsibilities
 */

#ifndef __EOSMGM_MGMOFSFILE__HH__
#define __EOSMGM_MGMOFSFILE__HH__

#include "common/Mapping.hh"
#include "common/Logging.hh"
#include "mgm/Messaging.hh"
#include "mgm/proc/IProcCommand.hh"
#include "mgm/Acl.hh"
#include "mgm/Workflow.hh"
#include <XrdOuc/XrdOucErrInfo.hh>
#include <XrdSfs/XrdSfsInterface.hh>
#include <string>
#include <map>
#include <vector>
#include <set>

USE_EOSMGMNAMESPACE

//! Forward declarations
class XrdSfsAio;
class XrdSecEntity;

//! Forward declaration from MGM namespace
namespace eos {
namespace mgm {
class Acl;
class Workflow;
}
namespace common {
class Path;
}
class IContainerMD;
class IFileMD;
}

//------------------------------------------------------------------------------
//! Class implementing files and operations
//------------------------------------------------------------------------------
class XrdMgmOfsFile : public XrdSfsFile, eos::common::LogId
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  XrdMgmOfsFile(char* user = 0, int MonID = 0):
    XrdSfsFile(user, MonID), eos::common::LogId()
  {
    vid = eos::common::VirtualIdentity::Nobody();
  }

  //----------------------------------------------------------------------------
  // Destructor
  //----------------------------------------------------------------------------
  virtual ~XrdMgmOfsFile();


  //----------------------------------------------------------------------------
  // hard link attributes
  //----------------------------------------------------------------------------
  static constexpr char* k_mdino = (char*)"sys.eos.mdino";
  static constexpr char* k_nlink = (char*)"sys.eos.nlink";

  static int
  handleHardlinkDelete(std::shared_ptr<eos::IContainerMD> cmd,
                       std::shared_ptr<eos::IFileMD> fmd,
                       eos::common::VirtualIdentity& vid);

  //----------------------------------------------------------------------------
  //----------------------------------------------------------------------------
  // utility function: create copy-on-write clone
  //----------------------------------------------------------------------------
  static const int cowUpdate = 0;              // do copy, for file updates
  static const int cowDelete = 1;              // do rename, for file deletes
  static const int cowUnlink =
    2;              // create hard link, when name vanishes e.g. Recycle
  static int create_cow(int cowType,
                        std::shared_ptr<eos::IContainerMD> dmd, std::shared_ptr<eos::IFileMD> fmd,
                        eos::common::VirtualIdentity& vid, XrdOucErrInfo& error);

  //----------------------------------------------------------------------------
  // open a file
  //----------------------------------------------------------------------------
  int open(eos::common::VirtualIdentity* vid,
           const char* fileName,
           XrdSfsFileOpenMode openMode,
           mode_t createMode,
           const XrdSecEntity* client,
           const char* opaque);

  int open(const char* fileName,
           XrdSfsFileOpenMode openMode,
           mode_t createMode,
           const XrdSecEntity* client = 0,
           const char* opaque = 0)
  {
    return open(0, fileName, openMode, createMode, client, opaque);
  }

  //----------------------------------------------------------------------------
  // close a file
  //----------------------------------------------------------------------------
  int close();

  //----------------------------------------------------------------------------
  //! get file name
  //----------------------------------------------------------------------------

  const char*
  FName()
  {
    return fileName.c_str();
  }


  //----------------------------------------------------------------------------
  //! return mmap address (we don't need it)
  //----------------------------------------------------------------------------

  int
  getMmap(void** Addr, off_t& Size)
  {
    if (Addr) {
      Addr = 0;
    }

    Size = 0;
    return SFS_OK;
  }

  //----------------------------------------------------------------------------
  //! file pre-read fakes ok (we don't need it)
  //----------------------------------------------------------------------------
  int read(XrdSfsFileOffset fileOffset, XrdSfsXferSize preread_sz)
  {
    return SFS_OK;
  }

  //----------------------------------------------------------------------------
  //! Read a partial result of a 'proc' interface command
  //!
  //! @param offset where to read from the result
  //! @param buff buffer where to place the result
  //! @param blen maximum size to read
  //!
  //! @return number of bytes read upon success or SFS_ERROR
  //!
  //! @note This read is only used to stream back 'proc' command results to
  //! the EOS shell since all normal files get a redirection or error during
  //! the file open.
  //----------------------------------------------------------------------------
  XrdSfsXferSize read(XrdSfsFileOffset offset, char* buff, XrdSfsXferSize blen);

  //----------------------------------------------------------------------------
  //! File read in async mode - NOT SUPPORTED
  //----------------------------------------------------------------------------
  int read(XrdSfsAio* aioparm)
  {
    static const char* epname = "aioread";
    return Emsg(epname, error, EOPNOTSUPP, "read aio - not supported",
                fileName.c_str());
  }

  //----------------------------------------------------------------------------
  //! Read file pages into a buffer and return corresponding checksums.
  //!
  //! @param  offset  - The offset where the read is to start. It may be
  //!                   unaligned with certain caveats relative to csvec.
  //! @param  buffer  - pointer to buffer where the bytes are to be placed.
  //! @param  rdlen   - The number of bytes to read. The amount must be an
  //!                   integral number of XrdSfsPage::Size bytes.
  //! @param  csvec   - A vector of entries to be filled with the cooresponding
  //!                   CRC32C checksum for each page. However, if the offset is
  //!                   unaligned, then csvec[0] contains the crc for the page
  //!                   fragment that brings it to alignment for csvec[1].
  //!                   It must be sized to hold all aligned XrdSys::Pagesize
  //!                   crc's plus additional ones for leading and ending page
  //!                   fragments, if any.
  //! @param  opts    - Processing options (see above).
  //!
  //! @return >= 0      The number of bytes that placed in buffer.
  //! @return SFS_ERROR File could not be read, error holds the reason.
  //----------------------------------------------------------------------------
  XrdSfsXferSize pgRead(XrdSfsFileOffset offset, char* buffer,
                        XrdSfsXferSize rdlen, uint32_t* csvec,
                        uint64_t opts = 0) override;

  //----------------------------------------------------------------------------
  //! Read file pages and checksums using asynchronous I/O - NOT SUPPORTED
  //!
  //! @param  aioparm - Pointer to async I/O object controlling the I/O.
  //! @param  opts    - Processing options (see above).
  //!
  //! @return SFS_OK    Request accepted and will be scheduled.
  //! @return SFS_ERROR File could not be read, error holds the reason.
  //-----------------------------------------------------------------------------
  int pgRead(XrdSfsAio* aioparm, uint64_t opts = 0) override
  {
    static const char* epname = "aioPgRead";
    return Emsg(epname, error, EOPNOTSUPP, "pgRead aio - not supported",
                fileName.c_str());
  }

  //----------------------------------------------------------------------------
  //! Write to file - NOT SUPPORTED (no use case)
  //----------------------------------------------------------------------------
  XrdSfsXferSize write(XrdSfsFileOffset fileOffset, const char* buffer,
                       XrdSfsXferSize buffer_size) override
  {
    static const char* epname = "write";
    return Emsg(epname, error, EOPNOTSUPP, "write - not supported",
                fileName.c_str());
  }

  //----------------------------------------------------------------------------
  //! File write in async mode - NOT SUPPORTED
  //----------------------------------------------------------------------------
  int write(XrdSfsAio* aioparm)
  {
    static const char* epname = "aiowrite";
    return Emsg(epname, error, EOPNOTSUPP, "write aio - not supported",
                fileName.c_str());
  }

  //----------------------------------------------------------------------------
  //! Write file pages into a file with corresponding checksums - NOT SUPPORTED
  //!
  //! @param  offset  - The offset where the write is to start. It may be
  //!                   unaligned with certain caveats relative to csvec.
  //! @param  buffer  - pointer to buffer containing the bytes to write.
  //! @param  wrlen   - The number of bytes to write. If amount is not an
  //!                   integral number of XrdSys::PageSize bytes, then this must
  //!                   be the last write to the file at or above the offset.
  //! @param  csvec   - A vector which contains the corresponding CRC32 checksum
  //!                   for each page or page fragment. If offset is unaligned
  //!                   then csvec[0] is the crc of the leading fragment to
  //!                   align the subsequent full page who's crc is in csvec[1].
  //!                   It must be sized to hold all aligned XrdSys::Pagesize
  //!                   crc's plus additional ones for leading and ending page
  //!                   fragments, if any.
  //! @param  opts    - Processing options (see above).
  //!
  //! @return >= 0      The number of bytes written.
  //! @return SFS_ERROR File could not be read, error holds the reason.
  //----------------------------------------------------------------------------
  XrdSfsXferSize pgWrite(XrdSfsFileOffset offset, char* buffer,
                         XrdSfsXferSize wrlen, uint32_t* csvec,
                         uint64_t opts = 0) override
  {
    static const char* epname = "PgWrite";
    return Emsg(epname, error, EOPNOTSUPP, "pgWrite - not supported",
                fileName.c_str());
  }

  //----------------------------------------------------------------------------
  //! Write file pages and checksums using asynchronous I/O - NOT SUPPORTED
  //!
  //! @param  aioparm - Pointer to async I/O object controlling the I/O.
  //! @param  opts    - Processing options (see above).
  //!
  //! @return SFS_OK    Request accepted and will be scheduled.
  //! @return SFS_ERROR File could not be read, error holds the reason.
  //----------------------------------------------------------------------------
  int pgWrite(XrdSfsAio* aioparm, uint64_t opts = 0) override
  {
    static const char* epname = "aioPgWrite";
    return Emsg(epname, error, EOPNOTSUPP, "pgWrite aio - not supported",
                fileName.c_str());
  }

  //----------------------------------------------------------------------------
  //! file sync
  //----------------------------------------------------------------------------
  int sync();

  //----------------------------------------------------------------------------
  //! file sync aio
  //----------------------------------------------------------------------------
  int sync(XrdSfsAio* aiop);

  //----------------------------------------------------------------------------
  // file stat
  //----------------------------------------------------------------------------
  int stat(struct stat* buf);

  //----------------------------------------------------------------------------
  // file truncate
  //----------------------------------------------------------------------------
  int truncate(XrdSfsFileOffset fileOffset);

  //----------------------------------------------------------------------------
  //! get checksum info (returns nothing - not supported)
  //----------------------------------------------------------------------------
  int
  getCXinfo(char cxtype[4], int& cxrsz)
  {
    return cxrsz = 0;
  }

  //----------------------------------------------------------------------------
  //! fctl fakes ok
  //----------------------------------------------------------------------------
  int
  fctl(int, const char*, XrdOucErrInfo&)
  {
    return 0;
  }

  //----------------------------------------------------------------------------
  //! error message function
  //----------------------------------------------------------------------------
  int Emsg(const char*, XrdOucErrInfo&, int, const char* x,
           const char* y = "");


#ifdef IN_TEST_HARNESS
public:
#else
private:
#endif
  //----------------------------------------------------------------------------
  //! Helper functions for the refactored open() method
  //----------------------------------------------------------------------------
  
  //----------------------------------------------------------------------------
  //! Process identity and path information
  //!
  //! @param invid virtual identity pointer
  //! @param inpath input path
  //! @param ininfo input opaque information  
  //! @param tident thread identifier
  //! @param client XRD security entity
  //! @param acc_op access operation
  //! @param vid virtual identity output
  //! @param path processed path output
  //!
  //! @return 0 on success, error code on failure
  //----------------------------------------------------------------------------
    int ProcessIdentityAndPath(eos::common::VirtualIdentity* invid,
                              const char* inpath, const char* ininfo, const char* tident,
                              const XrdSecEntity* client, Access_Operation acc_op,
                              eos::common::VirtualIdentity& vid, std::string& outpath);

  //----------------------------------------------------------------------------
  //! Handle file ID access
  //!
  //! @param spath path string
  //! @param fmd file metadata output
  //!
  //! @return 0 on success, error code on failure
  //----------------------------------------------------------------------------
  int HandleFidAccess(const std::string& spath, std::shared_ptr<eos::IFileMD>& fmd);



  //----------------------------------------------------------------------------
  //! Handle container and permissions
  //!
  //! @param path file path
  //! @param dmd container metadata output
  //! @param fmd file metadata output
  //! @param attrmap container attributes output
  //! @param attrmapF file attributes output
  //! @param acl ACL object output
  //! @param workflow workflow object output
  //!
  //! @return 0 on success, error code on failure
  //----------------------------------------------------------------------------
  int HandleContainerAndPermissions(const char* path, std::shared_ptr<eos::IContainerMD>& dmd,
                                    std::shared_ptr<eos::IFileMD>& fmd,
                                    eos::IContainerMD::XAttrMap& attrmap,
                                    eos::IFileMD::XAttrMap& attrmapF,
                                    Acl& acl, Workflow& workflow);

  //----------------------------------------------------------------------------
  //! Handle file creation
  //!
  //! @param path file path
  //! @param dmd container metadata
  //! @param fmd file metadata output
  //! @param attrmap container attributes
  //! @param attrmapF file attributes output
  //! @param acl ACL object
  //! @param versioning versioning depth
  //!
  //! @return 0 on success, error code on failure
  //----------------------------------------------------------------------------
  int HandleFileCreation(const char* path, std::shared_ptr<eos::IContainerMD>& dmd,
                         std::shared_ptr<eos::IFileMD>& fmd,
                         const eos::IContainerMD::XAttrMap& attrmap,
                         eos::IFileMD::XAttrMap& attrmapF,
                         const Acl& acl, int versioning);



  //----------------------------------------------------------------------------
  //! Construct redirection URL
  //!
  //! @param path file path
  //! @param fmd file metadata
  //! @param selectedfs selected filesystems
  //! @param proxys proxy endpoints
  //! @param firewalleps firewall endpoints
  //! @param capability capability string
  //! @param workflow workflow object
  //!
  //! @return constructed redirection URL
  //----------------------------------------------------------------------------
  std::string ConstructRedirectionUrl(const char* path, std::shared_ptr<eos::IFileMD>& fmd,
                                      const std::vector<unsigned int>& selectedfs,
                                      const std::vector<std::string>& proxys,
                                      const std::vector<std::string>& firewalleps,
                                      const std::string& capability,
                                      const Workflow& workflow);

  //----------------------------------------------------------------------------
  //! Process opaque parameters
  //!
  //! @param openOpaque opaque environment (output)
  //! @param client XRD security entity
  //! @param ininfo input opaque information
  //!
  //! @return 0 on success, error code on failure
  //----------------------------------------------------------------------------
  int ProcessOpaqueParameters(XrdOucEnv*& openOpaque,
                              const XrdSecEntity* client, const char* ininfo);

  //----------------------------------------------------------------------------
  //! Handle proc interface access
  //!
  //! @param path file path
  //! @param ininfo opaque information
  //! @param vid virtual identity
  //! @param client XRD security entity
  //! @param tident thread identifier
  //! @param logId log identifier
  //!
  //! @return SFS_OK/SFS_REDIRECT/SFS_STALL on success, SFS_ERROR on failure
  //----------------------------------------------------------------------------
  int HandleProcAccess(const char* path, const char* ininfo,
                       eos::common::VirtualIdentity& vid,
                       const XrdSecEntity* client,
                       const char* tident, const std::string& logId);

  //----------------------------------------------------------------------------
  //! Handle file access by file identifier (.fxid:)
  //!
  //! @param path file path (will be updated)
  //! @param fmd file metadata (output)
  //!
  //! @return 0 on success, error code on failure
  //----------------------------------------------------------------------------
  int HandleFxidAccess(const char*& path, std::shared_ptr<eos::IFileMD>& fmd);

  //----------------------------------------------------------------------------
  //! Handle path processing and validation
  //!
  //! @param path file path
  //! @param Mode file mode
  //! @param vid virtual identity
  //! @param ininfo input opaque information
  //! @param openOpaque opaque environment
  //! @param dmd container metadata (output)
  //!
  //! @return 0 on success, error code on failure
  //----------------------------------------------------------------------------
  int HandlePathProcessing(const char* path, mode_t Mode,
                           eos::common::VirtualIdentity& vid,
                           const char* ininfo, XrdOucEnv* openOpaque,
                           std::shared_ptr<eos::IContainerMD>& dmd);

  //----------------------------------------------------------------------------
  //! Handle ACL and permission checks
  //!
  //! @param path file path
  //! @param vid virtual identity
  //! @param dmd container metadata
  //! @param fmd file metadata
  //! @param attrmap container attributes
  //! @param attrmapF file attributes (output)
  //! @param acl ACL object (output)
  //! @param stdpermcheck standard permission check flag (output)
  //! @param d_uid directory uid
  //! @param d_gid directory gid
  //! @param sticky_owner sticky owner flag
  //! @param dotFxid fxid access flag
  //!
  //! @return 0 on success, error code on failure, special return for sys.proc redirection
  //----------------------------------------------------------------------------
  int HandleAclAndPermissions(const char* path, eos::common::VirtualIdentity& vid,
                              std::shared_ptr<eos::IContainerMD>& dmd,
                              std::shared_ptr<eos::IFileMD>& fmd,
                              const eos::IContainerMD::XAttrMap& attrmap,
                              eos::IFileMD::XAttrMap& attrmapF,
                              Acl& acl, bool& stdpermcheck,
                              uid_t d_uid, gid_t d_gid,
                              bool sticky_owner, bool dotFxid);

  //----------------------------------------------------------------------------
  //! Handle capability creation and policy evaluation
  //!
  //! @param path file path
  //! @param ininfo input opaque information
  //! @param vid virtual identity
  //! @param dmd container metadata
  //! @param fmd file metadata
  //! @param attrmap container attributes
  //! @param attrmapF file attributes
  //! @param openOpaque opaque environment
  //! @param capability capability string (output)
  //! @param layoutId layout ID (input/output)
  //! @param forcedFsId forced filesystem ID (output)
  //! @param forced_group forced group (output)
  //! @param fsIndex filesystem index (output)
  //! @param space space name (output)
  //! @param new_lid new layout ID (output)
  //! @param targetgeotag target geotag (output)
  //! @param bandwidth bandwidth (output)
  //! @param ioprio IO priority (output)
  //! @param iotype IO type (output)
  //! @param schedule schedule flag (output)
  //!
  //! @return 0 on success, SFS_REDIRECT for local redirect, error code on failure
  //----------------------------------------------------------------------------
  int HandleCapabilityCreation(const char* path, const char* ininfo,
                                eos::common::VirtualIdentity& vid,
                                std::shared_ptr<eos::IContainerMD>& dmd,
                                std::shared_ptr<eos::IFileMD>& fmd,
                                eos::IContainerMD::XAttrMap& attrmap,
                                const eos::IFileMD::XAttrMap& attrmapF,
                                XrdOucEnv* openOpaque,
                                XrdOucString& capability,
                                unsigned long& layoutId,
                                unsigned long& forcedFsId,
                                long& forced_group,
                                unsigned long& fsIndex,
                                std::string& space,
                                unsigned long& new_lid,
                                std::string& targetgeotag,
                                std::string& bandwidth,
                                                                 std::string& ioprio,
                                 std::string& iotype,
                                 bool& schedule);

  //----------------------------------------------------------------------------
  //! Handle namespace metadata retrieval and file lookup
  //!
  //! @param path file path
  //! @param vid virtual identity
  //! @param cPath path object
  //! @param dmd container metadata (output)
  //! @param fmd file metadata (output)
  //!
  //! @return 0 on success, error code on failure
  //----------------------------------------------------------------------------
  int HandleNamespaceMetadataRetrieval(const char* path,
                                       eos::common::VirtualIdentity& vid,
                                       eos::common::Path& cPath,
                                       std::shared_ptr<eos::IContainerMD>& dmd,
                                       std::shared_ptr<eos::IFileMD>& fmd);

  //----------------------------------------------------------------------------
  //! Handle ENOENT redirection logic
  //!
  //! @param path file path
  //! @param vid virtual identity
  //! @param cPath path object
  //! @param dmd container metadata (input/output)
  //!
  //! @return SFS_REDIRECT if redirected, SFS_ERROR on error, 0 to continue
  //----------------------------------------------------------------------------
  int HandleEnoentRedirection(const char* path,
                              eos::common::VirtualIdentity& vid,
                              eos::common::Path& cPath,
                              std::shared_ptr<eos::IContainerMD>& dmd);

  //----------------------------------------------------------------------------
  //! Parse external timestamp and attribute parameters from opaque
  //!
  //! @param openOpaque opaque environment
  //! @param ext_mtime_sec external modification time seconds (output)
  //! @param ext_mtime_nsec external modification time nanoseconds (output)
  //! @param ext_ctime_sec external creation time seconds (output)
  //! @param ext_ctime_nsec external creation time nanoseconds (output)
  //! @param ext_etag external ETag (output)
  //! @param ext_xattr_map external extended attributes (output)
  //----------------------------------------------------------------------------
  void ParseExternalTimestampsAndAttributes(XrdOucEnv* openOpaque,
                                            unsigned long long& ext_mtime_sec,
                                            unsigned long long& ext_mtime_nsec,
                                            unsigned long long& ext_ctime_sec,
                                            unsigned long long& ext_ctime_nsec,
                                            std::string& ext_etag,
                                            std::map<std::string, std::string>& ext_xattr_map);

  //----------------------------------------------------------------------------
  //! Handle read operation processing
  //!
  //! @param path file path
  //! @param vid virtual identity
  //! @param fmd file metadata
  //! @param openOpaque opaque environment
  //! @param redirectionhost redirection host (output)
  //! @param ecode error code (output)
  //! @param rcode return code (output)
  //! @param ininfo input opaque information
  //!
  //! @return 0 on success, SFS_ERROR/SFS_REDIRECT/SFS_STARTED/SFS_STALL on special cases
  //----------------------------------------------------------------------------
  int HandleReadOperation(const char* path,
                          eos::common::VirtualIdentity& vid,
                          std::shared_ptr<eos::IFileMD>& fmd,
                          XrdOucEnv* openOpaque,
                          XrdOucString& redirectionhost,
                          int& ecode,
                          int& rcode,
                          const char* ininfo);

  //----------------------------------------------------------------------------
  //! Handle write operation processing
  //!
  //! @param path file path
  //! @param ininfo input opaque information
  //! @param vid virtual identity
  //! @param dmd container metadata
  //! @param fmd file metadata (input/output)
  //! @param attrmapF file attributes (input/output)
  //! @param openOpaque opaque environment
  //! @param cPath path object
  //! @param Mode creation mode
  //! @param logId log identifier
  //!
  //! @return 0 on success, SFS_ERROR/SFS_REDIRECT/SFS_STARTED/SFS_STALL on special cases
  //----------------------------------------------------------------------------
  int HandleWriteOperation(const char* path, const char* ininfo,
                           eos::common::VirtualIdentity& vid,
                           std::shared_ptr<eos::IContainerMD>& dmd,
                           std::shared_ptr<eos::IFileMD>& fmd,
                           eos::IFileMD::XAttrMap& attrmapF,
                           XrdOucEnv* openOpaque,
                           eos::common::Path& cPath,
                           mode_t Mode,
                           const std::string& logId);

  //----------------------------------------------------------------------------
  //! Handle container and permissions
  //!
  //! @param path file path
  //! @param vid virtual identity
  //! @param isRW read-write flag
  //! @param isSharedFile shared file flag  
  //! @param stdpermcheck standard permission check flag
  //! @param dmd container metadata output
  //! @param fmd file metadata output
  //! @param attrmap container attributes output
  //! @param attrmapF file attributes output
  //! @param acl access control list output
  //! @param mFid file ID output
  //!
  //! @return 0 on success, error code on failure
  //----------------------------------------------------------------------------
    int HandleContainerAndPermissions(const std::string& path,
                                     const eos::common::VirtualIdentity& vid,
                                     bool isRW, bool isSharedFile, bool stdpermcheck,
                                     std::shared_ptr<eos::IContainerMD>& dmd,
                                     std::shared_ptr<eos::IFileMD>& fmd,
                                     eos::IContainerMD::XAttrMap& attrmap,
                                     eos::IFileMD::XAttrMap& attrmapF,
                                     eos::mgm::Acl& acl, unsigned long long& mFid);

  //----------------------------------------------------------------------------
  //! Handle file creation
  //!
  //! @param creation_path creation path
  //! @param path file path
  //! @param vid virtual identity
  //! @param Mode creation mode
  //! @param dmd container metadata
  //! @param fmd file metadata output
  //! @param attrmap container attributes
  //! @param attrmapF file attributes
  //! @param isCreation creation flag
  //! @param isAtomicUpload atomic upload flag
  //! @param ocUploadUuid upload UUID
  //! @param versioning versioning parameter
  //! @param mFid file ID output
  //! @param acl access control list
  //! @param open_flags open flags
  //! @param mEosKey encryption key
  //! @param mEosObfuscate obfuscation setting
  //! @param logId log identifier
  //!
  //! @return 0 on success, error code on failure
  //----------------------------------------------------------------------------
  int HandleFileCreation(const std::string& creation_path,
                         const std::string& path, const eos::common::VirtualIdentity& vid,
                         mode_t Mode, std::shared_ptr<eos::IContainerMD>& dmd,
                         std::shared_ptr<eos::IFileMD>& fmd,
                         const eos::IContainerMD::XAttrMap& attrmap,
                         const eos::IFileMD::XAttrMap& attrmapF,
                         bool isCreation, bool isAtomicUpload, const std::string& ocUploadUuid,
                         int versioning, unsigned long long& mFid,
                         const eos::mgm::Acl& acl, int open_flags,
                         const std::string& mEosKey, int mEosObfuscate,
                         const std::string& logId);

  //----------------------------------------------------------------------------
  //! Handle file scheduling
  //!
  //! @param path file path
  //! @param vid virtual identity
  //! @param fmd file metadata
  //! @param isRW read-write flag
  //! @param isCreation creation flag
  //! @param isPioReconstruct PIO reconstruct flag
  //! @param isPio PIO flag
  //! @param isRepair repair flag
  //! @param isFuse FUSE flag
  //! @param space storage space
  //! @param layoutId layout identifier
  //! @param bookingsize booking size
  //! @param forced_group forced group
  //! @param targetgeotag target geo tag
  //! @param excludefs excluded filesystems
  //! @param pio_reconstruct_fs PIO reconstruct filesystems
  //! @param selectedfs selected filesystems output
  //! @param unavailfs unavailable filesystems output
  //! @param replacedfs replaced filesystems output
  //! @param pio_replacement_fs PIO replacement filesystems output
  //! @param proxys proxy endpoints output
  //! @param firewalleps firewall endpoints output
  //! @param fsIndex filesystem index output
  //! @param tried_cgi tried CGI
  //! @param retc return code output
  //! @param isRecreation recreation flag output
  //!
  //! @return 0 on success, error code on failure
  //----------------------------------------------------------------------------
  int HandleFileScheduling(const std::string& path,
                           const eos::common::VirtualIdentity& vid,
                           std::shared_ptr<eos::IFileMD>& fmd,
                           bool isRW, bool isCreation, bool isPioReconstruct,
                           bool isPio, bool isRepair, bool isFuse,
                           const std::string& space, unsigned long layoutId,
                           uint64_t bookingsize, long forced_group,
                           const std::string& targetgeotag,
                           const std::vector<unsigned int>& excludefs,
                           const std::set<unsigned int>& pio_reconstruct_fs,
                           std::vector<unsigned int>& selectedfs,
                           std::vector<unsigned int>& unavailfs,
                           std::vector<unsigned int>& replacedfs,
                           std::vector<unsigned int>& pio_replacement_fs,
                           std::vector<std::string>& proxys,
                           std::vector<std::string>& firewalleps,
                           unsigned long& fsIndex, const std::string& tried_cgi,
                           int& retc, bool& isRecreation);

  //----------------------------------------------------------------------------
  //! Construct redirection URL
  //!
  //! @param path file path
  //! @param vid virtual identity
  //! @param selectedfs selected filesystems
  //! @param proxys proxy endpoints
  //! @param firewalleps firewall endpoints
  //! @param fsIndex filesystem index
  //! @param isRW read-write flag
  //! @param isPio PIO flag
  //! @param isPioReconstruct PIO reconstruct flag
  //! @param isFuse FUSE flag
  //! @param app_name application name
  //! @param ioPriority IO priority
  //! @param capability capability string
  //! @param hex_fid hexadecimal file ID
  //! @param logId log identifier
  //! @param openOpaque opaque environment
  //! @param fmd file metadata
  //! @param redirectionhost redirection host output
  //! @param targethost target host output
  //! @param targetport target port output
  //! @param targethttpport target HTTP port output
  //! @param ecode error code output
  //!
  //! @return 0 on success, error code on failure
  //----------------------------------------------------------------------------
  int ConstructRedirectionUrl(const std::string& path,
                              const eos::common::VirtualIdentity& vid,
                              const std::vector<unsigned int>& selectedfs,
                              const std::vector<std::string>& proxys,
                              const std::vector<std::string>& firewalleps,
                              unsigned long fsIndex, bool isRW, bool isPio,
                              bool isPioReconstruct, bool isFuse,
                              const std::string& app_name, const std::string& ioPriority,
                              const std::string& capability, const std::string& hex_fid,
                              const std::string& logId, XrdOucEnv* openOpaque,
                              std::shared_ptr<eos::IFileMD>& fmd,
                              XrdOucString& redirectionhost, XrdOucString& targethost,
                              int& targetport, int& targethttpport, int& ecode);

  //----------------------------------------------------------------------------
  //! Check if this is a client retry with exclusion of some diskserver. This
  //! happens usually for CMS workflows. To distinguish such a scenario from
  //! a legitimate retry due to a recoverable error, we need to serarch for the
  //! "tried=" opaque tag without a corresponding "triedrc=" tag.
  //!
  //! @param is_rw true if is RW otherwise false
  //! @param lid file layout id
  //!
  //! @return true if this is a retry for a RAIN file with the user excluding
  //!         some diskservers, otherwise false.
  //----------------------------------------------------------------------------
  bool IsRainRetryWithExclusion(bool is_rw, unsigned long lid) const;

  //----------------------------------------------------------------------------
  //! Parse the triedrc opaque info and return the corresponding error number
  //!
  //! @param input input string in the form of "enoent,ioerr,fserr,srverr"
  //!
  //! @return error number
  //----------------------------------------------------------------------------
  int GetTriedrcErrno(const std::string& input) const;

  //----------------------------------------------------------------------------
  //! Handle (delegated) TPC redirection
  //!
  //! @return true if redirection required (the error object will be properly
  //!         populated with the redirection host and port info), otherwise
  //!         false
  //----------------------------------------------------------------------------
  bool RedirectTpcAccess();

  //----------------------------------------------------------------------------
  //! Dump scheduling info
  //!
  //! @param selected_fs list of selected file systems
  //! @param proxys list of data proxy endpoints
  //! @param fwall_eps firewall entrypoints
  //----------------------------------------------------------------------------
  void LogSchedulingInfo(const std::vector<unsigned int>& selected_fs,
                         const std::vector<std::string>& proxy_eps,
                         const std::vector<std::string>& fwall_eps) const;

  //----------------------------------------------------------------------------
  //! Get file system ids excluded from scheduling
  //!
  //! return list of file system ids to exclude
  //----------------------------------------------------------------------------
  std::vector<unsigned int>
  GetExcludedFsids() const;

  //----------------------------------------------------------------------------
  //! Get the application name if specified
  //!
  //! @param open_opaque open opaque information
  //! @param client XrdSecEntity identifying the request
  //!
  //! @return application name or empty string if nothing specified
  //----------------------------------------------------------------------------
  static const std::string GetClientApplicationName(XrdOucEnv* open_opaque, const XrdSecEntity* client);

  //----------------------------------------------------------------------------
  //! Get POSIX open flags from the given XRootD open mode
  //!
  //! @param open_mode XRootD open mode see XrdSfsInterface.hh
  //!
  //! @return POSIX open flags see man 2 open
  //----------------------------------------------------------------------------
  static int
  GetPosixOpenFlags(XrdSfsFileOpenMode open_mode);

  //----------------------------------------------------------------------------
  //! Get XRootD acceess operation bases on the given open flags
  //!
  //! @param open_flags POSIX open flags
  //!
  //! @return access opeation type see XrdAccAuthorization.hh
  //----------------------------------------------------------------------------
  static Access_Operation
  GetXrdAccessOperation(int open_flags);

  int oh {0}; //< file handle
  std::string fileName; //< file name
  XrdOucEnv* openOpaque {nullptr}; //< opaque info given with 'open'
  unsigned long long mFid {0ull}; //< Namespace file identifier
  std::unique_ptr<IProcCommand> mProcCmd {nullptr}; // Proc command object
  std::shared_ptr<eos::IFileMD> fmd {nullptr}; //< File meta data object
  eos::common::VirtualIdentity vid; //< virtual ID of the client
  std::string mEosKey; ///< File specific encryption key
  //! Flag to toggle obfuscation (-1 take directory default, 0 disable, 1 enable)
  int mEosObfuscate { -1};
  bool mIsZeroSize {false}; //< Mark if file is zero size

  //----------------------------------------------------------------------------
  //! Structure to hold all open state variables
  //----------------------------------------------------------------------------
  struct OpenState {
    // Basic open parameters
    bool isCreation = false;
    bool isPio = false;              // parallel IO access
    bool isPioReconstruct = false;   // access with reconstruction
    bool isFuse = false;             // FUSE file access
    bool isAtomicUpload = false;     // atomic upload with hidden name
    bool isAtomicName = false;       // atomic file name
    bool isInjection = false;        // new injection upload
    bool isRepair = false;           // drop current disk replica
    bool isRepairRead = false;       // read for repair
    bool isTpc = false;              // TPC action
    bool isTouch = false;            // file touch
    bool isRW = false;               // read/write flag
    bool isRewrite = false;          // rewrite flag
    bool isSharedFile = false;       // shared file flag
    
    // CGI and opaque information
    XrdOucString ocUploadUuid;       // chunk upload ID
    std::string tried_cgi;           // tried hosts CGI
    std::string versioning_cgi;      // versioning CGI
    std::string ioPriority;          // IO priority string
    std::string app_name;            // application name
    
    // File system and placement info
    std::set<unsigned int> pio_reconstruct_fs;     // filesystem IDs to reconstruct
    std::vector<unsigned int> pio_replacement_fs;  // replacement filesystem IDs
    
    // File metadata
    uint64_t fmdsize = 0;            // file size
    unsigned long fmdlid = 0;        // file layout ID
    unsigned long long byfid = 0;    // file ID for access by fid
    unsigned long long bypid = 0;    // parent ID
    unsigned long long cid = 0;      // container ID
    eos::IFileMD::LocationVector vect_loc; // file locations
    
    // Workflow and processing
    XrdOucString currentWorkflow = "default";
    
    // Paths and names
    XrdOucString spath;              // sanitized path
    XrdOucString sinfo;              // sanitized info
    XrdOucString pinfo;              // processed info
    
    // Access control
    int open_flags = 0;              // POSIX open flags
    Access_Operation acc_op;         // XRootD access operation
    
    // Namespace and permission variables
    eos::IContainerMD::XAttrMap attrmap;    // container attributes
    Acl acl;                               // access control list
    Workflow workflow;                     // workflow object
    bool stdpermcheck = false;             // standard permission check flag
    int versioning = 0;                    // versioning flag
    uid_t d_uid = 0;                       // effective user ID
    gid_t d_gid = 0;                       // effective group ID
    std::string creation_path;             // original path for creation
    
    //--------------------------------------------------------------------------
    //! Reset all state variables to their default values
    //--------------------------------------------------------------------------
    void Reset() {
      isCreation = false;
      isPio = false;
      isPioReconstruct = false;
      isFuse = false;
      isAtomicUpload = false;
      isAtomicName = false;
      isInjection = false;
      isRepair = false;
      isRepairRead = false;
      isTpc = false;
      isTouch = false;
      isRW = false;
      isRewrite = false;
      isSharedFile = false;
      
      ocUploadUuid = "";
      tried_cgi.clear();
      versioning_cgi.clear();
      ioPriority.clear();
      app_name.clear();
      
      pio_reconstruct_fs.clear();
      pio_replacement_fs.clear();
      
      fmdsize = 0;
      fmdlid = 0;
      byfid = 0;
      bypid = 0;
      cid = 0;
      vect_loc.clear();
      
      currentWorkflow = "default";
      
      spath = "";
      sinfo = "";
      pinfo = "";
      
      open_flags = 0;
      acc_op = AOP_Any;
      
      // Reset new namespace and permission variables
      attrmap.clear();
      acl = Acl();
      workflow.Reset();
      stdpermcheck = false;
      versioning = 0;
      d_uid = 0;
      d_gid = 0;
      creation_path.clear();
    }
  };
  
  OpenState mOpenState; ///< Open state structure
};

#endif
