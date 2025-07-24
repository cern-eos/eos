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

/*----------------------------------------------------------------------------*/
/**
 * @file   XrdMgmOfsFile.hh
 *
 * @brief  XRootD OFS plugin class implementing file meta data handling of EOS
 *
 * Many functions in the MgmOfs interface take CGI parameters. The supported
 * CGI parameter are:
 * "eos.ruid" - uid role the client wants
 * "eos.rgid" - gid role the client wants
 * "eos.space" - space a user wants to use for scheduling a write
 * "eos.checksum" - checksum a file should have
 * "eos.lfn" - use this name as path name not the path parameter (used by prefix
 * redirector MGM's ...
 * "eos.bookingsize" - reserve the requested bytes in a file placement
 * "eos.cli.access=pio" - ask for a parallel open (changes the response of an
 * open for RAIN layouts)
 * "eos.app" - set the application name reported by monitoring
 * "eos.targetsize" - expected size of a file to be uploaded
 * "eos.blockchecksum=ignore" - disable block checksum verification
 *
 */
/*----------------------------------------------------------------------------*/

#ifndef __EOSMGM_MGMOFSFILE__HH__
#define __EOSMGM_MGMOFSFILE__HH__

#include "common/Mapping.hh"
#include "common/Logging.hh"
#include "mgm/Messaging.hh"
#include "mgm/proc/IProcCommand.hh"
#include <XrdOuc/XrdOucErrInfo.hh>
#include <XrdSfs/XrdSfsInterface.hh>

USE_EOSMGMNAMESPACE

//! Forward declaration
class XrdSfsAio;
class XrdSecEntity;

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

  //----------------------------------------------------------------------------
  //! Target connection parameters for redirection
  //----------------------------------------------------------------------------
  struct targetParams {
    int          targetport;
    int          targethttpport;
    std::string  targethost;
    std::string  redirectionsuffix;
    std::string  redirectionhost;

    bool validPort(int port) const {
      return port > 0 && port < 65536;
    }

    bool valid() const {
      return !targethost.empty() &&
             validPort(targetport) && validPort(targethttpport);
    }
  };
  //----------------------------------------------------------------------------
  //! Handle Proxy and Firewall Entrypoint scheduling
  //! This function may be deprecated in future versions
  static targetParams setProxyFwEntrypoint(
    const std::vector<std::string>& firewalleps,
    const std::vector<std::string>& proxys,
    size_t fsIndex,
    std::string_view fs_hostport,
    std::string_view fs_prefix
  );

#ifdef IN_TEST_HARNESS
public:
#else
private:
#endif
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
  static const std::string GetClientApplicationName(XrdOucEnv* open_opaque,
      const XrdSecEntity* client);

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

  //----------------------------------------------------------------------------
  //! Get Checksum type and value information from the oppenOpaque object
  //!
  //! @param cksumType, output parameter containing the checksum type (can be empty if no such information)
  //! @param cksumValue, output parameter containg the checksum value (can be empty if no such information)
  //----------------------------------------------------------------------------
  void getCksumFromOpaque(std::string & cksumType, std::string & cksumValue);

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
};

#endif
