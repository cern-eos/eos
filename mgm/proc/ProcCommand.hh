//------------------------------------------------------------------------------
//! @file ProcCommand.hh
//! @param Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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

#pragma once
#include "mgm/Namespace.hh"
#include "IProcCommand.hh"
#include <iomanip>
#include <json/json.h>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! @todo (esindril): This needs to be moved to the archive command
//! Class IFilter used as interface to implement various types of filters
//! for the archive and backup operations.
//------------------------------------------------------------------------------
class IFilter
{
public:
  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~IFilter() {};

  //----------------------------------------------------------------------------
  //! Filter the file entry
  //!
  //! @param entry_info entry information on which the filter is applied
  //!
  //! @return true if entry should be filtered out, otherwise false
  //----------------------------------------------------------------------------
  virtual bool FilterOutFile(const std::map<std::string,
                             std::string>& entry_info) = 0 ;

  //----------------------------------------------------------------------------
  //! Filter the directory entry
  //!
  //! @param path current directory path
  //!
  //! @return true if entry should be filtered out, otherwise false
  //----------------------------------------------------------------------------
  virtual bool FilterOutDir(const std::string& path) = 0;
};

//------------------------------------------------------------------------------
//! Class handling proc command execution
//------------------------------------------------------------------------------
class ProcCommand: public IProcCommand
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  ProcCommand();

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param vid identity of the user executing the current command
  //----------------------------------------------------------------------------
  ProcCommand(eos::common::VirtualIdentity& vid);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~ProcCommand();

  //----------------------------------------------------------------------------
  //! Open a proc command e.g. call the appropriate user or admin command and
  //! store the output in a resultstream of in case of find in temporary output
  //! files.
  //!
  //! @param inpath path indicating user or admin command
  //! @param info CGI describing the proc command
  //! @param vid_in virtual identity of the user requesting a command
  //! @param error object to store errors
  //!
  //! @return SFS_OK or client stall interval in seconds
  //----------------------------------------------------------------------------
  virtual int open(const char* path, const char* info,
                   eos::common::VirtualIdentity& vid,
                   XrdOucErrInfo* error) override;

  //----------------------------------------------------------------------------
  //! Read a part of the result stream created during open
  //!
  //! @param boff offset where to start
  //! @param buff buffer to store stream
  //! @param blen len to return
  //!
  //! @return number of bytes read
  //----------------------------------------------------------------------------
  virtual size_t read(XrdSfsFileOffset offset, char* buff,
                      XrdSfsXferSize blen) override;

  //----------------------------------------------------------------------------
  //! Get the size of the result stream
  //!
  //! @param buf stat structure to fill
  //!
  //! @return SFS_OK in any case
  //----------------------------------------------------------------------------
  virtual int stat(struct stat* buf) override;

  //----------------------------------------------------------------------------
  //! Close the proc stream and store the client's command comment
  //! in the comments logbook
  //!
  //! @return 0 if comment has been successfully stored otherwise != 0
  //----------------------------------------------------------------------------
  virtual int close() override;

  //----------------------------------------------------------------------------
  //! Method implementing the specific behavior of the command executed by the
  //! asynchronous thread - used only for protobuf commands
  //----------------------------------------------------------------------------
  virtual eos::console::ReplyProto ProcessRequest() noexcept override
  {
    // Default behavior for old (raw) style commands
    return eos::console::ReplyProto();
  }

  //----------------------------------------------------------------------------
  //! Add stdout, stderr to an external stdout, stderr variable
  //----------------------------------------------------------------------------
  void
  AddOutput(XrdOucString& lStdOut, XrdOucString& lStdErr)
  {
    lStdOut += stdOut;
    lStdErr += stdErr;
  }

  //----------------------------------------------------------------------------
  //! Add stdout, stderr to an external stdout, stderr variable
  //----------------------------------------------------------------------------
  void
  AddOutput(std::string& lStdOut, std::string& lStdErr)
  {
    lStdOut += stdOut.c_str();
    lStdErr += stdErr.c_str();
  }

  //----------------------------------------------------------------------------
  //! Open temporary output files for find commands
  //!
  //! @return true if successful otherwise false
  //----------------------------------------------------------------------------
  bool OpenTemporaryOutputFiles() override;

  //----------------------------------------------------------------------------
  //! Get the return code of a proc command
  //----------------------------------------------------------------------------
  inline int GetRetc() const
  {
    return retc;
  }

  //----------------------------------------------------------------------------
  //! Get stdErr of a proc command
  //----------------------------------------------------------------------------
  inline const char* GetStdErr() const
  {
    return stdErr.c_str();
  }


  //----------------------------------------------------------------------------
  //! Get the result stream  of a proc command
  //----------------------------------------------------------------------------
  inline const char* GetResult(size_t& size) const override
  {
    if (mClosed) {
      size = 0;
      return 0;
    }

    size = mResultStream.size();
    return mResultStream.c_str();
  }



  //----------------------------------------------------------------------------
  //! Get result file name
  //----------------------------------------------------------------------------
  inline const char* GetResultFn() const
  {
    return fresultStreamfilename.c_str();
  }

  //----------------------------------------------------------------------------
  //! List of user proc commands
  //----------------------------------------------------------------------------
  int Accounting();
  int Attr();
  int Archive();
  int Backup();
  int Cd();
  int Chmod();
  int DirInfo(const char* path);
  int DirJSON(uint64_t id, Json::Value* json, bool dolock = true);
  int Find();
  int File();
  int Fileinfo();
  int FileInfo(const char* path);
  int FileJSON(uint64_t id, Json::Value* json, bool dolock = true);
  int Fuse();
  int FuseX();
  int Ls();
  int Map();
  int Member();
  int Mkdir();
  int Motd();
  int Recycle();
  int Rm();
  int Rmdir();
  int Version();
  int Who();
  int Whoami();
  int UserQuota();

  //----------------------------------------------------------------------------
  //! List of admin proc commands
  //----------------------------------------------------------------------------
  int Chown();
  int Drain();
  int Fusex();
  int GeoSched();
  int Ns();
  int Rtlog();
  int Transfer();
  int Vid();
  int Access(); // @todo (faluchet) drop when move to 5.0.0
  int Config(); // @todo (faluchet) drop when move to 5.0.0
  int Debug(); // @todo (faluchet) drop when move to 5.0.0
  int Group(); // @todo (faluchet) drop when move to 5.0.0
  int Io(); // @todo (faluchet) drop when move to 5.0.0
  int Node(); // @todo (faluchet) drop when move to 5.0.0
  int Space(); // @todo (faluchet) drop when move to 5.0.0
  int AdminQuota(); // @todo (faluchet) drop when move to 5.0.0

  //----------------------------------------------------------------------------
  //! Send command to archive daemon and collect the response
  //!
  //! @param cmd archive command in JSON format
  //!
  //! @return 0 is successful, otherwise errno. The output of the command or
  //!         any possible error messages are saved in stdOut and stdErr.
  //----------------------------------------------------------------------------
  int ArchiveExecuteCmd(const::string& cmd);

  //----------------------------------------------------------------------------
  //! Response structure holding information about the status of an archived dir
  //----------------------------------------------------------------------------
  struct ArchDirStatus {
    std::string mTime;
    std::string mUuid;
    std::string mPath;
    std::string mOp;
    std::string mStatus;

    //--------------------------------------------------------------------------
    //! Constructor
    //--------------------------------------------------------------------------
    ArchDirStatus(const std::string& xtime, const std::string& uuid,
                  const std::string& path, const std::string& op,
                  const std::string& st):
      mTime(xtime), mUuid(uuid), mPath(path), mOp(op), mStatus(st)
    {};

    //--------------------------------------------------------------------------
    //! Destructor
    //--------------------------------------------------------------------------
    ~ArchDirStatus() {};
  };

  //----------------------------------------------------------------------------
  //! Create a result stream from stdOut, stdErr & retc
  //----------------------------------------------------------------------------
  void MakeResult();

  //----------------------------------------------------------------------------
  //! Helper function able to detect key value pair output and convert to http
  //! table format
  //----------------------------------------------------------------------------
  bool KeyValToHttpTable(XrdOucString& stdOut);

protected:
  eos::common::VirtualIdentity* pVid; ///< Pointer to virtual identity

private:
  XrdOucString mPath; ///< path argument for the proc command
  XrdOucString mCmd; ///< proc command name
  XrdOucString mSubCmd; ///< proc sub command name
  XrdOucString mArgs; ///< full args from opaque input
  std::string mResultStream; ///< string containing the assembled stream
  XrdOucEnv* pOpaque; ///< pointer to the opaque information object
  const char* ininfo; ///< original opaque info string
  bool mDoSort; ///< sort flag (true = sorting)
  const char* mSelection; ///< selection argument from the opaque request
  XrdOucString mOutFormat; ///< output format type e.g. fuse or json
  unsigned mOutDepth; ///< depth of aggregation along the topology tree

  //----------------------------------------------------------------------------
  //! The 'find' command does not keep results in memory but writes to
  //! a temporary output file which is streamed to the client
  //----------------------------------------------------------------------------
  FILE* fstdout;
  FILE* fstderr;
  FILE* fresultStream;
  XrdOucString fstdoutfilename;
  XrdOucString fstderrfilename;
  XrdOucString fresultStreamfilename;
  XrdOucErrInfo* mError;

  ssize_t mLen; ///< len of the result stream
  bool mAdminCmd; ///< indicates an admin command
  bool mUserCmd; ///< indicates a user command
  bool mFuseFormat; ///< indicates FUSE format
  bool mJsonFormat; ///< indicates JSON format
  bool mHttpFormat; ///< indicates HTTP format
  bool mClosed; ///< indicates the proc command has been closed already
  bool mSendRetc; ///< indicates to return the return code to the open call
  XrdOucString mJsonCallback; ///< sets the JSONP callback name in a response

  //----------------------------------------------------------------------------
  //! Create archive file. If successful then the archive file is copied to the
  //! arch_dir location. If not it sets the retc and stdErr string accordingly.
  //!
  //! @param arch_dir directory for which the archive file is created
  //! @param dst_url archive destination URL (i.e. CASTOR location)
  //! @param fid inode number of the archive root directory used for fast find
  //!        functionality of archived directories through .../proc/archive/
  //!
  //! @return void, it sets the global retc in case of error
  //----------------------------------------------------------------------------
  void ArchiveCreate(const std::string& arch_dir,
                     const std::string& dst_url, uint64_t fid);

  //----------------------------------------------------------------------------
  //! Get list of archived files from the proc/archive directory
  //!
  //! @param root root of subtree for which we collect archived entries
  //!
  //! @return vector containing the full path of the directories currently
  //!         archived
  //----------------------------------------------------------------------------
  std::vector<ArchDirStatus> ArchiveGetDirs(const std::string& root) const;

  //----------------------------------------------------------------------------
  //! Update the status of the archived directories depending on the information
  //! that we got from the archiver daemon. All ongoing transfers will be in
  //! status "transferring" while the rest will display the status of the
  //! archive.
  //!
  //! @param dirs vector of archived directories
  //! @param tx_dirs vector of ongoing transfers
  //! @param max_path_len max path length of the entries in the dirs vector
  //----------------------------------------------------------------------------
  void ArchiveUpdateStatus(std::vector<ArchDirStatus>& dirs,
                           std::vector<ArchDirStatus>& tx_dirs,
                           size_t& max_path_len);

  //----------------------------------------------------------------------------
  //! Get fileinfo for all files/dirs in the subtree and add it to the
  //! archive i.e.  do
  //! "find -d --fileinfo /dir/" for directories or
  //! "find -f --fileinfo /dir/ for files.
  //!
  //! @param arch_dir EOS directory being archived
  //! @param arch_ofs local archive file stream object
  //! @param num number of entries added
  //! @param is_file if true add file entries to the archive, otherwise
  //!                directories
  //! @param filter filter to be applied to the entries
  //!
  //! @return 0 if successful, otherwise errno
  //----------------------------------------------------------------------------
  int ArchiveAddEntries(const std::string& arch_dir, std::fstream& arch_ofs,
                        int& num, bool is_file, IFilter* filter = NULL);

  //----------------------------------------------------------------------------
  //! Make EOS sub-tree immutable by adding the sys.acl=z:i rule to all of the
  //! directories in the sub-tree.
  //!
  //! @param arch_dir EOS directory
  //! @param vect_files vector of special archive filenames
  //!
  //! @return 0 is successful, otherwise errno. It sets the global retc in case
  //!         of error.
  //----------------------------------------------------------------------------
  int MakeSubTreeImmutable(const std::string& arch_dir,
                           const std::vector<std::string>& vect_files);

  //----------------------------------------------------------------------------
  //! Make EOS sub-tree mutable by removing the sys.acl=z:i rule from all of the
  //! directories in the sub-tree.
  //!
  //! @param arch_dir EOS directory
  //!
  //! @return 0 is successful, otherwise errno. It sets the global retc in case
  //!         of error.
  //----------------------------------------------------------------------------
  int MakeSubTreeMutable(const std::string& arch_dir);

  //----------------------------------------------------------------------------
  //! Check that the user has the necessary permissions to do an archiving
  //! operation.
  //!
  //! @param arch_dir archive directory
  //!
  //! @return true if user is allowed, otherwise False
  //----------------------------------------------------------------------------
  bool ArchiveCheckAcl(const std::string& arch_dir) const;

  //----------------------------------------------------------------------------
  //! Format listing output. Includes combining the information that we get
  //! from the archiver daemon with the list of pending transfers at the MGM.
  //!
  //! @param cmd_json command to be sent to the archive daemon
  //----------------------------------------------------------------------------
  void ArchiveFormatListing(const std::string& cmd_json);

  //----------------------------------------------------------------------------
  //! Create backup file. If successful then the backup file is copied to the
  //! backup_dir location. If not it sets the retc and stdErr string accordingly.
  //!
  //! @param backup_dir directory for which the backup file is created
  //! @param dst_url backup destination URL ending with '/'
  //! @param twindow_type time window type which can refer either to the
  //!        mtime or the ctime
  //! @param twindow_val time window timestamp
  //! @param excl_xattr set of extended attributes which are not enforced and
  //!        also not checked during the verification step
  //!
  //! @return 0 if successful, otherwise errno. It sets the global retc in case
  //!         of error
  //----------------------------------------------------------------------------
  int BackupCreate(const std::string& backup_dir,
                   const std::string& dst_url,
                   const std::string& twindow_type,
                   const std::string& twindow_val,
                   const std::set<std::string>& excl_xattr);
};

EOSMGMNAMESPACE_END
