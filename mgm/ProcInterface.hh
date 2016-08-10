// ----------------------------------------------------------------------
// File: ProcInterface.hh
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

#ifndef __EOSMGM_PROCINTERFACE__HH__
#define __EOSMGM_PROCINTERFACE__HH__

#include "mgm/Namespace.hh"
#include "common/Logging.hh"
#include "common/Mapping.hh"
#include "proc/proc_fs.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdSfs/XrdSfsInterface.hh"
#include "XrdSec/XrdSecEntity.hh"

EOSMGMNAMESPACE_BEGIN

/**
 * @file   ProcInterface.hh
 *
 * @brief  ProcCommand class handling proc commands
 *
 * A proc command is identified by a user requesting to read a path like
 * '/proc/user' or '/proc/admin'. These two options specify either user or
 * admin commands. Admin commands can only be executed if a VID indicates
 * membership in the admin group, root or in same cases 'sss' authenticated
 * clients. A proc command is usually referenced with the tag 'mgm.cmd'.
 * In some cases there a sub commands defined by 'mgm.subcmd'.
 * Proc commands are executed in the 'open' function and the results
 * are provided as stdOut,stdErr and a return code which is assembled in an
 * opaque output stream with 3 keys indicating the three return objects.
 * The resultstream is streamed by a client like a file read using 'xrdcp'
 * issuing several read requests. On close the resultstream is freed.
 *
 * The implementations of user commands are found under mgm/proc/user/X.cc
 * The implementations of admin commands are found under mgm/proc/admin/X.cc
 * A new command has to be added to the if-else construct in the open function.
 */

//------------------------------------------------------------------------------
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
  virtual bool FilterOutFile(const std::map<std::string, std::string>& entry_info) = 0 ;

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
class ProcCommand: public eos::common::LogId
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  ProcCommand ();

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~ProcCommand ();

  //----------------------------------------------------------------------------
  //! Open a proc command e.g. call the appropriate user or admin commmand and
  //! store the output in a resultstream of in case of find in temporary output
  //! files.
  //!
  //! @param inpath path indicating user or admin command
  //! @param info CGI describing the proc command
  //! @param vid_in virtual identity of the user requesting a command
  //! @param error object to store errors
  //!
  //! @return SFS_OK in any case
  //----------------------------------------------------------------------------
  int open (const char* path, const char* info,
            eos::common::Mapping::VirtualIdentity &vid, XrdOucErrInfo *error);

  //----------------------------------------------------------------------------
  //! Read a part of the result stream created during open
  //!
  //! @param mOffset offset where to start
  //! @param buff buffer to store stream
  //! @param blen len to return
  //!
  //! @return number of bytes read
  //----------------------------------------------------------------------------
  int read (XrdSfsFileOffset offset, char *buff, XrdSfsXferSize blen);

  //----------------------------------------------------------------------------
  //! Get the size of the result stream
  //!
  //! @param buf stat structure to fill
  //!
  //! @return SFS_OK in any case
  //----------------------------------------------------------------------------
  int stat (struct stat* buf);

  //----------------------------------------------------------------------------
  //! Close the proc stream and store the clients comment for the command in the
  //! comment log file
  //!
  //! @return 0 if comment has been successfully stored otherwise != 0
  //----------------------------------------------------------------------------
  int close ();

  //----------------------------------------------------------------------------
  //! Add stdout,stderr to an external stdout,stderr variable
  //----------------------------------------------------------------------------
  void
  AddOutput (XrdOucString &lStdOut, XrdOucString &lStdErr)
  {
    lStdOut += stdOut;
    lStdErr += stdErr;
  }

  //----------------------------------------------------------------------------
  //! Open temporary outputfiles for find commands
  //!
  //! @return true if successful otherwise false
  //----------------------------------------------------------------------------
  bool OpenTemporaryOutputFiles ();

  //----------------------------------------------------------------------------
  //! Get the return code of a proc command
  //----------------------------------------------------------------------------
  inline int GetRetc ()
  {
    return retc;
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
  int Attr ();
  int Archive();
  int Backup();
  int Cd ();
  int Chmod ();
  int DirInfo (const char* path);
  int Find ();
  int File ();
  int Fileinfo ();
  int FileInfo (const char* path);
  int Fuse ();
  int Ls ();
  int Map ();
  int Member ();
  int Mkdir ();
  int Motd ();
  int Quota ();
  int Recycle ();
  int Rm ();
  int Rmdir ();
  int Version ();
  int Who ();
  int Whoami ();

  //----------------------------------------------------------------------------
  //! List of admin proc commands
  //----------------------------------------------------------------------------
  int Access ();
  int Chown ();
  int Config ();
  int Debug ();
  int Fs ();
  int Fsck ();
  int Group ();
  int Io ();
  int Node ();
  int Ns ();
  int AdminQuota ();
  int Rtlog ();
  int Space ();
  int Transfer ();
  int Vid ();
  int Vst ();

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
  //! Response structre holding information about the status of an archived dir
  //----------------------------------------------------------------------------
  struct ArchDirStatus
  {
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

private:
  XrdOucString path; //< path argument for the proc command
  eos::common::Mapping::VirtualIdentity* pVid; //< pointer to virtual identity
  XrdOucString mCmd; //< proc command name
  XrdOucString mSubCmd; //< proc sub command name
  XrdOucString mArgs; //< full args from opaque input

  XrdOucString stdOut; //< stdOut returned by proc command
  XrdOucString stdErr; //< stdErr returned by proc command
  XrdOucString stdJson; //< JSON output returned by proc command
  int retc; //< return code from the proc command
  XrdOucString mResultStream; //< string containing the assembled stream
  XrdOucEnv* pOpaque; //< pointer to the opaque information object
  const char* ininfo; //< original opaque info string
  bool mDoSort; //< sort flag (true = sorting)
  const char* mSelection; //< selection argument from the opaque request
  XrdOucString mOutFormat; //< output format type e.g. fuse or json

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

  XrdOucString mComment; //< comment issued by the user for the proc comamnd
  time_t mExecTime; //< execution time measured for the proc command

  size_t mLen; //< len of the result stream
  off_t mOffset; //< offset from where to read in the result stream
  bool mAdminCmd; // < indicates an admin command
  bool mUserCmd; //< indicates a user command

  bool mFuseFormat; //< indicates FUSE format
  bool mJsonFormat; //< indicates JSON format
  bool mHttpFormat; //< indicates HTTP format
  bool mClosed; //< indicates the proc command has been closed already
  XrdOucString mJsonCallback; //< sets the JSONP callback namein a response

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
                     const std::string& dst_url, int fid);

  //----------------------------------------------------------------------------
  //! Get list of archived files from the proc/archive directory
  //!
  //! @param root root of subtree for which we collect archvied entries
  //!
  //! @return vector containing the full path of the directories currently
  //!         archived
  //----------------------------------------------------------------------------
  std::vector<ArchDirStatus> ArchiveGetDirs(const std::string& root) const;

  //----------------------------------------------------------------------------
  //! Update the status of the archived directories depending on the infomation
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
  //! @param arch_dir EOS directory beeing archived
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
  //! @param cmd_json command to be sent to the archive dameon
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
  //         also not checked during the verification step
  //!
  //! @return 0 if successful, otherwise errno. It sets the global retc in case
  //!         of error
  //----------------------------------------------------------------------------
  int BackupCreate(const std::string& backup_dir,
                   const std::string& dst_url,
                   const std::string& twindow_type,
                   const std::string& twindow_val,
                   const std::set<std::string>& excl_xattr);

  //----------------------------------------------------------------------------
  //! Create a result stream from stdOut, stdErr & retc
  //----------------------------------------------------------------------------
  void MakeResult ();

  //----------------------------------------------------------------------------
  //! Helper function able to detect key value pair output and convert to http
  //! table format
  //----------------------------------------------------------------------------
  bool KeyValToHttpTable(XrdOucString &stdOut);
};


//------------------------------------------------------------------------------
//! Class ProcInterface
//------------------------------------------------------------------------------
class ProcInterface
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  ProcInterface () {};

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~ProcInterface () {};

  //----------------------------------------------------------------------------
  //! Check if a path is requesting a proc commmand
  //!
  //! @param path input path for a proc command
  //!
  //! @return true if proc command otherwise false
  //----------------------------------------------------------------------------
  static bool IsProcAccess (const char* path);

  //----------------------------------------------------------------------------
  //! Check if a proc command contains a 'write' action on the instance
  //!
  //! @param path input arguments for proc command
  //! @param info CGI for proc command
  //!
  //! @return true if write access otherwise false
   //----------------------------------------------------------------------------
  static bool IsWriteAccess (const char* path, const char* info);

  //----------------------------------------------------------------------------
  //! Authorize if the virtual ID can execute the requested command
  //!
  //! @param path specifies user or admin command path
  //! @param info CGI providing proc arguments
  //! @param vid virtual id of the client
  //! @param entity security entity object
  //!
  //! @return true if authorized otherwise false
  //----------------------------------------------------------------------------
  static bool Authorize (const char* path, const char* info,
                         eos::common::Mapping::VirtualIdentity &vid,
                         const XrdSecEntity* entity);
};

EOSMGMNAMESPACE_END

#endif // __EOSMGM_PROCINTERFACE__HH__
