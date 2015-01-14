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

/*----------------------------------------------------------------------------*/
#include "mgm/Namespace.hh"
#include "common/Logging.hh"
#include "common/Mapping.hh"
#include "proc/proc_fs.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdSfs/XrdSfsInterface.hh"
#include "XrdSec/XrdSecEntity.hh"

/*----------------------------------------------------------------------------*/

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
//! Class handling proc command execution
//------------------------------------------------------------------------------
class ProcCommand : public eos::common::LogId
{
private:

  //----------------------------------------------------------------------------
  //! Response structre holding information about the status of an archived dir
  //----------------------------------------------------------------------------
  struct ArchDirStatus
  {
    time_t ctime;
    std::string path;
    std::string status;

    ArchDirStatus(time_t ct, std::string dpath, std::string st):
      ctime(ct),
      path(dpath),
      status(st)
    {};

    ~ArchDirStatus() {};
  };


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
  unsigned mOutDepth; //< depth of aggregation along the topology tree


  // -------------------------------------------------------------------------
  //! the 'find' command does not keep results in memory but writes to
  //! a temporary output file which is streamed to the client
  // -------------------------------------------------------------------------
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

  // -------------------------------------------------------------------------
  //! Create a result stream from stdOut, stdErr & retc
  // -------------------------------------------------------------------------
  void MakeResult ();

  // helper function able to detect key value pair output and convert to http table format
  bool KeyValToHttpTable(XrdOucString &stdOut);
  bool mAdminCmd; // < indicates an admin command
  bool mUserCmd; //< indicates a user command

  bool mFuseFormat; //< indicates FUSE format
  bool mJsonFormat; //< indicates JSON format
  bool mHttpFormat; //< indicates HTTP format
  bool mClosed; //< indicates the proc command has been closed already

  //----------------------------------------------------------------------------
  //! Create archive file. If successful then the archive file is copied to the 
  //! arch_dir location. If not it sets the retc and stdErr string accordingly.
  //!
  //! @param arch_dir directory for which the archive file is created
  //! @param dst_url archive destination URL (i.e. CASTOR location)
  //! @param vect_files vector of special archive filenames
  //! @param fid inode number of the archive root directory used for fast find
  //!        functionality of archived directories through .../proc/archive/
  //!
  //! @return void, it sets the global retc in case of error
  //----------------------------------------------------------------------------
  void ArchiveCreate(const std::string& arch_dir,
                     const std::string& dst_url,
                     const std::vector<std::string>& vect_files,
                     int fid);


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
  //! Get list of archived files from the proc/archive directory
  //!
  //! @param root root of subtree for which we collect archvied entries
  //!
  //! @return vector containing the full path of the directories currently
  //!         archived
  //----------------------------------------------------------------------------
  std::vector<ArchDirStatus> ArchiveGetDirs(const std::string& root) const;


  //----------------------------------------------------------------------------
  //! Update the status of the archived directories dependin on the infomation
  //! that we got from the archiver daemon. All ongoin transfers will be in
  //! status "transferring" while the rest will display the status of the
  //! archive.
  //!
  //! @param dirs vector of archvied directories
  //! @param tx_dirs set containing the paths of ongoing transfers
  //! @param max_len_path maximum path length used later for listing
  //----------------------------------------------------------------------------
  void ArchiveUpdateStatus(std::vector<ArchDirStatus>& dirs,
                           const std::set<std::string>& tx_dirs,
                           size_t& max_len_path);


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
  //!
  //! @return 0 if successful, otherwise errno
  //----------------------------------------------------------------------------
  int ArchiveAddEntries(const std::string& arch_dir,
                        std::ofstream& arch_ofs,
                        int& num,
                        bool is_file);


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
  //! peration
  //!
  //! @param arch_dir archive directory
  //!
  //! @return true if user is allowed, otherwise False
  //----------------------------------------------------------------------------
  bool ArchiveCheckAcl(const std::string& arch_dir) const;


  //----------------------------------------------------------------------------
  //! Create backup file. If successful then the backup file is copied to the
  //! backup_dir location. If not it sets the retc and stdErr string accordingly.
  //!
  //! @param backup_dir directory for which the backup file is created
  //! @param dst_url backup destination URL ending with '/'
  //! @param twindow_type time window type which can refer either to the
  //!        mtime or the ctime
  //! @param twindow_val time window timestamp
  //!
  //! @return 0 if successful, otherwise errno. It sets the global retc in case
  //!         of error
  //----------------------------------------------------------------------------
  int BackupCreate(const std::string& backup_dir,
                   const std::string& dst_url,
                   const std::string& twindow_type,
                   const std::string& twindow_val);


public:

  //----------------------------------------------------------------------------
  //! The open function calls the requested cmd/subcmd and builds the result
  //----------------------------------------------------------------------------
  int open (const char* path,
            const char* info,
            eos::common::Mapping::VirtualIdentity &vid,
            XrdOucErrInfo *error);

  // -------------------------------------------------------------------------
  //! read a part of the result stream created during open
  // -------------------------------------------------------------------------
  int read (XrdSfsFileOffset offset, char *buff, XrdSfsXferSize blen);

  // -------------------------------------------------------------------------
  //! get the size of the result stream
  // -------------------------------------------------------------------------
  int stat (struct stat* buf);

  // -------------------------------------------------------------------------
  //! close a proc command
  // -------------------------------------------------------------------------
  int close ();

  // -------------------------------------------------------------------------
  //! add stdout,stderr to an external stdout,stderr variable
  // -------------------------------------------------------------------------

  void
  AddOutput (XrdOucString &lStdOut, XrdOucString &lStdErr)
  {
    lStdOut += stdOut;
    lStdErr += stdErr;
  }

  // -------------------------------------------------------------------------
  //! open temporary outputfiles for find commands
  // -------------------------------------------------------------------------
  bool OpenTemporaryOutputFiles ();

  // -------------------------------------------------------------------------
  //! get the return code of a proc command
  // -------------------------------------------------------------------------
  int
  GetRetc ()
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

  // -------------------------------------------------------------------------
  //! list of user proc commands
  // -------------------------------------------------------------------------
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
  int Mkdir ();
  int Motd ();
  int Quota ();
  int Recycle ();
  int Rm ();
  int Rmdir ();
  int Version ();
  int Who ();
  int Whoami ();

  // -------------------------------------------------------------------------
  //! list of admin proc commands
  // -------------------------------------------------------------------------
  int Access ();
  int Chown ();
  int Config ();
  int Debug ();
  int Fs ();
  int Fsck ();
  int GeoSched ();
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

  ProcCommand ();
  ~ProcCommand ();
};

class ProcInterface
{
private:

public:
  // -------------------------------------------------------------------------
  //! check if a path is requesting a proc commmand
  // -------------------------------------------------------------------------
  static bool IsProcAccess (const char* path);

  // -------------------------------------------------------------------------
  //! check if a proc command contains a 'write' action on the instance
  // -------------------------------------------------------------------------
  static bool IsWriteAccess (const char* path, const char* info);

  // -------------------------------------------------------------------------
  //! authorize if the virtual ID can execute the requested command
  // -------------------------------------------------------------------------
  static bool Authorize (
                         const char* path,
                         const char* info,
                         eos::common::Mapping::VirtualIdentity &vid,
                         const XrdSecEntity* entity
                         );

  // -------------------------------------------------------------------------
  //! Constructor
  // -------------------------------------------------------------------------
  ProcInterface ();

  // -------------------------------------------------------------------------
  //! Destructor
  // -------------------------------------------------------------------------
  ~ProcInterface ();
};

EOSMGMNAMESPACE_END

#endif
