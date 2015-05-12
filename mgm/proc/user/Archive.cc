//------------------------------------------------------------------------------
// File: Archive.cc
// Author: Elvin-Alin Sindrilaru <esindril@cern.ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2014 CERN/Switzerland                                  *
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
#include "mgm/ProcInterface.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/XrdMgmOfsDirectory.hh"
#include "mgm/Access.hh"
#include "mgm/Macros.hh"
#include "mgm/Acl.hh"
/*----------------------------------------------------------------------------*/
#include "common/SymKeys.hh"
/*----------------------------------------------------------------------------*/
#include <iomanip>
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

static const std::string ARCH_INIT = ".archive.init";
static const std::string ARCH_PUT_DONE = ".archive.put.done";
static const std::string ARCH_PUT_ERR = ".archive.put.err";
static const std::string ARCH_GET_DONE = ".archive.get.done";
static const std::string ARCH_GET_ERR = ".archive.get.err";
static const std::string ARCH_PURGE_DONE = ".archive.purge.done";
static const std::string ARCH_PURGE_ERR = ".archive.purge.err";
static const std::string ARCH_DELETE_ERR = ".archive.delete.err";
static const std::string ARCH_LOG = ".archive.log";


//------------------------------------------------------------------------------
// Archive command
//------------------------------------------------------------------------------
int
ProcCommand::Archive()
{

  struct stat statinfo;
  std::ostringstream cmd_json;
  std::string option = (pOpaque->Get("mgm.archive.option") ?
                        pOpaque->Get("mgm.archive.option") : "");

  // For listing we don't need an EOS path
  if (mSubCmd == "transfers")
  {
    if (option.empty())
    {
      stdErr = "error: need to provide the archive listing type";
      retc = EINVAL;
    }
    else
    {
      cmd_json << "{\"cmd\": " << "\"" << mSubCmd.c_str() << "\", "
               << "\"opt\": " <<  "\"" << option << "\", "
               << "\"uid\": " << "\"" << pVid->uid << "\", "
               << "\"gid\": " << "\"" << pVid->gid << "\", "
               << "\"format\": " << "\"pretty\" "
               << "}";
    }
  }
  else if (mSubCmd == "kill")
  {
    if (option.empty())
    {
      stdErr = "error: need to provide a job_uuid for kill";
      retc = EINVAL;
    }
    else
    {
      cmd_json << "{\"cmd\": " << "\"" << mSubCmd.c_str() << "\", "
               << "\"opt\": " << "\"" << option << "\", "
               << "\"uid\": " << "\"" << pVid->uid << "\", "
               << "\"gid\": " << "\"" << pVid->gid << "\" "
               << "}";
    }
  }
  else if (mSubCmd == "list")
  {
    XrdOucString spath = pOpaque->Get("mgm.archive.path");
    const char* inpath = spath.c_str();
    NAMESPACEMAP;
    if (info) info = 0;
    PROC_BOUNCE_ILLEGAL_NAMES;
    PROC_BOUNCE_NOT_ALLOWED;
    eos::common::Path cPath(path);
    spath = cPath.GetPath();

    // Make sure the EOS directory path ends with '/'
    if (spath[spath.length() - 1] != '/')
      spath += '/';

    std::vector<ArchDirStatus> arch_dirs = ArchiveGetDirs(spath.c_str());
    std::set<std::string> transfer_dirs;

    // First get the list of the ongoing transfers
    cmd_json << "{\"cmd\": \"transfers\", "
             << "\"opt\": \"all\", "
             << "\"uid\": \"" << pVid->uid << "\", "
             << "\"gid\": \"" << pVid->gid << "\", "
             << "\"format\": \"monitor\" "
             << "}";

    retc = ArchiveExecuteCmd(cmd_json.str());

    if (!retc)
    {
      // Parse response from the archiver regarding ongoing transfers
      size_t pos;
      std::istringstream iss(stdOut.c_str());
      std::string entry, key, value;
      stdOut = "";

      while (std::getline(iss, entry, '|'))
      {
        pos = entry.find('=');

        if (pos == std::string::npos)
        {
          stdErr = "error: archive response not in expected format";
          retc = EINVAL;
          break;
        }

        key = entry.substr(0, pos);
        value = entry.substr(pos + 1);

        if (key == "path")
          transfer_dirs.insert(value);
      }
    }

    // Create the table for displaying archive status informations
    std::string sdate;
    size_t max_path_len = 0;
    ArchiveUpdateStatus(arch_dirs, transfer_dirs, max_path_len);
    std::vector<size_t> col_size = {30, max_path_len + 5, 16};
    std::ostringstream oss;
    oss << '|' << std::setfill('-') << std::setw(col_size[0] + 1)
        << '|' << std::setw(col_size[1] + 1)
        << '|' << std::setw(col_size[2] + 1)
        << '|' << std::setfill(' ');
    std::string line = oss.str();
    oss.str("");
    oss.clear();

    // Add tabel header
    oss << line << std::endl
        << '|' << std::setw(col_size[0]) << std::setiosflags(std::ios_base::left)
        << "Creation date"
        << '|' << std::setw(col_size[1]) << std::setiosflags(std::ios_base::left)
        << "Path"
        << '|' << std::setw(col_size[2]) << std::setiosflags(std::ios_base::left)
        << "Status"
        << '|'
        << std::endl << line << std::endl;

    for (auto dir = arch_dirs.begin(); dir != arch_dirs.end(); ++dir)
    {
      sdate = asctime(localtime(&dir->ctime));
      sdate.erase(sdate.find('\n'));
      oss << '|' << std::setw(col_size[0]) << std::setiosflags(std::ios_base::left)
          << sdate
          << '|' << std::setw(col_size[1]) << std::setiosflags(std::ios_base::left)
          << dir->path
          << '|' << std::setw(col_size[2]) << std::setiosflags(std::ios_base::left)
          << dir->status
          << '|'
          << std::endl;
    }

    oss << line << std::endl;
    stdOut = oss.str().c_str();
    return SFS_OK;
  }
  else
  {
    XrdOucString spath = pOpaque->Get("mgm.archive.path");
    const char* inpath = spath.c_str();
    NAMESPACEMAP;
    if (info) info = 0;
    PROC_BOUNCE_ILLEGAL_NAMES;
    PROC_BOUNCE_NOT_ALLOWED;
    eos::common::Path cPath(path);
    spath = cPath.GetPath();

    // Make sure the EOS directory path ends with '/'
    if (spath[spath.length() - 1] != '/')
      spath += '/';

    // Check archive permissions
    if (!ArchiveCheckAcl(spath.c_str()))
    {
      stdErr = "error: failed archive ACL check";
      retc = EPERM;
      return SFS_OK;
    }

    std::ostringstream dir_stream;
    dir_stream << "root://" << gOFS->ManagerId.c_str() << "/" << spath.c_str();
    std::string dir_url = dir_stream.str();

    // Check that the requested path exists and is a directory
    if (gOFS->_stat(spath.c_str(), &statinfo, *mError, *pVid))
    {
      stdErr = "error: requested path does not exits";
      retc = EINVAL;
      return SFS_OK;
    }

    if (!S_ISDIR(statinfo.st_mode))
    {
      stdErr = "error:archive path is not a directory";
      retc = EINVAL;
      return SFS_OK;
    }

    // Used for creating/deleting a file in /eos/.../proc/archive with the same
    // name as the inode value. Used to provide archive fast find functionality.
    int fid = statinfo.st_ino;

    // Create vector containing the paths to all the possible special files
    std::ostringstream oss;
    std::vector<std::string> vect_paths;
    std::vector<std::string> vect_files =
      {ARCH_INIT, ARCH_PUT_DONE, ARCH_PUT_ERR, ARCH_GET_DONE, ARCH_GET_ERR,
       ARCH_PURGE_DONE, ARCH_PURGE_ERR, ARCH_DELETE_ERR, ARCH_LOG};

    for (auto it = vect_files.begin(); it != vect_files.end(); ++it)
    {
      oss.str("");
      oss.clear();
      oss << spath.c_str() << *it;
      vect_paths.push_back(oss.str());
    }

    if (mSubCmd == "create")
    {
      if (!gOFS->MgmArchiveDstUrl.length())
      {
        eos_err("archive destination not configured for this EOS instance");
        stdErr = "error: archive destination not configured for this EOS instance";
        retc = EINVAL;
        return SFS_OK;
      }

      // Build the destination dir by using the uid/gid of the user triggering
      // the archive operation e.g root:// ... //some/dir/gid1/uid1/
      std::string dir_sha256 = eos::common::SymKey::Sha256(spath.c_str());
      std::ostringstream dst_oss;
      dst_oss << gOFS->MgmArchiveDstUrl.c_str() << dir_sha256 << '/';
      std::string surl = dst_oss.str();

      // Make sure the destination directory does not exist
      XrdCl::URL url(surl);
      XrdCl::FileSystem fs(url);
      XrdCl::StatInfo *st_info = 0;
      XrdCl::XRootDStatus status = fs.Stat(url.GetPath(), st_info);

      if (status.IsOK())
      {
        stdErr = "error: archive dst=";
        stdErr += surl.c_str();
        stdErr += " already exists";
        eos_err("%s", stdErr.c_str());
        retc = EIO;
        return SFS_OK;
      }

      ArchiveCreate(spath.c_str(), surl, vect_files, fid);
      return SFS_OK;
    }
    else if ((mSubCmd == "put") ||
             (mSubCmd == "get") ||
             (mSubCmd == "purge") ||
             (mSubCmd == "delete"))
    {
      std::string arch_url = dir_url;

      if (option == "r")
      {
        // Retry failed operation
        option = "retry";
        std::string arch_err = spath.c_str();

        if (mSubCmd == "put")
        {
          arch_err += ARCH_PUT_ERR;
          arch_url += ARCH_PUT_ERR;
        }
        else if (mSubCmd == "get") // get retry
        {
          arch_err += ARCH_GET_ERR;
          arch_url += ARCH_GET_ERR;
        }
        else if (mSubCmd == "purge")
        {
          arch_err += ARCH_PURGE_ERR;
          arch_url += ARCH_PURGE_ERR;
        }
        else if (mSubCmd == "delete")
        {
          arch_err += ARCH_DELETE_ERR;
          arch_url += ARCH_DELETE_ERR;
        }

        if (gOFS->_stat(arch_err.c_str(), &statinfo, *mError, *pVid))
        {
          stdErr = "error: no failed ";
          stdErr += mSubCmd;
          stdErr += " file in directory: ";
          stdErr += spath.c_str();
          retc = EINVAL;
        }
      }
      else
      {
        // Check that the init/put archive file exists
        option = "";
        std::string arch_path = spath.c_str();

        if (mSubCmd == "put") // put
        {
          arch_path += ARCH_INIT;
          arch_url += ARCH_INIT;

          if (gOFS->_stat(arch_path.c_str(), &statinfo, *mError, *pVid))
          {
            stdErr = "error: no archive purge file in directory: ";
            stdErr += spath.c_str();
            retc = EINVAL;
          }
        }
        else if (mSubCmd == "get") // get
        {
          arch_path += ARCH_PURGE_DONE;
          arch_url += ARCH_PURGE_DONE;

          if (gOFS->_stat(arch_path.c_str(), &statinfo, *mError, *pVid))
          {
            stdErr = "error: no archive purge file in directory: ";
            stdErr += spath.c_str();
            retc = EINVAL;
          }
        }
        else if (mSubCmd == "purge") // purge
        {
          arch_path += ARCH_PUT_DONE;

          if (gOFS->_stat(arch_path.c_str(), &statinfo, *mError, *pVid))
          {
            arch_path = spath.c_str();
            arch_path += ARCH_GET_DONE;

            if (gOFS->_stat(arch_path.c_str(), &statinfo, *mError, *pVid))
            {
              stdErr = "error: purge can be done only after a successful " \
                "get or put operation";
              retc = EINVAL;
            }
            else
            {
              arch_url += ARCH_GET_DONE;
            }
          }
          else
          {
            arch_url += ARCH_PUT_DONE;
          }
        }
        else if (mSubCmd == "delete") // delete
        {
          if (pVid->uid == 0 && ((pVid->prot == "unix") || (pVid->prot == "sss")))
          {
            bool found = false;
            std::string arch_fn;

            // Check that archive exists in the current directory
            for (auto it = vect_files.begin(); it != vect_files.end(); ++it)
            {
              arch_fn = spath.c_str();
              arch_fn += *it;

              if ((*it != ARCH_LOG) && (!gOFS->_stat(arch_fn.c_str(), &statinfo, *mError, *pVid)))
              {
                arch_url += *it;
                found = true;
                break;
              }
            }

            if (!found)
            {
              stdErr = "error: current directory is not archived";
              retc = EINVAL;
            }
            else
            {
              // Delete the entry in /eos/.../proc/archive/
              std::ostringstream proc_fn;
              proc_fn << gOFS->MgmProcArchivePath << '/' << fid;

              if (gOFS->_rem(proc_fn.str().c_str(), *mError, *pVid))
                stdErr = "warning: unable to remove archive id from /proc fast find";
            }
          }
          else
          {
            stdErr = "error: permission denied, only admin can delete an archive";
            retc = EPERM;
          }
        }
      }

      cmd_json << "{\"cmd\": " << "\"" << mSubCmd.c_str() << "\", "
               << "\"src\": " << "\"" << arch_url << "\", "
               << "\"opt\": " << "\"" << option  << "\", "
               << "\"uid\": " << "\"" << pVid->uid << "\", "
               << "\"gid\": " << "\"" << pVid->gid << "\" "
               << "}";
    }
    else
    {
      stdErr = "error: operation not supported, needs to be one of the following: "
        "create, put, get or list";
      retc = EINVAL;
    }
  }

  // Send request to archiver process if no error occured
  if (!retc)
    retc = ArchiveExecuteCmd(cmd_json.str());

  eos_debug("retc=%i, stdOut=%s, stdErr=%s", retc, stdOut.c_str(), stdErr.c_str());
  return SFS_OK;
}


//------------------------------------------------------------------------------
// Get archive status for both already archived directories as well as for dirs
// that have ongoing transfers
//------------------------------------------------------------------------------
void
ProcCommand::ArchiveUpdateStatus(std::vector<ProcCommand::ArchDirStatus>& dirs,
                                 const std::set<std::string>& tx_dirs,
                                 size_t& max_len_path)
{
  std::string path;
  std::vector<std::string> vect_files = {ARCH_INIT, ARCH_PUT_DONE, ARCH_PUT_ERR,
                                         ARCH_GET_DONE, ARCH_GET_ERR, ARCH_PURGE_ERR,
                                         ARCH_PURGE_DONE, ARCH_DELETE_ERR};
  XrdSfsFileExistence exists_flag;
  XrdOucErrInfo out_error;

  for (auto dir = dirs.begin(); dir != dirs.end(); ++dir)
  {
    // Update the max path length
    if (dir->path.length() > max_len_path)
      max_len_path = dir->path.length();

    if (tx_dirs.find(dir->path) != tx_dirs.end())
    {
      dir->status = "transferring";
    }
    else
    {
      for (auto st_file = vect_files.begin(); st_file != vect_files.end(); ++st_file)
      {
        path = dir->path + *st_file;

        if (gOFS->_exists(path.c_str(), exists_flag, out_error) == SFS_OK &&
            ((exists_flag & XrdSfsFileExistIsFile) == true ))
        {
          if (*st_file == ARCH_INIT)
            dir->status = "created";
          else if (*st_file == ARCH_PUT_DONE)
            dir->status = "put done";
          else if (*st_file == ARCH_PUT_ERR)
            dir->status = "put failed";
          else if (*st_file == ARCH_GET_DONE)
            dir->status = "get done";
          else if (*st_file == ARCH_GET_ERR)
            dir->status = "get failed";
          else if (*st_file == ARCH_PURGE_DONE)
            dir->status = "purge done";
          else if (*st_file == ARCH_PURGE_ERR)
            dir->status = "purge failed";
          else if (*st_file == ARCH_DELETE_ERR)
            dir->status = "delete failed";
          break;
        }
      }
    }
  }
}


//------------------------------------------------------------------------------
// Get the list of files in proc/arhive whose name represents the fid of the
// archived directory
//------------------------------------------------------------------------------
std::vector<ProcCommand::ArchDirStatus>
ProcCommand::ArchiveGetDirs(const std::string& root) const
{
  const char* dname;
  struct stat buf;
  std::string full_path;
  std::map<std::string, time_t> fids;
  eos::common::Mapping::VirtualIdentity_t root_ident;
  eos::common::Mapping::Root(root_ident);
  XrdOucErrInfo out_error;
  XrdMgmOfsDirectory proc_dir = XrdMgmOfsDirectory();
  int retc = proc_dir._open(gOFS->MgmProcArchivePath.c_str(),
                            root_ident, static_cast<const char*>(0));

  if (retc)
    return std::vector<ArchDirStatus>();

  while ((dname = proc_dir.nextEntry()))
  {
    if (dname[0] != '.')
    {
      full_path = gOFS->MgmProcArchivePath.c_str();
      full_path += '/';
      full_path += dname;

      if (gOFS->_stat(full_path.c_str(), &buf, out_error, root_ident) == SFS_OK)
        fids[dname] = buf.st_ctime;
    }
  }

  proc_dir.close();
  std::istringstream iss;
  eos::ContainerMD* cmd = 0;
  eos::ContainerMD::id_t id;
  std::vector<ArchDirStatus> dirs;

  {
    eos::common::RWMutexReadLock nsLock(gOFS->eosViewRWMutex);

    for (auto fid = fids.begin(); fid != fids.end(); ++fid)
    {
      // Convert string id to ContainerMD:id_t
      iss.str("");
      iss.clear();
      iss.str(fid->first);
      iss >> id;

      try
      {
        cmd = gOFS->eosDirectoryService->getContainerMD(id);
        full_path = gOFS->eosView->getUri(cmd);

        // If archive directory is in the currently searched subtree
        if (full_path.find(root) == 0)
        {
          ArchDirStatus dstatus(fid->second, full_path, "unknown");
          dirs.push_back(dstatus);
        }
      }
      catch (eos::MDException &e)
      {
        errno = e.getErrno();
        eos_static_err("fid=%016x errno=%d msg=\"%s\"\n",
                       id, e.getErrno(), e.getMessage().str().c_str());
      }
    }
  }

  return dirs;
}


//------------------------------------------------------------------------------
// Send command to archive daemon and collect the response
//------------------------------------------------------------------------------
int
ProcCommand::ArchiveExecuteCmd(const::string& cmd)
{
  int retc = 0;
  int sock_linger = 0;
  zmq::context_t zmq_ctx(1);
  zmq::socket_t socket(zmq_ctx, ZMQ_REQ);
#if ZMQ_VERSION >= 20200
  int sock_timeout = 1500; // 1,5s
  socket.setsockopt(ZMQ_RCVTIMEO, &sock_timeout, sizeof(sock_timeout));
#endif
  socket.setsockopt(ZMQ_LINGER, &sock_linger, sizeof(sock_linger));

  try
  {
    socket.connect(gOFS->mArchiveEndpoint.c_str());
  }
  catch (zmq::error_t& zmq_err)
  {
    eos_static_err("connect to archiver failed");
    stdErr = "error: connect to archiver failed";
    retc = EINVAL;
  }

  if (!retc)
  {
    zmq::message_t msg((void*)cmd.c_str(), cmd.length(), NULL);

    try
    {
      if (!socket.send(msg))
      {
        stdErr = "error: send request to archiver";
        retc = EINVAL;
      }
      else if (!socket.recv(&msg))
      {
        stdErr = "error: no response from archiver";
        retc = EINVAL;
      }
      else
      {
        // Parse response from the archiver
        XrdOucString msg_str((const char*) msg.data(), msg.size());
        //eos_info("Msg_str:%s", msg_str.c_str());
        std::istringstream iss(msg_str.c_str());
        std::string status, line, response;
        iss >> status;

        // Discard whitespaces from the beginning
        while (getline(iss >> std::ws, line))
        {
          response += line;

          if (iss.good())
            response += '\n';
        }

        if (status == "OK")
        {
          stdOut = response.c_str();
        }
        else if (status == "ERROR")
        {
          stdErr = response.c_str();
          retc = EINVAL;
        }
        else
        {
          stdErr = "error: unknown response format from archiver";
          retc = EINVAL;
        }
      }
    }
    catch (zmq::error_t& zmq_err)
    {
      stdErr = "error: timeout getting response from archiver, msg: ";
      stdErr += zmq_err.what();
      retc = EINVAL;
    }
  }

  return retc;
}


//------------------------------------------------------------------------------
// Check if the current user has the necessary permissions to do an archiving
// operation
//------------------------------------------------------------------------------
bool
ProcCommand::ArchiveCheckAcl(const std::string& arch_dir) const
{
  bool is_allowed = false;
  eos::ContainerMD* dir = 0;
  eos::ContainerMD* copy_dir = 0;
  eos::ContainerMD::XAttrMap attrmap;

  {
    eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);

    // Check for the parent directory
    try
    {
      dir = gOFS->eosView->getContainer(arch_dir.c_str());
      copy_dir = new eos::ContainerMD(*dir);
      dir = copy_dir;
      eos::ContainerMD::XAttrMap::const_iterator it;

      for (it = dir->attributesBegin(); it != dir->attributesEnd(); ++it)
        attrmap[it->first] = it->second;

    }
    catch (eos::MDException &e)
    {
      dir = 0;
      eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                e.getErrno(), e.getMessage().str().c_str());
    }
  }

  if (dir)
  {
    // ACL and permission check
    eos::mgm::Acl acl(attrmap.count("sys.acl") ? attrmap["sys.acl"] : std::string(""),
                      attrmap.count("user.acl") ? attrmap["user.acl"] : std::string(""),
                      *pVid, attrmap.count("sys.eval.useracl"));
    eos_info("acl=%d a=%d egroup=%d mutable=%d", acl.HasAcl(), acl.CanArchive(),
             acl.HasEgroup(), acl.IsMutable());

    if (pVid->uid)
      is_allowed = acl.CanArchive();
    else
      is_allowed = true;
  }

  if (copy_dir)
    delete copy_dir;

  return is_allowed;
}


//------------------------------------------------------------------------------
// Create archive file.
//------------------------------------------------------------------------------
void
ProcCommand::ArchiveCreate(const std::string& arch_dir,
                           const std::string& dst_url,
                           const std::vector<std::string>& vect_files,
                           int fid)
{
  int num_dirs = 0;
  int num_files = 0;

  if (MakeSubTreeImmutable(arch_dir, vect_files))
    return;

  // Create the output directory if necessary and open the temporary file in
  // which we construct the archive file
  std::ostringstream oss;
  oss << "/tmp/eos.mgm/archive." << XrdSysThread::ID();
  std::string arch_fn = oss.str();
  std::ofstream arch_ofs(arch_fn.c_str());

  if (!arch_ofs.is_open())
  {
    eos_err("failed to open local archive file:%s", arch_fn.c_str());
    stdErr = "failed to open archive file at MGM ";
    retc = EIO;
    return;
  }

  // Write archive JSON header leaving blank the fields for the number of
  // files/dirs and timestamp which will be filled in later on
  arch_ofs << "{"
           << "\"src\": \"" << "root://" << gOFS->ManagerId << "/" << arch_dir << "\", "
           << "\"dst\": \"" << dst_url << "\", "
           << "\"svc_class\": \"" << gOFS->MgmArchiveSvcClass << "\", "
           << "\"dir_meta\": [\"uid\", \"gid\", \"mode\", \"attr\"], "
           << "\"file_meta\": [\"size\", \"mtime\", \"ctime\", \"uid\", \"gid\", "
           << "\"mode\", \"xstype\", \"xs\"], "
           << "\"uid\": \"" << pVid->uid << "\", "
           << "\"gid\": \"" << pVid->gid << "\", "
           << "\"timestamp\": " << std::setw(10) << "" << ", "
           << "\"num_dirs\": " << std::setw(10) << "" << ", "
           << "\"num_files\": " << std::setw(10) << ""
           << "}" << std::endl;

  // Add directories info
  if (ArchiveAddEntries(arch_dir, arch_ofs, num_dirs, false))
  {
    MakeSubTreeMutable(arch_dir);
    arch_ofs.close();
    unlink(arch_fn.c_str());
    return;
  }

  // Add files info
  if (ArchiveAddEntries(arch_dir, arch_ofs, num_files, true) || (num_files == 0))
  {
    MakeSubTreeMutable(arch_dir);
    arch_ofs.close();
    unlink(arch_fn.c_str());
    return;
  }

  // Rewind the stream and update the header with the number of files and dirs
  num_dirs--; // don't count current dir
  arch_ofs.seekp(0);
  arch_ofs << "{"
           << "\"src\": \"" << "root://" << gOFS->ManagerId << "/" << arch_dir << "\", "
           << "\"dst\": \"" << dst_url << "\", "
           << "\"svc_class\": \"" << gOFS->MgmArchiveSvcClass << "\", "
           << "\"dir_meta\": [\"uid\", \"gid\", \"mode\", \"attr\"], "
           << "\"file_meta\": [\"size\", \"mtime\", \"ctime\", \"uid\", \"gid\", "
           << "\"mode\", \"xstype\", \"xs\"], "
           << "\"uid\": \"" << pVid->uid << "\", "
           << "\"gid\": \"" << pVid->gid << "\", "
           << "\"timestamp\": " << std::setw(10) << time(static_cast<time_t*>(0)) << ", "
           << "\"num_dirs\": " << std::setw(10) << num_dirs << ", "
           << "\"num_files\": " << std::setw(10) << num_files
           << "}" << std::endl;
  arch_ofs.close();

  // Copy local archive file to archive directory in EOS
  struct XrdCl::JobDescriptor copy_job;
  copy_job.source.SetProtocol("file");
  copy_job.source.SetPath(arch_fn.c_str());
  copy_job.target.SetProtocol("root");
  copy_job.target.SetHostName("localhost");
  copy_job.target.SetUserName("root");
  std::string dst_path = arch_dir;
  dst_path += ARCH_INIT;
  copy_job.target.SetPath(dst_path);
  copy_job.target.SetParams("eos.ruid=0&eos.rgid=0");

  XrdCl::CopyProcess copy_proc;
  copy_proc.AddJob(&copy_job);
  XrdCl::XRootDStatus status_prep = copy_proc.Prepare();

  if (status_prep.IsOK())
  {
    XrdCl::XRootDStatus status_run = copy_proc.Run(0);

    if (!status_run.IsOK())
    {
      stdErr = "error: failed run for copy process, msg=";
      stdErr += status_run.ToStr().c_str();
      retc = EIO;
    }
  }
  else
  {
    stdErr = "error: failed prepare for copy process, msg=";
    stdErr += status_prep.ToStr().c_str();
    retc = EIO;
  }

  // Remove local archive file
  unlink(arch_fn.c_str());

  // Change the permissions on the archive file to 644
  eos::common::Mapping::VirtualIdentity_t root_ident;
  eos::common::Mapping::Root(root_ident);
  XrdSfsMode mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;

  if (gOFS->_chmod(dst_path.c_str(), mode, *mError, root_ident))
  {
    stdErr = "error: setting permisions on the archive file";
    retc = EIO;
  }

  // Add the dir inode to /proc/archive/ for fast find
  if (!retc)
  {
    oss.clear();
    oss.str("");
    oss << gOFS->MgmProcArchivePath << "/" << fid;

    if (gOFS->_touch(oss.str().c_str(), *mError, root_ident))
    {
      stdOut = "warning: failed to create file in /eos/.../proc/archive/";
      return;
    }
  }
}


//------------------------------------------------------------------------------
// Make EOS sub-tree immutable/mutable by adding/removing the sys.acl=z:i from
// all of the directories in the subtree.
//------------------------------------------------------------------------------
int
ProcCommand::MakeSubTreeImmutable(const std::string& arch_dir,
                                  const std::vector<std::string>& vect_files)
{
  bool found_archive = false;
  // Map of directories to set of files
  std::map< std::string, std::set<std::string> > found;

  // Check for already archived directories in the current sub-tree
  if (gOFS->_find(arch_dir.c_str(), *mError, stdErr, *pVid, found,
                 (const char*) 0, (const char*) 0))
  {
    eos_err("dir=%s list all err=%s", arch_dir.c_str(), stdErr.c_str());
    retc = errno;
    return retc;
  }

  for (auto it = found.begin(); it != found.end(); ++it)
  {
    for (auto itf = vect_files.begin(); itf != vect_files.end(); ++itf)
    {
      if (it->second.find(*itf) != it->second.end())
      {
        found_archive = true;
        stdErr = "error: another archive found in current sub-tree in ";
        stdErr += it->first.c_str();
        stdErr += itf->c_str();
        break;
      }
    }

    if (found_archive)
      break;
  }

  if (found_archive)
  {
    // Forbit archiving of archives
    retc = EPERM;
    return retc;
  }

  // Make the EOS sub-tree immutable e.g.: add sys.acl=z:i
  eos::common::Mapping::VirtualIdentity_t root_ident;
  eos::common::Mapping::Root(root_ident);
  const char* acl_key = "sys.acl";
  XrdOucString acl_val;

  for (auto it = found.begin(); it != found.end(); ++it)
  {
    acl_val = "";

    if (!gOFS->_attr_get(it->first.c_str(), *mError, *pVid,
                         (const char*) 0, acl_key, acl_val))
    {
      // Add immutable only if not already present
      int pos_z = acl_val.find("z:");

      if (pos_z != STR_NPOS)
      {
        if (acl_val.find('i', pos_z + 2) == STR_NPOS)
          acl_val.insert('i', pos_z + 2);
      }
      else
        acl_val += ",z:i";
    }
    else
      acl_val = "z:i";

    eos_debug("acl_key=%s, acl_val=%s", acl_key, acl_val.c_str());

    if (gOFS->_attr_set(it->first.c_str(), *mError, root_ident,
                         (const char*) 0, acl_key, acl_val.c_str()))
    {
      stdErr = "error: making EOS subtree immutable, dir=";
      stdErr += arch_dir.c_str();
      retc = mError->getErrInfo();
      break;
    }
  }

  return retc;
}


//----------------------------------------------------------------------------
// Make EOS sub-tree mutable by removing the sys.acl=z:i rule from all of the
// directories in the sub-tree.
//----------------------------------------------------------------------------
int
ProcCommand::MakeSubTreeMutable(const std::string& arch_dir)
{
  std::map< std::string, std::set<std::string> > found;

  // Get all dirs in current subtree
  if (gOFS->_find(arch_dir.c_str(), *mError, stdErr, *pVid, found,
                 (const char*) 0, (const char*) 0))
  {
    eos_err("dir=%s list all err=%s", arch_dir.c_str(), stdErr.c_str());
    retc = errno;
    return retc;
  }

  // Make the EOS sub-tree mutable e.g.: remove sys.acl=z:i
  eos::common::Mapping::VirtualIdentity_t root_ident;
  eos::common::Mapping::Root(root_ident);
  const char* acl_key = "sys.acl";
  XrdOucString acl_val;
  std::string new_acl;

  for (auto it = found.begin(); it != found.end(); ++it)
  {
    acl_val = "";

    if (!gOFS->_attr_get(it->first.c_str(), *mError, *pVid,
                         (const char*) 0, acl_key, acl_val))
    {
      std::istringstream iss(acl_val.c_str());
      std::string rule;
      new_acl = "";

      while (std::getline(iss, rule, ','))
      {
        if (rule.find("z:") == 0)
        {
          rule.erase(rule.find('i'), 1);

          if (rule.length() > 2)
          {
            new_acl += rule;
            new_acl += ',';
          }
        }
        else
        {
          // Don' modify the rest of the rules
          new_acl += rule;
          new_acl += ',';
        }
      }

      // Remove last comma
      if (new_acl.length())
        new_acl.erase(new_acl.length() - 1);

      acl_val = new_acl.c_str();
    }
    else
    {
      eos_warning("Dir=%s no xattrs", it->first.c_str());
      continue;
    }

    eos_debug("acl_key=%s, acl_val=%s", acl_key, acl_val.c_str());

    // Update the new sys.acl xattr
    if (acl_val.length())
    {
      if (gOFS->_attr_set(it->first.c_str(), *mError, root_ident,
                          (const char*) 0, acl_key, acl_val.c_str()))
      {
        stdErr = "error: making EOS subtree mutable (update sys.acl), dir=";
        stdErr += arch_dir.c_str();
        retc = mError->getErrInfo();
        break;
      }
    }
    else
    {
      // Completely remove the sys.acl xattr
      if (gOFS->_attr_rem(it->first.c_str(), *mError, root_ident,
                          (const char*) 0, acl_key))
      {
        stdErr = "error: making EOS subtree mutable (rm sys.acl), dir=";
        stdErr += arch_dir.c_str();
        retc = mError->getErrInfo();
        break;
      }
    }
  }

  return retc;
}


//------------------------------------------------------------------------------
// Get fileinfo for all files/dirs in the subtree and add it to the archive
//------------------------------------------------------------------------------
int
ProcCommand::ArchiveAddEntries(const std::string& arch_dir,
                               std::ofstream& arch_ofs,
                               int& num, bool is_file)
{
  num = 0;
  std::map<std::string, std::string> info_map;
  std::map<std::string, std::string> attr_map; // only for dirs

  // These keys should match the ones in the header dictionary
  if (is_file)
  {
    info_map.insert(std::make_pair("file", "")); // file name
    info_map.insert(std::make_pair("size", ""));
    info_map.insert(std::make_pair("mtime", ""));
    info_map.insert(std::make_pair("ctime", ""));
    info_map.insert(std::make_pair("uid", ""));
    info_map.insert(std::make_pair("gid", ""));
    info_map.insert(std::make_pair("mode", ""));
    info_map.insert(std::make_pair("xstype", ""));
    info_map.insert(std::make_pair("xs", ""));
  }
  else // dir
  {
    info_map.insert(std::make_pair("file", "")); // dir name
    info_map.insert(std::make_pair("uid", ""));
    info_map.insert(std::make_pair("gid", ""));
    info_map.insert(std::make_pair("mode", ""));
    info_map.insert(std::make_pair("xattrn", ""));
    info_map.insert(std::make_pair("xattrv", ""));
  }

  // In C++11 one can simply do:
  /*
    std::map<std::string, std::string> info_map =
    {
      {"size" : ""}, {"mtime": ""}, {"ctime": ""},
      {"uid": ""}, {"gid": ""}, {"xstype": ""}, {"xs": ""}
    };

    or

    std::vector<std::string, std::sting> info_map =  {
      {"uid": ""}, {"gid": ""}, {"attr": ""} };
  */

  std::string line;
  ProcCommand* cmd_find = new ProcCommand();
  XrdOucString info = "&mgm.cmd=find&mgm.path=";
  info += arch_dir.c_str();

  if (is_file)
    info += "&mgm.option=fI";
  else
    info += "&mgm.option=dI";

  cmd_find->open("/proc/user", info.c_str(), *pVid, mError);
  int ret = cmd_find->close();

  if (ret)
  {
    delete cmd_find;
    eos_err("find fileinfo on directory=%s failed", arch_dir.c_str());
    stdErr = "error: find fileinfo failed";
    retc = ret;
    return retc;
  }

  size_t spos = 0;
  size_t key_length = 0; // lenght of file/dir name - it could have spaces
  std::string rel_path;
  std::string key, value, pair;
  std::istringstream line_iss;
  std::ifstream result_ifs(cmd_find->GetResultFn());
  XrdOucString unseal_str;

  if (!result_ifs.good())
  {
    delete cmd_find;
    eos_err("failed to open find fileinfo result file on MGM");
    stdErr = "failed to open find fileinfo result file on MGM";
    retc = EIO;
    return retc;
  }

  char* tmp_buff = new char[4096*4];

  while (std::getline(result_ifs, line))
  {
    if (line.find("&mgm.proc.stderr=") == 0)
      continue;

    if (line.find("&mgm.proc.stdout=") == 0)
      line.erase(0, 17);

    unseal_str = XrdOucString(line.c_str());
    line = XrdMqMessage::UnSeal(unseal_str);
    num++;
    line_iss.clear();
    line_iss.str(line);

    // We assume that the keylength.file parameter is always first in the
    // output of fileinfo -m command
    while(line_iss.good())
    {
      line_iss >> pair;
      spos = pair.find('=');

      if ((spos == std::string::npos) || (!line_iss.good()))
        continue; // not in key=value format

      key = pair.substr(0, spos);
      value = pair.substr(spos + 1);

      if (key == "keylength.file")
      {
        key_length = static_cast<size_t>(atoi(value.c_str()));

        // Read in the file/dir path using the previously read key_length
        int full_length = key_length + 5; // 5 stands for "file="
        line_iss.read(tmp_buff, 1); // read the empty space before "file=..."
        line_iss.read(tmp_buff, full_length);
        tmp_buff[full_length] = '\0';
        pair = tmp_buff;
        spos = pair.find('=');
        key = pair.substr(0, spos);
        value = pair.substr(spos + 1);
      }

      if (info_map.find(key) == info_map.end())
        continue; // not what we are looking for

      if (key == "xattrn")
      {
        // The next token must be an xattrv
        std::string xattrn = value;
        line_iss >> pair;
        spos = pair.find('=');

        if ((spos == std::string::npos) || (!line_iss.good()))
        {
          delete[] tmp_buff;
          delete cmd_find;
          eos_err("malformed xattr pair format");
          stdErr = "malformed xattr pair format";
          retc = EINVAL;
          return retc;
        }

        key = pair.substr(0, spos);
        value = pair.substr(spos + 1);

        if (key != "xattrv")
        {
          delete[] tmp_buff;
          delete cmd_find;
          eos_err("not found expected xattrv");
          stdErr = "not found expected xattrv";
          retc = EINVAL;
          return retc;
        }

        attr_map[xattrn] = value;
      }
      else
      {
        info_map[key] = value;
        eos_debug("key=%s, value=%s", key.c_str(), value.c_str());
      }
    }

    // Add entry info to the archive file with the path names relative to the
    // current archive directory
    rel_path = info_map["file"];
    rel_path.erase(0, arch_dir.length());

    if (rel_path.empty())
      rel_path = "./";

    if (is_file)
    {
      arch_ofs << "[\"f\", \"" << rel_path << "\", "
               << "\"" << info_map["size"] << "\", "
               << "\"" << info_map["mtime"] << "\", "
               << "\"" << info_map["ctime"] << "\", "
               << "\"" << info_map["uid"] << "\", "
               << "\"" << info_map["gid"] << "\", "
               << "\"" << info_map["mode"] << "\", "
               << "\"" << info_map["xstype"] << "\", "
               << "\"" << info_map["xs"] << "\"]"
               << std::endl;
    }
    else
    {
      arch_ofs << "[\"d\", \"" << rel_path << "\", "
               << "\"" << info_map["uid"] << "\", "
               << "\"" << info_map["gid"] << "\", "
               << "\"" << info_map["mode"] << "\", "
               << "{";

      for (auto it = attr_map.begin(); it != attr_map.end(); /*empty*/)
      {
        arch_ofs << "\"" << it->first << "\": \"" << it->second << "\"";
        ++it;

        if (it != attr_map.end())
          arch_ofs << ", ";
      }

      arch_ofs << "}]" << std::endl;
      attr_map.clear();
    }
  }

  delete[] tmp_buff;
  delete cmd_find;
  return retc;
}

EOSMGMNAMESPACE_END
