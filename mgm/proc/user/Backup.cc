//------------------------------------------------------------------------------
// File: Backup.cc
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
#include <string>
#include <iomanip>
/*----------------------------------------------------------------------------*/
#include "mgm/ProcInterface.hh"
#include "mgm/XrdMgmOfs.hh"
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Backup command
//------------------------------------------------------------------------------
int ProcCommand::Backup()
{
  std::string src_surl = pOpaque->Get("mgm.backup.src");
  std::string dst_surl = pOpaque->Get("mgm.backup.dst");
  XrdCl::URL src_url(src_surl);
  XrdCl::URL dst_url(dst_surl);
  std::ostringstream oss;

  if (!src_url.IsValid() || !dst_url.IsValid())
  {
    stdErr = "error: both backup source and destination must be valid XRootD URLs";
    return SFS_OK;
  }

  // If local path we assume local EOS instance
  if (src_url.GetProtocol() == "file")
  {
    if (*src_surl.rbegin() != '/')
      src_surl += '/';

    oss << "root://" << gOFS->ManagerId << "/" << src_surl;
    src_url.FromString(oss.str());
    src_surl = src_url.GetURL();
  }

  if (dst_url.GetProtocol() == "file")
  {
    if (*dst_surl.rbegin() != '/')
      dst_surl += '/';

    oss.clear();
    oss.str("");
    oss << "root://" << gOFS->ManagerId << "/" << dst_surl;
    dst_url.FromString(oss.str());
    dst_surl = dst_url.GetURL();
  }

  // Check that the destination directory does not exist already
  eos_debug("backup src=%s, dst=%s", src_surl.c_str(), dst_surl.c_str());
  XrdCl::FileSystem fs(dst_url);
  XrdCl::StatInfo* stat_info = 0;
  XrdCl::XRootDStatus st = fs.Stat(dst_url.GetPath(), stat_info, 5);

  if (st.IsOK())
  {
    stdErr = "error: backup destination already exists";
    retc = EEXIST;
    return SFS_OK;
  }

  delete stat_info;

  // Create backup file and copy it to the destination location
  int ret = BackupCreate(src_surl, dst_surl);

  if (!ret)
  {
    std::string bfile_url = dst_url.GetURL();
    bfile_url += EOS_COMMON_PATH_BACKUP_FILE_PREFIX;
    bfile_url += "backup.file";
    std::ostringstream cmd_json;
    cmd_json << "{\"cmd\": \"backup\", "
	     << "\"src\": \"" << bfile_url.c_str() << "\", "
	     << "\"opt\": \"\", "
	     << "\"uid\": \"" << pVid->uid << "\", "
	     << "\"gid\": \"" << pVid->gid << "\" "
	     << "}";

    ret = ArchiveExecuteCmd(cmd_json.str());
    eos_debug("sending command: %s", cmd_json.str().c_str());
  }

  eos_debug("retc=%i, stdOut=%s, stdErr=%s", retc, stdOut.c_str(), stdErr.c_str());
  return SFS_OK;
}


//------------------------------------------------------------------------------
// Create backup file which uses functionality from the archive mechanism
//------------------------------------------------------------------------------
int
ProcCommand::BackupCreate(const std::string& src_surl,
			  const std::string& dst_surl)
{
  int num_dirs = 0;
  int num_files = 0;
  XrdCl::URL src_url(src_surl);

  // Open temporary file in which we construct the backup file
  std::ostringstream oss;
  oss << "/tmp/eos.mgm/backup." << XrdSysThread::ID();
  std::string backup_fn = oss.str();
  std::ofstream backup_ofs(backup_fn.c_str());

  if (!backup_ofs.is_open())
  {
    eos_err("failed to open local archive file:%s", backup_fn.c_str());
    stdErr = "failed to open archive file at MGM ";
    retc = EIO;
    return retc;
  }

  // Write backup JSON header leaving blank the fields for the number of
  // files/dirs and timestamp which will be filled in later on.
  // Note: we treat backups as archive get operations from tape to disk
  // therefore we need to swapt the src with destination in the header
  backup_ofs << "{"
	     << "\"src\": \"" << dst_surl << "\", "
	     << "\"dst\": \"" << src_surl << "\", "
	     << "\"svc_class\": \"\", "
	     << "\"dir_meta\": [\"uid\", \"gid\", \"mode\", \"attr\"], "
	     << "\"file_meta\": [\"size\", \"mtime\", \"ctime\", \"uid\", \"gid\", "
	     << "\"mode\", \"xstype\", \"xs\"], "
	     << "\"num_dirs\": " << std::setw(10) << "" << ", "
	     << "\"num_files\": " << std::setw(10) << "" << ", "
	     << "\"uid\": \"" << pVid->uid << "\", "
	     << "\"gid\": \"" << pVid->gid << "\", "
	     << "\"timestamp\": " << std::setw(10) << ""
	     << "}" << std::endl;

  // Add directories info
  if (ArchiveAddEntries(src_url.GetPath(), backup_ofs, num_dirs, false))
  {
    backup_ofs.close();
    unlink(backup_fn.c_str());
    return retc;
  }

  // Add files info
  if (ArchiveAddEntries(src_url.GetPath(), backup_ofs, num_files, true) ||
      (num_files == 0))
  {
    backup_ofs.close();
    unlink(backup_fn.c_str());
    return retc;
  }

  // Rewind the stream and update the header with the number of files and dirs
  num_dirs--; // don't count current dir
  backup_ofs.seekp(0);
  backup_ofs << "{"
	     << "\"src\": \"" << dst_surl << "\", "
	     << "\"dst\": \"" << src_surl << "\", "
	     << "\"svc_class\": \"\", "
	     << "\"dir_meta\": [\"uid\", \"gid\", \"mode\", \"attr\"], "
	     << "\"file_meta\": [\"size\", \"mtime\", \"ctime\", \"uid\", \"gid\", "
	     << "\"mode\", \"xstype\", \"xs\"], "
	     << "\"num_dirs\": " << std::setw(10) << num_dirs << ", "
	     << "\"num_files\": " << std::setw(10) << num_files << ", "
	     << "\"uid\": \"" << pVid->uid << "\", "
	     << "\"gid\": \"" << pVid->gid << "\", "
	     << "\"timestamp\": " << std::setw(10) << time(static_cast<time_t*>(0))
	     << "}" << std::endl;
  backup_ofs.close();

  // Copy local backup file to backup destination
  XrdCl::PropertyList properties;
  XrdCl::PropertyList result;
  XrdCl::URL url_src, url_dst;
  XrdCl::URL tmp_url(dst_surl);
  std::string dst_path = tmp_url.GetPath();
  dst_path += EOS_COMMON_PATH_BACKUP_FILE_PREFIX;
  dst_path += "backup.file";
  url_src.SetProtocol("file");
  url_src.SetPath(backup_fn.c_str());
  url_dst.SetProtocol(tmp_url.GetProtocol());
  url_dst.SetHostName(tmp_url.GetHostName());
  url_dst.SetUserName("root");
  url_dst.SetPath(dst_path);
  url_dst.SetParams("eos.ruid=0&eos.rgid=0");
  properties.Set("source", url_src);
  properties.Set("target", url_dst);

  XrdCl::CopyProcess copy_proc;
  copy_proc.AddJob(properties, &result);
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

  // Remove local backup file
  unlink(backup_fn.c_str());
  return 0;
}


EOSMGMNAMESPACE_END
