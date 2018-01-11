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

#include "common/Path.hh"
#include "mgm/proc/admin/Backup.hh"
#include "mgm/XrdMgmOfs.hh"
#include <string>
#include <iomanip>
#include <fstream>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//                            *** TwindowFilter ***
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// File/dir filter constructor
//------------------------------------------------------------------------------
TwindowFilter::TwindowFilter(const std::string& twindow_type,
                             const std::string& twindow_val):
  mTwindowType(twindow_type),
  mTwindowVal(twindow_val)
{}

//----------------------------------------------------------------------------
// Filter file entry if it is a version file i.e. contains ".sys.v#." or if
// it is ouside the timewindow
//----------------------------------------------------------------------------
bool
TwindowFilter::FilterOutFile(
  const std::map<std::string, std::string>& entry_info)
{
  if (mTwindowType.empty() || mTwindowVal.empty()) {
    return false;
  }

  std::string path = entry_info.find("file")->second;

  // Filter if this is a version file
  if (path.find(".sys.v#.") != std::string::npos) {
    return true;
  }

  const auto iter = entry_info.find(mTwindowType);

  if (iter == entry_info.end()) {
    return false;
  }

  char* end;
  std::string svalue = iter->second;
  float value = strtof(svalue.c_str(), &end);
  float ref_value = strtof(mTwindowVal.c_str(), &end);

  // Filter if ouside the timewindow
  if (value < ref_value) {
    return true;
  }

  // Extract directory/ies that need to stay - we try to remove empty dirs
  size_t pos = 0;

  while ((pos = path.rfind('/')) != std::string::npos) {
    path = path.substr(0, pos + 1);
    mSetDirs.insert(path);
    path = path.substr(0, pos);
  }

  // Always add root directory
  mSetDirs.insert("./");
  return false;
}

//----------------------------------------------------------------------------
// Filter the current dir entry
//----------------------------------------------------------------------------
bool
TwindowFilter::FilterOutDir(const std::string& path)
{
  if (mTwindowType.empty() || mTwindowVal.empty()) {
    return false;
  }

  if (mSetDirs.find(path) != mSetDirs.end()) {
    return false;
  }

  eos_debug("filter out directory=%s", path.c_str());
  return true;
}

//------------------------------------------------------------------------------
//                            *** ProcCommand ***
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Backup command
//------------------------------------------------------------------------------
int ProcCommand::Backup()
{
  std::string src_surl = (pOpaque->Get("mgm.backup.src") ?
                          pOpaque->Get("mgm.backup.src") : "");
  std::string dst_surl = (pOpaque->Get("mgm.backup.dst") ?
                          pOpaque->Get("mgm.backup.dst") : "");

  // Make sure the source and destiantion directories end with "/"
  if (*src_surl.rbegin() != '/') {
    src_surl += '/';
  }

  if (*dst_surl.rbegin() != '/') {
    dst_surl += '/';
  }

  XrdCl::URL src_url(src_surl);
  XrdCl::URL dst_url(dst_surl);
  std::ostringstream oss;

  if (!src_url.IsValid() || !dst_url.IsValid()) {
    stdErr = "error: both backup source and destination must be valid XRootD URLs";
    retc = EINVAL;
    return SFS_OK;
  }

  // If local path we assume local EOS instance
  if (src_url.GetProtocol() == "file") {
    if (*src_surl.rbegin() != '/') {
      src_surl += '/';
    }

    oss << "root://" << gOFS->ManagerId << "/" << src_surl;
    src_url.FromString(oss.str());
    src_surl = src_url.GetURL();
  }

  if (dst_url.GetProtocol() == "file") {
    if (*dst_surl.rbegin() != '/') {
      dst_surl += '/';
    }

    oss.clear();
    oss.str("");
    oss << "root://" << gOFS->ManagerId << "/" << dst_surl;
    dst_url.FromString(oss.str());
    dst_surl = dst_url.GetURL();
  }

  // Create backup file and copy it to the destination location
  std::string twindow_type = (pOpaque->Get("mgm.backup.ttime") ?
                              pOpaque->Get("mgm.backup.ttime") : "");
  std::string twindow_val = (pOpaque->Get("mgm.backup.vtime") ?
                             pOpaque->Get("mgm.backup.vtime") : "");

  if (!twindow_type.empty()  && (twindow_type != "ctime")
      && (twindow_type != "mtime")) {
    stdErr = "error: unkown time window type, should be ctime/mtime";
    retc = EINVAL;
    return SFS_OK;
  }

  // Get the list of excluded extended attribute values which are not enforced
  // and not checked during the verification step
  std::string token;
  std::string str_xattr = (pOpaque->Get("mgm.backup.excl_xattr") ?
                           pOpaque->Get("mgm.backup.excl_xattr") : "");
  std::set<std::string> set_xattrs;
  std::istringstream iss(str_xattr);

  while (std::getline(iss, token, ',')) {
    set_xattrs.insert(token);
  }

  // If create flag is not present then put job in the queue
  if (!pOpaque->Get("mgm.backup.create")) {
    int envlen;
    std::string job_spec = pOpaque->Env(envlen);

    if (!gOFS->SubmitBackupJob(job_spec)) {
      eos_err("error=\"backup job already pending\"");
      stdErr = "error: identic backup job already pending";
      retc = EINVAL;
    }

    return SFS_OK;
  }

  // Do the actual tree scan and build the backup file
  retc = BackupCreate(src_surl, dst_surl, twindow_type, twindow_val, set_xattrs);

  if (!retc) {
    std::string bfile_url = src_url.GetURL();
    bfile_url += EOS_COMMON_PATH_BACKUP_FILE_PREFIX;
    bfile_url += "backup.file";
    std::ostringstream cmd_json;
    // @todo (esindril): The force flag should be passed on from the console
    cmd_json << "{\"cmd\": \"backup\", "
             << "\"src\": \"" << bfile_url.c_str() << "\", "
             << "\"opt\": \"force\", "
             << "\"uid\": \"" << pVid->uid << "\", "
             << "\"gid\": \"" << pVid->gid << "\" "
             << "}";
    retc = ArchiveExecuteCmd(cmd_json.str());
    eos_debug("sending command: %s", cmd_json.str().c_str());
  }

  eos_debug("retc=%i, stdOut=%s, stdErr=%s", retc, stdOut.c_str(),
            stdErr.c_str());
  return SFS_OK;
}

//------------------------------------------------------------------------------
// Create backup file which uses functionality from the archive mechanism
//------------------------------------------------------------------------------
int
ProcCommand::BackupCreate(const std::string& src_surl,
                          const std::string& dst_surl,
                          const std::string& twindow_type,
                          const std::string& twindow_val,
                          const std::set<std::string>& excl_xattr)
{
  int num_dirs = 0;
  int num_files = 0;
  XrdCl::URL src_url(src_surl);
  // Create the output directory if necessary and open the temporary file in
  // which we construct the backup file
  std::ostringstream oss;
  oss << "/tmp/eos.mgm/backup." << XrdSysThread::ID();
  std::string backup_fn = oss.str();
  eos::common::Path cPath(backup_fn.c_str());

  if (!cPath.MakeParentPath(S_IRWXU)) {
    eos_err("Unable to create temporary outputfile directory /tmp/eos.mgm/");
    stdErr = "unable to create temporary output directory /tmp/eos.mgm/";
    retc = EIO;
    return retc;
  }

  // Own the directory by daemon
  if (::chown(cPath.GetParentPath(), 2, 2)) {
    eos_err("Unable to own temporary outputfile directory %s",
            cPath.GetParentPath());
    stdErr = "unable to own temporary output directory /tmp/eos.mgm/";
    retc = EIO;
    return retc;
  }

  // Create tmp file holding information about the file entries. If twindow
  // specified then also use a filter object.
  std::string files_fn = backup_fn + "_files";
  std::fstream files_ofs(files_fn.c_str(),
                         std::fstream::out | std::fstream::trunc);

  if (!files_ofs.is_open()) {
    eos_err("Failed to create files backup file=%s", files_fn.c_str());
    stdErr = "failed to create backup file at MGM ";
    retc = EIO;
    return retc;
  }

  files_ofs.clear();
  files_ofs.close();
  files_ofs.open(files_fn.c_str(), std::fstream::in | std::fstream::out);

  if (!files_ofs.is_open()) {
    unlink(files_fn.c_str());
    eos_err("Failed to open files backup file=%s", files_fn.c_str());
    stdErr = "failed to open backup file at MGM ";
    retc = EIO;
    return retc;
  }

  // Create filter
  std::unique_ptr<IFilter> filter(new TwindowFilter(twindow_type, twindow_val));

  // Add files info
  if (ArchiveAddEntries(src_url.GetPath(), files_ofs, num_files,
                        true, filter.get()) || (num_files == 0)) {
    files_ofs.close();
    unlink(files_fn.c_str());
    return retc;
  }

  // Create tmp file holding information about the dir entries
  std::string dirs_fn = backup_fn + "_dirs";
  std::fstream dirs_ofs(dirs_fn.c_str() ,
                        std::fstream::out | std::fstream::trunc);

  if (!dirs_ofs.is_open()) {
    files_ofs.close();
    unlink(files_fn.c_str());
    eos_err("Failed to create local backup file=%s", dirs_fn.c_str());
    stdErr = "failed to create backup file at MGM ";
    retc = EIO;
    return retc;
  }

  dirs_ofs.clear();
  dirs_ofs.close();
  dirs_ofs.open(dirs_fn.c_str(), std::fstream::in | std::fstream::out);

  if (!dirs_ofs.is_open()) {
    files_ofs.close();
    unlink(files_fn.c_str());
    unlink(dirs_fn.c_str());
    eos_err("Failed to open dirs backup file=%s", dirs_fn.c_str());
    stdErr = "failed to open backup file at MGM ";
    retc = EIO;
    return retc;
  }

  // Add dirs info
  if (ArchiveAddEntries(src_url.GetPath(), dirs_ofs, num_dirs, false,
                        filter.get())) {
    files_ofs.close();
    unlink(files_fn.c_str());
    dirs_ofs.close();
    unlink(dirs_fn.c_str());
    return retc;
  }

  // Create the final backup file, write JSON header, append dir and file info
  std::fstream backup_ofs(backup_fn.c_str(), std::fstream::out);

  if (!backup_ofs.is_open()) {
    eos_err("Failed to open local backup file=%s", backup_fn.c_str());
    stdErr = "failed to open backup file at MGM ";
    retc = EIO;
    files_ofs.close();
    unlink(files_fn.c_str());
    dirs_ofs.close();
    unlink(dirs_fn.c_str());
    return retc;
  }

  // Note: we treat backups as archive get operations from tape to disk
  // therefore we need to swap the src and destination in the header
  num_dirs--; // don't count current dir
  backup_ofs.seekp(0);
  backup_ofs << "{"
             << "\"src\": \"" << dst_surl << "\", "
             << "\"dst\": \"" << src_surl << "\", "
             << "\"svc_class\": \"\", "
             << "\"dir_meta\": [\"uid\", \"gid\", \"mode\", \"attr\"], "
             << "\"file_meta\": [\"size\", \"mtime\", \"ctime\", \"uid\", \"gid\", "
             << "\"mode\", \"xstype\", \"xs\"], "
             << "\"excl_xattr\": [";

  // Add the list of excluded xattrs
  for (auto it = excl_xattr.begin(); it != excl_xattr.end(); /*empty*/) {
    backup_ofs << "\"" << *it << "\"";
    ++it;

    if (it != excl_xattr.end()) {
      backup_ofs << ", ";
    }
  }

  backup_ofs << "], "
             << "\"uid\": \"" << pVid->uid << "\", "
             << "\"gid\": \"" << pVid->gid << "\", "
             << "\"twindow_type\": \"" << twindow_type << "\", "
             << "\"twindow_val\": \"" << twindow_val << "\", "
             << "\"timestamp\": " << std::setw(10) << time(static_cast<time_t*>(0)) << ", "
             << "\"num_dirs\": " << std::setw(10) << num_dirs << ", "
             << "\"num_files\": " << std::setw(10) << num_files
             << "}" << std::endl;
  // Append the directory entries
  dirs_ofs.seekp(0);
  backup_ofs << dirs_ofs.rdbuf();
  // Append the file entries
  files_ofs.seekp(0);
  backup_ofs << files_ofs.rdbuf();
  // Close all files
  files_ofs.close();
  dirs_ofs.close();
  backup_ofs.close();
  // Copy local backup file to backup source
  XrdCl::PropertyList properties;
  XrdCl::PropertyList result;
  XrdCl::URL url_src, url_dst;
  XrdCl::URL tmp_url(src_surl);
  std::string dst_path = tmp_url.GetPath();
  dst_path += EOS_COMMON_PATH_BACKUP_FILE_PREFIX;
  dst_path += "backup.file";
  url_src.SetProtocol("file");
  url_src.SetPath(backup_fn.c_str());
  url_dst.SetProtocol(tmp_url.GetProtocol());
  url_dst.SetHostName(tmp_url.GetHostName());
  url_dst.SetPort(tmp_url.GetPort());
  url_dst.SetUserName("root");
  url_dst.SetPath(dst_path);
  url_dst.SetParams("eos.ruid=0&eos.rgid=0");
  properties.Set("source", url_src);
  properties.Set("target", url_dst);
  XrdCl::CopyProcess copy_proc;
  copy_proc.AddJob(properties, &result);
  XrdCl::XRootDStatus status_prep = copy_proc.Prepare();

  if (status_prep.IsOK()) {
    XrdCl::XRootDStatus status_run = copy_proc.Run(0);

    if (!status_run.IsOK()) {
      eos_err("Failed run for copy process, msg=%s", status_run.ToStr().c_str());
      stdErr = "error: failed run for copy process, msg=";
      stdErr += status_run.ToStr().c_str();
      retc = EIO;
    }
  } else {
    eos_err("Failed prepare for copy process, msg=%s", status_prep.ToStr().c_str());
    stdErr = "error: failed prepare for copy process, msg=";
    stdErr += status_prep.ToStr().c_str();
    retc = EIO;
  }

  // Remove local files
  unlink(files_fn.c_str());
  unlink(dirs_fn.c_str());
  unlink(backup_fn.c_str());
  return retc;
}

EOSMGMNAMESPACE_END
