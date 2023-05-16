//------------------------------------------------------------------------------
// File: ErrorLogListener.cc
// Author: Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2023 CERN/Switzerland                                  *
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

//------------------------------------------------------------------------------
// This file is included source code in XrdMgmOfs.cc to make the code more
// transparent without slowing down the compilation time.
//------------------------------------------------------------------------------

namespace
{

//------------------------------------------------------------------------------
// Make sure the log file exists and can be accessed
//------------------------------------------------------------------------------
bool CheckFileExistanceAndPerm(const std::string& log_file, std::string& err)
{
  // Check that the log file exists and we're allowed to write to it
  struct stat info;

  if (::stat(log_file.c_str(), &info)) {
    if (errno == ENOENT) {
      // Try to create the log file
      int fd = open(log_file.c_str(), O_CREAT | S_IRUSR | S_IWUSR);

      if (fd == -1) {
        err = "cannot create log file";
        return false;
      }

      close(fd);
    } else {
      err = "cannot access log file";
      return false;
    }
  } else {
    // Check write permissions
    uid_t euid = geteuid();

    if (info.st_uid != euid) {
      err = "wrong owner of the log file";
      return false;
    }

    if ((info.st_mode & (S_IRUSR | S_IWUSR)) == 0) {
      err = "wrong permissions for log file";
      return false;
    }
  }

  return true;
}
}

//------------------------------------------------------------------------------
// Start thread listening for error messages and log them
//------------------------------------------------------------------------------
void
XrdMgmOfs::ErrorLogListenerThread(ThreadAssistant& assistant) noexcept
{
  static std::string channel = "/eos/*/errorreport";
  static std::string log_path = "/var/log/eos/mgm/error.log";
  std::string err_msg;

  if (!CheckFileExistanceAndPerm(log_path, err_msg)) {
    eos_static_err("msg=\"failed to stat QDB error log listener\" "
                   "err_msg=\"%s\"", err_msg.c_str());
    return;
  }

  std::string out;
  XrdSysLogger logger;
  logger.Bind(log_path.c_str(), 0);
  eos::mq::QdbErrorReportListener err_listener(mQdbContactDetails, channel);

  while (!assistant.terminationRequested()) {
    if (err_listener.fetch(out, &assistant)) {
      fprintf(stderr, "%s\n", out.c_str());
    }
  }
}
