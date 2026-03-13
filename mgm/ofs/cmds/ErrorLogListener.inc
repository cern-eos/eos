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
      int fd = open(log_file.c_str(), O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);

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
  ThreadAssistant::setSelfThreadName("ErrorLogListener");
  static std::string channel = "/eos/*/errorreport";
  static std::string log_path = "/var/log/eos/mgm/error.log";
  std::string err_msg;

  if (!CheckFileExistanceAndPerm(log_path, err_msg)) {
    eos_static_err("msg=\"failed to stat QDB error log listener\" "
                   "err_msg=\"%s\"", err_msg.c_str());
    return;
  }

  // Open log file or create if necessary
  FILE* file = fopen(log_path.c_str(), "a+");

  if (file == nullptr) {
    eos_static_err("msg=\"failed to open error log file\" path=\"%s\" errno=%d",
                   log_path.c_str(), errno);
    return;
  }

  std::string out;
  XrdSysLogger logger(fileno(file), 1);
  int retc = logger.Bind(log_path.c_str(), 1);
  // Disable XRootD log rotation
  logger.setRotate(0);
  eos::mq::QdbListener err_listener(mQdbContactDetails, channel);
  eos_static_info("msg=\"starting error report listener\" bind_retc=%d", retc);

  while (!assistant.terminationRequested()) {
    if (err_listener.fetch(out, &assistant)) {
      fprintf(file, "%s\n", out.c_str());
    }
  }

  (void) fflush(file);
  (void) fclose(file);
}
