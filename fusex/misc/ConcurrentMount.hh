//------------------------------------------------------------------------------
//! @file ConcurrentMount.hh
//! @author David Smith CERN
//! @brief Class to detect running eosxd and facilitate reattach of mount point
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2024 CERN/Switzerland                                  *
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

#include <string>
#include <vector>
#include <atomic>
#include <thread>

#include <sys/socket.h>
#include <sys/un.h>

/**
 * ConcurrentMount is a utility class used to avoid starting concurrent eosxd
 * for the same mount, but allowing reattaching an eosxd to its mount point,
 * e.g. after a lazy unmount.
 *
 * To detect concurrent mounts we use two exclusive flock advisory locks on
 * two dedicated lock files termed A & B. To allow reattaching of the mount
 * we use a unix domain socket to allow passing of the /dev/fuse filedescriptor
 * to an eosxd to allow it reattach the fs by calling mount() and exiting.
 *
 * The locking strategy is:
 * A+B are locked during mount/unmount transition. [take A, take B]
 * B   only is locked while the filesystem is mounted. [release A]
 *     The pid of an Unmounting process is recorded in lockfile B.
 * A   only is locked while the filesystem is being unmounted. [take A, release B]
 *
 * The usage of this class is:
 *
 *  StartMount(): tests if caller is the only instance running,
 *                if not we attempt to fetch and return the
 *                existing fuse file descriptor. If we indicate
 *                caller is not the only instance running the caller
 *                should not proceed with the calls below.
 *
 * MountDone():   we're primary eosxd and have mounted the fs.
 *                Caller supplies the fuse file descriptor. (The caller
 *                will then loop for the duration of the fuse session).
 *                This method starts a thread, the caller should not fork
 *                and call this object from a child process.
 *
 * Unmounting():  caller has finished their fuse session loop.
 *
 * Unlock():      caller has unmounted the fuse fs.
 *
 */
class ConcurrentMount
{
public:
  /**
   * Constructor, opens lock files.
   */
  ConcurrentMount(const std::string& locknameprefix);

  /**
   * Destrcutor, closes lock file descriptors.
   */
  ~ConcurrentMount();

  /**
   * Lock in preparation of mounting:
   * Returns -1 on error
   *          0 for successful lock (A + B locked)
   *          1 locks were consistent with existing ongoing mount.
   *            mntRes holds result of mount request sent via ipc
   *
   * If the lock is held by an existing mount some retries up to 5
   * seconds are made to avoid race on unmount.
   */
  int StartMount(int &mntRes);

  /**
   * Called after mounting and before entering the fuse session loop.
   */
  void MountDone(int fd);

  /**
   * Called after leaving the fuse session loop but before unmounting.
   */
  void Unmounting();

  /**
   * May be called once mount & unmount activity is done. (However the
   * destructor may be called without calling this method).
   */
  void Unlock();

  std::string lockpfx() const {
    return lockpfx_;
  }

private:
  // llock:
  // -2 unexpected timeout trying to acquire A.
  // -1 other error
  //  0  acquired A + B
  //  1 could not acquire B, still holds A
  int llock();

  int connectForFd();

  int runFdServer();

  int do_sendmsg(const int sock, const char *path, const int fd);

  int do_recvmsg(const int sock, int &fd);

  void shutdownFdServer();

  int  lockAfd_{ -1};
  int  lockBfd_{ -1};
  bool lockA_  {false};
  bool lockB_  {false};
  int  fuseFd_ {-1};
  struct sockaddr_storage sstorage_;
  struct sockaddr_un *saddr_;
  std::atomic<bool> serverExit_{false};
  std::vector<std::thread> serverThread_;
  std::string lockpfx_;
};
