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

#include "ConcurrentMount.hh"

#include <sys/file.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <poll.h>

ConcurrentMount::ConcurrentMount(const std::string& locknameprefix)
{
  lockpfx_ = locknameprefix;

  if (locknameprefix.empty()) {
    return;
  }

  // Prepare the sockaddr_un for a unix socket to be
  // for sending the fuse fd to another mount process.
  // The first mounter will unlink, bind, then listen.
  // A mount-requester (second or later mounter) will connect()
  std::string sockpath = locknameprefix + ".sock";
  saddr_ = (struct sockaddr_un*)&sstorage_;
  saddr_->sun_family = AF_LOCAL;
  strncpy(saddr_->sun_path, sockpath.c_str(), sizeof(saddr_->sun_path)-1);
  saddr_->sun_path[sizeof(saddr_->sun_path)-1] = '\0';

  const std::string fnA = locknameprefix + ".A.lock";
  const std::string fnB = locknameprefix + ".B.lock";
  lockAfd_ = open(fnA.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0600);

  if (lockAfd_ >= 0) {
    lockBfd_ = open(fnB.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0600);
  }

  if (lockAfd_ < 0 || lockBfd_ < 0) {
    fprintf(stderr, "# could not open lockfile %s errno=%d\n",
            (lockAfd_ < 0) ? fnA.c_str() : fnB.c_str(), errno);

    if (lockAfd_ >= 0) {
      close(lockAfd_);
    }

    lockAfd_ = -1;
  }
}

ConcurrentMount::~ConcurrentMount()
{
  Unlock();

  if (lockBfd_ >= 0) {
    close(lockBfd_);
  }

  if (lockAfd_ >= 0) {
    close(lockAfd_);
  }
}

int ConcurrentMount::StartMount(int &mntRes)
{
  mntRes = -1;
  const int rc = llock();

  if (rc == -2) {
    // unexpectedly could not acquire A (usually this should be held for
    // a short duraiton while the first eosxd is mounting). Assume we are
    // deadlocking an eosxd which is starting up; we return indicating a
    // eosxd is running; but we don't have the fuse fd available.
    return 1;
  }

  if (rc <= 0) {
    // other error or got A+B
    return rc;
  }

  // return code 1:
  // expected state of locks is A & !B
  if (!lockA_ || lockB_) return -1;

  // connect to the unix socket to get the peer's fuse fd
  mntRes = connectForFd();

  // unlock A
  if (lockA_) {
    if (flock(lockAfd_, LOCK_UN) == 0) {
      lockA_ = false;
    }
  }

  return 1;
}

void ConcurrentMount::MountDone(int fd)
{
  if (lockAfd_ < 0) {
    return;
  }

  if (lockA_) {
    if (flock(lockAfd_, LOCK_UN) == 0) {
      lockA_ = false;
    }
  }

  fuseFd_ = fd;
  serverThread_.emplace_back([this] { this->runFdServer(); });
}

void ConcurrentMount::Unmounting()
{
  if (lockAfd_ < 0 || lockBfd_ < 0) {
    return;
  }

  if (lockA_ || !lockB_) {
    return;
  }

  int ret;

  do {
    ret = flock(lockAfd_, LOCK_EX);
  } while (ret < 0 && errno == EINTR);

  if (ret < 0) {
    return;
  }

  lockA_ = true;

  if (flock(lockBfd_, LOCK_UN) == 0) {
    lockB_ = false;
  }

  shutdownFdServer();
}

void ConcurrentMount::Unlock()
{
  shutdownFdServer();

  if (lockBfd_ >= 0) {
    if (lockB_) {
      if (flock(lockBfd_, LOCK_UN) == 0) {
        lockB_ = false;
      }
    }
  }

  if (lockAfd_ >= 0) {
    if (lockA_) {
      if (flock(lockAfd_, LOCK_UN) == 0) {
        lockA_ = false;
      }
    }
  }
}

// -2 unexpected timeout trying to acquire A
// -1 other error
//  0  acquired A + B
//  1 could not acquire B, still holds A
int ConcurrentMount::llock()
{
  if (lockAfd_ < 0 || lockBfd_ < 0) {
    return -1;
  }

  if (lockA_ || lockB_) {
    return -1;
  }

  int ret;

  int retry = 120;
  do {
    ret = flock(lockAfd_, LOCK_EX | LOCK_NB);
    if (ret<0 && errno != EWOULDBLOCK) return -1;
    if (ret==0) break;

    if (retry) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
  } while (retry--);

  if (ret < 0) {
    // could not get A
    return -2;
  }

  lockA_ = true;

  if (flock(lockBfd_, LOCK_EX | LOCK_NB) < 0) {
    const int err = errno;

    if (err == EWOULDBLOCK) return 1;

    if (flock(lockAfd_, LOCK_UN) == 0) {
      lockA_ = false;
    }

    return -1;
  }

  lockB_ = true;

  return 0;
}

int ConcurrentMount::connectForFd() {
  if (lockAfd_ < 0 || lockBfd_ < 0) {
    return -1;
  }

  if (!lockA_ || lockB_) {
    return -1;
  }

  // prepare unix domain socket
  const int usock = socket(PF_LOCAL,SOCK_STREAM,0);
  if (usock<0) {
    return -1;
  }
  const int flags = fcntl(usock, F_GETFL);
  if (fcntl(usock, F_SETFL, flags | O_NONBLOCK)<0) {
    close(usock);
    return -1;
  }

  if (connect(usock, (struct sockaddr *)saddr_, SUN_LEN(saddr_))<0) {
    close(usock);
    return -1;
  }

  int fd=-1;
  if (do_recvmsg(usock, fd)<0) {
    close(usock);
    return -1;
  }

  close(usock);

  return fd;
}

int ConcurrentMount::runFdServer() {
  if (unlink(saddr_->sun_path)<0 && errno != ENOENT) {
    return -1;
  }

  // prepare unix domain socket
  const int usock = socket(PF_LOCAL,SOCK_STREAM,0);
  if (usock<0) {
    return -1;
  }
  const int flags = fcntl(usock, F_GETFL);
  if (fcntl(usock, F_SETFL, flags | O_NONBLOCK)<0) {
    close(usock);
    return -1;
  }

#ifndef __APPLE__
  if (fchmod(usock, 0700)<0) {
    close(usock);
    return -1;
  }
#endif

  if (bind(usock, (struct sockaddr *)saddr_, SUN_LEN(saddr_))<0) {
    close(usock);
    return -1;
  }

  if (listen(usock, 1)<0) {
    close(usock);
    return -1;
  }
  while(!serverExit_) {
    struct pollfd pfd[1];
    pfd[0].fd = usock;
    pfd[0].events = POLLIN;
    const int rc = poll(pfd, 1, 200);
    if (rc<0) {
      if (errno == EINTR || errno == EAGAIN) continue;
      close(usock);
      return -1;
    }
    if (rc==0) continue;
    if (pfd[0].revents & (POLLERR|POLLHUP|POLLNVAL)) {
      close(usock);
      return -1;
    }
    const int s2 = accept(usock, NULL, 0);
    if (s2<0) {
      continue;
    }
    (void)do_sendmsg(s2, saddr_->sun_path, fuseFd_);
    close(s2);
  }

  close(usock);
  unlink(saddr_->sun_path);

  return 0;
}

int ConcurrentMount::do_sendmsg(const int sock, const char *path, const int fd) {
  struct msghdr msg;
  struct cmsghdr *cmsghdr;
  struct iovec iov[1];
  ssize_t nbytes;
  union {
    struct cmsghdr hdr;
    char buf[CMSG_SPACE(sizeof(int))];
  } ctrl_msg;
  int *p;
  char metadata[32];

  snprintf(metadata,sizeof(metadata), "A%ld", (long)getpid());
  iov[0].iov_base = metadata;
  iov[0].iov_len = strlen(metadata)+1;
  memset(&ctrl_msg, 0, sizeof(ctrl_msg));
  cmsghdr = &ctrl_msg.hdr;
  cmsghdr->cmsg_len = CMSG_LEN(sizeof(int));
  cmsghdr->cmsg_level = SOL_SOCKET;
  cmsghdr->cmsg_type = SCM_RIGHTS;
  msg.msg_name = NULL;
  msg.msg_namelen = 0;
  msg.msg_iov = iov;
  msg.msg_iovlen = sizeof(iov) / sizeof(iov[0]);
  msg.msg_control = cmsghdr;
  msg.msg_controllen = cmsghdr->cmsg_len;
  msg.msg_flags = 0;
  p = (int *)CMSG_DATA(cmsghdr);
  *p = fd;

  nbytes = sendmsg(sock, &msg, 0);
  if (nbytes<0)
      return -1;

  return 0;
}

int ConcurrentMount::do_recvmsg(const int sock, int &fd) {
  struct msghdr msg;
  struct cmsghdr *cmsghdr;
  struct iovec iov[1];
  ssize_t nbytes;
  union {
    struct cmsghdr hdr;
    char buf[CMSG_SPACE(sizeof(int))];
  } ctrl_msg;
  int *p;
  char metadata[1024];

  iov[0].iov_base = metadata;
  iov[0].iov_len = sizeof(metadata);
  memset(&ctrl_msg, 0, sizeof(ctrl_msg));
  cmsghdr = &ctrl_msg.hdr;
  cmsghdr->cmsg_len = CMSG_LEN(sizeof(int));
  cmsghdr->cmsg_level = SOL_SOCKET;
  cmsghdr->cmsg_type = SCM_RIGHTS;
  msg.msg_name = NULL;
  msg.msg_namelen = 0;
  msg.msg_iov = iov;
  msg.msg_iovlen = sizeof(iov) / sizeof(iov[0]);
  msg.msg_control = cmsghdr;
  msg.msg_controllen = cmsghdr->cmsg_len;
  msg.msg_flags = 0;

  struct pollfd pfd[1];
  pfd[0].fd = sock;
  pfd[0].events = POLLIN;
  while(1) {
    const int rc = poll(pfd, 1, 5000);
    if (rc<0 && (errno == EINTR || errno == EAGAIN)) continue;
    if (rc<=0) return -1;
    break;
  }

  if (pfd[0].revents & (POLLERR|POLLNVAL)) return -1;

  nbytes = recvmsg(sock, &msg, 0);
  if (nbytes < 1 || metadata[0] != 'A')
    return -1;

  p = (int *)CMSG_DATA(cmsghdr);
  fd = *p;
  return 0;
}

void ConcurrentMount::shutdownFdServer() {
  serverExit_ = true;
  for(auto &thr: serverThread_) {
    thr.join();
  }
  serverThread_.clear();
  serverExit_ = false;
}
