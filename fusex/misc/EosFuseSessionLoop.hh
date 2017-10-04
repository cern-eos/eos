/*
 * XrdFuseThreadPool.hh
 *
 *  Created on: Oct 31, 2016
 *      Author: simonm
 */

#ifndef EOSFUSESESSIONLOOP_HH_
#define EOSFUSESESSIONLOOP_HH_

#include "ThreadPool.hh"

#include <memory>

extern "C"
{
#include <fuse/fuse_lowlevel.h>
}

struct fuse_in_header {
  uint32_t   len;
  uint32_t   opcode;
  uint32_t   unique;
  uint32_t   nodeid;
  uint32_t   uid;
  uint32_t   gid;
  uint32_t   pid;
  uint32_t   padding;
};



class EosFuseSessionLoop
{
  struct FuseTask {
    FuseTask(fuse_session* se, size_t bufsize, fuse_chan* chan) : se(se), chan(chan)
    {
      memset(&buf, 0, sizeof(fuse_buf));
      buf.mem  = new char[bufsize];
      buf.size = bufsize;
    }

    ~FuseTask()
    {
      delete[](char*)buf.mem;
    }

    void Run()
    {
      fuse_session_process_buf(se, &buf, chan);
    }

    fuse_session* se;
    fuse_buf      buf;
    fuse_chan*    chan;
  };

public:

  EosFuseSessionLoop(int metaMin, int metaMax, int ioMin, int ioMax) :
    metaPool(metaMin, metaMax), ioPool(ioMin, ioMax) { }


  virtual ~EosFuseSessionLoop()
  {
    metaPool.Stop();
    ioPool.Stop();
  }

  int Loop(fuse_session* se)
  {
    int res = 0;
    struct fuse_chan* ch = fuse_session_next_chan(se, NULL);
    size_t bufsize = fuse_chan_bufsize(ch);

    while (!fuse_session_exited(se)) {
      std::unique_ptr<FuseTask> task(new FuseTask(se, bufsize, ch));
      res = fuse_session_receive_buf(se, &task->buf, &task->chan);

      if (res == -EINTR) {
        continue;
      }

      if (res <= 0) {
        break;
      }

      if (IsIO(task->buf)) {
        ioPool.Execute(task.release());
      } else {
        metaPool.Execute(task.release());
      }
    }

    fuse_session_reset(se);
    return res < 0 ? -1 : 0;
  }

private:

  enum fuse_opcode {
    FUSE_READ  = 15,
    FUSE_WRITE = 16,
  };

  bool IsIO(fuse_buf& fbuf)
  {
    if (!(fbuf.flags & FUSE_BUF_IS_FD)) {
      fuse_in_header* in = reinterpret_cast<fuse_in_header*>(fbuf.mem);

      if (in->opcode == FUSE_READ || in->opcode == FUSE_WRITE) {
        return true;
      }

      return false;
    }

    return true;
  }

  ThreadPool<FuseTask> metaPool;
  ThreadPool<FuseTask> ioPool;
};

#endif /* EOSFUSESESSIONLOOP_HH_ */
