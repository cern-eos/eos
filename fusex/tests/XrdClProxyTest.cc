/*
 * XrdClProxyTest.cc
 *
 *  Created on: June 02, 2017
 *      Author: Andreas-Joachim Peters
 */



#include "fusex/data/xrdclproxy.hh"
#include "XrdSys/XrdSysTimer.hh"
#include "common/ShellCmd.hh"
#include <stdint.h>
#include <algorithm>
#include <vector>
#include "gtest/gtest.h"

TEST(XrdClProxy, Write) {
  eos::common::ShellCmd xrd("xrootd -p 21234 -n proxytest");
  XrdSysTimer sleeper;
  sleeper.Snooze(1);

  std::vector<char> buffer;
  buffer.resize(4096);
  for (size_t i=0; i < buffer.size(); i++)
  {
    buffer[i] = i % 256;
  }

  XrdCl::Proxy file;
  XrdCl::OpenFlags::Flags targetFlags = XrdCl::OpenFlags::Update | XrdCl::OpenFlags::Delete;
  XrdCl::Access::Mode mode = XrdCl::Access::UR | XrdCl::Access::UW | XrdCl::Access::UX;

  fprintf(stderr, "[01] open)\n");
  XrdCl::XRootDStatus status = file.Open("root://localhost:21234//tmp/xrdclproxytest", targetFlags, mode, 300);
  ASSERT_TRUE(status.IsOK());
  fprintf(stderr, "[02] waitopen)\n");
  status = file.WaitOpen();
  ASSERT_TRUE(status.IsOK());

  fprintf(stderr, "[03] write-sync \n");
  for (size_t i=0; i < 64; ++i)
  {
    fprintf(stderr, ".");
    status = file.Write(i, 1, &buffer[i], (uint16_t) 300);
    ASSERT_TRUE(status.IsOK());
  }

  status = file.Truncate(0);
  ASSERT_TRUE(status.IsOK());

  fprintf(stderr, "\n[04] write-async \n");
  for (size_t i=0; i < buffer.size(); ++i)
  {
    if (!(i % 1000))fprintf(stderr, ".");
    XrdCl::Proxy::write_handler handler = file.WriteAsyncPrepare(1);
    status = file.WriteAsync(i, 1, &buffer[i], handler, (uint16_t) 300);
    ASSERT_TRUE(status.IsOK());
  }

  status = file.WaitWrite();
  ASSERT_TRUE(status.IsOK());
  file.Collect();
  status = file.Close( (uint16_t) 0);
  ASSERT_TRUE(status.IsOK());
}

TEST(XrdClProxy, ReadSync) {
  eos::common::ShellCmd xrd("xrootd -p 21234 -n proxytest");
  XrdSysTimer sleeper;
  sleeper.Snooze(1);

  std::vector<int> buffer;
  buffer.resize(64 * 1024 * 1024);
  for (size_t i=0; i < buffer.size(); i++)
  {
    buffer[i] = i;
  }
  XrdCl::Proxy file;
  XrdCl::OpenFlags::Flags targetFlags = XrdCl::OpenFlags::Update | XrdCl::OpenFlags::Delete;
  XrdCl::Access::Mode mode = XrdCl::Access::UR | XrdCl::Access::UW | XrdCl::Access::UX;

  fprintf(stderr, "[01] open)\n");
  XrdCl::XRootDStatus status = file.Open("root://localhost:21234//tmp/xrdclproxytest", targetFlags, mode, 300);
  ASSERT_TRUE( status.IsOK() );
  fprintf(stderr, "[02] waitopen)\n");
  status = file.WaitOpen();
  ASSERT_TRUE( status.IsOK() );

  status = file.Truncate(0);
  ASSERT_TRUE( status.IsOK() );

  fprintf(stderr, "\n[03] write-async \n");
  for (size_t i=0; i < 64 ; ++i)
  {
    fprintf(stderr, ".");
    XrdCl::Proxy::write_handler handler = file.WriteAsyncPrepare(1);
    status = file.WriteAsync(4 * i * 1024 * 1024, 4 * 1024 * 1024, &buffer[i * 1024 * 1024], handler, (uint16_t) 300);
    ASSERT_TRUE( status.IsOK() );
  }

  status = file.WaitWrite();
  ASSERT_TRUE( status.IsOK() );

  fprintf(stderr, "\n[04] zero \n");
  for (size_t i=0; i < 64 * 1024 * 1024; i++)
  {
    buffer[i] = 0;
  }

  fprintf(stderr, "\n[05] read \n");
  ssize_t total_bytes = 0;
  for (size_t i=0; i < 330 ; ++i)
  {
    uint32_t bytesRead=0;
    fprintf(stderr, ".");
    status = file.Read(4 * i * 200 * 1024, 4 * 200 * 1024, &buffer[i * 200 * 1024], bytesRead, (uint16_t) 300);
    total_bytes += bytesRead;
    //fprintf(stderr, "----: bytesRead=%u\n", bytesRead);
    ASSERT_TRUE( status.IsOK() );
  }

  ASSERT_EQ( total_bytes, (4 * 1024 * 1024 * 64));

  fprintf(stderr, "\n[06] comparing \n");
  for (ssize_t i=0; i < 64 * 1024 * 1024; i++)
  {
    if (buffer[i] != (int) i)
    {
      ASSERT_EQ(buffer[i], i );
    }
  }

  fprintf(stderr, "\n[07] ra-efficiency=%f\n", file.get_readahead_efficiency());
  file.Collect();
  status = file.Close( (uint16_t) 0);
  ASSERT_TRUE( status.IsOK() );
}

TEST(XrdClProxy, ReadAsync) {
  eos::common::ShellCmd xrd("xrootd -p 21234 -n proxytest");
  XrdSysTimer sleeper;
  sleeper.Snooze(1);

  std::vector<int> buffer;
  buffer.resize(64 * 1024 * 1024);
  for (size_t i=0; i < buffer.size(); i++)
  {
    buffer[i] = i;
  }
  XrdCl::Proxy file;
  XrdCl::OpenFlags::Flags targetFlags = XrdCl::OpenFlags::Update | XrdCl::OpenFlags::Delete;
  XrdCl::Access::Mode mode = XrdCl::Access::UR | XrdCl::Access::UW | XrdCl::Access::UX;

  fprintf(stderr, "[01] open)\n");
  XrdCl::XRootDStatus status = file.Open("root://localhost:21234//tmp/xrdclproxytest", targetFlags, mode, 300);
  ASSERT_TRUE( status.IsOK() );
  fprintf(stderr, "[02] waitopen)\n");
  status = file.WaitOpen();
  ASSERT_TRUE( status.IsOK() );

  status = file.Truncate(0);
  ASSERT_TRUE( status.IsOK() );

  fprintf(stderr, "\n[03] write-async \n");
  for (size_t i=0; i < 64 ; ++i)
  {
    fprintf(stderr, ".");
    XrdCl::Proxy::write_handler handler = file.WriteAsyncPrepare(1);
    status = file.WriteAsync(4 * i * 1024 * 1024, 4 * 1024 * 1024, &buffer[i * 1024 * 1024], handler, (uint16_t) 300);
    ASSERT_TRUE( status.IsOK() );
  }

  status = file.WaitWrite();
  ASSERT_TRUE( status.IsOK() );

  fprintf(stderr, "\n[04] zero \n");
  for (size_t i=0; i < 64 * 1024 * 1024; i++)
  {
    buffer[i] = 0;
  }

  fprintf(stderr, "\n[05] read \n");
  ssize_t total_bytes = 0;
  for (size_t i=0; i < 330 ; ++i)
  {
    uint32_t bytesRead=0;
    fprintf(stderr, ".");
    XrdCl::Proxy::read_handler handler = file.ReadAsyncPrepare(4 * i * 200 * 1024, 4 * 200 * 1024);
    status = file.PreReadAsync(4 * i * 200 * 1024, 4 * 200 * 1024, handler, (uint16_t) 300);
    ASSERT_TRUE( status.IsOK() );
    status = file.WaitRead(handler);
    ASSERT_TRUE( status.IsOK() );
    //fprintf(stderr, "offset=%lu read=%u buffer=%x size=%lu\n", i * 200 * 1024, handler->vbuffer().size(), handler->buffer(), handler->vbuffer().size());
    status = file.ReadAsync(handler, 4 * 200 * 1024, &buffer[i * 200 * 1024], bytesRead);
    ASSERT_TRUE( status.IsOK() );
    total_bytes += bytesRead;

    ASSERT_TRUE( status.IsOK() );
  }

  ASSERT_EQ(total_bytes, (4 * 1024 * 1024 * 64));

  fprintf(stderr, "\n[06] comparing \n");
  for (ssize_t i=0; i < 64 * 1024 * 1024; i++)
  {
    if (buffer[i] != (int) i)
    {
      ASSERT_EQ(buffer[i], (int) i);
    }
  }

  fprintf(stderr, "\n[07] ra-efficiency=%f\n", file.get_readahead_efficiency());
  file.Collect();
  status = file.Close( (uint16_t) 0);
  ASSERT_TRUE( status.IsOK() );
}

TEST(XrdClProxy, ReadAheadStatic) {
  eos::common::ShellCmd xrd("xrootd -p 21234 -n proxytest");
  XrdSysTimer sleeper;
  sleeper.Snooze(1);

  std::vector<int> buffer;
  buffer.resize(64 * 1024 * 1024);
  for (size_t i=0; i < buffer.size(); i++)
  {
    buffer[i] = i;
  }
  XrdCl::Proxy file;
  XrdCl::OpenFlags::Flags targetFlags = XrdCl::OpenFlags::Update | XrdCl::OpenFlags::Delete;
  XrdCl::Access::Mode mode = XrdCl::Access::UR | XrdCl::Access::UW | XrdCl::Access::UX;

  fprintf(stderr, "[01] open)\n");
  XrdCl::XRootDStatus status = file.Open("root://localhost:21234//tmp/xrdclproxytest", targetFlags, mode, 300);
  ASSERT_TRUE( status.IsOK() );
  fprintf(stderr, "[02] waitopen)\n");
  status = file.WaitOpen();
  ASSERT_TRUE( status.IsOK() );

  status = file.Truncate(0);
  ASSERT_TRUE( status.IsOK() );

  fprintf(stderr, "\n[03] write-async \n");
  for (size_t i=0; i < 64 ; ++i)
  {
    fprintf(stderr, ".");
    XrdCl::Proxy::write_handler handler = file.WriteAsyncPrepare(1);
    status = file.WriteAsync(4 * i * 1024 * 1024, 4 * 1024 * 1024, &buffer[i * 1024 * 1024], handler, (uint16_t) 300);
    ASSERT_TRUE( status.IsOK() );
  }

  status = file.WaitWrite();
  ASSERT_TRUE( status.IsOK() );

  fprintf(stderr, "\n[04] zero \n");
  for (size_t i=0; i < 64 * 1024 * 1024; i++)
  {
    buffer[i] = 0;
  }

  file.set_readahead_strategy(XrdCl::Proxy::STATIC, 4096, 2 * 819200, 4 * 1024 * 1024);
  fprintf(stderr, "\n[05] read-ahead static 4k 1.6k 4M \n");
  ssize_t total_bytes = 0;
  for (size_t i=0; i < 330 ; ++i)
  {
    XrdSysTimer sleeper;
    //sleeper.Wait(1000);
    uint32_t bytesRead=0;
    fprintf(stderr, ".");
    status = file.Read(4 * i * 200 * 1024, 4 * 200 * 1024, &buffer[i * 200 * 1024], bytesRead, (uint16_t) 300);
    total_bytes += bytesRead;
    //fprintf(stderr, "----: bytesRead=%u\n", bytesRead);
    ASSERT_TRUE( status.IsOK() );
  }

  ASSERT_EQ(total_bytes, (4 * 1024 * 1024 * 64));

  fprintf(stderr, "\n[06] comparing \n");
  for (ssize_t i=0; i < 64 * 1024 * 1024; i++)
  {
    if (buffer[i] != (int) i)
    {
      ASSERT_EQ(buffer[i], (int) i );
    }
  }

  fprintf(stderr, "\n[07] ra-efficiency=%f\n", file.get_readahead_efficiency());
  ASSERT_EQ( (int) (1000000 * file.get_readahead_efficiency()), 99694824 );
  file.Collect();
  status = file.Close( (uint16_t) 0);
  ASSERT_TRUE( status.IsOK() );
}

TEST(XrdClProxy, ReadAheadStaticLarge) {
  eos::common::ShellCmd xrd("xrootd -p 21234 -n proxytest");
  XrdSysTimer sleeper;
  sleeper.Snooze(1);

  std::vector<int> buffer;
  buffer.resize(64 * 1024 * 1024);
  for (size_t i=0; i < buffer.size(); i++)
  {
    buffer[i] = i;
  }
  XrdCl::Proxy file;
  XrdCl::OpenFlags::Flags targetFlags = XrdCl::OpenFlags::Update | XrdCl::OpenFlags::Delete;
  XrdCl::Access::Mode mode = XrdCl::Access::UR | XrdCl::Access::UW | XrdCl::Access::UX;

  fprintf(stderr, "[01] open)\n");
  XrdCl::XRootDStatus status = file.Open("root://localhost:21234//tmp/xrdclproxytest", targetFlags, mode, 300);
  ASSERT_TRUE( status.IsOK() );
  fprintf(stderr, "[02] waitopen)\n");
  status = file.WaitOpen();
  ASSERT_TRUE( status.IsOK() );

  status = file.Truncate(0);
  ASSERT_TRUE( status.IsOK() );

  fprintf(stderr, "\n[03] write-async \n");
  for (size_t i=0; i < 64 ; ++i)
  {
    fprintf(stderr, ".");
    XrdCl::Proxy::write_handler handler = file.WriteAsyncPrepare(1);
    status = file.WriteAsync(4 * i * 1024 * 1024, 4 * 1024 * 1024, &buffer[i * 1024 * 1024], handler, (uint16_t) 300);
    ASSERT_TRUE( status.IsOK() );
  }

  status = file.WaitWrite();
  ASSERT_TRUE( status.IsOK() );

  fprintf(stderr, "\n[04] zero \n");
  for (size_t i=0; i < 64 * 1024 * 1024; i++)
  {
    buffer[i] = 0;
  }

  file.set_readahead_strategy(XrdCl::Proxy::STATIC, 4096, 6 * 1024 * 1024, 16 * 1024 * 1024);
  fprintf(stderr, "\n[05] read-ahead static 4k 8M 16M \n");
  ssize_t total_bytes = 0;
  for (size_t i=0; i < 330 ; ++i)
  {
    XrdSysTimer sleeper;
    //sleeper.Wait(1000);
    uint32_t bytesRead=0;
    fprintf(stderr, ".");
    status = file.Read(4 * i * 200 * 1024, 4 * 200 * 1024, &buffer[i * 200 * 1024], bytesRead, (uint16_t) 300);
    total_bytes += bytesRead;
    //fprintf(stderr, "----: bytesRead=%u\n", bytesRead);
    ASSERT_TRUE( status.IsOK() );
  }

  ASSERT_EQ( total_bytes, (4 * 1024 * 1024 * 64));

  fprintf(stderr, "\n[06] comparing \n");
  for (ssize_t i=0; i < 64 * 1024 * 1024; i++)
  {
    if (buffer[i] != (int) i)
    {
      ASSERT_EQ( buffer[i], (int) i );
    }
  }

  fprintf(stderr, "\n[07] ra-efficiency=%f %d\n", file.get_readahead_efficiency(), (int) (1000000 * file.get_readahead_efficiency()));

  ASSERT_EQ( (int) (1000000 * file.get_readahead_efficiency()), 99694824 );
  status = file.Close( (uint16_t) 0);
  ASSERT_TRUE( status.IsOK() );
}

TEST(XrdClProxy, ReadAheadSparse) {
  eos::common::ShellCmd xrd("xrootd -p 21234 -n proxytest");
  XrdSysTimer sleeper;
  sleeper.Snooze(1);

  std::vector<int> buffer;
  buffer.resize(64 * 1024 * 1024);
  for (size_t i=0; i < buffer.size(); i++)
  {
    buffer[i] = i;
  }
  XrdCl::Proxy file;
  XrdCl::OpenFlags::Flags targetFlags = XrdCl::OpenFlags::Update | XrdCl::OpenFlags::Delete;
  XrdCl::Access::Mode mode = XrdCl::Access::UR | XrdCl::Access::UW | XrdCl::Access::UX;

  fprintf(stderr, "[01] open)\n");
  XrdCl::XRootDStatus status = file.Open("root://localhost:21234//tmp/xrdclproxytest", targetFlags, mode, 300);
  ASSERT_TRUE( status.IsOK() );
  fprintf(stderr, "[02] waitopen)\n");
  status = file.WaitOpen();
  ASSERT_TRUE( status.IsOK() );

  status = file.Truncate(0);
  ASSERT_TRUE( status.IsOK() );

  fprintf(stderr, "\n[03] write-async \n");
  for (size_t i=0; i < 64 ; ++i)
  {
    fprintf(stderr, ".");
    XrdCl::Proxy::write_handler handler = file.WriteAsyncPrepare(1);
    status = file.WriteAsync(4 * i * 1024 * 1024, 4 * 1024 * 1024, &buffer[i * 1024 * 1024], handler, (uint16_t) 300);
    ASSERT_TRUE( status.IsOK() );
  }

  status = file.WaitWrite();
  ASSERT_TRUE( status.IsOK() );

  fprintf(stderr, "\n[04] zero \n");
  for (size_t i=0; i < 64 * 1024 * 1024; i++)
  {
    buffer[i] = 0;
  }

  file.set_readahead_strategy(XrdCl::Proxy::STATIC, 4096, 2 * 1024 * 1024, 4 * 1024 * 1024);
  fprintf(stderr, "\n[05] read-ahead static 4k 2M 4M \n");
  ssize_t total_bytes = 0;
  for (size_t i=0; i < 330 ; i+=2)
  {
    XrdSysTimer sleeper;
    //sleeper.Wait(1000);
    uint32_t bytesRead=0;
    fprintf(stderr, ".");
    status = file.Read(4 * i * 200 * 1024, 4 * 200 * 1024, &buffer[i * 200 * 1024], bytesRead, (uint16_t) 300);
    total_bytes += bytesRead;
    //fprintf(stderr, "----: bytesRead=%u\n", bytesRead);
    ASSERT_TRUE( status.IsOK() );
  }

  fprintf(stderr, "total_bytes = %lu\n", total_bytes);
  ASSERT_EQ( total_bytes, 134348800 );

  fprintf(stderr, "\n[06] comparing \n");
  for (size_t k=0; k < 330 ; k+=2)
  for (size_t l=0; l < 200 * 1024; l++)
  {
    size_t i = (k * 200 * 1024) + l;
    if (i < 67108864)
    {
      if (buffer[i] != (int) i)
      {
        ASSERT_EQ( buffer[i], (int) i );
      }
    }
  }

  fprintf(stderr, "\n[07] ra-efficiency=%f %d\n", file.get_readahead_efficiency(), (int) (1000000 * file.get_readahead_efficiency()));

  fprintf(stderr, "\n[07] ra-efficiency=%f\n", file.get_readahead_efficiency());

  ASSERT_EQ ( (int) (1000000 * file.get_readahead_efficiency()), 96073176 );

  file.Collect();
  status = file.Close( (uint16_t) 0);
  ASSERT_TRUE( status.IsOK() );
}

TEST(XrdClProxy, ReadAheadDisable) {
  eos::common::ShellCmd xrd("xrootd -p 21234 -n proxytest");
  XrdSysTimer sleeper;
  sleeper.Snooze(1);

  std::vector<int> buffer;
  buffer.resize(64 * 1024 * 1024);
  for (size_t i=0; i < buffer.size(); i++)
  {
    buffer[i] = i;
  }
  XrdCl::Proxy file;
  XrdCl::OpenFlags::Flags targetFlags = XrdCl::OpenFlags::Update | XrdCl::OpenFlags::Delete;
  XrdCl::Access::Mode mode = XrdCl::Access::UR | XrdCl::Access::UW | XrdCl::Access::UX;

  fprintf(stderr, "[01] open)\n");
  XrdCl::XRootDStatus status = file.Open("root://localhost:21234//tmp/xrdclproxytest", targetFlags, mode, 300);
  ASSERT_TRUE( status.IsOK() );
  fprintf(stderr, "[02] waitopen)\n");
  status = file.WaitOpen();
  ASSERT_TRUE( status.IsOK() );

  status = file.Truncate(0);
  ASSERT_TRUE( status.IsOK() );

  fprintf(stderr, "\n[03] write-async \n");
  for (size_t i=0; i < 64 ; ++i)
  {
    fprintf(stderr, ".");
    XrdCl::Proxy::write_handler handler = file.WriteAsyncPrepare(1);
    status = file.WriteAsync(4 * i * 1024 * 1024, 4 * 1024 * 1024, &buffer[i * 1024 * 1024], handler, (uint16_t) 300);
    ASSERT_TRUE( status.IsOK() );
  }

  status = file.WaitWrite();
  ASSERT_TRUE( status.IsOK() );

  fprintf(stderr, "\n[04] zero \n");
  for (size_t i=0; i < 64 * 1024 * 1024; i++)
  {
    buffer[i] = 0;
  }

  file.set_readahead_strategy(XrdCl::Proxy::STATIC, 4096, 2 * 1024 * 1024, 4 * 1024 * 1024);
  fprintf(stderr, "\n[05] read-ahead static 4k 2M 4M \n");
  ssize_t total_bytes = 0;
  for (size_t i=0; i < 330 ; i+=(i + 1))
  {
    XrdSysTimer sleeper;
    //sleeper.Wait(1000);
    uint32_t bytesRead=0;
    fprintf(stderr, ".");
    status = file.Read(4 * i * 200 * 1024, 4 * 200 * 1024, &buffer[i * 200 * 1024], bytesRead, (uint16_t) 300);
    total_bytes += bytesRead;
    //fprintf(stderr, "----: bytesRead=%u\n", bytesRead);
    ASSERT_TRUE( status.IsOK() );
  }

  fprintf(stderr, "\n[06] comparing \n");
  for (size_t k=0; k < 330 ; k+=(k + 1))
  for (size_t l=0; l < 200 * 1024; l++)
  {
    size_t i = (k * 200 * 1024) + l;
    {
      if (buffer[i] != (int) i)
      {
        ASSERT_EQ( buffer[i], (int) i );
      }
    }
  }

  fprintf(stderr, "\n[07] ra-efficiency=%f %d\n", file.get_readahead_efficiency(), (int) (1000000 * file.get_readahead_efficiency()));

  fprintf(stderr, "\n[07] ra-efficiency=%f\n", file.get_readahead_efficiency());

  ASSERT_EQ ( (int) (1000000 * file.get_readahead_efficiency()), 29777778 );


  file.Collect();
  status = file.Close( (uint16_t) 100);

  ASSERT_TRUE( status.IsOK() );
}

TEST(XrdClProxy, ReadAheadBackward) {
  eos::common::ShellCmd xrd("xrootd -p 21234 -n proxytest");
  XrdSysTimer sleeper;
  sleeper.Snooze(1);

  std::vector<int> buffer;
  buffer.resize(64 * 1024 * 1024);
  for (size_t i=0; i < buffer.size(); i++)
  {
    buffer[i] = i;
  }
  XrdCl::Proxy file;
  XrdCl::OpenFlags::Flags targetFlags = XrdCl::OpenFlags::Update | XrdCl::OpenFlags::Delete;
  XrdCl::Access::Mode mode = XrdCl::Access::UR | XrdCl::Access::UW | XrdCl::Access::UX;

  fprintf(stderr, "[01] open)\n");
  XrdCl::XRootDStatus status = file.Open("root://localhost:21234//tmp/xrdclproxytest", targetFlags, mode, 300);
  ASSERT_TRUE( status.IsOK() );
  fprintf(stderr, "[02] waitopen)\n");
  status = file.WaitOpen();
  ASSERT_TRUE( status.IsOK() );

  status = file.Truncate(0);
  ASSERT_TRUE( status.IsOK() );

  fprintf(stderr, "\n[03] write-async \n");
  for (size_t i=0; i < 64 ; ++i)
  {
    fprintf(stderr, ".");
    XrdCl::Proxy::write_handler handler = file.WriteAsyncPrepare(1);
    status = file.WriteAsync(4 * i * 1024 * 1024, 4 * 1024 * 1024, &buffer[i * 1024 * 1024], handler, (uint16_t) 300);
    ASSERT_TRUE( status.IsOK() );
  }

  status = file.WaitWrite();
  ASSERT_TRUE( status.IsOK() );

  fprintf(stderr, "\n[04] zero \n");
  for (size_t i=0; i < 64 * 1024 * 1024; i++)
  {
    buffer[i] = 0;
  }

  file.set_readahead_strategy(XrdCl::Proxy::STATIC, 4096, 2 * 819200, 4 * 1024 * 1024);
  fprintf(stderr, "\n[05] read-ahead static 4k 1.6M 4M \n");
  ssize_t total_bytes = 0;
  for (int i=329; i >= 0 ; i--)
  {
    XrdSysTimer sleeper;
    //sleeper.Wait(1000);
    uint32_t bytesRead=0;
    fprintf(stderr, ".");
    status = file.Read(4 * i * 200 * 1024, 4 * 200 * 1024, &buffer[i * 200 * 1024], bytesRead, (uint16_t) 300);
    total_bytes += bytesRead;
    //fprintf(stderr, "----: bytesRead=%u\n", bytesRead);
    ASSERT_TRUE( status.IsOK() );
  }

  ASSERT_EQ( total_bytes, (4 * 1024 * 1024 * 64));

  fprintf(stderr, "\n[06] comparing \n");
  for (ssize_t i=0; i < 64 * 1024 * 1024; i++)
  {
    if (buffer[i] != (int) i)
    {
      ASSERT_EQ( buffer[i], (int) i );
    }
  }

  fprintf(stderr, "\n[07] ra-efficiency=%f\n", file.get_readahead_efficiency());
  ASSERT_EQ (file.get_readahead_efficiency(), 0 );

  file.Collect();
  status = file.Close( (uint16_t) 0);
  ASSERT_TRUE( status.IsOK() );
}

TEST(XrdClProxy, ReadAheadDynamic) {
  eos::common::ShellCmd xrd("xrootd -p 21234 -n proxytest");
  XrdSysTimer sleeper;
  sleeper.Snooze(1);

  std::vector<int> buffer;
  buffer.resize(64 * 1024 * 1024);
  for (size_t i=0; i < buffer.size(); i++)
  {
    buffer[i] = i;
  }
  XrdCl::Proxy file;
  XrdCl::OpenFlags::Flags targetFlags = XrdCl::OpenFlags::Update | XrdCl::OpenFlags::Delete;
  XrdCl::Access::Mode mode = XrdCl::Access::UR | XrdCl::Access::UW | XrdCl::Access::UX;

  fprintf(stderr, "[01] open)\n");
  XrdCl::XRootDStatus status = file.Open("root://localhost:21234//tmp/xrdclproxytest", targetFlags, mode, 300);
  ASSERT_TRUE( status.IsOK() );
  fprintf(stderr, "[02] waitopen)\n");
  status = file.WaitOpen();
  ASSERT_TRUE( status.IsOK() );

  status = file.Truncate(0);
  ASSERT_TRUE( status.IsOK() );

  fprintf(stderr, "\n[03] write-async \n");
  for (size_t i=0; i < 64 ; ++i)
  {
    fprintf(stderr, ".");
    XrdCl::Proxy::write_handler handler = file.WriteAsyncPrepare(1);
    status = file.WriteAsync(4 * i * 1024 * 1024, 4 * 1024 * 1024, &buffer[i * 1024 * 1024], handler, (uint16_t) 300);
    ASSERT_TRUE( status.IsOK() );
  }

  status = file.WaitWrite();
  ASSERT_TRUE( status.IsOK() );

  fprintf(stderr, "\n[04] zero \n");
  for (size_t i=0; i < 64 * 1024 * 1024; i++)
  {
    buffer[i] = 0;
  }

  file.set_readahead_strategy(XrdCl::Proxy::DYNAMIC, 4096, 1 * 1024 * 1024, 8 * 1024 * 1024);
  fprintf(stderr, "\n[05] read-ahead dynamic 4k 1M 8M \n");
  ssize_t total_bytes = 0;
  for (size_t i=0; i < 330 ; ++i)
  {
    XrdSysTimer sleeper;
    //sleeper.Wait(1000);
    uint32_t bytesRead=0;
    fprintf(stderr, ".");
    status = file.Read(4 * i * 200 * 1024, 4 * 200 * 1024, &buffer[i * 200 * 1024], bytesRead, (uint16_t) 300);
    total_bytes += bytesRead;
    //fprintf(stderr, "----: bytesRead=%u\n", bytesRead);
    ASSERT_TRUE( status.IsOK() );
  }

  ASSERT_EQ ( total_bytes, (4 * 1024 * 1024 * 64));

  fprintf(stderr, "\n[06] comparing \n");
  for (ssize_t i=0; i < 64 * 1024 * 1024; i++)
  {
    if (buffer[i] != (int) i)
    {
      ASSERT_EQ( buffer[i], (int) i );
    }
  }
  fprintf(stderr, "\n[07] ra-efficiency=%f %d\n", file.get_readahead_efficiency(), (int) (1000000 * file.get_readahead_efficiency()));

  fprintf(stderr, "\n[07] ra-efficiency=%f\n", file.get_readahead_efficiency());
  ASSERT_EQ ( (int) (1000000 * file.get_readahead_efficiency()), 99169920 );
  file.Collect();
  status = file.Close( (uint16_t) 0);
  ASSERT_TRUE( status.IsOK() );
}
