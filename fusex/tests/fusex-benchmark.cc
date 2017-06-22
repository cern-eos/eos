#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "common/Timing.hh"

#define LOOP_1 100
#define LOOP_2 100
#define LOOP_4 100

int main(int argc, char* argv[])
{
  eos::common::Timing tm("Test");
  char name[1024];

  size_t ino=0;
  int testno=0;

  int test_start = (argc > 1) ? atoi(argv[1]) : 0;
  int test_stop = (argc > 2) ? atoi(argv[2]) : 999999;

  // ------------------------------------------------------------------------ //
  testno = 1;
  COMMONTIMING("test-start", &tm);
  struct stat buf;

  if ( (testno>= test_start) && (testno <= test_stop))
  {
    for (size_t i=0; i < LOOP_1; i++)
    {
      snprintf(name, sizeof (name), "test-same");
      int fd = creat(name, S_IRWXU);
      if (fd > 0)
      {
        close(fd);
      }
      else
      {
        fprintf(stderr, "[test=%03d] creat failed i=%lu\n", testno, i);
        exit(testno);
      }
      if (stat(name, &buf))
      {
        fprintf(stderr, "[test=%03d] creation failed i=%lu\n", testno, i);
        exit(testno);
      }
      else
      {
        if (ino)
        {
          if (buf.st_ino == ino)
          {
            fprintf(stderr, "[test=%03d] inode sequence violation i=%lu\n", testno, i);
            exit(testno);
          }
        }
        else
        {
          ino = buf.st_ino;
        }
      }
      if (unlink(name))
      {
        fprintf(stderr, "[test=%03d] unlink failed i=%lu\n", testno, i);
        exit(testno);
      }
    }
    COMMONTIMING("create-delete-loop", &tm);
  }


  // ------------------------------------------------------------------------ //
  testno = 2;
  if ( (testno>= test_start) && (testno <= test_stop))
  {
    for (size_t i=0; i < LOOP_2; i++)
    {
      snprintf(name, sizeof (name), "test-mkdir-%04lu", i);
      if (mkdir(name, S_IRWXU))
      {
        fprintf(stderr, "[test=%03d] mkdir failed i=%lu\n", testno, i);
        exit(testno);
      }
    }
    COMMONTIMING("mkdir-flat-loop", &tm);
  }
  // ------------------------------------------------------------------------ //
  testno = 3;

  if ( (testno>= test_start) && (testno <= test_stop))
  {
    for (size_t i=0; i < LOOP_2; i++)
    {
      snprintf(name, sizeof (name), "test-mkdir-%04lu", i);
      if (rmdir(name))
      {
        fprintf(stderr, "[test=%03d] rmdir failed i=%lu\n", testno, i);
        exit(testno);
      }
    }
    COMMONTIMING("rmdir-flat-loop", &tm);
  }
  // ------------------------------------------------------------------------ //
  testno = 4;

  if ( (testno>= test_start) && (testno <= test_stop))
  {
    ino = 0;
    for (size_t i=0; i < LOOP_4; i++)
    {
      snprintf(name, sizeof (name), "test-file-%lu", i);
      int fd = creat(name, S_IRWXU);
      if (fd < 0)
      {
        fprintf(stderr, "[test=%03d] creat failed i=%lu\n", testno, i);
        exit(testno);
      }
      char buffer[4];
      memcpy(buffer, &i, sizeof (int));

      if (stat(name, &buf))
      {
        fprintf(stderr, "[test=%03d] creation failed i=%lu\n", testno, i);
        exit(testno);
      }
      else
      {
        if (ino)
        {
          if (buf.st_ino == ino)
          {
            fprintf(stderr, "[test=%03d] inode sequence violation i=%lu\n", testno, i);
            exit(testno);
          }
        }
        else
        {
          ino = buf.st_ino;
        }
      }
      ssize_t nwrite = pwrite(fd, buffer, 4, i);
      if (nwrite != 4)
      {
        fprintf(stderr, "[test=%03d] pwrite failed %ld i=%lu\n", testno, nwrite, i);
        exit(testno);
      }
      close(fd);
    }
    COMMONTIMING("create-pwrite-loop", &tm);
  }

  // ------------------------------------------------------------------------ //
  testno = 5;

  if ( (testno>= test_start) && (testno <= test_stop))
  {
    for (size_t i=0; i < LOOP_4; i++)
    {
      snprintf(name, sizeof (name), "test-file-%lu", i);

      if (unlink(name))
      {
        fprintf(stderr, "[test=%03d] unlink failed i=%lu\n", testno, i);
        exit(testno);
      }
    }
    COMMONTIMING("delete-loop", &tm);
  }

  tm.Print();
  fprintf(stdout, "realtime = %.02f", tm.RealTime());
}
