#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "common/Timing.hh"
#include "common/ShellCmd.hh"

#define LOOP_1 100
#define LOOP_2 100
#define LOOP_4 100
#define LOOP_6 3
#define LOOP_7 100
#define LOOP_8 100
#define LOOP_9 1000
#define LOOP_10 10000

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

  if ( (testno >= test_start) && (testno <= test_stop))
  {
    fprintf(stderr, ">>> test %04d\n", testno);
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
  if ( (testno >= test_start) && (testno <= test_stop))
  {
    fprintf(stderr, ">>> test %04d\n", testno);
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

  if ( (testno >= test_start) && (testno <= test_stop))
  {
    fprintf(stderr, ">>> test %04d\n", testno);
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

  if ( (testno >= test_start) && (testno <= test_stop))
  {
    fprintf(stderr, ">>> test %04d\n", testno);
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

  if ( (testno >= test_start) && (testno <= test_stop))
  {
    fprintf(stderr, ">>> test %04d\n", testno);
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

  // ------------------------------------------------------------------------ //
  testno = 6;

  if ( (testno >= test_start) && (testno <= test_stop))
  {
    fprintf(stderr, ">>> test %04d\n", testno);
    for (size_t i=0; i < LOOP_6; i++)
    {

      eos::common::ShellCmd makethedir("mkdir -p a/b/c/d/e/f/g/h/i/j/k/1/2/3/4/5/6/7/8/9/0");
      eos::common::cmd_status rc = makethedir.wait(5);
      if (rc.exit_code)
      {
        fprintf(stderr, "[test=%03d] mkdir -p failed i=%lu\n", testno, i);
        exit(testno);
      }
      eos::common::ShellCmd removethedir("rm -rf a/");
      rc = removethedir.wait(5);
      if (rc.exit_code)
      {
        fprintf(stderr, "[test=%03d] rm -rf failed i=%lu\n", testno, i);
        exit(testno);
      }
    }
    COMMONTIMING("mkdir-p-loop", &tm);
  }

  // ------------------------------------------------------------------------ //
  testno = 7;

  if ( (testno >= test_start) && (testno <= test_stop))
  {
    fprintf(stderr, ">>> test %04d\n", testno);
    for (size_t i=0; i < LOOP_7; i++)
    {
      char execline[1024];
      snprintf(execline, sizeof (execline), "for name in `seq 1 100`; do echo %lu.$name >> append.%d; done", i, LOOP_7);
      eos::common::ShellCmd appendfile(execline);
      eos::common::cmd_status rc = appendfile.wait(5);
      if (rc.exit_code)
      {
        fprintf(stderr, "[test=%03d] echo >> failed i=%lu\n", testno, i);
        exit(testno);
      }
    }
    char execline[1024];
    snprintf(execline, sizeof (execline), "rm -rf append.%d", LOOP_7);
    eos::common::ShellCmd removethefile(execline);
    eos::common::cmd_status rc = removethefile.wait(5);
    if (rc.exit_code)
    {
      fprintf(stderr, "[test=%03d] rm -rf failed \n", testno);
      exit(testno);
    }
    COMMONTIMING("echo-append-loop", &tm);
  }

  // ------------------------------------------------------------------------ //
  testno = 8;

  if ( (testno >= test_start) && (testno <= test_stop))
  {
    fprintf(stderr, ">>> test %04d\n", testno);
    for (size_t i=0; i < LOOP_8; i++)
    {
      char execline[1024];
      snprintf(execline, sizeof (execline), "cp /etc/passwd pwd1 && mv passwd pwd2 && stat pwd1 || stat pwd2 && mv pwd2 pwd1 && stat pwd2 || stat pwd1 &&  rm -rf pwd1");

      eos::common::ShellCmd renames(execline);
      eos::common::cmd_status rc = renames.wait(5);
      if (rc.exit_code)
      {
        fprintf(stderr, "[test=%03d] circular-rename failed i=%lu\n", testno, i);
        exit(testno);
      }
    }
    COMMONTIMING("rename-circular-loop", &tm);
  }

  // ------------------------------------------------------------------------ //
  testno = 9;

  if ( (testno >= test_start) && (testno <= test_stop))
  {
    fprintf(stderr, ">>> test %04d\n", testno);
    snprintf(name, sizeof (name), "ftruncate");
    unlink(name);
    int fd = open(name, O_CREAT|O_RDWR, S_IRWXU);
    if (fd > 0)
    {
      for (size_t i=0; i < LOOP_9; i++)
      {
        int rc = ftruncate (fd, i);
        if (rc)
        {
          fprintf(stderr, "[test=%03d] failed ftruncate linear truncate i=%lu rc=%d errno=%d\n", testno, i, rc, errno);
          exit(testno);
        }
        struct stat buf;
        if (fstat(fd, &buf))
        {
          fprintf(stderr, "[test=%03d] failed stat linear truncate i=%lu\n", testno, i);
          exit(testno);
        }
        else
        {
          if ( (size_t) buf.st_size != i)
          {
            fprintf(stderr, "[test=%03d] falsed size linear truncate i=%lu size=%lu\n", testno, i, buf.st_size);
            exit(testno);
          }
        }
      }
      close(fd);
      if (unlink(name))
      {
        fprintf(stderr, "[test=%03d] failed unlink linear truncate \n", testno);
        exit(testno);
      }
    }

    COMMONTIMING("truncate-expand-loop", &tm);
  }

  // ------------------------------------------------------------------------ //
  testno = 10;

  if ( (testno >= test_start) && (testno <= test_stop))
  {
    
    snprintf(name, sizeof (name), "fjournal");
    unlink(name);    

    int fd = open(name, O_CREAT|O_RDWR, S_IRWXU);
    for (int i = 0; i< LOOP_10; i+=2)
    {
      ssize_t nwrite = pwrite(fd, &i, 4, (i*4) + (2*1024*1024));
      if (nwrite != 4)
      {
	fprintf(stderr, "[test=%03d] failed linear(1) write i=%d\n", testno, i);
	exit(testno);
      }
    }
    for (int i = 0; i< LOOP_10; i+=2)
    {
      int v;
      ssize_t nread = pread(fd, &v, 4, (i*4) + (2*1024*1024));
      if (nread != 4)
      {
	fprintf(stderr, "[test=%03d] failed linear read i=%d\n", testno, i);
	exit(testno);
      }
      if (v != i)
      {
	fprintf(stderr, "[test=%03d] inconsistent(1) read i=%d != v=%d\n", testno, i, v);
	exit(testno);
      }
    }
    fdatasync(fd);
    
    for (int i = 1; i< LOOP_10; i+=2)
    {
      ssize_t nwrite = pwrite(fd, &i, 4, i*4 + 2*1024*1024);
      if (nwrite != 4)
      {
	fprintf(stderr, "[test=%03d] failed linear(2) write i=%d\n", testno, i);
	exit(testno);
      }
    }
    for (int i = 0; i< LOOP_10; i+=1)
    {
      int v;
      ssize_t nread = pread(fd, &v, 4, i*4 + 2*1024*1024);
      if (nread != 4)
      {
	fprintf(stderr, "[test=%03d] failed linear read i=%d\n", testno, i);
	exit(testno);
      }
      if (v != i)
      {
	fprintf(stderr, "[test=%03d] inconsistent(2) read i=%d != v=%d\n", testno, i, v);
	exit(testno);
      }
    }

    fdatasync(fd);
    close(fd);
    
    unlink(name);

    COMMONTIMING("journal-cache-timing", &tm);
  }

  tm.Print();
  fprintf(stdout, "realtime = %.02f", tm.RealTime());
}
