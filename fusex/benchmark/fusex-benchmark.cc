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
#define LOOP_11 100
#define LOOP_12 10
#define LOOP_13 10
#define LOOP_14 100

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
    int fd = open(name, O_CREAT | O_RDWR, S_IRWXU);
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
    fprintf(stderr, ">>> test %04d\n", testno);
    snprintf(name, sizeof (name), "fjournal");
    unlink(name);

    int fd = open(name, O_CREAT | O_RDWR, S_IRWXU);

    if (fd < 0)
    {
      fprintf(stderr, "[test=%03d] creat failed\n", testno);
      exit(testno);
    }

    for (int i = 0; i < LOOP_10; i+=2)
    {
      ssize_t nwrite = pwrite(fd, &i, 4, (i * 4) + (2 * 1024 * 1024));
      if (nwrite != 4)
      {
        fprintf(stderr, "[test=%03d] failed linear(1) write i=%d\n", testno, i);
        exit(testno);
      }
    }
    for (int i = 0; i < LOOP_10; i+=2)
    {
      int v;
      ssize_t nread = pread(fd, &v, 4, (i * 4) + (2 * 1024 * 1024));
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

    for (int i = 1; i < LOOP_10; i+=2)
    {
      ssize_t nwrite = pwrite(fd, &i, 4, i * 4 + 2 * 1024 * 1024);
      if (nwrite != 4)
      {
        fprintf(stderr, "[test=%03d] failed linear(2) write i=%d\n", testno, i);
        exit(testno);
      }
    }
    for (int i = 0; i < LOOP_10; i+=1)
    {
      int v;
      ssize_t nread = pread(fd, &v, 4, i * 4 + 2 * 1024 * 1024);
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


  // ------------------------------------------------------------------------ //
  testno = 11;

  if ( (testno >= test_start) && (testno <= test_stop))
  {
    fprintf(stderr, ">>> test %04d\n", testno);

    eos::common::ShellCmd makethedir("dd if=/dev/urandom of=/var/tmp/random bs=1k count=16");
    eos::common::cmd_status rc = makethedir.wait(60);
    if (rc.exit_code)
    {
      fprintf(stderr, "[test=%03d] creation of random contents file failed\n", testno);
      exit(testno);
    }

    for (size_t i=0; i < LOOP_11; i++)
    {
      eos::common::ShellCmd ddcompare("dd if=/var/tmp/random of=random bs=1k count=16; diff /var/tmp/random random");
      eos::common::cmd_status rc = ddcompare.wait(10);
      if (rc.exit_code)
      {
        fprintf(stderr, "[test=%03d] dd & compare failed i=%lu\n", testno, i);
        exit(testno);
      }
    }
    eos::common::ShellCmd removethefiles("rm -rf random /var/tmp/random");
    rc = removethefiles.wait(5);
    if (rc.exit_code)
    {
      fprintf(stderr, "[test=%03d] rm -rf failed\n", testno);
      exit(testno);
    }
    COMMONTIMING("dd-diff-16k-loop", &tm);
  }


  // ------------------------------------------------------------------------ //
  testno = 12;

  if ( (testno >= test_start) && (testno <= test_stop))
  {
    fprintf(stderr, ">>> test %04d\n", testno);

    eos::common::ShellCmd makethedir("dd if=/dev/urandom of=/var/tmp/random bs=1M count=16");
    eos::common::cmd_status rc = makethedir.wait(60);
    if (rc.exit_code)
    {
      fprintf(stderr, "[test=%03d] creation of random contents file failed\n", testno);
      exit(testno);
    }

    for (size_t i=0; i < LOOP_12; i++)
    {
      eos::common::ShellCmd ddcompare("dd if=/var/tmp/random of=random bs=1M count=16; diff /var/tmp/random random");
      eos::common::cmd_status rc = ddcompare.wait(10);
      if (rc.exit_code)
      {
        fprintf(stderr, "[test=%03d] dd & compare failed i=%lu\n", testno, i);
        exit(testno);
      }
    }
    eos::common::ShellCmd removethefiles("rm -rf random /var/tmp/random");
    rc = removethefiles.wait(5);
    if (rc.exit_code)
    {
      fprintf(stderr, "[test=%03d] rm -rf failed\n", testno);
      exit(testno);
    }
    COMMONTIMING("dd-diff-16M-loop", &tm);
  }



  // ------------------------------------------------------------------------ //
  testno = 13;

  if ( (testno >= test_start) && (testno <= test_stop))
  {
    char buffer[1024];
    for (size_t i=0; i < 1024; ++i)
    {
      buffer [i] = (char) (i % 256);
    }

    fprintf(stderr, ">>> test %04d\n", testno);
    for (size_t i=0; i < LOOP_13; i++)
    {
      snprintf(name, sizeof (name), "test-unlink");
      int fd = open(name, O_CREAT | O_RDWR | O_TRUNC, S_IRWXU);
      if (fd < 0)
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
        // unlinking the file
        if (unlink(name))
        {
          fprintf(stderr, "[test=%03d] unlink failed i=%lu\n", testno, i);
          exit(testno);
        }
        if (!stat(name, &buf))
        {
          fprintf(stderr, "[test=%03d] file visible after ulink i=%lu\n", testno, i);
          exit(testno);
        }
        for (size_t i = 0; i < 4000; ++i)
        {
          ssize_t nwrite = write(fd, buffer, sizeof (buffer));
          if (nwrite != sizeof (buffer))
          {
            fprintf(stderr, "[test=%3d] write after unlink failed errno=%d i=%lu\n", testno, errno, i);
            exit(testno);
          }
          fstat(fd, &buf);
          if ( (errno != 2) || (buf.st_size != (off_t)((i + 1) * sizeof (buffer))))
          {
            fprintf(stderr, "[test=%3d] stat after write gives wrong size errno=%d size=%ld i=%lu\n", testno, errno, buf.st_size, i);
            exit(testno);
          }
        }
        for (size_t i = 0; i < 4000; ++i)
        {
          memset (buffer, 0, sizeof (buffer));
          ssize_t nread = pread(fd, buffer, sizeof (buffer), i * sizeof (buffer));
          if (nread != sizeof (buffer))
          {
            fprintf(stderr, "[test=%3d] read after unlink failed errno=%d i=%lu\n", testno, errno, i);
            exit(testno);
          }
          for (size_t l = 0; l < sizeof (buffer);  ++l)
          {
            if (buffer[l] != ( (char) (((size_t) l) % 256)))
            {
              fprintf(stderr, "[test=%3d] wrong contents for read after unlink i=%lu l=%lu b=%x\n", testno, i, l, buffer[l]);
              exit(testno);
            }
          }
        }
        memset(&buf, 0, sizeof (struct stat));
        fstat(fd, &buf);
        if ( (errno != 2 ) || (buf.st_size != 4000 * sizeof (buffer)) )
        {
          fprintf(stderr, "[test=%3d] stat after read gives wrong size errno=%d size=%ld\n", testno, errno, buf.st_size);
          exit(testno);
        }
      }
      close(fd);
    }
    COMMONTIMING("write-unlinked-loop", &tm);
  }

  // ------------------------------------------------------------------------ //
  testno = 14;

  if ((testno >= test_start) && (testno <= test_stop)) {
    fprintf(stderr, ">>> test %04d\n", testno);

    for (size_t i = 0; i < LOOP_14; i++) {
      int fd = creat("lockme", S_IRWXU);
      
      int lock_rc = lockf(fd, F_LOCK, 0);
      int tlock_rc = lockf(fd, F_TLOCK,0);
      int ulock_rc = lockf(fd, F_ULOCK, 0);
      int lockagain_rc = lockf(fd, F_LOCK, 0);
      close (fd);
      unlink("lockme");
      if (lock_rc | tlock_rc | ulock_rc | lockagain_rc)
      {
	fprintf(stderr,"[test=%3d] %d %d %d %d\n", testno, lock_rc, tlock_rc, ulock_rc, lockagain_rc);
	exit(testno);
      }
    }

    COMMONTIMING("rename-circular-loop", &tm);
  }


  tm.Print();
  fprintf(stdout, "realtime = %.02f", tm.RealTime());
}
