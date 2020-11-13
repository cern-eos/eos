#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <dirent.h>
#include <errno.h>
#include <sys/types.h>
#include <vector>
#include <string>
#include <set>

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
#define LOOP_15 100
#define LOOP_16 100
#define LOOP_17 1234
#define LOOP_18 100
#define LOOP_19 100
#define LOOP_20 10

int main(int argc, char* argv[])
{
  eos::common::Timing tm("Test");
  char name[1024];
  size_t ino = 0;
  int testno = 0;
  int test_start = (argc > 1) ? atoi(argv[1]) : 0;
  int test_stop = (argc > 2) ? atoi(argv[2]) : 999999;
  // ------------------------------------------------------------------------ //
  testno = 1;
  COMMONTIMING("test-start", &tm);
  struct stat buf;

  if ((testno >= test_start) && (testno <= test_stop)) {
    fprintf(stderr, ">>> test %04d\n", testno);

    for (size_t i = 0; i < LOOP_1; i++) {
      snprintf(name, sizeof(name), "test-same");
      int fd = creat(name, S_IRWXU);

      if (fd > 0) {
        close(fd);
      } else {
        fprintf(stderr, "[test=%03d] creat failed i=%lu\n", testno, i);
        exit(testno);
      }

      if (stat(name, &buf)) {
        fprintf(stderr, "[test=%03d] creation failed i=%lu\n", testno, i);
        exit(testno);
      } else {
        if (ino) {
          if (buf.st_ino == ino) {
            fprintf(stderr, "[test=%03d] inode sequence violation i=%lu\n", testno, i);
            exit(testno);
          }
        } else {
          ino = buf.st_ino;
        }
      }

      if (unlink(name)) {
        fprintf(stderr, "[test=%03d] unlink failed i=%lu\n", testno, i);
        exit(testno);
      }
    }

    COMMONTIMING("create-delete-loop", &tm);
  }

  // ------------------------------------------------------------------------ //
  testno = 2;

  if ((testno >= test_start) && (testno <= test_stop)) {
    fprintf(stderr, ">>> test %04d\n", testno);

    for (size_t i = 0; i < LOOP_2; i++) {
      snprintf(name, sizeof(name), "test-mkdir-%04lu", i);

      if (mkdir(name, S_IRWXU)) {
        fprintf(stderr, "[test=%03d] mkdir failed i=%lu\n", testno, i);
        exit(testno);
      }
    }

    COMMONTIMING("mkdir-flat-loop", &tm);
  }

  // ------------------------------------------------------------------------ //
  testno = 3;

  if ((testno >= test_start) && (testno <= test_stop)) {
    fprintf(stderr, ">>> test %04d\n", testno);

    for (size_t i = 0; i < LOOP_2; i++) {
      snprintf(name, sizeof(name), "test-mkdir-%04lu", i);

      if (rmdir(name)) {
        fprintf(stderr, "[test=%03d] rmdir failed i=%lu\n", testno, i);
        exit(testno);
      }
    }

    COMMONTIMING("rmdir-flat-loop", &tm);
  }

  // ------------------------------------------------------------------------ //
  testno = 4;

  if ((testno >= test_start) && (testno <= test_stop)) {
    fprintf(stderr, ">>> test %04d\n", testno);
    ino = 0;

    for (size_t i = 0; i < LOOP_4; i++) {
      snprintf(name, sizeof(name), "test-file-%lu", i);
      int fd = creat(name, S_IRWXU);

      if (fd < 0) {
        fprintf(stderr, "[test=%03d] creat failed i=%lu\n", testno, i);
        exit(testno);
      }

      char buffer[4];
      memcpy(buffer, &i, sizeof(int));

      if (stat(name, &buf)) {
        fprintf(stderr, "[test=%03d] creation failed i=%lu\n", testno, i);
        exit(testno);
      } else {
        if (ino) {
          if (buf.st_ino == ino) {
            fprintf(stderr, "[test=%03d] inode sequence violation i=%lu\n", testno, i);
            exit(testno);
          }
        } else {
          ino = buf.st_ino;
        }
      }

      ssize_t nwrite = pwrite(fd, buffer, 4, i);

      if (nwrite != 4) {
        fprintf(stderr, "[test=%03d] pwrite failed %ld i=%lu\n", testno, nwrite, i);
        exit(testno);
      }

      close(fd);
    }

    COMMONTIMING("create-pwrite-loop", &tm);
  }

  // ------------------------------------------------------------------------ //
  testno = 5;

  if ((testno >= test_start) && (testno <= test_stop)) {
    fprintf(stderr, ">>> test %04d\n", testno);

    for (size_t i = 0; i < LOOP_4; i++) {
      snprintf(name, sizeof(name), "test-file-%lu", i);

      if (unlink(name)) {
        fprintf(stderr, "[test=%03d] unlink failed i=%lu\n", testno, i);
        exit(testno);
      }
    }

    COMMONTIMING("delete-loop", &tm);
  }

  // ------------------------------------------------------------------------ //
  testno = 6;

  if ((testno >= test_start) && (testno <= test_stop)) {
    fprintf(stderr, ">>> test %04d\n", testno);

    for (size_t i = 0; i < LOOP_6; i++) {
      eos::common::ShellCmd
      makethedir("mkdir -p a/b/c/d/e/f/g/h/i/j/k/1/2/3/4/5/6/7/8/9/0");
      eos::common::cmd_status rc = makethedir.wait(5);

      if (rc.exit_code) {
        fprintf(stderr, "[test=%03d] mkdir -p failed i=%lu\n", testno, i);
        exit(testno);
      }

      eos::common::ShellCmd removethedir("rm -rf a/");
      rc = removethedir.wait(5);

      if (rc.exit_code) {
        fprintf(stderr, "[test=%03d] rm -rf failed i=%lu\n", testno, i);
        exit(testno);
      }
    }

    COMMONTIMING("mkdir-p-loop", &tm);
  }

  // ------------------------------------------------------------------------ //
  testno = 7;

  if ((testno >= test_start) && (testno <= test_stop)) {
    fprintf(stderr, ">>> test %04d\n", testno);

    for (size_t i = 0; i < LOOP_7; i++) {
      char execline[1024];
      snprintf(execline, sizeof(execline),
               "for name in `seq 1 100`; do echo %lu.$name >> append.%d; done", i, LOOP_7);
      eos::common::ShellCmd appendfile(execline);
      eos::common::cmd_status rc = appendfile.wait(5);

      if (rc.exit_code) {
        fprintf(stderr, "[test=%03d] echo >> failed i=%lu\n", testno, i);
        exit(testno);
      }
    }

    char execline[1024];
    snprintf(execline, sizeof(execline), "rm -rf append.%d", LOOP_7);
    eos::common::ShellCmd removethefile(execline);
    eos::common::cmd_status rc = removethefile.wait(5);

    if (rc.exit_code) {
      fprintf(stderr, "[test=%03d] rm -rf failed \n", testno);
      exit(testno);
    }

    COMMONTIMING("echo-append-loop", &tm);
  }

  // ------------------------------------------------------------------------ //
  testno = 8;

  if ((testno >= test_start) && (testno <= test_stop)) {
    fprintf(stderr, ">>> test %04d\n", testno);

    for (size_t i = 0; i < LOOP_8; i++) {
      char execline[1024];
      snprintf(execline, sizeof(execline),
               "cp /etc/passwd pwd1 && mv passwd pwd2 && stat pwd1 || stat pwd2 && mv pwd2 pwd1 && stat pwd2 || stat pwd1 &&  rm -rf pwd1");
      eos::common::ShellCmd renames(execline);
      eos::common::cmd_status rc = renames.wait(5);

      if (rc.exit_code) {
        fprintf(stderr, "[test=%03d] circular-rename failed i=%lu\n", testno, i);
        exit(testno);
      }
    }

    COMMONTIMING("rename-circular-loop", &tm);
  }

  // ------------------------------------------------------------------------ //
  testno = 9;

  if ((testno >= test_start) && (testno <= test_stop)) {
    fprintf(stderr, ">>> test %04d\n", testno);
    snprintf(name, sizeof(name), "ftruncate");
    unlink(name);
    int fd = open(name, O_CREAT | O_RDWR, S_IRWXU);

    if (fd > 0) {
      for (size_t i = 0; i < LOOP_9; i++) {
        int rc = ftruncate(fd, i);

        if (rc) {
          fprintf(stderr,
                  "[test=%03d] failed ftruncate linear truncate i=%lu rc=%d errno=%d\n", testno,
                  i, rc, errno);
          exit(testno);
        }

        struct stat buf;

        if (fstat(fd, &buf)) {
          fprintf(stderr, "[test=%03d] failed stat linear truncate i=%lu\n", testno, i);
          exit(testno);
        } else {
          if ((size_t) buf.st_size != i) {
            fprintf(stderr, "[test=%03d] failed size linear truncate i=%lu size=%lu\n",
                    testno, i, buf.st_size);
            exit(testno);
          }
        }
      }

      close(fd);

      if (unlink(name)) {
        fprintf(stderr, "[test=%03d] failed unlink linear truncate \n", testno);
        exit(testno);
      }
    }

    COMMONTIMING("truncate-expand-loop", &tm);
  }

  // ------------------------------------------------------------------------ //
  testno = 10;

  if ((testno >= test_start) && (testno <= test_stop)) {
    fprintf(stderr, ">>> test %04d\n", testno);
    snprintf(name, sizeof(name), "fjournal");
    unlink(name);
    int fd = open(name, O_CREAT | O_RDWR, S_IRWXU);

    if (fd < 0) {
      fprintf(stderr, "[test=%03d] creat failed\n", testno);
      exit(testno);
    }

    for (int i = 0; i < LOOP_10; i += 2) {
      ssize_t nwrite = pwrite(fd, &i, 4, (i * 4) + (2 * 1024 * 1024));

      if (nwrite != 4) {
        fprintf(stderr, "[test=%03d] failed linear(1) write i=%d\n", testno, i);
        exit(testno);
      }
    }

    for (int i = 0; i < LOOP_10; i += 2) {
      int v;
      ssize_t nread = pread(fd, &v, 4, (i * 4) + (2 * 1024 * 1024));

      if (nread != 4) {
        fprintf(stderr, "[test=%03d] failed linear read i=%d nread=%ld \n", testno, i, nread);
        exit(testno);
      }

      if (v != i) {
        fprintf(stderr, "[test=%03d] inconsistent(1) read i=%d != v=%d\n", testno, i,
                v);
        exit(testno);
      }
    }

    fdatasync(fd);

    for (int i = 1; i < LOOP_10; i += 2) {
      ssize_t nwrite = pwrite(fd, &i, 4, i * 4 + 2 * 1024 * 1024);

      if (nwrite != 4) {
        fprintf(stderr, "[test=%03d] failed linear(2) write i=%d\n", testno, i);
        exit(testno);
      }
    }

    for (int i = 0; i < LOOP_10; i += 1) {
      int v;
      ssize_t nread = pread(fd, &v, 4, i * 4 + 2 * 1024 * 1024);

      if (nread != 4) {
        fprintf(stderr, "[test=%03d] failed linear read i=%d\n", testno, i);
        exit(testno);
      }

      if (v != i) {
        fprintf(stderr, "[test=%03d] inconsistent(2) read i=%d != v=%d\n", testno, i,
                v);
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

  if ((testno >= test_start) && (testno <= test_stop)) {
    fprintf(stderr, ">>> test %04d\n", testno);
    eos::common::ShellCmd
    makethedir("dd if=/dev/urandom of=/var/tmp/random bs=1k count=16");
    eos::common::cmd_status rc = makethedir.wait(60);

    if (rc.exit_code) {
      fprintf(stderr, "[test=%03d] creation of random contents file failed\n",
              testno);
      exit(testno);
    }

    for (size_t i = 0; i < LOOP_11; i++) {
      eos::common::ShellCmd
      ddcompare("dd if=/var/tmp/random of=random bs=1k count=16; diff /var/tmp/random random");
      eos::common::cmd_status rc = ddcompare.wait(10);

      if (rc.exit_code) {
        fprintf(stderr, "[test=%03d] dd & compare failed i=%lu\n", testno, i);
        exit(testno);
      }
    }

    eos::common::ShellCmd removethefiles("rm -rf random /var/tmp/random");
    rc = removethefiles.wait(5);

    if (rc.exit_code) {
      fprintf(stderr, "[test=%03d] rm -rf failed\n", testno);
      exit(testno);
    }

    COMMONTIMING("dd-diff-16k-loop", &tm);
  }

  // ------------------------------------------------------------------------ //
  testno = 12;

  if ((testno >= test_start) && (testno <= test_stop)) {
    fprintf(stderr, ">>> test %04d\n", testno);
    eos::common::ShellCmd
    makethedir("dd if=/dev/urandom of=/var/tmp/random bs=1M count=16");
    eos::common::cmd_status rc = makethedir.wait(60);

    if (rc.exit_code) {
      fprintf(stderr, "[test=%03d] creation of random contents file failed\n",
              testno);
      exit(testno);
    }

    for (size_t i = 0; i < LOOP_12; i++) {
      eos::common::ShellCmd
      ddcompare("dd if=/var/tmp/random of=random bs=1M count=16; diff /var/tmp/random random");
      eos::common::cmd_status rc = ddcompare.wait(10);

      if (rc.exit_code) {
        fprintf(stderr, "[test=%03d] dd & compare failed i=%lu\n", testno, i);
        exit(testno);
      }
    }

    eos::common::ShellCmd removethefiles("rm -rf random /var/tmp/random");
    rc = removethefiles.wait(5);

    if (rc.exit_code) {
      fprintf(stderr, "[test=%03d] rm -rf failed\n", testno);
      exit(testno);
    }

    COMMONTIMING("dd-diff-16M-loop", &tm);
  }

  // ------------------------------------------------------------------------ //
  testno = 13;

  if ((testno >= test_start) && (testno <= test_stop)) {
    char buffer[1024];

    for (size_t i = 0; i < 1024; ++i) {
      buffer [i] = (char)(i % 256);
    }

    fprintf(stderr, ">>> test %04d\n", testno);

    for (size_t i = 0; i < LOOP_13; i++) {
      snprintf(name, sizeof(name), "test-unlink");
      int fd = open(name, O_CREAT | O_RDWR | O_TRUNC, S_IRWXU);

      if (fd < 0) {
        fprintf(stderr, "[test=%03d] creat failed i=%lu\n", testno, i);
        exit(testno);
      }

      if (stat(name, &buf)) {
        fprintf(stderr, "[test=%03d] creation failed i=%lu\n", testno, i);
        exit(testno);
      } else {
        // unlinking the file
        if (unlink(name)) {
          fprintf(stderr, "[test=%03d] unlink failed i=%lu\n", testno, i);
          exit(testno);
        }

        if (!stat(name, &buf)) {
          fprintf(stderr, "[test=%03d] file visible after ulink i=%lu\n", testno, i);
          exit(testno);
        }

        for (size_t i = 0; i < 4000; ++i) {
          ssize_t nwrite = write(fd, buffer, sizeof(buffer));

          if (nwrite != sizeof(buffer)) {
            fprintf(stderr, "[test=%3d] write after unlink failed errno=%d i=%lu\n", testno,
                    errno, i);
            exit(testno);
          }

          fstat(fd, &buf);

          if ((errno != 2) || (buf.st_size != (off_t)((i + 1) * sizeof(buffer)))) {
            fprintf(stderr,
                    "[test=%3d] stat after write gives wrong size errno=%d size=%ld i=%lu\n",
                    testno, errno, buf.st_size, i);
            exit(testno);
          }
        }

        for (size_t i = 0; i < 4000; ++i) {
          memset(buffer, 0, sizeof(buffer));
          ssize_t nread = pread(fd, buffer, sizeof(buffer), i * sizeof(buffer));

          if (nread != sizeof(buffer)) {
            fprintf(stderr, "[test=%3d] read after unlink failed errno=%d i=%lu\n", testno,
                    errno, i);
            exit(testno);
          }

          for (size_t l = 0; l < sizeof(buffer); ++l) {
            if (buffer[l] != ((char)(((size_t) l) % 256))) {
              fprintf(stderr,
                      "[test=%3d] wrong contents for read after unlink i=%lu l=%lu b=%x\n", testno, i,
                      l, buffer[l]);
              exit(testno);
            }
          }
        }

        memset(&buf, 0, sizeof(struct stat));
        fstat(fd, &buf);

        if ((errno != 2) || (buf.st_size != 4000 * sizeof(buffer))) {
          fprintf(stderr,
                  "[test=%3d] stat after read gives wrong size errno=%d size=%ld\n", testno,
                  errno, buf.st_size);
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
      int tlock_rc = lockf(fd, F_TLOCK, 0);
      int ulock_rc = lockf(fd, F_ULOCK, 0);
      int lockagain_rc = lockf(fd, F_LOCK, 0);
      close(fd);
      unlink("lockme");

      if (lock_rc | tlock_rc | ulock_rc | lockagain_rc) {
        fprintf(stderr, "[test=%3d] %d %d %d %d\n", testno, lock_rc, tlock_rc, ulock_rc,
                lockagain_rc);
        exit(testno);
      }
    }

    COMMONTIMING("create-lockf-loop", &tm);
  }

  // ------------------------------------------------------------------------ //
  testno = 15;

  if ((testno >= test_start) && (testno <= test_stop)) {
    fprintf(stderr, ">>> test %04d\n", testno);

    for (size_t i = 0; i < LOOP_15; i++) {
      snprintf(name, sizeof(name), "test-same");
      int fd = creat(name, S_IRWXU);

      if (fd > 0) {
        close(fd);
      } else {
        fprintf(stderr, "[test=%03d] creat failed i=%lu\n", testno, i);
        exit(testno);
      }

      if (unlink(name)) {
        fprintf(stderr, "[test=%03d] unlink failed i=%lu\n", testno, i);
        exit(testno);
      }

      if (symlink("../test",name)) {
	fprintf(stderr, "[test=%03d] symlink failed i=%lu errno=%d\n", testno, i, errno);
        exit(testno);
      }

      if (unlink(name)) {
        fprintf(stderr, "[test=%03d] unlink failed i=%lu\n", testno, i);
        exit(testno);
      }
    }

    COMMONTIMING("create-symlink-loop", &tm);
  }

  // ------------------------------------------------------------------------ //
  testno = 16;

  if ((testno >= test_start) && (testno <= test_stop)) {
    fprintf(stderr, ">>> test %04d\n", testno);

    for (size_t i = 0; i < LOOP_16; i++) {
      snprintf(name, sizeof(name), "test-same");

      if (mkdir(name, S_IRWXU)) {
        fprintf(stderr, "[test=%03d] mkdir failed i=%lu errno=%d\n", testno, i, errno);
        exit(testno);
      }

      if (rmdir(name)) {
        fprintf(stderr, "[test=%03d] unlink failed i=%lu\n", testno, i);
        exit(testno);
      }

      if (symlink("../test",name)) {
	fprintf(stderr, "[test=%03d] symlink failed i=%lu errno=%d\n", testno, i, errno);
        exit(testno);
      }

      if (unlink(name)) {
        fprintf(stderr, "[test=%03d] unlink failed i=%lu\n", testno, i);
        exit(testno);
      }
    }

    COMMONTIMING("mkdir-symlink-loop", &tm);
  }

  // ------------------------------------------------------------------------ //
  testno = 17;

  if ((testno >= test_start) && (testno <= test_stop)) {
    fprintf(stderr, ">>> test %04d\n", testno);
    std::set<std::string> names;
    std::set<std::string> found;
    names.insert(".");
    names.insert("..");

    for (size_t i = 0; i < LOOP_17; i++) {
      snprintf(name, sizeof(name), "test-readdir-%lu", i);

      if (mkdir(name, S_IRWXU)) {
        fprintf(stderr, "[test=%03d] mkdir failed i=%lu errno=%d\n", testno, i, errno);
        exit(testno);
      }
      names.insert(name);
    }

    DIR* dir = opendir(".");
    struct dirent* rdir=0;

    size_t cnt=0;
    std::vector<std::string> position;
    position.resize(LOOP_17+2);

    do {
      //      fprintf(stdout, "pos=%ld\n", telldir(dir));
      rdir = readdir(dir);
      if (rdir) {
	//fprintf(stdout, "pos=%ld name=%s\n", telldir(dir), rdir->d_name);
      } else {
	//fprintf(stdout, "EOF pos=%ld\n", telldir(dir));
      }
      off_t offset = telldir(dir);
      seekdir(dir, offset);
      if (rdir) {
	position[offset-1] = rdir->d_name;
	if (found.count(rdir->d_name)) {
	  fprintf(stderr,"[test=%03d] readdir failed duplicated item got=%s\n", testno, rdir->d_name);
	  exit(testno);
	}
	if (!names.count(rdir->d_name)) {
	  fprintf(stderr,"[test=%03d] readdir failed missing item got=%s\n", testno, rdir->d_name);
	  exit(testno);
	}
	found.insert(rdir->d_name);
      }
      cnt++;
    } while (rdir);

    for (size_t i = 0; i< 10*LOOP_17; i++) {
      size_t idx = LOOP_17 * (1.0 * std::rand() / (RAND_MAX));
      seekdir (dir, idx);
      rdir = readdir (dir);
      if (rdir) {
	if (position[idx] != std::string(rdir->d_name)) {
	  fprintf(stderr,"[test=%03d] readdir failed inconsistent entry got=%s for index=%lu\n", testno, rdir->d_name, idx);
	  exit(testno);
	}
      }
    }

    // create one more directory
    mkdir ("onemore", S_IRWXU);
    
    // check the original positions
    for (size_t i = 0; i< LOOP_17; i++) {
      size_t idx = LOOP_17;
      seekdir (dir, idx);
      rdir = readdir (dir);
      if (rdir) {
	if (position[idx] != std::string(rdir->d_name)) {
	  fprintf(stderr,"[test=%03d] readdir failed inconsistent entry got=%s for index=%lu\n", testno, rdir->d_name, idx);
	  exit(testno);
	}
      }
    }

    seekdir (dir, LOOP_17+2);
    rdir = readdir (dir);
    if (rdir) {
      if ( std::string("onemore") != std::string(rdir->d_name) ) {
	fprintf(stderr,"[test=%03d] readdir failed to get one new directory in correct position\n", testno);
	exit(testno);
      }
      // fprintf(stdout, "got on new position %s\n", rdir->d_name);
    }

    // remove one directory
    rmdir(position[2].c_str());
    
    for (size_t i = 0; i < LOOP_17; i++) {
      seekdir (dir, i+3);
      rdir = readdir (dir);
      if ( position[2] == std::string(rdir->d_name) ) {
	fprintf(stderr,"[test=%03d] readdir failed to have correct position after deletion\n", testno);
	exit(testno);
      }
    }

    if (dir) {
      closedir(dir);
    }

    for (size_t i = 0 ; i< position.size(); ++i) {
      rmdir(position[i].c_str());
    }
    rmdir("onemore");
    COMMONTIMING("readdir-loop", &tm);
  }

  // ------------------------------------------------------------------------ //
  testno = 18;

  if ((testno >= test_start) && (testno <= test_stop)) {
    fprintf(stderr, ">>> test %04d\n", testno);

    int fd = creat("lockme", S_IRWXU);
    if (ftruncate(fd, 1000)) {
      fprintf(stderr,"[test=%3d] errno=%d\n", testno, errno);
      exit(testno);
    }

    close(fd);
    fd = open("lockme", 0, 0);

    struct flock fl;
    memset(&fl, 0, sizeof(fl));

    fl.l_type = F_RDLCK;
    fl.l_whence = SEEK_SET;
    fl.l_start = 100;
    fl.l_len = 100;
    fl.l_pid = 0;

    for (size_t i = 0; i < LOOP_18; i++) {

      int do_shared_lock  = fcntl(fd, F_SETLKW, &fl);
      
      if ( do_shared_lock == -1) {
        fprintf(stderr, "[test=%3d] shared lock failed errno=%d\n", testno, errno);
        exit(testno);
      }
    }

    close(fd);
    unlink("lockme");
    
    COMMONTIMING("shared-lock-loop", &tm);
  }

  // ------------------------------------------------------------------------ //
  testno = 19;

  if ((testno >= test_start) && (testno <= test_stop)) {
    fprintf(stderr, ">>> test %04d\n", testno);

    int fd = creat("lockme", S_IRWXU);
    if (ftruncate(fd, 1000)) {
      fprintf(stderr,"[test=%3d] errno=%d\n", testno, errno);
      exit(testno);
    }

    struct flock fl;
    memset(&fl, 0, sizeof(fl));

    fl.l_type = F_WRLCK;
    fl.l_whence = SEEK_SET;
    fl.l_start = 100;
    fl.l_len = 100;
    fl.l_pid = 0;

    for (size_t i = 0; i < LOOP_19; i++) {

      int do_shared_lock  = fcntl(fd, F_SETLKW, &fl);
      
      if ( do_shared_lock == -1) {
        fprintf(stderr, "[test=%3d] exclusive lock failed errno=%d\n", testno, errno);
        exit(testno);
      }
    }

    close(fd);
    unlink("lockme");
    
    COMMONTIMING("exclusive-lock-loop", &tm);
  }

  // ------------------------------------------------------------------------ //
  testno = 20;

  if ((testno >= test_start) && (testno <= test_stop)) {
    fprintf(stderr, ">>> test %04d\n", testno);

    char buffer[1024];
    char rbuffer[1024];
    sprintf(buffer,"https://git.test.cern.ch");

    for (size_t i = 0; i < LOOP_19; i++) {

      int fd = creat("config.lock", S_IRWXU);
      if (fd < 0) {
	fprintf(stderr, "[test=%3d] file creation failed errno=%d\n", testno, errno);
	exit(testno);
      }
      size_t nwrite = (size_t)write(fd, buffer, strlen(buffer)+1);
      if (nwrite != (strlen(buffer)+1)) {
	fprintf(stderr,"[test=%3d] file write failed - wrote %lu/%lu - errno=%d\n", testno, nwrite, strlen(buffer)+1, errno);
	fprintf(stderr,"[test=%3d] iteration=%lu\n", testno, i);
	exit(testno);
      }

      close(fd);

      if (rename("config.lock","config")) {
	fprintf(stderr,"[test=%3d] file rename failed - errno=%d\n", testno, errno);
	fprintf(stderr,"[test=%3d] iteration=%lu\n", testno, i);
	exit(testno);
      }
      fd = open("config", 0);
      if (fd<0) {
	fprintf(stderr,"[test=%3d] file open for read failed - errno=%d\n", testno, errno);
	fprintf(stderr,"[test=%3d] iteration=%lu\n", testno, i);
	exit(testno);
      }
      memset(rbuffer, 0, 1024);
      size_t nread = (size_t) read(fd,rbuffer, 1024);
      if (nread != (strlen(buffer)+1)) {
	fprintf(stderr,"[test=%3d] file read failed - read %lu/%lu - errno=%d\n", testno, nread, strlen(buffer)+1, errno);
	fprintf(stderr,"[test=%3d] iteration=%lu\n", testno, i);
	exit(testno);
      }
      if (strncmp(buffer, rbuffer, strlen(buffer)+1)) {
	fprintf(stderr,"[test=%3d] file read wrong contents - read %lu/%lu", testno, nread, strlen(buffer)+1);
	fprintf(stderr,"[test=%3d] iteration=%lu\n", testno, i);
	exit(testno);
      }
      close(fd);
    }
    COMMONTIMING("version-rename-loop", &tm);
  }

  tm.Print();
  fprintf(stdout, "realtime = %.02f\n", tm.RealTime());
}
