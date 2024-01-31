#include <string>
#include <iostream>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstdlib>
#include <vector>
#include <ctime>

void usage()
{
  fprintf(stderr, "ioverify <path> <nfiles> <nverify>\n");
  fprintf(stderr,
          " example: ioverify /var/tmp/ 2048 0    # creates 2048 1MB test pattern files under /var/tmp/\n");
  fprintf(stderr,
          "          ioverify /var/tmp/ 2048 1000 # runs 1000 random verifications on all 2048 files under /var/tmp/\n");
  exit(-1);
}

int main(int argc, char* argv[])
{
  int retc = 0;

  if (argc != 4) {
    usage();
  }

  std::string prefix = argv[1];
  size_t nfiles = std::strtol(argv[2], 0, 10);
  size_t nverify = std::strtol(argv[3], 0, 10);
  fprintf(stderr, "running: prefix=%s nfiles=%lu nverify=%lu\n",
          prefix.c_str(),
          nfiles,
          nverify);

  if (nverify == 0) {
    // this is the creation of pattern files
    for (size_t n = 0; n < nfiles; n++) {
      unsigned char buffer[64 * 1024];

      for (size_t i = 0; i < 65536; ++i) {
        buffer[i] = (n + i) % 256;
      }

      std::string path = prefix;
      path += "/pattern.";
      path += std::to_string(n);
      int fd = creat(path.c_str(), S_IRWXU);

      if (fd > 0) {
        for (size_t l = 0; l < 16; l++) {
          ssize_t nwr = write(fd, buffer, 64 * 1024);

          if (nwr != 64 * 1024) {
            fprintf(stderr, "error: failed to write loop=%lu nrw=%ld\n", l, nwr);
            exit(-1);
          }
        }

        close(fd);
        fprintf(stderr, "ok: wrote pattern file path='%s' pattern-tpe=%lu\n",
                path.c_str(), n);
      } else {
        fprintf(stderr, "error: failed to create path='%s' errno=%d\n", path.c_str(),
                errno);
        exit(-1);
      }
    }
  } else {
    std::srand(std::time(nullptr));
    std::vector<int> fds;

    for (size_t i = 0; i < nfiles; i++) {
      std::string path = prefix;
      path += "/pattern.";
      path += std::to_string(i);
      fds.push_back(open(path.c_str(), 0));

      if (fds[i] < 1) {
        fprintf(stderr, "error: open failed for path='%s' errno=%d\n", path.c_str(),
                errno);
        retc = -1;
      }
    }

    for (size_t v = 0 ; v < nverify; v++) {
      for (size_t i = 0; i < nfiles; i++) {
        size_t size = std::rand() % (1024);
        off_t offset = std::rand() % ((1024 * 1024) - size);
        unsigned char buffer[1024];
        size_t nr = pread(fds[i], buffer, size, offset);

        if (nr != size) {
          fprintf(stderr, "error: failed to read file=%lu offset=%lu size=%lu read=%lu\n",
                  i, offset, size, nr);
          retc = -1;
        }

        // verify pattern
        for (size_t l = 0; l < size; l++) {
          if (buffer[l] != ((offset + l + i) % 256)) {
            fprintf(stderr,
                    "error: pattern for file=%lu offset=%lu should be %x but we got %x\n", i,
                    offset + l, (unsigned int)(offset + l + i) % 256, buffer[l]);
            retc = -1;
          }
        }
      }
    }

    for (size_t i = 0; i < nfiles; i++) {
      close(fds[i]);
    }
  }

  exit(retc);
}
