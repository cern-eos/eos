/*----------------------------------------------------------------------------*/
#include "XrdFstOfs/XrdFstOfsChecksum.hh"
/*----------------------------------------------------------------------------*/
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
/*----------------------------------------------------------------------------*/

char buffer[65536];

bool 
XrdFstOfsChecksum::ScanFile(const char* path) 
{
  int fd = open(path, O_RDONLY);
  if (fd<0) {
    return false;
  }

  Reset();

  int nread=0;
  off_t offset = 0;

  do {
    errno = 0;
    nread = read(fd,buffer,sizeof(buffer));
    if (nread<0) {
      close(fd);
      return false;
    }
    Add(buffer, nread, offset);
    offset += nread;
  } while (nread == sizeof(buffer));
  Finalize();  
  close(fd);
  return true;
}
