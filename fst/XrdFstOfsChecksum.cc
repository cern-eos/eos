/*----------------------------------------------------------------------------*/
#include "XrdFstOfs/XrdFstOfsChecksum.hh"
/*----------------------------------------------------------------------------*/
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/time.h>
/*----------------------------------------------------------------------------*/

bool 
XrdFstOfsChecksum::ScanFile(const char* path, unsigned long long &scansize, float &scantime, int rate) 
{
  static int buffersize=1024*1024;
  struct timezone tz;
  struct timeval  opentime;
  struct timeval  currenttime;
  scansize = 0;
  scantime = 0;

  gettimeofday(&opentime,&tz);

  int fd = open(path, O_RDONLY);
  if (fd<0) {
    return false;
  }

  Reset();

  int nread=0;
  off_t offset = 0;

  char* buffer = (char*) malloc(buffersize);
  if (!buffer)
    return false;

  do {
    errno = 0;
    nread = read(fd,buffer,buffersize);
    if (nread<0) {
      close(fd);
      free(buffer);
      return false;
    }
    Add(buffer, nread, offset);
    offset += nread;

    if (rate) {
      // regulate the verification rate
      gettimeofday(&currenttime,&tz);
      scantime = ( ((currenttime.tv_sec - opentime.tv_sec)*1000.0) + ((currenttime.tv_usec - opentime.tv_usec)/1000.0 ));
      float expecttime = (1.0 * offset / rate) / 1000.0;
      if (expecttime > scantime) {
	usleep(1000.0*(expecttime - scantime));
      }
    }
  } while (nread == buffersize);

  gettimeofday(&currenttime,&tz);
  scantime = ( ((currenttime.tv_sec - opentime.tv_sec)*1000.0) + ((currenttime.tv_usec - opentime.tv_usec)/1000.0 ));
  scansize = (unsigned long long) offset;

  Finalize();  
  close(fd);
  free(buffer);
  return true;
}
