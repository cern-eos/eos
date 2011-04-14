/*----------------------------------------------------------------------------*/
#include "fst/checksum/CheckSum.hh"
/*----------------------------------------------------------------------------*/
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/time.h>
#include <sys/mman.h>
/*----------------------------------------------------------------------------*/
#include "fst/checksum/Adler.hh"
#include "fst/checksum/CRC32.hh"
#include "fst/checksum/CRC32C.hh"
#include "fst/checksum/CheckSum.hh"
#include "fst/checksum/MD5.hh"
#include "fst/checksum/SHA1.hh"
#include "common/Path.hh"

EOSFSTNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
bool 
CheckSum::ScanFile(const char* path, unsigned long long &scansize, float &scantime, int rate) 
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

/*----------------------------------------------------------------------------*/
bool 
CheckSum::OpenMap(const char* mapfilepath, size_t maxfilesize, size_t blocksize, bool isRW) 
{
  struct stat buf;
  eos::common::Path cPath(mapfilepath);
  // check if the directory exists
  if (stat(cPath.GetParentPath(),&buf)) {
    return false;
  }

  BlockSize = blocksize;

  if (isRW) {
    ChecksumMapFd = open(mapfilepath, O_RDWR | O_CREAT, (mode_t) 0600);
  } else {
    ChecksumMapFd = open(mapfilepath, O_RDONLY);
  }

  if (ChecksumMapFd <0) {
    //    fprintf(stderr,"open map failed\n");
    return false;
  }

  ChecksumMapSize = ((maxfilesize / blocksize)+1) * (GetCheckSumLen());
  if (isRW) {
    if (posix_fallocate(ChecksumMapFd, 0, ChecksumMapSize)) {
      close(ChecksumMapFd);
      //      fprintf(stderr,"posix allocate failed\n");
      return false;
    }
    ChecksumMap = (char*)mmap(0, ChecksumMapSize, PROT_READ | PROT_WRITE, MAP_SHARED, ChecksumMapFd, 0);
  } else {
    ChecksumMap = (char*)mmap(0, ChecksumMapSize, PROT_READ, MAP_SHARED, ChecksumMapFd, 0);
  }

  if (ChecksumMap == MAP_FAILED) {
    close(ChecksumMapFd);
    //    fprintf(stderr,"mmap failed\n");
    return false;
  }
  return true;
}

/*----------------------------------------------------------------------------*/
bool 
CheckSum::SyncMap() 
{
  if (ChecksumMapFd) {
    if (ChecksumMap) {
      if (!msync(ChecksumMap, ChecksumMapSize,MS_SYNC)) {
        return true;
      }
    }
  }
  return false;
}

/*----------------------------------------------------------------------------*/
bool 
CheckSum::ChangeMap(size_t newsize, bool shrink) 
{
  // newsize is the real file size
  newsize = ((newsize / BlockSize)+1) * (GetCheckSumLen());
  if ((!ChecksumMapFd) || (!ChecksumMap)) 
    return false;

  if (ChecksumMapSize == newsize) {
    return true;
  }
  if ((!shrink) && (ChecksumMapSize > newsize))
    return true;

  if (!SyncMap()) {
    //    fprintf(stderr,"CheckSum:ChangeMap sync failed\n");
    return false;
  }

  if (ftruncate(ChecksumMapFd, newsize)) {
    ChecksumMapSize = 0;
    //    fprintf(stderr,"CheckSum:ChangeMap ftruncate failed\n");
    return false;
  }

  ChecksumMap = (char*)mremap(ChecksumMap, ChecksumMapSize, newsize, MREMAP_MAYMOVE);
  if (ChecksumMap == MAP_FAILED) {
    ChecksumMapSize = 0;
    return false;
  }

  ChecksumMapSize = newsize;


  return true;
}

/*----------------------------------------------------------------------------*/
bool 
CheckSum::CloseMap()
{
  SyncMap();

  if (ChecksumMapFd) {
    if (ChecksumMap) {
      if (munmap(ChecksumMap, ChecksumMapSize)) {
        close(ChecksumMapFd);
        return false;
      } else {
        close(ChecksumMapFd);
        return true;
      }
    }
    close (ChecksumMapFd);
  }
  return false;
}

/*----------------------------------------------------------------------------*/
void
CheckSum::AlignBlockExpand(off_t offset, size_t len, off_t &aligned_offset, size_t &aligned_len)
{
  aligned_offset = offset- (offset%BlockSize);
  aligned_len    = len + (offset%BlockSize);
  if (aligned_len % BlockSize) {
    aligned_len += ((BlockSize - (aligned_len%BlockSize)));
  }
  return;
}

/*----------------------------------------------------------------------------*/
void
CheckSum::AlignBlockShrink(off_t offset, size_t len, off_t &aligned_offset, size_t &aligned_len)
{
  off_t start = offset;
  off_t stop  = offset + len;

  if (start%BlockSize) {
    start = start + BlockSize - (start%BlockSize);
  }

  if (stop%BlockSize) {
    stop = stop - (stop%BlockSize);
  }

  aligned_offset = start;
  aligned_len = (stop-start);

  return;
}

/*----------------------------------------------------------------------------*/
bool 
CheckSum::AddBlockSum(off_t offset, const char* buffer, size_t len)
{
  // --------------------------------------------------------------------------------------
  // !this only calculates the checksum on full blocks, not matching edge is not calculated
  // --------------------------------------------------------------------------------------

  off_t aligned_offset;
  size_t aligned_len;

  AlignBlockShrink(offset, len, aligned_offset, aligned_len);

  if (aligned_len) {
    off_t endoffset = aligned_offset + aligned_len;
    off_t position=offset;
    const char* bufferptr=buffer + (aligned_offset-offset);
    // loop over all blocks
    for (position = aligned_offset; position < endoffset; position += BlockSize) {
      // checksum this block
      Reset();
      Add(bufferptr, BlockSize,0);
      Finalize();
      // write the checksum page
      if (!SetXSMap(position))
        return false;
      nXSBlocksWritten++;
      bufferptr += BlockSize;
    }
  }
  return true;
}

/*----------------------------------------------------------------------------*/
bool 
CheckSum::CheckBlockSum(off_t offset, const char* buffer, size_t len)
{
  // --------------------------------------------------------------------------------------
  // !this only checks the checksum on full blocks, not matching edge is not calculated
  // --------------------------------------------------------------------------------------

  off_t aligned_offset;
  size_t aligned_len;

  AlignBlockShrink(offset, len, aligned_offset, aligned_len);

  if (aligned_len) {
    off_t endoffset = aligned_offset + aligned_len;
    off_t position=offset;
    const char* bufferptr=buffer + (aligned_offset-offset);
    // loop over all blocks
    for (position = aligned_offset; position < endoffset; position += BlockSize) {
      // checksum this block
      Reset();
      Add(bufferptr, BlockSize,0);
      Finalize();
      // compare the checksum page
      if (!VerifyXSMap(position))
        return false;
      nXSBlocksChecked++;
      bufferptr += BlockSize;
    }
  }
  return true;
}

/*----------------------------------------------------------------------------*/
bool 
CheckSum::SetXSMap(off_t offset) 
{
  if (!ChangeMap((offset+BlockSize), false))
    return false;

  off_t mapoffset = (offset / BlockSize) * GetCheckSumLen();
  
  int len;
  const char* cks = GetBinChecksum(len);
  for (int i=0; i < len; i++) {
    ChecksumMap[i+mapoffset] = cks[i];
  }
  return true;
}

/*----------------------------------------------------------------------------*/
bool 
CheckSum::VerifyXSMap(off_t offset) 
{
  if (!ChangeMap((offset+BlockSize), false))
    return false;
  off_t mapoffset = (offset / BlockSize) * GetCheckSumLen();
  
  int len;
  const char* cks = GetBinChecksum(len);
  for (int i=0; i < len; i++) {
    if (ChecksumMap[i+mapoffset] && (ChecksumMap[i+mapoffset] != cks[i]))
      return false;
  }
  return true;
}

/*----------------------------------------------------------------------------*/
bool 
CheckSum::AddBlockSumHoles(int fd)
{
  // ---------------------------------------------------------------------------
  // ! this routine (re-)computes all the checksums for blocks with '0' checksum
  // ! you have to call this after OpenMap and before CloseMap
  // ---------------------------------------------------------------------------

  struct stat buf;
  if (fstat(fd,&buf)) {
    //    fprintf(stderr,"AddBlockSumHoles: stat failed\n");
    return false;
  } else {

    if (!ChangeMap(buf.st_size)) {
      //      fprintf(stderr,"AddBlockSumHoles: changemap failed %llu\n", buf.st_size);
      return false;
    }

    char* buffer = (char*) malloc(BlockSize);
    if (buffer) {
      size_t len = GetCheckSumLen();
      size_t nblocks = ChecksumMapSize / len;
      bool iszero;
      for (size_t i = 0; i < nblocks; i++) {
        iszero = true;
        for (size_t n = 0; n < len; n++) {
          //          fprintf(stderr,"%ld %ld %llu %d\n", i, n, (i*len)+n,  ChecksumMap[(i*len)+n]);
          if (ChecksumMap[ (i*len)+ n ]) {
            iszero=false;
            break;
          }
        }
        if (iszero) {
          int nrbytes = pread(fd,buffer,BlockSize, i*BlockSize);
          if (nrbytes <0) {
            continue;
          }
          if (nrbytes < (int)BlockSize) {
            // fill the last block
            memset(buffer+nrbytes, 0, BlockSize-nrbytes);
            nrbytes = BlockSize;
          }

          if (!AddBlockSum( i*BlockSize, buffer, nrbytes)) {
            //            fprintf(stderr,"AddBlockSumHoles: checksumming failed\n");
            
            free(buffer);
            return false;
          }
          nXSBlocksWrittenHoles++;
        }
      }
      free (buffer);
      return true;
    } else {
      //      fprintf(stderr,"AddBlockSumHoles: malloc failed\n");
      return false;
    }
  }  
}
  
/*----------------------------------------------------------------------------*/
EOSFSTNAMESPACE_END
