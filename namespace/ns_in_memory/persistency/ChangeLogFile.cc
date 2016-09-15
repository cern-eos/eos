/************************************************************************
 * eos - the cern disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   ChangeLog like store
//------------------------------------------------------------------------------

#include "namespace/ns_in_memory/persistency/ChangeLogFile.hh"
#include "namespace/ns_in_memory/persistency/ChangeLogConstants.hh"
#include "namespace/utils/SmartPtrs.hh"
#include "namespace/utils/DataHelper.hh"
#include "XrdSys/XrdSysPthread.hh"
#include "XrdSys/XrdSysTimer.hh"

#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/uio.h>
#ifdef __linux__
#include <sys/inotify.h>
#endif
#include <poll.h>
#include <stdint.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <iomanip>
#include <stdio.h>
#include <fcntl.h>

#define CHANGELOG_MAGIC 0x45434847
#define RECORD_MAGIC    0x4552

namespace eos
{
//----------------------------------------------------------------------------
// Check the header - returns flags number
//----------------------------------------------------------------------------
static uint32_t checkHeader(int fd, const std::string& name)
{
  uint32_t magic;
  uint32_t flags;

  if (read(fd, &magic, 4) != 4) {
    MDException ex(errno);
    ex.getMessage() << "Unable to read the magic number from: " << name;
    throw ex;
  }

  if (magic != CHANGELOG_MAGIC) {
    MDException ex(EFAULT);
    ex.getMessage() << "Unrecognized file type: " << name;
    throw ex;
  }

  if (read(fd, &flags, 4) != 4) {
    MDException ex(errno);
    ex.getMessage() << "Unable to read the version number from: " << name;
    throw ex;
  }

  return flags;
}

//---------------------------------------------------------------------------
// Open the log file
//----------------------------------------------------------------------------
void ChangeLogFile::open(const std::string& name, int flags,
                         uint16_t contentFlag)
{
  //--------------------------------------------------------------------------
  // Check if the file is open already
  //--------------------------------------------------------------------------
  if (pIsOpen) {
    MDException ex(EFAULT);
    ex.getMessage() << "Changelog file is already open";
    throw ex;
  }

  //--------------------------------------------------------------------------
  // Check if the open flags are conflicting
  //--------------------------------------------------------------------------
  if ((flags & ReadOnly) &&
      ((flags & Append) || (flags & Truncate) || (flags & Create))) {
    MDException ex(EFAULT);
    ex.getMessage() << "Conflicting open flags";
    throw ex;
  }

  //--------------------------------------------------------------------------
  // Check open flags
  //--------------------------------------------------------------------------
  int openFlags = 0;

  if (flags & ReadOnly) {
    openFlags = O_RDONLY;
  } else {
    openFlags = O_RDWR;
  }

  //--------------------------------------------------------------------------
  // Try to open the file
  //--------------------------------------------------------------------------
  int fd = ::open(name.c_str(), openFlags);
  FileSmartPtr fdPtr(fd);

  //--------------------------------------------------------------------------
  // Check the format
  //--------------------------------------------------------------------------
  if (fd >= 0) {
    uint32_t fileFlags = checkHeader(fd, name);
    uint8_t  version   = 0;
    decodeHeaderFlags(fileFlags, version, pContentFlag, pUserFlags);

    if (version == 0 || version > 1) {
      MDException ex(EFAULT);
      ex.getMessage() << "Unsupported version: " << name;
      throw ex;
    }

    if (contentFlag && contentFlag != pContentFlag) {
      MDException ex(EFAULT);
      ex.getMessage() << "Log file exists: " << name << " ";
      ex.getMessage() << "and the requested content flag (0x";
      ex.getMessage() << std::setbase(16) << contentFlag << ") does not ";
      ex.getMessage() << "match the one read from file (0x";
      ex.getMessage() << std::setbase(16) << pContentFlag << ")";
      throw ex;
    }

    //------------------------------------------------------------------------
    // Can we append?
    //------------------------------------------------------------------------
    if (!(flags & Append) && !(flags & ReadOnly)) {
      MDException ex(EFAULT);
      ex.getMessage() << "The log file exists: " << name << ": ";
      ex.getMessage() << "but neither Append nor ReadOnly flag is specified";
      throw ex;
    }

    //------------------------------------------------------------------------
    // Truncate if needed
    //------------------------------------------------------------------------
    if ((flags & Truncate) && ::ftruncate(fd, getFirstOffset()) != 0) {
      MDException ex(EFAULT);
      ex.getMessage() << "Unable to truncate: " << name << ": ";
      ex.getMessage() << strerror(errno);
      throw ex;
    }

#ifdef __linux__

    if (flags & ReadOnly) {
      //----------------------------------------------------------------------
      // Initialize inotify if needed
      //----------------------------------------------------------------------
      pInotifyFd = inotify_init();

      if (pInotifyFd < 0) {
        MDException ex(errno);
        ex.getMessage() << "Unable to initialize inotify: " << name << ": ";
        ex.getMessage() << strerror(errno);
        throw ex;
      }

      pWatchFd = inotify_add_watch(pInotifyFd, name.c_str(), IN_MODIFY);

      if (pWatchFd < 0) {
        cleanUpInotify();
        MDException ex(errno);
        ex.getMessage() << "Unable to add watch event IN_MODIFY for inotify: ";
        ex.getMessage() << name << ": " << strerror(errno);
        throw ex;
      }

      //----------------------------------------------------------------------
      // Make the descriptor non-blocking
      //----------------------------------------------------------------------
      int savedFlags = fcntl(pInotifyFd, F_GETFL);

      if (savedFlags == -1) {
        cleanUpInotify();
        MDException ex(errno);
        ex.getMessage() << "Unable to get the flags of inotify descriptor: ";
        ex.getMessage() << strerror(errno);
        throw ex;
      }

      if (fcntl(pInotifyFd, F_SETFL, savedFlags | O_NONBLOCK) != 0) {
        cleanUpInotify();
        MDException ex(errno);
        ex.getMessage() << "Unable to make the inotify descriptor ";
        ex.getMessage() << "non-blocking: " << strerror(errno);
        throw ex;;
      }
    }

#endif
    //------------------------------------------------------------------------
    // Move to the end
    //------------------------------------------------------------------------
    lseek(fd, 0, SEEK_END);
    fdPtr.release();
    pFd = fd;
    pIsOpen  = true;
    pVersion = version;
    pFileName = name;
    return;
  }

  //--------------------------------------------------------------------------
  // Create the file if need be
  //--------------------------------------------------------------------------
  if (!(flags & Create)) {
    MDException ex(EFAULT);
    ex.getMessage() << "File does not exist and Create flag is absent: " << name;
    throw ex;
  }

  fd = ::open(name.c_str(), O_CREAT | O_EXCL | O_RDWR, 0644);
  fdPtr.grab(fd);

  //--------------------------------------------------------------------------
  // Check if the file was successfuly created
  //--------------------------------------------------------------------------
  if (fd == -1) {
    MDException ex(EFAULT);
    ex.getMessage() << "Unable to create changelog file " << name;
    ex.getMessage() << ": " << strerror(errno);
    throw ex;
  }

  //--------------------------------------------------------------------------
  // Write the magic number and version
  //--------------------------------------------------------------------------
  uint32_t magic = CHANGELOG_MAGIC;

  if (write(fd, &magic, 4) != 4) {
    MDException ex(errno);
    ex.getMessage() << "Unable to write magic number: " << name;
    throw ex;
  }

  uint8_t  version = 1;
  uint32_t tmp;
  uint32_t fileFlags = 0;
  pContentFlag = contentFlag;
  fileFlags |= version;
  tmp = contentFlag;
  fileFlags |= (tmp << 8);

  if (write(fd, &fileFlags, 4) != 4) {
    MDException ex(errno);
    ex.getMessage() << "Unable to write the flags: " << name;
    throw ex;
  }

  fdPtr.release();
  pFd        = fd;
  pIsOpen    = true;
  pVersion   = 1;
  pSeqNumber = 0;
}

//----------------------------------------------------------------------------
// Close the log
//----------------------------------------------------------------------------
void ChangeLogFile::close()
{
  if (pFd != -1) {
    ::close(pFd);
    pIsOpen = false;
  }

  cleanUpInotify();
}

//----------------------------------------------------------------------------
// Clean up inotify
//----------------------------------------------------------------------------
void ChangeLogFile::cleanUpInotify()
{
#ifdef __linux__

  if (pWatchFd != -1) {
    inotify_rm_watch(pInotifyFd, pWatchFd);
    pWatchFd   = -1;
  }

  if (pInotifyFd != -1) {
    ::close(pInotifyFd);
    pInotifyFd = -1;
  }

#endif
}

//----------------------------------------------------------------------------
// Sync the buffers to disk
//----------------------------------------------------------------------------
void ChangeLogFile::sync()
{
  if (!pIsOpen) {
    return;
  }

  if (fsync(pFd) != 0) {
    MDException ex(errno);
    ex.getMessage() << "Unable to sync the changelog file: ";
    ex.getMessage() << strerror(errno);
    throw ex;
  }
}

//----------------------------------------------------------------------------
// Store the record in the log
//----------------------------------------------------------------------------
uint64_t ChangeLogFile::storeRecord(char type, Buffer& record)
{
  if (!pIsOpen) {
    MDException ex(EFAULT);
    ex.getMessage() << "Changelog file is not open";
    throw ex;
  }

  //--------------------------------------------------------------------------
  // Allign the buffer to 4 bytes and calculate the checksum
  //--------------------------------------------------------------------------
  uint32_t nsize = record.size();
  nsize = (nsize + 3) >> 2 << 2;

  if (nsize > 65535) {
    MDException ex(EFAULT);
    ex.getMessage() << "Record too big";
    throw ex;
  }

  record.resize(nsize);
  //--------------------------------------------------------------------------
  // Initialize the data and calculate the checksum
  //--------------------------------------------------------------------------
  uint16_t size   = record.size();
  uint64_t offset = ::lseek(pFd, 0, SEEK_END);
  uint64_t seq    = 0;
  uint16_t magic  = RECORD_MAGIC;
  uint32_t opts   = type; // occupy the first byte (little endian)
  // the rest is unused for the moment
  uint32_t chkSum = DataHelper::computeCRC32(&seq, 8);
  chkSum = DataHelper::updateCRC32(chkSum, &opts, 4);
  chkSum = DataHelper::updateCRC32(chkSum,
                                   record.getDataPtr(),
                                   record.getSize());
  //--------------------------------------------------------------------------
  // Store the data
  //--------------------------------------------------------------------------
  iovec vec[7];
  vec[0].iov_base = &magic;
  vec[0].iov_len = 2;
  vec[1].iov_base = &size;
  vec[1].iov_len = 2;
  vec[2].iov_base = &chkSum;
  vec[2].iov_len = 4;
  vec[3].iov_base = &seq;
  vec[3].iov_len = 8;
  vec[4].iov_base = &opts;
  vec[4].iov_len = 4;
  vec[5].iov_base = (void*)record.getDataPtr();
  vec[5].iov_len = record.size();
  vec[6].iov_base = &chkSum;
  vec[6].iov_len = 4;

  if (writev(pFd, vec, 7) != (ssize_t)(24 + record.size())) {
    MDException ex(errno);
    ex.getMessage() << "Unable to write the record data at offset 0x";
    ex.getMessage() << std::setbase(16) << offset << "; ";
    ex.getMessage() << strerror(errno);
    throw ex;
  }

  return offset;
}

//----------------------------------------------------------------------------
// Read the record at given offset
//----------------------------------------------------------------------------
uint8_t ChangeLogFile::readRecord(uint64_t offset, Buffer& record)
{
  if (!pIsOpen) {
    MDException ex(EFAULT);
    ex.getMessage() << "Read: Changelog file is not open";
    throw ex;
  }

  //--------------------------------------------------------------------------
  // Read first part of the record
  //--------------------------------------------------------------------------
  uint16_t* magic;
  uint16_t* size;
  uint32_t* chkSum1;
  uint64_t* seq;
  uint32_t  chkSum2;
  uint8_t*  type;
  char      buffer[20];

  if (pread(pFd, buffer, 20, offset) != 20) {
    MDException ex(errno);
    ex.getMessage() << "Read: Error reading at offset: " << offset;
    throw ex;
  }

  magic   = (uint16_t*)(buffer);
  size    = (uint16_t*)(buffer + 2);
  chkSum1 = (uint32_t*)(buffer + 4);
  seq     = (uint64_t*)(buffer + 8);
  type    = (uint8_t*)(buffer + 16);

  //--------------------------------------------------------------------------
  // Check the consistency
  //--------------------------------------------------------------------------
  if (*magic != RECORD_MAGIC) {
    MDException ex(EFAULT);
    ex.getMessage() << "Read: Record's magic number is wrong at offset: " << offset;
    throw ex;
  }

  //--------------------------------------------------------------------------
  // Read the second part of the buffer
  //--------------------------------------------------------------------------
  record.resize(*size + 4, 0);

  if (pread(pFd, record.getDataPtr(), *size + 4, offset + 20) != *size + 4) {
    MDException ex(errno);
    ex.getMessage() << "Read: Error reading at offset: " << offset + 9;
    throw ex;
  }

  record.grabData(record.size() - 4, &chkSum2, 4);
  record.resize(*size);
  //--------------------------------------------------------------------------
  // Check the checksum
  //--------------------------------------------------------------------------
  uint32_t crc = DataHelper::computeCRC32(seq, 8);
  crc = DataHelper::updateCRC32(crc, (buffer + 16), 4); // opts
  crc = DataHelper::updateCRC32(crc, record.getDataPtr(), record.getSize());

  if (*chkSum1 != crc || *chkSum1 != chkSum2) {
    MDException ex(EFAULT);
    ex.getMessage() << "Read: Record's checksums do not match.";
    throw ex;
  }

  return *type;
}

//----------------------------------------------------------------------------
// Scan all the records in the changelog file
//----------------------------------------------------------------------------
uint64_t ChangeLogFile::scanAllRecords(ILogRecordScanner* scanner,
                                       bool autorepair)
{
  return scanAllRecordsAtOffset(scanner, getFirstOffset(), autorepair);
}

//----------------------------------------------------------------------------
// Scan all the records in the changelog file starting from a given
// offset
//----------------------------------------------------------------------------
uint64_t ChangeLogFile::scanAllRecordsAtOffset(ILogRecordScanner* scanner,
    uint64_t           startOffset,
    bool               autorepair)
{
  if (!pIsOpen) {
    MDException ex(EFAULT);
    ex.getMessage() << "Scan: Changelog file is not open";
    throw ex;
  }

  //--------------------------------------------------------------------------
  // Get the offset information
  //--------------------------------------------------------------------------
  off_t end = ::lseek(pFd, 0, SEEK_END);

  if (end == -1) {
    MDException ex(EFAULT);
    ex.getMessage() << "Scan: Unable to find the end of the log file: ";
    ex.getMessage() << strerror(errno);
    throw ex;
  }

  off_t offset = ::lseek(pFd, startOffset, SEEK_SET);

  if (offset != (off_t)startOffset) {
    MDException ex(EFAULT);
    ex.getMessage() << "Scan: Unable to find the record data at offset 0x";
    ex.getMessage() << std::setbase(16) << startOffset << "; ";
    ex.getMessage() << strerror(errno);
    throw ex;
  }

  //--------------------------------------------------------------------------
  // Read all the records
  //--------------------------------------------------------------------------
  uint8_t          type;
  Buffer           data;
  size_t progress = 0;
  time_t start_time = time(0);
  time_t now = start_time;
  std::string fname = pFileName;
  fname.erase(0, pFileName.rfind("/") + 1);

  while (offset < end) {
    bool proceed = false;
    bool readerror = false;

    try {
      type = readRecord(offset, data);
      proceed = scanner->processRecord(offset, type, data);
      offset += data.size();
      offset += 24;
    } catch (MDException& e) {
      readerror = true;
    }

    if (readerror) {
      if (autorepair) {
        // evt. try to skip this record
        off_t newOffset = ChangeLogFile::findRecordMagic(pFd, offset + 4, (off_t)0);

        if (newOffset == (off_t) - 1) {
          char msg[4096];
          snprintf(msg, 4096,
                   "error: definite corruption in file changelog after offset %llx\n",
                   (long long)offset);
          addWarningMessage(msg);
          MDException ex(EIO);
          ex.getMessage() <<
                          "error: Changelog file has a corruption at end of file - check synchronization or repair the file manually";
          throw ex;
        }

        if ((newOffset - offset) < 1024) {
          char msg[4096];
          snprintf(msg, 4096,
                   "error: discarded block from offset [ %llx <=> %llx ] [ len=%lu ] \n",
                   (long long)offset, (long long)newOffset, (unsigned long)(newOffset - offset));
          addWarningMessage(msg);
          offset = newOffset;
          continue;
        } else {
          char msg[4096];
          snprintf(msg, 4096,
                   "error: large block corruption at offset [ %llx <=> %llx ] [ len=%lu ] \n",
                   (long long)offset, (long long)newOffset, (unsigned long)(newOffset - offset));
          addWarningMessage(msg);
          MDException ex(EIO);
          ex.getMessage() <<
                          "error: Changelog file has a >1kb corruption - too risky - repair the file manually";
          throw ex;
        }
      } else {
        char msg[4096];
        snprintf(msg, 4096, "error: corruption in file changelog at offset %llx\n",
                 (long long)offset);
        addWarningMessage(msg);
        MDException ex(EIO);
        ex.getMessage() <<
                        "error: Changelog file has corruption - autorepair is disabled";
        throw ex;
      }
    }

    if (!proceed) {
      break;
    }

    now = time(0);

    if ((100.0 * offset / end) > progress) {
      double estimate = (1 + end - offset) / ((1.0 * offset /
                                              (now + 1 - start_time)));

      if (progress == 0) {
        fprintf(stderr, "PROGRESS [ scan %-64s ] %02u%% estimate none \n",
                fname.c_str(), (unsigned int)progress);
      } else {
        fprintf(stderr, "PROGRESS [ scan %-64s ] %02u%% estimate %3.02fs\n",
                fname.c_str(), (unsigned int)progress, estimate);
      }

      progress += 5;
    }
  }

  now = time(0);
  fprintf(stderr, "ALERT    [ %-64s ] finished in %ds\n", fname.c_str(),
          (int)(now - start_time));
  return offset;
}

//----------------------------------------------------------------------------
// Follow a file
//----------------------------------------------------------------------------
uint64_t ChangeLogFile::follow(ILogRecordScanner* scanner,
                               uint64_t           startOffset)
{
  //--------------------------------------------------------------------------
  // Check if the file is open
  //--------------------------------------------------------------------------
  if (!pIsOpen) {
    MDException ex(EFAULT);
    ex.getMessage() << "Follow: Changelog file is not open";
    throw ex;
  }

  //--------------------------------------------------------------------------
  // Off we go - we only exit if an error occurs
  //--------------------------------------------------------------------------
  Descriptor   fd(pFd);
  off_t        offset = startOffset;
  uint16_t*    magic;
  uint16_t*    size;
  uint32_t*    chkSum1;
//    uint64_t    *seq;
  uint32_t     chkSum2;
  uint8_t*     type;
  char         buffer[20];
  Buffer       record;

  while (1) {
    //------------------------------------------------------------------------
    // Read the header
    //------------------------------------------------------------------------
    unsigned bytesRead = 0;

    try {
      bytesRead = fd.tryRead(buffer, 20, offset);
    } catch (DescriptorException& e) {
      MDException ex(errno);
      ex.getMessage() << "Follow: Error reading at offset: " << offset << ": ";
      ex.getMessage() << e.getMessage().str();
      throw ex;
    }

    if (bytesRead != 20) {
      return offset;
    }

    magic   = (uint16_t*)(buffer);
    size    = (uint16_t*)(buffer + 2);
    chkSum1 = (uint32_t*)(buffer + 4);
//        seq     = (uint64_t*)(buffer+8);
    type    = (uint8_t*)(buffer + 16);

    //------------------------------------------------------------------------
    // Check the consistency
    //------------------------------------------------------------------------
    if (*magic != RECORD_MAGIC) {
      MDException ex(EFAULT);
      ex.getMessage() << "Follow: Record's magic number is wrong at offset: "
                      << offset;
      throw ex;
    }

    //------------------------------------------------------------------------
    // Read the second part of the buffer
    //------------------------------------------------------------------------
    record.resize(*size + 4, 0);
    bytesRead = 0;

    try {
      bytesRead = fd.tryRead(record.getDataPtr(), *size + 4, offset + 20);
    } catch (DescriptorException& e) {
      MDException ex(errno);
      ex.getMessage() << "Follow: Error reading at offset: " << offset + 9;
      ex.getMessage() << ": " << e.getMessage().str();
      throw ex;
    }

    if ((ssize_t)bytesRead != (*size + 4)) {
      return offset;
    }

    record.grabData(record.size() - 4, &chkSum2, 4);
    record.resize(*size);

    //------------------------------------------------------------------------
    // Check the checksum
    //------------------------------------------------------------------------
    if (*chkSum1 != chkSum2) {
      // evt. try to skip this record
      off_t newOffset = ChangeLogFile::findRecordMagic(pFd, offset + 4, (off_t)0);

      if (newOffset == (off_t) - 1) {
        MDException ex(EFAULT);
        ex.getMessage() <<
                        "Follow: Record's checksums do not match - unable to skip record";
        throw ex;
      }

      if ((newOffset - offset) < 1024) {
        char msg[4096];
        snprintf(msg, 4096,
                 "error: discarded block from offset [ %llx <=> %llx ] [ len=%lu ] \n",
                 (long long)offset, (long long)newOffset, (unsigned long)(newOffset - offset));
        addWarningMessage(msg);
        offset = newOffset;
        continue;
      } else {
        MDException ex(EFAULT);
        ex.getMessage() <<
                        "Follow: Record's checksums do not match - need to skip more than 1k";
        throw ex;
      }
    }

    //------------------------------------------------------------------------
    // Call the listener and clean up
    //------------------------------------------------------------------------
    scanner->processRecord(offset, *type, record);
    offset += record.size();
    offset += 24;
    record.clear();
  }
}

//----------------------------------------------------------------------------
// Find the record header starting at offset - the log files are aligned
// to 4 bytes so the magic should be at [(offset mod 4) == 0]
//----------------------------------------------------------------------------
off_t ChangeLogFile::findRecordMagic(int fd, off_t offset, off_t offsetLimit)
{
  uint32_t magic = 0;

  while (1) {
    if (pread(fd, &magic, 4, offset) != 4) {
      return -1;
    }

    if ((magic & 0x0000ffff) == RECORD_MAGIC) {
      return offset;
    }

    offset += 4;

    if (offsetLimit && offset >= offsetLimit) {
      return -1;
    }
  }

  return -1;
}

//----------------------------------------------------------------------------
// Wait for a modification event in a changelog file with inotify or if not
// available wait <polltime> micro seconds
//----------------------------------------------------------------------------
void ChangeLogFile::wait(uint32_t polltime)
{
  //--------------------------------------------------------------------------
  // We're on linux so we can try inotify if it initialized right.
  //--------------------------------------------------------------------------
#ifdef __linux__
  if (pInotifyFd  >= 0 && pWatchFd >= 0) {
    //------------------------------------------------------------------------
    // Wait 500 milisecs for the new data, if there is none by that time
    // just exit
    //------------------------------------------------------------------------
    pollfd pollDesc;
    memset(&pollDesc, 0, sizeof(pollfd));
    pollDesc.events |= (POLLIN | POLLPRI);
    pollDesc.fd     = pInotifyFd;

    while (1) {
      int status = poll(&pollDesc, 1, 500);

      if (status < 0 && errno != EINTR) {
        MDException ex(EFAULT);
        ex.getMessage() << "Wait: inotify poll failed: ";
        ex.getMessage() << strerror(errno);
        throw ex;
      }

      if (status == 0) {
        return;
      }

      if (status > 0) {
        break;
      }
    }

    //------------------------------------------------------------------------
    // Read all the queued events.
    // We configured inotify to tell us about one type of event on one
    // descriptor so we don't really care about looking inside the event
    // struct.
    //------------------------------------------------------------------------
    while (1) {
      inotify_event event;
      int status = read(pInotifyFd, &event, sizeof(inotify_event));

      if (status <= 0) {
        if (errno == EINTR) {
          continue;
        }

        if (errno == EAGAIN || errno == EWOULDBLOCK) {
          return;
        }

        MDException ex(errno);
        ex.getMessage() << "Wait: inotify read failed: ";
        ex.getMessage() << strerror(errno);
        throw ex;
      }
    }
  }

#else
  XrdSysTimer sleeper;
  sleeper.Wait(polltime / 1000);
#endif
}

//----------------------------------------------------------------------------
// Adjust size
//----------------------------------------------------------------------------
off_t guessSize(int fd, off_t offset, Buffer& buffer, off_t startHint = 0)
{
  //--------------------------------------------------------------------------
  // Find a magic number of the next record
  //--------------------------------------------------------------------------
  if (startHint && startHint - offset >= 70000) {
    return -1;
  }

  if (!startHint) {
    startHint = offset + 24;
  }

  off_t newOffset = ChangeLogFile::findRecordMagic(fd, startHint, offset + 70000);

  if (newOffset == (off_t) - 1) {
    return -1;
  }

  //--------------------------------------------------------------------------
  // Is the new size correct?
  //--------------------------------------------------------------------------
  off_t newSize = newOffset - offset - 24;

  if (newSize > 65535 || newSize < 0) {
    return -1;
  }

  buffer.resize(newSize);

  if (pread(fd, buffer.getDataPtr(), newSize, offset + 20) != newSize) {
    return -1;
  }

  return newSize;
}

//----------------------------------------------------------------------------
// Reconstruct record at offset
//----------------------------------------------------------------------------
static off_t reconstructRecord(int fd, off_t offset,
                               off_t fsize, Buffer& buffer, uint8_t& type,
                               LogRepairStats& stats)
{
  uint16_t* magic;
  uint16_t  size;
  uint32_t* chkSum1;
  uint64_t* seq;
  uint32_t  chkSum2;
  char      buff[20];

  //--------------------------------------------------------------------------
  // Read the record header data and second checksum
  //--------------------------------------------------------------------------
  if (pread(fd, buff, 20, offset) != 20) {
    return -1;
  }

  magic   = (uint16_t*)(buff);
  size    = *(uint16_t*)(buff + 2);
  chkSum1 = (uint32_t*)(buff + 4);
  seq     = (uint64_t*)(buff + 8);
  type    = *(uint8_t*)(buff + 16);
  uint32_t crcHead = DataHelper::computeCRC32(seq, 8);
  crcHead = DataHelper::updateCRC32(crcHead, (buff + 16), 4); // opts
  //--------------------------------------------------------------------------
  // Try to reading the record data - if the read fails then the size
  // may be incorrect, so try to compensate
  //--------------------------------------------------------------------------
  buffer.resize(size);

  if (pread(fd, buffer.getDataPtr(), size, offset + 20) != size) {
    ++stats.fixedWrongSize;
    off_t offSize;
    offSize = guessSize(fd, offset, buffer);

    if (offSize == (off_t) - 1) {
      return -1;
    }

    size = offSize;
  }

  if (pread(fd, &chkSum2, 4, offset + 20 + size) != 4) {
    return -1;
  }

  //--------------------------------------------------------------------------
  // The magic wrong
  //--------------------------------------------------------------------------
  bool wrongMagic = false;

  if (*magic != RECORD_MAGIC) {
    wrongMagic = true;
  }

  //--------------------------------------------------------------------------
  // Check the sums
  //--------------------------------------------------------------------------
  bool okChecksum1 = true;
  bool okChecksum2 = true;
  uint32_t crc = DataHelper::updateCRC32(crcHead,
                                         buffer.getDataPtr(),
                                         buffer.getSize());

  if (*chkSum1 != crc) {
    okChecksum1 = false;
  }

  if (chkSum2 != crc) {
    okChecksum2 = false;
  }

  if (okChecksum1 || okChecksum2) {
    if (!okChecksum1 || !okChecksum2) {
      ++stats.fixedWrongChecksum;
    }

    if (wrongMagic) {
      ++stats.fixedWrongMagic;
    }

    return offset + size + 24;
  }

  //--------------------------------------------------------------------------
  // Checksums incorrect - parhaps size is wrong - try to find another
  // record magic
  // The first magic we find may not be the right one, so we need to do
  // it many times
  //--------------------------------------------------------------------------
  off_t startHint = offset + 24;

  while (1) {
    //------------------------------------------------------------------------
    // Estimate new size
    //------------------------------------------------------------------------
    off_t offSize;
    offSize = guessSize(fd, offset, buffer, startHint);

    if (offSize == (off_t) - 1) {
      return -1;
    }

    size = offSize;

    if (pread(fd, &chkSum2, 4, offset + 20 + size) != 4) {
      return -1;
    }

    startHint += size + 4;
    //------------------------------------------------------------------------
    // Check the checksums
    //------------------------------------------------------------------------
    okChecksum1 = true;
    okChecksum2 = true;
    crc = DataHelper::updateCRC32(crcHead,
                                  buffer.getDataPtr(),
                                  buffer.getSize());

    if (*chkSum1 != crc) {
      okChecksum1 = false;
    }

    if (chkSum2 != crc) {
      okChecksum2 = false;
    }

    if (okChecksum1 || okChecksum2) {
      if (!okChecksum1 || !okChecksum2) {
        ++stats.fixedWrongChecksum;
      }

      if (wrongMagic) {
        ++stats.fixedWrongMagic;
      }

      ++stats.fixedWrongSize;
      return offset + size + 24;
    }
  }

  return -1;
}

//----------------------------------------------------------------------------
//! Repair a changelog
//----------------------------------------------------------------------------
void ChangeLogFile::repair(const std::string&  filename,
                           const std::string&  newFilename,
                           LogRepairStats&     stats,
                           ILogRepairFeedback* feedback)
{
  time_t startTime = time(0);
  //--------------------------------------------------------------------------
  // Open the input and output files and check out the header
  //--------------------------------------------------------------------------
  int fd = ::open(filename.c_str(), O_RDONLY);

  if (fd == -1) {
    MDException ex(errno);
    ex.getMessage() << "Unrecognized file type: " << filename;
    throw ex;
  }

  FileSmartPtr fdPtr(fd);
  uint16_t contentFlag = 0;

  try {
    uint32_t headerFlags = checkHeader(fd, filename);
    uint8_t  version     = 0;
    uint8_t  userFlags   = 0;
    decodeHeaderFlags(headerFlags, version, contentFlag, userFlags);

    if (feedback) {
      feedback->reportHeaderStatus(true, "", version, contentFlag);
    }
  } catch (MDException& e) {
    if (feedback) {
      feedback->reportHeaderStatus(false, e.getMessage().str(), 0, 0);
    }
  }

  ChangeLogFile output;
  output.open(newFilename, Create, contentFlag);
  //--------------------------------------------------------------------------
  // Reconstructing...
  //--------------------------------------------------------------------------
  Buffer  buff;
  uint8_t type;
  off_t   fsize  = ::lseek(fd, 0, SEEK_END);
  off_t   offset = 8; // offset of the first record
  stats.bytesTotal    = fsize;
  stats.bytesAccepted = 8; // the file header size

  while (offset < fsize) {
    //------------------------------------------------------------------------
    // Reconstruct the header at the offset
    //------------------------------------------------------------------------
    off_t newOffset = reconstructRecord(fd, offset, fsize, buff, type,
                                        stats);
    ++stats.scanned;

    //------------------------------------------------------------------------
    // We were successful
    //------------------------------------------------------------------------
    if (newOffset != (off_t) - 1) {
      ++stats.healthy;
      stats.bytesAccepted += (newOffset - offset);
      output.storeRecord(type, buff);
    }
    //------------------------------------------------------------------------
    // Unsuccessful for whatever reason - offsets cannot be trusted anymore
    // so try to find a magic number of a new record
    //------------------------------------------------------------------------
    else {
      ++stats.notFixed;
      newOffset = ChangeLogFile::findRecordMagic(fd, offset + 4, (off_t)0);

      if (newOffset == (off_t) - 1) {
        stats.bytesDiscarded += (fsize - offset);
        break;
      }

      {
        char msg[4096];
        snprintf(msg, 4096,
                 "error: discarded block from offset [ %llx <=> %llx ] [ len=%lu ] \n",
                 (long long)offset, (long long)newOffset, (unsigned long)(newOffset - offset));
        fprintf(stderr, msg);
        fflush(stderr);
      }

      stats.bytesDiscarded += (newOffset - offset);
    }

    //------------------------------------------------------------------------
    // We either successfully reconstructed the previous record or found
    // an offset of another one
    //------------------------------------------------------------------------
    offset = newOffset;
    stats.timeElapsed = time(0) - startTime;

    if (feedback) {
      feedback->reportProgress(stats);
    }
  }
}

//----------------------------------------------------------------------------
// Set the user flags
//----------------------------------------------------------------------------
void ChangeLogFile::setUserFlags(uint8_t flags)
{
  //--------------------------------------------------------------------------
  // Check if the file is open
  //--------------------------------------------------------------------------
  if (!pIsOpen) {
    MDException ex(EFAULT);
    ex.getMessage() << "setUserFlags: Changelog file is not open";
    throw ex;
  }

  uint32_t tmp;
  uint32_t fileFlags = 0;
  fileFlags |= pVersion;
  tmp = pContentFlag;
  fileFlags |= (tmp << 8);
  tmp = flags;
  fileFlags |= (tmp << 24);

  if (pwrite(pFd, &fileFlags, 4, 4) != 4) {
    MDException ex(errno);
    ex.getMessage() << "Unable to write user flags: ";
    ex.getMessage() << strerror(errno);
    throw ex;
  }

  pUserFlags = flags;
}

//------------------------------------------------------------------------
//! Add compaction mark
//------------------------------------------------------------------------
void ChangeLogFile::addCompactionMark()
{
  //--------------------------------------------------------------------------
  // Check if the file is open
  //--------------------------------------------------------------------------
  if (!pIsOpen) {
    MDException ex(EFAULT);
    ex.getMessage() << "setUserFlags: Changelog file is not open";
    throw ex;
  }

  //--------------------------------------------------------------------------
  // Write a compacting stamp
  //--------------------------------------------------------------------------
  Buffer buffer;
  buffer.putData("DUMMY", 5);
  storeRecord(eos::COMPACT_STAMP_RECORD_MAGIC, buffer);
  //--------------------------------------------------------------------------
  // Cleanup
  //--------------------------------------------------------------------------
  setUserFlags(getUserFlags() | eos::LOG_FLAG_COMPACTED);
}
}
