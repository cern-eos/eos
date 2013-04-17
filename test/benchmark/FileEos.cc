//------------------------------------------------------------------------------
// File: FileEos.cc
// Author: Elvin-Alin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
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

/*-----------------------------------------------------------------------------*/
#include <fstream>
#include <iostream>
#include <sys/stat.h>
/*-----------------------------------------------------------------------------*/
#include "FileEos.hh"
#include "Result.hh"
#include "XrdSfs/XrdSfsInterface.hh"
#include "fst/layout/FileIoPlugin.hh"
#include "fst/io/AsyncMetaHandler.hh"
#include "fst/layout/RaidMetaLayout.hh"
#include "fst/layout/RaidDpLayout.hh"
#include "fst/layout/ReedSLayout.hh"
#include "common/Timing.hh"
#include "common/LayoutId.hh"
/*-----------------------------------------------------------------------------*/


EOSBMKNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
FileEos::FileEos(const std::string& filePath,
                 const std::string& bmkInstance,
                 uint64_t           fileSize,
                 uint32_t           blockSize):
  eos::common::LogId(),
  mFilePath(filePath),
  mBmkInstance(bmkInstance),
  mFileSize(fileSize),
  mBlockSize(blockSize)
{
  // empty
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
FileEos::~FileEos()
{
  // empty
}


//------------------------------------------------------------------------------
// Write file
//------------------------------------------------------------------------------
int
FileEos::Write(Result*& result)
{
  eos_debug("Calling function");
  int retc = 0;
  uint64_t file_size = mFileSize;
  uint32_t block_size = mBlockSize;
  eos::common::Timing wr_timing("write");

  // Fill buffer with random characters
  char* buffer = new char[block_size];
  std::ifstream urandom("/dev/urandom", std::ios::in | std::ios::binary);
  urandom.read(buffer, block_size);
  urandom.close();

  // Open the file for writing and get and XrdFileIo object
  eos::fst::AsyncMetaHandler* file_handler = new eos::fst::AsyncMetaHandler();
  mode_t mode_sfs = S_IRUSR | S_IWUSR | S_IXUSR | S_IRGRP | S_IROTH;
  XrdSfsFileOpenMode flags_sfs = SFS_O_CREAT | SFS_O_RDWR;
  eos::fst::FileIo* file = eos::fst::FileIoPlugin::GetIoObject(
                             eos::common::LayoutId::kXrdCl,
                             NULL, NULL, NULL);
  
  COMMONTIMING("OPEN", &wr_timing);
  std::string full_path = mBmkInstance;
  full_path += "/";
  full_path += mFilePath;
  retc = file->Open(full_path, flags_sfs, mode_sfs, "");

  if (retc)
  {
    eos_err("Error while opening file: %s", full_path.c_str());
    delete file;
    delete file_handler;
    return retc;
  }

  COMMONTIMING("WRITE", &wr_timing);

  // Do the actual writing
  uint64_t offset = 0;
  uint64_t length = 0;
  int64_t nwrite;

  while (file_size > 0)
  {
    length = ((file_size > block_size) ?  block_size : file_size);
    nwrite = file->Write(offset, buffer, length, file_handler);

    if (nwrite != (int64_t)length)
    {
      eos_err("Failed while doing write at offset=%llu", offset);
      retc = -1;
      break;
    }

    offset += nwrite;
    file_size -= nwrite;
  }

  COMMONTIMING("WAIT_ASYNC", &wr_timing);

  // Collect all the write responses
  if (!file_handler->WaitOK())
  {
    eos_err("Error while waiting for write async responses");
    retc = -1;
  }

  COMMONTIMING("CLOSE", &wr_timing);
  retc = file->Close();
  COMMONTIMING("END", &wr_timing);

  // Collect statistics for this operation in the result object at job level
  ResultProto& pb_result = result->GetPbResult();
  float transaction_time = wr_timing.GetTagTimelapse("OPEN", "END");
  pb_result.add_timestamp(GetTimestamp());
  pb_result.add_opentime(wr_timing.GetTagTimelapse("OPEN", "WRITE"));
  pb_result.add_readtime(0);
  pb_result.add_readwaitasync(0);
  pb_result.add_writetime(wr_timing.GetTagTimelapse("WRITE", "WAIT_ASYNC"));
  pb_result.add_writewaitasync(wr_timing.GetTagTimelapse("WAIT_ASYNC", "CLOSE"));
  pb_result.add_closetime(wr_timing.GetTagTimelapse("CLOSE", "END"));
  pb_result.add_transactiontime(wr_timing.GetTagTimelapse("OPEN", "END"));
  pb_result.add_readspeed(0);
  pb_result.add_writespeed(((float)offset / eos::common::MB) /
                           (transaction_time / 1000.0));
  pb_result.add_readtotal(0);
  pb_result.add_writetotal(offset);

  delete file;
  delete file_handler;
  delete[] buffer;
  return retc;
}


//------------------------------------------------------------------------------
// Read a file in gateway mode
//------------------------------------------------------------------------------
int
FileEos::ReadGw(Result*& result)
{
  eos_debug("Calling function");
  int retc = 0;
  char* buffer;
  std::vector<char*> vect_buff;
  int32_t total_buffs = 64;
  uint64_t file_size = mFileSize;
  uint32_t block_size = mBlockSize;
  eos::common::Timing rd_timing("rdgw");

  // Allocate a pool of read buffers and then do round-robin for reading in them
  // so as to minimse the probability of two requests writing in the same buffer
  // at the same time
  for (int32_t i = 0; i < total_buffs; i++)
  {
    buffer = new char[block_size];
    vect_buff.push_back(buffer);
  }

  // Open the file for reading and get and XrdFileIo object
  eos::fst::AsyncMetaHandler* file_handler = new eos::fst::AsyncMetaHandler();
  XrdSfsFileOpenMode flags_sfs = SFS_O_RDONLY;
  eos::fst::FileIo* file = eos::fst::FileIoPlugin::GetIoObject(
                             eos::common::LayoutId::kXrdCl,
                             NULL, NULL, NULL);
  COMMONTIMING("OPEN", &rd_timing);
  std::string full_path = mBmkInstance;
  full_path += "/";
  full_path += mFilePath;
  retc = file->Open(full_path, flags_sfs, 0, "fst.readahead=true");

  if (retc)
  {
    eos_err("Error while opening files: %s", full_path.c_str());
    delete file;
    delete file_handler;
    return retc;
  }

  COMMONTIMING("READ", &rd_timing);

  // Do the actual reading
  int32_t indx_buff = 0;
  uint64_t offset = 0;
  uint64_t length = 0;
  int64_t nread;

  while (file_size > 0)
  {
    length = ((file_size > block_size) ?  block_size : file_size);
    nread = file->Read(offset, vect_buff[indx_buff], length, file_handler, true);
    offset += nread;
    file_size -= nread;
    indx_buff = (indx_buff + 1) % total_buffs;
  }

  COMMONTIMING("WAIT_ASYNC", &rd_timing);

  // Collect all the read responses
  if (!file_handler->WaitOK())
  {
    eos_err("Error while waiting for read async responses");
    retc = -1;
  }

  COMMONTIMING("CLOSE", &rd_timing);
  retc = file->Close();
  COMMONTIMING("END", &rd_timing);

  // Collect statistics for this operation in the result object at thread level
  ResultProto& pb_result = result->GetPbResult();
  float transaction_time = rd_timing.GetTagTimelapse("OPEN", "END");
  pb_result.add_timestamp(GetTimestamp());
  pb_result.add_opentime(rd_timing.GetTagTimelapse("OPEN", "READ"));
  pb_result.add_readtime(rd_timing.GetTagTimelapse("READ", "WAIT_ASYNC"));
  pb_result.add_readwaitasync(rd_timing.GetTagTimelapse("WAIT_ASYNC", "CLOSE"));
  pb_result.add_writetime(0);
  pb_result.add_writewaitasync(0);
  pb_result.add_closetime(rd_timing.GetTagTimelapse("CLOSE", "END"));
  pb_result.add_transactiontime(rd_timing.GetTagTimelapse("OPEN", "END"));
  pb_result.add_readspeed(((float)offset / eos::common::MB) /
                          (transaction_time / 1000.0));
  pb_result.add_writespeed(0);
  pb_result.add_readtotal(offset);
  pb_result.add_writetotal(0);

  //Free allocated memory
  for (uint32_t i = 0; i < vect_buff.size(); i++)
  {
    delete[] vect_buff[i];
  }

  vect_buff.clear();
  delete file;
  delete file_handler;
  return retc;
}


//------------------------------------------------------------------------------
// Read a file in parallel IO mode
//------------------------------------------------------------------------------
int
FileEos::ReadPio(Result*& result)
{
  eos_debug("Calling function");
  int retc = 0;
  char* buffer;
  std::vector<char*> vect_buff;
  int32_t total_buffs = 64;
  uint64_t file_size = mFileSize;
  uint32_t block_size = mBlockSize;
  eos::common::Timing rd_timing("rdpio");
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;
  XrdCl::XRootDStatus status;
  eos::fst::RaidMetaLayout* file = 0;
  XrdSfsFileOpenMode flags_sfs = SFS_O_RDONLY; // open for read by default

  // Allocate a pool of read buffers and then do round-robin for reading in them
  // so as to minimse the probability of two requests writing in the same buffer
  // at the same time
  for (int32_t i = 0; i < total_buffs; i++)
  {
    buffer = new char[block_size];
    vect_buff.push_back(buffer);
  }

  // Create an XrdCl::FileSystem object and do PIO request
  COMMONTIMING("OPEN", &rd_timing);
  XrdCl::URL url(mBmkInstance);

  if (!url.IsValid())
  {
    cerr << "URL is invalid." << endl;
    return -1;
  }

  XrdCl::FileSystem* fs = new XrdCl::FileSystem(url);
  std::string request = mFilePath;
  request += "?mgm.pcmd=open";
  arg.FromString(request);
  status = fs->Query(XrdCl::QueryCode::OpaqueFile, arg, response);

  if (status.IsOK())
  {
    //..........................................................................
    // Parse output
    //..........................................................................
    XrdOucString tag;
    XrdOucString stripePath;
    std::vector<std::string> stripeUrls;
    XrdOucString origResponse = response->GetBuffer();
    XrdOucString stringOpaque = response->GetBuffer();

    while (stringOpaque.replace("?", "&")) {}

    while (stringOpaque.replace("&&", "&")) {}

    XrdOucEnv* openOpaque = new XrdOucEnv(stringOpaque.c_str());
    char* opaqueInfo = (char*) strstr(origResponse.c_str(), "&&mgm.logid");

    if (opaqueInfo)
    {
      opaqueInfo += 2;
      eos::common::LayoutId::layoutid_t layout = openOpaque->GetInt("mgm.lid");

      for (unsigned int i = 0; i <= eos::common::LayoutId::GetStripeNumber(layout);
           i++)
      {
        tag = "pio.";
        tag += static_cast<int>(i);
        stripePath = "root://";
        stripePath += openOpaque->Get(tag.c_str());
        stripePath += "/";
        stripePath += mFilePath.c_str();
        stripeUrls.push_back(stripePath.c_str());
      }

      if (eos::common::LayoutId::GetLayoutType(layout) == eos::common::LayoutId::kRaidDP)
      {
        file = new eos::fst::RaidDpLayout(NULL, layout, NULL, NULL,
                                          eos::common::LayoutId::kXrdCl);
      }
      else if ((eos::common::LayoutId::GetLayoutType(layout) == eos::common::LayoutId::kRaid6) ||
               (eos::common::LayoutId::GetLayoutType(layout) == eos::common::LayoutId::kArchive))
      {
        file = new eos::fst::ReedSLayout(NULL, layout, NULL, NULL,
                                         eos::common::LayoutId::kXrdCl);
      }
      else
      {
        eos_err("No such supported layout for PIO");
        file = 0;
      }

      if (file)
      {
        retc = file->OpenPio(stripeUrls, flags_sfs, 0, opaqueInfo);

        if (retc)
        {
          eos_err("error=open PIO failed for path=%s", mFilePath.c_str());

          //Free allocated memory
          for (uint32_t i = 0; i < vect_buff.size(); i++)
          {
            delete vect_buff[i];
          }

          delete response;
          delete openOpaque;
          delete file;
          return retc;
        }
      }
      else
      {
        delete response;
        delete openOpaque;
        delete file;
        cout << "Falling back to read gw.(0)" << endl;
        return ReadGw(result);
      }
    }
    else
    {
      eos_err("error=opaque info not what we expected");
      delete response;
      delete openOpaque;
      delete file;
      cout << "Falling back to read gw.(1)" << endl;
      return ReadGw(result);
    }
  }
  else
  {
    eos_warning("Failed to get PIO request falling back to GW mode");

    //Free allocated memory
    for (uint32_t i = 0; i < vect_buff.size(); i++)
    {
      delete vect_buff[i];
    }

    delete response;
    cout << "Falling back to read gw.(2)" << endl;
    return ReadGw(result);
  }

  COMMONTIMING("READ", &rd_timing);

  // Do the actual reading
  int32_t indx_buff = 0;
  uint64_t offset = 0;
  uint64_t length = 0;
  int64_t nread;

  while (file_size > 0)
  {
    length = ((file_size > block_size) ?  block_size : file_size);
    nread = file->Read(offset, vect_buff[indx_buff], length);
    offset += nread;
    file_size -= nread;
    indx_buff = (indx_buff + 1) % total_buffs;
  }

  COMMONTIMING("CLOSE", &rd_timing);
  retc = file->Close();
  COMMONTIMING("END", &rd_timing);
  
  // Collect statistics for this operation in the result object at thread level
  ResultProto& pb_result = result->GetPbResult();
  float transaction_time = rd_timing.GetTagTimelapse("OPEN", "END");
  pb_result.add_timestamp(GetTimestamp());
  pb_result.add_opentime(rd_timing.GetTagTimelapse("OPEN", "READ"));
  pb_result.add_readtime(rd_timing.GetTagTimelapse("READ", "CLOSE"));
  pb_result.add_readwaitasync(0);
  pb_result.add_writetime(0);
  pb_result.add_writewaitasync(0);
  pb_result.add_closetime(rd_timing.GetTagTimelapse("CLOSE", "END"));
  pb_result.add_transactiontime(rd_timing.GetTagTimelapse("OPEN", "END"));
  pb_result.add_readspeed(((float)offset / eos::common::MB) /
                          (transaction_time / 1000.0));
  pb_result.add_writespeed(0);
  pb_result.add_readtotal(offset);
  pb_result.add_writetotal(0);

  //Free allocated memory
  for (uint32_t i = 0; i < vect_buff.size(); i++)
  {
    delete[] vect_buff[i];
  }

  vect_buff.clear();
  delete file;
  delete response;
  return retc;
}


//------------------------------------------------------------------------------
// Write and read file in gateway mode
//------------------------------------------------------------------------------
int
FileEos::ReadWriteGw(Result*& result)
{
  eos_debug("Calling function");
  int retc = 0;
  retc += Write(result);
  retc += ReadGw(result);
  return retc;
}


//------------------------------------------------------------------------------
// Write and read file in parallel IO mode
//------------------------------------------------------------------------------
int
FileEos::ReadWritePio(Result*& result)
{
  eos_debug("Calling function");
  int retc = 0;
  retc += Write(result);
  retc += ReadPio(result);
  return retc;
}


//------------------------------------------------------------------------------
// Get current timestamp as a string
//------------------------------------------------------------------------------
std::string
FileEos::GetTimestamp()
{
  std::string time_str;
  char time_buff[1024];
  time_t current_time;
  struct tm* tm;
  time(&current_time);
  tm = localtime(&current_time);
  sprintf(time_buff, "%02d/%02d/%04d %02d:%02d:%02d", tm->tm_mday, tm->tm_mon + 1,
          tm->tm_year + 1900, tm->tm_hour, tm->tm_min, tm->tm_sec);
  time_str = time_buff;
  return time_str;
}

EOSBMKNAMESPACE_END
