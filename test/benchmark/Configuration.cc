//------------------------------------------------------------------------------
// File: Configuration.cc
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

/*----------------------------------------------------------------------------*/
#include <string>
#include <fstream>
#include <iostream>
#include <iomanip>
#include <sstream>
#include <functional>
/*----------------------------------------------------------------------------*/
#include <uuid/uuid.h>
/*----------------------------------------------------------------------------*/
#include "Configuration.hh"
#include "DirEos.hh"
#include "ProtoIo.hh"
#include "common/StringConversion.hh"
/*----------------------------------------------------------------------------*/

using namespace std;

EOSBMKNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
Configuration::Configuration():
  eos::common::LogId()
{
  mPbConfig = new ConfigProto();
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
Configuration::~Configuration()
{
  mFileNames.clear();

  if (mPbConfig)
  {
    delete mPbConfig;
  }
}


//------------------------------------------------------------------------------
// Set the low level configuration object taking the ownership
//------------------------------------------------------------------------------
void
Configuration::SetPbConfig(ConfigProto* pbConfig)
{
  if (mPbConfig) delete mPbConfig;

  mPbConfig = pbConfig;
}


//------------------------------------------------------------------------------
// Get the low level configuration object (ProtBuf object)
//------------------------------------------------------------------------------
ConfigProto&
Configuration::GetPbConfig() const
{
  return *mPbConfig;
}


//------------------------------------------------------------------------------
// Generate file names used for the write operations
//------------------------------------------------------------------------------
void
Configuration::GenerateFileNames()
{
  string gen_filename;
  uuid_t gen_uuid;
  char char_uuid[40];
  uint32_t num_files = mPbConfig->numfiles();
  uint32_t num_jobs = mPbConfig->numjobs();

  if (mPbConfig->access() == ConfigProto_AccessMode_CONCURRENT)
  {
    // All jobs access the same files
    mFileNames.reserve(num_files);

    for (uint32_t i = 0; i < num_files; i++)
    {
      uuid_generate_time(gen_uuid);
      uuid_unparse(gen_uuid, char_uuid);
      gen_filename = mPbConfig->benchmarkdir();
      gen_filename += char_uuid;
      mFileNames.push_back(gen_filename);
    }
  }
  else if (mPbConfig->access() == ConfigProto_AccessMode_PARALLEL)
  {
    // Each job gets a separate set of files on which it works
    mFileNames.reserve(num_jobs * num_files);

    for (uint32_t i = 0; i < num_jobs; i++)
    {
      for (uint32_t j = 0; j < num_files; j++)
      {
        uuid_generate_time(gen_uuid);
        uuid_unparse(gen_uuid, char_uuid);
        gen_filename = mPbConfig->benchmarkdir();
        gen_filename += char_uuid;
        mFileNames.push_back(gen_filename);
      }
    }
  }
}


//------------------------------------------------------------------------------
// Print configuration
//------------------------------------------------------------------------------
void
Configuration::Print()
{
  std::stringstream sstr;
  sstr << left << setw(190) << setfill('*') << "" << endl;
  std::string star_line = sstr.str();
  sstr.str("");
  sstr << left << setw(190) << setfill('-') << "" << endl;
  std::string minus_line =  sstr.str();
  cout << endl << star_line
       << setw(100) << right << "C o n f i g u r a t i o n" << endl
       << star_line
       << setw(30) << left << setfill('.') <<  "EOS instance" << setfill(' ')
       << setw(40) << left << mPbConfig->benchmarkinstance()
       << setw(30) << left << setfill('.') << "Test path" << setfill(' ')
       << setw(40) << left << mPbConfig->benchmarkdir() << endl
       << setw(30) << left << setfill('.') << "File size" << setfill(' ')
       << setw(40) << left
       << eos::common::StringConversion::GetPrettySize(mPbConfig->filesize())
       << setw(30) << left << setfill('.') << "Block size" << setfill(' ')
       << setw(40) << left
       << eos::common::StringConversion::GetPrettySize(mPbConfig->blocksize())
       << endl
       << setw(30) << left << setfill('.') << "File layout" << setfill(' ')
       << setw(40) << left << GetFileLayout(mPbConfig->filelayout())
       << setw(30) << left << setfill('.') << "Number of files" << setfill(' ')
       << setw(40) << left << mPbConfig->numfiles() << endl
       << setw(30) << left << setfill('.') << "Job type" << setfill(' ')
       << setw(40) << left << (mPbConfig->jobtype() ? "process" : "thread")
       << setw(30) << left << setfill('.') << "Number of jobs" << setfill(' ')
       << setw(40) << left << mPbConfig->numjobs() << endl
       << setw(30) << left << setfill('.') << "Operation" << setfill(' ')
       << setw(40) << left << GetOperation(mPbConfig->operation())
       << setw(30) << left << setfill('.') << "Access mode" << setfill(' ')
       << setw(40) << left << (mPbConfig->access() ? "parallel" : "concurrent")
       << endl
       << setw(30) << left << setfill('.') << "Read pattern" << setfill(' ')
       << setw(40) << left << GetPattern(mPbConfig->pattern()) << endl;

  if (mPbConfig->pattern() == ConfigProto_PatternType_RANDOM)
  {
    cout << "Number of requests: " << mPbConfig->offset_size() << endl;
    cout << "Requests (offset, length): " << endl;

    for (int i = 0; i < mPbConfig->offset_size(); i++)
    {
      cout << "( " << left << setw(10) << mPbConfig->offset(i)
           << ","  << left << setw(10) << mPbConfig->length(i) << " )   ";

      if ((i + 1) % 4 == 0) cout << endl;
    }

    cout << endl;
  }

  cout << minus_line << endl << endl;
}


//------------------------------------------------------------------------------
// Check whether directory path exists and has correct attributes, if it does
// not exist then it is created and set the correct attributes.
//------------------------------------------------------------------------------
bool
Configuration::CheckDirAndFiles()
{
  bool ret = true;
  DirEos* dir = new DirEos(mPbConfig->benchmarkdir(),
                           mPbConfig->benchmarkinstance());

  // Check if directory exists and if not create it
  if (!dir->Exist())
  {
    if (!dir->Create())
    {
      eos_err("Could not create working directory");
      ret = false;
    }

    // Set directory attributes to match the required configuration
    if (!dir->SetConfig(*mPbConfig))
    {
      eos_err("Error while trying to set attributes to the dir");
      ret = false;
    }
  }
  else if (!dir->MatchConfig(*mPbConfig))
  {
    ret = false;
  }

  if (!ret)
  {
    delete dir;
    return ret;
  }

  // If operation is read only then we have to check that we have enough files
  // in the benchmark directoy, if not we abort
  if ((mPbConfig->operation() == ConfigProto_OperationType_READ_GW) ||
      (mPbConfig->operation() == ConfigProto_OperationType_READ_PIO))
  {
    uint32_t required_files = 0;
    mFileNames = dir->GetMatchingFiles(mPbConfig->filesize());

    if (mPbConfig->access() == ConfigProto_AccessMode_CONCURRENT)
    {
      required_files = mPbConfig->numfiles();
    }
    else if (mPbConfig->access() == ConfigProto_AccessMode_PARALLEL)
    {
      required_files = mPbConfig->numjobs() * mPbConfig->numfiles();
    }

    if (mFileNames.size() <= required_files)
    {
      eos_err("Not enough files in dir for the read operation");
      ret = false;
    }
  }
  else
  {
    // Generate the file names used for the benchmark run
    GenerateFileNames();
  }

  delete dir;
  return ret;
}


//------------------------------------------------------------------------------
// Create configuration file - accept input from the console and build up the
// configuration object which is then written to the file supplied as an arg.
//------------------------------------------------------------------------------
bool
Configuration::CreateConfigFile(const string& outputFile)
{
  string input_value = "";
  uint64_t block_size;
  ConfigProto_OperationType op_type;

  // Get benchmark instance 
  cout << "Benchmarked instance: ";

  while (input_value.empty())
  {
    if (!getline(cin, input_value))
    {
      return false;
    }
  }

  mPbConfig->set_benchmarkinstance(input_value);
  
  // Get benchmark directory where operations are done
  input_value = "";
  cout << "Benchmark directory: ";

  while (input_value.empty())
  {
    if (!getline(cin, input_value))
    {
      return false;
    }
  }

  // Make sure that the directory ends with one "/"
  if (*input_value.rbegin() != '/')
  {
    input_value += "/";
  }

  mPbConfig->set_benchmarkdir(input_value);

  // Get the file size
  while (1)
  {
    size_t pos;
    uint64_t file_size = 0;
    input_value = "";
    cout << "File size (KB|MB|GB): ";

    while (input_value.empty())
    {
      if (!getline(cin, input_value))
      {
        return false;
      }
    }

    if ((pos = input_value.rfind("KB")) != string::npos)
    {
      // File size given in KB
      input_value = input_value.erase(pos);
      file_size = eos::common::KB;
    }
    else if ((pos = input_value.rfind("MB")) != string::npos)
    {
      // File size given in MB
      input_value = input_value.erase(pos);
      file_size = eos::common::MB;
    }
    else if ((pos = input_value.rfind("GB")) != string::npos)
    {
      // File size given in GB
      input_value = input_value.erase(pos);
      file_size = eos::common::GB;
    }
    else
    {
      cout << "Input value is invalid! " << endl;
      continue;
    }

    char* pEnd;
    float size;

    if ((size = strtol(input_value.c_str(), &pEnd, 10)) == 0L)
    {
      cout << "Input value is invalid! " << endl;
      continue;
    }

    file_size *= size;
    mPbConfig->set_filesize(file_size);
    break;
  }

  // Get the number of files
  while (1)
  {
    input_value = "";
    cout << "Number of files: ";

    while (input_value.empty())
    {
      if (!getline(cin, input_value))
      {
        return false;
      }
    }

    char* pEnd;
    uint32_t no_files;

    if ((no_files = strtol(input_value.c_str(), &pEnd, 10)) == 0L)
    {
      cout << "Input value is invalid! " << endl;
      continue;
    }

    mPbConfig->set_numfiles(no_files);
    break;
  }

  // Get block size for rd/wr operations
  while (1)
  {
    size_t pos;
    input_value = "";
    cout << "Block size (KB|MB|GB): ";

    while (input_value.empty())
    {
      if (!getline(cin, input_value))
      {
        return false;
      }
    }

    if ((pos = input_value.rfind("KB")) != string::npos)
    {
      // File size given in KB
      input_value = input_value.erase(pos);
      block_size = eos::common::KB;
    }
    else if ((pos = input_value.rfind("MB")) != string::npos)
    {
      // File size given in MB
      input_value = input_value.erase(pos);
      block_size = eos::common::MB;
    }
    else if ((pos = input_value.rfind("GB")) != string::npos)
    {
      // File size given in GB
      input_value = input_value.erase(pos);
      block_size = eos::common::GB;
    }
    else
    {
      cout << "Input value is invalid! " << endl;
      continue;
    }

    char* pEnd;
    uint64_t size;

    if ((size = strtol(input_value.c_str(), &pEnd, 10)) == 0L)
    {
      cout << "Input value is invalid! " << endl;
      continue;
    }

    block_size *= size;
    mPbConfig->set_blocksize(block_size);
    break;
  }

  // Get file layout for benchmark
  while (1)
  {
    ConfigProto_FileLayoutType file_type;
    input_value = "";
    cout << "File layout (plain|replica|raiddp|raid6|archive): ";

    while (input_value.empty())
    {
      if (!getline(cin, input_value))
      {
        return false;
      }
    }

    file_type = Configuration::GetFileLayout(input_value);

    if (file_type == ConfigProto_FileLayoutType_NOLAYOUT)
    {
      cout << "Input value is invalid!" << endl;
      continue;
    }

    mPbConfig->set_filelayout(file_type);
    break;
  }

  // For the replica type of layaout get the number of replicas
  if (mPbConfig->filelayout() == ConfigProto_FileLayoutType_REPLICA)
  {
    while (1)
    {
      input_value = "";
      cout << "Number of replicas: ";

      while (input_value.empty())
      {
        if (!getline(cin, input_value))
        {
          return false;
        }
      }

      char* pEnd;
      uint32_t no_replicas;

      if ((no_replicas = strtol(input_value.c_str(), &pEnd, 10)) == 0L)
      {
        cout << "Input value is invalid! " << endl;
        continue;
      }

      mPbConfig->set_noreplicas(no_replicas);
      break;
    }
  }

  // Get type of execution task
  while (1)
  {
    ConfigProto_JobType job_type;
    input_value = "";
    cout << "Execution type (thread|process): ";

    while (input_value.empty())
    {
      if (!getline(cin, input_value))
      {
        return false;
      }
    }

    if (input_value.compare("thread") == 0)
    {
      job_type = ConfigProto_JobType_THREAD;
    }
    else if (input_value.compare("process") == 0)
    {
      job_type = ConfigProto_JobType_PROCESS;
    }
    else
    {
      cout << "Input value is invalid!" << endl;
      continue;
    }

    mPbConfig->set_jobtype(job_type);
    break;
  }

  // Get the number of jobs to be launched (threads/processes)
  while (1)
  {
    input_value = "";
    cout << "Number of jobs: ";

    while (input_value.empty())
    {
      if (!getline(cin, input_value))
      {
        return false;
      }
    }

    char* pEnd;
    uint32_t no_jobs;

    if ((no_jobs = strtol(input_value.c_str(), &pEnd, 10)) == 0L)
    {
      cout << "Input value is invalid! " << endl;
      continue;
    }

    mPbConfig->set_numjobs(no_jobs);
    break;
  }

  // Get operation type
  while (1)
  {
    input_value = "";
    cout << "Operation (write|read_gw|read_pio|rdwr_gw|rdwr_pio): ";

    while (input_value.empty())
    {
      if (!getline(cin, input_value))
      {
        return false;
      }
    }

    op_type = Configuration::GetOperation(input_value);

    if (op_type == ConfigProto_OperationType_NOTYPE)
    {
      cout << "Input value is invalid!" << endl;
      continue;
    }

    mPbConfig->set_operation(op_type);
    break;
  }

  // Get pattern type and generate set of random requests if needed
  if (op_type != ConfigProto_OperationType_WRITE)
  {
    while (1)
    {
      ConfigProto_PatternType pattern_type;
      input_value = "";
      cout << "Read pattern (full|random): ";

      while (input_value.empty())
      {
        if (!getline(cin, input_value))
        {
          return false;
        }
      }

      pattern_type = Configuration::GetPattern(input_value);

      if (pattern_type == ConfigProto_PatternType_NOPATTERN)
      {
        cout << "Input value is invalid!" << endl;
        continue;
      }

      mPbConfig->set_pattern(pattern_type);

      // Generate a set of random (offset, length) pairs
      uint64_t offset;
      uint64_t length;
      srand(time(NULL));

      if (pattern_type == ConfigProto_PatternType_RANDOM)
      {
        uint32_t no_requests;

        while (1)
        {
          input_value = "";
          cout << "Number of requests: ";

          while (input_value.empty())
          {
            if (!getline(cin, input_value))
            {
              return false;
            }
          }

          char* pEnd;

          if ((no_requests = strtol(input_value.c_str(), &pEnd, 10)) == 0L)
          {
            cout << "Input value is invalid! " << endl;
            continue;
          }

          break;
        }

        // Generate randomly the read requests
        uint64_t file_size = mPbConfig->filesize();

        for (uint32_t i = 0; i < no_requests; i++)
        {
          offset = (rand() % file_size) + 1;
          length = (rand() % (file_size - offset)) + 1;
          mPbConfig->add_offset(offset);
          mPbConfig->add_length(length);
        }
      }

      break;
    }
  }

  // If multiple jobs then decide on the type of access
  if (mPbConfig->numjobs() > 1)
  {
    // Get the type of access (parallel/concurrent)
    // parallel - no two jobs access the same file
    // concurrent - all jobs access the same files
    while (1)
    {
      ConfigProto_AccessMode access_type;
      input_value = "";
      cout << "Access type (parallel/concurrent): ";

      while (input_value.empty())
      {
        if (!getline(cin, input_value))
        {
          return false;
        }
      }

      if (input_value.compare("parallel") == 0)
      {
        access_type = ConfigProto_AccessMode_PARALLEL;
      }
      else if (input_value.compare("concurrent") == 0)
      {
        access_type = ConfigProto_AccessMode_CONCURRENT;
      }
      else
      {
        cout << "Input value is invalid!" << endl;
        continue;
      }

      mPbConfig->set_access(access_type);
      break;
    }
  }

  // Write the configuration to the supplied output file
  ProtoWriter writer(outputFile);
  if (!writer(*mPbConfig))
  {
    cout << "Error while writing configuration to file" << endl;
    return false;
  }
    
  return true;
}


//------------------------------------------------------------------------------
// Read in configuration from file
//------------------------------------------------------------------------------
bool
Configuration::ReadFromFile(const string& fileName)
{
  ProtoReader reader(fileName);
  ConfigProto* tmp_conf = reader.ReadNext<ConfigProto>();

  if (tmp_conf)
  {
    SetPbConfig(tmp_conf);
    return true;
  }

  return false;
}


//------------------------------------------------------------------------------
// Get string representation for the file layout
//------------------------------------------------------------------------------
string
Configuration::GetFileLayout(ConfigProto_FileLayoutType fileType)
{
  if (fileType == ConfigProto_FileLayoutType_PLAIN)   return "plain";

  if (fileType == ConfigProto_FileLayoutType_REPLICA) return "replica";

  if (fileType == ConfigProto_FileLayoutType_RAIDDP)  return "raiddp";

  if (fileType == ConfigProto_FileLayoutType_RAID6)   return "raid6";

  if (fileType == ConfigProto_FileLayoutType_ARCHIVE) return "archive";

  return "";
}


//------------------------------------------------------------------------------
// Get int representation for the file layout
//------------------------------------------------------------------------------
ConfigProto_FileLayoutType
Configuration::GetFileLayout(const string& fileType)
{
  if (fileType.compare("plain") == 0)    return ConfigProto_FileLayoutType_PLAIN;

  if (fileType.compare("replica") == 0)  return ConfigProto_FileLayoutType_REPLICA;

  if (fileType.compare("raiddp") == 0)   return ConfigProto_FileLayoutType_RAIDDP;

  if (fileType.compare("raid6") == 0)    return ConfigProto_FileLayoutType_RAID6;

  if (fileType.compare("archive") == 0)  return ConfigProto_FileLayoutType_ARCHIVE;

  return ConfigProto_FileLayoutType_NOLAYOUT;
}


//------------------------------------------------------------------------------
// Get string representation for the operation layout
//------------------------------------------------------------------------------
string
Configuration::GetOperation(ConfigProto_OperationType opType)
{
  if (opType == ConfigProto_OperationType_WRITE)    return "write";

  if (opType == ConfigProto_OperationType_READ_GW)  return "read_gw";

  if (opType == ConfigProto_OperationType_READ_PIO) return "read_pio";

  if (opType == ConfigProto_OperationType_RDWR_GW)  return "rdwr_gw";

  if (opType == ConfigProto_OperationType_RDWR_PIO) return "rdwr_pio";

  return "";
}


//------------------------------------------------------------------------------
// Get int representation for the operation type
//------------------------------------------------------------------------------
ConfigProto_OperationType
Configuration::GetOperation(const string& opType)
{
  if (opType.compare("write") == 0)    return ConfigProto_OperationType_WRITE;

  if (opType.compare("read_gw") == 0)  return ConfigProto_OperationType_READ_GW;

  if (opType.compare("read_pio") == 0) return ConfigProto_OperationType_READ_PIO;

  if (opType.compare("rdwr_gw") == 0)  return ConfigProto_OperationType_RDWR_GW;

  if (opType.compare("rdwr_pio") == 0) return ConfigProto_OperationType_RDWR_PIO;

  return ConfigProto_OperationType_NOTYPE;
}


//------------------------------------------------------------------------------
// Get string representation for the pattern type
//------------------------------------------------------------------------------
string
Configuration::GetPattern(ConfigProto_PatternType patternType)
{
  if (patternType == ConfigProto_PatternType_FULL) return "full";

  if (patternType == ConfigProto_PatternType_RANDOM) return "random";

  return "";
}


//------------------------------------------------------------------------------
// Get int representation for the pattern type
//------------------------------------------------------------------------------
ConfigProto_PatternType
Configuration::GetPattern(const string& patternType)
{
  if (patternType.compare("full") == 0)    return ConfigProto_PatternType_FULL;

  if (patternType.compare("random") == 0)  return ConfigProto_PatternType_RANDOM;

  return ConfigProto_PatternType_NOPATTERN;
}


//------------------------------------------------------------------------------
// Compute hash value for the current object as the hash value of a string
// made up by concatenating some of the fields of the current object
//------------------------------------------------------------------------------
size_t
Configuration::GetHash()
{
  size_t hash_value;
  std::stringstream sstr;
  sstr << mPbConfig->filesize() << mPbConfig->numfiles() << mPbConfig->blocksize()
       << mPbConfig->operation() << mPbConfig->filelayout() << mPbConfig->noreplicas()
       << mPbConfig->jobtype() << mPbConfig->numjobs() << mPbConfig->access()
       << mPbConfig->pattern();

  if (mPbConfig->pattern() == ConfigProto_PatternType_RANDOM)
  {
    for (int32_t i = 0; i < mPbConfig->offset_size(); i++)
    {
      sstr << mPbConfig->offset(i) << mPbConfig->length(i);
    }
  }

  hash_value = std::hash<std::string>()(sstr.str());
  return hash_value;
}


EOSBMKNAMESPACE_END
