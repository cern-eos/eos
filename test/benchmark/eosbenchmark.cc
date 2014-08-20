//------------------------------------------------------------------------------
// File: eosbenchmark.cc
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
#include <iomanip>
#include <ctime>
#include <map>
#include <utility>
#include <vector>
#include <sys/types.h>
#include <sys/wait.h>
/*-----------------------------------------------------------------------------*/
#include "eosbenchmark.hh"
#include "ProtoIo.hh"
#include "FileEos.hh"
#include "Result.hh"
#include "Configuration.hh"
#include "common/Path.hh"
#include "common/StringConversion.hh"
#include "XrdCl/XrdClDefaultEnv.hh"
/*-----------------------------------------------------------------------------*/
#include <getopt.h>
/*-----------------------------------------------------------------------------*/

using namespace std;

EOSBMKNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Print help for command line
//------------------------------------------------------------------------------
void Usage()
{
  cout << "Usage: eosbenchmark <OPTIONS> " << endl
       << left << setw(60) << " --create-config <config.file>  "
       << left << setw(50) << " Prompt for configuration values which will " << endl
       << left << setw(60) << " "
       << left << setw(50) << " be saved in the supplied configuration file " << endl
       << left << setw(60) << " --list-config <config.file> "
       << left << setw(50) << " List the configurations saved in the supplied file " <<
       endl
       << left << setw(60) << " --run-config <config.file> --output <results.file>"
       << left << setw(50) << " Run configuration and write results in output file " <<
       endl
       << left << setw(60) <<
       " --list-results <results.file> [--config <config.file>] "
       << left << setw(50) << " List only runs matching the configuration. If config"
       << endl
       << left << setw(60) << " "
       << left << setw(50) << " file is not present then it lists all runs " << endl
       << left << setw(60) << " --help "
       << left << setw(50) << " Print out this menu" << endl;
}


//------------------------------------------------------------------------------
// Start thread executing a particular function
//------------------------------------------------------------------------------
int
ThreadStart(pthread_t& thread, TypeFunc func, void* arg)
{
  return pthread_create(&thread, NULL, func, arg);
}


//------------------------------------------------------------------------------
// Start routine executed by each job
//------------------------------------------------------------------------------
void*
StartRoutine(void* arg)
{
  int (FileEos::*operation_callback)(Result*
                                     &);  // define operation function pointer
  struct ConfIdStruct* arg_thread = (struct ConfIdStruct*)(arg);
  Configuration& config = static_cast<Configuration&>(arg_thread->config);
  ConfigProto pb_config = config.GetPbConfig();
  uint32_t id_thread = arg_thread->id;

  // Decide on the type of operation to be done and save it as a callback
  switch (pb_config.operation())
  {
  case ConfigProto_OperationType_WRITE:
    operation_callback = &FileEos::Write;
    break;

  case ConfigProto_OperationType_READ_GW:
    operation_callback = &FileEos::ReadGw;
    break;

  case ConfigProto_OperationType_READ_PIO:
    operation_callback = &FileEos::ReadPio;
    break;

  case ConfigProto_OperationType_RDWR_GW:
    operation_callback = &FileEos::ReadWriteGw;
    break;

  case ConfigProto_OperationType_RDWR_PIO:
    operation_callback = &FileEos::ReadWritePio;
    break;

  default:
    eos_static_err("No such supported operation.");
    delete arg_thread;
    return NULL;
  }

  // Create file objects
  uint32_t start_indx = 0;
  uint32_t end_indx = 0;

  if (pb_config.access() == ConfigProto_AccessMode_PARALLEL)
  {
    start_indx = id_thread * pb_config.numfiles();
    end_indx = (id_thread + 1) * pb_config.numfiles();
  }
  else if (pb_config.access() == ConfigProto_AccessMode_CONCURRENT)
  {
    start_indx = 0;
    end_indx = pb_config.numfiles();
  }

  // Result object which collects all the partial results
  Result* job_result = new Result();

  for (uint32_t i = start_indx; i < end_indx; i++)
  {
    eos_static_debug("Execute operation for file:%s, at index:%i ",
                     config.GetFileName(i).c_str(), i);
    int retc = -1;
    FileEos* file = new FileEos(config.GetFileName(i),
                                pb_config.benchmarkinstance(),
                                pb_config.filesize(),
                                pb_config.blocksize());
    // Execute the required operation
    retc = (*file.*operation_callback)(job_result);

    if (retc)
    {
      cerr << "Error while executin operation on file" << endl;
      delete file;
      delete job_result;
      return NULL;
    }

    delete file;
  }

  // Delete the memory allocated for argument passing
  delete arg_thread;
  // Display the statistics collected by the current thread
  return job_result;
}


//------------------------------------------------------------------------------
// Run benchmark using threads
//------------------------------------------------------------------------------
void
RunThreadConfig(Configuration& config, const string& outputFile)
{
  Result merged_result;
  std::vector<pthread_t> vect_threads;
  ConfigProto pb_config = config.GetPbConfig();
  uint32_t num_jobs = pb_config.numjobs();

  // Start all threads and run the proper operations on the files
  for (uint32_t i = 0; i < num_jobs; i++)
  {
    // Arguments passed to the thread are allocated dynamically and then
    // deleted by the thread at the end of the StartRoutine method
    pthread_t thread;
    struct ConfIdStruct* arg_thread = new ConfIdStruct(config, i);
    vect_threads.push_back(thread);
    ThreadStart(vect_threads[i], StartRoutine, (void*) arg_thread);
  }

  // Join all threads and collect the results for the run
  for (uint32_t i = 0; i < num_jobs; i++)
  {
    Result* ret_result;
    pthread_join(vect_threads[i], (void**) &ret_result);

    if (ret_result != NULL)
    {
      merged_result.Merge(*ret_result);
      delete ret_result;
    }
  }

  // Write the configuration and final result object to the file
  ProtoWriter writer(outputFile);

  if (!writer(config.GetPbConfig()) || !writer(merged_result.GetPbResult()))
  {
    cerr << "Errot while writing config and result objects to file. " <<
         endl;
    exit(-1);
  }
}


//------------------------------------------------------------------------------
// Run benchmark using process
//------------------------------------------------------------------------------
void
RunProcessConfig(Configuration& config, const string& outputFile)
{
  // Use pipes to send back information to parent
  Result merged_result;
  ConfigProto& ll_config = config.GetPbConfig();
  uint32_t num_jobs = ll_config.numjobs();
  int** pipefd = new int*[num_jobs];

  for (unsigned int i = 0; i < num_jobs; i++)
  {
    pipefd[i] = new int[2];

    if (pipe(pipefd[i]) == -1)
    {
      eos_static_err("error=error opening pipe");
      exit(-1);
    }
  }

  size_t buff_size;
  pid_t* cpid = new pid_t[num_jobs];

  for (uint32_t i = 0; i < num_jobs; i++)
  {
    cpid[i] = fork();

    if (cpid[i] == -1)
    {
      eos_static_err("error=error in fork");
      exit(-1);
    }

    if (cpid[i] == 0)
    {
      //child process
      close(pipefd[i][0]);    //close reading end
      struct ConfIdStruct* arg_process = new ConfIdStruct(config, i);
      Result* proc_result = static_cast<Result*>(StartRoutine(arg_process));
      std::string str_result;
      ResultProto& ll_result = proc_result->GetPbResult();
      buff_size = ll_result.ByteSize();
      str_result.reserve(buff_size);
      str_result = ll_result.SerializeAsString();
      // Write first the size of the result object and then the object itself
      write(pipefd[i][1], &buff_size, sizeof(buff_size));
      write(pipefd[i][1], str_result.c_str(), buff_size);
      close(pipefd[i][1]);  //close writing end
      delete proc_result;
      exit(EXIT_SUCCESS);
    }
  }

  //............................................................................
  // Parent process
  //............................................................................
  for (unsigned int i = 0; i < num_jobs; i++)
  {
    std::string read_buff;
    close(pipefd[i][1]);   //close writing end
    // Read first the size of the result object and then the object itself
    read(pipefd[i][0], &buff_size, sizeof(buff_size));
    read_buff.resize(buff_size);
    read(pipefd[i][0], &read_buff[0], buff_size);
    Result* proc_result = new Result();
    ResultProto& ll_result = proc_result->GetPbResult();
    ll_result.ParseFromString(read_buff);
    merged_result.Merge(*proc_result);
    delete proc_result;
    waitpid(cpid[i], NULL, 0);   //wait child process
    close(pipefd[i][0]);         //close reading end
    cout << "Finish parent wait" << endl;
  }

  //............................................................................
  // Free memory
  //............................................................................
  for (unsigned int i = 0; i < num_jobs; i++)
  {
    delete pipefd[i];
  }

  delete[] pipefd;
  delete[] cpid;
  // Write the configuration and final result object to the file
  ProtoWriter writer(outputFile);

  if (!writer(config.GetPbConfig()) || !writer(merged_result.GetPbResult()))
  {
    cerr << "Error while trying to write config and result objects to file. " <<
         endl;
    exit(-1);
  }
}


//------------------------------------------------------------------------------
// Do a run using the configuration supplied
//------------------------------------------------------------------------------
void
RunConfiguration(const string& configFile, const string& outputFile)
{
  Configuration config;

  if (!config.ReadFromFile(configFile))
  {
    cerr << "Could not read configuration from the input file." << endl;
    exit(-1);
  }

  ConfigProto pb_config = config.GetPbConfig();

  // Check that the path and files exist
  if (!config.CheckDirAndFiles())
  {
    cerr << "Failed while checking dir and files." << endl;
    exit(-1);
  }

  // Start processing using either threads or processes
  if (pb_config.jobtype() == ConfigProto_JobType_THREAD)
  {
    RunThreadConfig(config, outputFile);
  }
  else if (pb_config.jobtype() == ConfigProto_JobType_PROCESS)
  {
    RunProcessConfig(config, outputFile);
  }
}


//------------------------------------------------------------------------------
// Print results from file filtering by the configuration
//------------------------------------------------------------------------------
void
PrintResults(const string& resultsFile, const string& configFile)
{
  if (resultsFile.empty())
  {
    cerr << "Results file is empty." << endl;
    return;
  }

  Configuration* reference_config = 0;
  typedef std::map< size_t, std::pair< Configuration*, Result* > >
  MapConfigResults;

  if (!configFile.empty())
  {
    reference_config = new Configuration();

    if (!reference_config->ReadFromFile(configFile))
    {
      cerr << "Failed to read config from file." << endl;
      delete reference_config;
      return;
    }

    reference_config->Print();
  }

  size_t hash;
  Configuration* current_config;
  Result* current_result;
  ConfigProto* pb_config;
  ResultProto* pb_result;
  MapConfigResults map_config;
  ProtoReader reader(resultsFile);

  do
  {
    current_config = new Configuration();
    current_result = new Result();
    pb_config = reader.ReadNext<ConfigProto>();
    pb_result = reader.ReadNext<ResultProto>();

    if (!pb_config || !pb_result)
    {
      delete current_config;
      delete current_result;
      break;
    }

    current_config->SetPbConfig(pb_config);
    current_result->SetPbResult(pb_result);
    hash = current_config->GetHash();

    // If configuration already in map then just append the new result
    if (map_config.count(hash))
    {
      Result* ptr_result = map_config[hash].second;
      ptr_result->Merge(*current_result);
      delete current_config;
      delete current_result;
    }
    else
    {
      map_config[hash] = std::make_pair(current_config, current_result);
    }
  }
  while (1);

  // Print the results matching the configuration supplied
  if (reference_config)
  {
    hash = reference_config->GetHash();

    if (map_config.count(hash))
    {
      Result* ptr_result = map_config[hash].second;
      ptr_result->Print();
    }
    else
    {
      cout << "No matching configuration in the supplied file." << endl;
    }
  }
  else
  {
    // If there is no reference config then we print all
    for (MapConfigResults::iterator iter = map_config.begin();
         iter != map_config.end(); iter++)
    {
      current_config = iter->second.first;
      current_result = iter->second.second;
      current_config->Print();
      current_result->Print();
      delete current_config;
      delete current_result;
    }
  }

  // Free allocated memory
  for (MapConfigResults::iterator iter = map_config.begin();
       iter != map_config.end(); iter++)
  {
    delete iter->second.first;
    delete iter->second.second;
  }

  map_config.clear();
  delete reference_config;
  return;
}

EOSBMKNAMESPACE_END

using namespace eos::benchmark;

//------------------------------------------------------------------------------
// Main function
//------------------------------------------------------------------------------
int main(int argc, char* argv[])
{
  if (argc < 2)
  {
    Usage();
    exit(-1);
  }

  //..........................................................................
  // When running in process mode, we have to set XRD_ENABLEFORKHANDLERS=1
  // which amounts to using the below of the new XrdCl
  //..........................................................................
  XrdCl::Env* env = XrdCl::DefaultEnv::GetEnv();
  env->PutInt("RunForkHandler", 1);
  bool done_work = false; // true when creating or listing a configurate
  bool do_run    = false; // mark if we are doing a run on a configuration
  bool do_print  = false; // mark if we are to print the results from a file
  string configFile;      // file name holding the run configuration
  string resultsFile;     // file name holding the run results
  string outputFile;      // file name where the run results are to saved
  // Set up the loggin infrastructure
  eos::common::Mapping::VirtualIdentity_t vid;
  eos::common::Mapping::Root(vid);
  eos::common::Logging::Init();
  eos::common::Logging::SetUnit("bmk@localhost");
  eos::common::Logging::gShortFormat = true;
  XrdOucString bmk_debug = getenv("EOS_BMK_DEBUG");

  if ((getenv("EOS_BMK_DEBUG")) && (bmk_debug != "0"))
  {
    eos::common::Logging::SetLogPriority(LOG_DEBUG);
  }
  else
  {
    eos::common::Logging::SetLogPriority(LOG_INFO);
  }

  FILE* fstderr;

  // Open log file
  if (getuid())
  {
    char logfile[1024];
    snprintf(logfile, sizeof(logfile) - 1, "/tmp/eos-fuse.%d.log", getuid());

    // Running as a user ... we log into /tmp/eos-fuse.$UID.log
    if (!(fstderr = freopen(logfile, "a+", stderr)))
    {
      fprintf(stderr, "error: cannot open bmk log file %s\n", logfile);
    }
  }
  else
  {
    // Running as root ... we log into /var/log/eos/fuse
    eos::common::Path cPath("/var/log/eos/bmk/bmk.log");
    cPath.MakeParentPath(S_IRWXU | S_IRGRP | S_IROTH);

    if (!(fstderr = freopen(cPath.GetPath(), "a+", stderr)))
    {
      fprintf(stderr, "error: cannot open bmk log file %s\n", cPath.GetPath());
    }
  }

  while (1)
  {
    static struct option long_options[] =
    {
      {"create-config", required_argument, 0, 'c'},
      {"list-config",   required_argument, 0, 'l'},
      {"list-results",  required_argument, 0, 'p'},
      {"config",        required_argument, 0, 'f'},
      {"run-config",    required_argument, 0, 'r'},
      {"output",        required_argument, 0, 'o'},
      {"help",          no_argument,       0, 'h'},
      {0, 0, 0, 0}
    };
    // getopt_long stores the option index here
    int option_index = 0;
    int c = getopt_long(argc, argv, "c:l:p:f:r:o:h",
                        long_options, &option_index);

    // Detect the end of the options
    if (c == -1)
      break;

    switch (c)
    {
    case 'c':
    {
      configFile = optarg;
      cout << "Create configuration option with file: " << configFile << endl;
      Configuration config;
      config.CreateConfigFile(configFile);
      done_work = true;
      break;
    }

    case 'l':
    {
      configFile = optarg;
      cout << "Print configuration file: " << configFile << endl;
      Configuration config;

      if (!config.ReadFromFile(configFile))
      {
        cerr << "Failed to read configuration from file: " << configFile << endl;
        exit(-1);
      }

      config.Print();
      done_work = true;
      break;
    }

    case 'p':
    {
      resultsFile = optarg;
      do_print = true;
      break;
    }

    case 'f':
    {
      configFile = optarg;
      cout << "Filter only the ones matching configuration: "
           << configFile << endl;
      break;
    }

    case 'r':
    {
      configFile = optarg;
      cout << "Run configuration: "
           << configFile << endl;
      do_run = true;
      break;
    }

    case 'o':
    {
      outputFile = optarg;
      cout << "Output file for the run : "
           << outputFile << endl;
      break;
    }

    case '?':
    {
      // getopt_long already printed an error message
      break;
    }

    default:
      exit(-1);
    }
  }

  if (!done_work)
  {
    if (do_run)
    {
      if (outputFile.empty())
      {
        cout << "No output file specified." << endl;
        Usage();
      }
      else
      {
        // We are about to run a configuration
        RunConfiguration(configFile, outputFile);
      }
    }
    else if (do_print)
    {
      // Print the results from a file optionally matching the supplied config
      PrintResults(resultsFile, configFile);
    }
    else
    {
      Usage();
    }
  }

  fclose(fstderr);
  return 0;
}

