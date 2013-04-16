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
/*-----------------------------------------------------------------------------*/
#include "eosbenchmark.hh"
#include "ProtoIo.hh"
#include "FileEos.hh"
#include "Result.hh"
#include "Configuration.hh"
#include "common/Path.hh"
#include "common/StringConversion.hh"
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
       << left << setw(50) << " List the configurations saved in the supplied file " << endl
       << left << setw(60) << " --run-config <config.file> --output <results.file>"
       << left << setw(50) << " Run configuration and write results in output file "<< endl
       << left << setw(60) << " --list-results <results.file> [--config <config.file>] "
       << left << setw(50) << " List only runs matching the configuration. If config" << endl
       << left << setw(60) << " "
       << left << setw(50) << " file is not present then it lists all runs " << endl
       << left << setw(60) << " --help "
       << left << setw(50) << " Print out this menu" << endl;
}


//------------------------------------------------------------------------------
// Start thread executing a particular function
//------------------------------------------------------------------------------
int
ThreadStart( pthread_t& thread, TypeFunc func, void* arg )
{
  return pthread_create( &thread, NULL, func, arg );
}


//------------------------------------------------------------------------------
// Start routine executed by each thread
//------------------------------------------------------------------------------
void*
StartRoutine( void* arg )
{
  int (FileEos::*operation_callback)(Result*&);  // define operation function pointer
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
      cerr << "[" << __FUNCTION__ << "]" << "No such operation on the file." << endl;
      exit(-1);     
  }

  // Create file objects
  uint32_t start_indx;
  uint32_t end_indx;

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
  Result* thread_result = new Result(); 
  
  for (uint32_t i = start_indx; i < end_indx; i++)
  {
    cout << "Execute operation for file: " << config.GetFileName(i)
         << " at index: " << i
         << " in thread: " <<  pthread_self()
         << endl;
    
    int retc = -1;
    FileEos* file = new FileEos(config.GetFileName(i),
                                pb_config.benchmarkinstance(),
                                pb_config.filesize(),
                                pb_config.blocksize());

    // Execute the required operation
    retc = (*file.*operation_callback)(thread_result);
    
    if (retc)
    {
      cerr << "[" << __FUNCTION__ << "]" << "Error while executing operation on file. " << endl;
      delete file;
      return NULL;
    }
      
    delete file;
  }

  // Delete the memory allocated for argument passing 
  delete arg_thread;

  // Display the statistics collected by the current thread
  //thread_result->Print();  
  return thread_result;
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
  
  // Start all threads and run the proper operations on the files
  for (uint32_t i = 0; i < pb_config.numjobs(); i++)
  {
    // Arguments passed to the thread are allocated dynamically and then
    // deleted by the thread in the StartRoutine method
    pthread_t thread;
    struct ConfIdStruct* arg_thread = new ConfIdStruct(config, i);
    vect_threads.push_back(thread);
    ThreadStart(vect_threads[i], StartRoutine, (void*) arg_thread);
  }

  // Join all threads and collect the results for the run
  for (uint32_t i = 0; i < pb_config.numjobs(); i++)
  {
    Result* ret_result;
    pthread_join(vect_threads[i], (void**) &ret_result);
    if ( ret_result != NULL )
    {
      merged_result.Merge(*ret_result);    
      delete ret_result;
    }
  }
  
  // Write the configuration and final result object to the file
  ProtoWriter writer(outputFile);
  merged_result.ComputeGroupStatistics();

  if (!writer(config.GetPbConfig()) || !writer(merged_result.GetPbResult()))
  {
    cerr << "[" << __FUNCTION__ << "]" << "Errot while trying to write "
         << " configuration and result objects to file. " << endl;
    exit(-1);      
  }
}


//------------------------------------------------------------------------------
// Run benchmark using process
//------------------------------------------------------------------------------
void
RunProcessConfig(const Configuration& config, const string& resultsFile)
{



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
    exit(-1);
  }

  ConfigProto pb_config = config.GetPbConfig();

  // Check that the path exists and if not create it
  if (!config.CheckDirAndFiles()) {
    cerr << "[" << __FUNCTION__ << "]" << "Failed while checking dir and files. " << endl;
    exit(-1);
  }
  
  // Start processing using either threads or processes
  if (pb_config.jobtype() == ConfigProto_JobType_THREAD)
  {
    RunThreadConfig(config, outputFile);
  }
  else if (pb_config.jobtype() == ConfigProto_JobType_THREAD)
  {
    RunProcessConfig(config, outputFile);
  }
  else
  {
    cerr << "[" << __FUNCTION__ << "]" << "No such job type. " << endl;
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
  typedef std::map< size_t, std::pair< Configuration*, std::vector<Result*> > > MapConfigResults;

  if (!configFile.empty())
  {
    reference_config = new Configuration();
    if (!reference_config->ReadFromFile(configFile))
    {
      cerr << "Failed to read config from file." << endl;
      return;
    }
  }

  Configuration* current_config = new Configuration();
  Result* current_result = new Result();
  ConfigProto* pb_config;
  ResultProto* pb_result;
  MapConfigResults map_configs;

  ProtoReader reader(resultsFile);
  do {
    pb_config = reader.ReadNext<ConfigProto>();
    pb_result = reader.ReadNext<ResultProto>();

    if (!pb_config || !pb_result) break;
    
    current_config->SetPbConfig(pb_config);
    current_result->SetPbResult(pb_result);

    current_config->Print();
    current_result->Print();
  }
  while (1);

  delete current_config;
  delete current_result;
}

EOSBMKNAMESPACE_END

using namespace eos::benchmark;

//------------------------------------------------------------------------------
// Main function 
//------------------------------------------------------------------------------
int main(int argc, char* argv[])
{
  if ( argc < 2 ) {
    Usage();
    exit(-1);
  }

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
    eos::common::Logging::SetLogPriority(LOG_DEBUG);
  }

  FILE* fstderr;
    
  //............................................................................
  // Open log file
  //..........................................................................
  if (getuid())
  {
    char logfile[1024];
    snprintf(logfile, sizeof ( logfile) - 1, "/tmp/eos-fuse.%d.log", getuid());

    //..........................................................................
    // Running as a user ... we log into /tmp/eos-fuse.$UID.log
    //..........................................................................
    if (!(fstderr = freopen(logfile, "a+", stderr)))
    {
      fprintf(stderr, "error: cannot open bmk log file %s\n", logfile);
    }
  }
  else
  {
    //..........................................................................
    // Running as root ... we log into /var/log/eos/fuse
    //..........................................................................
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
          {"create-config", required_argument, 0, 'a'},
          {"list-config",   required_argument, 0, 'b'},
          {"list-results",  required_argument, 0, 'c'},
          {"config",        required_argument, 0, 'd'},
          {"run-config",    required_argument, 0, 'e'},
          {"output",        required_argument, 0, 'f'},
          {"help",          no_argument,       0, 'h'},
          {0, 0, 0, 0}
        };

    // getopt_long stores the option index here
    int option_index = 0;
    int c = getopt_long (argc, argv, "a:b:c:d:e:f:h",
                         long_options, &option_index);
    
    // Detect the end of the options
    if (c == -1)
      break;
      
    switch (c)
    {
      case 'a':
        {
          configFile = optarg;
          cout << "Create configuration option with file: " << configFile << endl;
          Configuration config;
          config.CreateConfigFile(configFile);
          done_work = true;
          break;
        }

      case 'b':
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

      case 'c':
        {
          resultsFile = optarg;
          do_print = true;
          break;
        }

      case 'd':
        {
          configFile = optarg;
          cout << "Filter only the ones matching configuration: "
               << configFile << endl;
          break;
        }
        
      case 'e':
        {
          configFile = optarg;
          cout << "Run configuration: "
               << configFile << endl;
          do_run = true;
          break;
        }
        
      case 'f':
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

  if (!done_work) {
    if (do_run) {
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
      PrintResults(resultsFile, configFile);
    }
    else
    {
      Usage();
    }
  }

  //TODO: Deal with errors in any of the previous functions
}

