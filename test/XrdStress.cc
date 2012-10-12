//------------------------------------------------------------------------------
// File: XrdStress.cc
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
#include <iostream>
#include <sstream>
#include <fstream>
#include <vector>
#include <set>
/*-----------------------------------------------------------------------------*/
#include <stdlib.h>
#include <unistd.h>
#include <uuid/uuid.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/time.h>
#include <math.h>
/*-----------------------------------------------------------------------------*/
#include "XrdStress.hh"
#include <XrdPosix/XrdPosixXrootd.hh>
#include <XrdClient/XrdClient.hh>
#include <XrdClient/XrdClientEnv.hh>
/*-----------------------------------------------------------------------------*/

XrdPosixXrootd posixXrootd;  ///< xrootd posix instance

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
XrdStress::XrdStress( unsigned int nChilds,
                      unsigned int nFiles,
                      size_t       sBlock,
                      off_t        sFile,
                      std::string  pTest,
                      std::string  op,
                      bool         verb,
                      bool         useProcess,
                      bool         concurrent):
  verbose( verb ),
  processMode( useProcess ),
  concurrentMode(concurrent ),
  sizeFile( sFile ),
  sizeBlock( sBlock ),
  numChilds( nChilds ),
  numFiles( nFiles ),
  pathTest( pTest ),
  opType( op )
{
  if ( processMode ) {
    childType = "process";
    //..........................................................................
    // When running in process mode, we have to set XRD_ENABLEFORKHANDLERS=1
    //..........................................................................
    if ( setenv( "XRD_ENABLEFORKHANDLERS", "1", 1 ) ) {
      fprintf( stderr, "Error while trying to set XRD_ENABLEFORKHANDLERS. \n" );
      exit( 1 );
    }
  } else {
    childType = "thread";

    for ( unsigned int i = 0; i < numChilds; i++ ) {
      pthread_t thread;
      vectChilds.push_back( thread );
    }
  }

  //............................................................................
  // Generate the name of the files only in WR or RDWR mode
  //............................................................................
  if ( opType == "wr" || opType == "rdwr" ) {
    std::string gen_filename;
    uuid_t genUuid;
    char charUuid[40];
    vectFilename.reserve( numChilds * numFiles );

    if ( concurrentMode ) {
      //........................................................................
      // Generate file names for first job
      //........................................................................
      for ( unsigned int idf = 0; idf < numFiles ; idf++ ) {
        uuid_generate_time( genUuid );
        uuid_unparse( genUuid, charUuid );
        gen_filename = pathTest;
        gen_filename += charUuid;
        vectFilename.push_back( gen_filename );
      }

      //........................................................................
      // For the rest of the jobs copy the file names form the first one
      //........................................................................
      for ( unsigned int idj = 1; idj < numChilds; idj++ ) {
        for ( unsigned int idf = 0; idf < numFiles ; idf++ ) {
          vectFilename.push_back( vectFilename[idf] );
        }
      }
    }
    else {
      //........................................................................
      // In non-concurrent mode all jobs operate on different files
      //........................................................................
      for ( unsigned int indx = 0; indx < ( numChilds * numFiles ); indx++ ) {
        uuid_generate_time( genUuid );
        uuid_unparse( genUuid, charUuid );
        gen_filename = pathTest;
        gen_filename += charUuid;
        vectFilename.push_back( gen_filename );
      }
    }
  } else if ( opType == "rd" ) {
    //..........................................................................
    // If no files in vect. then read the files from the directory
    //..........................................................................
    unsigned int num_entries = GetListFilenames();
    
    if ( num_entries == 0 ) {
      fprintf( stderr, "error=no files in directory.\n" );
      exit( 1 );
    }

    if ( concurrentMode ) {
      if ( num_entries > numFiles ) {
        //......................................................................
        // Jobs will run (concurrently) only on the first numFiles
        //......................................................................
        while ( vectFilename.size() >= numFiles ) {
          vectFilename.pop_back();
        }
      }

      //........................................................................
      // Duplicate the file names form the first job to all the others
      //........................................................................
      for ( unsigned int idj = 1; idj < numChilds; idj++ ) {
        for ( unsigned int idf = 0; idf < numFiles ; idf++ ) {
          vectFilename.push_back( vectFilename[idf] );
        }
      }      
    }
    else {
      //..........................................................................
      // If not in concurrent mode and not enough files in dir. set the new no.
      // of files so that each job receives the same number of different files
      // Each file is processed only once, by only one job.
      //..........................................................................
      if ( ( ( num_entries / numChilds ) != numFiles ) ) {
        numFiles = ceil( num_entries / numChilds );
      }
    }
  }

  //............................................................................
  // Reserve space in vectors for statistics
  //............................................................................
  avgRdRate.reserve( numChilds );
  avgWrRate.reserve( numChilds );
  avgOpen.reserve( numChilds );

  //............................................................................
  // Set the type of the function call
  //............................................................................
  if ( opType == "rd" ) {
    callback = XrdStress::RdProc;
  } else if ( opType == "wr" ) {
    callback = XrdStress::WrProc;
  } else if ( opType == "rdwr" ) {
    callback = XrdStress::RdWrProc;
  }
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
XrdStress::~XrdStress()
{
  vectChilds.clear();
  avgRdRate.clear();
  avgWrRate.clear();
  avgOpen.clear();
  vectFilename.clear();

  if ( processMode ) {
    //..........................................................................
    // When running in process mode, we have to unset XRD_ENABLEFORKHANDLERS=1
    //..........................................................................
    if ( unsetenv( "XRD_ENABLEFORKHANDLERS" ) ) {
      fprintf( stderr, "Error while trying to unset XRD_ENABLEFORKHANDLERS. \n" );
    }
  }
}


//------------------------------------------------------------------------------
// Generic function to run tests in thread/process mode
//------------------------------------------------------------------------------
void
XrdStress::RunTest()
{
  if ( processMode ) {
    RunTestProcesses();
  } else {
    RunTestThreads();
    WaitThreads();
  }
}


//------------------------------------------------------------------------------
// Run tests using threads
//------------------------------------------------------------------------------
void
XrdStress::RunTestThreads()
{
  for ( unsigned int i = 0; i < numChilds; i++ ) {
    ChildInfo* ti = ( ChildInfo* ) calloc( 1, sizeof( ChildInfo ) );
    ti->idChild = i;
    ti->pXrdStress = this;
    ThreadStart( vectChilds[i], ( *callback ), ( void* ) ti );
  }
}


//------------------------------------------------------------------------------
// Run tests using processes
//------------------------------------------------------------------------------
void
XrdStress::RunTestProcesses()
{
  //............................................................................
  // Use pipes to send back information to parent
  //............................................................................
  int** pipefd = ( int** ) calloc( numChilds, sizeof( int* ) );

  for ( unsigned int i = 0; i < numChilds; i++ ) {
    pipefd[i] = ( int* ) calloc( 2, sizeof( int ) );

    if ( pipe( pipefd[i] ) == -1 ) {
      fprintf( stderr, "error=error opening pipe.\n" );
      exit( 1 );
    }
  }

  pid_t* cpid = ( pid_t* ) calloc( numChilds, sizeof( pid_t ) );

  for ( unsigned int i = 0; i < numChilds; i++ ) {
    cpid[i] = fork();

    if ( cpid[i] == -1 ) {
      fprintf( stdout, "error=error in fork().\n" );
      exit( 1 );
    }

    if ( cpid[i] == 0 ) { //child process
      char writebuffer[30];
      close( pipefd[i][0] );    //close reading end
      ChildInfo* info = ( ChildInfo* ) calloc( 1, sizeof( ChildInfo ) );
      info->pXrdStress = this;
      info->idChild = i;
      info->avgRdVal = 0;
      info->avgWrVal = 0;
      info->avgOpenVal = 0;

      //........................................................................
      // Call function
      //........................................................................
      ( *callback )( info );

      if ( opType == "rd" ) {
        sprintf( writebuffer, "%g %g\n", info->avgRdVal, info->avgOpenVal );
      } else if ( opType == "wr" ) {
        sprintf( writebuffer, "%g %g\n", info->avgWrVal, info->avgOpenVal );
      } else if ( opType == "rdwr" ) {
        sprintf( writebuffer, "%g %g %g\n", info->avgWrVal, info->avgRdVal, info->avgOpenVal );
      }

      write( pipefd[i][1], writebuffer, strlen( writebuffer ) );
      free( info );
      close( pipefd[i][1] );  //close writing end
      exit( EXIT_SUCCESS );
    }
  }

  //............................................................................
  // Parent process
  //............................................................................
  for ( unsigned int i = 0; i < numChilds; i++ ) {
    char readbuffer[30];
    close( pipefd[i][1] );   //close writing end
    read( pipefd[i][0], readbuffer, sizeof( readbuffer ) );
    std::stringstream ss( std::stringstream::in | std::stringstream::out );
 
    if ( opType == "rd" ) {
      ss << readbuffer;
      ss >> avgRdRate[i];
      ss >> avgOpen[i];
    } else if ( opType == "wr" ) {
      ss << readbuffer;
      ss >> avgWrRate[i];
      ss >> avgOpen[i];
    } else if ( opType == "rdwr" ) {
      ss << readbuffer;
      ss >> avgWrRate[i];
      ss >> avgRdRate[i];
      ss >> avgOpen[i];
    }

    waitpid( cpid[i], NULL, 0 ); //wait child process
    close( pipefd[i][0] );       //close reading end
  }

  //............................................................................
  // Free memory
  //............................................................................
  for ( unsigned int i = 0; i < numChilds; i++ ) {
    free( pipefd[i] );
  }

  free( pipefd );
  free( cpid );
  ComputeStatistics();
}


//------------------------------------------------------------------------------
// Wait for all threads to finish
//------------------------------------------------------------------------------
void
XrdStress::WaitThreads()
{
  for ( unsigned int i = 0; i < numChilds; i++ ) {
    ChildInfo* arg;
    pthread_join( vectChilds[i], ( void** )&arg );
    free( arg );
  }

  ComputeStatistics();
}


//------------------------------------------------------------------------------
// Start thread executing a particular function
//------------------------------------------------------------------------------
int
XrdStress::ThreadStart( pthread_t& thread, TypeFunc func, void* arg )
{
  return pthread_create( &thread, NULL, func, arg );
}


//------------------------------------------------------------------------------
// Compute statistics
//------------------------------------------------------------------------------
void
XrdStress::ComputeStatistics()
{
  double rd_mean, wr_mean, open_mean = 0;
  double rd_std, wr_std;

  for ( unsigned int i = 0; i < numChilds; i++ ) {
    open_mean += avgOpen[i];
  }

  open_mean /= numChilds;

  if ( opType == "rd" ) {
    rd_std = GetStdDev( avgRdRate, rd_mean );
    fprintf( stdout, "info=\"all %s read info\" mean=%g MB/s, stddev=%g open/s=%g \n",
             childType.c_str(), rd_mean, rd_std, open_mean );
  } else if ( opType == "wr" ) {
    wr_std = GetStdDev( avgWrRate, wr_mean );
    fprintf( stdout, "info=\"all %s write info\" mean=%g MB/s, stddev= %g open/s=%g \n",
             childType.c_str(), wr_mean, wr_std, open_mean );
  } else if ( opType == "rdwr" ) {
    rd_std = GetStdDev( avgRdRate, rd_mean );
    wr_std = GetStdDev( avgWrRate, wr_mean );
    fprintf( stdout, "info=\"all %s read info\" mean=%g MB/s stddev=%g open/s=%g \n",
             childType.c_str(), rd_mean, rd_std, open_mean );
    fprintf( stdout, "info=\"all %s write info\" mean=%g MB/s stddev= %g open/s=%g \n",
             childType.c_str(), wr_mean, wr_std, open_mean );
  }
}


//------------------------------------------------------------------------------
// Compute standard deviation and mean for the input provided
//------------------------------------------------------------------------------
double
XrdStress::GetStdDev( std::vector<double>& avg, double& mean )
{
  double std = 0;
  mean = 0;

  for ( unsigned int i = 0; i < numChilds; i++ ) {
    mean += avg[i];
  }

  mean = mean / numChilds;

  for ( unsigned int i = 0; i < numChilds; i++ ) {
    std += pow( ( avg[i] - mean ), 2 );
  }

  std /= numChilds;
  std = sqrt( std );
  return std;
}


//------------------------------------------------------------------------------
// Read the names of the files in the directory
//------------------------------------------------------------------------------
int
XrdStress::GetListFilenames()
{
  std::string file_path( "" );
  std::stringstream ssFileName( std::stringstream::in | std::stringstream::out );
  DIR* dir = XrdPosixXrootd::Opendir( pathTest.c_str() );
  struct dirent* dir_entry;
  unsigned int no = 0;

  while ( ( dir_entry = XrdPosixXrootd::Readdir( dir ) ) != NULL ) {
    file_path = pathTest;
    file_path += dir_entry->d_name;
    ssFileName << file_path;
    ssFileName << " ";
    no++;
  }

  vectFilename.clear();
  vectFilename.reserve( no );

  for ( unsigned int i = 0; i < no; i++ ) {
    ssFileName >> file_path;
    vectFilename.push_back( file_path );
  }

  return no;
}


//------------------------------------------------------------------------------
// Read procedure
//------------------------------------------------------------------------------
void*
XrdStress::RdProc( void* arg )
{
  bool change = true;
  int sample = 0;
  double rate = 0;
  double open_per_sec = 0;
  off_t sizeReadFile = 0;
  off_t total_offset = 0;
  unsigned int count_open = 0;
  ChildInfo* pti = static_cast<ChildInfo*>( arg );
  XrdStress* pxt = pti->pXrdStress;
  char* buffer = new char[pxt->sizeBlock];

  //............................................................................
  // Initialize time structures
  //............................................................................
  int deltaTime = DELTATIME;
  double duration = 0;
  struct timeval start, end;
  struct timeval time1, time2;
  gettimeofday( &start, NULL );
  gettimeofday( &time1, NULL );
  unsigned int startIndx = pti->idChild * pxt->numFiles;
  unsigned int endIndx = ( pti->idChild + 1 ) * pxt->numFiles;

  //............................................................................
  // Loop over all files corresponding to the current thread
  //............................................................................
  for ( unsigned int indx = startIndx; indx < endIndx ; indx++ ) {
    std::string urlFile = pxt->vectFilename[indx];
    struct stat buf;
    XrdPosixXrootd::Stat( urlFile.c_str(), &buf );
    sizeReadFile = buf.st_size;
    count_open++;
    int fdWrite = XrdPosixXrootd::Open( urlFile.c_str(), O_RDONLY,
                                        kXR_ur | kXR_uw | kXR_gw | kXR_gr | kXR_or );

    if ( fdWrite < 0 ) {
      fprintf( stderr, "error=error while opening read file: %s.\n", urlFile.c_str() );
      delete[] buffer;
      free( arg );
      exit( 1 );
    }

    //..........................................................................
    // Read from file
    //..........................................................................
    off_t offset = 0;
    unsigned long long noBlocks = sizeReadFile / pxt->sizeBlock;
    size_t lastRead = sizeReadFile % pxt->sizeBlock;

    for ( unsigned long long i = 0 ; i < noBlocks ; i++ ) {
      XrdPosixXrootd::Pread( fdWrite, buffer, pxt->sizeBlock, offset );
      offset += pxt->sizeBlock;
    }

    if ( lastRead ) {
      XrdPosixXrootd::Pread( fdWrite, buffer, lastRead, offset );
      offset += lastRead;
    }

    total_offset += offset;

    if ( pxt->verbose ) {
      if ( change ) { //true
        gettimeofday( &time2, NULL );
        duration = ( time2.tv_sec - time1.tv_sec ) + ( ( time2.tv_usec - time1.tv_usec ) / 1e6 );

        if ( duration > deltaTime ) {
          sample++;
          change = !change;
          duration = ( time2.tv_sec - start.tv_sec ) + ( ( time2.tv_usec - start.tv_usec ) / 1e6 );
          open_per_sec = ( double )count_open / duration;
          rate = ( ( double )total_offset / ( 1024 * 1024 ) ) / duration;
          fprintf( stdout, "info=\"read partial\" %s=%i step=%i mean=%g MB/s open/s=%g \n",
                   pxt->childType.c_str(), pti->idChild, sample, rate, open_per_sec );
        }
      } else {   //false
        gettimeofday( &time1, NULL );
        duration = ( time1.tv_sec - time2.tv_sec ) + ( ( time1.tv_usec - time2.tv_usec ) / 1e6 );

        if ( duration > deltaTime ) {
          sample++;
          change = !change;
          duration = ( time1.tv_sec - start.tv_sec ) + ( ( time1.tv_usec - start.tv_usec ) / 1e6 );
          open_per_sec = ( double )count_open / duration;
          rate = ( ( double )total_offset / ( 1024 * 1024 ) ) / duration;
          fprintf( stdout, "info=\"read partial\" %s=%i step=%i mean=%g MB/s open/s=%g \n",
                   pxt->childType.c_str(), pti->idChild, sample, rate, open_per_sec );
        }
      }
    }

    XrdPosixXrootd::Close( fdWrite );
  }

  delete[] buffer;

  //............................................................................
  // Get overall values
  //............................................................................
  gettimeofday( &end, NULL );
  duration = ( end.tv_sec - start.tv_sec ) + ( ( end.tv_usec - start.tv_usec ) / 1e6 );
  rate = ( ( double )total_offset / ( 1024 * 1024 ) ) / duration;
  open_per_sec = static_cast<double>( count_open / duration );

  if ( pxt->verbose ) {
    fprintf( stdout, "info=\"read final\" %s=%i  mean=%g MB/s open/s=%g \n",
             pxt->childType.c_str(), pti->idChild, rate, open_per_sec );
  }

  pti->avgRdVal = rate;
  pxt->avgRdRate[pti->idChild] = rate;

  if ( pti->avgOpenVal != 0 ) {
    pti->avgOpenVal = ( pti->avgOpenVal + open_per_sec ) / 2;
  } else {
    pti->avgOpenVal = open_per_sec;
  }

  pxt->avgOpen[pti->idChild] = pti->avgOpenVal;
  return arg;
}


//------------------------------------------------------------------------------
// Write procedure
//------------------------------------------------------------------------------
void*
XrdStress::WrProc( void* arg )
{
  int sample = 0;
  bool change = true;
  double rate = 0;
  double open_per_sec = 0;
  unsigned int count_open = 0;
  off_t total_offset = 0;
  ChildInfo* pti = static_cast<ChildInfo*>( arg );
  XrdStress* pxt = pti->pXrdStress;

  //............................................................................
  // Fill buffer with random characters
  //............................................................................
  char* buffer = new char[pxt->sizeBlock];
  std::ifstream urandom( "/dev/urandom", std::ios::in | std::ios::binary );
  urandom.read( buffer, pxt->sizeBlock );
  urandom.close();

  //............................................................................
  // Initialize time structures
  //............................................................................
  float duration = 0;
  int deltaTime = DELTATIME;
  struct timeval start, end;
  struct timeval time1, time2;
  
  gettimeofday( &start, NULL );
  gettimeofday( &time1, NULL );

  unsigned int startIndx = pti->idChild * pxt->numFiles;
  unsigned int endIndx = ( pti->idChild + 1 ) * pxt->numFiles;

  //............................................................................
  // Loop over all files corresponding to the current job
  //............................................................................
  for ( unsigned int indx = startIndx; indx < endIndx ; indx++ ) {
    std::string urlFile = pxt->vectFilename[indx];
    count_open++;
    int fdWrite = XrdPosixXrootd::Open( urlFile.c_str(),
                                        kXR_async | kXR_mkpath | kXR_open_updt | kXR_new,
                                        kXR_ur | kXR_uw | kXR_gw | kXR_gr | kXR_or );

    if ( fdWrite < 0 ) {
      fprintf( stderr,  "error=error while opening write file.\n " );
      delete[] buffer;
      free( arg );
      exit( 1 );
    }

    //..........................................................................
    // Write to file
    //..........................................................................
    size_t offset = 0;
    unsigned long long noBlocks = pxt->sizeFile / pxt->sizeBlock;
    size_t lastWrite = pxt->sizeFile % pxt->sizeBlock;

    for ( unsigned long long i = 0 ; i < noBlocks ; i++ ) {
      XrdPosixXrootd::Pwrite( fdWrite, buffer, pxt->sizeBlock, offset );
      offset += pxt->sizeBlock;
    }

    if ( lastWrite ) {
      XrdPosixXrootd::Pwrite( fdWrite, buffer, lastWrite, offset );
      offset += lastWrite;
    }

    total_offset += offset;

    if ( pxt->verbose ) {
      if ( change ) { //true
        gettimeofday( &time2, NULL );
        duration = ( time2.tv_sec - time1.tv_sec ) + ( ( time2.tv_usec - time1.tv_usec ) / 1e6 );

        if ( duration > deltaTime ) {
          sample++;
          change = !change;
          duration = ( time2.tv_sec - start.tv_sec ) + ( ( time2.tv_usec - start.tv_usec ) / 1e6 );
          open_per_sec = ( double )count_open / duration;
          rate = ( ( double )total_offset / ( 1024 * 1024 ) ) / duration;
          fprintf( stdout, "info=\"write partial\" %s=%i step=%i mean=%g MB/s open/s=%g \n",
                   pxt->childType.c_str(), pti->idChild, sample, rate, open_per_sec );
        }
      } else {   //false
        gettimeofday( &time1, NULL );
        duration = ( time1.tv_sec - time2.tv_sec ) + ( ( time1.tv_usec - time2.tv_usec ) / 1e6 );

        if ( duration > deltaTime ) {
          sample++;
          change = !change;
          duration = ( time1.tv_sec - start.tv_sec ) + ( ( time1.tv_usec - start.tv_usec ) / 1e6 );
          open_per_sec = ( double )count_open / duration;
          rate = ( ( double )total_offset / ( 1024 * 1024 ) ) / duration;
          fprintf( stdout, "info=\"write partial\" %s=%i step=%i mean=%g MB/s open/s=%g \n",
                   pxt->childType.c_str(), pti->idChild, sample, rate, open_per_sec );
        }
      }
    }
   
    XrdPosixXrootd::Close( fdWrite );    
  }

  delete[] buffer;

  //............................................................................
  // Get overall values
  //............................................................................
  gettimeofday( &end, NULL );
  duration = ( end.tv_sec - start.tv_sec ) + ( ( end.tv_usec - start.tv_usec ) / 1e6 );
  open_per_sec = static_cast<double>( count_open / duration );
  rate = ( ( double )total_offset / ( 1024 * 1024 ) ) / duration;

  if ( pxt->verbose ) {
    fprintf( stdout, "info=\"write final\" %s=%i mean=%g MB/s open/s=%g \n",
             pxt->childType.c_str(), pti->idChild, rate, open_per_sec );
  }

  pxt->avgWrRate[pti->idChild] = rate;
  pti->avgWrVal = rate;
  pti->avgOpenVal = open_per_sec;
  pxt->avgOpen[pti->idChild] = pti->avgOpenVal;
  return arg;
}


//------------------------------------------------------------------------------
// Read and write procedure
//------------------------------------------------------------------------------
void*
XrdStress::RdWrProc( void* arg )
{
  WrProc( arg );
  RdProc( arg );
  return arg;
}


//------------------------------------------------------------------------------
// Main function
//------------------------------------------------------------------------------
int main( int argc, char* argv[] )
{
  int c;
  bool verbose = false;
  bool process_mode = false;
  bool concurrent_mode = false;
  unsigned int num_jobs = 0;
  unsigned int num_files = 0;
  std::string sTmp( "" );
  std::string path( "" );
  std::string op_type( "" );
  std::string testName( "" );
  size_t size_block = 1024 * 1024;         //1MB
  size_t size_file = 100 * 1024 * 1024;    //100MB  -  default values
  std::string usage = "Usage:  xrdstress -d <dir path>\
                               \n\t\t -o <rd/wr/rdwr>\
                               \n\t\t -j <num_jobs>\
                               \n\t\t -f <num_files>\
                               \n\t\t [-b <size_block: 1KB, 1MB>]\
                               \n\t\t [-s <size_file: 1KB, 1MB>]\
                               \n\t\t [-c run in concurrent mode \
                               \n\t\t [-n <testName>]   \
                               \n\t\t [-v verbose]\
                               \n\t\t [-p use processes]\
                               \n\t\t [-h display help] \n";
  const std::string arrayOp[] = {"rd" , "wr", "rdwr"};
  std::set<std::string> setOp( arrayOp, arrayOp + 3 );

  while ( ( c = getopt( argc, argv, "d:o:j:f:s:b:n:vphc" ) ) != -1 ) {
    switch ( c ) {
    case 'h': { // display help information
      std::cout << usage << std::endl;
      exit( 1 );
    }

    case 'c': { // run in concurrent mode i.e. all jobs access the same files
      concurrent_mode = true;
      break;
    }
      
    case 'j': { //no. of jobs
      num_jobs = static_cast<unsigned int>( atoi( optarg ) );
      break;
    }

    case 'd': { //directory path
      path = optarg;
      //........................................................................
      // Check path to see if it extsts
      //........................................................................
      struct stat buff;
      int ret = XrdPosixXrootd::Stat( path.c_str(), &buff );

      if ( ret != 0 ) {
        std::cout << "The path requested does not exists. Xrootd::stat failed." << std::endl
                  << usage << std::endl;
        exit( 1 );
      }

      break;
    }

    case 'o': { //operation type
      op_type = optarg;

      if ( setOp.find( op_type ) == setOp.end() ) {
        std::cout << "Type of operation unknown. " << std::endl
                  << usage << std::endl;
        exit( 1 );
      }

      break;
    }

    case 'n': { //test name
      testName = optarg;
      break;
    }

    case 's': { //size file
      sTmp = optarg;
      std::string sNo = sTmp.substr( 0, sTmp.size() - 2 );
      std::string sBytes = sTmp.substr( sTmp.size() - 2 );

      if ( sBytes == "KB" ) {
        size_file = atoi( sNo.c_str() ) * 1024;
      } else if ( sBytes == "MB" ) {
        size_file = atoi( sNo.c_str() ) * 1024 * 1024;
      }

      break;
    }

    case 'b': { //size block
      sTmp = optarg;
      std::string sNo = sTmp.substr( 0, sTmp.size() - 2 );
      std::string sBytes = sTmp.substr( sTmp.size() - 2 );

      if ( sBytes == "KB" ) {
        size_block = atoi( sNo.c_str() ) * 1024;
      } else if ( sBytes == "MB" ) {
        size_block = atoi( sNo.c_str() ) * 1024 * 1024;
      }

      break;
    }

    case 'f': { //number of files
      num_files = atoi( optarg );
      break;
    }

    case 'v': { //verbose mode
      verbose = true;
      break;
    }

    case 'p': { //run with processes or threads
      process_mode = true;
      break;
    }

    case ':': {
      std::cout << usage << std::endl;
      exit( 1 );
      break;
    }
    }
  }
  
  //............................................................................
  // If one of the critical params. is missing exit
  //............................................................................
  if ( ( path == "" ) || ( op_type == "" ) || ( num_jobs == 0 ) || ( num_files == 0 ) ) {
    std::cout << usage << std::endl;
    exit( 1 );
  }

  //............................................................................
  // Generate uuid for test name if none provided
  //............................................................................
  if ( testName == "" ) {
    uuid_t genUuid;
    char charUuid[40];
    uuid_generate_time( genUuid );
    uuid_unparse( genUuid, charUuid );
    testName = charUuid;
  }

  //............................................................................
  // Construct full path
  //............................................................................
  if ( path.rfind( "/" ) != path.size() ) {
    path += "/";
  }

  path += testName;
  path += "/";
  std::cout << "Directory path = " << path << " using block size for operations of: "
            << ( size_block / 1024 ) << " KB" << std::endl <<  std::endl;
  
  mode_t mode = kXR_ur | kXR_uw | kXR_gw | kXR_gr | kXR_or;
  XrdPosixXrootd::Mkdir( path.c_str(), mode );
  XrdStress* test = new XrdStress( num_jobs, num_files,
                                   size_block, size_file,
                                   path, op_type, verbose,
                                   process_mode, concurrent_mode );
  test->RunTest();
  delete test;
}
