//------------------------------------------------------------------------------
//! @file XrdStress.cc
//! @author Elvin-Alin Sindrilaru - CERN
//! @brief Class capable of doing a stress test ( read/write operations) on the 
//!        files of a directory using either threads or processes.
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
#include <string>
#include <pthread.h>
/*-----------------------------------------------------------------------------*/

#define DELTATIME 10                ///< print statistics every 10 seconds

typedef void* ( *TypeFunc )( void* );
class XrdStress;


//------------------------------------------------------------------------------
//! Struct ChildInfo
//------------------------------------------------------------------------------
struct ChildInfo {
  int        idChild;       ///< child id
  XrdStress* pXrdStress;    ///< pointer to the test class
  double     avgRdVal;      ///< avg read value for current thread
  double     avgWrVal;      ///< avg write value for current thread
  double     avgOpenVal;    ///< avg open value for current thread
};


//------------------------------------------------------------------------------
//! Class XrdStress
//------------------------------------------------------------------------------
class XrdStress
{
  public:

    //--------------------------------------------------------------------------
    //! Constructor
    //--------------------------------------------------------------------------
    XrdStress( unsigned int nChilds,
               unsigned int nFiles,
               size_t       sBlock,
               off_t        sFile,
               std::string  pTest,
               std::string  op,
               bool         verb,
               bool         processmode,
               bool         concurrent );



    //--------------------------------------------------------------------------
    //! Destructor
    //--------------------------------------------------------------------------
    ~XrdStress();


    //--------------------------------------------------------------------------
    //! Genering function to start running tests
    //--------------------------------------------------------------------------
    void RunTest();

  private:

    bool         verbose;        ///< verbose mode on
    bool         processMode;    ///< run test using processes, else threads
    bool         concurrentMode; ///< all jobs process the same files
    off_t        sizeFile;       ///< size of file used for testing
    size_t       sizeBlock;      ///< block size for operations (rd/wr)
    TypeFunc     callback;       ///< type of operation executed
    unsigned int numChilds;      ///< number of children used ( threads/processes )
    unsigned int numFiles;       ///< number of files used for the test per child
    std::string  pathTest;       ///< directory where the testing takes place
    std::string  opType;         ///< type of operation ( rd/wr/rdwr )
    std::string  childType;      ///< type or children ( threads/processes )

    std::vector<double>      avgRdRate;    ///< avg read rate per child
    std::vector<double>      avgWrRate;    ///< avg write rate per child
    std::vector<double>      avgOpen;      ///< avg open operations per child
    std::vector<pthread_t>   vectChilds;   ///< vector of childs
    std::vector<std::string> vectFilename; ///< vector of all files used

  
    //--------------------------------------------------------------------------
    //! Run test using threads
    //--------------------------------------------------------------------------
    void RunTestThreads();


    //--------------------------------------------------------------------------
    //! Run test using processes
    //--------------------------------------------------------------------------
    void RunTestProcesses();


    //--------------------------------------------------------------------------
    //! Wait for all threads to finish
    //--------------------------------------------------------------------------
    void WaitThreads();


    //--------------------------------------------------------------------------
    //! Start thread excuting a particular function
    //!
    //! @param thread thread to be started
    //! @param func function to be executed by the thread
    //! @param arg aguments to the function
    //!
    //! @return 0 if successful, errno otherwise
    //!
    //--------------------------------------------------------------------------
    int ThreadStart( pthread_t& thread, TypeFunc func, void* arg );


    //--------------------------------------------------------------------------
    //!  Compute statistics for all jobs
    //--------------------------------------------------------------------------
    void ComputeStatistics();


    //--------------------------------------------------------------------------
    //! Compute standard deviation and mean for current input
    //!
    //! @param avg average value (rd/wr) per child
    //! @param mean the mean value of all jobs
    //!
    //! @return std. dev of all jobs
    //!
    //--------------------------------------------------------------------------
    double GetStdDev( std::vector<double>& avg, double& mean );


    //--------------------------------------------------------------------------
    //! Read the name of the files from the directory
    //!
    //! @return the number of files in the directory
    //!
    //--------------------------------------------------------------------------
    int GetListFilenames();


    //--------------------------------------------------------------------------
    //! Read procedure
    //!
    //! @param arg pointer to child info structure passed around
    //!
    //! @return pointer to updated child info stucture
    //!
    //--------------------------------------------------------------------------
    static void* RdProc( void* arg );


    //--------------------------------------------------------------------------
    //! Write procedure
    //!
    //! @param arg pointer to child info structure passed around
    //!
    //! @return pointer to updated child info stucture
    //!
    //--------------------------------------------------------------------------
    static void* WrProc( void* arg );


    //--------------------------------------------------------------------------
    //! Read-write procedure
    //!
    //! @param arg pointer to child info structure passed around
    //!
    //! @return pointer to updated child info stucture
    //!
    //--------------------------------------------------------------------------
    static void* RdWrProc( void* arg );

};

