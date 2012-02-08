// ----------------------------------------------------------------------
// File: XrdStress.cc
// Author: Elvin-Alin Sindrilaru - CERN
// ----------------------------------------------------------------------

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
#include <cstring>
#include <iostream>
#include <sstream>
#include <fstream>
#include <vector>
#include <set>
/*-----------------------------------------------------------------------------*/
#include <unistd.h>
#include <uuid/uuid.h>
#include <pthread.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/time.h>
#include <math.h>
/*-----------------------------------------------------------------------------*/
#include <XrdPosix/XrdPosixXrootd.hh>
#include <XrdClient/XrdClient.hh>
#include <XrdClient/XrdClientEnv.hh>
/*-----------------------------------------------------------------------------*/

/******************************************************************************
 *                                                                            *
 * !!!!! WHEN RUNNING IN PROCEESS MODE: export XRD_ENABLEFORKHANDLERS=1 !!!!! *
 *                                                                            *
 ******************************************************************************/

#define DELTATIME 10                //print statistics every 10 seconds

typedef void* (*TypeFunc)(void*);   
class XrdStress;                    

struct ChildInfo
{
  int      idChild;
  XrdStress* pXrdStress;
  double   avgRdVal;
  double   avgWrVal;
  double   avgOpenVal;
};

XrdPosixXrootd posixXrootd;

class XrdStress
{
 public:
  
  /*-----------------------------------------------------------------------------*/
  XrdStress(unsigned int nChilds, unsigned int nFiles, size_t sBlock,
          off_t sFile, std::string pTest, std::string op, bool verb, bool processmode):
      verbose(verb),
      processMode(processmode),
      sizeFile(sFile),
      sizeBlock(sBlock),
      noChilds(nChilds),
      noFiles(nFiles),
      pathTest(pTest),
      opType(op)
  {
    if (processMode) {
      childType = "process";
    }
    else {
      childType = "thread";
    }
    
    //generate the name of the files only in WR or RDWR mode
    if (opType == "wr" || opType == "rdwr")
    {
      std::string genFilename;
      uuid_t genUuid;
      char charUuid[40];
      
      arrayFilename = (char**) calloc(noChilds * noFiles , sizeof(char*));
      for (unsigned int indx = 0; indx < (noChilds * noFiles); indx++)
      {
        arrayFilename[indx] = (char*) calloc(256, sizeof(char));
        uuid_generate_time(genUuid);
        uuid_unparse(genUuid, charUuid);
        genFilename = pathTest;
        genFilename += charUuid;
        arrayFilename[indx] = (char*) strncpy(arrayFilename[indx], genFilename.c_str(), genFilename.size());
        arrayFilename[indx][genFilename.size()] = '\0';
      }
    }
    else if (opType == "rd")
    {
      //if no files in vect. then read the files from the directory
      unsigned int tmpFiles = GetListFilenames();
      if (tmpFiles == 0) {
        fprintf(stderr,"error=no files in directory.\n");
        exit(1);
      }
      //if not enough files in dir. set the new no. of files 
      if (((tmpFiles / noChilds) != noFiles)) {
        noFiles = tmpFiles / noChilds;
      }      
    }

    //allocate threads and statistics arrays
    arrayChilds = (pthread_t*) calloc(noChilds, sizeof(pthread_t));
    avgRdRate = (double*) calloc(noChilds, sizeof(double));
    avgWrRate = (double*) calloc(noChilds, sizeof(double));
    avgOpen = (double*) calloc(noChilds, sizeof(double));
             
    //set the function call
    if (opType == "rd") {   
      callback = XrdStress::RdProc;
    }
    else if (opType == "wr") {
      callback = XrdStress::WrProc;
    }
    else if (opType == "rdwr") {
      callback = XrdStress::RdWrProc;
    }
  };

  
  /*-----------------------------------------------------------------------------*/
  ~XrdStress() {
    free(arrayChilds);
    free(avgRdRate);
    free(avgWrRate);
    free(avgOpen);
    for (unsigned int indx = 0; indx < noChilds * noFiles; indx++) {
      free(arrayFilename[indx]);
    }
    free(arrayFilename);
  };

  
  /*-----------------------------------------------------------------------------*/
  void RunTest()
  {
    if (processMode) {
      //using processes
      RunTestProcesses();
    }
    else {
      //using threads 
      RunTestThreads();
      WaitJobs();
    }
  }
 
 /*-----------------------------------------------------------------------------*/
 /*-----------------------------------------------------------------------------*/
 private:
  bool verbose;
  bool processMode;
  off_t sizeFile;
  size_t sizeBlock;
  TypeFunc callback;
  double* avgRdRate;
  double* avgWrRate;
  double* avgOpen;
  unsigned int noChilds;
  unsigned int noFiles;
  pthread_t* arrayChilds;
  std::string pathTest;
  std::string opType;
  std::string childType;
  char** arrayFilename;

  
  /*-----------------------------------------------------------------------------*/
  void RunTestThreads()
  {
    //start threads
    for (unsigned int i = 0; i < noChilds; i++)
    {
      ChildInfo* ti = (ChildInfo*) calloc(1, sizeof(ChildInfo));
      ti->idChild = i;
      ti->pXrdStress = this;
      ThreadStart(arrayChilds[i], (*callback), (void *) ti);
    }
  };


  /*-----------------------------------------------------------------------------*/
  void RunTestProcesses()
  {
    //use pipes to send back information to parent
    int **pipefd = (int**) calloc(noChilds, sizeof(int*));

    for (unsigned int i = 0; i < noChilds; i++)
    {
      pipefd[i] = (int*) calloc(2, sizeof(int));
      if (pipe(pipefd[i]) == -1)
        {
          fprintf(stderr, "error=error opening pipe.\n");
          exit(1);
        }
    }    
    
    pid_t* cpid = (pid_t*) calloc(noChilds, sizeof(pid_t));
    for (unsigned int i = 0; i < noChilds; i++)
    {
      cpid[i] = fork();
      if (cpid[i] == -1)
      {
        fprintf(stdout, "error=error in fork().\n");
        exit(1);
      }
     
      if (cpid[i] == 0)    //child process
      {
        char writebuffer[30];
        close(pipefd[i][0]);      //close reading end
        ChildInfo* info = (ChildInfo*) calloc(1, sizeof(ChildInfo));
        info->pXrdStress = this;
        info->idChild = i;
        info->avgRdVal = 0;
        info->avgWrVal = 0;
        info->avgOpenVal = 0;

        //call function
        (*callback)(info);           
        
        if (opType == "rd") {
          sprintf(writebuffer, "%g %g\n", info->avgRdVal, info->avgOpenVal);
        }
        else if (opType == "wr") {
          sprintf(writebuffer, "%g %g\n", info->avgWrVal, info->avgOpenVal);
        }
        else if (opType == "rdwr") {
          sprintf(writebuffer, "%g %g %g\n", info->avgWrVal, info->avgRdVal, info->avgOpenVal);
        }

        write(pipefd[i][1], writebuffer, strlen(writebuffer));
        free(info);
        close(pipefd[i][1]);    //close writing end
        exit(EXIT_SUCCESS);
      }
    }

    //parent process
    for (unsigned int i = 0; i < noChilds; i++)
    {
      char readbuffer[30];
      close(pipefd[i][1]);     //close writing end
      read(pipefd[i][0], readbuffer, sizeof(readbuffer));

      std::stringstream ss(std::stringstream::in | std::stringstream::out);
      if (opType == "rd") {
        ss << readbuffer;
        ss >> avgRdRate[i];
        ss >> avgOpen[i];
      }
      else if (opType == "wr") {
        ss << readbuffer;
        ss >> avgWrRate[i];
        ss >> avgOpen[i];
      }
      else if (opType == "rdwr")
      {        
        ss << readbuffer;
        ss >> avgWrRate[i];
        ss >> avgRdRate[i];
        ss >> avgOpen[i];
      }
      
      waitpid(cpid[i], NULL, 0);   //wait child process
      close(pipefd[i][0]);         //close reading end
    }

    //free memory
    for (unsigned int i = 0; i < noChilds; i++) {
       free(pipefd[i]);
    }
    free(pipefd);
    free(cpid);

    ComputeStatistics();      
  };

  
  /*-----------------------------------------------------------------------------*/
  void WaitJobs()
  {
    for (unsigned int i = 0; i < noChilds; i++)
    {
      ChildInfo* arg;
      pthread_join(arrayChilds[i], (void**)&arg);
      free(arg);
    }

    ComputeStatistics();
  };

  
  /*-----------------------------------------------------------------------------*/
  int ThreadStart(pthread_t& thread, TypeFunc f, void* arg)
  {
    return pthread_create(&thread, NULL, f, arg);
  };
 
  
  /*-----------------------------------------------------------------------------*/
  //compute std. deviation and mean
  void ComputeStatistics()
  {
    double rdMean, wrMean, openMean = 0;
    double rdStd, wrStd;

    for (unsigned int i = 0; i < noChilds; i++)
    {
      openMean += avgOpen[i];
    }

    openMean /= noChilds;
    
    if (opType == "rd")
    {
      rdStd = GetStdDev(avgRdRate, rdMean);
      fprintf(stdout, "info=\"%s read info\" mean=%g MB/s, stddev=%g ; open/s=%g \n", childType.c_str(), rdMean, rdStd, openMean);
    }
    else if (opType == "wr")
    {
      wrStd = GetStdDev(avgWrRate, wrMean);
      fprintf(stdout, "info=\"%s write info\" mean=%g MB/s, stddev= %g ; open/s=%g \n", childType.c_str(), wrMean, wrStd, openMean);
    }
    else if (opType == "rdwr")
    {
      rdStd = GetStdDev(avgRdRate, rdMean);
      wrStd = GetStdDev(avgWrRate, wrMean);
      fprintf(stdout, "info=\"%s read info\" mean=%g MB/s stddev=%g  open/s=%g \n", childType.c_str(), rdMean, rdStd, openMean);
      fprintf(stdout, "info=\"%s write info\" mean=%g MB/s stddev= %g  open/s=%g \n", childType.c_str(), wrMean, wrStd, openMean);
    }
  };

  
  /*-----------------------------------------------------------------------------*/
  double GetStdDev(double* avg, double& mean)
  {
    double std = 0;
    mean = 0;
    for (unsigned int i = 0; i < noChilds; i++) {
      mean += avg[i];
    }

    mean = mean / noChilds;
    for (unsigned int i = 0; i < noChilds; i++) {
      std += pow((avg[i] - mean), 2);
    }

    std /= noChilds;
    std = sqrt(std);
    return std;
  };

  
  /*-----------------------------------------------------------------------------*/
  //read the name of the files in the directory
  int GetListFilenames()
  {
    std::string dName("");
    std::stringstream ssFileName (std::stringstream::in | std::stringstream::out);
    DIR* dir = XrdPosixXrootd::Opendir(pathTest.c_str());
    struct dirent* dEntry;
   
    unsigned int no = 0;
    while ((dEntry = XrdPosixXrootd::Readdir(dir)) != NULL) {
      dName = pathTest;
      dName += dEntry->d_name;
      ssFileName << dName;
      ssFileName << " ";
      no++;
    }

    arrayFilename = (char**) calloc(no, sizeof(char*));
    for (unsigned int i = 0; i < no; i++)
    {
      arrayFilename[i] = (char*) calloc(256, sizeof(char));
      ssFileName >> dName;
      arrayFilename[i] = strncpy(arrayFilename[i], dName.c_str(), dName.size());
      arrayFilename[i][dName.size()] = '\0';
    }

    return no;
  };  

  
  /*-----------------------------------------------------------------------------*/
  static void* RdProc(void* arg)
  {
    double rate = 0;
    double openPerSec = 0;
    off_t sizeReadFile = 0;
    off_t totalOffset = 0;
    unsigned int noOpen = 0;
    ChildInfo* pti = static_cast<ChildInfo*>(arg); 
    XrdStress* pxt = pti->pXrdStress;
    size_t sizeBuff = pxt->sizeBlock + 1;
    char* buffer = new char[sizeBuff];
    bool change = true;

    //initialize time structures
    int deltaTime = DELTATIME;
    double duration = 0;
    struct timeval start, end;
    struct timeval time1, time2;

    gettimeofday(&start, NULL);
    gettimeofday(&time1, NULL);

    int sample = 0;
    unsigned int startIndx = pti->idChild * pxt->noFiles;
    unsigned int endIndx = (pti->idChild + 1) * pxt->noFiles;

    //loop over all files corresponding to the current thread
    for (unsigned int indx = startIndx; indx < endIndx ; indx++)
    {
      std::string urlFile = pxt->arrayFilename[indx];

      //get file size
      struct stat buf;
      XrdPosixXrootd::Stat(urlFile.c_str(), &buf);
      sizeReadFile = buf.st_size;

      noOpen++;
      int fdWrite = XrdPosixXrootd::Open(urlFile.c_str(), O_RDONLY,
                                         kXR_ur | kXR_uw | kXR_gw | kXR_gr | kXR_or);
      if (fdWrite < 0) {
        fprintf(stderr, "error=error while opening read file.\n");
        delete[] buffer;
        free(arg);
        exit(1);      
      }
 
      //read from file
      off_t offset = 0;
      unsigned long long noBlocks = sizeReadFile / pxt->sizeBlock;
      size_t lastRead = sizeReadFile % pxt->sizeBlock;
   
      for (unsigned long long i = 0 ; i < noBlocks ; i++)
      {
        XrdPosixXrootd::Pread(fdWrite, buffer, pxt->sizeBlock, offset);
        offset += pxt->sizeBlock;
      }

      if (lastRead) {
        XrdPosixXrootd::Pread(fdWrite, buffer, lastRead, offset);
        offset += lastRead;
      }

      totalOffset += offset;
      
      if (pxt->verbose) {
        if (change)   //true
        {
          gettimeofday(&time2, NULL);
          duration = (time2.tv_sec - time1.tv_sec) + ((time2.tv_usec - time1.tv_usec) / 1e6);
          if (duration > deltaTime) {
            sample++;
            change = !change;
            duration = (time2.tv_sec - start.tv_sec) + ((time2.tv_usec - start.tv_usec) / 1e6);
            openPerSec = (double)noOpen / duration;
            rate = ((double)totalOffset / (1024 * 1024)) / duration;
            fprintf(stdout, "info=\"read partial\" %s=%i step=%i mean=%g MB/s open/s=%g \n",
                    pxt->childType.c_str(), pti->idChild, sample, rate, openPerSec);
          }
        }
        else {     //false
          gettimeofday(&time1, NULL);
          duration = (time1.tv_sec - time2.tv_sec) + ((time1.tv_usec - time2.tv_usec) / 1e6);
          if (duration > deltaTime) {
            sample++;
            change = !change;
            duration = (time1.tv_sec - start.tv_sec) + ((time1.tv_usec - start.tv_usec) / 1e6);
            openPerSec = (double)noOpen / duration;
            rate = ((double)totalOffset / (1024 * 1024)) / duration;
            fprintf(stdout, "info=\"read partial\" %s=%i step=%i mean=%g MB/s open/s=%g \n",
                    pxt->childType.c_str(), pti->idChild, sample, rate, openPerSec);
          }
        }
      }
   
      //close file
      XrdPosixXrootd::Close(fdWrite);
    }
    
    delete[] buffer;

    //get overall values
    gettimeofday(&end, NULL);
    duration = (end.tv_sec - start.tv_sec) + ((end.tv_usec - start.tv_usec) / 1e6);
    rate = ((double)totalOffset / (1024 * 1024)) / duration;
    openPerSec = (double)noOpen / duration;
    if (pxt->verbose) {
      fprintf(stdout, "info=\"read final\" %s=%i  mean=%g MB/s open/s=%g \n",
              pxt->childType.c_str(), pti->idChild, rate, openPerSec);
    }

    pti->avgRdVal = rate;
    pxt->avgRdRate[pti->idChild] = rate;

    if (pti->avgOpenVal != 0 ) {
      pti->avgOpenVal = (pti->avgOpenVal + openPerSec) / 2;  
    }
    else {
      pti->avgOpenVal = openPerSec;
    }
    pxt->avgOpen[pti->idChild] = pti->avgOpenVal;
      
    return arg;
  };


  /*-----------------------------------------------------------------------------*/
  static void* WrProc(void* arg)
  {
    bool change = true;
    double rate = 0;
    double openPerSec = 0;
    unsigned int noOpen =0;
    off_t totalOffset = 0;
    ChildInfo* pti = static_cast<ChildInfo*>(arg); 
    XrdStress* pxt = pti->pXrdStress;

    //get some random characters
    size_t sizeBuff = pxt->sizeBlock + 1;
    char* buffer = new char[sizeBuff];
    std::ifstream urandom("/dev/urandom", std::ios::in | std::ios::binary);
    urandom.read(buffer, pxt->sizeBlock);
    urandom.close();

    //initialize time structures
    float duration = 0;
    int deltaTime = DELTATIME;           
    struct timeval start, end;
    struct timeval time1, time2;

    gettimeofday(&start, NULL);
    gettimeofday(&time1, NULL);

    int sample = 0;
    unsigned int startIndx = pti->idChild * pxt->noFiles;
    unsigned int endIndx = (pti->idChild + 1) * pxt->noFiles;

    //loop over all files corresponding to the current thread
    for (unsigned int indx = startIndx; indx < endIndx ; indx++)
    {
      std::string urlFile = pxt->arrayFilename[indx];

      noOpen++;
      int fdWrite = XrdPosixXrootd::Open(urlFile.c_str(),
                                         kXR_async | kXR_mkpath | kXR_open_updt | kXR_new,
                                         kXR_ur | kXR_uw | kXR_gw | kXR_gr | kXR_or );

      if (fdWrite < 0) {
        fprintf(stderr,  "error=error while opening write file.\n ");
        delete[] buffer;
        free(arg);
        exit(1);      
      }

      //write to file
      size_t offset = 0;
      unsigned long long noBlocks = pxt->sizeFile / pxt->sizeBlock;
      size_t lastWrite = pxt->sizeFile % pxt->sizeBlock;

      for (unsigned long long i = 0 ; i < noBlocks ; i++)
      {
        XrdPosixXrootd::Pwrite(fdWrite, buffer, pxt->sizeBlock, offset);
        offset += pxt->sizeBlock;     
      }

      if (lastWrite) {
        XrdPosixXrootd::Pwrite(fdWrite, buffer, lastWrite, offset);
        offset += lastWrite;
      }

      totalOffset += offset;

      if (pxt->verbose) {
        if (change)   //true
        {
          gettimeofday(&time2, NULL);
          duration = (time2.tv_sec - time1.tv_sec) + ((time2.tv_usec - time1.tv_usec) / 1e6);
          if (duration > deltaTime) {
            sample++;
            change = !change;
            duration = (time2.tv_sec - start.tv_sec) + ((time2.tv_usec - start.tv_usec) / 1e6);
            openPerSec = (double)noOpen / duration;
            rate = ((double)totalOffset / (1024 * 1024)) / duration;
            fprintf(stdout, "info=\"write partial\" %s=%i step=%i mean=%g MB/s open/s=%g \n",
                    pxt->childType.c_str(), pti->idChild, sample, rate, openPerSec);
          }
        }
        else {     //false
          gettimeofday(&time1, NULL);
          duration = (time1.tv_sec - time2.tv_sec) + ((time1.tv_usec - time2.tv_usec) / 1e6);
          if (duration > deltaTime) {
            sample++;
            change = !change;
            duration = (time1.tv_sec - start.tv_sec) + ((time1.tv_usec - start.tv_usec) / 1e6);
            openPerSec = (double)noOpen / duration;
            rate = ((double)totalOffset / (1024 * 1024)) / duration;
            fprintf(stdout, "info=\"write partial\" %s=%i step=%i mean=%g MB/s open/s=%g \n",
                    pxt->childType.c_str(), pti->idChild, sample, rate, openPerSec);
          }
        }
      }
    
      //close file
      XrdPosixXrootd::Close(fdWrite);
    }

    delete[] buffer;
 
    //get overall values    
    gettimeofday(&end, NULL);
    duration = (end.tv_sec - start.tv_sec) + ((end.tv_usec - start.tv_usec) / 1e6);
    openPerSec = (double)noOpen / duration;
    rate = ((double)totalOffset / (1024 * 1024)) / duration;
    if (pxt->verbose) {
      fprintf(stdout, "info=\"write final\" %s=%i  mean=%g MB/s open/s=%g \n",
              pxt->childType.c_str(), pti->idChild, rate, openPerSec);
    }
  
    pxt->avgWrRate[pti->idChild] = rate;
    pti->avgWrVal = rate;
    pti->avgOpenVal = openPerSec;
    pxt->avgOpen[pti->idChild] = pti->avgOpenVal;

    return arg;
  };


  /*-----------------------------------------------------------------------------*/
  static void* RdWrProc(void* arg)
  {
    WrProc(arg);
    RdProc(arg);
    return arg;
  }; 
};


//------------------------------------------------------------------------------
//------------------------------------------------------------------------------
int main (int argc, char* argv[])
{
  int c;
  bool verbose = false;
  bool processmode = false;
  unsigned int noChilds = 0;
  unsigned int noFiles = 0;
  std::string sTmp("");
  std::string path("");
  std::string opType("");
  std::string testName("");
  size_t sizeBlock = 4 * 1024;            //4KB
  size_t sizeFile = 100 * 1024 * 1024;    //100MB  -  default values
  
  std::string usage = "Usage:  xrdstress -d <dir path>\
                               \n\t\t -o <rd/wr/rdwr>\
                               \n\t\t -c <noChilds>\
                               \n\t\t -f <noFiles>\
                               \n\t\t [-s <sizeFile: 1KB,1MB>]\
                               \n\t\t [-b <sizeBlock: 1KB, 1MB>]\
                               \n\t\t [-n <testName>]\
                               \n\t\t [-v verbose]\
                               \n\t\t [-p use processes]";
  
  const std::string arrayOp[] = {"rd" , "wr", "rdwr"};
  std::set<std::string> setOp(arrayOp, arrayOp + 3);
  
  while ((c = getopt(argc, argv, "d:o:c:f:s:b:n:vp")) != -1)
  {
    switch(c)
    {
      case 'c':   //no of children
        {
          noChilds = static_cast<unsigned int>(atoi(optarg));
          break;
        }
      case 'd':  //directory path 
        {
          path = optarg;
          //check path to see if it extsts
          struct stat buff;
          int ret = XrdPosixXrootd::Stat(path.c_str(), &buff);
          if (ret != 0) {
            std::cout << "The path requested does not exists." << std::endl
                      << usage << std::endl;
            exit(1);            
          }            
          break;
        }
      case 'o':   //operation type
        {
          opType = optarg;
          if (setOp.find(opType) == setOp.end())
          {
            std::cout << "Type of operation unknown. " << std::endl
                      << usage << std::endl;
            exit(1);
          }
          break;         
        }
      case 'n':   //test name 
        {
          testName = optarg;
          break;
        }
      case 's':   //size file
        {
          sTmp = optarg;
          std::string sNo = sTmp.substr(0, sTmp.size() - 2);
          std::string sBytes = sTmp.substr(sTmp.size() - 2);

          if (sBytes == "KB") {
            sizeFile = atoi(sNo.c_str()) * 1024;
          }
          else if (sBytes == "MB") {
            sizeFile = atoi(sNo.c_str()) * 1024 * 1024;
          }
          break;
        }
      case 'b':   //size block
        {
          sTmp = optarg;
          std::string sNo = sTmp.substr(0, sTmp.size() - 2);
          std::string sBytes = sTmp.substr(sTmp.size() - 2);

          if (sBytes == "KB") {
            sizeBlock = atoi(sNo.c_str()) * 1024;
          }
          else if (sBytes == "MB") {
            sizeBlock = atoi(sNo.c_str()) * 1024 * 1024;
          }
          break;
        }
      case 'f':
        {
          noFiles = atoi(optarg);
          break;
        }
      case 'v':  //verbose mode
        {
          verbose = true;
          break;
        }
      case 'p':  //run with processes or threads 
        {
          processmode = true;  
          break;
        }

      case ':':
        {
          std::cout << usage << std::endl;
          exit(1);
          break;
        }        
    }
  }

  //if one of the critical params. is missing exit
  if ((path == "") || (opType == "") || (noChilds == 0) || (noFiles == 0))
  {
     std::cout << usage << std::endl;
     exit(1);
  }
  
  //generate uuid for test name if none provided
  if (testName == "") {
    uuid_t genUuid;
    char charUuid[40];
    uuid_generate_time(genUuid);
    uuid_unparse(genUuid, charUuid);
    testName = charUuid;
  }

  //construct full path
  if (path.rfind("/") != path.size()) {
    path += "/";
  }
  path += testName;
  path += "/";
  mode_t mode = kXR_ur | kXR_uw | kXR_gw | kXR_gr | kXR_or;
  std::cout << "path=" << path << std::endl; 
  XrdPosixXrootd::Mkdir(path.c_str(), mode);

  XrdStress* test = new XrdStress(noChilds, noFiles, sizeBlock, sizeFile, path, opType, verbose, processmode);
  test->RunTest();

  delete test;
}
