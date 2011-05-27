/*----------------------------------------------------------------------------*/
#include "common/Attr.hh"
#include "fst/ScanDir.hh"
/*----------------------------------------------------------------------------*/
#include <cstdlib>
#include <cstring>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>
#include <fts.h>
#include <syslog.h>
#include <unistd.h>
#include <fcntl.h>
/*----------------------------------------------------------------------------*/


EOSFSTNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
ScanDir::~ScanDir()
{ 
  if ((bgThread && thread)) {
    XrdSysThread::Cancel(thread);
    XrdSysThread::Join(thread,NULL);
    closelog(); 
  }
  if (buffer) {
    free(buffer);
  }
}

/*----------------------------------------------------------------------------*/
void scandir_cleanup_paths(void *arg) 
{
  char **paths = (char**)arg;
  if (paths)
    free (paths);
}

/*----------------------------------------------------------------------------*/
void scandir_cleanup_fts(void *arg) 
{
  FTS *tree = (FTS*)arg;
  if (tree)
    fts_close(tree);
}

/*----------------------------------------------------------------------------*/
void ScanDir::ScanFiles()
{
  char **paths = (char**) calloc(2, sizeof(char*));
  if (!paths) {
    return ;
  }

  pthread_cleanup_push(scandir_cleanup_paths, paths);

  paths[0] = (char*) dirPath.c_str();
  paths[1] = 0;

  FTS *tree = fts_open(paths, FTS_NOCHDIR, 0);

  if (!tree){
    fprintf(stderr, "error: fts_open failed! \n");
    free(paths);
    return;
  }

  pthread_cleanup_push(scandir_cleanup_fts, tree);

  
  FTSENT *node;
  while ((node = fts_read(tree))) {
    if (node->fts_level > 0 && node->fts_name[0] == '.') {
      fts_set(tree, node, FTS_SKIP);
      } else {
        if (node->fts_info && FTS_F) {
          XrdOucString filePath = node->fts_accpath;
          if (!filePath.matches("*.xsmap")){
            if (!bgThread)
              fprintf(stderr,"[ScanDir] processing file %s\n",filePath.c_str());
            CheckFile(filePath.c_str());
          }
        }
    }    
    XrdSysThread::CancelPoint();
  }
  if (fts_close(tree)){
    fprintf(stderr, "error: fts_close failed \n");
  }  
  free(paths);
  pthread_cleanup_pop(0);
  pthread_cleanup_pop(0);
}


/*----------------------------------------------------------------------------*/
void ScanDir::CheckFile(const char* filepath)
{
  float scantime;
  unsigned long layoutid=0;
  unsigned long long scansize;
  std::string filePath, checksumType, checksumVal, checksumStamp, logicalFileName;
  
  filePath = filepath;
  eos::common::Attr *attr = eos::common::Attr::OpenAttr(filePath.c_str());
  
  noTotalFiles++;

  // get last modification time
  struct stat buf1;
  struct stat buf2;
  if (stat(filePath.c_str(), &buf1)) {
    fprintf(stderr,"error: cannot stat %s\n", filePath.c_str());
    return ;
  }
  
  if (attr){
    checksumType    = attr->Get("user.eos.checksumtype");
    checksumVal     = attr->Get("user.eos.checksum");
    checksumStamp   = attr->Get("user.eos.timestamp");
    logicalFileName = attr->Get("user.eos.lfn");
    
    if (RescanFile(checksumStamp)){
      if (checksumType.compare("")){
        XrdOucString envstring = "eos.layout.checksum="; envstring += checksumType.c_str();
        XrdOucEnv env(envstring.c_str());
        unsigned long checksumtype = eos::common::LayoutId::GetChecksumFromEnv(env);
	layoutid = eos::common::LayoutId::GetId(eos::common::LayoutId::kPlain, checksumtype);
	if (!ScanFileLoadAware(filePath.c_str(), scansize, scantime, checksumVal, layoutid,logicalFileName.c_str())){
          if ( (! stat(filePath.c_str(), &buf2)) && (buf1.st_mtime == buf2.st_mtime)) {
            if (bgThread)
              syslog(LOG_ERR,"corrupted  file checksum: localpath=%slfn=\"%s\" \n", filePath.c_str(), logicalFileName.c_str());
            else
              fprintf(stderr,"[ScanDir] corrupted  file checksum: localpath=%slfn=\"%s\" \n", filePath.c_str(), logicalFileName.c_str());
          } else {
            if (bgThread) 
              fprintf(stderr,"[ScanDir] file %s has been modified during the scan ... ignoring checksum error\n", filePath.c_str());
          }
        }
	//collect statistics
	durationScan += scantime;
	totalScanSize += scansize;
        
	if (!attr->Set("user.eos.timestamp", GetTimestamp())) {
	  fprintf(stderr, "error: [CheckFile] Can not set extended attrbutes to file. \n");
        }
      } else {
        noNoCheckumFiles++;
      }
    }
    delete attr;
  }
}


/*----------------------------------------------------------------------------*/
eos::fst::CheckSum* ScanDir::GetBlockXS(const char* filepath)
{
  long long int maxfilesize=0;
  unsigned long layoutid=0;
  std::string checksumType,checksumSize, logicalFileName;
  XrdOucString fileXSPath = filepath;
 
  eos::common::Attr *attr = eos::common::Attr::OpenAttr(fileXSPath.c_str());

  if (attr){
    checksumType    = attr->Get("user.eos.blockchecksum");
    checksumSize    = attr->Get("user.eos.blocksize");
    logicalFileName = attr->Get("user.eos.lfn");
    delete attr;

    if (checksumType.compare("")){
      XrdOucString envstring = "eos.layout.blockchecksum="; envstring += checksumType.c_str();
      XrdOucEnv env(envstring.c_str());
      unsigned long checksumtype = eos::common::LayoutId::GetBlockChecksumFromEnv(env);

      int blockSize = atoi(checksumSize.c_str());
      int blockSizeSymbol = eos::common::LayoutId::BlockSizeEnum(blockSize);

      layoutid = eos::common::LayoutId::GetId(eos::common::LayoutId::kPlain, eos::common::LayoutId::kNone, 0,  blockSizeSymbol, checksumtype);

      eos::fst::CheckSum *checksum = eos::fst::ChecksumPlugins::GetChecksumObject(layoutid, true);

      if (checksum) {
        // get size of XS file
        struct stat info;
        
        if (stat(fileXSPath.c_str(), &info)) {
          fprintf(stderr,"error: cannot open file %s\n", fileXSPath.c_str());
          maxfilesize = 0;
        } else {
          maxfilesize = info.st_size;
        } 
        
        if (checksum->OpenMap(fileXSPath.c_str(), maxfilesize, blockSize, false)) {
          return checksum;
        } else {
          delete checksum;
          return NULL;
        }
      } else {
        fprintf(stderr,"error: cannot get checksum object for layout id %lx\n", layoutid);
      }
    }
    else
      return NULL; 
  }

  return NULL;
}


/*----------------------------------------------------------------------------*/
std::string ScanDir::GetTimestamp()
{
  
  char buffer[65536];
  size_t size = sizeof(buffer) - 1;
  long int timestamp;
  struct timeval tv;

  gettimeofday(&tv, NULL);
  timestamp  = tv.tv_sec * 1000000 + tv.tv_usec;
  
  snprintf(buffer, size, "%li", timestamp);
  return std::string(buffer);
}


/*----------------------------------------------------------------------------*/
bool ScanDir::RescanFile(std::string fileTimestamp)
{
  if (!fileTimestamp.compare("")) 
    return true;   //first time we check

  long int oldTime = atol(fileTimestamp.c_str()),
    newTime = atol(GetTimestamp().c_str());
  
  if (((newTime - oldTime) / 1000000) < testInterval) {
    return false;
  } else {
    return true;
  }
}


/*----------------------------------------------------------------------------*/
void* ScanDir::StaticThreadProc(void* arg)
{
  return reinterpret_cast<ScanDir*>(arg)->ThreadProc();
}


/*----------------------------------------------------------------------------*/
void* ScanDir::ThreadProc(void)
{
  XrdSysThread::SetCancelOn();
  do {

    struct timezone tz;
    struct timeval tv_start, tv_end;
    
    noScanFiles = 0;
    totalScanSize = 0;
    noCorruptFiles = 0;
    noNoCheckumFiles = 0;
    noTotalFiles = 0;
    
    if (bgThread) {
      // run every 4 hours
      for (size_t s=0; s < (4*3600); s++) {
        XrdSysThread::CancelPoint();
        sleep(1);
      }
    }

    gettimeofday(&tv_start, &tz);
    ScanFiles();
    gettimeofday(&tv_end, &tz);
    
    durationScan = ((tv_end.tv_sec - tv_start.tv_sec) * 1000.0) + ((tv_end.tv_usec - tv_start.tv_usec) / 1000.0);
    if (bgThread) {
      syslog(LOG_ERR,"Directory: %s, files=%li scanduration=%.02f [s] scansize=%lli [Bytes] [ %lli MB] scannedfiles=%li  corruptedfiles=%li skippedfiles=%li\n", dirPath.c_str(), noTotalFiles, (durationScan / 1000.0), totalScanSize, ((totalScanSize / 1000) / 1000), noScanFiles, noCorruptFiles,noNoCheckumFiles);
    } else {
      fprintf(stderr,"[ScanDir] Directory: %s, files=%li scanduration=%.02f [s] scansize=%lli [Bytes] [ %lli MB] scannedfiles=%li  corruptedfiles=%li skippedfiles=%li\n", dirPath.c_str(), noTotalFiles, (durationScan / 1000.0), totalScanSize, ((totalScanSize / 1000) / 1000), noScanFiles, noCorruptFiles,noNoCheckumFiles);
    }

    if (!bgThread)
      break;

    XrdSysThread::CancelPoint();
  }  while(1);
  return NULL;
}


/*----------------------------------------------------------------------------*/
bool ScanDir::ScanFileLoadAware(const char* path, unsigned long long &scansize, float &scantime, std::string checksumVal, unsigned long layoutid, const char* lfn)
{
  double load;
  bool retVal, corruptBlockXS = false;
  int len, currentRate = rateBandwidth;
  std::string filePath, fileXSPath, checksumComp;
  struct timezone tz;
  struct timeval  opentime;
  struct timeval  currenttime;
  eos::fst::CheckSum *normalXS, *blockXS;

  scansize = 0;
  scantime = 0;

  filePath = path;
  fileXSPath = filePath + ".xsmap";

  normalXS = eos::fst::ChecksumPlugins::GetChecksumObject(layoutid);
  if (!normalXS) {
    fprintf(stderr,"error: cannot get checksum object for %lx\n", layoutid);
    return false;
  }

  gettimeofday(&opentime,&tz);

  int fd = open(path, O_RDONLY | O_DIRECT);
  if (fd<0) {
    delete normalXS;
    return false;
  }

  blockXS = GetBlockXS(fileXSPath.c_str());
  normalXS->Reset();

  int nread=0;
  off_t offset = 0;

  do {
    errno = 0;
    nread = read(fd,buffer,bufferSize);
    if (nread<0) {
      close(fd);
      free(buffer);
      if (blockXS) {
        blockXS->CloseMap();
        delete blockXS;
      }
      delete normalXS;
      return false;
    }
    
    if (!corruptBlockXS && blockXS)
      if (!blockXS->CheckBlockSum(offset, buffer, bufferSize))
	corruptBlockXS = true;
    
    normalXS->Add(buffer, nread, offset);
    offset += nread;
 
    if (currentRate) {
      // regulate the verification rate
      gettimeofday(&currenttime,&tz);
      scantime = ( ((currenttime.tv_sec - opentime.tv_sec)*1000.0) + ((currenttime.tv_usec - opentime.tv_usec)/1000.0 ));
      float expecttime = (1.0 * offset / currentRate) / 1000.0;
      if (expecttime > scantime) {
	usleep(1000.0*(expecttime - scantime));
      }
      //adjust the rate according to the load information
      load = fstLoad->GetDiskRate("sda", "millisIO") / 1000.0;
      if (load > 0.7){
	//adjust currentRate
	if (currentRate > 5)
	  currentRate = 0.9 * currentRate; 	
      } else {
	currentRate = rateBandwidth;     
      }
    }
  } while (nread == bufferSize);

  gettimeofday(&currenttime,&tz);
  scantime = ( ((currenttime.tv_sec - opentime.tv_sec)*1000.0) + ((currenttime.tv_usec - opentime.tv_usec)/1000.0 ));
  scansize = (unsigned long long) offset;

  //check file checksum
  checksumComp = normalXS->GetBinChecksum(len);
  if (checksumComp.compare(0, len, checksumVal)){
    noCorruptFiles++;
    retVal = false;
  }
  else {
    retVal = true;
  }
 
  //check block checksum
  if (corruptBlockXS){
    if (bgThread)
      syslog(LOG_ERR,"corrupted block checksum: localpath=%s blockxspath=%s lfn=%s\n", path,fileXSPath.c_str(),lfn);
    else
      fprintf(stderr,"[ScanDir] corrupted block checksum: localpath=%s blockxspath=%s lfn=%s\n", path,fileXSPath.c_str(),lfn);

    retVal &= false;
  }
  else {
    retVal &= true;
  }

  //collect statistics
  noScanFiles++; 

  normalXS->Finalize();  
  if (blockXS) {
    blockXS->CloseMap();
    delete blockXS;
  }

  delete normalXS;
  close(fd);

  XrdSysThread::CancelPoint();
  return retVal;
}

EOSFSTNAMESPACE_END
