/*----------------------------------------------------------------------------*/
#include <cstdio>
/*----------------------------------------------------------------------------*/
#include "fst/XrdFstOfs.hh"
#include "fst/RaidDPScan.hh"
#include "XrdClient/XrdClient.hh"
#include "XrdOuc/XrdOucString.hh"
/*----------------------------------------------------------------------------*/


EOSFSTNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
RaidDPScan::RaidDPScan(const char* pathFile, bool bgthread=true)
{

  thread  = 0;
  bgThread = bgthread;

  fileName = (char*) calloc(4096, sizeof(char));
  memcpy(fileName, pathFile, strlen(pathFile));

  if (bgThread){
    XrdSysThread::Run(&thread, RaidDPScan::StaticThreadProc, static_cast<void *>(this), XRDSYSTHREAD_HOLD, "Recover Thread");
  }
}


/*----------------------------------------------------------------------------*/
bool RaidDPScan::RecoverFile(){

  int aread;
  char *buffer;
  long long offset;
  long long sizeFile;
  long int sizeBuffer = 4 *1024 * 1024;
  XrdClient *client = new XrdClient(fileName);  
  
  //open file for writing
  if (!client->Open(kXR_ur | kXR_uw | kXR_gw | kXR_gr | kXR_or, kXR_async | kXR_mkpath | kXR_open_updt, false)) {
    fprintf(stderr, "Failed to open file: %s\n", fileName);
    return false;;
  }           

  //get the file size and read it all
  struct XrdClientStatInfo statinfo;
  if(!(client->Stat(&statinfo))){
    fprintf(stderr, "Error trying to stat the file.\n");
    return false;
  }
  
  offset = -1; //switch to recover mode
  sizeFile = statinfo.size;
  buffer = (char*) calloc(sizeBuffer, sizeof(char));

  if (!(aread = client->Read(buffer, offset, sizeFile)) || (aread != sizeFile)){
    fprintf(stderr, "Error while reading the file.\n");
    free(buffer);
    delete client;
    return false;
  }

  free(buffer);
  delete client;
  return true;
}


/*----------------------------------------------------------------------------*/
void* RaidDPScan::StaticThreadProc(void* arg)
{
  return reinterpret_cast<RaidDPScan*>(arg)->ThreadProc();
}


/*----------------------------------------------------------------------------*/
void* RaidDPScan::ThreadProc(){
  
  RecoverFile(); 
  return NULL;
}


/*----------------------------------------------------------------------------*/
RaidDPScan::~RaidDPScan(){

  free(fileName);

}

EOSFSTNAMESPACE_END
