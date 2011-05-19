#include <cstdio>
#include <cstdlib>
/* ------------------------------------------------------------------------- */
#include <sys/time.h>
/* ------------------------------------------------------------------------- */
#include "XrdOuc/XrdOucString.hh"
#include "XrdClient/XrdClient.hh"
#include "XrdClient/XrdClientAdmin.hh"
/* ------------------------------------------------------------------------- */
/* ------------------------------------------------------------------------- */

int main (int argc, char** argv){

  int bandwidth;
  std::string src, dest;

  if (argc < 3){
    fprintf(stderr, "Usage: eostxcp <src> <dest> <bandwidthwidth>\n");
    exit(0);    
  }

  src  = argv[1];
  dest = argv[2];
  bandwidth = atoi(argv[3]);
  
  XrdClient* readClient = new XrdClient(src.c_str());
  XrdClient* writeClient = new XrdClient(dest.c_str());

  if ( (!readClient) || (!writeClient) ) {
    exit(0);
  }

  if (!readClient->Open(0, 0, false)){
    fprintf(stderr, "Could not open file for reading. \n");
    delete readClient;
    delete writeClient;
    exit(0);
  }

  vecString vs;
  vecBool vb;
  bool existFile = false;
  XrdOucString sTmp = dest.c_str();
  
  vs.Push_back(sTmp);
  vb.Push_back(existFile);
  
  if (!(writeClient->Open(kXR_ur | kXR_uw | kXR_gw | kXR_gr | kXR_or , kXR_mkpath | kXR_open_updt, false))){
    if (!(writeClient->Open(kXR_ur | kXR_uw | kXR_gw | kXR_gr | kXR_or , kXR_mkpath | kXR_new, false))){
      fprintf(stderr, "Error while opening file for writing.\n");
      delete readClient;
      delete writeClient;
      exit(0);
    }
  }
 
  // simple copy loop
  int buffersize = 1024*1024;
  char* cpbuffer = (char*)malloc(buffersize);
  if (!cpbuffer) {
    fprintf(stderr, "Failed to allocate copy buffer.\n");
    delete readClient;
    delete writeClient;
    exit(0);
  }

  
  //do the transfer limiting the bandwidthwidth
  struct timeval abs_start_time, abs_stop_time;
  struct timezone tz1, tz2;
  long long offset = 0;
  bool failed =  false;

  gettimeofday(&abs_start_time,&tz1);

  do {
    int nread = readClient->Read(cpbuffer,offset,buffersize);
    if (nread>0) {
      if (!writeClient->Write(cpbuffer, offset, nread)) {
	failed = true;
	break;
      }
    }

    if (nread != buffersize) {
      offset += nread;
      break;
    }

    offset += nread;

    gettimeofday (&abs_stop_time, &tz2);
    float abs_time=((float)((abs_stop_time.tv_sec - abs_start_time.tv_sec) *1000 +
			    (abs_stop_time.tv_usec - abs_start_time.tv_usec) / 1000));
    // regulate the io - sleep as desired
    float exp_time = offset/bandwidth/1000.0;
  
    if (abs_time < exp_time) {
      usleep((int)(1000*(exp_time - abs_time)));
    }

  } while (1);


  // free the copy buffer
  free(cpbuffer);

  if (failed){
    fprintf(stderr, "Error while trying to write to file.\n");
    delete readClient;
    delete writeClient;
    exit(0);
  }
  
  return 1;
}
