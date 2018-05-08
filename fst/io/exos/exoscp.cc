#include "exosfile.hh"

int main()
{
  {
    exosfile exosf("/eos/myfile","rados.md=andreas&rados.data=andreas_ec&rados.user=andreas");
    exosf.open(0);
    if (0)
      for ( auto i = 0 ; i< 1000; i++) {
	fprintf(stderr,"next ino=%s\n", exosf.nextInode().c_str());
	fprintf(stderr,"open ino=%d\n", exosf.open(0));
      }
  }
  if (0)
  for ( auto i = 0 ; i< 1000; i++) {
    char fname[1024];
    snprintf(fname,sizeof(fname),"/eos/file.%08x",i);
    exosfile exosf(fname,"rados.md=andreas&rados.data=andreas_ec&rados.user=andreas");
    fprintf(stderr,"create %d := ",exosf.open(O_CREAT));
    fprintf(stderr,"%s\n",exosf.dump().c_str());
  }

  if (0)
  for ( auto i = 0 ; i< 1000; i++) {
    char fname[1024];
    snprintf(fname,sizeof(fname),"/eos/file.%08x",i);
    exosfile exosf(fname,"rados.md=andreas&rados.data=andreas_ec&rados.user=andreas");
    fprintf(stderr,"open %d := ",exosf.open(0));
    fprintf(stderr,"%s\n",exosf.dump().c_str());
  }
  
  if (0)
  for ( auto i = 0 ; i< 1000; i++) {
    char fname[1024];
    snprintf(fname,sizeof(fname),"/eos/file.%08x",i);
    exosfile exosf(fname,"rados.md=andreas&rados.data=andreas_ec&rados.user=andreas");
    fprintf(stderr,"open %d := ",exosf.open(O_RDWR));
    fprintf(stderr,"%s\n",exosf.dump().c_str());
  }

  if (0)
  {
    exosfile exosf("/eos/file.000000d8","rados.md=andreas&rados.data=andreas_ec&rados.user=andreas");
    char buffer[65536];
    exosf.open(0);
    fprintf(stderr,"test: 0, 1000\n");
    exosf.read(buffer, 0, 1000);
    fprintf(stderr,"test: 1000000, 300000\n");
    exosf.read(buffer, 1000000, 300000);
    fprintf(stderr,"test: 33554432-1000, 33554432\n");
    exosf.read(buffer, 33554432-1000, 33554432);
    fprintf(stderr,"test: 33554432-1000, 33554432+1000\n");
    exosf.read(buffer, 33554432-1000, 33554432+1000);
    fprintf(stderr,"test: 33554432-1000, 33554432+33554432\n");
    exosf.read(buffer, 33554432-1000, 33554432+33554432);
    fprintf(stderr,"test: 33554432-1000, 33554432+33554432+1000\n");
    exosf.read(buffer, 33554432-1000, 33554432+33554432+1000);
    fprintf(stderr,"test: 33554432-1000, 33554432+33554432+2000\n");
    exosf.read(buffer, 33554432-1000, 33554432+33554432+2000);
  }

  if (0)
  {
    exosfile exosf("/eos/file.write","rados.md=andreas&rados.data=andreas&rados.user=andreas");
    char* buffer = (char*) malloc(128*1024*1024);

    exosf.open(O_CREAT);
    fprintf(stderr,"test: 0, 1000\n");
    exosf.write(buffer, 0, 1000);
    fprintf(stderr,"test: 1000000, 300000\n");
    exosf.write(buffer, 1000000, 300000);
    fprintf(stderr,"test: 33554432-1000, 33554432\n");
    exosf.write(buffer, 33554432-1000, 33554432);
    fprintf(stderr,"test: 33554432-1000, 33554432+1000\n");
    exosf.write(buffer, 33554432-1000, 33554432+1000);
    fprintf(stderr,"test: 33554432-1000, 33554432+33554432\n");
    exosf.write(buffer, 33554432-1000, 33554432+33554432);
    fprintf(stderr,"test: 33554432-1000, 33554432+33554432+1000\n");
    exosf.write(buffer, 33554432-1000, 33554432+33554432+1000);
    fprintf(stderr,"test: 33554432-1000, 33554432+33554432+2000\n");
    exosf.write(buffer, 33554432-1000, 33554432+33554432+2000);
    free(buffer);
  }

  if (0)
  {
    exosfile exosf("/eos/file.aiowrite","rados.md=andreas&rados.data=andreas&rados.user=andreas");
    char* buffer = (char*) malloc(128*1024*1024);

    exosf.open(O_CREAT);
    fprintf(stderr,"test: 0, 1000\n");
    exosf.aio_write(buffer, 0, 1000);
    fprintf(stderr,"test: 1000000, 300000\n");
    exosf.aio_write(buffer, 1000000, 300000);
    fprintf(stderr,"test: 33554432-1000, 33554432\n");
    exosf.aio_write(buffer, 33554432-1000, 33554432);
    fprintf(stderr,"test: 33554432-1000, 33554432+1000\n");
    exosf.aio_write(buffer, 33554432-1000, 33554432+1000);
    fprintf(stderr,"test: 33554432-1000, 33554432+33554432\n");
    exosf.aio_write(buffer, 33554432-1000, 33554432+33554432);
    fprintf(stderr,"test: 33554432-1000, 33554432+33554432+1000\n");
    exosf.aio_write(buffer, 33554432-1000, 33554432+33554432+1000);
    fprintf(stderr,"test: 33554432-1000, 33554432+33554432+2000\n");
    exosf.aio_write(buffer, 33554432-1000, 33554432+33554432+2000);

    fprintf(stderr,"flush: %d \n", exosf.aio_flush());
    free(buffer);
  }

  if (0)
  {
    exosfile exosf("/eos/file.aioseqwrite","rados.md=andreas&rados.data=andreas_ec&rados.user=andreas");
    char* buffer = (char*) malloc(128*1024*1024);

    exosf.open(O_CREAT);
    for (size_t i=0; i< 1024; ++i)
    {
      exosf.aio_write(buffer,i* 1024*1024, 1024*1024);
    }
    fprintf(stderr,"flush: %d \n", exosf.aio_flush());
    fprintf(stderr,"flush done\n");
    free(buffer);

    fprintf(stderr,"unlink: %d\n",exosf.unlink());
  }

  if (0)
  {
    exosfile exosf("/eos/file.aioseqwrite","rados.md=andreas&rados.data=andreas_ec&rados.user=andreas");
    char* buffer = (char*) malloc(128*1024*1024);

    exosf.open(0);
    for (size_t i=0; i< 1024; ++i)
    {
      exosf.read(buffer,i* 1024*1024, 1024*1024);
    }
    fprintf(stderr,"read done - efficienty %.02f\n", exosf.get_readahead_efficiency());
    free(buffer);
  }


  if (0)
  {
    exosfile exosf("/eos/file.aioseqwrite","rados.md=andreas&rados.data=andreas&rados.user=andreas");
    char* buffer = (char*) malloc(128*1024*1024);

    exosf.open(0);
    for (size_t i=1023; i>0; --i)
    {
      exosf.read(buffer,i* 1024*1024, 1024*1024);
    }
    for (size_t i=0; i< 1024; ++i)
    {
      exosf.read(buffer,i* 1024*1024, 1024*1024);
    }
    fprintf(stderr,"read done - efficienty %.02f\n", exosf.get_readahead_efficiency());
    free(buffer);
  }

  if (0)
  {
    exosfile exosf("/eos/file.aiolock","rados.md=andreas&rados.data=andreas_ec&rados.user=andreas");
    fprintf(stderr,"create: %d\n", exosf.open(O_CREAT));
    fprintf(stderr,"islocked: %d\n", exosf.locked());
    fprintf(stderr,"lock: %d\n",exosf.lock());
    fprintf(stderr,"islocked: %d\n", exosf.locked());
    fprintf(stderr,"unlock: %d\n", exosf.unlock());
  }

  if (1)
  {
    exosfile exosf("/eos/file.aioseqwritetruncate","rados.md=andreas&rados.data=andreas_ec&rados.user=andreas");
    char* buffer = (char*) malloc(128*1024*1024);

    exosf.open(O_CREAT);
    for (size_t i=0; i< 1024; ++i)
    {
      exosf.aio_write(buffer,i* 1024*1024, 1024*1024);
    }
    free(buffer);

    fprintf(stderr,"truncate: %d\n",exosf.truncate( 512 * 1024*1024 ));
    fprintf(stderr,"truncate: %d\n",exosf.truncate( (512 * 1024*1024)-1 ));
    fprintf(stderr,"truncate: %d\n",exosf.truncate( 724 * 1024*1024 ));
    fprintf(stderr,"truncate: %d\n",exosf.truncate( 0 ));
    fprintf(stderr,"unlink: %d\n",exosf.unlink());
  }  
}
