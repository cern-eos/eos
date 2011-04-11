#include "fst/Load.hh"
int main() {

  eos::fst::Load load(1);
  load.Monitor();

  while(1) {
    printf("%lu rx %.02f MiB/s \t tx %.02f MiB/s \t",(unsigned long)time(NULL), load.GetNetRate("eth0","rxbytes")/1024.0/1024.0,load.GetNetRate("eth0","txbytes")/1024.0/1024.0);
    printf("rd %.02f MB/s \twd %.02f MB/s\n", load.GetDiskRate("/data22","readSectors")*512.0/1000000.0, load.GetDiskRate("/data22","writeSectors")*512.0/1000000.0);
    sleep(1);
  }

}
