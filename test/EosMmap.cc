// ----------------------------------------------------------------------
// File: EosMmap.cc
// Author: Andreas-Joachim Peters - CERN
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
/*-----------------------------------------------------------------------------*/

#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>


void usage() {
  fprintf(stderr,"usage: eos-mmap <file>\n");
  exit(-1);
}

int main(int argc, char* argv[]) {
  if (argc != 2) {
    usage();
  }

  int fd = open(argv[1],0,0);
  void *mapptr=0;
  struct stat buf;
  if (stat(argv[1],&buf)) {
    usage();
  }
  

  if (fd>0) {
    printf("[eos-mmap] mapping %lld bytes ...\n", (unsigned long long) buf.st_size);
    mapptr = mmap(0, buf.st_size, PROT_READ,MAP_SHARED, fd,0);
    int percentage=0;
    int last_percentage=0;
    if (mapptr) {
      for (off_t i =0; i < (off_t)(buf.st_size/sizeof(unsigned long long)); i+=10) {
	unsigned long long b = *((unsigned long long*)mapptr + i);
	b = 0;
	percentage = 1+ (int) ( i * 100.0 / buf.st_size );
	if ( !(percentage%10)) {
	  if (last_percentage != percentage) {
	    printf("[eos-mmap] %03d %% cached\r",percentage);
	    last_percentage = percentage;
	  }
	}
      }
    }
  }
  printf("\r[eos-mmap] file is fully mmaped\n");
  sleep(10000000);
}
