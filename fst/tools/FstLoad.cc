// ----------------------------------------------------------------------
// File: FstLoad.cc
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

#include "fst/Load.hh"

int
main ()
{
 eos::fst::Load load(1);
 load.Monitor();

 while (1)
 {
   printf("%lu rx %.02f MiB/s \t tx %.02f MiB/s \t", (unsigned long) time(NULL), load.GetNetRate("eth0", "rxbytes") / 1024.0 / 1024.0, load.GetNetRate("eth0", "txbytes") / 1024.0 / 1024.0);
   printf("rd %.02f MB/s \twd %.02f MB/s\n", load.GetDiskRate("/data22", "readSectors")*512.0 / 1000000.0, load.GetDiskRate("/data22", "writeSectors")*512.0 / 1000000.0);
   sleep(1);
 }

}
