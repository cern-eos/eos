//------------------------------------------------------------------------------
// File: eosfilesync.cc
// Author: Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

/********************A****************************************************
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

/*----------------------------------------------------------------------------*/
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <sys/types.h>
#include <unistd.h>
/*----------------------------------------------------------------------------*/
#ifndef __APPLE__
#include <sys/inotify.h>
#endif

#include "XrdOuc/XrdOucString.hh"
#include "XrdSys/XrdSysTimer.hh"
#include "XrdCl/XrdClFile.hh"
#include "common/Logging.hh"
/*----------------------------------------------------------------------------*/

#define PROGNAME "eosfilesync"
#define TRANSFERBLOCKSIZE 1024*1024*4
#define PAGESIZE (64*1024ll)

//------------------------------------------------------------------------------
// Usage
//------------------------------------------------------------------------------
void usage()
{
  fprintf( stderr, "usage: %s <src-path> <dst-url> [--debug]\n", PROGNAME );
  exit( -1 );
}

// -------------------------------------------------------------
// INOTIFY structures/buffer
// -------------------------------------------------------------

#ifndef __APPLE__
#define EVENT_SIZE  ( sizeof (struct inotify_event) )
#define EVENT_BUF_LEN     ( 1024 * ( EVENT_SIZE + 16 ) )
#else
// - dummy -
#define EVENT_BUF_LEN 1024
#endif

char buffer[EVENT_BUF_LEN];


//------------------------------------------------------------------------------
// Main function
//------------------------------------------------------------------------------
int main( int argc, char* argv[] )
{
  if ( argc < 3 ) {
    usage();
  }

  eos::common::Logging::Init();
  eos::common::Logging::SetUnit( "eosfilesync" );
  eos::common::Logging::SetLogPriority( LOG_NOTICE );
  XrdOucString debugstring = "";

  if ( argc == 4 ) {
    debugstring = argv[3];
  }

  if ( ( debugstring == "--debug" ) || ( debugstring == "-d" ) ) {
    eos::common::Logging::SetLogPriority( LOG_DEBUG );
  }

  if ( ( debugstring == "--debug" ) || ( debugstring == "-d" ) ) {
    eos::common::Logging::SetLogPriority( LOG_DEBUG );
  }

  eos_static_notice( "starting %s=>%s", argv[1], argv[2] );
  int fd = 0;
  off_t localoffset = 0;
  off_t remoteoffset = 0;
  struct timezone tz;
  struct timeval sync1;
  struct timeval sync2;
  bool isdumpfile = false; // dump files are not 'follower' files in append mode,
                           // they need to be resynched completely
  
  gettimeofday( &sync1, &tz );
  XrdOucString sourcefile = argv[1];
  XrdOucString dsturl = argv[2];
  std::string sdsturl = dsturl.c_str();

  XrdCl::URL dUrl(sdsturl);

  if ( sourcefile.endswith( ".dump" ) ) {
    isdumpfile = true;
  }

  XrdCl::File* file  = 0;  
  
  int inotify_fd = -1;
  int watch_fd = -1;

#ifndef __APPLE__
  inotify_fd = inotify_init();

  if ( inotify_fd < 0 ) {
    fprintf(stderr,"error: unable to initialize inotify interface - will use polling\n");
  }
#endif

  do {
    fd = open( sourcefile.c_str(), O_RDONLY );

    if ( fd < 0 ) {
      XrdSysTimer sleeper;
      sleeper.Wait( 1000 );
    }
  } while ( fd < 0 );

 
 again:
  XrdCl::OpenFlags::Flags flags_xrdcl;
  XrdCl::Access::Mode mode_xrdcl = XrdCl::Access::UR | XrdCl::Access::UW | XrdCl::Access::GR |
                        XrdCl::Access::GW | XrdCl::Access::OR;
  
  file = new XrdCl::File();

#ifndef __APPLE__
  if (watch_fd) {
    inotify_rm_watch(inotify_fd, watch_fd);
    close(watch_fd);
  }

  watch_fd = inotify_add_watch(inotify_fd, sourcefile.c_str(), IN_MODIFY | IN_MOVE_SELF );

#endif

  if ( !file ) {
    fprintf( stderr, "Error: cannot create XrdCl object\n" );
    exit( -1 );

  }

  flags_xrdcl = XrdCl::OpenFlags::MakePath | XrdCl::OpenFlags::Update;

  if ( !file->Open( dsturl.c_str(), flags_xrdcl, mode_xrdcl ).IsOK() ) {
    eos_static_info( "Creating the file..." );
    flags_xrdcl = XrdCl::OpenFlags::MakePath | XrdCl::OpenFlags::New;
    delete file;
    file = new XrdCl::File();
    if ( !file->Open( dsturl.c_str(), flags_xrdcl, mode_xrdcl ).IsOK() ) {
      eos_static_err( "cannot open remote file %s", dsturl.c_str() );
      delete file;
      exit( -1 );
    }
  }

  struct stat srcstat;

  XrdCl::StatInfo* dststat = 0;

  do {
    struct stat src_curr_stat;

    if ( fstat( fd, &srcstat ) ) {
      eos_static_err( "cannot stat source file %s - retry in 1 minute ...", sourcefile.c_str() );
      sleep( 60 );
      continue;
    }
    
    int cantstat=0;

    do {
      if (!(cantstat=stat(sourcefile.c_str(), &src_curr_stat))) {
	if (src_curr_stat.st_ino != srcstat.st_ino) {
	  eos_static_notice("source file has been replaced");
	  close(fd);
	  do {
	    fd= open (sourcefile.c_str(),O_RDONLY);
	    if (fd<0) {
	      sleep(1);
	    }
	  } while(fd <0 );

	  if (fstat(fd, &srcstat)) {
	    eos_static_err("cannot stat source file %s - retry in 1 minute ...", sourcefile.c_str());
	    sleep(60);
	    continue;
	  }

	  eos_static_notice("re-opened source file");

	  if (isdumpfile) {
	    if (!file->Truncate( 0 ).IsOK() ) {
	      eos_static_crit("couldn't truncate remote file");
	      sleep(60);
	      if (file) delete file;
	      goto again;
	    }
	  } else {
	    eos_static_notice("re-opened source file");
	    file->Close();
	    if (file) delete file;
	    
	    XrdCl::FileSystem FsSync ( dUrl );
	    XrdOucString sourcebackupfile = sourcefile;
	    sourcebackupfile += ".";
	    sourcebackupfile += (int)time(NULL);
	    std::string s_file = sourcefile.c_str();
	    std::string d_file = sourcebackupfile.c_str();
	    if (!FsSync.Mv(s_file, d_file).IsOK()) {
	      eos_static_crit("couldn't rename %s=>%s\n", sourcefile.c_str(), sourcebackupfile.c_str());
	    }
	    goto again;
	  }
	}
      } else {
	sleep(1);
      }
    } while (cantstat);

    if ( !file->Stat( true, dststat ).IsOK() ) {
      eos_static_crit( "cannot stat destination file %s", dsturl.c_str() );
      delete file;
      exit( -1 );
    }

    localoffset = srcstat.st_size;
    remoteoffset = dststat->GetSize();

    if ( isdumpfile ) {
      if ( !file->Truncate( 0 ).IsOK() ) {
        eos_static_crit("couldn't truncate remote file");
	sleep(60);
	if (file) delete file;
	goto again;
      }

      remoteoffset = 0;
    } else {
      if ( remoteoffset > localoffset ) {
        eos_static_err( "remote file is longer than local file - force truncation\n" );

        if ( !file->Truncate( 0 ).IsOK() ) {
          eos_static_crit("couldn't truncate remote file");
	  sleep(60);
	  if (file) delete file;
	  
	  XrdCl::FileSystem FsSync ( dUrl );
	  XrdOucString sourcebackupfile = sourcefile;
	  sourcebackupfile += ".";
	  sourcebackupfile += (int)time(NULL);
	  std::string s_file = sourcefile.c_str();
	  std::string d_file = sourcebackupfile.c_str();
	  if (!FsSync.Mv(s_file, d_file).IsOK()) {
	    eos_static_crit("couldn't rename %s=>%s\n", sourcefile.c_str(), sourcebackupfile.c_str());
	  }
	  goto again;
        }

        remoteoffset = 0;
      }
    }

    size_t transfersize = 0;

    if ( ( localoffset - remoteoffset ) > TRANSFERBLOCKSIZE ) {
      transfersize = TRANSFERBLOCKSIZE;
    }
    else {
      transfersize = ( localoffset - remoteoffset );
    }

    off_t mapoffset = remoteoffset - ( remoteoffset % PAGESIZE );
    size_t mapsize = transfersize + ( remoteoffset % PAGESIZE );

    if (!transfersize) {      
      if ( (inotify_fd  >= 0 ) && (watch_fd >=0) ) {
	// use inotify to wait for changes on our file, we don't really care to look at the event since we look only at a single file
	ssize_t length = read( inotify_fd, buffer, EVENT_BUF_LEN );
	if (!length) {
	  eos_static_crit("read via inotify returned errno=%d", errno);
	}
      } else {
	// sleep 1ms
	usleep(1000);
      }
    } else {
      eos_static_debug( "remoteoffset=%llu module=%llu mapoffset=%llu mapsize =%lu",
                        ( unsigned long long )remoteoffset,
                        ( unsigned long long )remoteoffset % PAGESIZE,
                        ( unsigned long long )mapoffset,
                        ( unsigned long )mapsize );
      char* copyptr = ( char* ) mmap( NULL, mapsize, PROT_READ, MAP_SHARED, fd, mapoffset );

      if ( !copyptr ) {
        eos_static_crit( "cannot map source file at %llu", ( unsigned long long )remoteoffset );
        exit( -1 );
      }

      if ( !file->Write( remoteoffset, transfersize, copyptr + ( remoteoffset % PAGESIZE ) ).IsOK() ) {
        eos_static_err( "cannot write remote block at %llu/%lu",
                        ( unsigned long long )remoteoffset,
                        ( unsigned long )transfersize );
        sleep( 60 );
      }

      munmap( copyptr, mapsize );
    }

    gettimeofday( &sync2, &tz );
    float syncperiod = ( ( ( sync2.tv_sec - sync1.tv_sec ) * 1000.0 ) +
                         ( ( sync2.tv_usec - sync1.tv_usec ) / 1000.0 ) );

    // every 1000 ms we send a sync command
    if ( syncperiod > 1000 ) {
      if ( !file->Sync().IsOK() ) {
        eos_static_crit( "cannot sync remote file" );
        delete dststat;
        delete file;
        exit( -1 );
      }
    }

    if ( isdumpfile ) {
      // update the file every 5 seconds
      sleep( 5 );
    }
  } while ( 1 );

  delete dststat;
  delete file;
}


