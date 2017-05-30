/*
 * journalcache.hh
 *
 *  Created on: Mar 15, 2017
 *      Author: Michal Simon
 *
 ************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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

#ifndef FUSEX_JOURNALCACHE_HH_
#define FUSEX_JOURNALCACHE_HH_

#include "cache.hh"
#include "cachelock.hh"
#include "cachesyncer.hh"

#include "interval_tree.hh"

#include <stdint.h>

#include <string>

class journalcache : public cache
{

  struct header_t
  {
    uint64_t offset;
    uint64_t size;
  } ;

  struct update_t
  {
    off_t  offset;
    size_t size;
    const void  *buff;

    bool operator<( const update_t &u ) const
    {
      return offset < u.offset;
    }
  } ;

public:

  journalcache();
  journalcache( fuse_ino_t _ino );
  virtual ~journalcache();

  // base class interface
  virtual int attach(std::string& cookie);
  virtual int detach(std::string& cookie);
  virtual int unlink();

  virtual ssize_t pread( void *buf, size_t count, off_t offset );
  virtual ssize_t peek_read( char* &buf, size_t count, off_t offset );
  virtual void release_read();

  virtual ssize_t pwrite( const void *buf, size_t count, off_t offset );

  virtual int truncate( off_t );
  virtual int sync();

  virtual size_t size();

  virtual int set_attr(std::string& key, std::string& value) {return 0;}
  virtual int attr(std::string key, std::string& value) {return 0;}

  virtual int remote_sync( cachesyncer &syncer );

  static int init();

private:

  void process_intersection( interval_tree<uint64_t, const void*> &write, interval_tree<uint64_t, uint64_t>::iterator acr, std::vector<update_t> &updates );

  int location( std::string &path, bool mkpath=true );

  static uint64_t offset_for_update( uint64_t offset, uint64_t shift )
  {
    return offset + sizeof ( header_t ) + shift;
  }

  int update_cache( std::vector<update_t> &updates );

  int read_journal();

  fuse_ino_t                        ino;
  size_t                            cachesize;
  int                               fd;
  // the value is the offset in the cache file
  interval_tree<uint64_t, uint64_t> journal;
  size_t                            nbAttached;
  cachelock                         clck;
  XrdSysMutex                       mtx;
  bufferllmanager::shared_buffer    buffer;
  static bufferllmanager            sBufferManager;
  static std::string                sLocation;
  static size_t                     sMaxSize;
} ;

#endif /* FUSEX_JOURNALCACHE_HH_ */
