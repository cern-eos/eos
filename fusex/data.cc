//------------------------------------------------------------------------------
//! @file data.cc
//! @author Andreas-Joachim Peters CERN
//! @brief meta data handling class
//------------------------------------------------------------------------------

/************************************************************************
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

#include "data.hh"
#include "kv.hh"
#include "MacOSXHelper.hh"
#include "common/Logging.hh"

#include <iostream>
#include <sstream>


/* -------------------------------------------------------------------------- */
data::data() : ioflush(0), ioqueue_max_backlog(1000)
/* -------------------------------------------------------------------------- */
{

}

/* -------------------------------------------------------------------------- */
data::~data()
/* -------------------------------------------------------------------------- */
{

}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
data::init()
/* -------------------------------------------------------------------------- */
{
  
}

/* -------------------------------------------------------------------------- */
data::shared_data
/* -------------------------------------------------------------------------- */
data::get(fuse_req_t req,
           fuse_ino_t ino)
/* -------------------------------------------------------------------------- */
{
  XrdSysMutexHelper mLock(datamap);
  if (datamap.count(ino))
  {
    shared_data io = datamap[ino];
    return io;
  }
  else
  {
    std::string mdstream;
    shared_data io = std::make_shared<datax>();
    io->set_id(ino);
    datamap[io->id()] = io;
    return io;
  }
}

/* -------------------------------------------------------------------------- */
uint64_t
/* -------------------------------------------------------------------------- */
data::commit(fuse_req_t req,
              data::shared_data io
             )
/* -------------------------------------------------------------------------- */
{
  XrdSysMutexHelper mLock(datamap);
  datamap[io->id()]=io;

  ioflush.Lock();

  while (ioqueue.size() == ioqueue_max_backlog)
    ioflush.Wait();
  ioqueue.insert(io->id());

  ioflush.Signal();
  ioflush.UnLock();

  return io->id();
}

/* -------------------------------------------------------------------------- */
void 
/* -------------------------------------------------------------------------- */
data::unlink(fuse_ino_t ino)
/* -------------------------------------------------------------------------- */
{
  XrdSysMutexHelper mLock(datamap);
  if (datamap.count(ino))
  {
    datamap[ino]->unlink();
    datamap.erase(ino);
    eos_static_info("datacache::unlink size=%lu", datamap.size());
  }
  else
  {
    shared_data io = std::make_shared<datax>();
    io->set_id(ino);
    io->unlink();
  }
}

  
  
/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
data::dataxflush()
/* -------------------------------------------------------------------------- */
{
  while (1)
  {
    {
      ioflush.Lock();
      while (ioqueue.size() == 0)
        ioflush.Wait();

      auto it= ioqueue.begin();
      uint64_t ino = *it;
      ioqueue.erase(it);

      ioflush.UnLock();
      {
        XrdSysMutexHelper mLock(datamap);
        if (datamap.count(ino))
        {
          eos_static_info("datacache::flush ino=%08lx", (unsigned long long) ino);

          shared_data io = datamap[ino];
          io->flush();
        }
      }
    }
  }
}
