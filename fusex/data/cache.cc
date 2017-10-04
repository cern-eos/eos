//------------------------------------------------------------------------------
//! @file cache.cc
//! @author Andreas-Joachim Peters CERN
//! @brief cache handler implementation
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

#include "cache.hh"
#include "diskcache.hh"
#include "memorycache.hh"
#include "journalcache.hh"
#include "common/Logging.hh"
#include "common/Path.hh"
#include "common/StringConversion.hh"
#include <unistd.h>

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
cachehandler::init(cachehandler::cacheconfig & _config)
/* -------------------------------------------------------------------------- */
{
  config = _config;

  if (config.type == cachehandler::cache_t::INVALID)
    return EINVAL;

  if (config.type == cachehandler::cache_t::DISK)
  {
    if (diskcache::init())
    {

      fprintf(stderr, "error: cache directory %s or %s cannot be initialized - check existance/permissions!\n",
              config.location.c_str(), config.journal.c_str());
      return EPERM;
    }
  }
  if (config.journal.length())
  {
    if (journalcache::init())
    {
      fprintf(stderr, "error: journal directory %s or %s cannot be initialized - check existance/permissions!\n",
              config.location.c_str(), config.journal.c_str());
      return EPERM;
    }
  }
  return 0;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
cachehandler::init_daemonized()
/* -------------------------------------------------------------------------- */
{
  int rc=0;

  if (config.type == cachehandler::cache_t::INVALID)
    return EINVAL;

  if (config.type == cachehandler::cache_t::DISK)
  {
    rc = diskcache::init_daemonized();
    if (rc) return rc;
   }
  if (config.journal.length())
  {
    rc = journalcache::init_daemonized();
    if (rc ) return rc;
  }
  return 0;
}

/* -------------------------------------------------------------------------- */
void
/* -------------------------------------------------------------------------- */
cachehandler::logconfig()
{
  eos_static_warning("data-cache-type        := %s",
                     (config.type == cachehandler::cache_t::MEMORY) ? "memory" :
                     "disk");

  if (config.type == cachehandler::cache_t::DISK)
  {
    eos_static_warning("data-cache-location  := %s",
                       config.location.c_str());

    std::string s;



    if (config.total_file_cache_size == 0)
    {
      eos_static_warning("data-cache-size      := unlimited");
    }
    else
    {
      eos_static_warning("data-cache-size      := %s",
                         eos::common::StringConversion::GetReadableSizeString(s, config.total_file_cache_size, "B"));
    }

    if (config.per_file_cache_max_size == 0)
    {
      eos_static_warning("cache-file-size      := unlimited");
    }
    else
    {
      eos_static_warning("cache-file-max-size  := %s",
                         eos::common::StringConversion::GetReadableSizeString(s, config.per_file_cache_max_size, "B"));
    }

    if (config.journal.length())
    {
      eos_static_warning("journal-location     := %s",
                         config.journal.c_str());
      if (config.total_file_journal_size == 0)
      {
        eos_static_warning("journal-cache-size   := unlimited");
      }
      else
      {
        eos_static_warning("journal-cache-size   := %s",
                           eos::common::StringConversion::GetReadableSizeString(s, config.total_file_journal_size, "B"));
      }
      if (config.per_file_journal_max_size == 0)
      {
        eos_static_warning("file-journal-max-size:= unlimited");
      }
      else
      {
        eos_static_warning("file-journal-max-size:= %s",
                           eos::common::StringConversion::GetReadableSizeString(s, config.per_file_journal_max_size, "B"));
      }
    }
    else
    {
      eos_static_warning("journal-location     := disabled");
    }
  }
}

/* -------------------------------------------------------------------------- */
cache::shared_io
/* -------------------------------------------------------------------------- */
cachehandler::get(fuse_ino_t ino)
/* -------------------------------------------------------------------------- */
{
  XrdSysMutexHelper mLock(instance());

  if (!instance().count(ino))
  {
    cache::shared_io entry;

    entry = std::make_shared<cache::io>(ino);

    if (instance().inmemory())
    {
      entry->set_file ( new memorycache(ino) );
    }
    else
    {
      entry->set_file ( new diskcache (ino) );
    }

    if (instance().journaled())
    {
      entry->set_journal ( new journalcache(ino) );
    }

    (instance())[ino] =  entry;
    return entry;
  }
  else
  {

    return (instance())[ino];
  }
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
cachehandler::rm(fuse_ino_t ino)
/* -------------------------------------------------------------------------- */
{
  XrdSysMutexHelper mLock(instance());

  if (instance().count(ino))
  {
    instance().erase(ino);
  }
  return 0;
}


