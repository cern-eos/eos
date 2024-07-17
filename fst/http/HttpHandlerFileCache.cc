// ----------------------------------------------------------------------
// File: HttpHandlerFileCache.cc
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2024 CERN/Switzerland                                  *
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

#include "fst/http/HttpHandlerFileCache.hh"

EOSFSTNAMESPACE_BEGIN

HttpHandlerFileCache::HttpHandlerFileCache()
{
  mThreadId.reset(&HttpHandlerFileCache::Run, this);
}

HttpHandlerFileCache::~HttpHandlerFileCache()
{
  mThreadId.join();
}

bool HttpHandlerFileCache::insert(const HttpHandlerFileCache::Key &k, XrdFstOfsFile* const v)
{
  if (!k) return false;

  return false;
}

XrdFstOfsFile* HttpHandlerFileCache::remove(const HttpHandlerFileCache::Key &k)
{
  if (!k) return nullptr;

  return nullptr;
}


/*----------------------------------------------------------------------------*/
void HttpHandlerFileCache::Run(ThreadAssistant& assistant) noexcept
{
  while (!assistant.terminationRequested())
  {
    assistant.wait_for(std::chrono::seconds(1));
  }
}

EOSFSTNAMESPACE_END
