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

//------------------------------------------------------------------------------
// author: Georgios Bitzes <georgios.bitzes@cern.ch>
// desc:   A helper function to run non-atomic "rm -rf" on a path
//------------------------------------------------------------------------------

#pragma once
#include "namespace/interface/IContainerMD.hh"
#include "namespace/interface/IFileMD.hh"
#include "namespace/interface/ContainerIterators.hh"
#include "namespace/Namespace.hh"

EOSNSNAMESPACE_BEGIN

class RmrfHelper
{
public:

//------------------------------------------------------------------------------
// Run rm -rf on a given directory path.
//------------------------------------------------------------------------------
  static void nukeDirectory(eos::IView* view, const std::string& path)
  {
    IFileMDSvc* fileSvc = view->getFileMDSvc();
    std::shared_ptr<eos::IContainerMD> cont = view->getContainer(path);
    std::shared_ptr<eos::IFileMD> file;

    for (auto itf = eos::FileMapIterator(cont); itf.valid(); itf.next()) {
      file = fileSvc->getFileMD(itf.value());

      if (file) {
        fileSvc->removeFile(file.get());
      }
    }

    std::vector<std::string> subcontainers;

    for (auto itc = eos::ContainerMapIterator(cont); itc.valid(); itc.next()) {
      std::ostringstream newpath;
      newpath << path;

      if (path[path.size() - 1] != '/') {
        newpath << "/";
      }

      newpath << itc.key();
      subcontainers.emplace_back(newpath.str());
    }

    for (size_t i = 0; i < subcontainers.size(); i++) {
      nukeDirectory(view, subcontainers[i]);
    }

    view->removeContainer(path);
  }
};

EOSNSNAMESPACE_END
