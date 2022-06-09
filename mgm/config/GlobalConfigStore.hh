//------------------------------------------------------------------------------
// File: GlobalConfigStore.hh
// Author: Abhishek Lekshmanan - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2022 CERN/Switzerland                                  *
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

#include "common/config/ConfigStore.hh"
#include "mgm/FsView.hh"

namespace eos::mgm
{

class GlobalConfigStore final: public common::ConfigStore
{
public:
  GlobalConfigStore(FsView* _fsView): common::ConfigStore(), fsView(_fsView) {}
  ~GlobalConfigStore() = default;

  bool save(const std::string& key, const std::string& val) override
  {
    if (fsView == nullptr) {
      eos_crit("%s", "msg=\"Cannot save, FsView in Invalid State!\"");
      return false;
    }

    return fsView->SetGlobalConfig(key, val);
  }

  std::string load(const std::string& key) override
  {
    if (fsView == nullptr) {
      eos_crit("%s", "msg=\"Cannot load, FsView in Invalid State!\"");
      return {};
    }

    return fsView->GetGlobalConfig(key);
  }

private:
  FsView* fsView;
};

} // namespace eos::mgm


