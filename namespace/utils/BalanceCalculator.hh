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
// desc:   Utility to calculate balance statistics over a set of files
//------------------------------------------------------------------------------

#pragma once
namespace eos
{
//------------------------------------------------------------------------------
//! Class to calculate balance statistics over a set of files
//------------------------------------------------------------------------------
class BalanceCalculator
{
public:

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  BalanceCalculator()
  {
    filesystembalance.set_empty_key(0);
    spacebalance.set_empty_key("");
    schedulinggroupbalance.set_empty_key("");
    sizedistribution.set_empty_key(-1);
    sizedistributionn.set_empty_key(-1);
  }

  //----------------------------------------------------------------------------
  //! Take the given fmd into account for statistics calculations
  //----------------------------------------------------------------------------
  void account(const std::shared_ptr<eos::IFileMD>& fmd)
  {
    for (unsigned int i = 0; i < fmd->getNumLocation(); i++) {
      auto loc = fmd->getLocation(i);
      size_t size = fmd->getSize();

      if (!loc) {
        eos_static_err("fsid 0 found %s %llu", fmd->getName().c_str(), fmd->getId());
        continue;
      }

      filesystembalance[loc] += size;

      if ((i == 0) && (size)) {
        auto bin = (int) log10((double) size);
        sizedistribution[ bin ] += size;
        sizedistributionn[ bin ]++;
      }

      eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
      eos::common::FileSystem* filesystem = FsView::gFsView.mIdView.lookupByID(loc);

      if (filesystem != nullptr) {
        eos::common::FileSystem::fs_snapshot_t fs;

        if (filesystem->SnapShotFileSystem(fs, true)) {
          spacebalance[fs.mSpace] += size;
          schedulinggroupbalance[fs.mGroup] += size;
        }
      }
    }
  }

  //----------------------------------------------------------------------------
  //! Print a summary into the given stream
  //----------------------------------------------------------------------------
  void printSummary(std::ofstream& ss)
  {
    XrdOucString sizestring = "";

    for (const auto& it : filesystembalance) {
      ss << "fsid=" << it.first << " \tvolume=";
      ss << std::left << std::setw(12) <<
         eos::common::StringConversion::GetReadableSizeString(sizestring, it.second,
             "B");
      ss << " \tnbytes=" << it.second << std::endl;
    }

    for (const auto& its : spacebalance) {
      ss << "space=" << its.first << " \tvolume=";
      ss << std::left << std::setw(12) <<
         eos::common::StringConversion::GetReadableSizeString(sizestring, its.second,
             "B");
      ss << " \tnbytes=" << its.second << std::endl;
    }

    for (const auto& itg : schedulinggroupbalance) {
      ss << "sched=" << itg.first << " \tvolume=";
      ss << std::left << std::setw(12) <<
         eos::common::StringConversion::GetReadableSizeString(sizestring, itg.second,
             "B");
      ss << " \tnbytes=" << itg.second << std::endl;
    }

    for (const auto& itsd : sizedistribution) {
      unsigned long long lowerlimit = 0;
      unsigned long long upperlimit = 0;

      if (((itsd.first) - 1) > 0) {
        lowerlimit = pow(10, (itsd.first));
      }

      if ((itsd.first) > 0) {
        upperlimit = pow(10, (itsd.first) + 1);
      }

      XrdOucString sizestring1;
      XrdOucString sizestring2;
      XrdOucString sizestring3;
      XrdOucString sizestring4;
      unsigned long long avgsize = (sizedistributionn[itsd.first]
                                    ? itsd.second / sizedistributionn[itsd.first] : 0);
      ss << "sizeorder=" << std::right << setfill('0') << std::setw(
           2) << itsd.first;
      ss << " \trange=[ " << setfill(' ') << std::left << std::setw(12);
      ss << eos::common::StringConversion::GetReadableSizeString(
           sizestring1, lowerlimit, "B");
      ss << " ... " << std::left << std::setw(12);
      ss << eos::common::StringConversion::GetReadableSizeString(
           sizestring2, upperlimit, "B") << " ]";
      ss << " volume=" << std::left << std::setw(12);
      ss << eos::common::StringConversion::GetReadableSizeString(
           sizestring3, itsd.second, "B");
      ss << " \tavgsize=" << std::left << std::setw(12);
      ss << eos::common::StringConversion::GetReadableSizeString(
           sizestring4, avgsize, "B");
      ss << " \tnbytes=" << itsd.second;
      ss << " \t avgnbytes=" << avgsize;
      ss << " \t nfiles=" << sizedistributionn[itsd.first];
    }
  }

private:
  google::dense_hash_map<unsigned long, unsigned long long> filesystembalance;
  google::dense_hash_map<std::string, unsigned long long> spacebalance;
  google::dense_hash_map<std::string, unsigned long long> schedulinggroupbalance;
  google::dense_hash_map<int, unsigned long long> sizedistribution;
  google::dense_hash_map<int, unsigned long long> sizedistributionn;
};

}
