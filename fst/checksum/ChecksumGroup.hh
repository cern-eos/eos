// ----------------------------------------------------------------------
// File: ChecksumGroup.hh
// Author: Gianmaria Del Monte - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2025 CERN/Switzerland                                  *
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

#ifndef __EOSFST_CHECKSUM_GROUP_HH__
#define __EOSFST_CHECKSUM_GROUP_HH__

#include "fst/checksum/CheckSum.hh"
#include "fst/checksum/ChecksumPlugins.hh"

EOSFSTNAMESPACE_BEGIN

//----------------------------------------------------------------------------
//! @brief Manages a group of different checksum calculations.
//!
//! This class holds a map of CheckSum objects, each identified by its type.
//! It allows designating one checksum type as the "default" and performing
//! operations across all managed checksums.
//----------------------------------------------------------------------------
class ChecksumGroup
{
public:
  //----------------------------------------------------------------------------
  //! Get the default checksum
  //!
  //! @return checksum object
  //----------------------------------------------------------------------------
  inline CheckSum* GetDefault() const
  {
    if (mDefaultType == eos::common::LayoutId::eChecksum::kNone) {
      return nullptr;
    }

    return mChecksums.at(mDefaultType).get();
  }

  //----------------------------------------------------------------------------
  //! Set the default checksum for the group
  //! The object takes ownership of the checksum object
  //!
  //! @param xs ptr to checksum object
  //! @param xsType checksum type
  //----------------------------------------------------------------------------
  inline void SetDefault(CheckSum* xs,
                         eos::common::LayoutId::eChecksum xsType)
  {
    if (!xs) {
      return;
    }

    SetDefault(std::unique_ptr<CheckSum>(xs), xsType);
  }

  //----------------------------------------------------------------------------
  //! Set the default checksum for the group
  //!
  //! @param xs ptr to checksum object
  //! @param xsType checksum type
  //----------------------------------------------------------------------------
  inline void SetDefault(std::unique_ptr<CheckSum>&& xs,
                         eos::common::LayoutId::eChecksum xsType)
  {
    if (!xs) {
      return;
    }

    mDefaultType = xsType;
    mChecksums[xsType] = std::move(xs);
  }

  //----------------------------------------------------------------------------
  //! Add an alternative checksum to the group
  //! The object takes ownership of the checksum object
  //!
  //! @param xs ptr to checksum object
  //! @param xsType checksum type
  //----------------------------------------------------------------------------
  void AddAlternative(CheckSum* xs,
                      eos::common::LayoutId::eChecksum xsType)
  {
    mChecksums[xsType] = std::unique_ptr<CheckSum>(xs);
  }

  //----------------------------------------------------------------------------
  //! Add an alternative checksum to the group
  //!
  //! @param xs ptr to checksum object
  //! @param xsType checksum type
  //----------------------------------------------------------------------------
  void AddAlternative(std::unique_ptr<CheckSum>&& xs,
                      const std::string xsType)
  {
    auto t = static_cast<eos::common::LayoutId::eChecksum>
             (eos::common::LayoutId::GetChecksumFromString(xsType));
    mChecksums[t] = std::move(xs);
  }

  void AddAlternative(eos::common::LayoutId::eChecksum xs)
  {
    AddAlternative(ChecksumPlugins::GetXsObj(xs), xs);
  }

  //----------------------------------------------------------------------------
  //! Add a data chunk to all managed checksums
  //!
  //! @param xs buffer pointer to data buffer
  //! @param length size of the data chunk in bytes
  //! @param offset offst of the data chunk within the overall data stream
  //----------------------------------------------------------------------------
  bool Add(const char* buffer, size_t length, off_t offset)
  {
    for (auto& [_, xs] : mChecksums) {
      xs->Add(buffer, length, offset);
    }

    return true;
  }

  //----------------------------------------------------------------------------
  //! Initialize the default checksum with a prior state
  //!
  //! @param offsetInit the starting offset for the checksum calculation
  //! @param lengthInit the total expected length of the data for the checksum
  //! @param xsInitHex a hexadecimal string representing the initial checksum value
  //----------------------------------------------------------------------------
  void ResetInitDefault(off_t offsetInit, size_t lengthInit,
                        const char* xsInitHex)
  {
    ResetInit(mDefaultType, offsetInit, lengthInit, xsInitHex);
  }

  //----------------------------------------------------------------------------
  //! Initialize the specific checksum with a prior state
  //!
  //! @param xsType type of the checksum to initialize
  //! @param offsetInit the starting offset for the checksum calculation
  //! @param lengthInit the total expected length of the data for the checksum
  //! @param xsInitHex a hexadecimal string representing the initial checksum value
  //----------------------------------------------------------------------------
  void ResetInit(eos::common::LayoutId::eChecksum xsType, off_t offsetInit,
                 size_t lengthInit, const char* xsInitHex)
  {
    if (!mChecksums.count(xsType)) {
      return;
    }

    mChecksums[xsType]->ResetInit(offsetInit, lengthInit, xsInitHex);
  }

  //----------------------------------------------------------------------------
  //! Initialize the specific checksum with a prior state
  //!
  //! @param name name of the checksum to initialize
  //! @param offsetInit the starting offset for the checksum calculation
  //! @param lengthInit the total expected length of the data for the checksum
  //! @param xsInitHex a hexadecimal string representing the initial checksum value
  //----------------------------------------------------------------------------
  void ResetInit(std::string name, off_t offsetInit,
                 size_t lengthInit, const char* xsInitHex)
  {
    auto t = static_cast<eos::common::LayoutId::eChecksum>
             (eos::common::LayoutId::GetChecksumFromString(name));
    return ResetInit(t, offsetInit, lengthInit, xsInitHex);
  }

  //----------------------------------------------------------------------------
  //! Removes all checksum objects from the group
  //----------------------------------------------------------------------------
  void Clear()
  {
    mDefaultType = eos::common::LayoutId::eChecksum::kNone;
    mChecksums.clear();
  }

  //----------------------------------------------------------------------------
  //! Checks if the group contains any checksum objects
  //----------------------------------------------------------------------------
  bool HasChecksums() const
  {
    return mChecksums.size();
  }

  //----------------------------------------------------------------------------
  //! Checks if any checksum in the group requires recalculation
  //----------------------------------------------------------------------------
  bool NeedsRecalculation() const
  {
    for (const auto &[_, xs] : mChecksums) {
      if (xs->NeedsRecalculation()) {
        return true;
      }
    }

    return false;
  }

  //----------------------------------------------------------------------------
  //! Marks all the checksums in the group as dirty
  //----------------------------------------------------------------------------
  void SetDirty()
  {
    for (auto &[_, xs] : mChecksums) {
      xs->SetDirty();
    }
  }

  //----------------------------------------------------------------------------
  //! Finalizes the calculation for all checksums in the group
  //----------------------------------------------------------------------------
  void Finalize()
  {
    for (auto &[_, xs] : mChecksums) {
      xs->Finalize();
    }
  }

  off_t
  GetMaxOffset() const
  {
    return GetDefault()->GetMaxOffset();
  }

  //----------------------------------------------------------------------------
  //! Retrieves a map of all the alternative checksums
  //----------------------------------------------------------------------------
  std::map<eos::common::LayoutId::eChecksum, CheckSum*>
  GetAlternatives() const
  {
    std::map<eos::common::LayoutId::eChecksum, CheckSum*> res;

    for (auto &[xsType, xs] : mChecksums) {
      if (xsType != mDefaultType) {
        res[xsType] = xs.get();
      }
    }

    return res;
  }

  //----------------------------------------------------------------------------
  //! Resets the checksums in the group
  //----------------------------------------------------------------------------
  void Reset()
  {
    for (auto& [_, xs] : mChecksums) {
      xs->Reset();
    }
  }

  //----------------------------------------------------------------------------
  //! Scan of a complete file using an opened layout
  //----------------------------------------------------------------------------
  bool ScanFile(CheckSum::ReadCallBack rcb, unsigned long long& scansize,
                float& scantime, int rate = 0)
  {
    // @note: not nice, since this is a copy of the CheckSum implementation
    static int buffersize = 1024 * 1024;
    struct timezone tz;
    struct timeval opentime;
    struct timeval currenttime;
    scansize = 0;
    scantime = 0;
    gettimeofday(&opentime, &tz);
    Reset();
    //move at the right location in the  file
    int nread = 0;
    off_t offset = 0;
    char* buffer = (char*) malloc(buffersize);

    if (!buffer) {
      return false;
    }

    do {
      errno = 0;
      rcb.data.offset = offset;
      rcb.data.buffer = buffer;
      rcb.data.size = buffersize;
      nread = rcb.call(&rcb.data);

      if (nread < 0) {
        free(buffer);
        return false;
      }

      if (nread > 0) {
        Add(buffer, nread, offset);
        offset += nread;
      }

      if (rate) {
        // regulate the verification rate
        gettimeofday(&currenttime, &tz);
        scantime = (((currenttime.tv_sec - opentime.tv_sec) * 1000.0) + ((
                      currenttime.tv_usec - opentime.tv_usec) / 1000.0));
        float expecttime = (1.0 * offset / rate) / 1000.0;

        if (expecttime > scantime) {
          usleep(1000.0 * (expecttime - scantime));
        }
      }
    } while (nread == buffersize);

    gettimeofday(&currenttime, &tz);
    scantime = (((currenttime.tv_sec - opentime.tv_sec) * 1000.0) + ((
                  currenttime.tv_usec - opentime.tv_usec) / 1000.0));
    scansize = (unsigned long long) offset;
    Finalize();
    free(buffer);
    return true;
  }

private:
  eos::common::LayoutId::eChecksum mDefaultType {eos::common::LayoutId::eChecksum::kNone};
  std::map<eos::common::LayoutId::eChecksum, std::unique_ptr<CheckSum>>
      mChecksums;
};

EOSFSTNAMESPACE_END

#endif