//------------------------------------------------------------------------------
//! @file BehaviourConfig.hh
//------------------------------------------------------------------------------

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

#include "common/Namespace.hh"
#include "common/Logging.hh"
#include <mutex>

EOSCOMMONNAMESPACE_BEGIN

//! Type of supported behaviours
enum struct BehaviourType {
  None,
  RainMinFsidEntry,
  All,
};

//------------------------------------------------------------------------------
//! Class MgmBehaviour - object used to store the MGM behaviour changes
//------------------------------------------------------------------------------
class BehaviourConfig: public eos::common::LogId
{
public:

  //----------------------------------------------------------------------------
  //! Convert string to behaviour type
  //!
  //! @param input string representation
  //!
  //! @return behaviour type object
  //----------------------------------------------------------------------------
  static BehaviourType
  ConvertStringToBehaviour(const std::string& input);

  //----------------------------------------------------------------------------
  //! Convert behaviour type to string
  //!
  //! @param btype behaviour type object
  //!
  //! @return string representation
  //----------------------------------------------------------------------------
  static std::string
  ConvertBehaviourToString(const BehaviourType& btype);

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  BehaviourConfig() = default;

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~BehaviourConfig() = default;

  //----------------------------------------------------------------------------
  //! Check if there is any behaviour change
  //!
  //! @return true if behaviour changes are registered, otherwise false
  //----------------------------------------------------------------------------
  bool IsEmpty() const
  {
    std::unique_lock<std::mutex> lock(mMutex);
    return mMapBehaviours.empty();
  }

  //----------------------------------------------------------------------------
  //! Set behaviour change
  //!
  //! @param behaviour type of behaviour
  //! @param value configuration value
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool Set(BehaviourType behaviour, const std::string& value);

  //----------------------------------------------------------------------------
  //! Get behaviour configuration value
  //!
  //! @param behaviour behaviour type
  //!
  //! @return string
  //----------------------------------------------------------------------------
  std::string Get(const BehaviourType& behaviour) const;

  //----------------------------------------------------------------------------
  //! Clean the given behaviour type
  //!
  //! @param behaviour type of behaviour
  //----------------------------------------------------------------------------
  void Clear(const BehaviourType& behaviour);

  //----------------------------------------------------------------------------
  //! Check if given behaviour exists in the map. We don't care about its
  //! configuration value in this case.
  //!
  //! @param behaviour behaviour type
  //!
  //! @return true if it exists in the map, otherwise false
  //----------------------------------------------------------------------------
  bool Exists(const BehaviourType& behaviour) const;

  //----------------------------------------------------------------------------
  //! List all configured behaviours
  //!
  //! @param return map of behaviours
  //----------------------------------------------------------------------------
  std::map<std::string, std::string>
  List() const;

private:
  std::map<BehaviourType, std::string> mMapBehaviours;
  mutable std::mutex mMutex;
};

EOSCOMMONNAMESPACE_END
