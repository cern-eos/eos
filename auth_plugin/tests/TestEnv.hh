//------------------------------------------------------------------------------
// File: TestEnv.hh
// Author: Elvin Sindrilaru <esindril@cern.ch> CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2013 CERN/Switzerland                                  *
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
#include <map>
#include <string>
/*----------------------------------------------------------------------------*/
#include "Namespace.hh"
/*----------------------------------------------------------------------------*/

EOSAUTHTEST_NAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! TestEnv class - not thread safe
//------------------------------------------------------------------------------
class TestEnv
{
 public:

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  TestEnv();


  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~TestEnv();
  

  //----------------------------------------------------------------------------
  //! Add new entry to the map of parameters
  //!
  //! @param key key to be inserted
  //! @param value value to the inserted
  //!
  //----------------------------------------------------------------------------
  void SetMapping(const std::string& key, const std::string& value);

  
  //----------------------------------------------------------------------------
  //! Get value corresponding to the key from the map
  //!
  //! @param key key to be searched in the map
  //!
  //! @return value stored in the map
  //! 
  //----------------------------------------------------------------------------
  std::string GetMapping(const std::string& key) const;
  

 private:

  std::map<std::string, std::string> mMapParam; ///< map testing parameters  
};

EOSAUTHTEST_NAMESPACE_END
