//------------------------------------------------------------------------------
//! @author Georgios Bitzes <georgios.bitzes@cern.ch>
//! @brief Small in-memory changelist for applying onto STL sets
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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

#pragma once

#include "namespace/Namespace.hh"
#include <list>
#include <stdlib.h>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! A ChangeList to apply onto an STL set
//------------------------------------------------------------------------------
template<typename T>
class SetChangeList
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  SetChangeList() {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~SetChangeList() {}

  //----------------------------------------------------------------------------
  //! Insert an item into the list.
  //----------------------------------------------------------------------------
  void push_back(const T& element)
  {
    mItems.emplace_back(OperationType::kInsertion, element);
  }

  //----------------------------------------------------------------------------
  //! Insert a tombstone into the list.
  //----------------------------------------------------------------------------
  void erase(const T& element)
  {
    mItems.emplace_back(OperationType::kDeletion, element);
  }

  //----------------------------------------------------------------------------
  //! Get size.
  //----------------------------------------------------------------------------
  size_t size() const
  {
    return mItems.size();
  }

  //----------------------------------------------------------------------------
  //! Clear.
  //----------------------------------------------------------------------------
  void clear()
  {
    mItems.clear();
  }

  //----------------------------------------------------------------------------
  //! Apply change list to given container.
  //----------------------------------------------------------------------------
  template<typename Container>
  void apply(Container& container) const
  {
    for (auto it = mItems.begin(); it != mItems.end(); it++) {
      if (it->operationType == OperationType::kInsertion) {
        container.insert(it->item);
      } else if (it->operationType == OperationType::kDeletion) {
        container.erase(it->item);
      }
    }
  }

private:
  enum class OperationType {
    kInsertion,
    kDeletion
  };

  struct Item {
    Item(OperationType op, const T& it) : operationType(op), item(it) {}
    OperationType operationType;
    T item;
  };

  std::list<Item> mItems;
};


EOSNSNAMESPACE_END
