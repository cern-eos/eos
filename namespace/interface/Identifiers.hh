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

//------------------------------------------------------------------------------
// author: Georgios Bitzes <georgios.bitzes@cern.ch>
// desc:   Phantom types for file and container identifiers
//------------------------------------------------------------------------------

#ifndef EOS_NS_I_IDENTIFIERS_HH
#define EOS_NS_I_IDENTIFIERS_HH

#include "common/Murmur3.hh"
#include "namespace/Namespace.hh"

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Phantom types: strongly typed uint64_t, identifying files and containers.
//!
//! Unless explicitly asked with obj.(get/set)UnderlyingUInt64(), this will
//! generate glorious compiler errors when you try to misuse, such as adding
//! two FileIdentifiers together (which makes zero sense), accidentally store
//! them as int32, or try to mix them up.
//!
//! Bugs which would previously be detectable only at runtime, will now generate
//! compiler errors.
//!
//! Conversion to/from uint64_t should happen only when absolutely necessary,
//! at the boundaries of serialization / deserialization.
//!
//! Any sensible compiler should generate the same machine code, as with a plain
//! uint64_t - there should be no performance penalty.
//------------------------------------------------------------------------------


//------------------------------------------------------------------------------
//! FileIdentifier class
//------------------------------------------------------------------------------
class FileIdentifier
{
public:
  //----------------------------------------------------------------------------
  //! Prevent implicit conversions between this type and uint64_t, by making
  //! the constructor explicit.
  //----------------------------------------------------------------------------
  explicit FileIdentifier(uint64_t src) : val(src) {}

  //----------------------------------------------------------------------------
  //! Construct empty FileIdentifier.
  //----------------------------------------------------------------------------
  FileIdentifier() : val(0) {}

  //----------------------------------------------------------------------------
  //! Retrieve the underlying uint64_t. Use this only if you have to, ie
  //! when serializing to disk.
  //!
  //! The name is long and ugly on purpose, to make you think twice before
  //! using it. ;)
  //----------------------------------------------------------------------------
  uint64_t getUnderlyingUInt64() const
  {
    return val;
  }

  //----------------------------------------------------------------------------
  //! Comparison operator, so we can store those as keys in maps, etc.
  //----------------------------------------------------------------------------
  bool operator<(const FileIdentifier& other) const
  {
    return val < other.val;
  }

  //----------------------------------------------------------------------------
  //! Equality operator.
  //----------------------------------------------------------------------------
  bool operator==(const FileIdentifier& other) const
  {
    return val == other.val;
  }

private:
  uint64_t val;
};

//------------------------------------------------------------------------------
//! ContainerIdentifier class
//------------------------------------------------------------------------------
class ContainerIdentifier
{
public:
  //----------------------------------------------------------------------------
  //! Prevent implicit conversions between this type and uint64_t, by making
  //! the constructor explicit.
  //----------------------------------------------------------------------------
  explicit ContainerIdentifier(uint64_t src) : val(src) {}

  //----------------------------------------------------------------------------
  //! Construct empty ContainerIdentifier.
  //----------------------------------------------------------------------------
  ContainerIdentifier() : val(0) {}

  //----------------------------------------------------------------------------
  //! Retrieve the underlying uint64_t. Use this only if you have to, ie
  //! when serializing to disk.
  //!
  //! The name is long and ugly on purpose, to make you think twice before
  //! using it. ;)
  //----------------------------------------------------------------------------
  uint64_t getUnderlyingUInt64() const
  {
    return val;
  }

  //----------------------------------------------------------------------------
  //! Comparison operator, so we can store those as keys in maps, etc.
  //----------------------------------------------------------------------------
  bool operator<(const ContainerIdentifier& other) const
  {
    return val < other.val;
  }

  //----------------------------------------------------------------------------
  //! Equality operator.
  //----------------------------------------------------------------------------
  bool operator==(const ContainerIdentifier& other) const
  {
    return val == other.val;
  }

private:
  uint64_t val;
};

EOSNSNAMESPACE_END

namespace Murmur3 {

  //----------------------------------------------------------------------------
  //! MurmurHasher specialization for FileIdentifier.
  //----------------------------------------------------------------------------
  template<>
  struct MurmurHasher<eos::FileIdentifier> {
    size_t operator()(const eos::FileIdentifier &key) const
    {
      return MurmurHasher<uint64_t>{}(key.getUnderlyingUInt64());
    }
  };

  //----------------------------------------------------------------------------
  //! MurmurHasher specialization for ContainerIdentifier.
  //----------------------------------------------------------------------------
  template<>
  struct MurmurHasher<eos::ContainerIdentifier> {
    size_t operator()(const eos::ContainerIdentifier &key) const
    {
      return MurmurHasher<uint64_t>{}(key.getUnderlyingUInt64());
    }
  };
}

namespace std {
  //--------------------------------------------------------------------------
  // ! Specialization of std::hash for FileIdentifier
  //--------------------------------------------------------------------------
  template<> struct hash<eos::FileIdentifier>
  {
    typedef eos::FileIdentifier argument_type;
    typedef std::size_t result_type;

    result_type operator()(argument_type const& fid) const noexcept
    {
      return Murmur3::MurmurHasher<argument_type>{}(fid);
    }
  };

  //--------------------------------------------------------------------------
  // ! Specialization of std::hash for ContainerIdentifier
  //--------------------------------------------------------------------------
  template<> struct hash<eos::ContainerIdentifier>
  {
    typedef eos::ContainerIdentifier argument_type;
    typedef std::size_t result_type;

    result_type operator()(argument_type const& cid) const noexcept
    {
      return Murmur3::MurmurHasher<argument_type>{}(cid);
    }
  };
} // namespace std

#endif
