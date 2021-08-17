//------------------------------------------------------------------------------
//! @file File.hh
//! @author Cedric Caffy - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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

#ifndef EOS_FILE_HH
#define EOS_FILE_HH

#include "mgm/Namespace.hh"
#include <string>
#include <optional>

EOSBULKNAMESPACE_BEGIN

/**
 * This class contains information
 * about a file hold in a bulk-request
 */
class File {
public:
  File();
  File(const std::string & path);
  void setPath(const std::string & path);
  void setError(const std::string & error);
  void setError(const std::optional<std::string> & error);
  /**
   * Set the error passed in parameter to the file
   * only if there is not already an error set
   * @param error the error to set
   */
  void setErrorIfNotAlreadySet(const std::string & error);

  std::string getPath() const;
  std::optional<std::string> getError() const;

  bool operator==(const File & other) const;
  bool operator<(const File & other) const;

private:
  /**
   * The path of the file
   */
  std::string mPath;
  /**
   * An eventual error message
   */
  std::optional<std::string> mError;
};

EOSBULKNAMESPACE_END

#endif // EOS_FILE_HH
