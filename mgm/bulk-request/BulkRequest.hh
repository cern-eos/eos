//------------------------------------------------------------------------------
//! @file BulkRequest.hh
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

#ifndef EOS_BULKREQUEST_HH
#define EOS_BULKREQUEST_HH

#include "mgm/Namespace.hh"
#include <string>
#include <set>
#include <map>
#include "FileCollection.hh"

EOSBULKNAMESPACE_BEGIN

/**
 * This class represents a BulkRequest object
 */
class BulkRequest {
public:


  /**
   * The type a bulk request can be
   */
  enum Type {
    PREPARE_STAGE,
    PREPARE_EVICT
  };

  /**
   * Initializes the bulk request with the id passed in parameter
   * @param id the unique identifier of the bulk request
   */
  BulkRequest(const std::string & id);
  /**
   * Returns the id of this bulk request
   * @return the id of this bulk request
   */
  const std::string getId() const;
  /**
   * Returns the type of this bulk request
   * @return the type of this bulk request
   */
  virtual const Type getType() const = 0;

  /**
   * Returns the files contained in this bulk request
   * @return the set containing the files this bulk request manages
   */
  const std::shared_ptr<FileCollection::Files> getFiles() const;

  /**
   * Add a file path to this bulk request
   * @param file the pointer to the file to add to this bulk request
   */
  void addPath(const std::string & path);

  /**
   * Add an error text to the path that
   * @param path the path of the file that had an error
   * @param error the error that the file has
   */
  void addError(const std::string & path, const std::string & error);

  virtual ~BulkRequest(){}

  /**
   * Return the string representation of the bulk request type passed in parameter
   * @param type the type of the bulk request
   * @return the string representation of the bulk request type passed in parameter
   */
  static const std::string bulkRequestTypeToString(const Type & bulkRequestType);

private:
  //Id of the bulk request
  std::string mId;
  //The collection of files that are hold by this bulk-request
  FileCollection mFileCollection;
  //Map containing the string representation of each bulk-request type
  static const std::map<Type,std::string> BULK_REQ_TYPE_TO_STRING_MAP;
};

EOSBULKNAMESPACE_END
#endif // EOS_BULKREQUEST_HH
