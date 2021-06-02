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
#include "common/VirtualIdentity.hh"
#include <string>
#include <set>
#include <map>

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
    PREPARE_STAGE
  };

  /**
   * Initializes the bulk request with the id passed in parameter
   * @param id the unique identifier of the bulk request
   * @param clientVid the virtual identity of the client who submitted the bulk request
   */
  BulkRequest(const std::string & id, const common::VirtualIdentity & clientVid);
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
   * Returns the paths of the files contained in this bulk request
   * @return the set containing the paths of the files this bulk request manages
   */
  const std::set<std::string> & getPaths() const;

  /**
   * Add a file path to this bulk request
   * @param path the path of the file to add to this bulk request
   */
  void addPath(const std::string & path);

  /**
   * Get the virtual identity of the client who created / submitted the bulk request
   * @return the virtual identity of the client who created / submitted the bulk request
   */
  const common::VirtualIdentity & getClientVirtualIdentity() const;

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
  //Paths of the files contained in the bulk request
  std::set<std::string> mPaths;
  // Virtual identity of the client who created/submitted the bulk request
  eos::common::VirtualIdentity mClientVid;
  //Initialize the map containing the string representation of each bulk-request type
  static const std::map<Type,std::string> createTypeToStringMap();
  //Map containing the string representation of each bulk-request type
  static const std::map<Type,std::string> BULK_REQ_TYPE_TO_STRING_MAP;
};

EOSBULKNAMESPACE_END
#endif // EOS_BULKREQUEST_HH
