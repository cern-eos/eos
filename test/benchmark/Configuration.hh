//------------------------------------------------------------------------------
// File: Configuration.hh
// Author: Elvin-Alin Sindrilaru - CERN
//------------------------------------------------------------------------------

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

#ifndef __EOSBMK_CONFIGURATION_HH__
#define __EOSBMK_CONFIGURATION_HH__

/*----------------------------------------------------------------------------*/
#include "Namespace.hh"
#include "common/Logging.hh"
#include "test/benchmark/ConfigProto.pb.h"
/*----------------------------------------------------------------------------*/

EOSBMKNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class Configuration - extends the ConfigProto class obtained from compiling
//! the protocol buffer ConfigProto.proto by adding additional methods for
//! handling the information in such a configuration.
//------------------------------------------------------------------------------
class Configuration: public eos::common::LogId
{
  public:

    //--------------------------------------------------------------------------
    //! Constructor
    //--------------------------------------------------------------------------
    Configuration();


    //--------------------------------------------------------------------------
    //! Destructor
    //--------------------------------------------------------------------------
    virtual ~Configuration();


    //--------------------------------------------------------------------------
    //! Disable copy constructor
    //--------------------------------------------------------------------------
    Configuration(const Configuration&) = delete;


    //--------------------------------------------------------------------------
    //! Disable copy operator
    //--------------------------------------------------------------------------
    Configuration& operator =(const Configuration&) = delete;


    //--------------------------------------------------------------------------
    //! Set the low level configuration object
    //!
    //! @param pbConfig low level config object (ProtoBuf object)
    //!
    //--------------------------------------------------------------------------
    void SetPbConfig(ConfigProto* pbConfig);


    //--------------------------------------------------------------------------
    //! Get the low level configuration object (ProtBuf object)
    //!
    //! @return low level config object
    //!
    //--------------------------------------------------------------------------
    ConfigProto& GetPbConfig() const;


    //--------------------------------------------------------------------------
    //! Print configuration saved in the supplied file
    //--------------------------------------------------------------------------
    void Print();


    //--------------------------------------------------------------------------
    //! Check directory path exists and has correct attributes, if it does not
    //! exist then it is created and set the correct attributes. Also make sure
    //! that the required files for the operations are in place and if not they
    //! are generated.
    //!
    //! @return true if setup successful, otherwise false
    //!
    //--------------------------------------------------------------------------
    bool CheckDirAndFiles();


    //--------------------------------------------------------------------------
    //! Create configuration file - accept input from the console and build up
    //! the configuration object which is then written to the file supplied as
    //! an arg.
    //!
    //! @param outputFile file to which the newly configuration is saved
    //!
    //! @return true if successful, otherwise false
    //!
    //--------------------------------------------------------------------------
    bool CreateConfigFile(const std::string& outputFile);


    //--------------------------------------------------------------------------
    //! Read in configuration from file
    //!
    //! @param fileName file from which the configuration is read
    //!
    //! @return true if successful, otherwise false
    //!
    //--------------------------------------------------------------------------
    bool ReadFromFile(const std::string& fileName);


    //--------------------------------------------------------------------------
    //! Get int representation for the pattern type
    //!
    //! @parameter patternType string representation of the pattern type
    //!
    //! @return int representation of the pattern type
    //!
    //--------------------------------------------------------------------------
    static ConfigProto_PatternType GetPattern(const std::string& patternType);


    //--------------------------------------------------------------------------
    //! Get string representation for the pattern type
    //!
    //! @param patternType int representation of the pattern type
    //!
    //! @return string representation of the pattern type
    //!
    //--------------------------------------------------------------------------
    static std::string GetPattern(ConfigProto_PatternType patternType);


    //--------------------------------------------------------------------------
    //! Get int representation for the operation type
    //!
    //! @param opType string representation of the operation type
    //!
    //! @return int representation of the operation type
    //!
    //--------------------------------------------------------------------------
    static ConfigProto_OperationType GetOperation(const std::string& opType);


    //--------------------------------------------------------------------------
    //! Get string representation for the operation layout
    //!
    //! @param opType int representation of the operation type
    //!
    //! @return string representation of the operation type
    //!
    //--------------------------------------------------------------------------
    static std::string GetOperation(ConfigProto_OperationType opType);


    //--------------------------------------------------------------------------
    //! Get int representation for the file layout
    //!
    //! @param fileType string representation of the file type
    //!
    //! @return int representation of the file type
    //!
    //--------------------------------------------------------------------------
    static ConfigProto_FileLayoutType GetFileLayout(const std::string& fileType);


    //--------------------------------------------------------------------------
    //! Get string representation for the file layout
    //!
    //! @param fileType int representation of the file type
    //!
    //! @return string representation of the file type
    //!
    //--------------------------------------------------------------------------
    static std::string GetFileLayout(ConfigProto_FileLayoutType fileType);


    //--------------------------------------------------------------------------
    //! Generate the absolute path to the files which will be used by the
    //! threads/processes during the run
    //--------------------------------------------------------------------------
    void GenerateFileNames();


    //--------------------------------------------------------------------------
    //! Compute hash value for the current object as the hash value of a string
    //! made up by concatenating some of the fields of the current object
    //!
    //! @return hash value
    //!
    //--------------------------------------------------------------------------
    size_t GetHash();


    //--------------------------------------------------------------------------
    //! Get the file name at position indx from the vector of generated files
    //!
    //! @param indx index in the vector of file names
    //!
    //! @return file name
    //!
    //--------------------------------------------------------------------------
    inline std::string GetFileName(uint32_t indx) const
    {
      return mFileNames[indx];
    };

  private:

    std::vector<std::string> mFileNames; ///! generated file names
    ConfigProto* mPbConfig;  ///< low level ConfigProto class holding all the info

};

EOSBMKNAMESPACE_END

#endif // __EOSBMK_CONFIGURATION_HH__
