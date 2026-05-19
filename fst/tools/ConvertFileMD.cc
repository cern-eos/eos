// /************************************************************************
//  * EOS - the CERN Disk Storage System                                   *
//  * Copyright (C) 2022 CERN/Switzerland                                  *
//  *                                                                      *
//  * This program is free software: you can redistribute it and/or modify *
//  * it under the terms of the GNU General Public License as published by *
//  * the Free Software Foundation, either version 3 of the License, or    *
//  * (at your option) any later version.                                  *
//  *                                                                      *
//  * This program is distributed in the hope that it will be useful,      *
//  * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
//  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
//  * GNU General Public License for more details.                         *
//  *                                                                      *
//  * You should have received a copy of the GNU General Public License    *
//  * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
//  ************************************************************************

#include "CLI/CLI.hpp"
#include "common/Logging.hh"
#include "common/StringUtils.hh"
#include "fst/filemd/FmdAttr.hh"
#include "fst/utils/FSPathHandler.hh"
#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>

//------------------------------------------------------------------------------
//! Set a protobuf message field by name using a string value. Returns true on
//! success, false otherwise (unknown field, repeated field, or parse error).
//------------------------------------------------------------------------------
static bool
SetProtoFieldByName(google::protobuf::Message& msg, const std::string& field_name,
                    const std::string& value, std::string& err)
{
  const google::protobuf::Descriptor* desc = msg.GetDescriptor();
  const google::protobuf::Reflection* refl = msg.GetReflection();
  const google::protobuf::FieldDescriptor* field = desc->FindFieldByName(field_name);

  if (field == nullptr) {
    err = "unknown field '" + field_name + "'";
    return false;
  }

  if (field->is_repeated()) {
    err = "field '" + field_name + "' is repeated and not supported";
    return false;
  }

  try {
    switch (field->cpp_type()) {
    case google::protobuf::FieldDescriptor::CPPTYPE_INT32:
      refl->SetInt32(&msg, field, static_cast<int32_t>(std::stoll(value)));
      break;

    case google::protobuf::FieldDescriptor::CPPTYPE_INT64:
      refl->SetInt64(&msg, field, std::stoll(value));
      break;

    case google::protobuf::FieldDescriptor::CPPTYPE_UINT32:
      refl->SetUInt32(&msg, field, static_cast<uint32_t>(std::stoull(value)));
      break;

    case google::protobuf::FieldDescriptor::CPPTYPE_UINT64:
      refl->SetUInt64(&msg, field, std::stoull(value));
      break;

    case google::protobuf::FieldDescriptor::CPPTYPE_DOUBLE:
      refl->SetDouble(&msg, field, std::stod(value));
      break;

    case google::protobuf::FieldDescriptor::CPPTYPE_FLOAT:
      refl->SetFloat(&msg, field, std::stof(value));
      break;

    case google::protobuf::FieldDescriptor::CPPTYPE_BOOL:
      refl->SetBool(&msg, field, (value == "true" || value == "1" || value == "True"));
      break;

    case google::protobuf::FieldDescriptor::CPPTYPE_STRING:
      refl->SetString(&msg, field, value);
      break;

    default:
      err = "unsupported field type for '" + field_name + "'";
      return false;
    }
  } catch (const std::exception& e) {
    err = std::string("failed to parse value '") + value + "': " + e.what();
    return false;
  }

  return true;
}

bool configureLogger(FILE* fp)
{
  if (fp == nullptr) {
    return false;
  }

  // Redirect stdout and stderr to the log file
  int ret = dup2(fileno(fp), fileno(stdout));

  if (ret != -1) {
    ret = dup2(fileno(fp), fileno(stderr));
  }

  return ret != -1;
}

int
main(int argc, char* argv[])
{
  std::string log_file {""};
  std::string log_level {"err"}; // accepts info, debug, err, crit, warning etc.
  CLI::App app("Tool to inspect filemd metadata");
  app.add_option("--log-level", log_level, "Logging level");
  app.require_subcommand();
  std::string file_path;
  auto inspect_subcmd = app.add_subcommand("inspect",
                        "inspect filemd attributes");
  inspect_subcmd->add_option("--path", file_path, "full path to file")
  ->required();
  inspect_subcmd->add_option("--log-file", log_file, "Log file for operations");
  std::string modify_path;
  std::string modify_field;
  std::string modify_value;
  auto modify_subcmd = app.add_subcommand("modify", "modify a filemd attribute field");
  modify_subcmd->add_option("--path", modify_path, "full path to file")->required();
  modify_subcmd
      ->add_option("--field", modify_field, "name of the protobuf field to overwrite")
      ->required();
  modify_subcmd->add_option("--value", modify_value, "new value for the given field")
      ->required();
  modify_subcmd->add_option("--log-file", log_file, "Log file for operations");

  try {
    app.parse(argc, argv);
  } catch (const CLI::ParseError& e) {
    return app.exit(e);
  }

  auto& g_logger = eos::common::Logging::GetInstance();
  g_logger.SetLogPriority(g_logger.GetPriorityByString(log_level.c_str()));
  g_logger.SetUnit("EOSFileMD");
  std::unique_ptr<FILE, int(*)(FILE*)> fptr {fopen(log_file.c_str(), "a+"), &fclose};

  if (fptr.get() && !configureLogger(fptr.get())) {
    std::cerr << "error: failed to setup logging using log_file: " << log_file
              << std::endl;
    return -1;
  }

  auto attr_handler = std::make_unique<eos::fst::FmdAttrHandler>(
                        eos::fst::makeFSPathHandler(""));

  if (app.got_subcommand("inspect")) {
    auto [status, fmd] = attr_handler->LocalRetrieveFmd(file_path);

    if (!status) {
      std::cerr << "error: failed to retreive filemd for path="
                << file_path << std::endl;
    }

    std::cout << fmd.mProtoFmd.DebugString() << std::endl;
  }

  if (app.got_subcommand("modify")) {
    auto [status, fmd] = attr_handler->LocalRetrieveFmd(modify_path);

    if (!status) {
      std::cerr << "error: failed to retrieve filemd for path=" << modify_path
                << std::endl;
      return -1;
    }

    std::string err;

    if (!SetProtoFieldByName(fmd.mProtoFmd, modify_field, modify_value, err)) {
      std::cerr << "error: " << err << std::endl;
      return -1;
    }

    if (!attr_handler->LocalPutFmd(fmd, modify_path)) {
      std::cerr << "error: failed to store updated filemd for path=" << modify_path
                << std::endl;
      return -1;
    }

    std::cout << fmd.mProtoFmd.DebugString() << std::endl;
  }

  return 0;
}
