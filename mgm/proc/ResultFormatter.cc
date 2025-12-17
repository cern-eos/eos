//------------------------------------------------------------------------------
//! @file ResultFormatter.cc
//! @brief Implementation of ResultFormatter utility class
//! @author Octavian-Mihai Matei - CERN
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

#include "mgm/proc/ResultFormatter.hh"
#include "mgm/proc/IProcCommand.hh"
#include "common/StringConversion.hh"
#include "common/Logging.hh"
#include <XrdOuc/XrdOucTokenizer.hh>
#include <XrdOuc/XrdOucString.hh>
#include <json/json.h>
#include <sstream>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Format output from string-based results
//------------------------------------------------------------------------------
std::string
ResultFormatter::Format(const std::string& stdOut,
                        const std::string& stdErr,
                        int retc,
                        const std::string& format,
                        const std::string& cmd,
                        const std::string& subcmd,
                        const std::string& callback,
                        const eos::common::VirtualIdentity* vid)
{
  if (format == "json") {
    return FormatJson(stdOut, stdErr, retc, cmd, subcmd, callback, vid);
  } else if (format == "http") {
    return FormatHttp(stdOut, stdErr, retc, cmd, subcmd);
  } else if (format == "fuse") {
    return FormatFuse(stdOut);
  } else {
    return FormatDefault(stdOut, stdErr, retc);
  }
}

//------------------------------------------------------------------------------
// Format output from XrdOucString-based results
//------------------------------------------------------------------------------
std::string
ResultFormatter::Format(const XrdOucString& stdOut,
                        const XrdOucString& stdErr,
                        int retc,
                        const std::string& format,
                        const XrdOucString& cmd,
                        const XrdOucString& subcmd,
                        const XrdOucString& callback,
                        const eos::common::VirtualIdentity* vid)
{
  // Delegate to the std::string version
  return Format(std::string(stdOut.c_str()),
                std::string(stdErr.c_str()),
                retc,
                format,
                std::string(cmd.c_str()),
                std::string(subcmd.c_str()),
                std::string(callback.c_str()),
                vid);
}

//------------------------------------------------------------------------------
// Format output from protobuf ReplyProto
//------------------------------------------------------------------------------
std::string
ResultFormatter::FormatFromProto(const eos::console::ReplyProto& reply,
                                 const std::string& format,
                                 const std::string& cmd,
                                 const std::string& subcmd,
                                 const std::string& callback,
                                 const eos::common::VirtualIdentity* vid)
{
  return Format(reply.std_out(), reply.std_err(), reply.retc(),
                format, cmd, subcmd, callback, vid);
}

//------------------------------------------------------------------------------
// Format output in default CGI format
//------------------------------------------------------------------------------
std::string
ResultFormatter::FormatDefault(const std::string& stdOut,
                               const std::string& stdErr,
                               int retc)
{
  using eos::common::StringConversion;
  // Convert to XrdOucString for Seal operation
  XrdOucString xStdOut = stdOut.c_str();
  XrdOucString xStdErr = stdErr.c_str();
  StringConversion::Seal(xStdOut);
  StringConversion::Seal(xStdErr);
  std::string result = "mgm.proc.stdout=";
  result += xStdOut.c_str();
  result += "&mgm.proc.stderr=";
  result += xStdErr.c_str();
  result += "&mgm.proc.retc=";
  result += std::to_string(retc);
  return result;
}

//------------------------------------------------------------------------------
// Format output in JSON format (reuses IProcCommand logic)
//------------------------------------------------------------------------------
std::string
ResultFormatter::FormatJson(const std::string& stdOut,
                            const std::string& stdErr,
                            int retc,
                            const std::string& cmd,
                            const std::string& subcmd,
                            const std::string& callback,
                            const eos::common::VirtualIdentity* vid)
{
  using eos::common::StringConversion;
  Json::Value json;

  try {
    json["errormsg"] = stdErr;
    json["retc"] = std::to_string(retc);
    Json::Value jsonOut;

    if (!stdOut.empty()) {
      // Reuse existing conversion logic from IProcCommand
      jsonOut = IProcCommand::ConvertOutputToJsonFormat(stdOut);
    }

    if (!cmd.empty()) {
      if (!subcmd.empty()) {
        json[cmd][subcmd] = jsonOut;
      } else {
        json[cmd] = jsonOut;
      }
    } else {
      json["result"] = jsonOut;
    }
  } catch (Json::Exception& e) {
    eos_static_err("Json conversion exception cmd=%s subcmd=%s emsg=\"%s\"",
                   cmd.c_str(), subcmd.c_str(), e.what());
    json["errormsg"] = "illegal string in json conversion";
    json["retc"] = std::to_string(EFAULT);
  }

  std::ostringstream oss;
  oss << json;
  std::string jsonStr = oss.str();

  if (!callback.empty()) {
    // JSONP format
    std::string result = callback;
    result += "([\n";
    result += jsonStr;
    result += "\n]);";
    return result;
  } else {
    bool isHttp = false;

    if (vid) {
      std::string prot_str(vid->prot.c_str());
      isHttp = (prot_str.find("http") == 0);
    }

    if (isHttp) {
      return jsonStr;
    } else {
      XrdOucString xJsonStr = jsonStr.c_str();
      StringConversion::Seal(xJsonStr);
      std::string result = "mgm.proc.json=";
      result += xJsonStr.c_str();
      return result;
    }
  }
}

//------------------------------------------------------------------------------
// Format output in HTTP format
//------------------------------------------------------------------------------
std::string
ResultFormatter::FormatHttp(const std::string& stdOut,
                            const std::string& stdErr,
                            int retc,
                            const std::string& cmd,
                            const std::string& subcmd)
{
  std::string result;
  result += "<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.1//EN\" "
            "\"http://www.w3.org/TR/xhtml11/DTD/xhtml11.dtd\">\n";
  result += "<html>\n";
  result += "<TITLE>EOS-HTTP</TITLE> "
            "<link rel=\"stylesheet\" "
            "href=\"http://www.w3.org/StyleSheets/Core/Midnight\"> \n";
  result += "<meta charset=\"utf-8\"> \n";

  // Block cross-site scripting in responses
  if (!stdErr.empty()) {
    result += "<meta http-equiv=\"Content-Security-Policy\" "
              "content=\"script-src https://code.jquery.com 'self';\">\n";
  }

  result += "<div class=\"httptable\" id=\"";
  result += cmd;
  result += "_";
  result += subcmd;
  result += "\">\n";
  std::string output = stdOut;

  if (!output.empty() && KeyValToHttpTable(output)) {
    result += output;
  } else {
    if (!stdErr.empty() || retc) {
      result += stdOut;
      result += "<h3>&#9888;&nbsp;<font color=\"red\">";
      result += stdErr;
      result += "</font></h3>";
    } else {
      if (stdOut.empty()) {
        result += "<h3>&#10004;&nbsp;Success!</h3>";
      } else {
        result += stdOut;
      }
    }
  }

  result += "</div></html>";
  return result;
}

//------------------------------------------------------------------------------
// Format output in FUSE format
//------------------------------------------------------------------------------
std::string
ResultFormatter::FormatFuse(const std::string& stdOut)
{
  std::string result = stdOut;

  if (!result.empty() && result[result.length() - 1] != '\n') {
    result += "\n";
  }

  return result;
}

//------------------------------------------------------------------------------
// Convert key-value monitor output to HTML table (copied from ProcCommand)
//------------------------------------------------------------------------------
bool
ResultFormatter::KeyValToHttpTable(std::string& output)
{
  using eos::common::StringConversion;
  // Replace "= " with "=\"\""
  XrdOucString xoutput = output.c_str();

  while (xoutput.replace("= ", "=\"\"")) {}

  output = xoutput.c_str();
  std::string stmp = output;
  XrdOucTokenizer tokenizer(const_cast<char*>(stmp.c_str()));
  const char* line;
  bool ok = true;
  std::vector<std::string> keys;
  std::vector<std::map<std::string, std::string>> keyvaluetable;
  std::string table;

  while ((line = tokenizer.GetLine())) {
    if (strlen(line) <= 1) {
      continue;
    }

    std::map<std::string, std::string> keyval;

    if (StringConversion::GetKeyValueMap(line, keyval,
                                         "=", " ", &keys)) {
      keyvaluetable.push_back(keyval);
    } else {
      ok = false;
      break;
    }
  }

  if (ok && !keys.empty()) {
    table += R"literal(<style>
table {
  table-layout:auto;
}
</style>
)literal";

    table += "<table border=\"8\" cellspacing=\"10\" cellpadding=\"20\">\n";
    
    // Build the header
    table += "<tr>\n";
    for (size_t i = 0; i < keys.size(); i++) {
      table += "<th><font size=\"2\">";
      
      // For keys don't print lengthy strings like a.b.c.d ... just print d
      std::string dotkeys = keys[i];
      size_t dotpos = dotkeys.rfind(".");
      if (dotpos != std::string::npos) {
        dotkeys.erase(0, dotpos + 1);
      }
      
      table += keys[i];
      table += "</font></th>\n";
    }
    table += "</tr>\n";

    // Build the rows
    for (size_t i = 0; i < keyvaluetable.size(); i++) {
      table += "<tr>\n";
      for (size_t j = 0; j < keys.size(); j++) {
        table += "<td nowrap=\"nowrap\"><font size=\"2\">";
        
        XrdOucString sizestring = keyvaluetable[i][keys[j]].c_str();
        unsigned long long val = StringConversion::GetSizeFromString(sizestring);
        
        if (errno || val == 0 || (!sizestring.isdigit())) {
          XrdOucString decodeURI = keyvaluetable[i][keys[j]].c_str();
          // Remove URI encoded spaces
          while (decodeURI.replace("%20", " ")) {}
          table += decodeURI.c_str();
        } else {
          StringConversion::GetReadableSizeString(sizestring, val, "");
          table += sizestring.c_str();
        }
        
        table += "</font></td>\n";
      }
      table += "</tr>\n";
    }

    table += "</table>\n";
    output = table;
  }

  return ok;
}

EOSMGMNAMESPACE_END
