//------------------------------------------------------------------------------
//! @file ConsoleCompletion.cc
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

#include "ConsoleMain.hh"
#include "ConsoleCompletion.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucTokenizer.hh"
#include <string.h>
#include <readline/readline.h>
#include <readline/history.h>
#include <vector>

extern int com_ls(char*);

//------------------------------------------------------------------------------
// Helper function to extact the dirname and base name from a absolute or
// relative path. For example:
// "/a/b/c/d"  -> dirname: "/a/b/c/"   and basename: "d"
// "/a/b/c/d/" -> dirname: "/a/b/c/d/" and basename: ""
// "x/y/z"     -> dirname: "x/y/"      and basename: "z"
// "x/y/z/"    -> dirname: "x/y/z/"    and basename: ""
// ""          -> dirname: ""          and basename: ""
//------------------------------------------------------------------------------
void eos_path_split(const std::string& input, std::string& dirname,
                    std::string& basename)
{
  if (input.empty()) {
    dirname.clear();
    basename.clear();
    return;
  }

  size_t pos = input.rfind('/');

  if (pos == std::string::npos) {
    dirname.clear();
    basename = input;
    return;
  }

  dirname = input.substr(0, pos + 1);
  basename = input.substr(pos + 1);
  return;
}

//------------------------------------------------------------------------------
// EOS console custom completion function to be used by the readline library
// to provide autocompletion.
//------------------------------------------------------------------------------
char** eos_console_completion(const char* text, int start, int end)
{
  char** matches;
  matches = (char**) 0;
  // Disable filename cmopletion if our generator finds no matches
  rl_attempted_completion_over = 1;

  // If this word is at the start of the line, then it is a command
  // to complete.  Otherwise it is the name of a file in the current
  // directory.
  if (start == 0) {
    rl_completion_append_character = ' ';
    matches = rl_completion_matches(text, eos_command_generator);
    return matches;
  }

  XrdOucString cmd = rl_line_buffer;

  if (cmd.beginswith("mkdir ") ||
      cmd.beginswith("rmdir ") ||
      cmd.beginswith("find ") ||
      cmd.beginswith("cd ") ||
      cmd.beginswith("chown ") ||
      cmd.beginswith("chmod ") ||
      cmd.beginswith("attr ") ||
      cmd.beginswith("acl ")) {
    // dir completion
    rl_completion_append_character = '\0';
    matches = rl_completion_matches(text, eos_dir_generator);
  }

  if (cmd.beginswith("rm ") ||
      cmd.beginswith("ls ") ||
      cmd.beginswith("fileinfo ")) {
    // dir/file completion
    rl_completion_append_character = '\0';
    matches = rl_completion_matches(text, eos_all_generator);
  }

  return (matches);
}

//------------------------------------------------------------------------------
// EOS entry (files/directories) generator
//------------------------------------------------------------------------------
char* eos_entry_generator(const char* text, int state, bool only_dirs)
{
  static size_t index;
  static std::vector<std::string> entries;

  // If this is a new word to complete, initialize now.  This includes
  // saving the length of TEXT for efficiency, and initializing the index
  // variable to 0.
  if (!state) {
    index = 0;
    entries.clear();
    std::string inarg = text;
    std::string dirname;
    std::string basename;
    eos_path_split(inarg, dirname, basename);

    if (dirname.empty()) {
      inarg = gPwd.c_str();
    } else {
      if (dirname.at(0) == '/') {
        inarg = dirname;
      } else {
        inarg = gPwd.c_str() + dirname;
      }
    }

    bool oldsilent = silent;
    silent = true;
    XrdOucString comarg = "-F ";
    comarg += inarg.c_str();
    char buffer[4096];
    sprintf(buffer, "%s", comarg.c_str());
    com_ls((char*) buffer);
    silent = oldsilent;
    XrdOucTokenizer subtokenizer((char*) rstdout.c_str());

    do {
      subtokenizer.GetLine();
      XrdOucString entry = subtokenizer.GetToken();

      if (entry.length()) {
        if (entry.endswith('\n')) {
          entry.erase(entry.length() - 1);
        }

        if (only_dirs && !entry.endswith('/')) {
          continue;
        }

        if (rl_completion_type == 63) { // ? - list possible completions
          // When lising completions we need to return the basename of the
          // candidates
          if (basename.empty() ||
              ((strncmp(basename.c_str(), entry.c_str(), basename.length()) == 0) &&
               // Exclude exact matches
               (basename.length() < (size_t)entry.length()))) {
            entries.push_back(entry.c_str());
          }
        } else if (rl_completion_type == 9) { // TAB - do standard completion
          // When doing the standard completion we need to return the full path
          // as given by the user initially (i.e. not including the pwd deduction).
          if (basename.empty() ||
              (strncmp(basename.c_str(), entry.c_str(), basename.length()) == 0)) {
            std::string add_path = dirname;
            add_path += entry.c_str();
            entries.push_back(add_path.c_str());
          }
        }
      } else {
        break;
      }
    } while (1);
  }

  if (index < entries.size()) {
    return strdup(entries[index++].c_str());
  }

  return ((char*) 0);
}

//------------------------------------------------------------------------------
// EOS directories generator - similar to the above
//------------------------------------------------------------------------------
char* eos_dir_generator(const char* text, int state)
{
  return eos_entry_generator(text, state, true);
}

//------------------------------------------------------------------------------
// EOS files and directories generator - similar to the above
//------------------------------------------------------------------------------
char* eos_all_generator(const char* text, int state)
{
  return eos_entry_generator(text, state);
}

//------------------------------------------------------------------------------
// Generator function for command completion - similar to the above.
//------------------------------------------------------------------------------
char* eos_command_generator(const char* text, int state)
{
  static size_t index;
  static std::vector<std::string> completions;

  // If this is a new word to complete, initialize now.  This includes
  // saving the length of TEXT for efficiency, and initializing the index
  // variable to 0.
  if (!state) {
    int len = strlen(text);
    completions.clear();
    index = 0;
    char* name;
    int i = 0;

    while ((name = commands[i].name)) {
      if (strncmp(name, text, len) == 0) {
        completions.push_back(name);
      }

      ++i;
    }
  } else {
    ++index;
  }

  if (index < completions.size()) {
    return strdup(completions[index].c_str());
  }

  return ((char*) 0);
}
