//------------------------------------------------------------------------------
//! @file ConsoleCompletion.hh
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

#pragma once

//------------------------------------------------------------------------------
//! EOS console custom completion function to be used by the readline library
//! to provide autocompletion.
//!
//! @param text text to be completed
//! @param start start index of text in the rl_line_buffer
//! @param end end index of text in the rl_line_buffer
//!
//! @return NULL if no completion available, otherwise an array of strings to
//!         be used
//------------------------------------------------------------------------------
char** eos_console_completion(const char* text, int start, int end);

//------------------------------------------------------------------------------
//! EOS entry (files/directories) generator
//!
//! @param text partial text to be completed
//! @param state 0 the first time the function is called allowing the generator
//!        to perform initialization and positive non-zeor integer for each
//!        subsequent call.
//! @param only_files if true then return only file entries, otherwise return
//!        both files and directories
//!
//! @return NULL when there are no more completion possibilites left, otherwise
//!        return a string that must be allocated with malloc() since readline
//!        will free the strings when it has finised with them.
//------------------------------------------------------------------------------
char* eos_entry_generator(const char* text, int state,
                          bool only_files = false);

//------------------------------------------------------------------------------
//! EOS directories generator - similar to the above
//------------------------------------------------------------------------------
char* eos_dir_generator(const char* text, int state);

//------------------------------------------------------------------------------
//! EOS files and directories generator - similar to the above
//------------------------------------------------------------------------------
char* eos_all_generator(const char* text, int state);

//------------------------------------------------------------------------------
//! Generator function for command completion - similar to the above
//------------------------------------------------------------------------------
char* eos_command_generator(const char* text, int state);

//------------------------------------------------------------------------------
//! Helper function to extact the dirname and base name from a absolute or
//! relative path. For example:
//! "/a/b/c/d"  -> dirname: "/a/b/c/"   and basename: "d"
//! "/a/b/c/d/" -> dirname: "/a/b/c/d/" and basename: ""
//! "x/y/z"     -> dirname: "x/y/"      and basename: "z"
//! "x/y/z/"    -> dirname: "x/y/z/"    and basename: ""
//! ""          -> dirname: ""          and basename: ""
//! "x"         -> dirname: ""          and basename: "x"
//!
//! @param input input path to process
//! @param dirname dirname component
//! @param basename basename component
//------------------------------------------------------------------------------
void eos_path_split(const std::string& input, std::string& dirname,
                    std::string& basename);

