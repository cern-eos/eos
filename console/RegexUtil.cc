//------------------------------------------------------------------------------
//! @file RegexUtil.hh
//! @author Stefan Isidorovic <stefan.isidorovic@comtrade.com>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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

#include "RegexUtil.hh"

RegexUtil::RegexUtil()
  : m_regex(), m_tokenize(false),  m_regex_flags(0), m_origin("")
{
  for (unsigned i = 0; i < max_num_of_matches; ++i) {
    m_matches[i].rm_so = 0;
    m_matches[i].rm_eo = 0;
  }
}

void RegexUtil::SetRegex(std::string regex_txt, int flags)
{
  int status = regcomp(&(m_regex), regex_txt.c_str(),
                       flags ? flags : REG_EXTENDED | REG_NEWLINE);

  if (status != 0) {
    char error_message[256];
    regerror(status, &(m_regex), error_message, 256);
    throw std::string(error_message);
  }
}

void RegexUtil::initTokenizerMode()
{
  if (m_origin.empty()) {
    throw std::string("No origin set!");
  }

  int nomatch = regexec(&(m_regex), m_origin.c_str(),
                        max_num_of_matches, m_matches, 0);

  if (nomatch) {
    throw std::string("Nothing matches.");
  }

  m_tokenize = true;
}

std::string RegexUtil::Match()
{
  if (!m_tokenize) {
    throw std::string("RegexUtil: Tokenizer mode doesn't initialized!");
  }

  return std::string(
           m_origin.begin() + m_matches[0].rm_so,
           m_origin.begin() + m_matches[0].rm_eo);
}
