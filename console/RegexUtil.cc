#include "RegexUtil.hh"

RegexUtil::RegexUtil()
  : m_tokenize(false),  m_origin("")
{}

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
