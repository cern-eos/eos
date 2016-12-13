#include "RegexUtil.hh"

RegexUtil::RegexUtil()
  : m_tokenize(false),  m_origin(nullptr)
{}

void RegexUtil::SetRegex(std::string regex_txt, int flags)
{
  int status = regcomp(&(this->m_regex),regex_txt.c_str(),
		       flags ? flags : REG_EXTENDED | REG_NEWLINE);

  if (status != 0) {
    char error_message[256];
    regerror(status, &(this->m_regex), error_message, 256);
    throw std::string(error_message);
  }
}

void RegexUtil::initTokenizerMode()
{
  if (this->m_origin ==  nullptr) {
    throw std::string("No origin set!");
  }

  int nomatch = regexec(&(this->m_regex), this->m_origin->c_str(),
			this->max_num_of_matches, this->m_matches, 0);

  if (nomatch) {
    throw std::string("Nothing matches.");
  }

  this->m_tokenize = true;
}

std::string RegexUtil::Match()
{
  if (!this->m_tokenize) {
    throw std::string("RegexUtil: Tokenizer mode doesn't initialized!");
  }

  return std::string(
	   this->m_origin->begin() + this->m_matches[0].rm_so,
	   this->m_origin->begin() + this->m_matches[0].rm_eo);
}
