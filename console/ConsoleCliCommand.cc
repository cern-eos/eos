#include <string.h>
#include <assert.h>
#include <algorithm>
#include "ConsoleCliCommand.hh"

#define HELP_PADDING 30

std::vector<std::string> *
split_keywords (std::string keywords, char delimiter)
{
  std::vector<std::string> *split_keywords = new std::vector<std::string>;

  if (keywords.length() == 0)
    return split_keywords;

  size_t i = 0;
  std::string token("");

  while (i < keywords.length())
  {
    if (keywords[i] == delimiter)
    {
      if (token != "")
      {
        split_keywords->push_back(token);
        token = "";
      }
    }
    else
      token += keywords[i];

    i++;
  }
  if (token != "")
    split_keywords->push_back(token);

  return split_keywords;
}

ParseError::ParseError(CliBaseOption *option, std::string message)
  : m_option(option),
    m_message(message)
{}

CliBaseOption::CliBaseOption(std::string name, std::string desc)
  : m_name(name),
    m_description(desc),
    m_required(false),
    m_hidden(false)
{};

CliBaseOption::~CliBaseOption() {};

CliOption::CliOption(std::string name, std::string desc, std::string keywords)
  : CliBaseOption(name, desc),
    m_group(0)
{
  m_keywords = split_keywords(keywords, ',');
};

CliOption::CliOption(const CliOption &option)
  : CliOption(option.m_name, option.m_description, "")
{
  m_required = option.required();
  m_hidden = option.hidden();

  for (std::vector<std::string>::const_iterator it = option.m_keywords->cbegin();
       it != option.m_keywords->cend(); it++)
    m_keywords->push_back(*it);
}

CliOption::~CliOption()
{
  delete m_keywords;
  m_keywords = 0;
}

void
CliOption::set_group(OptionsGroup *group)
{
  if (m_group == group)
    return;

  if (m_group)
    m_group->remove_option(this);

  m_group = group;
}

std::string
CliOption::has_keyword(std::string keyword)
{
  std::vector<std::string>::const_iterator it = m_keywords->cbegin();
  for (; it != m_keywords->cend(); it++)
  {
    if (keyword.compare(*it) == 0)
      return *it;
  }

  return "";
}

AnalysisResult *
CliOption::analyse(std::vector<std::string> &cli_args)
{
  std::pair<std::string, std::vector<std::string>> ret;
  std::vector<std::string>::iterator it = cli_args.begin();

  AnalysisResult *res = new AnalysisResult;
  res->start = res->end = it;

  for (; it != cli_args.end(); it++)
  {
    if (has_keyword(*it) != "")
    {
      ret.first = m_name;
      res->start = it;
      res->end = it + 1;
      break;
    }
  }

  res->values = ret;
  res->error_msg = "";

  return res;
}

std::string
CliOption::join_keywords()
{
  std::string keyword("");

  for (size_t i = 0; i < m_keywords->size(); i++)
  {
    keyword += m_keywords->at(i);
    if (i < m_keywords->size() - 1)
      keyword += std::string("|");
  }

  return keyword;
}

char *
CliOption::keywords_repr()
{
  char *repr = NULL;
  std::string keyword = join_keywords();

  if (keyword != "")
  {
    if (!m_required)
      keyword = "[" + keyword + "]";
    repr = strdup(keyword.c_str());
  }

  return repr;
}

std::string
CliOption::repr()
{
  return m_keywords->at(0);
}

char *
CliOption::help_string()
{
  if (m_description == "")
    return NULL;

  char *help_str = 0;
  std::string keyword("");

  for (size_t i = 0; i < m_keywords->size(); i++)
    {
      keyword += m_keywords->at(i);
      if (i < m_keywords->size() - 1)
	keyword += std::string("|");
    }

  if (keyword != "")
  {
    int str_size = keyword.length() + m_description.length() + HELP_PADDING + 10;
    help_str = new char[str_size];
  }

  sprintf(help_str, "%*s\t\t- %s\n", HELP_PADDING, keyword.c_str(), m_description.c_str());

  return help_str;
}

CliOptionWithArgs::CliOptionWithArgs(std::string name,
                                     std::string keywords,
                                     std::string desc,
				     std::string joint_keywords,
				     int min_num_args,
				     int max_num_args)
  : CliOption::CliOption(name, keywords, desc)
{
  m_min_num_args = min_num_args;
  m_max_num_args = max_num_args;
  m_joint_keywords = split_keywords(joint_keywords, ',');
}

// std::string
// CliOptionWithArgs::has_keyword(std::string keyword)
// {
//   CliOption::has_keyword(keyword);

//   std::vector<std::string>::iterator it = m_joint_keywords->begin();
//   for (; it != m_joint_keywords->end(); it++)
//   {
//     std::string current = *it;
//     if (keyword.compare(0, current.length(), current) == 0)
//       return "true";
//   }

//   return "";
// }

AnalysisResult *
CliOptionWithArgs::analyse(std::vector<std::string> &cli_args)
{
  std::pair<std::string, std::vector<std::string>> ret;
  std::vector<std::string>::iterator it = cli_args.begin();
  AnalysisResult *res = new AnalysisResult;
  res->error_msg = "";

  for (; it != cli_args.end(); it++)
  {
    std::string keyword = *it;
    std::string matched_kw = has_keyword(keyword);

    if (matched_kw == "")
      continue;

    ret.first = m_name;
    res->start = it;
    // If the max number of args is -1, we take it till the 
    int n_args = m_max_num_args;
    it++;
    while (n_args != 0 && it != cli_args.end())
    {
      ret.second.push_back(*it);

      if (n_args != -1)
	n_args--;

      it++;
    }

    res->end = it;
    break;
  }

  res->values = ret;

  return res;
}

CliPositionalOption::CliPositionalOption(std::string name, std::string desc,
                                         int position, int num_args, std::string repr)
  : CliBaseOption(name, desc),
    m_position(position),
    m_num_args(num_args),
    m_repr(repr)
{
  assert(m_position > 0 || m_position == -1);
}

CliPositionalOption::CliPositionalOption(std::string name, std::string desc,
                                         int position, int num_args,
                                         std::string repr, bool required)
  : CliPositionalOption(name, desc, position, 1, repr)
{
  m_required = required;
}

CliPositionalOption::CliPositionalOption(std::string name, std::string desc,
                                         int position, std::string repr)
  : CliPositionalOption(name, desc, position, 1, repr)
{}

CliPositionalOption::CliPositionalOption(const CliPositionalOption &option)
  : CliPositionalOption(option.name(), option.description(),
                        option.m_position, option.m_num_args, option.m_repr,
                        option.m_required)
{}

CliPositionalOption::~CliPositionalOption() {};

char *
CliPositionalOption::help_string()
{
  if (m_description == "")
    return NULL;

  char *help_str;
  std::string repr = m_repr;
  int str_length = m_description.length() + repr.length() + HELP_PADDING + 10;
  help_str = new char[str_length];

  sprintf(help_str, "%*s\t\t- %s\n", HELP_PADDING, repr.c_str(), m_description.c_str());

  return help_str;
}

AnalysisResult *
CliPositionalOption::analyse(std::vector<std::string> &cli_args)
{
  AnalysisResult *res;

  if (cli_args.size() == 0 || m_position > (int) cli_args.size())
  {
    if (!m_required)
      return NULL;

    res = new AnalysisResult;
    res->values.first = m_name;
    res->error_msg = "Error: Please specify " + repr() + ".";

    return res;
  }

  res = new AnalysisResult;
  int init_pos = m_position - 1;

  res->values.first = m_name;
  res->start = cli_args.begin() + init_pos;
  res->error_msg = "";

  int num_args = m_num_args;
  if (m_num_args == -1)
    num_args = cli_args.size() - init_pos;

  int i;
  for (i = init_pos; i < init_pos + num_args && i < (int) cli_args.size(); i++)
    res->values.second.push_back(cli_args.at(i));

  res->end = res->start + i;

  if (m_num_args != -1 && i < init_pos + num_args)
    res->error_msg = "Error: Too few arguments for " + m_repr + ".";

  return res;
}

std::string
CliPositionalOption::repr()
{
  if (!m_required)
    return "[" + m_repr + "]";

  return m_repr;
}

ConsoleCliCommand::ConsoleCliCommand(const std::string &name,
                                     const std::string &description)
  : m_name(name),
    m_description(description),
    m_subcommands(0),
    m_main_group(0),
    m_positional_options(0),
    m_parent_command(0),
    m_errors(0),
    m_groups(0)
{}

ConsoleCliCommand::~ConsoleCliCommand()
{
  size_t i;

  delete m_main_group;
  m_main_group = 0;

  if (m_groups)
  {
    for(i = 0; i < m_groups->size(); i++)
    {
      delete m_groups->at(i);
    }

    delete m_groups;
    m_groups = 0;
  }

  if (m_positional_options)
  {
    std::map<int, CliPositionalOption *>::const_iterator it;
    for (it = m_positional_options->cbegin(); it != m_positional_options->cend(); it++)
      delete (*it).second;

    delete m_positional_options;
    m_positional_options = 0;
  }

  if (m_subcommands)
  {
    for(i = 0; i < m_subcommands->size(); i++)
      {
        delete m_subcommands->at(i);
      }
    delete m_subcommands;
    m_subcommands = 0;
  }
}

void
ConsoleCliCommand::add_option(CliOption *option)
{
  if (m_main_group == 0)
    m_main_group = new OptionsGroup;

  m_main_group->add_option(option);
}

void
ConsoleCliCommand::add_option(CliPositionalOption *option)
{
  if (m_positional_options == 0)
    m_positional_options = new std::map<int, CliPositionalOption *>;

  int pos = option->position();
  if (m_positional_options->count(pos) != 0)
  {
    delete m_positional_options->at(pos);
    m_positional_options->erase(pos);
  }
  m_positional_options->insert({pos, option});
}

void
ConsoleCliCommand::add_subcommand(ConsoleCliCommand *subcommand)
{
  assert(subcommand != this);

  if (m_subcommands == 0)
    m_subcommands = new std::vector<ConsoleCliCommand *>;

  if (std::find(m_subcommands->begin(), m_subcommands->end(), subcommand) == m_subcommands->end())
    m_subcommands->push_back(subcommand);
  subcommand->set_parent(this);
}

void
ConsoleCliCommand::add_option(const CliOption &option)
{
  CliOption *new_obj = new CliOption(option);
  add_option(new_obj);
}

void
ConsoleCliCommand::add_option(const CliPositionalOption &option)
{
  CliPositionalOption *new_obj = new CliPositionalOption(option);
  add_option(new_obj);
}

void
ConsoleCliCommand::add_options(std::vector<CliOption> options)
{
  std::vector<CliOption>::const_iterator it = options.cbegin();
  for (; it != options.cend(); it++)
  {
    add_option(*it);
  }
}

void
ConsoleCliCommand::add_options(std::vector<CliPositionalOption> options)
{
  std::vector<CliPositionalOption>::const_iterator it = options.cbegin();
  for (; it != options.cend(); it++)
  {
    add_option(*it);
  }
}

OptionsGroup*
ConsoleCliCommand::add_grouped_options(std::vector<CliOption> options)
{
  if (options.size() == 0)
    return 0;

  OptionsGroup *group = new OptionsGroup;
  add_group(group);

  group->add_options(options);

  return group;
}

void
ConsoleCliCommand::add_group(OptionsGroup *group)
{
  if (!m_groups)
    m_groups = new std::vector<OptionsGroup *>;

  if (std::find(m_groups->begin(), m_groups->end(), group) == m_groups->end())
    m_groups->push_back(group);
}

void
ConsoleCliCommand::add_error(const ParseError *error)
{
  if (!m_errors)
    m_errors = new std::vector<const ParseError *>;

  m_errors->push_back(error);
}

bool
ConsoleCliCommand::has_errors()
{
  return m_errors && !m_errors->empty();
}

ConsoleCliCommand *
ConsoleCliCommand::is_subcommand(std::vector<std::string> &cli_args)
{
  if (cli_args.size() == 0)
    return 0;

  std::vector<ConsoleCliCommand *>::const_iterator it = m_subcommands->cbegin();
  for (; it != m_subcommands->cend(); it++)
  {
    if ((*it)->name().compare(cli_args[0]) == 0)
      return *it;
  }

  return 0;
}

void
ConsoleCliCommand::analyse_group(OptionsGroup *group, std::vector<std::string> &cli_args)
{
  std::vector<CliOption *>* options = group->options();
  if (options)
  {
    bool option_found = false;
    std::vector<CliOption *>::iterator it = options->begin();
    for (; it != options->end(); it++)
      {
        AnalysisResult *res = (*it)->analyse(cli_args);
        if (res->values.first != "")
        {
          if (option_found && group != m_main_group)
          {
            add_error(new ParseError(0, "Error: Use only one option: " + group->options_repr()));
            delete res;
            return;
          }

          m_options_map.insert(res->values);
          cli_args.erase(res->start, res->end);
          option_found = true;
        }
        delete res;
      }
  }
}

ConsoleCliCommand *
ConsoleCliCommand::parse(std::vector<std::string> &cli_args)
{
  if (m_subcommands)
  {
    ConsoleCliCommand *subcommand = is_subcommand(cli_args);

    if (subcommand)
    {
      std::vector<std::string> subcommand_args(cli_args);
      subcommand_args.erase(subcommand_args.begin());

      return subcommand->parse(subcommand_args);
    }
  }

  if (m_main_group)
    analyse_group(m_main_group, cli_args);

  if (m_groups)
  {
    std::vector<OptionsGroup *>::const_iterator it;
    for (it = m_groups->cbegin(); it != m_groups->cend(); it++)
      analyse_group(*it, cli_args);
  }

  int num_args_processed = (int) cli_args.size();
  if (m_positional_options)
  {
    std::map<int, CliPositionalOption *>::iterator pos_it = m_positional_options->begin();
    for (; pos_it != m_positional_options->end(); pos_it++)
      {
        AnalysisResult *res = (*pos_it).second->analyse(cli_args);

        if (!res) // not required and not found
          continue;

        if (res->error_msg == "")
          m_options_map.insert(res->values);
        else
          add_error(new ParseError((*pos_it).second, res->error_msg));

        num_args_processed -= (res->end - res->start);

        delete res;
      }
  }

  if (num_args_processed > 0)
    add_error(new ParseError(0, "Error: Wrong number of arguments."));

  return this;
}

ConsoleCliCommand *
ConsoleCliCommand::parse(std::string &cli_args)
{
  std::vector<std::string>* cli_args_vector = split_keywords(cli_args, ' ');
  return parse(*cli_args_vector);
}

ConsoleCliCommand *
ConsoleCliCommand::parse(std::string cli_args)
{
  std::vector<std::string>* cli_args_vector = split_keywords(cli_args, ' ');
  return parse(*cli_args_vector);
}

bool
ConsoleCliCommand::has_value(std::string option_name)
{
  return m_options_map.count(option_name);
}

bool
ConsoleCliCommand::has_values()
{
  return !m_options_map.empty();
}

std::vector<std::string>
ConsoleCliCommand::get_value (std::string option_name)
{
  return m_options_map[option_name];
}

void
ConsoleCliCommand::print_help_for_options(std::vector<CliOption *>* options)
{
  std::vector<CliOption *>::const_iterator it;
  for (it = options->cbegin(); it != options->cend(); it++)
  {
    char *str = (*it)->help_string();

    if (str != NULL)
    {
      fprintf(stdout, str);
      free(str);
    }
  }
}

void
ConsoleCliCommand::print_help()
{
  if (m_main_group)
    print_help_for_options(m_main_group->options());

  if (m_groups)
  {
    for (size_t i = 0; i < m_groups->size(); i++)
      print_help_for_options(m_groups->at(i)->options());
  }

  if (m_positional_options)
  {
    std::map<int, CliPositionalOption *>::const_iterator it;
    for (it = m_positional_options->cbegin(); it != m_positional_options->cend(); it++)
    {
      char *str = (*it).second->help_string();

      if (str != NULL)
      {
        fprintf(stdout, str);
        free(str);
      }
    }
  }
}

void
ConsoleCliCommand::print_usage()
{
  std::string subcomm_repr = subcommands_repr();
  std::string kw_repr = keywords_repr();
  std::string pos_options_repr = positional_options_repr();
  std::string command_and_options = m_name;

  if (subcomm_repr != "")
    command_and_options += " " + subcomm_repr;

  if (kw_repr != "")
    command_and_options += " " + kw_repr;

  if (m_groups)
  {
    std::vector<OptionsGroup *>::const_iterator it;
    for (it = m_groups->cbegin(); it != m_groups->cend(); it++)
    {
      std::string group_repr((*it)->name());

      if (group_repr == "")
        group_repr = (*it)->options_repr();

      command_and_options += " " + ((*it)->required() ? group_repr : "[" + group_repr + "]");
    }
  }

  if (pos_options_repr != "")
    command_and_options += " " + pos_options_repr;

  if (m_parent_command)
    command_and_options = m_parent_command->name() + " " + command_and_options;

  fprintf(stdout, "Usage: %s", command_and_options.c_str());
  if (m_description != "") {
    fprintf(stdout, " : %s\n", m_description.c_str());
  }
  print_help();
}

void
ConsoleCliCommand::print_errors()
{
  if (!m_errors)
    return;

  std::string errors_str("");
  std::vector<const ParseError*>::const_iterator it;
  for (it = m_errors->cbegin(); it != m_errors->cend(); it++)
  {
    errors_str += (*it)->message() + "\n";
  }
  fprintf(stdout, errors_str.c_str());
}

void
ConsoleCliCommand::set_parent(const ConsoleCliCommand *parent)
{
  m_parent_command = parent;
}

std::string
ConsoleCliCommand::keywords_repr()
{
  std::vector<CliOption *>* options;
  std::string repr("");

  if (!m_main_group)
    return repr;

  options = m_main_group->options();

  if (!options)
    return repr;

  for (size_t i = 0; i < options->size(); i++)
  {
    if (options->at(i)->hidden())
      continue;

    std::string option_repr = options->at(i)->repr();

    if (!options->at(i)->required())
      option_repr = "[" + option_repr + "]";

    repr += option_repr;

    if (i < options->size() - 2)
      repr += " ";
  }

  return repr;
}

std::string
ConsoleCliCommand::subcommands_repr()
{
  std::string repr("");

  if (!m_subcommands)
    return repr;

  for (size_t i = 0; i < m_subcommands->size(); i++)
  {
    repr += m_subcommands->at(i)->name();
    if (i < m_subcommands->size() - 1)
      repr += "|";
  }

  return repr;
}

std::string
ConsoleCliCommand::positional_options_repr()
{
  std::string repr("");

  if (!m_positional_options)
    return repr;

  std::map<int, CliPositionalOption *>::const_iterator it;
  for (it = m_positional_options->cbegin(); it != m_positional_options->cend(); it++)
  {
    repr += (*it).second->repr();

    if (it == --m_positional_options->cend())
      break;

    repr += " ";
  }

  return repr;
}

OptionsGroup::OptionsGroup(std::string name)
  : m_name(name),
    m_options(0),
    m_required(false)
{}

OptionsGroup::OptionsGroup()
  : OptionsGroup("")
{}

OptionsGroup::~OptionsGroup()
{
  if (m_options)
  {
    size_t i;
    for(i = 0; i < m_options->size(); i++)
    {
      delete m_options->at(i);
    }
    delete m_options;
    m_options = 0;
  }
}

void
OptionsGroup::add_option(CliOption *option)
{
  if (!m_options)
    m_options = new std::vector<CliOption *>;

  if (option->group() != this)
  {
    option->set_group(this);
    m_options->push_back(option);
  }
}

void
OptionsGroup::add_option(CliOption option)
{
  CliOption *option_ptr = new CliOption(option);
  add_option(option_ptr);
}

void
OptionsGroup::add_options(std::vector<CliOption> options)
{
  std::vector<CliOption>::const_iterator it = options.cbegin();
  for (; it != options.cend(); it++)
  {
    add_option(*it);
  }
}


void
OptionsGroup::remove_option(CliOption *option)
{
  option->set_group(0);

  if (!m_options)
    return;

  std::vector<CliOption *>::iterator it;
  for (it = m_options->begin(); it != m_options->end(); it++)
  {
    if (*it == option)
    {
      m_options->erase(it);
      return;
    }
  }
}

std::string
OptionsGroup::options_repr()
{
  std::string repr = "";

  if (!m_options)
    return repr;

  size_t i;
  std::vector<const CliOption *>::iterator it;
  for (i = 0; i < m_options->size(); i++)
  {
    repr += m_options->at(i)->repr();

    if (i != m_options->size() - 1)
      repr += "|";
  }

  return repr;
}
