// ----------------------------------------------------------------------
// File: ConsoleCliCommand.cc
// Author: Joaquim Rocha - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2013 CERN/Switzerland                                  *
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

#include <string.h>
#include <assert.h>
#include <sstream>
#include <algorithm>
#include <climits>
#include "ConsoleCliCommand.hh"
#include "common/StringTokenizer.hh"

#define HELP_PADDING 50
#define DESC_LINE_LENGTH 70

bool isFloatEvalFunc (const CliOptionWithArgs *option,
                      std::vector<std::string> &args,
                      std::string **error,
                      void *userData)
{
  for (size_t i = 0; i < args.size(); i++)
  {
    for (size_t s = 0; s < args[i].length(); s++)
    {
      istringstream ss(args[i]);
      float number;
      if (!(ss >> number))
      {
        if (error)
          *error = new std::string("Error: Option " + option->repr() + " needs a float.");
        return false;
      }
    }
  }
  return true;
}

bool isIntegerEvalFunc (const CliOptionWithArgs *option,
                        std::vector<std::string> &args,
                        std::string **error,
                        void *userData)
{
  for (size_t i = 0; i < args.size(); i++)
  {
    size_t s = 0;

    if (args[i][0] == '-')
      s++;

    for (; s < args[i].length(); s++)
    {
      if (!std::isdigit(args[i][s]))
      {
        if (error)
          *error = new std::string("Error: Option " + option->repr() + " needs an integer.");
        return false;
      }
    }
  }
  return true;
}

bool isNumberInRangeEvalFunc (const CliOptionWithArgs *option,
                              std::vector<std::string> &args,
                              std::string **error,
                              const std::pair<float, float> *range)
{
  for (size_t i = 0; i < args.size(); i++)
  {
    for (size_t s = 0; s < args[i].length(); s++)
    {
      istringstream ss(args[i]);
      float number;
      if (!(ss >> number) || (range->first > number || number > range->second))
      {
        ostringstream limit;
        if (error)
        {
          *error = new std::string("Error: Option " + option->repr() + " needs to be between ");
          limit << range->first << " and " << range->second;
          (*error)->append(limit.str());
        }
        return false;
      }
    }
  }
  return true;
}

bool isPositiveNumberEvalFunc(const CliOptionWithArgs *option,
                              std::vector<std::string> &args,
                              std::string **error,
                              void *data)
{
  std::pair<float, float> groupSizeRange = {0.0, (float) INT_MAX};
  if (!isNumberInRangeEvalFunc (option, args, 0, &groupSizeRange))
  {
    *error = new std::string("Error: Option " + option->repr() + " needs to be "
                             "a positive number.");
    return false;
  }

  return true;
}

bool isNegativeNumberEvalFunc(const CliOptionWithArgs *option,
                              std::vector<std::string> &args,
                              std::string **error,
                              void *data)
{
  std::pair<float, float> groupSizeRange = {(float) -INT_MAX, 0};
  if (!isNumberInRangeEvalFunc (option, args, 0, &groupSizeRange))
  {
    *error = new std::string("Error: Option " + option->repr() + " needs to be "
                             "a negative number.");
    return false;
  }

  return true;
}

bool isChoiceEvalFunc (const CliOptionWithArgs *option,
                       std::vector<std::string> &args,
                       std::string **error,
                       const std::vector<std::string> *choices)
{
  assert(choices->size() != 0);

  for (size_t i = 0; i < args.size(); i++)
  {
    if (std::find(choices->begin(), choices->end(), args[i]) == choices->end())
    {
      if (error)
      {
        *error = new std::string("Error: Option " + option->repr() + " needs to be");
        (*error)->append(" " + choices->at(0));
        for (size_t c = 1; c < choices->size(); c++)
          (*error)->append((c == choices->size() - 1 ? " or " : ", ") + choices->at(c));
      }
      return false;
    }
  }
  return true;
}

std::vector<std::string> *
splitKeywords (std::string keywords, char delimiter)
{
  std::vector<std::string> *splitKeywords = new std::vector<std::string>;
  eos::common::StringTokenizer tokenizer(keywords, delimiter);
  tokenizer.GetLine();
  const char *token = tokenizer.GetToken();

  while(token)
  {
    splitKeywords->push_back(token);
    token = tokenizer.GetToken();
  }

  return splitKeywords;
}

ParseError::ParseError(CliBaseOption *option, std::string message)
  : mOption(option),
    mMessage(message)
{}

CliBaseOption::CliBaseOption(std::string name, std::string desc)
  : mName(name),
    mDescription(desc),
    mRequired(false),
    mHidden(false)
{};

CliBaseOption::~CliBaseOption() {};

CliOption::CliOption(std::string name, std::string desc, std::string keywords)
  : CliBaseOption(name, desc),
    mGroup(0)
{
  mKeywords = splitKeywords(keywords, ',');
};

CliOption::CliOption(const CliOption &option)
  : CliOption(option.mName, option.mDescription, "")
{
  mRequired = option.required();
  mHidden = option.hidden();

  if (option.mKeywords)
  {
    mKeywords = new std::vector<std::string>;
    std::vector<std::string>::const_iterator it;

    for (it = option.mKeywords->cbegin(); it != option.mKeywords->cend(); it++)
      mKeywords->push_back(*it);
  }
}

CliOption::~CliOption()
{
  delete mKeywords;
  mKeywords = 0;
}

void
CliOption::setGroup(OptionsGroup *group)
{
  if (mGroup == group)
    return;

  if (mGroup)
    mGroup->removeOption(this);

  mGroup = group;
}

std::string
CliOption::hasKeyword(std::string keyword)
{
  if (!mKeywords)
    return "";

  std::vector<std::string>::const_iterator it = mKeywords->cbegin();
  for (; it != mKeywords->cend(); it++)
  {
    if (keyword.compare(*it) == 0)
      return *it;
  }

  return "";
}

AnalysisResult *
CliOption::analyse(std::vector<std::string> &cliArgs)
{
  std::pair<std::string, std::vector<std::string>> ret;
  std::vector<std::string>::iterator it = cliArgs.begin();

  AnalysisResult *res = new AnalysisResult;
  res->start = res->end = it;

  for (; it != cliArgs.end(); it++)
  {
    if (hasKeyword(*it) != "")
    {
      ret.first = mName;
      res->start = it;
      res->end = it + 1;
      break;
    }
  }

  res->values = ret;
  res->errorMsg = "";

  return res;
}

std::string
CliOption::joinKeywords()
{
  std::string keyword("");

  if (!mKeywords)
    return "";

  for (size_t i = 0; i < mKeywords->size(); i++)
  {
    keyword += mKeywords->at(i);
    if (i < mKeywords->size() - 1)
      keyword += std::string("|");
  }

  return keyword;
}

char *
CliOption::keywordsRepr()
{
  char *repr = NULL;
  std::string keyword = joinKeywords();

  if (keyword != "")
  {
    if (!mRequired)
      keyword = "[" + keyword + "]";
    repr = strdup(keyword.c_str());
  }

  return repr;
}

std::string
CliOption::repr() const
{
  return mKeywords ? mKeywords->at(0) : "";
}

static std::string
truncateDescString(const std::string &description, const std::string &prefix)
{
  std::string desc("");
  size_t lineStart = 0, lineEnd = 0;

  for (size_t i = 0; i < description.length(); i++)
  {
    if (i == description.length() - 1)
        lineEnd = i + 1;

    if (lineEnd != lineStart &&
        (i - lineStart > DESC_LINE_LENGTH || i == description.length() - 1 ||
         description[lineEnd] == '\n'))
    {
      if (lineEnd > DESC_LINE_LENGTH || desc.back() == '\n')
        desc += "\n" + std::string(HELP_PADDING, ' ') + prefix;

      desc += std::string(description, lineStart, lineEnd - lineStart);

      lineStart = lineEnd + 1;
      lineEnd = lineStart;

      continue;
    }

    if (description[i] == ' ' || description[i] == '\n')
      lineEnd = i;
  }

  desc += "\n";

  return desc;
}

char *
CliOption::helpString()
{
  if (mDescription == "" || !mKeywords)
    return NULL;

  char *helpStr = 0;
  std::string keyword("");

  for (size_t i = 0; i < mKeywords->size(); i++)
    {
      keyword += mKeywords->at(i);
      if (i < mKeywords->size() - 1)
	keyword += std::string("|");
    }

  if (keyword != "")
  {
    int strSize = keyword.length() + HELP_PADDING + 10;
    helpStr = new char[strSize];
  }

  sprintf(helpStr, "%*s\t- ", HELP_PADDING, keyword.c_str());

  std::string help(helpStr);
  help += truncateDescString(mDescription, "\t  ");

  delete[] helpStr;

  return strdup(help.c_str());
}

CliOptionWithArgs::CliOptionWithArgs(std::string name,
                                     std::string desc,
                                     std::string keywords,
                                     int numArgs,
                                     std::string repr,
                                     bool required)
  : CliOption::CliOption(name, desc, keywords),
    mRepr(repr),
    mNumArgs(numArgs),
    mEvalFunctions(0),
    mUserData(0)
{
  mRequired = required;
}

CliOptionWithArgs::CliOptionWithArgs(std::string name,
                                     std::string desc,
                                     std::string keywords,
                                     std::string repr,
                                     bool required)
  : CliOptionWithArgs(name, desc, keywords, 1, repr, required)
{}

CliOptionWithArgs::~CliOptionWithArgs()
{
  if (mEvalFunctions)
    delete mEvalFunctions;
  mEvalFunctions = 0;

  if (mUserData)
    delete mUserData;
  mUserData = 0;
}

char *
CliOptionWithArgs::helpString()
{
  if (mDescription == "" || !mKeywords)
    return NULL;

  char *helpStr = 0;
  std::string keyword("");
  ostringstream helpRepr(mRepr);

  if (helpRepr == "")
  {
    if (mNumArgs == -1)
      helpRepr << "<value1> <value2> ...";
    else
    {
      for (int i = 1; i <= mNumArgs; i++)
      {
        helpRepr << "<value" << i << ">" << (i == mNumArgs ? "" : " ");
      }
      helpRepr << "...";
    }
  }

  for (size_t i = 0; i < mKeywords->size(); i++)
    {
      keyword += mKeywords->at(i);

      if (keyword.back() == '=')
        keyword += helpRepr.str();
      else
        keyword += " " + helpRepr.str();

      if (i < mKeywords->size() - 1)
	keyword += std::string("|");
    }

  if (keyword != "")
  {
    int strSize = keyword.length() + HELP_PADDING + 10;
    helpStr = new char[strSize];
  }

  sprintf(helpStr, "%*s\t- ", HELP_PADDING, keyword.c_str());

  std::string help(helpStr);
  help += truncateDescString(mDescription, "\t  ");

  delete[] helpStr;

  return strdup(help.c_str());
}

void
CliOptionWithArgs::addEvalFunction(evalFuncCb func, void *userData)
{
  if (!mEvalFunctions)
  {
    mEvalFunctions = new std::vector<evalFuncCb>;
    mUserData = new std::vector<void *>;
  }

  mEvalFunctions->push_back(func);
  mUserData->push_back(userData);
}

std::string
CliOptionWithArgs::hasKeyword(std::string keyword)
{
  if (!mKeywords)
    return "";

  std::vector<std::string>::const_iterator it = mKeywords->cbegin();
  for (; it != mKeywords->cend(); it++)
  {
    if (keyword.compare(*it) == 0)
      return *it;

    const std::string& kw = *it;
    // If the current keyword ends up in an '=' we check
    // if its a prefix of the given arguemtn
    if (kw.back() == '=' &&
        keyword.compare(0, kw.length(), kw) == 0)
    {
      return *it;
    }
  }

  return "";
}

std::string
CliOptionWithArgs::repr() const
{
  std::string reprStr = mRepr;

  if (reprStr != "" && mKeywords)
  {
    const std::string &firstKw = mKeywords->front();
    reprStr = firstKw + (firstKw.back() == '=' ? "" : " ") + reprStr;
  }

  return reprStr;
}

AnalysisResult *
CliOptionWithArgs::commonAnalysis(std::vector<std::string> &cliArgs,
                                  int initPos, const std::string &firstArg)
{
  AnalysisResult *res;
  std::vector<std::string> optionArgs;
  int numArgs = mNumArgs == -1 ? max((int) cliArgs.size(), 1) : mNumArgs;

  if (firstArg != "")
  {
    optionArgs.insert(optionArgs.begin(), firstArg);
    numArgs--;
  }

  if (initPos + numArgs <= (int) cliArgs.size())
    optionArgs.insert(optionArgs.end(), cliArgs.cbegin() + initPos,
                      cliArgs.cbegin() + initPos + numArgs);

  if (numArgs > (int) optionArgs.size())
  {
    if (!mRequired)
      return NULL;

    res = new AnalysisResult;
    res->values.first = mName;
    res->start = cliArgs.begin() + initPos;

    if (optionArgs.size() == 0)
    {
      res->end = res->start;
      res->errorMsg = "Error: Please specify " + repr() + ".";
    }
    else
    {
      res->end = res->start + min((int) optionArgs.size(), numArgs);
      res->errorMsg = "Error: Too few arguments for " + repr() + ".";
    }

    return res;
  }

  res = new AnalysisResult;

  res->values.first = mName;
  res->start = cliArgs.begin() + initPos;
  res->errorMsg = "";

  std::string *evalErrorMsg = 0;

  if (shouldEvaluate())
  {
    for (size_t f = 0; f < mEvalFunctions->size(); f++)
    {
      if (!mEvalFunctions->at(f)(this, optionArgs, &evalErrorMsg, mUserData->at(f)))
      {
        goto bailout;
      }
    }
  }

  int i;
  for (i = 0; i < (int) optionArgs.size(); i++)
    res->values.second.push_back(optionArgs.at(i));


 bailout:
  res->end = res->start + numArgs;

  if (evalErrorMsg)
    res->errorMsg = std::string(evalErrorMsg->c_str());

  delete evalErrorMsg;

  return res;
}

AnalysisResult *
CliOptionWithArgs::analyse(std::vector<std::string> &cliArgs)
{
  AnalysisResult *res;
  size_t initPos;
  std::string firstArg("");

  for (initPos = 0; initPos < cliArgs.size(); initPos++)
  {
    const std::string &foundKw = hasKeyword(cliArgs[initPos]);
    if (foundKw != "")
    {
      if (foundKw[foundKw.length() - 1] == '=')
        firstArg = cliArgs[initPos].substr(foundKw.length());

      break;
    }
  }

  initPos++;

  res = commonAnalysis(cliArgs, initPos, firstArg);

  // we decrease the start of our options because we need to include
  // the keyword used.
  if (res && initPos <=  cliArgs.size())
    res->start = res->start - 1;

  return res;
}

CliPositionalOption::CliPositionalOption(std::string name, std::string desc,
                                         int position, int numArgs, std::string repr)
  : CliPositionalOption(name, desc, position, numArgs, repr, false)
{}

CliPositionalOption::CliPositionalOption(std::string name, std::string desc,
                                         int position, int numArgs,
                                         std::string repr, bool required)
  : CliOptionWithArgs(name, desc, "", numArgs, repr, required),
    mPosition(position)
{
  assert(mPosition > 0 || mPosition == -1);
}

CliPositionalOption::CliPositionalOption(std::string name, std::string desc,
                                         int position, std::string repr)
  : CliPositionalOption(name, desc, position, 1, repr)
{}

CliPositionalOption::CliPositionalOption(const CliPositionalOption &option)
  : CliPositionalOption(option.name(), option.description(),
                        option.mPosition, option.mNumArgs, option.mRepr,
                        option.mRequired)
{
  if (option.mEvalFunctions)
    mEvalFunctions = new std::vector<evalFuncCb>(*option.mEvalFunctions);
  if (option.mUserData)
    mUserData = new std::vector<void *>(*option.mUserData);
}

CliPositionalOption::~CliPositionalOption() {};

char *
CliPositionalOption::helpString()
{
  if (mDescription == "")
    return NULL;

  char *helpStr;
  std::string repr = mRepr;
  int strLength = repr.length() + HELP_PADDING + 10;
  helpStr = new char[strLength];

  sprintf(helpStr, "%*s\t- ", HELP_PADDING, repr.c_str());

  std::string help(helpStr);
  help += truncateDescString(mDescription, "\t  ");

  delete[] helpStr;

  return strdup(help.c_str());
}

AnalysisResult *
CliPositionalOption::analyse(std::vector<std::string> &cliArgs)
{
  return commonAnalysis(cliArgs, mPosition - 1, "");
}

std::string
CliPositionalOption::repr() const
{
  return mRepr;
}

ConsoleCliCommand::ConsoleCliCommand(const std::string &name,
                                     const std::string &description)
  : mName(name),
    mDescription(description),
    mSubcommands(0),
    mMainGroup(0),
    mPositionalOptions(0),
    mParentCommand(0),
    mErrors(0),
    mGroups(0)
{}

ConsoleCliCommand::~ConsoleCliCommand()
{
  size_t i;

  delete mMainGroup;
  mMainGroup = 0;

  if (mGroups)
  {
    for(i = 0; i < mGroups->size(); i++)
    {
      delete mGroups->at(i);
    }

    delete mGroups;
    mGroups = 0;
  }

  if (mPositionalOptions)
  {
    std::map<int, CliPositionalOption *>::const_iterator it;
    for (it = mPositionalOptions->cbegin(); it != mPositionalOptions->cend(); it++)
      delete (*it).second;

    delete mPositionalOptions;
    mPositionalOptions = 0;
  }

  if (mSubcommands)
  {
    for(i = 0; i < mSubcommands->size(); i++)
      {
        delete mSubcommands->at(i);
      }
    delete mSubcommands;
    mSubcommands = 0;
  }

  clean();

  mErrors = 0;
}

void
ConsoleCliCommand::clean()
{
  if (mErrors)
  {
    for (size_t i = 0; i < mErrors->size(); i++)
      delete mErrors->at(i);

    mErrors->clear();
  }

  mOptionsMap.clear();
}

void
ConsoleCliCommand::addOption(CliOption *option)
{
  if (mMainGroup == 0)
    mMainGroup = new OptionsGroup;

  mMainGroup->addOption(option);
}

void
ConsoleCliCommand::addOption(CliPositionalOption *option)
{
  if (mPositionalOptions == 0)
    mPositionalOptions = new std::map<int, CliPositionalOption *>;

  int pos = option->position();
  if (mPositionalOptions->count(pos) != 0)
  {
    delete mPositionalOptions->at(pos);
    mPositionalOptions->erase(pos);
  }
  mPositionalOptions->insert({pos, option});
}

void
ConsoleCliCommand::addSubcommand(ConsoleCliCommand *subcommand)
{
  assert(subcommand != this);

  if (mSubcommands == 0)
    mSubcommands = new std::vector<ConsoleCliCommand *>;

  if (std::find(mSubcommands->begin(), mSubcommands->end(), subcommand) == mSubcommands->end())
    mSubcommands->push_back(subcommand);
  subcommand->setParent(this);
}

void
ConsoleCliCommand::addOption(const CliOption &option)
{
  CliOption *newObj = new CliOption(option);
  addOption(newObj);
}

void
ConsoleCliCommand::addOption(const CliOptionWithArgs &option)
{
  CliOptionWithArgs *newObj = new CliOptionWithArgs(option);
  addOption(newObj);
}

void
ConsoleCliCommand::addOption(const CliPositionalOption &option)
{
  CliPositionalOption *newObj = new CliPositionalOption(option);
  addOption(newObj);
}

void
ConsoleCliCommand::addOptions(std::vector<CliOption> options)
{
  std::vector<CliOption>::const_iterator it = options.cbegin();
  for (; it != options.cend(); it++)
  {
    addOption(*it);
  }
}

void
ConsoleCliCommand::addOptions(std::vector<CliOptionWithArgs> options)
{
  std::vector<CliOptionWithArgs>::const_iterator it = options.cbegin();
  for (; it != options.cend(); it++)
  {
    addOption(*it);
  }
}

void
ConsoleCliCommand::addOptions(std::vector<CliPositionalOption> options)
{
  std::vector<CliPositionalOption>::const_iterator it = options.cbegin();
  for (; it != options.cend(); it++)
  {
    addOption(*it);
  }
}

OptionsGroup*
ConsoleCliCommand::addGroupedOptions(std::vector<CliOption> options)
{
  if (options.size() == 0)
    return 0;

  OptionsGroup *group = new OptionsGroup;
  addGroup(group);

  group->addOptions(options);

  return group;
}

OptionsGroup*
ConsoleCliCommand::addGroupedOptions(std::vector<CliOptionWithArgs> options)
{
  if (options.size() == 0)
    return 0;

  OptionsGroup *group = new OptionsGroup;
  addGroup(group);

  group->addOptions(options);

  return group;
}

void
ConsoleCliCommand::addGroup(OptionsGroup *group)
{
  if (!mGroups)
    mGroups = new std::vector<OptionsGroup *>;

  if (std::find(mGroups->begin(), mGroups->end(), group) == mGroups->end())
    mGroups->push_back(group);
}

void
ConsoleCliCommand::addError(const ParseError *error)
{
  if (!mErrors)
    mErrors = new std::vector<const ParseError *>;

  mErrors->push_back(error);
}

bool
ConsoleCliCommand::hasErrors()
{
  return mErrors && !mErrors->empty();
}

ConsoleCliCommand *
ConsoleCliCommand::isSubcommand(std::vector<std::string> &cliArgs)
{
  if (cliArgs.size() == 0)
    return 0;

  std::vector<ConsoleCliCommand *>::const_iterator it = mSubcommands->cbegin();
  for (; it != mSubcommands->cend(); it++)
  {
    if ((*it)->name().compare(cliArgs[0]) == 0)
      return *it;
  }

  return 0;
}

void
ConsoleCliCommand::analyseGroup(OptionsGroup *group, std::vector<std::string> &cliArgs)
{
  std::vector<CliOption *>* options = group->options();
  if (options)
  {
    bool optionFound = false;
    std::vector<CliOption *>::iterator it = options->begin();
    for (; it != options->end(); it++)
      {
        AnalysisResult *res = (*it)->analyse(cliArgs);

        if (!res)
          continue;

        if (res->values.first != "")
        {
          if (optionFound && group != mMainGroup)
          {
            addError(new ParseError(0, "Error: Use only one option: " + group->optionsRepr()));
            delete res;
            return;
          }

          if (res->errorMsg == "")
            mOptionsMap.insert(res->values);
          else
            addError(new ParseError(*it, res->errorMsg));

          cliArgs.erase(res->start, res->end);
          optionFound = true;
        }
        delete res;
      }

    if (!optionFound && group->required())
      addError(new ParseError(0, "Error: You have to use at least one "
                                 "of these options: " + group->optionsRepr()));
  }
}

ConsoleCliCommand *
ConsoleCliCommand::parse(std::vector<std::string> &cliArgs)
{
  clean();

  if (mSubcommands)
  {
    ConsoleCliCommand *subcommand = isSubcommand(cliArgs);

    if (subcommand)
    {
      std::vector<std::string> subcommandArgs(cliArgs);
      subcommandArgs.erase(subcommandArgs.begin());

      return subcommand->parse(subcommandArgs);
    }
  }

  if (mMainGroup)
    analyseGroup(mMainGroup, cliArgs);

  if (mGroups)
  {
    std::vector<OptionsGroup *>::const_iterator it;
    for (it = mGroups->cbegin(); it != mGroups->cend(); it++)
      analyseGroup(*it, cliArgs);
  }

  int numArgsProcessed = (int) cliArgs.size();
  if (mPositionalOptions)
  {
    std::map<int, CliPositionalOption *>::iterator pos_it = mPositionalOptions->begin();
    for (; pos_it != mPositionalOptions->end(); pos_it++)
      {
        AnalysisResult *res = (*pos_it).second->analyse(cliArgs);

        if (!res) // not required and not found
          continue;

        if (res->errorMsg == "")
          mOptionsMap.insert(res->values);
        else
          addError(new ParseError((*pos_it).second, res->errorMsg));

        numArgsProcessed -= (res->end - res->start);

        delete res;
      }
  }

  if (numArgsProcessed > 0)
    addError(new ParseError(0, "Error: Unknown arguments found."));

  return this;
}

ConsoleCliCommand *
ConsoleCliCommand::parse(const std::string &cliArgs)
{
  std::vector<std::string>* cliArgsVector = splitKeywords(cliArgs, ' ');
  ConsoleCliCommand *cmd = parse(*cliArgsVector);

  delete cliArgsVector;

  return cmd;
}

bool
ConsoleCliCommand::hasValue(std::string optionName)
{
  return mOptionsMap.count(optionName);
}

bool
ConsoleCliCommand::hasValues()
{
  return !mOptionsMap.empty();
}

std::string
ConsoleCliCommand::getValue (std::string optionName)
{
  return mOptionsMap[optionName][0];
}

std::vector<std::string>
ConsoleCliCommand::getValues (std::string optionName)
{
  return mOptionsMap[optionName];
}

void
ConsoleCliCommand::printHelpForOptions(std::vector<CliOption *>* options)
{
  std::vector<CliOption *>::const_iterator it;
  for (it = options->cbegin(); it != options->cend(); it++)
  {
    char *str = (*it)->helpString();

    if (str != NULL)
    {
      fprintf(stdout, str);
      free(str);
    }
  }
}

void
ConsoleCliCommand::printHelp()
{
  if (mMainGroup)
    printHelpForOptions(mMainGroup->options());

  if (mGroups)
  {
    for (size_t i = 0; i < mGroups->size(); i++)
      printHelpForOptions(mGroups->at(i)->options());
  }

  if (mPositionalOptions)
  {
    std::map<int, CliPositionalOption *>::const_iterator it;
    for (it = mPositionalOptions->cbegin(); it != mPositionalOptions->cend(); it++)
    {
      char *str = (*it).second->helpString();

      if (str != NULL)
      {
        fprintf(stdout, str);
        free(str);
      }
    }
  }
}

void
ConsoleCliCommand::printUsage()
{
  std::string subcommRepr = subcommandsRepr();
  std::string kwRepr = keywordsRepr();
  std::string posOptionsRepr = positionalOptionsRepr();
  std::string commandAndOptions = mName;

  if (subcommRepr != "")
    commandAndOptions += " " + subcommRepr;

  if (kwRepr != "")
    commandAndOptions += " " + kwRepr;

  if (mGroups)
  {
    std::vector<OptionsGroup *>::const_iterator it;
    for (it = mGroups->cbegin(); it != mGroups->cend(); it++)
    {
      std::string groupRepr((*it)->name());

      if (groupRepr == "")
        groupRepr = (*it)->optionsRepr();

      commandAndOptions += " " + ((*it)->required() ? groupRepr : "[" + groupRepr + "]");
    }
  }

  if (posOptionsRepr != "")
    commandAndOptions += " " + posOptionsRepr;

  if (mParentCommand)
    commandAndOptions = mParentCommand->name() + " " + commandAndOptions;

  fprintf(stdout, "Usage: %s", commandAndOptions.c_str());
  if (mDescription != "") {
    fprintf(stdout, " : %s\n", mDescription.c_str());
  }
  printHelp();
}

void
ConsoleCliCommand::printErrors()
{
  if (!mErrors)
    return;

  std::string errorsStr("");
  std::vector<const ParseError*>::const_iterator it;
  for (it = mErrors->cbegin(); it != mErrors->cend(); it++)
  {
    errorsStr += (*it)->message() + "\n";
  }
  fprintf(stdout, errorsStr.c_str());
}

void
ConsoleCliCommand::setParent(const ConsoleCliCommand *parent)
{
  mParentCommand = parent;
}

std::string
ConsoleCliCommand::keywordsRepr()
{
  std::vector<CliOption *>* options;
  std::string repr("");

  if (!mMainGroup)
    return repr;

  options = mMainGroup->options();

  if (!options)
    return repr;

  for (size_t i = 0; i < options->size(); i++)
  {
    if (options->at(i)->hidden())
      continue;

    std::string optionRepr = options->at(i)->repr();

    if (!options->at(i)->required())
      optionRepr = "[" + optionRepr + "]";

    if (repr != "")
      repr += " ";

    repr += optionRepr;
  }

  return repr;
}

std::string
ConsoleCliCommand::subcommandsRepr()
{
  std::string repr("");

  if (!mSubcommands)
    return repr;

  for (size_t i = 0; i < mSubcommands->size(); i++)
  {
    repr += mSubcommands->at(i)->name();
    if (i < mSubcommands->size() - 1)
      repr += "|";
  }

  return repr;
}

std::string
ConsoleCliCommand::positionalOptionsRepr()
{
  std::string repr("");

  if (!mPositionalOptions)
    return repr;

  std::map<int, CliPositionalOption *>::const_iterator it;
  for (it = mPositionalOptions->cbegin(); it != mPositionalOptions->cend(); it++)
  {
    const std::string &currentRepr = (*it).second->repr();
    repr += (*it).second->required() ? currentRepr : "[" + currentRepr + "]";

    if (it == --mPositionalOptions->cend())
      break;

    repr += " ";
  }

  return repr;
}

OptionsGroup::OptionsGroup(std::string name)
  : mName(name),
    mOptions(0),
    mRequired(false)
{}

OptionsGroup::OptionsGroup()
  : OptionsGroup("")
{}

OptionsGroup::~OptionsGroup()
{
  if (mOptions)
  {
    size_t i;
    for(i = 0; i < mOptions->size(); i++)
    {
      delete mOptions->at(i);
    }
    delete mOptions;
    mOptions = 0;
  }
}

void
OptionsGroup::addOption(CliOption *option)
{
  if (!mOptions)
    mOptions = new std::vector<CliOption *>;

  if (option->group() != this)
  {
    option->setGroup(this);
    mOptions->push_back(option);
  }
}

void
OptionsGroup::addOption(const CliOption &option)
{
  CliOption *optionPtr = new CliOption(option);
  optionPtr->setRequired(false);
  addOption(optionPtr);
}

void
OptionsGroup::addOption(const CliOptionWithArgs &option)
{
  CliOptionWithArgs *optionPtr = new CliOptionWithArgs(option);
  optionPtr->setRequired(false);
  addOption(optionPtr);
}

void
OptionsGroup::addOptions(std::vector<CliOption> options)
{
  std::vector<CliOption>::const_iterator it = options.cbegin();
  for (; it != options.cend(); it++)
  {
    addOption(*it);
  }
}

void
OptionsGroup::addOptions(std::vector<CliOptionWithArgs> options)
{
  std::vector<CliOptionWithArgs>::const_iterator it = options.cbegin();
  for (; it != options.cend(); it++)
  {
    addOption(*it);
  }
}

void
OptionsGroup::removeOption(CliOption *option)
{
  option->setGroup(0);

  if (!mOptions)
    return;

  std::vector<CliOption *>::iterator it;
  for (it = mOptions->begin(); it != mOptions->end(); it++)
  {
    if (*it == option)
    {
      mOptions->erase(it);
      return;
    }
  }
}

std::string
OptionsGroup::optionsRepr()
{
  std::string repr = "";

  if (!mOptions)
    return repr;

  size_t i;
  std::vector<const CliOption *>::iterator it;
  for (i = 0; i < mOptions->size(); i++)
  {
    repr += mOptions->at(i)->repr();

    if (i != mOptions->size() - 1)
      repr += "|";
  }

  return repr;
}
