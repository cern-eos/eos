// ----------------------------------------------------------------------
// File: ConsoleCliCommand.hh
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

#include <string>
#include <vector>
#include <utility>
#include <map>

class CliBaseOption;
class ConsoleCliCommand;
class OptionsGroup;

class ParseError {
public:
  ParseError(CliBaseOption *option, std::string message);
  CliBaseOption* option() const { return mOption; };
  std::string message() const { return mMessage; };

private:
  CliBaseOption *mOption;
  std::string mMessage;
};

typedef struct
{
  std::pair<std::string, std::vector<std::string>> values;
  std::vector<std::string>::iterator start;
  std::vector<std::string>::iterator end;
  std::string errorMsg;
} AnalysisResult;

class CliBaseOption {
public:
  CliBaseOption(std::string name, std::string desc);
  virtual ~CliBaseOption();
  virtual AnalysisResult* analyse(std::vector<std::string> &cliArgs) = 0;
  virtual char* helpString() { return strdup(""); };
  virtual char* keywordsRepr() { return strdup(""); };
  virtual const char* name() const { return mName.c_str(); };
  virtual const char* description() const { return mDescription.c_str(); };
  virtual bool required() const { return mRequired; };
  virtual void setRequired(bool req) { mRequired = req; };
  virtual bool hidden() const { return mHidden; };
  virtual void setHidden(bool hidden) { mHidden = hidden; };

protected:
  std::string mName;
  std::string mDescription;
  bool mRequired;
  bool mHidden;
};

class CliOption : public CliBaseOption {
public:
  CliOption(std::string name, std::string desc, std::string keywords);
  CliOption(const CliOption &option);
  virtual ~CliOption();
  virtual AnalysisResult* analyse(std::vector<std::string> &cliArgs);
  virtual char* helpString();
  virtual char* keywordsRepr();
  virtual std::string repr();
  virtual void setGroup(OptionsGroup *group);
  virtual const OptionsGroup *group() const { return mGroup; };

protected:
  std::vector<std::string> *mKeywords;
  OptionsGroup *mGroup;

  virtual std::string hasKeyword(std::string keyword);
  virtual std::string joinKeywords();
};

class CliOptionWithArgs : public CliOption {
public:
  CliOptionWithArgs(std::string name, std::string desc, std::string keywords,
                    std::string jointKeywords,
                    int minNumArgs, int maxNumArgs);
  CliOptionWithArgs(std::string name, std::string desc,
                    std::string keywords,
                    int minNumArgs, int maxNumArgs,
                    bool required);
  virtual AnalysisResult* analyse(std::vector<std::string> &cliArgs);

private:
  int mMinNumArgs;
  int mMaxNumArgs;
  std::vector<std::string> *mJointKeywords;
};

class CliPositionalOption : public CliBaseOption {
public:
  CliPositionalOption(std::string name, std::string desc, int position,
                      int numArgs, std::string repr);
  CliPositionalOption(std::string name, std::string desc, int position,
                      int numArgs, std::string repr, bool required);
  CliPositionalOption(std::string name, std::string desc, int position,
                      std::string repr);
  CliPositionalOption(const CliPositionalOption &option);
  ~CliPositionalOption();
  virtual AnalysisResult* analyse(std::vector<std::string> &cliArgs);
  virtual char* helpString();
  virtual std::string repr();
  int position() { return mPosition; };

private:
  int mPosition;
  int mNumArgs;
  std::string mRepr;
};

class OptionsGroup {
public:
  OptionsGroup();
  OptionsGroup(std::string name);
  virtual ~OptionsGroup();
  void addOption(CliOption *option);
  void addOption(CliOption option);
  void addOptions(std::vector<CliOption> options);
  void removeOption(CliOption *option);
  std::vector<CliOption *>* options() { return mOptions; };
  bool required() const { return mRequired; };
  void setRequired(bool req) { mRequired = req; };
  std::string optionsRepr();
  std::string name() const { return mName; };
  void setName(std::string name) { mName = name; };

private:
  std::string mName;
  std::vector<CliOption *> *mOptions;
  bool mRequired;
};

class ConsoleCliCommand {
public:
  ConsoleCliCommand (const std::string &name, const std::string &description);
  ~ConsoleCliCommand ();
  void addSubcommand(ConsoleCliCommand *subcommand);
  void addOption(CliOption *option);
  void addOption(CliPositionalOption *option);
  void addOption(const CliPositionalOption &option);
  void addOption(const CliOption &option);
  void addOptions(std::vector<CliOption> options);
  void addOptions(std::vector<CliPositionalOption> options);
  void addGroup(OptionsGroup *group);
  OptionsGroup* addGroupedOptions(std::vector<CliOption> options);
  bool hasErrors();
  ConsoleCliCommand* parse(std::vector<std::string> &cliArgs);
  ConsoleCliCommand* parse(std::string &cliArgs);
  ConsoleCliCommand* parse(std::string cliArgs);
  bool hasValue(std::string optionName);
  bool hasValues();
  std::vector<std::string> getValue(std::string optionName);
  void printHelp();
  void printUsage();
  void printErrors();
  std::string name() const { return mName;};
  void setParent(const ConsoleCliCommand *parent);

private:
  std::string mName;
  std::string mDescription;
  std::vector<ConsoleCliCommand *> *mSubcommands;
  OptionsGroup *mMainGroup;
  std::map<int, CliPositionalOption *> *mPositionalOptions;
  const ConsoleCliCommand *mParentCommand;
  std::map<std::string, std::vector<std::string>> mOptionsMap;
  std::vector<const ParseError *> *mErrors;
  std::vector<OptionsGroup *> *mGroups;
  void analyseGroup(OptionsGroup *group, std::vector<std::string> &cliArgs);
  ConsoleCliCommand* isSubcommand(std::vector<std::string> &cliArgs);
  void printHelpForOptions(std::vector<CliOption *>* options);

  std::string keywordsRepr();
  std::string subcommandsRepr();
  std::string positionalOptionsRepr();
  void addError(const ParseError *);
  void clean();
};
