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

#define optionIsFloatEvalFunc ((evalFuncCb) isFloatEvalFunc)
#define optionIsIntegerEvalFunc ((evalFuncCb) isIntegerEvalFunc)
#define optionIsNumberInRangeEvalFunc ((evalFuncCb) isNumberInRangeEvalFunc)
#define optionIsChoiceEvalFunc ((evalFuncCb) isChoiceEvalFunc)

class CliBaseOption;
class CliCheckableOption;
class ConsoleCliCommand;
class OptionsGroup;

typedef bool (*evalFuncCb) (const CliCheckableOption *option,
                            std::vector<std::string> &args,
                            std::string **error,
                            void *userData);

bool isFloatEvalFunc (const CliCheckableOption *option,
                      std::vector<std::string> &args,
                      std::string **error,
                      void *userData);

bool isIntegerEvalFunc (const CliCheckableOption *option,
                        std::vector<std::string> &args,
                        std::string **error,
                        void *userData);

bool isNumberInRangeEvalFunc (const CliCheckableOption *option,
                              std::vector<std::string> &args,
                              std::string **error,
                              const std::pair<float, float> *range);

bool isChoiceEvalFunc (const CliCheckableOption *option,
                       std::vector<std::string> &args,
                       std::string **error,
                       const std::vector<std::string> *choices);

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
  virtual std::string repr() const = 0;

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
  virtual std::string repr() const;
  virtual void setGroup(OptionsGroup *group);
  virtual const OptionsGroup *group() const { return mGroup; };

protected:
  std::vector<std::string> *mKeywords;
  OptionsGroup *mGroup;

  virtual std::string hasKeyword(std::string keyword);
  virtual std::string joinKeywords();
};

class CliCheckableOption {
public:
  CliCheckableOption() : mEvalFunctions(0), mUserData(0) {}
  virtual ~CliCheckableOption();
  bool shouldEvaluate() const { return mEvalFunctions && mUserData; };
  void addEvalFunction(evalFuncCb func, void *userData);
protected:
  std::vector<evalFuncCb> *mEvalFunctions;
  std::vector<void *> *mUserData;
};

class CliOptionWithArgs : public CliOption, public CliCheckableOption {
public:
  CliOptionWithArgs(std::string name, std::string desc,
                    std::string keywords, int numArgs,
                    std::string repr, bool required);
  virtual AnalysisResult* analyse(std::vector<std::string> &cliArgs);
  virtual std::string repr() const;

protected:
  virtual std::string hasKeyword(std::string keyword);

private:
  int mNumArgs;
  std::string mRepr;
};

class CliPositionalOption : public CliBaseOption, public CliCheckableOption {
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
  virtual std::string repr() const;
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
  std::vector<std::string> getValues(std::string optionName);
  std::string getValue(std::string optionName);
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
