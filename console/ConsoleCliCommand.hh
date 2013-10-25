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

#ifndef CONSOLE_CLI_COMMAND_HH
#define CONSOLE_CLI_COMMAND_HH

#include <string>
#include <vector>
#include <utility>
#include <map>

#define optionIsFloatEvalFunc ((evalFuncCb) isFloatEvalFunc)
#define optionIsIntegerEvalFunc ((evalFuncCb) isIntegerEvalFunc)
#define optionIsNumberInRangeEvalFunc ((evalFuncCb) isNumberInRangeEvalFunc)
#define optionIsChoiceEvalFunc ((evalFuncCb) isChoiceEvalFunc)
#define optionIsPositiveNumberEvalFunc ((evalFuncCb) isPositiveNumberEvalFunc)
#define optionIsNegativeNumberEvalFunc ((evalFuncCb) isNegativeNumberEvalFunc)

class CliBaseOption;
class CliOptionWithArgs;
class ConsoleCliCommand;
class OptionsGroup;

typedef bool (*evalFuncCb) (const CliOptionWithArgs *option,
                            std::vector<std::string> &args,
                            std::string **error,
                            void *userData);

bool isFloatEvalFunc (const CliOptionWithArgs *option,
                      std::vector<std::string> &args,
                      std::string **error,
                      void *userData);

bool isIntegerEvalFunc (const CliOptionWithArgs *option,
                        std::vector<std::string> &args,
                        std::string **error,
                        void *userData);

bool isNumberInRangeEvalFunc (const CliOptionWithArgs *option,
                              std::vector<std::string> &args,
                              std::string **error,
                              const std::pair<float, float> *range);

bool isPositiveNumberEvalFunc (const CliOptionWithArgs *option,
                               std::vector<std::string> &args,
                               std::string **error,
                               void *data);

bool isNegativeNumberEvalFunc (const CliOptionWithArgs *option,
                               std::vector<std::string> &args,
                               std::string **error,
                               void *data);

bool isChoiceEvalFunc (const CliOptionWithArgs *option,
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
  virtual void setName(const std::string &name) { mName = name; };
  virtual void setDescription(const std::string &desc) { mDescription = desc; };
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

class CliOptionWithArgs : public CliOption{
public:
  CliOptionWithArgs(std::string name, std::string desc,
                    std::string keywords, int numArgs,
                    std::string repr, bool required);
  CliOptionWithArgs(std::string name, std::string desc,
                    std::string keywords, std::string repr,
                    bool required);
  CliOptionWithArgs(const CliOptionWithArgs &otherOption);
  ~CliOptionWithArgs();
  virtual AnalysisResult* analyse(std::vector<std::string> &cliArgs);
  virtual std::string repr() const;
  virtual char* helpString();
  bool shouldEvaluate() const { return mEvalFunctions && mUserData; };
  void addEvalFunction(evalFuncCb func, void *userData);

protected:
  virtual std::string hasKeyword(std::string keyword);
  std::string mRepr;
  int mNumArgs;
  std::vector<evalFuncCb> *mEvalFunctions;
  std::vector<void *> *mUserData;

  AnalysisResult* commonAnalysis(std::vector<std::string> &cliArgs,
                                 int initPos,
                                 const std::string &firstArg);
};

class CliPositionalOption : public CliOptionWithArgs {
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
  void setPosition(int position) { mPosition = position; }

private:
  int mPosition;
};

class OptionsGroup {
public:
  OptionsGroup();
  OptionsGroup(std::string name);
  OptionsGroup(const OptionsGroup &otherGroup);
  virtual ~OptionsGroup();
  void addOption(CliOption *option);
  void addOption(const CliOption &option);
  void addOption(const CliOptionWithArgs &option);
  void addOptions(std::vector<CliOption> options);
  void addOptions(std::vector<CliOptionWithArgs> options);
  CliOption *getOption(const std::string &name) const;
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
  ConsoleCliCommand (const ConsoleCliCommand &otherCmd);
  ~ConsoleCliCommand ();
  void addSubcommand(ConsoleCliCommand *subcommand);
  void addOption(CliOption *option);
  void addOption(CliPositionalOption *option);
  void addOption(const CliPositionalOption &option);
  void addOption(const CliOption &option);
  void addOption(const CliOptionWithArgs &option);
  void addOptions(std::vector<CliOption> options);
  void addOptions(std::vector<CliPositionalOption> options);
  void addOptions(std::vector<CliOptionWithArgs> options);
  void addGroup(OptionsGroup *group);
  CliOption *getOption(const std::string &name) const;
  OptionsGroup* addGroupedOptions(std::vector<CliOption> options);
  OptionsGroup* addGroupedOptions(std::vector<CliOptionWithArgs> options);
  bool hasErrors();
  ConsoleCliCommand* parse(std::vector<std::string> &cliArgs);
  ConsoleCliCommand* parse(const std::string &cliArgs);
  bool hasValue(std::string optionName);
  bool hasValues();
  std::vector<std::string> getValues(std::string optionName);
  std::string getValue(std::string optionName);
  void printHelp() const;
  void printUsage() const;
  void printErrors() const;
  std::string name() const { return mName;};
  std::string description() const { return mDescription; };
  void setName(const std::string &name) { mName = name;};
  void setDescription(const std::string &desc) { mDescription = desc; };
  void setParent(const ConsoleCliCommand *parent);
  const ConsoleCliCommand *parent() const { return mParentCommand; };
  std::vector<ConsoleCliCommand *> *subcommands() const { return mSubcommands; };
  void setStandalone(bool standalone) { mStandalone = standalone; };
  bool standalone() const { return mStandalone; };

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
  /* mStandalone dictates whether this command can be used without any
   * subcommands; When adding the first subcommand to a command, the latter
   * will become non-standalone so, if it should be, remember to call
   * setStandalone(true) on it; adding other subcommand (after the first) will
   * not change the standalone setting because it means that the user might
   * have already changed it! */
  bool mStandalone;

  void analyseGroup(OptionsGroup *group, std::vector<std::string> &cliArgs);
  ConsoleCliCommand* isSubcommand(std::vector<std::string> &cliArgs);
  void printHelpForOptions(std::vector<CliOption *>* options) const;
  std::string keywordsRepr() const;
  std::string subcommandsRepr() const;
  std::string positionalOptionsRepr() const;
  void addError(const ParseError *);
  void clean();
};

#endif // CONSOLE_CLI_COMMAND_HH
