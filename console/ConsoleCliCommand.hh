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
  CliBaseOption* option() const { return m_option; };
  std::string message() const { return m_message; };

private:
  CliBaseOption *m_option;
  std::string m_message;
};

typedef struct
{
  std::pair<std::string, std::vector<std::string>> values;
  std::vector<std::string>::iterator start;
  std::vector<std::string>::iterator end;
  std::string error_msg;
} AnalysisResult;

class CliBaseOption {
public:
  CliBaseOption(std::string name, std::string desc);
  virtual ~CliBaseOption();
  virtual AnalysisResult* analyse(std::vector<std::string> &cli_args) = 0;
  virtual char* help_string() { return strdup(""); };
  virtual char* keywords_repr() { return strdup(""); };
  virtual const char* name() const { return m_name.c_str(); };
  virtual const char* description() const { return m_description.c_str(); };
  virtual bool required() const { return m_required; };
  virtual void set_required(bool req) { m_required = req; };
  virtual bool hidden() const { return m_hidden; };
  virtual void set_hidden(bool hidden) { m_hidden = hidden; };

protected:
  std::string m_name;
  std::string m_description;
  bool m_required;
  bool m_hidden;
};

class CliOption : public CliBaseOption {
public:
  CliOption(std::string name, std::string desc, std::string keywords);
  CliOption(const CliOption &option);
  virtual ~CliOption();
  virtual AnalysisResult* analyse(std::vector<std::string> &cli_args);
  virtual char* help_string();
  virtual char* keywords_repr();
  virtual std::string repr();
  virtual void set_group(OptionsGroup *group);
  virtual const OptionsGroup *group() const { return m_group; };

protected:
  std::vector<std::string> *m_keywords;
  OptionsGroup *m_group;

  virtual std::string has_keyword(std::string keyword);
  virtual std::string join_keywords();
};

class CliOptionWithArgs : public CliOption {
public:
  CliOptionWithArgs(std::string name, std::string desc, std::string keywords,
                    std::string joint_keywords,
                    int min_num_args, int max_num_args);
  CliOptionWithArgs(std::string name, std::string desc,
                    std::string keywords,
                    int min_num_args, int max_num_args,
                    bool required);
  virtual AnalysisResult* analyse(std::vector<std::string> &cli_args);

private:
  int m_min_num_args;
  int m_max_num_args;
  std::vector<std::string> *m_joint_keywords;
};

class CliPositionalOption : public CliBaseOption {
public:
  CliPositionalOption(std::string name, std::string desc, int position,
                      int num_args, std::string repr);
  CliPositionalOption(std::string name, std::string desc, int position,
                      int num_args, std::string repr, bool required);
  CliPositionalOption(std::string name, std::string desc, int position,
                      std::string repr);
  CliPositionalOption(const CliPositionalOption &option);
  ~CliPositionalOption();
  virtual AnalysisResult* analyse(std::vector<std::string> &cli_args);
  virtual char* help_string();
  virtual std::string repr();
  int position() { return m_position; };

private:
  int m_position;
  int m_num_args;
  std::string m_repr;
};

class OptionsGroup {
public:
  OptionsGroup();
  virtual ~OptionsGroup();
  void add_option(CliOption *option);
  void remove_option(CliOption *option);
  std::vector<CliOption *>* options() { return m_options; };
  bool required() const { return m_required; };
  void set_required(bool req) { m_required = req; };
  std::string options_repr();

private:
  std::vector<CliOption *> *m_options;
  bool m_required;
};

class ConsoleCliCommand {
public:
  ConsoleCliCommand (const std::string &name, const std::string &description);
  ~ConsoleCliCommand ();
  void add_subcommand(ConsoleCliCommand *subcommand);
  void add_option(CliOption *option);
  void add_option(CliPositionalOption *option);
  void add_option(const CliPositionalOption &option);
  void add_option(const CliOption &option);
  void add_options(std::vector<CliOption> options);
  void add_group(OptionsGroup *group);
  OptionsGroup* add_grouped_options(std::vector<CliOption> options);
  bool has_errors();
  ConsoleCliCommand* parse(std::vector<std::string> &cli_args);
  ConsoleCliCommand* parse(std::string &cli_args);
  ConsoleCliCommand* parse(std::string cli_args);
  bool has_value(std::string option_name);
  bool has_values();
  std::vector<std::string> get_value(std::string option_name);
  void print_help();
  void print_usage();
  void print_errors();
  std::string name() const { return m_name;};
  void set_parent(const ConsoleCliCommand *parent);

private:
  std::string m_name;
  std::string m_description;
  std::vector<ConsoleCliCommand *> *m_subcommands;
  OptionsGroup *m_main_group;
  std::map<int, CliPositionalOption *> *m_positional_options;
  const ConsoleCliCommand *m_parent_command;
  std::map<std::string, std::vector<std::string>> m_options_map;
  std::vector<const ParseError *> *m_errors;
  std::vector<OptionsGroup *> *m_groups;
  void analyse_group(OptionsGroup *group, std::vector<std::string> &cli_args);
  ConsoleCliCommand* is_subcommand(std::vector<std::string> &cli_args);
  void print_help_for_options(std::vector<CliOption *>* options);

  std::string keywords_repr();
  std::string subcommands_repr();
  std::string positional_options_repr();
  void add_error(const ParseError *);
};
