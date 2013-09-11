#include <string>
#include <vector>
#include <utility>
#include <map>

typedef struct
{
  std::pair<std::string, std::vector<std::string>> values;
  std::vector<std::string>::iterator start;
  std::vector<std::string>::iterator end;
} AnalysisResult;

class CliBaseOption {
public:
  CliBaseOption(std::string name, std::string desc);
  virtual ~CliBaseOption();
  virtual AnalysisResult* analyse(std::vector<std::string> &cli_args) = 0;
  virtual char* help_string() { return strdup(""); };
  virtual char* keywords_repr() { return strdup(""); };
  virtual const char* name() const { return m_name.c_str(); };

protected:
  std::string m_name;
  std::string m_description;
  bool m_required;
};

class CliOption : public CliBaseOption {
public:
  CliOption(std::string name, std::string desc, std::string keywords);
  CliOption(const CliOption &option);
  virtual ~CliOption();
  virtual AnalysisResult* analyse(std::vector<std::string> &cli_args);
  virtual char* help_string();
  virtual char* keywords_repr();

protected:
  std::vector<std::string> *m_keywords;

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

class ConsoleCliOptions {
public:
  ConsoleCliCommand (const std::string &name, const std::string &description);
  ~ConsoleCliCommand ();
  void add_option(CliOption *option);
  void add_option(const CliOption &option);
  void add_options(std::vector<CliOption> options);
  void parse(std::vector<std::string> &cli_args);
  void parse(std::string &cli_args);
  void parse(std::string cli_args);
  bool has_value(std::string option_name);
  bool has_values();
  std::vector<std::string> get_value(std::string option_name);
  char* keywords_repr();
  void print_help();
  void print_usage();

private:
  std::string m_name;
  std::string m_description;
  std::vector<CliOption *> *m_options;
  std::map<std::string, std::vector<std::string>> m_options_map;
};
