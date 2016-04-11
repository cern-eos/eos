#include <sstream>
#include <iomanip>
#include <fstream>
#include "RamCloud.h"
#include "Context.h"
#include "TableEnumerator.h"
#include "boost/program_options.hpp"

using namespace RAMCloud;
static Context* sContext;
static RamCloud* sClient;

//-----------------------------------------------------------------------------
// List table contents
//-----------------------------------------------------------------------------
void table_list(const std::string& name)
{
  std::string key_header = "Key", data_header = "Data";
  uint32_t key_max_len = key_header.length(),
    data_max_len = data_header.length();
  std::map<std::string, std::string> map;

  try {
    uint64_t table_id = sClient->getTableId(name.c_str());
    TableEnumerator iter(*sClient, table_id, false);
    uint32_t key_len = 0, data_len = 0;
    const void* key_buff = 0, *data_buff = 0;

    while (iter.hasNext())
    {
      iter.nextKeyAndData(&key_len, &key_buff, &data_len, &data_buff);

      if (strlen(static_cast<const char*>(data_buff)) != data_len)
      {
	// This might be an int64_t data type
	int64_t numeric_val = *static_cast<const int64_t*>(data_buff);
	map.emplace(std::string((char*)key_buff, key_len),
		    std::to_string(numeric_val));
      }
      else
      {
	map.emplace(std::string((char*)key_buff, key_len),
		    std::string((char*)data_buff, data_len));
      }

      if (key_len > key_max_len)
	key_max_len = key_len;

      if (data_len > data_max_len)
	data_max_len = data_len;
    }
  }
  catch (TableDoesntExistException& e) {
    std::cout << "Table doesn't exist" << std::endl;
    return;
  }
  catch (ClientException& e) {
    std::cout << "Error in client operation" << std::endl;
    return;
  }

  // Display the contents of the map
  std::ostringstream oss;
  oss << '|' << std::setfill('-') << std::setw(key_max_len + 1)
      << '|' << std::setw(data_max_len + 1)
      << '|' << std::setfill(' ');
  std::string line = oss.str();
  oss.str("");
  oss.clear();

  // Create table header
  oss << line << std::endl
      << '|' << std::setw(key_max_len) << std::setiosflags(std::ios_base::left)
      << "Key"
      << '|'<< std::setw(data_max_len) << std::setiosflags(std::ios_base::left)
      << "Data"
      << '|' << std::endl << line << std::endl;

  // Add the data in the table
  for (auto pair: map)
  {
    oss << '|' << std::setw(key_max_len) << std::setfill(' ')
	<< std::setiosflags(std::ios_base::left)
	<< pair.first
	<< '|'<< std::setw(data_max_len) << std::setfill(' ')
	<< std::setiosflags(std::ios_base::left)
	<< pair.second
	<< '|' << std::endl;
  }

  if (!map.empty())
  {
    oss << line << std::endl;
  }

  std::cout << oss.str();
  return;
}

//-----------------------------------------------------------------------------
// Delete table contents
//-----------------------------------------------------------------------------
void table_delete(const std::string& table_name)
{
  try {
    uint64_t table_id = sClient->getTableId(table_name.c_str());
    TableEnumerator iter(*sClient, table_id, true);
    uint32_t size = 0;
    const void* buffer = 0;

    while (iter.hasNext())
    {
      iter.next(&size, &buffer);
      Object object(buffer, size);
      sClient->remove(table_id, static_cast<const void*>(object.getKey()),
		      object.getKeyLength());
    }
  }
  catch (TableDoesntExistException& e) {}

  std::cout << "Table " << table_name << " contents deleted" << std::endl;
}

//-----------------------------------------------------------------------------
// Drop table
//-----------------------------------------------------------------------------
void table_drop(const std::string& table_name)
{
  try {
    sClient->dropTable(table_name.c_str());
  }
  catch (ClientException& e) {};

  std::cout << "Table " << table_name << " dropped" << std::endl;
}


static std::map<std::string, std::function<void(const std::string&)>>
  table_ops =
{
  { "list", table_list },
  { "delete" , table_delete },
  { "drop", table_drop }
};

//------------------------------------------------------------------------------
// Do table operation
//------------------------------------------------------------------------------
void table_operation(const std::string& table_name, const std::string& op)
{
  if (table_ops.find(op) == table_ops.end())
  {
    std::cout << "Unknown table operation: " << op << std::endl;
    return;
  }

  // Run the required function
  table_ops[op](table_name);
}

int main(int argc, char* argv[])
{
  namespace po = boost::program_options;
  std::string config_file, external_storage, rc_namespace, config_file_ext_st;
  po::options_description common_options("Common");
  common_options.add_options()
    ("help,h", "Display help message")
    ("config,c", po::value<std::string>(&config_file)->default_value(".ramcloudcli"),
     "Specify path for configuration file");

  po::options_description config_options("RAMCloud");
  config_options.add_options()
    ("namespace,n", po::value<std::string>(&rc_namespace),
     "Specify RAMCloud namespace")
    ("externalStorage,x", po::value<std::string>(&external_storage)->default_value
     ("localhost:5254"), "Locator for external storage server containing cluster "
     "configuration information")
    ("configFileExternalStorage", po::value<std::string>(&config_file_ext_st),
     "Configuration file for the clietn accessing the external storage i.e. LogCabin "
     "or ZooKeeper");

  po::options_description table_options("Table");
  std::string table_name, table_op;
  table_options.add_options()
    ("table,t", po::value<std::string>(&table_name)->default_value(""), "Table name")
    ("operation,o", po::value<std::string>(&table_op)->default_value("list"),
     "Table opteration: list, listkeys, listvalues, delete, drop");

  // Do onw pass with just the common options so we can get the alternate config
  // file location, if specified. Then do a second pass with all the options for
  // real
  po::variables_map throw_away;
  po::store(po::command_line_parser(argc, argv).options(common_options)
	    .allow_unregistered().run(), throw_away);
  po::notify(throw_away);
  po::options_description all_options("Usage");
  all_options.add(common_options).add(config_options).add(table_options);
  po::variables_map vm;
  std::ifstream config_input(config_file.c_str());
  po::store(po::parse_command_line(argc, argv, all_options), vm);
  // True here lets the config file contain unknown key/value pairs
  po::store(po::parse_config_file(config_input, all_options, true), vm);
  po::notify(vm);

  if (vm.count("help"))
  {
    std::cout << all_options << std::endl;
    return 1;
  }

  if (!vm.count("namespace"))
  {
    std::cerr << "No RAMCloud namespace specified" << std::endl;
    return 1;
  }

  // Create RAMCloud client object
  if (!vm.count("externalStorage"))
  {
    std::cerr << "No external storage specified" << std::endl;
    return 1;
  }
  else
  {
    try {
      sContext = new Context(false);
      sContext->configFileExternalStorage = config_file_ext_st;
      sClient = new RamCloud(sContext, external_storage.c_str(), rc_namespace.c_str());
    }
    catch (ClientException& e) {
      std::cerr << "Error while creating RAMCloud client" << std::endl;
      return 1;
    }
  }

  if (!table_name.empty())
  {
    table_operation(table_name, table_op);
  }

  // Free memory
  delete sContext;
  delete sClient;
  return 0;
}
