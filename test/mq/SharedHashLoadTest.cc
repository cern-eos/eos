/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2024 CERN/Switzerland                                  *
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

#include "common/CLI11.hpp"
#include "common/Locators.hh"
#include "common/PasswordHandler.hh"
#include "mq/MessagingRealm.hh"
#include "mq/SharedHashWrapper.hh"
#include "namespace/ns_quarkdb/QdbContactDetails.hh"
#include "qclient/QClient.hh"
#include "qclient/shared/SharedManager.hh"
#include "qclient/shared/SharedHashSubscription.hh"
#include <random>
#include <string>
#include <sstream>
#include <stdexcept>
#include <functional>

#define SSTR(message) static_cast<std::ostringstream&>(std::ostringstream().flush() << message).str()

using eos::common::PasswordHandler;
using eos::common::SharedHashLocator;
std::unique_ptr<eos::mq::MessagingRealm> gMsgRealm {nullptr};
std::unique_ptr<eos::mq::SharedHashWrapper> gSharedHash {nullptr};
std::mutex gMutexTerminate;
std::condition_variable gCvTerminate;
bool gSignalTerminate {false};

//! Structure modelling the type of shared has updates:
//! kPersistent - this is stored in the raft journal and persisted in QDB
//! kTransient  - this is kept only in memory of QDB and set to potential
//!               interested subsribers
//! kLocal      - this never leaves the current client memory and is never
//!               send or persisted in QDB
enum UpdateType {
  kPersistent = 0,
  kTransient,
  kLocal
};

//------------------------------------------------------------------------------
// QDB cluster member validator
//------------------------------------------------------------------------------
struct MemberValidator: public CLI::Validator {
  MemberValidator(): Validator("MEMBER")
  {
    func_ = [](const std::string & str) {
      qclient::Members members;

      if (!members.parse(str)) {
        return SSTR("Failed parsing members: " << str << "."
                    << "Expected format is a comma-separated list of servers.");
      }

      return std::string();
    };
  }
};

//------------------------------------------------------------------------------
// Logging object
//------------------------------------------------------------------------------
class Logger
{
public:
  //------------------------------------------------------------------------------
  // Log method
  //------------------------------------------------------------------------------
  void log(std::string_view data)
  {
    std::unique_lock<std::mutex> lock(mMutex);
    fprintf(stderr, "%s\n", data.data());
  }

private:
  std::mutex mMutex;

};

//------------------------------------------------------------------------------
// Generate a random alpha-numeric string of the given length
//------------------------------------------------------------------------------
std::string RandomString(std::string::size_type length)
{
  static const std::string chrs = "0123456789"
                                  "abcdefghijklmnopqrstuvwxyz"
                                  "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  thread_local static std::mt19937 rg{std::random_device{}()};
  thread_local static std::uniform_int_distribution<std::string::size_type>
  pick(0, chrs.length() - 2);
  std::string s;
  s.reserve(length);

  while (length--) {
    s += chrs[pick(rg)];
  }

  return s;
}

//------------------------------------------------------------------------------
// Generate list of random keys of given length
//------------------------------------------------------------------------------
std::vector<std::string>
GenerateStrings(unsigned int num_keys, unsigned int key_length,
                const std::string prefix = "")
{
  std::vector<std::string> keys;

  while (num_keys--) {
    keys.push_back(prefix + RandomString(key_length));
  }

  return keys;
}

//------------------------------------------------------------------------------
// Pseudo random picker [0, lenght - 1]
//------------------------------------------------------------------------------
unsigned int RandomPick(unsigned int length)
{
  return (rand() % length);
}

//------------------------------------------------------------------------------
// Handle producer functionality
//------------------------------------------------------------------------------
void HandleProducer(unsigned int num_keys, unsigned int key_length,
                    unsigned int value_length, unsigned int batch_size,
                    unsigned long timeout_sec, UpdateType upd_type,
                    Logger* logger)
{
  using namespace std::chrono;
  srand(time(NULL));
  std::vector<std::string> keys = GenerateStrings(num_keys, key_length, "key_");
  std::vector<std::string> values = GenerateStrings(num_keys * 2, value_length,
                                    "val_");
  uint64_t count = 0ull;
  unsigned int tmp_batch = 0ul;
  auto start_ts = system_clock::now();
  auto deadline =  start_ts + seconds(timeout_sec);

  while (true) {
    if (timeout_sec && ((count & 0x03ff) == 0) &&
        (system_clock::now() > deadline)) {
      break;
    }

    eos::mq::SharedHashWrapper::Batch batch;
    tmp_batch = batch_size;

    while (tmp_batch--) {
      switch (upd_type) {
      case UpdateType::kLocal:
        batch.SetLocal(keys[RandomPick(keys.size())],
                       values[RandomPick(values.size())]);
        break;

      case UpdateType::kTransient:
        batch.SetTransient(keys[RandomPick(keys.size())],
                           values[RandomPick(values.size())]);
        break;

      case UpdateType::kPersistent:
        batch.SetDurable(keys[RandomPick(keys.size())],
                         values[RandomPick(values.size())]);
        break;

      default:
        throw std::runtime_error("unknown update type");
        break;
      }
    }

    gSharedHash->set(batch);
    ++count;
  }

  auto finish_ts = system_clock::now();
  auto duration_ms = duration_cast<milliseconds>(finish_ts - start_ts);
  // Compute some statistics and log summary
  std::ostringstream oss;
  oss << "INFO: Producer statistics tid=" << std::this_thread::get_id() << "\n"
      << "      Number of updates: " << count << "\n"
      << "      Update rate:       "
      << (1000.0 * count) / duration_ms.count() << " Hz";
  logger->log(oss.str());
}

//------------------------------------------------------------------------------
// Class Stats - collecting statistics about requests handled by the consumer
//------------------------------------------------------------------------------
class Stats
{
public:
  //------------------------------------------------------------------------------
  // Callback to be used by the subcription
  //------------------------------------------------------------------------------
  void Callback(qclient::SharedHashUpdate&& upd)
  {
    Collect();
  }

  //------------------------------------------------------------------------------
  // Trigger stats collection
  //------------------------------------------------------------------------------
  void Collect()
  {
    auto ts = std::chrono::seconds(std::time(nullptr)).count();
    std::unique_lock<std::mutex> lock(mMutex);
    mFreqMap[ts]++;
  }

  //------------------------------------------------------------------------------
  // Dump summary of the collected statistics
  //------------------------------------------------------------------------------
  void DumpSummary(Logger* logger)
  {
    uint64_t total = 0ull;
    double min_freq = 0, avg_freq = 0, max_freq = 0;
    {
      std::unique_lock<std::mutex> lock(mMutex);

      for (const auto& elem : mFreqMap) {
        total += elem.second;

        if (min_freq) {
          if (min_freq > elem.second) {
            min_freq = elem.second;
          }
        } else {
          min_freq = elem.second;
        }

        if (max_freq) {
          if (max_freq < elem.second) {
            max_freq = elem.second;
          }
        } else {
          max_freq = elem.second;
        }
      }

      if (mFreqMap.size() > 2) {
        avg_freq = total * 1.0 /
                   (mFreqMap.rbegin()->first - mFreqMap.begin()->first);
      }
    }
    std::ostringstream oss;
    oss << "info: consumer statistics\n"
        << " total upd: " << total << "\n"
        << " min freq:  " << min_freq << " Hz\n"
        << " avg freq:  " << avg_freq << " Hz\n"
        << " max freq:  " << max_freq << " Hz\n";
    logger->log(oss.str());
  }

  //------------------------------------------------------------------------------
  // Get timestamp of the last received request
  //------------------------------------------------------------------------------
  uint64_t GetLastTs() const
  {
    uint64_t ts = 0ull;
    std::unique_lock<std::mutex> lock(mMutex);

    if (!mFreqMap.empty()) {
      ts = mFreqMap.rbegin()->first;
    }

    return ts;
  }

private:
  mutable std::mutex mMutex;
  //! Map timestamp values to number of requests
  std::map<uint64_t, uint64_t> mFreqMap;
};

//------------------------------------------------------------------------------
// Register signal handler
//------------------------------------------------------------------------------
void ConsumerSignalHandler(int sig)
{
  (void) signal(SIGINT, SIG_IGN);
  std::cout << "info: calling signal handler" << std::endl;
  {
    std::unique_lock<std::mutex> lock(gMutexTerminate);
    gSignalTerminate = true;
  }
  gCvTerminate.notify_one();
}

//------------------------------------------------------------------------------
// Handle consumer functionality
//------------------------------------------------------------------------------
void HandleConsumer(unsigned long timeout_sec, Logger* logger)
{
  using std::placeholders::_1;
  Stats stats;
  std::unique_ptr<qclient::SharedHashSubscription> sub = gSharedHash->subscribe();
  sub->attachCallback(std::bind(&Stats::Callback, &stats, _1));

  while (true) {
    std::unique_lock<std::mutex> lock(gMutexTerminate);

    if (timeout_sec) {
      if (gCvTerminate.wait_for(lock, std::chrono::seconds(timeout_sec),
      [&]() {
      return gSignalTerminate;
    })) {
        //std::cout << "info: consumer signalled to terminate" << std::endl;
      } else {
        //std::cout << "info: consumer timeout expired" << std::endl;
      }
    } else {
      gCvTerminate.wait(lock, [&]() {
        return gSignalTerminate;
      });
    }

    break;
  }

  stats.DumpSummary(logger);
}

//------------------------------------------------------------------------------
// Given a sub-command, these are common options that need to be present
//------------------------------------------------------------------------------
void AddClusterOptions(CLI::App* subcmd, std::string& membersStr,
                       MemberValidator& memberValidator, std::string& password,
                       std::string& passwordFile, unsigned int& connection_retries,
                       unsigned long& timeout)
{
  subcmd->add_option("--members", membersStr,
                     "One or more members of the QDB cluster")
  ->required()
  ->check(memberValidator);
  subcmd->add_option("--timeout", timeout,
                     "Execution timeout - default infinite i.e 0");
  subcmd->add_option("--connection-retries", connection_retries,
                     "Number of connection retries - default infinite");
  auto passwordGroup = subcmd->add_option_group("Authentication",
                       "Specify QDB authentication options");
  passwordGroup->add_option("--password", password,
                            "The password for connecting to the QDB cluster - can be empty");
  passwordGroup->add_option("--password-file", passwordFile,
                            "The passwordfile for connecting to the QDB cluster - can be empty");
  passwordGroup->require_option(0, 1);
}

//------------------------------------------------------------------------------
// Main
//------------------------------------------------------------------------------
int main(int argc, char* argv[])
{
  Logger logger;
  CLI::App app("Tool to generate load for a SharedHash object stored in QDB");
  app.require_subcommand();
  // Basic parameters
  std::string members_input;
  MemberValidator member_validator;
  std::string password_data;
  std::string password_file;
  unsigned int connection_retries = 0;
  unsigned long timeout_sec = 0ul;
  // Producer subcommand
  auto producer_subcmd = app.add_subcommand("producer",
                         "Add producer that updates shared hash values");
  AddClusterOptions(producer_subcmd, members_input, member_validator,
                    password_data, password_file, connection_retries, timeout_sec);
  std::string hash_name {"hash-load-test"};
  producer_subcmd->add_option("--target-hash", hash_name,
                              "Target hash name")->default_val(hash_name);
  unsigned int num_keys {10};
  producer_subcmd->add_option("--num-keys", num_keys,
                              "Number of keys to target")->default_val(std::to_string(num_keys));
  unsigned int key_length {65};
  producer_subcmd->add_option("--key-length", key_length,
                              "Size of the keys")->default_val(std::to_string(key_length));
  unsigned int value_length {65};
  producer_subcmd->add_option("--value-length", value_length,
                              "Size of the values")->default_val(std::to_string(value_length));
  unsigned int concurrency {1};
  producer_subcmd->add_option("--concurrency", concurrency,
                              "Number of concurrent threads")->default_val(std::to_string(concurrency));
  unsigned int batch_size {1};
  producer_subcmd->add_option("--batch-upd-size", batch_size,
                              "Number of keys updated in one batch")->default_val(std::to_string(batch_size));
  // Lambda that converts string to internal update types
  auto upd_transformer = [](const std::string & input) -> std::string {
    if (input == "persistent")
    {
      return std::to_string(UpdateType::kPersistent);
    } else if (input == "transient")
    {
      return std::to_string(UpdateType::kTransient);
    } else if (input == "local")
    {
      return std::to_string(UpdateType::kLocal);
    } else
    {
      throw CLI::ValidationError(SSTR("unknown update type " << input));
    }
  };
  UpdateType upd_type = kLocal;
  producer_subcmd->add_option("--update-type", upd_type,
                              "Update type: persistent, transient or local")->transform(
                                upd_transformer)->default_str("local");
  // Consumer subcommand
  auto consumer_subcmd = app.add_subcommand("consumer",
                         "Add consumer of shared hash updates");
  AddClusterOptions(consumer_subcmd, members_input, member_validator,
                    password_data, password_file, connection_retries, timeout_sec);
  consumer_subcmd->add_option("--target-hash", hash_name,
                              "Target hash name")->default_val(hash_name);

  // Do the command line parsing
  try {
    app.parse(argc, argv);
  } catch (const CLI::ParseError& e) {
    std::cerr << "error: " << e.what() << std::endl;
    return app.exit(e);
  }

  // Handle password file option
  if (!password_file.empty()) {
    if (!PasswordHandler::readPasswordFile(password_file, password_data)) {
      std::cerr << "Could not read passwordfile: '" << password_file
                << "'. Ensure the file exists, and its permissions are 400."
                << std::endl;
      return 1;
    }
  }

  // Setup qclient to be used for QDB connection
  qclient::Members members = qclient::Members::fromString(members_input);
  eos::QdbContactDetails contact_details(members, password_data);
  qclient::Options opts = contact_details.constructOptions();

  if (connection_retries) {
    opts.retryStrategy = qclient::RetryStrategy::NRetries(connection_retries);
  }

  // Build the shared hash object
  std::unique_ptr<qclient::SharedManager> qsm {
    new qclient::SharedManager(contact_details.members,
                               contact_details.constructSubscriptionOptions())};
  gMsgRealm.reset(new eos::mq::MessagingRealm(qsm.get()));
  eos::common::SharedHashLocator hash_locator("dummy",
      SharedHashLocator::Type::kNode,
      hash_name);
  gSharedHash.reset(new eos::mq::SharedHashWrapper(gMsgRealm.get(),
                    hash_locator));

  if (producer_subcmd->parsed()) {
    std::cout << "info: handle producer" << std::endl;
    std::list<std::thread*> pool_threads;

    for (unsigned int i = 0; i < concurrency; ++i) {
      pool_threads.push_back(new std::thread(&HandleProducer, num_keys, key_length,
                                             value_length, batch_size, timeout_sec,
                                             upd_type, &logger));
    }

    for (auto* ptr_thread : pool_threads) {
      ptr_thread->join();
      delete ptr_thread;
    }
  } else if (consumer_subcmd->parsed()) {
    // Add signal handler for Control-C
    (void) signal(SIGINT, ConsumerSignalHandler);
    HandleConsumer(timeout_sec, &logger);
  }

  gSharedHash.reset();
  gMsgRealm.reset();
  qsm.reset();
  return 0;
}
