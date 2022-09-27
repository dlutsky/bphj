#include <iostream>
#include <sstream>
#include <chrono>
#include <ratio>
#include "command.h"
#include "database/config.h"
#include "database/database.h"
#include "database/database_builder.h"
#include "query/query_graph.h"
#include "parser/sparql_parser.h"
#include "util/string_util.h"
#include "util/file_directory.h"


/* CommandBuilder */

Command CommandBuilder::createByName(const std::string& exec_name, const std::string& cmd_name) {
  if(CreateCommand::NAME == cmd_name) {
    return std::unique_ptr<CreateCommand>(new CreateCommand(exec_name));
  }
  else if(LoadCommand::NAME == cmd_name) {
    return std::unique_ptr<LoadCommand>(new LoadCommand(exec_name));
  }
  else if(QueryCommand::NAME == cmd_name) {
    return std::unique_ptr<QueryCommand>(new QueryCommand(exec_name));
  }
  std::stringstream ss;
  ss << cmd_name << ": command not found";
  CommandBase::printError(exec_name, ss.str());
  return nullptr;
}

Command CommandBuilder::parse(int argc, char **argv) {
  std::vector<std::string> tokens;
  std::map<std::string , std::string> options;
  for(int i=1; i<argc; i++) {
    if(argv[i][0] == '-' && argv[i][1] == '-' ) {
      std::vector<std::string> splits = StringUtil::split(argv[i], '=');
      std::string option = splits[0].substr(2);
      if(splits.size() > 1) {
        options[option] = splits[1];
      }
      else {
        options[option] = "";
      }
    }
    else {
      tokens.push_back(argv[i]);
    }
  }
  if(options.count("help")) {
    if(tokens.empty()) {
      return std::unique_ptr<HelpCommand>(new HelpCommand(argv[0]));
    }
    return std::unique_ptr<HelpCommand>(new HelpCommand(argv[0], tokens[0]));
  }
  if(tokens.empty()) {
    CommandBase::printError(argv[0], "no command specified");
    return nullptr;
  }
  Command cmd = createByName(argv[0], tokens[0]);
  if(cmd) {
    cmd->params.assign(tokens.begin() + 1, tokens.end());
    cmd->options = options;
  }
  return cmd;
}


/* CommandBase */

CommandBase::CommandBase(const std::string& exec_name) : exec_name(exec_name) {}

CommandBase::CommandBase(const std::string& exec_name, const std::string& cmd_name)
  : exec_name(exec_name), cmd_name(cmd_name) {}

void CommandBase::printError(const std::string& exec_name, const std::string& errorMsg) {
  std::cerr << "\e[1m" << exec_name << ":\e[0m "
            << "\033[1;31merror:\033[0m " << errorMsg
            << std::endl;
}

/* CreateCommand */

const std::string CreateCommand::NAME = "create";

CreateCommand::CreateCommand(const std::string& exec_name) : CommandBase(exec_name) {
  std::stringstream ss;
  ss << exec_name << " " << CreateCommand::NAME;
  cmd_name = ss.str();
}

void CreateCommand::run() {
  if(params.size() < 1) {
    printUsage(exec_name);
    return;
  }
  Database::create(params[0]);
}

void CreateCommand::printUsage(const std::string& exec_name)
{
  std::cout << "Usage: " << exec_name << " " << NAME << " [options] <dbname>\n"
            << std::endl
            << "Options:\n"
            << "\t--help\t\tShow this help mesage for create command\n"
            << std::endl;
}

/* LoadCommand */

const std::string LoadCommand::NAME = "load";

LoadCommand::LoadCommand(const std::string& exec_name) : CommandBase(exec_name) {
  std::stringstream ss;
  ss << exec_name << " " << LoadCommand::NAME;
  cmd_name = ss.str();
}

void LoadCommand::run() {
  if(params.size() < 2) {
    printError(cmd_name, "ss");
    printUsage(exec_name);
    return;
  }
  if(options.count("config-file")) {
    Config::loadConfig(options["config-file"]);
  }
  DatabaseBuilder db_builder(params[0]);
  std::vector<std::string> rdf_files;
  if(!parseFileArguments(rdf_files)) {
    exit(1);
  }
  if(rdf_files.empty()) {
    std::cerr << "Cannot find RDF files" << std::endl;
    exit(1);
  }
  std::cout<<"Loading RDF data files into database "<<params[0]<<" ..."<<std::endl;
  if(!db_builder.buildFromRDFFiles(rdf_files)) {
    std::cerr << "\e[1m" << exec_name << ":\e[0m "
              << "\033[1;31merror:\033[0m " << "Failed to load from RDF files"
              << std::endl;
    exit(1);
  }
  std::cout<<"Load succeed."<<std::endl;
}

void LoadCommand::printUsage(const std::string& exec_name)
{
  std::cout << "Usage: " << exec_name << " " << NAME << " [options] <dbname> <file>*\n"
            << std::endl
            << "Options:\n"
            << "\t--config-file=<configfile>\t\tSpecify config file name\n"
            << "\t--help\t\tShow this help mesage for load command\n"
            << std::endl;
}

bool LoadCommand::parseFileArguments(std::vector<std::string>& rdf_files) {
  for(int i=1; i<params.size(); i++) {
    if(Directory::exist(params[i])) {
      Directory::getFiles(params[i], rdf_files);
      continue;
    }
    if(!File::exist(params[i])) {
      std::size_t found = params[i].find_last_of("/\\");
      Directory::getFiles(params[i].substr(0, found), rdf_files, params[i].substr(found));
      continue;
    }
    rdf_files.push_back(params[i]);
  }
  return true;
}

/* QueryCommand */

const std::string QueryCommand::NAME = "query";

QueryCommand::QueryCommand(const std::string& exec_name) : CommandBase(exec_name) {
  std::stringstream ss;
  ss << exec_name << " " << QueryCommand::NAME;
  cmd_name = ss.str();
}

void QueryCommand::run() {
  if(options.count("config-file")) {
    Config::loadConfig(options["config-file"]);
  }
  Database db(params[0]);
  db.open();

  if(params.size() < 1) {
    printUsage(exec_name);
    db.close();
    return;
  }

  std::vector<std::string> rq_files;
  if(options.count("input-file")) {
    if(!parseFileArguments(options["input-file"], rq_files)) {
      std::cout<<"invalid input file"<<std::endl;
      db.close();
      return;
    }
    for(std::vector<std::string>::iterator it = rq_files.begin(), end = rq_files.end(); it != end; ++it) {
      SPARQLFileParser rq_parser(*it);
      while(true) {
        QueryGraph query_graph;
        if(!rq_parser.parse(&query_graph)) {
          db.close();
          return;
        }

        bool explain =false;
        if(options.count("explain") && options["explain"] == "true") {
          explain = true;
        }
        bool silent =false;
        if(options.count("show-results") && options["show-results"] == "false") {
          silent = true;
        }

        auto start = std::chrono::high_resolution_clock::now();
        if(options.count("output-file")) {
          db.executeQuery(&query_graph, options["output-file"], explain, silent);
        }
        else {
          db.executeQuery(&query_graph, explain, silent);
        }
        auto done = std::chrono::high_resolution_clock::now();
        double exec_time = std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(done-start).count();
        std::cout << std::endl << "Execution time: " << exec_time << " ms" << std::endl;
      }
    }
    db.close();
    return;
  }

  while(true) {
    std::cout<<">>";
    std::string input;
    getline(std::cin, input);
    if(input == "exit") {
      break;
    }
    if(input == "") {
      continue;
    }
    SPARQLFileParser rq_parser(input);
    QueryGraph query_graph;
    if(!rq_parser.parse(&query_graph)) {
      std::cout<<"invalid query"<<std::endl;
      continue;
    }
    bool explain =false;
    if(options.count("explain") && options["explain"] == "true") {
      explain = true;
    }
    bool silent =false;
    if(options.count("show-results") && options["show-results"] == "false") {
      silent = true;
    }

    auto started = std::chrono::high_resolution_clock::now();
    if(options.count("output-file")) {
      db.executeQuery(&query_graph, options["output-file"], explain, silent);
    }
    else {
      db.executeQuery(&query_graph, explain, silent);
    }
    auto done = std::chrono::high_resolution_clock::now();
    double exec_time = std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(done-started).count();
    std::cout << std::endl << "Execution time: " << exec_time << " ms" << std::endl;
  }

  db.close();
}

void QueryCommand::printUsage(const std::string& exec_name)
{
  std::cout << "Usage: " << exec_name << " " << NAME << " [options] <dbname> <queryfile>*\n"
            << std::endl
            << "Options:\n"
            << "\t--config-file=<configfile>\t\tSpecify config file name\n"
            << "\t--input-file=<infile>\t\tSpecify input file name\n"
            << "\t--output-file=<outfile>\t\tSpecify output file name\n"
            << "\t--help\t\t\t\tShow this help mesage for query command\n"
            << std::endl;
}

bool QueryCommand::parseFileArguments(std::string file_path, std::vector<std::string>& rq_files) {
  if(!File::exist(file_path)) {
    std::size_t found = file_path.find_last_of("/\\");
    Directory::getFiles(file_path.substr(0, found), rq_files, file_path.substr(found));
  } else {
    rq_files.push_back(file_path);
  }
  return true;
}

/* HelpCommand */

HelpCommand::HelpCommand(const std::string& exec_name) : CommandBase(exec_name) {}

HelpCommand::HelpCommand(const std::string& exec_name, const std::string& cmd_name)
  : CommandBase(exec_name, cmd_name) {}

void HelpCommand::run() {
  if(cmd_name.empty()) {
    printUsage(exec_name);
    return;
  }
  if(CreateCommand::NAME == cmd_name) {
    CreateCommand::printUsage(exec_name);
  }
  else if(LoadCommand::NAME == cmd_name) {
    LoadCommand::printUsage(exec_name);
  }
  else if(QueryCommand::NAME == cmd_name) {
    QueryCommand::printUsage(exec_name);
  }
  else {
    printUsage(exec_name);
  }
}

void HelpCommand::printUsage(const std::string& exec_name)
{
  std::cout << "Usage: " << exec_name << " <command> [options]\n"
            << std::endl
            << "Available Commands:\n"
            << "\tcreate\t\tCreate a new database\n"
            << "\tload\t\tLoad RDF data into a speicific database\n"
            << "\tquery\t\tRun SPARQL Queries\n"
            << std::endl
            << "Options:\n"
            << "\t--help\t\tShow this help mesage\n"
            << std::endl;
}
