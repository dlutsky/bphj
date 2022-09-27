#ifndef COMMAND_H
#define COMMAND_H

#include <vector>
#include <map>
#include <memory>


class CommandBase;
typedef std::unique_ptr<CommandBase> Command;


class CommandBuilder {
public:
  static Command parse(int argc, char **argv);
protected:
  static Command createByName(const std::string& exec_name, const std::string& cmd_name);
};

class CommandBase {
public:
  CommandBase(const std::string& exec_name);
  CommandBase(const std::string& exec_name, const std::string& cmd_name);
  virtual void run() = 0;

  static void printError(const std::string& exec_name, const std::string& errorMsg);

protected:
  friend class CommandBuilder;

  std::string exec_name;
  std::string cmd_name;
  std::vector<std::string> params;
  std::map<std::string , std::string> options;
};

class CreateCommand : public CommandBase {
public:
  const static std::string NAME;

  CreateCommand(const std::string& exec_name);
  virtual void run() override;

  static void printUsage(const std::string& exec_name);
};

class LoadCommand : public CommandBase {
public:
  const static std::string NAME;

  LoadCommand(const std::string& exec_name);
  virtual void run() override;

  static void printUsage(const std::string& exec_name);

protected:
  bool parseFileArguments(std::vector<std::string>& rdf_files);
};

class QueryCommand : public CommandBase {
public:
  const static std::string NAME;

  QueryCommand(const std::string& exec_name);
  virtual void run() override;

  static void printUsage(const std::string& exec_name);

protected:
  bool parseFileArguments(std::string file_path, std::vector<std::string>& rq_files);
};

class HelpCommand : public CommandBase {
public:
  HelpCommand(const std::string& exec_name);
  HelpCommand(const std::string& exec_name, const std::string& cmd_name);
  virtual void run() override;

  static void printUsage(const std::string& exec_name);
};



#endif
