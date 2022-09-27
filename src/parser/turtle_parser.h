#ifndef TURTLE_PARSER_H
#define TURTLE_PARSER_H

#include <cstdio>
#include <cstdint>
#include <string>
#include <tuple>
#include <vector>


struct TurtleParseState;
struct TurtleParseContext;

class TurtleParser {
public:
  typedef std::tuple<std::string, std::string, std::string> Triple;

  TurtleParser(const std::string& input_str);
  virtual ~TurtleParser();

  bool parse(std::string& subject, std::string& predicate, std::string& object);

protected:
  TurtleParser(FILE * input_file);
  TurtleParseState* state;
  TurtleParseContext* context;

  std::vector<Triple> triples;
  int current_read_pos;

  FILE * input_file;
};

class TurtleFileParser : public TurtleParser {
public:
  TurtleFileParser(const std::string& file_path);
  ~TurtleFileParser();
};


#endif
