#ifndef RDF_UTIL
#define RDF_UTIL

#include <string>
#include <sstream>


class RDFVocabulary {
public:
  const static std::string RDF_TYPE;
  const static std::string RDF_FIRST;
  const static std::string RDF_REST;
  const static std::string RDF_NIL;
};

class XSDVocabulary {
public:
  const static std::string XSD_STRING;
  const static std::string XSD_BOOLEAN;
  const static std::string XSD_INTEGER;
  const static std::string XSD_DECIMAL;
  const static std::string XSD_FLOAT;
  const static std::string XSD_DOUBLE;
  const static std::string XSD_DURATION;
  const static std::string XSD_DATETIME;
  const static std::string XSD_TIME;
  const static std::string XSD_DATE;
};

class BlankNodeIdGenerator {
public:
  BlankNodeIdGenerator();
  BlankNodeIdGenerator(int init_no);
  ~BlankNodeIdGenerator();

  std::string generate();

private:
  const static std::string DEFAULT_PREFIX;
  int seq_no;
};


#endif
