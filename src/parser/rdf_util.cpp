#include "rdf_util.h"


const std::string RDFVocabulary::RDF_TYPE = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>";
const std::string RDFVocabulary::RDF_FIRST = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#first>";
const std::string RDFVocabulary::RDF_REST = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#rest>";
const std::string RDFVocabulary::RDF_NIL = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>";


const std::string XSDVocabulary::XSD_STRING = "<http://www.w3.org/2001/XMLSchema#string>";
const std::string XSDVocabulary::XSD_BOOLEAN = "<http://www.w3.org/2001/XMLSchema#boolean>";
const std::string XSDVocabulary::XSD_INTEGER = "<http://www.w3.org/2001/XMLSchema#integer>";
const std::string XSDVocabulary::XSD_DECIMAL = "<http://www.w3.org/2001/XMLSchema#decimal>";
const std::string XSDVocabulary::XSD_FLOAT = "<http://www.w3.org/2001/XMLSchema#float>";
const std::string XSDVocabulary::XSD_DOUBLE = "<http://www.w3.org/2001/XMLSchema#double>";
const std::string XSDVocabulary::XSD_DURATION = "<http://www.w3.org/2001/XMLSchema#duration>";
const std::string XSDVocabulary::XSD_DATETIME = "<http://www.w3.org/2001/XMLSchema#datetime>";
const std::string XSDVocabulary::XSD_TIME = "<http://www.w3.org/2001/XMLSchema#time>";
const std::string XSDVocabulary::XSD_DATE = "<http://www.w3.org/2001/XMLSchema#date>";


BlankNodeIdGenerator::BlankNodeIdGenerator() : seq_no(0) {}

BlankNodeIdGenerator::BlankNodeIdGenerator(int init_no) : seq_no(init_no) {}

BlankNodeIdGenerator::~BlankNodeIdGenerator() {}

const std::string BlankNodeIdGenerator::DEFAULT_PREFIX = "_:";

std::string BlankNodeIdGenerator::generate() {
  std::stringstream ss;
  ss << DEFAULT_PREFIX << seq_no;
  seq_no++;
  return ss.str();
}
