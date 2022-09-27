/* pure parser */
%define api.pure full
%locations

%define parse.error verbose

%define api.prefix {ttl}

%lex-param      { yyscan_t scanner }

%parse-param    { yyscan_t scanner }
%parse-param    { TurtleParseContext& context }
%parse-param    { std::vector<std::tuple<std::string, std::string, std::string>>& triples }



%{

#include <iostream>
#include <cstring>
#include <sstream>
#include <map>
#include <vector>
#include "turtle_parser.h"
#include "turtle_lexer.h"
#include "rdf_util.h"


struct TurtleParseState {
  yyscan_t scanner;
  YY_BUFFER_STATE buffer;
};

struct TurtleParseContext {
  std::string base;
  std::map<std::string, std::string> prefixes;
  std::map<std::string, std::string> blank_nodes;
  BlankNodeIdGenerator bknode_id_gen;
  std::vector<char*> subjects;
  std::vector<char*> predicates;

  TurtleParseContext() : bknode_id_gen(0) {
  }
};


int ttlerror(YYLTYPE* llocp, yyscan_t scanner, TurtleParseContext& context, std::vector<TurtleParser::Triple>& triples, const char *msg) {
  std::cerr << "Turtle Parsing Error: " << msg;
  std::cerr << " on line " << llocp->first_line+1 << ", column " << llocp->first_column << "." << std::endl;
  return 0;
}


%}



%code requires {

#include <cstdlib>
#include <string>
#include <vector>

#define yyconst const

#ifndef YY_TYPEDEF_YY_SCANNER_T
#define YY_TYPEDEF_YY_SCANNER_T
typedef void* yyscan_t;
#endif

#ifndef YY_TYPEDEF_YY_BUFFER_STATE
#define YY_TYPEDEF_YY_BUFFER_STATE
typedef struct yy_buffer_state *YY_BUFFER_STATE;
#endif

/* Size of default input buffer. */
#ifndef YY_BUF_SIZE
#ifdef __ia64__
/* On IA-64, the buffer size is 16k, not 8k.
 * Moreover, YY_BUF_SIZE is 2*YY_READ_BUF_SIZE in the general case.
 * Ditto for the __ia64__ case accordingly.
 */
#define YY_BUF_SIZE 32768
#else
#define YY_BUF_SIZE 16384
#endif /* __ia64__ */
#endif


struct TurtleParseContext;

}


%union {
  char* strval;
}

/* Keywords */
%token BASE SPARQL_BASE PREFIX SPARQL_PREFIX
%token BOOL_TRUE BOOL_FALSE
%token END_OF_FILE

%token <strval> DECIMAL DOUBLE INTEGER
%token <strval> IRI IDENTIFIER STRING
%token <strval> LANGTAG

%token TYPE ANON


%type <strval> prefix_id
%type <strval> literal rdf_literal numeric_literal boolean_literal
%type <strval> iri blank_node type
%type <strval> collection item_list item_list_not_empty
%type <strval> predicate object
%type <strval> blank_node_property_list


%destructor { free($$); } <strval>

%start statement


%code provides {

#define YYSTYPE         TTLSTYPE
#define YYLTYPE         TTLLTYPE

YY_BUFFER_STATE ttl_scan_string (yyconst char *str, yyscan_t scanner);
YY_BUFFER_STATE ttl_create_buffer(FILE *file, int size, yyscan_t scanner);
void ttl_switch_to_buffer(YY_BUFFER_STATE new_buffer, yyscan_t scanner);
void ttl_delete_buffer (YY_BUFFER_STATE buf, yyscan_t scanner);

int ttllex_init (yyscan_t* scanner);
int ttllex_destroy (yyscan_t scanner);
int ttllex(YYSTYPE * yylval, YYLTYPE * yylloc, yyscan_t scanner);

}


%%

/* 2 */
statement:
  directive { YYACCEPT; }
  |
  triples { YYACCEPT; }
  |
  END_OF_FILE { YYABORT; }
  ;


/* 3 */
directive:
  prefix
  |
  base
  ;


/* 4 */
prefix:
  PREFIX prefix_id ':' IRI '.' {
    context.prefixes[$2] = $4;
    free($2);
    $2 = nullptr;
    free($4);
    $4 = nullptr;
  }
  |
  SPARQL_PREFIX prefix_id ':' IRI {
    context.prefixes[$2] = $4;
    free($2);
    $2 = nullptr;
    free($4);
    $4 = nullptr;
  }
  ;

prefix_id:
  IDENTIFIER { $$ = $1; }
  |
  /* empty */ { $$ = strdup(""); }
  ;

/* 5 */
base:
  BASE IRI '.'  { context.base = $2; free($2); $2 = nullptr; }
  |
  SPARQL_BASE IRI  { context.base = $2; free($2); $2 = nullptr; }
  ;

/* 6 */
triples:
  subject1 predicate_object_list '.' {
    char* subject = context.subjects.back();
    free(subject);
    context.subjects.pop_back();
  }
  |
  subject2 predicate_object_list '.' {
    char* subject = context.subjects.back();
    free(subject);
    context.subjects.pop_back();
  }
  |
  subject2 '.' {
    char* subject = context.subjects.back();
    free(subject);
    context.subjects.pop_back();
  }
  ;

/* 7 */
predicate_object_list:
  verb object_list {
    char* predicate = context.predicates.back();
    free(predicate);
    context.predicates.pop_back();
  }
  |
  predicate_object_list ';' verb object_list {
    char* predicate = context.predicates.back();
    free(predicate);
    context.predicates.pop_back();
  }
  ;

/* 8 */
object_list:
  object {
    triples.push_back(std::make_tuple(context.subjects.back(), context.predicates.back(), $1));
    free($1);
    $1 = nullptr;
  }
  |
  object_list ',' object {
    triples.push_back(std::make_tuple(context.subjects.back(), context.predicates.back(), $3));
    free($3);
    $3 = nullptr;
  }
  ;


/* 9 */
verb:
  predicate { context.predicates.push_back($1); }
  |
  type { context.predicates.push_back($1); }
  ;

/* 10 */
subject1:
  iri { context.subjects.push_back($1); }
  |
  blank_node { context.subjects.push_back($1); }
  |
  collection { context.subjects.push_back($1); }
  ;

subject2:
  blank_node_property_list { context.subjects.push_back($1); }
  ;

/* 11 */
predicate:
  iri { $$ = $1; }
  ;

type:
  IDENTIFIER {
    if(strcmp($1, "a") != 0){
      yyerror (&yylloc, scanner, context, triples, YY_("syntax error, unexpected IDENTIFIER, expecting ':' or 'a'"));
      YYERROR;
    }
    free($1);
    $1 = nullptr;
    $$ = strdup(RDFVocabulary::RDF_TYPE.c_str());
  }
  ;

/* 12 */
object:
  iri { $$ = $1; }
  |
  literal { $$ = $1; }
  |
  blank_node { $$ = $1; }
  |
  blank_node_property_list { $$ = $1; }
  |
  collection { $$ = $1; }
  ;

/* 14 */
blank_node_property_list:
  open_square_bracket predicate_object_list ']' {
    char *blankNode = context.subjects.back();
    context.subjects.pop_back();
    $$ = blankNode;
  }
  ;

open_square_bracket:
  '[' {
    std::string bknode_id = context.bknode_id_gen.generate();
    context.subjects.push_back(strdup(bknode_id.c_str()));
  }
  ;

blank_node:
  '_' ':' IDENTIFIER {
    std::string bknode_id;
    if(context.blank_nodes.count($3)) {
      bknode_id = context.blank_nodes[$3];
    }
    else {
      bknode_id = context.bknode_id_gen.generate();
      context.blank_nodes[$3] = bknode_id;
    }
    free($3);
    $3 = nullptr;
    $$ = strdup(bknode_id.c_str());
  }
  |
  ANON {
    std::string bknode_id = context.bknode_id_gen.generate();
    $$ = strdup(bknode_id.c_str());
  }
  ;

/* 15 */
collection:
  '(' item_list ')' { $$ = $2; }
  ;

item_list:
  item_list_not_empty {
    triples.push_back(std::make_tuple(context.subjects.back(), RDFVocabulary::RDF_REST, RDFVocabulary::RDF_NIL));
    free(context.subjects.back());
    context.subjects.pop_back();
    $$ = $1;
  }
  |
  {
    std::string bknode_id = context.bknode_id_gen.generate();
    char* item = strdup(bknode_id.c_str());
    triples.push_back(std::make_tuple(item, RDFVocabulary::RDF_FIRST, RDFVocabulary::RDF_NIL));
    triples.push_back(std::make_tuple(item, RDFVocabulary::RDF_REST, RDFVocabulary::RDF_NIL));
    $$ = item;
  }
  ;

item_list_not_empty:
  object {
    std::string bknode_id = context.bknode_id_gen.generate();
    char* item = strdup(bknode_id.c_str());
    triples.push_back(std::make_tuple(item, RDFVocabulary::RDF_FIRST, $1));
    free($1);
    context.subjects.push_back(strdup(bknode_id.c_str()));
    $$ = item;
  }
  |
  item_list_not_empty object {
    std::string bknode_id = context.bknode_id_gen.generate();
    char* item = strdup(bknode_id.c_str());
    triples.push_back(std::make_tuple(context.subjects.back(), RDFVocabulary::RDF_REST, item));
    triples.push_back(std::make_tuple(item, RDFVocabulary::RDF_FIRST, $2));
    free($2);
    free(context.subjects.back());
    context.subjects.pop_back();
    context.subjects.push_back(item);
    $$ = $1;
  }
  ;

/* 13 */
literal:
  rdf_literal { $$ = $1; }
  |
  numeric_literal { $$ = $1; }
  |
  boolean_literal { $$ = $1; }
  ;


/* 16 */
numeric_literal:
  INTEGER {
    std::stringstream ss;
    ss << "\"" << $1 << "\"" << "^^" << XSDVocabulary::XSD_INTEGER;
    $$ = strdup(ss.str().c_str());
  }
  |
  DECIMAL {
    std::stringstream ss;
    ss << "\"" << $1 << "\"" << "^^" << XSDVocabulary::XSD_DECIMAL;
    $$ = strdup(ss.str().c_str());
  }
  |
  DOUBLE {
    std::stringstream ss;
    ss << "\"" << $1 << "\"" << "^^" << XSDVocabulary::XSD_DOUBLE;
    $$ = strdup(ss.str().c_str());
  }
  ;

rdf_literal:
  STRING {
    std::stringstream ss;
    ss << "\"" << $1 << "\"" << "^^" << XSDVocabulary::XSD_STRING;
    free($1);
    $1 = nullptr;
    $$ = strdup(ss.str().c_str());
  }
  |
  STRING LANGTAG {
    std::stringstream ss;
    ss << "\"" << $1 << "\"" << $2;
    free($1);
    $1 = nullptr;
    free($2);
    $2 = nullptr;
    $$ = strdup(ss.str().c_str());
  }
  |
  STRING TYPE iri {
    std::stringstream ss;
    ss << "\"" << $1 << "\"" << "^^" << $3;
    free($1);
    $1 = nullptr;
    free($3);
    $3 = nullptr;
    $$ = strdup(ss.str().c_str());
  }
  ;

boolean_literal:
  BOOL_TRUE {
    std::stringstream ss;
    ss << "\"true\"" << "^^" << XSDVocabulary::XSD_BOOLEAN;
    $$ = strdup(ss.str().c_str());
  }
  |
  BOOL_FALSE {
    std::stringstream ss;
    ss << "\"false\"" << "^^" << XSDVocabulary::XSD_BOOLEAN;
    $$ = strdup(ss.str().c_str());
  }
  ;

iri:
  IRI {
    std::string str = $1;
    if(!context.base.empty() && str.find("://")==std::string::npos){
      str.insert(str.begin()+1, context.base.begin()+1, context.base.end()-1);
    }
    free($1);
    $1 = nullptr;
    $$ = strdup(str.c_str());
  }
  |
  IDENTIFIER ':' IDENTIFIER {
    std::string str = context.prefixes[$1];
    str.insert(str.length()-1, $3, strlen($3));
    free($1);
    $1 = nullptr;
    free($3);
    $3 = nullptr;
    $$ = strdup(str.c_str());
  }
  |
  ':' IDENTIFIER{
    std::string str = context.prefixes[""];
    str.insert(str.length()-1, $2, strlen($2));
    free($2);
    $2 = nullptr;
    $$ = strdup(str.c_str());
  }
  ;

%%



TurtleParser::TurtleParser(FILE * input_file) : input_file(input_file) {
  state = new TurtleParseState();
  context = new TurtleParseContext();
  ttllex_init(&state->scanner);
  state->buffer = ttl_create_buffer(this->input_file, YY_BUF_SIZE, state->scanner);
  ttl_switch_to_buffer(state->buffer, state->scanner);
  current_read_pos = 0;
}

TurtleParser::TurtleParser(const std::string& input_str) : input_file(NULL) {
  state = new TurtleParseState();
  context = new TurtleParseContext();
  ttllex_init(&state->scanner);
  state->buffer = ttl_scan_string(input_str.c_str(), state->scanner);
  current_read_pos = 0;
}

TurtleParser::~TurtleParser() {
  ttl_delete_buffer(state->buffer, state->scanner);
  ttllex_destroy(state->scanner);
  delete state;
  delete context;
}

bool TurtleParser::parse(std::string& subject, std::string& predicate, std::string& object) {
  if(current_read_pos >= triples.size()) {
    current_read_pos = 0;
    triples.clear();
    while(!triples.size()) {
      if(ttlparse(state->scanner, *context, triples) != 0) {
        return false;
      }
    }
  }
  subject = std::get<0>(triples[current_read_pos]);
  predicate = std::get<1>(triples[current_read_pos]);
  object = std::get<2>(triples[current_read_pos]);
  current_read_pos++;
  return true;
}

TurtleFileParser::TurtleFileParser(const std::string& file_path) : TurtleParser(fopen(file_path.c_str(), "r")) {}

TurtleFileParser::~TurtleFileParser() {
  fclose(input_file);
}
