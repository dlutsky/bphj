/* pure parser */
%define api.pure full
%locations

%define parse.error verbose

%define api.prefix {rq}


%lex-param      { yyscan_t scanner }

%parse-param    { yyscan_t scanner }
%parse-param    { SPARQLParseContext* context }
%parse-param    { QueryGraph* query_graph }




%{

#include <iostream>
#include <cstring>
#include <sstream>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "sparql_parser.h"
#include "sparql_lexer.h"
#include "rdf_util.h"
#include "util/memory_pool.h"


struct SPARQLParseState {
  yyscan_t scanner;
  YY_BUFFER_STATE buffer;
};

struct SPARQLParseContext {
  std::string base;
  std::unordered_map<std::string, std::string> prefixes;
  std::unordered_set<std::string> variables;
  bool all_variables;
  std::unordered_map<std::string, std::string> blank_nodes;
  BlankNodeIdGenerator bknode_id_gen;
  MemoryPool<QueryResource> resource_pool;
  std::vector<QueryResource*> subjects;
  std::vector<QueryResource*> predicates;
  QueryPattern* pattern;
  QueryPattern* parent_pattern;

  SPARQLParseContext() : bknode_id_gen(0) {
    pattern = nullptr;
    parent_pattern = nullptr;
  }
  ~SPARQLParseContext() {
    pattern = nullptr;
    parent_pattern = nullptr;
  }
};


int rqerror(YYLTYPE* llocp, yyscan_t scanner, SPARQLParseContext* context, QueryGraph* query_graph, const char *msg) {
  std::cerr << "SPARQL Parsing Error: " << msg;
  std::cerr << " on line " << llocp->first_line+1 << ", column " << llocp->first_column << "." << std::endl;
  return 0;
}


%}

%code requires {

#include <cstdlib>
#include <string>
#include "query/query_graph.h"

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

void yyunget (yyscan_t yyscanner);

struct SPARQLParseContext;

}



%union {
  char* strval;
  QueryResource *resource;
  QueryExpression *expression;
  QueryPattern *pattern;
}


/* Keywords */
%token BASE PREFIX SELECT CONSTRUCT ASK DESCRIBE
%token ORDER BY ASC DESC
%token DISTINCT REDUCED OFFSET LIMIT
%token FROM NAMED WHERE
%token UNION FILTER GRAPH OPTIONAL
%token IN NOT EXISTS MINUS BOOL_TRUE BOOL_FALSE
%token BIC_BOUND BIC_STR BIC_LANG BIC_DATATYPE BIC_SAMETERM BIC_LANGMATCHES
%token BIC_ISIRI BIC_ISURI BIC_ISBLANK BIC_ISLITERAL BIC_REGEX
%token END_OF_FILE

%token OP_OR OP_AND OP_EQ OP_NE OP_LT OP_GT OP_LE OP_GE
%token TYPE ANON NIL


%token <strval> DECIMAL DOUBLE INTEGER
%token <strval> VARIABLE IRI IDENTIFIER STRING
%token <strval> LANGTAG


%type <strval> prefix
%type <strval> literal rdf_literal numeric_literal boolean_literal iri
%type <resource> object
%type <resource> blank_node_property_list
%type <resource> var_element iri_element literal_element blank_node_element type_element
%type <expression> constraint built_in_call function_call
%type <expression> expression bracketted_expression expression_list
%type <expression> conditional_expression relation_expression primary_expression
%type <expression> unary_expression numeric_expression
%type <pattern> group_graph_pattern graph_pattern construct_template


%left OP_OR
%left OP_AND
%left OP_EQ OP_NE OP_LT OP_GT OP_LE OP_GE
%left '+' '-'
%left '*' '/'
%right UMINUS
%left '(' ')'

%destructor { free($$); } <strval>

%start query



%code provides {

#define YYSTYPE         RQSTYPE
#define YYLTYPE         RQLTYPE

YY_BUFFER_STATE rq_scan_string (yyconst char *str, yyscan_t scanner);
YY_BUFFER_STATE rq_create_buffer(FILE *file, int size, yyscan_t scanner);
void rq_switch_to_buffer(YY_BUFFER_STATE new_buffer, yyscan_t scanner);
void rq_delete_buffer (YY_BUFFER_STATE buf, yyscan_t scanner);

int rqlex_init (yyscan_t* scanner);
int rqlex_destroy (yyscan_t scanner);
int rqlex(YYSTYPE * yylval, YYLTYPE * yylloc, yyscan_t scanner);

}



%%

/* 1 */
query:
  prologue select_query { YYACCEPT; }
  |
  prologue construct_query { YYACCEPT; }
  |
  prologue describe_query { YYACCEPT; }
  |
  prologue ask_query { YYACCEPT; }
  |
  END_OF_FILE { YYABORT; }
  ;

/* 2 */
prologue:
  base_declaration prefix_declaration
  ;

/* 3 */
base_declaration:
  BASE IRI { context->base = $2; free($2); $2 = nullptr; }
  |
  ;

/* 4 */
prefix_declaration:
  prefix_list
  |
  ;

prefix_list:
  PREFIX prefix ':' IRI {
    context->prefixes[$2] = $4;
    free($2);
    $2 = nullptr;
    free($4);
    $4 = nullptr;
  }
  |
  prefix_list PREFIX prefix ':' IRI {
    context->prefixes[$3] = $5;
    free($3);
    $3 = nullptr;
    free($5);
    $5 = nullptr;
  }
  ;

prefix:
  IDENTIFIER { $$ = $1; }
  |
  /* empty */ { $$ = strdup(""); }
  ;

/* 5 */
select_query:
  SELECT duplicate_modifier select_list dataset_clause where_clause solution_modifier {
    query_graph->setQueryForm(SelectQuery);
  }
  ;

duplicate_modifier:
  DISTINCT { query_graph->setDuplicateModifier(Distinct); }
  |
  REDUCED { query_graph->setDuplicateModifier(Reduced); }
  |
  /* empty */
  ;

select_list:
  variables
  |
  '*' { context->all_variables = true; }
  ;

variables:
  VARIABLE {
    query_graph->addProjection($1);
    free($1);
    $1 = nullptr;
  }
  |
  variables VARIABLE {
    query_graph->addProjection($2);
    free($2);
    $2 = nullptr;
  }
  ;

/* 6 */
construct_query:
  CONSTRUCT construct_template dataset_clause where_clause solution_modifier {
    query_graph->setQueryForm(ConstructQuery);
  }
  ;

/* 7 */
describe_query:
  DESCRIBE describe_list dataset_clause where_clause {
    query_graph->setQueryForm(DescribeQuery);
  }
  ;

describe_list:
  var_or_iri_list
  |
  '*' { context->all_variables = true; }
  ;

var_or_iri_list:
  VARIABLE { free($1); $1 = nullptr; }
  |
  iri { free($1); $1 = nullptr; }
  |
  var_or_iri_list VARIABLE { free($2); $2 = nullptr; }
  |
  var_or_iri_list iri { free($2); $2 = nullptr; }
  ;

/* 8 */
ask_query:
  ASK dataset_clause where_clause {
    query_graph->setQueryForm(AskQuery);
  }
  ;


/* 9 */
dataset_clause:
  from_clause
  |
  ;

from_clause:
  FROM {
    yyerror (&yylloc, scanner, context, query_graph, YY_("syntax error, FROM clause is currently not supported, expecting ':' or 'a'"));
    YYERROR;
  }
  ;

/* 13 */
where_clause:
  WHERE group_graph_pattern { query_graph->pattern = $2; }
  |
  group_graph_pattern { query_graph->pattern = $1; }
  ;

/* 14 */
solution_modifier:
  order_clause limit_offset_clause
  |
  limit_offset_clause
  |
  /* empty */ { yyunget(scanner); }
  ;

/* 15 */
limit_offset_clause:
  limit_clause
  |
  offset_clause
  |
  limit_clause offset_clause
  |
  offset_clause limit_clause
  ;

/* 16 */
order_clause:
  ORDER BY order_conditions
  ;

order_conditions:
  order_condition
  |
  order_conditions order_condition

/* 17 */
order_condition:
  ASC bracketted_expression {
    Order order;
    order.ascending = true;
    order.condition = $2;
    query_graph->addOrder(order);
  }
  |
  DESC bracketted_expression {
    Order order;
    order.ascending = false;
    order.condition = $2;
    query_graph->addOrder(order);
  }
  |
  constraint {
    Order order;
    order.ascending = true;
    order.condition = $1;
    query_graph->addOrder(order);
  }
  |
  VARIABLE {
    Order order;
    order.ascending = true;
    order.condition = query_graph->createExpression();
    order.condition->type = QueryExpression::Variable;
    order.condition->value = $1;
    query_graph->addOrder(order);
    free($1);
    $1 = nullptr;
  }
  ;

/* 18 */
limit_clause:
  LIMIT INTEGER { query_graph->setLimit(atol($2)); free($2); $2 = nullptr; }
  ;

/* 19 */
offset_clause:
  OFFSET INTEGER { query_graph->setOffset(atol($2)); free($2); $2 = nullptr; }
  ;

/* 20 */
group_graph_pattern:
  pattern_lbracket graph_pattern pattern_rbracket {
    $$ = $2;
  }
  ;

pattern_lbracket:
  '{' {
    context->parent_pattern = context->pattern;
    context->pattern = query_graph->createPattern();
    context->pattern->type = QueryPattern::Basic;
  }
  ;

pattern_rbracket:
  '}' {
    context->pattern = context->parent_pattern;
  }
  ;

graph_pattern:
  basic_graph_pattern { $$ = context->pattern; }
  |
  union_graph_pattern { $$ = context->pattern; }
  ;

basic_graph_pattern:
  triples_same_subject '.'
  |
  basic_graph_pattern triples_same_subject '.'
  |
  basic_graph_pattern filter
  |
  basic_graph_pattern optional_graph_pattern
  ;


/* 23 */
optional_graph_pattern:
  OPTIONAL group_graph_pattern {  context->pattern->optionals.push_back($2); }
  ;

/* 25 */
union_graph_pattern:
  group_graph_pattern {
    context->pattern->sub_patterns.push_back($1);
    context->pattern->type = QueryPattern::Union;
  }
  |
  union_graph_pattern UNION group_graph_pattern {
    context->pattern->sub_patterns.push_back($3);
  }
  ;

/* 26 */
filter:
  FILTER constraint { context->pattern->filters.push_back($2); }
  ;

/* 27 */
constraint:
  bracketted_expression { $$ = $1; }
  |
  built_in_call { $$ = $1; }
  |
  function_call { $$ = $1; }
  ;

/* 28 */
function_call:
  iri '(' expression_list ')' {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::Function;
    exp->value = $1;
    exp->arg_list.insert(exp->arg_list.end(), $3->arg_list.begin(), $3->arg_list.end());
    free($1);
    $1 = nullptr;
    query_graph->removeExpression($3);
  }
  |
  iri NIL {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::Function;
    exp->value = $1;
    free($1);
    $1 = nullptr;
  }
  ;

/* 30 */
construct_template:
  construct_lbracket construct_triples construct_rbracket {
    query_graph->construct_pattern = context->pattern;
  }
  ;

construct_lbracket:
  '{' {
    context->pattern = query_graph->createPattern();
    context->pattern->type = QueryPattern::Basic;
  }
  ;

construct_rbracket:
  '}'
  ;

/* 31 */
construct_triples:
  triples_same_subject
  |
  construct_triples '.' triples_same_subject
  ;

/* 32 */
triples_same_subject:
  subject1 property_list {
    QueryResource *subject = context->subjects.back();
    context->resource_pool.free(subject);
    context->subjects.pop_back();
  }
  |
  subject2 {
    QueryResource *subject = context->subjects.back();
    context->resource_pool.free(subject);
    context->subjects.pop_back();
  }
  |
  subject2 property_list {
    QueryResource *subject = context->subjects.back();
    context->resource_pool.free(subject);
    context->subjects.pop_back();
  }
  ;

subject1:
  var_element { context->subjects.push_back($1); }
  |
  iri_element { context->subjects.push_back($1); }
  |
  blank_node_element { context->subjects.push_back($1); }
  ;

subject2:
  blank_node_property_list { context->subjects.push_back($1); }
  ;

/* 33 */
property_list:
  verb object_list {
    QueryResource *predicate = context->predicates.back();
    context->resource_pool.free(predicate);
    context->predicates.pop_back();
  }
  |
  property_list ';' verb object_list {
    QueryResource *predicate = context->predicates.back();
    context->resource_pool.free(predicate);
    context->predicates.pop_back();
  }
  ;

/* 35 */
object_list:
  object {
    QueryResource *object = $1;
    int node = query_graph->addNode(QueryNode(*context->subjects.back(), *context->predicates.back(), *object));
    context->pattern->nodes.push_back(node);
    context->resource_pool.free(object);
  }
  |
  object_list ',' object {
    QueryResource *object = $3;
    int node = query_graph->addNode(QueryNode(*context->subjects.back(), *context->predicates.back(), *object));
    context->pattern->nodes.push_back(node);
    context->resource_pool.free(object);
  }
  ;

/* 36 */
object:
  var_element { $$ = $1; }
  |
  iri_element { $$ = $1; }
  |
  literal_element { $$ = $1; }
  |
  blank_node_element { $$ = $1; }
  |
  blank_node_property_list { $$ = $1; }
  ;

/* 37 */
verb:
  var_element { context->predicates.push_back($1); }
  |
  iri_element { context->predicates.push_back($1); }
  |
  type_element { context->predicates.push_back($1); }
  ;

type_element:
  IDENTIFIER {
    if(strcmp($1, "a") != 0 && strcmp($1, "A") != 0){
      yyerror (&yylloc, scanner, context, query_graph, YY_("syntax error, unexpected IDENTIFIER, expecting ':' or 'a'"));
      YYERROR;
    }
    free($1);
    $1 = nullptr;
    QueryResource *resource = context->resource_pool.alloc();
    resource->type = QueryResource::IRI;
    resource->value = RDFVocabulary::RDF_TYPE;
    $$ = resource;
  }
  ;

/* 39 */
blank_node_property_list:
  open_square_bracket property_list ']' {
    QueryResource *blankNode = context->subjects.back();
    context->subjects.pop_back();
    $$ = blankNode;
  }
  ;

open_square_bracket:
  '[' {
    QueryResource *subject = context->resource_pool.alloc();
    subject->type = QueryResource::Variable;
    subject->value = context->bknode_id_gen.generate();
    context->subjects.push_back(subject);
  }
  ;

var_element:
  VARIABLE {
    QueryResource *resource = context->resource_pool.alloc();
    resource->type = QueryResource::Variable;
    resource->value = $1;
    if(context->all_variables){
      if(!context->variables.count(resource->value)){
        context->variables.insert(resource->value);
        query_graph->addProjection(resource->value);
      }
    }
    free($1);
    $1 = nullptr;
    $$ = resource;
  }
  ;

iri_element:
  iri {
    QueryResource *resource = context->resource_pool.alloc();
    resource->type = QueryResource::IRI;
    resource->value = $1;
    free($1);
    $1 = nullptr;
    $$ = resource;
  }
  ;

literal_element:
  rdf_literal {
    QueryResource *resource = context->resource_pool.alloc();
    resource->type = QueryResource::Literal;
    resource->value = $1;
    free($1);
    $1 = nullptr;
    $$ = resource;
  }
  ;

blank_node_element:
  '_' ':' IDENTIFIER {
    std::string bknode_id;
    if(context->blank_nodes.count($3)) {
      bknode_id = context->blank_nodes[$3];
    }
    else {
      bknode_id = context->bknode_id_gen.generate();
      context->blank_nodes[$3] = bknode_id;
    }
    free($3);
    $3 = nullptr;
    QueryResource *resource = context->resource_pool.alloc();
    resource->type = QueryResource::Variable;
    resource->value = bknode_id;
    $$ = resource;
  }
  |
  ANON {
    QueryResource *resource = context->resource_pool.alloc();
    resource->type = QueryResource::Variable;
    resource->value = context->bknode_id_gen.generate();
    $$ = resource;
  }
  ;

expression_list:
  expression {
    QueryExpression* exp = query_graph->createExpression();;
    exp->type = QueryExpression::ArgumentList;
    exp->arg_list.push_back($1);
    $$ = exp;
  }
  |
  expression_list ',' expression {
    $1->arg_list.push_back($3);
    $$ = $1;
  }
  ;

/* 46 */
expression:
  conditional_expression { $$ = $1; }
  ;

/* 47 */
conditional_expression:
  conditional_expression OP_OR conditional_expression {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::Or;
    exp->arg_list.push_back($1);
    exp->arg_list.push_back($3);
    $$ = exp;
  }
  |
  conditional_expression OP_AND conditional_expression {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::And;
    exp->arg_list.push_back($1);
    exp->arg_list.push_back($3);
    $$ = exp;
  }
  |
  relation_expression { $$ = $1; }
  ;

/* 50 */
relation_expression:
  numeric_expression OP_EQ numeric_expression {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::Equal;
    exp->arg_list.push_back($1);
    exp->arg_list.push_back($3);
    $$ = exp;
  }
  |
  numeric_expression OP_NE numeric_expression {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::NotEqual;
    exp->arg_list.push_back($1);
    exp->arg_list.push_back($3);
    $$ = exp;
  }
  |
  numeric_expression OP_LT numeric_expression {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::Less;
    exp->arg_list.push_back($1);
    exp->arg_list.push_back($3);
    $$ = exp;
  }
  |
  numeric_expression OP_GT numeric_expression {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::Greater;
    exp->arg_list.push_back($1);
    exp->arg_list.push_back($3);
    $$ = exp;
  }
  |
  numeric_expression OP_LE numeric_expression {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::LessOrEqual;
    exp->arg_list.push_back($1);
    exp->arg_list.push_back($3);
    $$ = exp;
  }
  |
  numeric_expression OP_GE numeric_expression {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::GreaterOrEqual;
    exp->arg_list.push_back($1);
    exp->arg_list.push_back($3);
    $$ = exp;
  }
  |
  numeric_expression { $$ = $1; }
  ;

/* 51 */
numeric_expression:
  numeric_expression '+' numeric_expression {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::Plus;
    exp->arg_list.push_back($1);
    exp->arg_list.push_back($3);
    $$ = exp;
  }
  |
  numeric_expression '-' numeric_expression {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::Minus;
    exp->arg_list.push_back($1);
    exp->arg_list.push_back($3);
    $$ = exp;
  }
  |
  numeric_expression '*' numeric_expression {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::Mul;
    exp->arg_list.push_back($1);
    exp->arg_list.push_back($3);
    $$ = exp;
  }
  |
  numeric_expression '/' numeric_expression {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::Div;
    exp->arg_list.push_back($1);
    exp->arg_list.push_back($3);
    $$ = exp;
  }
  |
  unary_expression { $$ = $1; }
  ;

/* 54 */
unary_expression:
  '!' primary_expression %prec UMINUS {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::Not;
    exp->arg_list.push_back($2);
    $$ = exp;
  }
  |
  '+' primary_expression %prec UMINUS {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::UnaryPlus;
    exp->arg_list.push_back($2);
    $$ = exp;
  }
  |
  '-' primary_expression %prec UMINUS {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::UnaryMinus;
    exp->arg_list.push_back($2);
    $$ = exp;
  }
  |
  primary_expression { $$ = $1; }
  ;

/* 55 */
primary_expression:
  bracketted_expression { $$ = $1; }
  |
  built_in_call { $$ = $1; }
  |
  function_call { $$ = $1; }
  |
  iri {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::IRI;
    exp->value = $1;
    free($1);
    $1 = nullptr;
    $$ = exp;
  }
  |
  VARIABLE {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::Variable;
    exp->value = $1;
    free($1);
    $1 = nullptr;
    $$ = exp;
  }
  |
  literal {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::Literal;
    exp->value = $1;
    free($1);
    $1 = nullptr;
    $$ = exp;
  }
  ;

/* 56 */
bracketted_expression:
  '(' expression ')' { $$ = $2; }
  ;

/* 57 */
built_in_call:
  BIC_BOUND '(' VARIABLE ')' {
    QueryExpression* arg = query_graph->createExpression();
    arg->type = QueryExpression::Variable;
    arg->value = $3;
    free($3);
    $3 = nullptr;
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::Builtin_bound;
    exp->arg_list.push_back(arg);
    $$ = exp;
  }
  |
  BIC_STR bracketted_expression {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::Builtin_str;
    exp->arg_list.push_back($2);
    $$ = exp;
  }
  |
  BIC_LANG bracketted_expression {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::Builtin_lang;
    exp->arg_list.push_back($2);
    $$ = exp;
  }
  |
  BIC_DATATYPE bracketted_expression {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::Builtin_datatype;
    exp->arg_list.push_back($2);
    $$ = exp;
  }
  |
  BIC_SAMETERM '(' expression ',' expression ')' {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::Builtin_sameterm;
    exp->arg_list.push_back($3);
    exp->arg_list.push_back($5);
    $$ = exp;
  }
  |
  BIC_LANGMATCHES '(' expression ',' expression ')' {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::Builtin_langmatches;
    exp->arg_list.push_back($3);
    exp->arg_list.push_back($5);
    $$ = exp;
  }
  |
  BIC_ISIRI bracketted_expression {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::Builtin_isiri;
    exp->arg_list.push_back($2);
    $$ = exp;
  }
  |
  BIC_ISURI bracketted_expression {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::Builtin_isuri;
    exp->arg_list.push_back($2);
    $$ = exp;
  }
  |
  BIC_ISBLANK bracketted_expression {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::Builtin_isblank;
    exp->arg_list.push_back($2);
    $$ = exp;
  }
  |
  BIC_ISLITERAL bracketted_expression {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::Builtin_isliteral;
    exp->arg_list.push_back($2);
    $$ = exp;
  }
  |
  BIC_REGEX '(' expression ',' expression_list ')'  {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::Builtin_regex;
    exp->arg_list.push_back($3);
    exp->arg_list.insert(exp->arg_list.end(), $5->arg_list.begin(), $5->arg_list.end());
    query_graph->removeExpression($5);
    $$ = exp;
  }
  ;

literal:
  rdf_literal { $$ = $1; }
  |
  numeric_literal { $$ = $1; }
  |
  boolean_literal { $$ = $1; }
  ;

/* 60 */
rdf_literal:
  STRING {
    std::stringstream ss;
    ss << "\"" << $1 << "\"" << XSDVocabulary::XSD_STRING;
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

/* 61 */
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

/* 65 */
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

/* 67 */
iri:
  IRI {
    std::string str = $1;
    if(!context->base.empty() && str.find("://")==std::string::npos){
      str.insert(str.begin()+1, context->base.begin()+1, context->base.end()-1);
    }
    free($1);
    $1 = nullptr;
    $$ = strdup(str.c_str());
  }
  |
  IDENTIFIER ':' IDENTIFIER {
    std::string str = context->prefixes[$1];
    str.insert(str.length()-1, $3, strlen($3));
    free($1);
    $1 = nullptr;
    free($3);
    $3 = nullptr;
    $$ = strdup(str.c_str());
  }
  |
  ':' IDENTIFIER{
    std::string str = context->prefixes[""];
    str.insert(str.length()-1, $2, strlen($2));
    free($2);
    $2 = nullptr;
    $$ = strdup(str.c_str());
  }
  ;

%%


SPARQLParser::SPARQLParser(FILE * input_file) : input_file(input_file) {
  state = new SPARQLParseState();
  rqlex_init(&state->scanner);
  state->buffer = rq_create_buffer(this->input_file, YY_BUF_SIZE, state->scanner);
  rq_switch_to_buffer(state->buffer, state->scanner);
}

SPARQLParser::SPARQLParser(const std::string& query_string) : input_file(NULL) {
  state = new SPARQLParseState();
  rqlex_init(&state->scanner);
  state->buffer = rq_scan_string(query_string.c_str(), state->scanner);
}

SPARQLParser::~SPARQLParser() {
  rq_delete_buffer(state->buffer, state->scanner);
  rqlex_destroy(state->scanner);
  delete state;
}

bool SPARQLParser::parse(QueryGraph* query_graph) {
  SPARQLParseContext context;
  context.all_variables = false;
  bool res = rqparse(state->scanner, &context, query_graph) == 0;
  return res;
}


bool SPARQLParser::parse(const std::string& query_string, QueryGraph* query_graph){
  SPARQLParser parser(query_string);
  bool res = parser.parse(query_graph);
  return res;
}

SPARQLFileParser::SPARQLFileParser(const std::string& file_path) : SPARQLParser(fopen(file_path.c_str(), "r")) {}

SPARQLFileParser::~SPARQLFileParser() {
  fclose(input_file);
}
