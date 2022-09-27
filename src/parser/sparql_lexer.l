/* pure scanner */
%option reentrant

/* make flex's pure calling sequence compatible with bison’s */
%option bison-bridge
%option bison-locations

%option case-insensitive
%option noyywrap
%option yylineno

%option prefix="rq"


%{
#include "sparql_lexer.h"

#define YY_USER_INIT yylineno = 0; yycolumn = 0;

#define YY_USER_ACTION yylloc->first_line = yylloc->last_line = yylineno; \
  yylloc->first_column = yycolumn; yylloc->last_column = yycolumn+yyleng-1; \
  yycolumn += yyleng;


%}




%%
BASE            { return BASE; }
PREFIX          { return PREFIX; }
SELECT          { return SELECT; }
CONSTRUCT       { return CONSTRUCT; }
ASK             { return ASK; }
DESCRIBE        { return DESCRIBE; }

ORDER           { return ORDER; }
BY              { return BY; }
ASC             { return ASC; }
DESC            { return DESC; }
DISTINCT        { return DISTINCT; }
REDUCED         { return REDUCED; }
OFFSET          { return OFFSET; }
LIMIT           { return LIMIT; }

FROM            { return FROM; }
NAMED           { return NAMED; }
WHERE           { return WHERE; }

UNION           { return UNION; }
FILTER          { return FILTER; }
GRAPH           { return GRAPH; }
OPTIONAL        { return OPTIONAL; }

IN              { return IN; }
NOT             { return NOT; }
EXISTS          { return EXISTS; }
MINUS           { return MINUS; }
TRUE            { return BOOL_TRUE; }
FALSE           { return BOOL_FALSE; }

BOUND           { return BIC_BOUND; }
STR             { return BIC_STR; }
LANG            { return BIC_LANG; }
DATATYPE        { return BIC_DATATYPE; }
SAMETERM        { return BIC_SAMETERM; }
LANGMATCHES     { return BIC_LANGMATCHES; }

ISIRI           { return BIC_ISIRI; }
ISURI           { return BIC_ISURI; }
ISBLANK         { return BIC_ISBLANK; }
ISLITERAL       { return BIC_ISLITERAL; }
REGEX           { return BIC_REGEX; }


"||"            { return OP_OR; }
"&&"            { return OP_AND; }
"="             { return OP_EQ; }
"!="            { return OP_NE; }
"<"             { return OP_LT; }
">"             { return OP_GT; }
"<="            { return OP_LE; }
">="            { return OP_GE; }

"^^"            { return TYPE; }
"["[ ]*"]"      { return ANON; }
"("[ ]*")"      { return NIL; }

[-+*/:_()[\]{},.;]      { return yytext[0]; }


[?$][A-Za-z0-9_]+	{
  yylval->strval = strdup(yytext);
  return VARIABLE;
}

"<"[^<>"{}|^`\\]+">" {
  yylval->strval = strdup(yytext);
  return IRI;
}

[A-Za-z]["."A-Za-z0-9_]* {
  yylval->strval = strdup(yytext);
  return IDENTIFIER;
}

-?[0-9]+	{
  yylval->strval = strdup(yytext);
  return INTEGER;
}

-?[0-9]+"."[0-9]* |
-?"."[0-9]+	{
  yylval->strval = strdup(yytext);
  return DECIMAL;
}

-?[0-9]+"."[0-9]*[eE][+-]?[0-9]+ |
-?"."[0-9]+[eE][+-]?[0-9]+ |
-?[0-9]+[eE][+-]?[0-9]+	{
  yylval->strval = strdup(yytext);
  return DOUBLE;
}

\"[^\"\n]+\" |
'[^'\n]*' {
  yylval->strval = strndup(yytext+1, strlen(yytext)-2);
  return STRING;
}

\"{3}[^\"\n]+\"{3} |
'{3}[^'\n]*'{3} {
  yylval->strval = strndup(yytext+3, strlen(yytext)-4);
  return STRING;
}

"@"[a-zA-Z]+("-"[a-zA-Z0-9]+)* {
  yylval->strval = strdup(yytext);
  return LANGTAG;
}


[ \t\r\n\f]+ ;       /* white space */
"#".*$ ;             /* comment */

<<EOF>>              { return END_OF_FILE; }

%%


void yyunget (yyscan_t yyscanner) {
  struct yyguts_t * yyg = (struct yyguts_t*)yyscanner;
  yyless(0);
}
