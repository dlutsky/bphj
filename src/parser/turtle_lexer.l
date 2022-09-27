/* pure scanner */
%option reentrant

/* make flex's pure calling sequence compatible with bison’s */
%option bison-bridge
%option bison-locations

%option noyywrap
%option yylineno

%option prefix="ttl"


%{
#include "turtle_lexer.h"

#define YY_USER_INIT yylineno = 0; yycolumn = 0;

#define YY_USER_ACTION yylloc->first_line = yylloc->last_line = yylineno; \
  yylloc->first_column = yycolumn; yylloc->last_column = yycolumn+yyleng-1; \
  yycolumn += yyleng;


%}


%%

@base                 { return BASE; }
@prefix               { return PREFIX; }
(?i:BASE)             { return SPARQL_BASE; }
(?i:PREFIX)           { return SPARQL_PREFIX; }
true                  { return BOOL_TRUE; }
false                 { return BOOL_FALSE; }


"^^"            { return TYPE; }
"["[ ]*"]"      { return ANON; }

[:_()[\],.;]      { return yytext[0]; }


"<"[^<>{}|\\]+">" {
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

\"[^\"\n]*\" |
'[^'\n]*' {
  yylval->strval = strndup(yytext+1, strlen(yytext)-2);
  return STRING;
}

\"{3}[^\"\n]*\"{3} |
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
