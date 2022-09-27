/* A Bison parser, made by GNU Bison 3.7.5.  */

/* Bison interface for Yacc-like parsers in C

   Copyright (C) 1984, 1989-1990, 2000-2015, 2018-2021 Free Software Foundation,
   Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

/* DO NOT RELY ON FEATURES THAT ARE NOT DOCUMENTED in the manual,
   especially those whose name start with YY_ or yy_.  They are
   private implementation details that can be changed or removed.  */

#ifndef YY_RQ_SPARQL_LEXER_H_INCLUDED
# define YY_RQ_SPARQL_LEXER_H_INCLUDED
/* Debug traces.  */
#ifndef RQDEBUG
# if defined YYDEBUG
#if YYDEBUG
#   define RQDEBUG 1
#  else
#   define RQDEBUG 0
#  endif
# else /* ! defined YYDEBUG */
#  define RQDEBUG 0
# endif /* ! defined YYDEBUG */
#endif  /* ! defined RQDEBUG */
#if RQDEBUG
extern int rqdebug;
#endif
/* "%code requires" blocks.  */
#line 71 "sparql_parser.y"


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


#line 94 "sparql_lexer.h"

/* Token kinds.  */
#ifndef RQTOKENTYPE
# define RQTOKENTYPE
  enum rqtokentype
  {
    RQEMPTY = -2,
    RQEOF = 0,                     /* "end of file"  */
    RQerror = 256,                 /* error  */
    RQUNDEF = 257,                 /* "invalid token"  */
    BASE = 258,                    /* BASE  */
    PREFIX = 259,                  /* PREFIX  */
    SELECT = 260,                  /* SELECT  */
    CONSTRUCT = 261,               /* CONSTRUCT  */
    ASK = 262,                     /* ASK  */
    DESCRIBE = 263,                /* DESCRIBE  */
    ORDER = 264,                   /* ORDER  */
    BY = 265,                      /* BY  */
    ASC = 266,                     /* ASC  */
    DESC = 267,                    /* DESC  */
    DISTINCT = 268,                /* DISTINCT  */
    REDUCED = 269,                 /* REDUCED  */
    OFFSET = 270,                  /* OFFSET  */
    LIMIT = 271,                   /* LIMIT  */
    FROM = 272,                    /* FROM  */
    NAMED = 273,                   /* NAMED  */
    WHERE = 274,                   /* WHERE  */
    UNION = 275,                   /* UNION  */
    FILTER = 276,                  /* FILTER  */
    GRAPH = 277,                   /* GRAPH  */
    OPTIONAL = 278,                /* OPTIONAL  */
    IN = 279,                      /* IN  */
    NOT = 280,                     /* NOT  */
    EXISTS = 281,                  /* EXISTS  */
    MINUS = 282,                   /* MINUS  */
    BOOL_TRUE = 283,               /* BOOL_TRUE  */
    BOOL_FALSE = 284,              /* BOOL_FALSE  */
    BIC_BOUND = 285,               /* BIC_BOUND  */
    BIC_STR = 286,                 /* BIC_STR  */
    BIC_LANG = 287,                /* BIC_LANG  */
    BIC_DATATYPE = 288,            /* BIC_DATATYPE  */
    BIC_SAMETERM = 289,            /* BIC_SAMETERM  */
    BIC_LANGMATCHES = 290,         /* BIC_LANGMATCHES  */
    BIC_ISIRI = 291,               /* BIC_ISIRI  */
    BIC_ISURI = 292,               /* BIC_ISURI  */
    BIC_ISBLANK = 293,             /* BIC_ISBLANK  */
    BIC_ISLITERAL = 294,           /* BIC_ISLITERAL  */
    BIC_REGEX = 295,               /* BIC_REGEX  */
    END_OF_FILE = 296,             /* END_OF_FILE  */
    OP_OR = 297,                   /* OP_OR  */
    OP_AND = 298,                  /* OP_AND  */
    OP_EQ = 299,                   /* OP_EQ  */
    OP_NE = 300,                   /* OP_NE  */
    OP_LT = 301,                   /* OP_LT  */
    OP_GT = 302,                   /* OP_GT  */
    OP_LE = 303,                   /* OP_LE  */
    OP_GE = 304,                   /* OP_GE  */
    TYPE = 305,                    /* TYPE  */
    ANON = 306,                    /* ANON  */
    NIL = 307,                     /* NIL  */
    DECIMAL = 308,                 /* DECIMAL  */
    DOUBLE = 309,                  /* DOUBLE  */
    INTEGER = 310,                 /* INTEGER  */
    VARIABLE = 311,                /* VARIABLE  */
    IRI = 312,                     /* IRI  */
    IDENTIFIER = 313,              /* IDENTIFIER  */
    STRING = 314,                  /* STRING  */
    LANGTAG = 315,                 /* LANGTAG  */
    UMINUS = 316                   /* UMINUS  */
  };
  typedef enum rqtokentype rqtoken_kind_t;
#endif

/* Value type.  */
#if ! defined RQSTYPE && ! defined RQSTYPE_IS_DECLARED
union RQSTYPE
{
#line 110 "sparql_parser.y"

  char* strval;
  QueryResource *resource;
  QueryExpression *expression;
  QueryPattern *pattern;

#line 179 "sparql_lexer.h"

};
typedef union RQSTYPE RQSTYPE;
# define RQSTYPE_IS_TRIVIAL 1
# define RQSTYPE_IS_DECLARED 1
#endif

/* Location type.  */
#if ! defined RQLTYPE && ! defined RQLTYPE_IS_DECLARED
typedef struct RQLTYPE RQLTYPE;
struct RQLTYPE
{
  int first_line;
  int first_column;
  int last_line;
  int last_column;
};
# define RQLTYPE_IS_DECLARED 1
# define RQLTYPE_IS_TRIVIAL 1
#endif



int rqparse (yyscan_t scanner, SPARQLParseContext* context, QueryGraph* query_graph);
/* "%code provides" blocks.  */
#line 164 "sparql_parser.y"


#define YYSTYPE         RQSTYPE
#define YYLTYPE         RQLTYPE

YY_BUFFER_STATE rq_scan_string (yyconst char *str, yyscan_t scanner);
YY_BUFFER_STATE rq_create_buffer(FILE *file, int size, yyscan_t scanner);
void rq_switch_to_buffer(YY_BUFFER_STATE new_buffer, yyscan_t scanner);
void rq_delete_buffer (YY_BUFFER_STATE buf, yyscan_t scanner);

int rqlex_init (yyscan_t* scanner);
int rqlex_destroy (yyscan_t scanner);
int rqlex(YYSTYPE * yylval, YYLTYPE * yylloc, yyscan_t scanner);


#line 221 "sparql_lexer.h"

#endif /* !YY_RQ_SPARQL_LEXER_H_INCLUDED  */
