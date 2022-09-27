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

#ifndef YY_TTL_TURTLE_LEXER_H_INCLUDED
# define YY_TTL_TURTLE_LEXER_H_INCLUDED
/* Debug traces.  */
#ifndef TTLDEBUG
# if defined YYDEBUG
#if YYDEBUG
#   define TTLDEBUG 1
#  else
#   define TTLDEBUG 0
#  endif
# else /* ! defined YYDEBUG */
#  define TTLDEBUG 0
# endif /* ! defined YYDEBUG */
#endif  /* ! defined TTLDEBUG */
#if TTLDEBUG
extern int ttldebug;
#endif
/* "%code requires" blocks.  */
#line 58 "turtle_parser.y"


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


#line 93 "turtle_lexer.h"

/* Token kinds.  */
#ifndef TTLTOKENTYPE
# define TTLTOKENTYPE
  enum ttltokentype
  {
    TTLEMPTY = -2,
    TTLEOF = 0,                    /* "end of file"  */
    TTLerror = 256,                /* error  */
    TTLUNDEF = 257,                /* "invalid token"  */
    BASE = 258,                    /* BASE  */
    SPARQL_BASE = 259,             /* SPARQL_BASE  */
    PREFIX = 260,                  /* PREFIX  */
    SPARQL_PREFIX = 261,           /* SPARQL_PREFIX  */
    BOOL_TRUE = 262,               /* BOOL_TRUE  */
    BOOL_FALSE = 263,              /* BOOL_FALSE  */
    END_OF_FILE = 264,             /* END_OF_FILE  */
    DECIMAL = 265,                 /* DECIMAL  */
    DOUBLE = 266,                  /* DOUBLE  */
    INTEGER = 267,                 /* INTEGER  */
    IRI = 268,                     /* IRI  */
    IDENTIFIER = 269,              /* IDENTIFIER  */
    STRING = 270,                  /* STRING  */
    LANGTAG = 271,                 /* LANGTAG  */
    TYPE = 272,                    /* TYPE  */
    ANON = 273                     /* ANON  */
  };
  typedef enum ttltokentype ttltoken_kind_t;
#endif

/* Value type.  */
#if ! defined TTLSTYPE && ! defined TTLSTYPE_IS_DECLARED
union TTLSTYPE
{
#line 95 "turtle_parser.y"

  char* strval;

#line 132 "turtle_lexer.h"

};
typedef union TTLSTYPE TTLSTYPE;
# define TTLSTYPE_IS_TRIVIAL 1
# define TTLSTYPE_IS_DECLARED 1
#endif

/* Location type.  */
#if ! defined TTLLTYPE && ! defined TTLLTYPE_IS_DECLARED
typedef struct TTLLTYPE TTLLTYPE;
struct TTLLTYPE
{
  int first_line;
  int first_column;
  int last_line;
  int last_column;
};
# define TTLLTYPE_IS_DECLARED 1
# define TTLLTYPE_IS_TRIVIAL 1
#endif



int ttlparse (yyscan_t scanner, TurtleParseContext& context, std::vector<std::tuple<std::string, std::string, std::string>>& triples);
/* "%code provides" blocks.  */
#line 124 "turtle_parser.y"


#define YYSTYPE         TTLSTYPE
#define YYLTYPE         TTLLTYPE

YY_BUFFER_STATE ttl_scan_string (yyconst char *str, yyscan_t scanner);
YY_BUFFER_STATE ttl_create_buffer(FILE *file, int size, yyscan_t scanner);
void ttl_switch_to_buffer(YY_BUFFER_STATE new_buffer, yyscan_t scanner);
void ttl_delete_buffer (YY_BUFFER_STATE buf, yyscan_t scanner);

int ttllex_init (yyscan_t* scanner);
int ttllex_destroy (yyscan_t scanner);
int ttllex(YYSTYPE * yylval, YYLTYPE * yylloc, yyscan_t scanner);


#line 174 "turtle_lexer.h"

#endif /* !YY_TTL_TURTLE_LEXER_H_INCLUDED  */
