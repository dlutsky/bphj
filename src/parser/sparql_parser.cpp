/* A Bison parser, made by GNU Bison 3.7.5.  */

/* Bison implementation for Yacc-like parsers in C

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

/* C LALR(1) parser skeleton written by Richard Stallman, by
   simplifying the original so-called "semantic" parser.  */

/* DO NOT RELY ON FEATURES THAT ARE NOT DOCUMENTED in the manual,
   especially those whose name start with YY_ or yy_.  They are
   private implementation details that can be changed or removed.  */

/* All symbols defined below should begin with yy or YY, to avoid
   infringing on user name space.  This should be done even for local
   variables, as they might otherwise be expanded by user macros.
   There are some unavoidable exceptions within include files to
   define necessary library symbols; they are noted "INFRINGES ON
   USER NAME SPACE" below.  */

/* Identify Bison output, and Bison version.  */
#define YYBISON 30705

/* Bison version string.  */
#define YYBISON_VERSION "3.7.5"

/* Skeleton name.  */
#define YYSKELETON_NAME "yacc.c"

/* Pure parsers.  */
#define YYPURE 2

/* Push parsers.  */
#define YYPUSH 0

/* Pull parsers.  */
#define YYPULL 1

/* Substitute the type names.  */
#define YYSTYPE         RQSTYPE
#define YYLTYPE         RQLTYPE
/* Substitute the variable and function names.  */
#define yyparse         rqparse
#define yylex           rqlex
#define yyerror         rqerror
#define yydebug         rqdebug
#define yynerrs         rqnerrs

/* First part of user prologue.  */
#line 19 "sparql_parser.y"


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



#line 130 "sparql_parser.cpp"

# ifndef YY_CAST
#  ifdef __cplusplus
#   define YY_CAST(Type, Val) static_cast<Type> (Val)
#   define YY_REINTERPRET_CAST(Type, Val) reinterpret_cast<Type> (Val)
#  else
#   define YY_CAST(Type, Val) ((Type) (Val))
#   define YY_REINTERPRET_CAST(Type, Val) ((Type) (Val))
#  endif
# endif
# ifndef YY_NULLPTR
#  if defined __cplusplus
#   if 201103L <= __cplusplus
#    define YY_NULLPTR nullptr
#   else
#    define YY_NULLPTR 0
#   endif
#  else
#   define YY_NULLPTR ((void*)0)
#  endif
# endif

#include "sparql_lexer.h"
/* Symbol kind.  */
enum yysymbol_kind_t
{
  YYSYMBOL_YYEMPTY = -2,
  YYSYMBOL_YYEOF = 0,                      /* "end of file"  */
  YYSYMBOL_YYerror = 1,                    /* error  */
  YYSYMBOL_YYUNDEF = 2,                    /* "invalid token"  */
  YYSYMBOL_BASE = 3,                       /* BASE  */
  YYSYMBOL_PREFIX = 4,                     /* PREFIX  */
  YYSYMBOL_SELECT = 5,                     /* SELECT  */
  YYSYMBOL_CONSTRUCT = 6,                  /* CONSTRUCT  */
  YYSYMBOL_ASK = 7,                        /* ASK  */
  YYSYMBOL_DESCRIBE = 8,                   /* DESCRIBE  */
  YYSYMBOL_ORDER = 9,                      /* ORDER  */
  YYSYMBOL_BY = 10,                        /* BY  */
  YYSYMBOL_ASC = 11,                       /* ASC  */
  YYSYMBOL_DESC = 12,                      /* DESC  */
  YYSYMBOL_DISTINCT = 13,                  /* DISTINCT  */
  YYSYMBOL_REDUCED = 14,                   /* REDUCED  */
  YYSYMBOL_OFFSET = 15,                    /* OFFSET  */
  YYSYMBOL_LIMIT = 16,                     /* LIMIT  */
  YYSYMBOL_FROM = 17,                      /* FROM  */
  YYSYMBOL_NAMED = 18,                     /* NAMED  */
  YYSYMBOL_WHERE = 19,                     /* WHERE  */
  YYSYMBOL_UNION = 20,                     /* UNION  */
  YYSYMBOL_FILTER = 21,                    /* FILTER  */
  YYSYMBOL_GRAPH = 22,                     /* GRAPH  */
  YYSYMBOL_OPTIONAL = 23,                  /* OPTIONAL  */
  YYSYMBOL_IN = 24,                        /* IN  */
  YYSYMBOL_NOT = 25,                       /* NOT  */
  YYSYMBOL_EXISTS = 26,                    /* EXISTS  */
  YYSYMBOL_MINUS = 27,                     /* MINUS  */
  YYSYMBOL_BOOL_TRUE = 28,                 /* BOOL_TRUE  */
  YYSYMBOL_BOOL_FALSE = 29,                /* BOOL_FALSE  */
  YYSYMBOL_BIC_BOUND = 30,                 /* BIC_BOUND  */
  YYSYMBOL_BIC_STR = 31,                   /* BIC_STR  */
  YYSYMBOL_BIC_LANG = 32,                  /* BIC_LANG  */
  YYSYMBOL_BIC_DATATYPE = 33,              /* BIC_DATATYPE  */
  YYSYMBOL_BIC_SAMETERM = 34,              /* BIC_SAMETERM  */
  YYSYMBOL_BIC_LANGMATCHES = 35,           /* BIC_LANGMATCHES  */
  YYSYMBOL_BIC_ISIRI = 36,                 /* BIC_ISIRI  */
  YYSYMBOL_BIC_ISURI = 37,                 /* BIC_ISURI  */
  YYSYMBOL_BIC_ISBLANK = 38,               /* BIC_ISBLANK  */
  YYSYMBOL_BIC_ISLITERAL = 39,             /* BIC_ISLITERAL  */
  YYSYMBOL_BIC_REGEX = 40,                 /* BIC_REGEX  */
  YYSYMBOL_END_OF_FILE = 41,               /* END_OF_FILE  */
  YYSYMBOL_OP_OR = 42,                     /* OP_OR  */
  YYSYMBOL_OP_AND = 43,                    /* OP_AND  */
  YYSYMBOL_OP_EQ = 44,                     /* OP_EQ  */
  YYSYMBOL_OP_NE = 45,                     /* OP_NE  */
  YYSYMBOL_OP_LT = 46,                     /* OP_LT  */
  YYSYMBOL_OP_GT = 47,                     /* OP_GT  */
  YYSYMBOL_OP_LE = 48,                     /* OP_LE  */
  YYSYMBOL_OP_GE = 49,                     /* OP_GE  */
  YYSYMBOL_TYPE = 50,                      /* TYPE  */
  YYSYMBOL_ANON = 51,                      /* ANON  */
  YYSYMBOL_NIL = 52,                       /* NIL  */
  YYSYMBOL_DECIMAL = 53,                   /* DECIMAL  */
  YYSYMBOL_DOUBLE = 54,                    /* DOUBLE  */
  YYSYMBOL_INTEGER = 55,                   /* INTEGER  */
  YYSYMBOL_VARIABLE = 56,                  /* VARIABLE  */
  YYSYMBOL_IRI = 57,                       /* IRI  */
  YYSYMBOL_IDENTIFIER = 58,                /* IDENTIFIER  */
  YYSYMBOL_STRING = 59,                    /* STRING  */
  YYSYMBOL_LANGTAG = 60,                   /* LANGTAG  */
  YYSYMBOL_61_ = 61,                       /* '+'  */
  YYSYMBOL_62_ = 62,                       /* '-'  */
  YYSYMBOL_63_ = 63,                       /* '*'  */
  YYSYMBOL_64_ = 64,                       /* '/'  */
  YYSYMBOL_UMINUS = 65,                    /* UMINUS  */
  YYSYMBOL_66_ = 66,                       /* '('  */
  YYSYMBOL_67_ = 67,                       /* ')'  */
  YYSYMBOL_68_ = 68,                       /* ':'  */
  YYSYMBOL_69_ = 69,                       /* '{'  */
  YYSYMBOL_70_ = 70,                       /* '}'  */
  YYSYMBOL_71_ = 71,                       /* '.'  */
  YYSYMBOL_72_ = 72,                       /* ';'  */
  YYSYMBOL_73_ = 73,                       /* ','  */
  YYSYMBOL_74_ = 74,                       /* ']'  */
  YYSYMBOL_75_ = 75,                       /* '['  */
  YYSYMBOL_76___ = 76,                     /* '_'  */
  YYSYMBOL_77_ = 77,                       /* '!'  */
  YYSYMBOL_YYACCEPT = 78,                  /* $accept  */
  YYSYMBOL_query = 79,                     /* query  */
  YYSYMBOL_prologue = 80,                  /* prologue  */
  YYSYMBOL_base_declaration = 81,          /* base_declaration  */
  YYSYMBOL_prefix_declaration = 82,        /* prefix_declaration  */
  YYSYMBOL_prefix_list = 83,               /* prefix_list  */
  YYSYMBOL_prefix = 84,                    /* prefix  */
  YYSYMBOL_select_query = 85,              /* select_query  */
  YYSYMBOL_duplicate_modifier = 86,        /* duplicate_modifier  */
  YYSYMBOL_select_list = 87,               /* select_list  */
  YYSYMBOL_variables = 88,                 /* variables  */
  YYSYMBOL_construct_query = 89,           /* construct_query  */
  YYSYMBOL_describe_query = 90,            /* describe_query  */
  YYSYMBOL_describe_list = 91,             /* describe_list  */
  YYSYMBOL_var_or_iri_list = 92,           /* var_or_iri_list  */
  YYSYMBOL_ask_query = 93,                 /* ask_query  */
  YYSYMBOL_dataset_clause = 94,            /* dataset_clause  */
  YYSYMBOL_from_clause = 95,               /* from_clause  */
  YYSYMBOL_where_clause = 96,              /* where_clause  */
  YYSYMBOL_solution_modifier = 97,         /* solution_modifier  */
  YYSYMBOL_limit_offset_clause = 98,       /* limit_offset_clause  */
  YYSYMBOL_order_clause = 99,              /* order_clause  */
  YYSYMBOL_order_conditions = 100,         /* order_conditions  */
  YYSYMBOL_order_condition = 101,          /* order_condition  */
  YYSYMBOL_limit_clause = 102,             /* limit_clause  */
  YYSYMBOL_offset_clause = 103,            /* offset_clause  */
  YYSYMBOL_group_graph_pattern = 104,      /* group_graph_pattern  */
  YYSYMBOL_pattern_lbracket = 105,         /* pattern_lbracket  */
  YYSYMBOL_pattern_rbracket = 106,         /* pattern_rbracket  */
  YYSYMBOL_graph_pattern = 107,            /* graph_pattern  */
  YYSYMBOL_basic_graph_pattern = 108,      /* basic_graph_pattern  */
  YYSYMBOL_optional_graph_pattern = 109,   /* optional_graph_pattern  */
  YYSYMBOL_union_graph_pattern = 110,      /* union_graph_pattern  */
  YYSYMBOL_filter = 111,                   /* filter  */
  YYSYMBOL_constraint = 112,               /* constraint  */
  YYSYMBOL_function_call = 113,            /* function_call  */
  YYSYMBOL_construct_template = 114,       /* construct_template  */
  YYSYMBOL_construct_lbracket = 115,       /* construct_lbracket  */
  YYSYMBOL_construct_rbracket = 116,       /* construct_rbracket  */
  YYSYMBOL_construct_triples = 117,        /* construct_triples  */
  YYSYMBOL_triples_same_subject = 118,     /* triples_same_subject  */
  YYSYMBOL_subject1 = 119,                 /* subject1  */
  YYSYMBOL_subject2 = 120,                 /* subject2  */
  YYSYMBOL_property_list = 121,            /* property_list  */
  YYSYMBOL_object_list = 122,              /* object_list  */
  YYSYMBOL_object = 123,                   /* object  */
  YYSYMBOL_verb = 124,                     /* verb  */
  YYSYMBOL_type_element = 125,             /* type_element  */
  YYSYMBOL_blank_node_property_list = 126, /* blank_node_property_list  */
  YYSYMBOL_open_square_bracket = 127,      /* open_square_bracket  */
  YYSYMBOL_var_element = 128,              /* var_element  */
  YYSYMBOL_iri_element = 129,              /* iri_element  */
  YYSYMBOL_literal_element = 130,          /* literal_element  */
  YYSYMBOL_blank_node_element = 131,       /* blank_node_element  */
  YYSYMBOL_expression_list = 132,          /* expression_list  */
  YYSYMBOL_expression = 133,               /* expression  */
  YYSYMBOL_conditional_expression = 134,   /* conditional_expression  */
  YYSYMBOL_relation_expression = 135,      /* relation_expression  */
  YYSYMBOL_numeric_expression = 136,       /* numeric_expression  */
  YYSYMBOL_unary_expression = 137,         /* unary_expression  */
  YYSYMBOL_primary_expression = 138,       /* primary_expression  */
  YYSYMBOL_bracketted_expression = 139,    /* bracketted_expression  */
  YYSYMBOL_built_in_call = 140,            /* built_in_call  */
  YYSYMBOL_literal = 141,                  /* literal  */
  YYSYMBOL_rdf_literal = 142,              /* rdf_literal  */
  YYSYMBOL_numeric_literal = 143,          /* numeric_literal  */
  YYSYMBOL_boolean_literal = 144,          /* boolean_literal  */
  YYSYMBOL_iri = 145                       /* iri  */
};
typedef enum yysymbol_kind_t yysymbol_kind_t;




#ifdef short
# undef short
#endif

/* On compilers that do not define __PTRDIFF_MAX__ etc., make sure
   <limits.h> and (if available) <stdint.h> are included
   so that the code can choose integer types of a good width.  */

#ifndef __PTRDIFF_MAX__
# include <limits.h> /* INFRINGES ON USER NAME SPACE */
# if defined __STDC_VERSION__ && 199901 <= __STDC_VERSION__
#  include <stdint.h> /* INFRINGES ON USER NAME SPACE */
#  define YY_STDINT_H
# endif
#endif

/* Narrow types that promote to a signed type and that can represent a
   signed or unsigned integer of at least N bits.  In tables they can
   save space and decrease cache pressure.  Promoting to a signed type
   helps avoid bugs in integer arithmetic.  */

#ifdef __INT_LEAST8_MAX__
typedef __INT_LEAST8_TYPE__ yytype_int8;
#elif defined YY_STDINT_H
typedef int_least8_t yytype_int8;
#else
typedef signed char yytype_int8;
#endif

#ifdef __INT_LEAST16_MAX__
typedef __INT_LEAST16_TYPE__ yytype_int16;
#elif defined YY_STDINT_H
typedef int_least16_t yytype_int16;
#else
typedef short yytype_int16;
#endif

/* Work around bug in HP-UX 11.23, which defines these macros
   incorrectly for preprocessor constants.  This workaround can likely
   be removed in 2023, as HPE has promised support for HP-UX 11.23
   (aka HP-UX 11i v2) only through the end of 2022; see Table 2 of
   <https://h20195.www2.hpe.com/V2/getpdf.aspx/4AA4-7673ENW.pdf>.  */
#ifdef __hpux
# undef UINT_LEAST8_MAX
# undef UINT_LEAST16_MAX
# define UINT_LEAST8_MAX 255
# define UINT_LEAST16_MAX 65535
#endif

#if defined __UINT_LEAST8_MAX__ && __UINT_LEAST8_MAX__ <= __INT_MAX__
typedef __UINT_LEAST8_TYPE__ yytype_uint8;
#elif (!defined __UINT_LEAST8_MAX__ && defined YY_STDINT_H \
       && UINT_LEAST8_MAX <= INT_MAX)
typedef uint_least8_t yytype_uint8;
#elif !defined __UINT_LEAST8_MAX__ && UCHAR_MAX <= INT_MAX
typedef unsigned char yytype_uint8;
#else
typedef short yytype_uint8;
#endif

#if defined __UINT_LEAST16_MAX__ && __UINT_LEAST16_MAX__ <= __INT_MAX__
typedef __UINT_LEAST16_TYPE__ yytype_uint16;
#elif (!defined __UINT_LEAST16_MAX__ && defined YY_STDINT_H \
       && UINT_LEAST16_MAX <= INT_MAX)
typedef uint_least16_t yytype_uint16;
#elif !defined __UINT_LEAST16_MAX__ && USHRT_MAX <= INT_MAX
typedef unsigned short yytype_uint16;
#else
typedef int yytype_uint16;
#endif

#ifndef YYPTRDIFF_T
# if defined __PTRDIFF_TYPE__ && defined __PTRDIFF_MAX__
#  define YYPTRDIFF_T __PTRDIFF_TYPE__
#  define YYPTRDIFF_MAXIMUM __PTRDIFF_MAX__
# elif defined PTRDIFF_MAX
#  ifndef ptrdiff_t
#   include <stddef.h> /* INFRINGES ON USER NAME SPACE */
#  endif
#  define YYPTRDIFF_T ptrdiff_t
#  define YYPTRDIFF_MAXIMUM PTRDIFF_MAX
# else
#  define YYPTRDIFF_T long
#  define YYPTRDIFF_MAXIMUM LONG_MAX
# endif
#endif

#ifndef YYSIZE_T
# ifdef __SIZE_TYPE__
#  define YYSIZE_T __SIZE_TYPE__
# elif defined size_t
#  define YYSIZE_T size_t
# elif defined __STDC_VERSION__ && 199901 <= __STDC_VERSION__
#  include <stddef.h> /* INFRINGES ON USER NAME SPACE */
#  define YYSIZE_T size_t
# else
#  define YYSIZE_T unsigned
# endif
#endif

#define YYSIZE_MAXIMUM                                  \
  YY_CAST (YYPTRDIFF_T,                                 \
           (YYPTRDIFF_MAXIMUM < YY_CAST (YYSIZE_T, -1)  \
            ? YYPTRDIFF_MAXIMUM                         \
            : YY_CAST (YYSIZE_T, -1)))

#define YYSIZEOF(X) YY_CAST (YYPTRDIFF_T, sizeof (X))


/* Stored state numbers (used for stacks). */
typedef yytype_uint8 yy_state_t;

/* State numbers in computations.  */
typedef int yy_state_fast_t;

#ifndef YY_
# if defined YYENABLE_NLS && YYENABLE_NLS
#  if ENABLE_NLS
#   include <libintl.h> /* INFRINGES ON USER NAME SPACE */
#   define YY_(Msgid) dgettext ("bison-runtime", Msgid)
#  endif
# endif
# ifndef YY_
#  define YY_(Msgid) Msgid
# endif
#endif


#ifndef YY_ATTRIBUTE_PURE
# if defined __GNUC__ && 2 < __GNUC__ + (96 <= __GNUC_MINOR__)
#  define YY_ATTRIBUTE_PURE __attribute__ ((__pure__))
# else
#  define YY_ATTRIBUTE_PURE
# endif
#endif

#ifndef YY_ATTRIBUTE_UNUSED
# if defined __GNUC__ && 2 < __GNUC__ + (7 <= __GNUC_MINOR__)
#  define YY_ATTRIBUTE_UNUSED __attribute__ ((__unused__))
# else
#  define YY_ATTRIBUTE_UNUSED
# endif
#endif

/* Suppress unused-variable warnings by "using" E.  */
#if ! defined lint || defined __GNUC__
# define YY_USE(E) ((void) (E))
#else
# define YY_USE(E) /* empty */
#endif

#if defined __GNUC__ && ! defined __ICC && 407 <= __GNUC__ * 100 + __GNUC_MINOR__
/* Suppress an incorrect diagnostic about yylval being uninitialized.  */
# define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN                            \
    _Pragma ("GCC diagnostic push")                                     \
    _Pragma ("GCC diagnostic ignored \"-Wuninitialized\"")              \
    _Pragma ("GCC diagnostic ignored \"-Wmaybe-uninitialized\"")
# define YY_IGNORE_MAYBE_UNINITIALIZED_END      \
    _Pragma ("GCC diagnostic pop")
#else
# define YY_INITIAL_VALUE(Value) Value
#endif
#ifndef YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
# define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
# define YY_IGNORE_MAYBE_UNINITIALIZED_END
#endif
#ifndef YY_INITIAL_VALUE
# define YY_INITIAL_VALUE(Value) /* Nothing. */
#endif

#if defined __cplusplus && defined __GNUC__ && ! defined __ICC && 6 <= __GNUC__
# define YY_IGNORE_USELESS_CAST_BEGIN                          \
    _Pragma ("GCC diagnostic push")                            \
    _Pragma ("GCC diagnostic ignored \"-Wuseless-cast\"")
# define YY_IGNORE_USELESS_CAST_END            \
    _Pragma ("GCC diagnostic pop")
#endif
#ifndef YY_IGNORE_USELESS_CAST_BEGIN
# define YY_IGNORE_USELESS_CAST_BEGIN
# define YY_IGNORE_USELESS_CAST_END
#endif


#define YY_ASSERT(E) ((void) (0 && (E)))

#if 1

/* The parser invokes alloca or malloc; define the necessary symbols.  */

# ifdef YYSTACK_USE_ALLOCA
#  if YYSTACK_USE_ALLOCA
#   ifdef __GNUC__
#    define YYSTACK_ALLOC __builtin_alloca
#   elif defined __BUILTIN_VA_ARG_INCR
#    include <alloca.h> /* INFRINGES ON USER NAME SPACE */
#   elif defined _AIX
#    define YYSTACK_ALLOC __alloca
#   elif defined _MSC_VER
#    include <malloc.h> /* INFRINGES ON USER NAME SPACE */
#    define alloca _alloca
#   else
#    define YYSTACK_ALLOC alloca
#    if ! defined _ALLOCA_H && ! defined EXIT_SUCCESS
#     include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
      /* Use EXIT_SUCCESS as a witness for stdlib.h.  */
#     ifndef EXIT_SUCCESS
#      define EXIT_SUCCESS 0
#     endif
#    endif
#   endif
#  endif
# endif

# ifdef YYSTACK_ALLOC
   /* Pacify GCC's 'empty if-body' warning.  */
#  define YYSTACK_FREE(Ptr) do { /* empty */; } while (0)
#  ifndef YYSTACK_ALLOC_MAXIMUM
    /* The OS might guarantee only one guard page at the bottom of the stack,
       and a page size can be as small as 4096 bytes.  So we cannot safely
       invoke alloca (N) if N exceeds 4096.  Use a slightly smaller number
       to allow for a few compiler-allocated temporary stack slots.  */
#   define YYSTACK_ALLOC_MAXIMUM 4032 /* reasonable circa 2006 */
#  endif
# else
#  define YYSTACK_ALLOC YYMALLOC
#  define YYSTACK_FREE YYFREE
#  ifndef YYSTACK_ALLOC_MAXIMUM
#   define YYSTACK_ALLOC_MAXIMUM YYSIZE_MAXIMUM
#  endif
#  if (defined __cplusplus && ! defined EXIT_SUCCESS \
       && ! ((defined YYMALLOC || defined malloc) \
             && (defined YYFREE || defined free)))
#   include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
#   ifndef EXIT_SUCCESS
#    define EXIT_SUCCESS 0
#   endif
#  endif
#  ifndef YYMALLOC
#   define YYMALLOC malloc
#   if ! defined malloc && ! defined EXIT_SUCCESS
void *malloc (YYSIZE_T); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
#  ifndef YYFREE
#   define YYFREE free
#   if ! defined free && ! defined EXIT_SUCCESS
void free (void *); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
# endif
#endif /* 1 */

#if (! defined yyoverflow \
     && (! defined __cplusplus \
         || (defined RQLTYPE_IS_TRIVIAL && RQLTYPE_IS_TRIVIAL \
             && defined RQSTYPE_IS_TRIVIAL && RQSTYPE_IS_TRIVIAL)))

/* A type that is properly aligned for any stack member.  */
union yyalloc
{
  yy_state_t yyss_alloc;
  YYSTYPE yyvs_alloc;
  YYLTYPE yyls_alloc;
};

/* The size of the maximum gap between one aligned stack and the next.  */
# define YYSTACK_GAP_MAXIMUM (YYSIZEOF (union yyalloc) - 1)

/* The size of an array large to enough to hold all stacks, each with
   N elements.  */
# define YYSTACK_BYTES(N) \
     ((N) * (YYSIZEOF (yy_state_t) + YYSIZEOF (YYSTYPE) \
             + YYSIZEOF (YYLTYPE)) \
      + 2 * YYSTACK_GAP_MAXIMUM)

# define YYCOPY_NEEDED 1

/* Relocate STACK from its old location to the new one.  The
   local variables YYSIZE and YYSTACKSIZE give the old and new number of
   elements in the stack, and YYPTR gives the new location of the
   stack.  Advance YYPTR to a properly aligned location for the next
   stack.  */
# define YYSTACK_RELOCATE(Stack_alloc, Stack)                           \
    do                                                                  \
      {                                                                 \
        YYPTRDIFF_T yynewbytes;                                         \
        YYCOPY (&yyptr->Stack_alloc, Stack, yysize);                    \
        Stack = &yyptr->Stack_alloc;                                    \
        yynewbytes = yystacksize * YYSIZEOF (*Stack) + YYSTACK_GAP_MAXIMUM; \
        yyptr += yynewbytes / YYSIZEOF (*yyptr);                        \
      }                                                                 \
    while (0)

#endif

#if defined YYCOPY_NEEDED && YYCOPY_NEEDED
/* Copy COUNT objects from SRC to DST.  The source and destination do
   not overlap.  */
# ifndef YYCOPY
#  if defined __GNUC__ && 1 < __GNUC__
#   define YYCOPY(Dst, Src, Count) \
      __builtin_memcpy (Dst, Src, YY_CAST (YYSIZE_T, (Count)) * sizeof (*(Src)))
#  else
#   define YYCOPY(Dst, Src, Count)              \
      do                                        \
        {                                       \
          YYPTRDIFF_T yyi;                      \
          for (yyi = 0; yyi < (Count); yyi++)   \
            (Dst)[yyi] = (Src)[yyi];            \
        }                                       \
      while (0)
#  endif
# endif
#endif /* !YYCOPY_NEEDED */

/* YYFINAL -- State number of the termination state.  */
#define YYFINAL  7
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   326

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  78
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  68
/* YYNRULES -- Number of rules.  */
#define YYNRULES  157
/* YYNSTATES -- Number of states.  */
#define YYNSTATES  253

/* YYMAXUTOK -- Last valid token kind.  */
#define YYMAXUTOK   316


/* YYTRANSLATE(TOKEN-NUM) -- Symbol number corresponding to TOKEN-NUM
   as returned by yylex, with out-of-bounds checking.  */
#define YYTRANSLATE(YYX)                                \
  (0 <= (YYX) && (YYX) <= YYMAXUTOK                     \
   ? YY_CAST (yysymbol_kind_t, yytranslate[YYX])        \
   : YYSYMBOL_YYUNDEF)

/* YYTRANSLATE[TOKEN-NUM] -- Symbol number corresponding to TOKEN-NUM
   as returned by yylex.  */
static const yytype_int8 yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,    77,     2,     2,     2,     2,     2,     2,
      66,    67,    63,    61,    73,    62,    71,    64,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,    68,    72,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,    75,     2,    74,     2,    76,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,    69,     2,    70,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     1,     2,     3,     4,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15,    16,    17,    18,    19,    20,    21,    22,    23,    24,
      25,    26,    27,    28,    29,    30,    31,    32,    33,    34,
      35,    36,    37,    38,    39,    40,    41,    42,    43,    44,
      45,    46,    47,    48,    49,    50,    51,    52,    53,    54,
      55,    56,    57,    58,    59,    60,    65
};

#if RQDEBUG
  /* YYRLINE[YYN] -- Source line where rule number YYN was defined.  */
static const yytype_int16 yyrline[] =
{
       0,   186,   186,   188,   190,   192,   194,   199,   204,   205,
     210,   211,   215,   223,   233,   235,   240,   246,   248,   249,
     254,   256,   260,   266,   275,   282,   288,   290,   294,   296,
     298,   300,   305,   313,   314,   318,   326,   328,   333,   335,
     337,   342,   344,   346,   348,   353,   357,   359,   363,   370,
     377,   384,   398,   403,   408,   414,   422,   428,   430,   434,
     436,   438,   440,   446,   451,   456,   463,   468,   470,   472,
     477,   487,   498,   504,   511,   516,   518,   523,   529,   535,
     543,   545,   547,   551,   556,   562,   571,   578,   588,   590,
     592,   594,   596,   601,   603,   605,   609,   625,   633,   642,
     659,   670,   681,   698,   707,   714,   722,   727,   735,   743,
     748,   756,   764,   772,   780,   788,   796,   801,   809,   817,
     825,   833,   838,   845,   852,   859,   864,   866,   868,   870,
     879,   888,   900,   905,   917,   924,   931,   938,   946,   954,
     961,   968,   975,   982,   993,   995,   997,  1002,  1010,  1020,
    1033,  1039,  1045,  1054,  1060,  1069,  1079,  1089
};
#endif

/** Accessing symbol of state STATE.  */
#define YY_ACCESSING_SYMBOL(State) YY_CAST (yysymbol_kind_t, yystos[State])

#if 1
/* The user-facing name of the symbol whose (internal) number is
   YYSYMBOL.  No bounds checking.  */
static const char *yysymbol_name (yysymbol_kind_t yysymbol) YY_ATTRIBUTE_UNUSED;

/* YYTNAME[SYMBOL-NUM] -- String name of the symbol SYMBOL-NUM.
   First, the terminals, then, starting at YYNTOKENS, nonterminals.  */
static const char *const yytname[] =
{
  "\"end of file\"", "error", "\"invalid token\"", "BASE", "PREFIX",
  "SELECT", "CONSTRUCT", "ASK", "DESCRIBE", "ORDER", "BY", "ASC", "DESC",
  "DISTINCT", "REDUCED", "OFFSET", "LIMIT", "FROM", "NAMED", "WHERE",
  "UNION", "FILTER", "GRAPH", "OPTIONAL", "IN", "NOT", "EXISTS", "MINUS",
  "BOOL_TRUE", "BOOL_FALSE", "BIC_BOUND", "BIC_STR", "BIC_LANG",
  "BIC_DATATYPE", "BIC_SAMETERM", "BIC_LANGMATCHES", "BIC_ISIRI",
  "BIC_ISURI", "BIC_ISBLANK", "BIC_ISLITERAL", "BIC_REGEX", "END_OF_FILE",
  "OP_OR", "OP_AND", "OP_EQ", "OP_NE", "OP_LT", "OP_GT", "OP_LE", "OP_GE",
  "TYPE", "ANON", "NIL", "DECIMAL", "DOUBLE", "INTEGER", "VARIABLE", "IRI",
  "IDENTIFIER", "STRING", "LANGTAG", "'+'", "'-'", "'*'", "'/'", "UMINUS",
  "'('", "')'", "':'", "'{'", "'}'", "'.'", "';'", "','", "']'", "'['",
  "'_'", "'!'", "$accept", "query", "prologue", "base_declaration",
  "prefix_declaration", "prefix_list", "prefix", "select_query",
  "duplicate_modifier", "select_list", "variables", "construct_query",
  "describe_query", "describe_list", "var_or_iri_list", "ask_query",
  "dataset_clause", "from_clause", "where_clause", "solution_modifier",
  "limit_offset_clause", "order_clause", "order_conditions",
  "order_condition", "limit_clause", "offset_clause",
  "group_graph_pattern", "pattern_lbracket", "pattern_rbracket",
  "graph_pattern", "basic_graph_pattern", "optional_graph_pattern",
  "union_graph_pattern", "filter", "constraint", "function_call",
  "construct_template", "construct_lbracket", "construct_rbracket",
  "construct_triples", "triples_same_subject", "subject1", "subject2",
  "property_list", "object_list", "object", "verb", "type_element",
  "blank_node_property_list", "open_square_bracket", "var_element",
  "iri_element", "literal_element", "blank_node_element",
  "expression_list", "expression", "conditional_expression",
  "relation_expression", "numeric_expression", "unary_expression",
  "primary_expression", "bracketted_expression", "built_in_call",
  "literal", "rdf_literal", "numeric_literal", "boolean_literal", "iri", YY_NULLPTR
};

static const char *
yysymbol_name (yysymbol_kind_t yysymbol)
{
  return yytname[yysymbol];
}
#endif

#ifdef YYPRINT
/* YYTOKNUM[NUM] -- (External) token number corresponding to the
   (internal) symbol number NUM (which must be that of a token).  */
static const yytype_int16 yytoknum[] =
{
       0,   256,   257,   258,   259,   260,   261,   262,   263,   264,
     265,   266,   267,   268,   269,   270,   271,   272,   273,   274,
     275,   276,   277,   278,   279,   280,   281,   282,   283,   284,
     285,   286,   287,   288,   289,   290,   291,   292,   293,   294,
     295,   296,   297,   298,   299,   300,   301,   302,   303,   304,
     305,   306,   307,   308,   309,   310,   311,   312,   313,   314,
     315,    43,    45,    42,    47,   316,    40,    41,    58,   123,
     125,    46,    59,    44,    93,    91,    95,    33
};
#endif

#define YYPACT_NINF (-148)

#define yypact_value_is_default(Yyn) \
  ((Yyn) == YYPACT_NINF)

#define YYTABLE_NINF (-1)

#define yytable_value_is_error(Yyn) \
  0

  /* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
     STATE-NUM.  */
static const yytype_int16 yypact[] =
{
      11,   -56,  -148,    26,   232,    33,  -148,  -148,    75,   -24,
      64,    43,  -148,  -148,  -148,  -148,    44,  -148,   101,  -148,
    -148,   -34,  -148,    64,   235,  -148,    -7,  -148,  -148,  -148,
      39,  -148,    61,    64,    -3,  -148,  -148,    71,    44,  -148,
    -148,    64,    56,    -7,  -148,  -148,  -148,    97,    22,  -148,
      57,    57,  -148,    57,  -148,  -148,  -148,  -148,   100,  -148,
    -148,  -148,    28,   127,  -148,    -7,  -148,  -148,   139,   152,
      -7,  -148,    41,   149,  -148,   235,  -148,    39,   150,   160,
    -148,  -148,  -148,   150,     4,  -148,  -148,   153,    95,   207,
     162,  -148,  -148,  -148,   177,    41,   250,   186,   187,  -148,
    -148,   146,   230,   267,  -148,  -148,    57,   -16,   211,  -148,
    -148,  -148,  -148,  -148,  -148,  -148,  -148,  -148,  -148,   231,
     100,  -148,  -148,   223,   100,  -148,  -148,  -148,    98,  -148,
    -148,  -148,  -148,  -148,   160,   -10,  -148,   160,   224,   229,
     229,   229,   234,   236,   229,   229,   229,   229,   238,   144,
    -148,  -148,  -148,  -148,   -31,  -148,  -148,  -148,   229,   229,
    -148,    98,  -148,  -148,   211,  -148,  -148,   240,  -148,  -148,
    -148,   144,   144,  -148,  -148,  -148,  -148,   144,  -148,  -148,
    -148,  -148,  -148,  -148,   219,   219,   219,  -148,   239,   125,
    -148,    96,  -148,  -148,  -148,  -148,  -148,  -148,  -148,  -148,
     -31,  -148,   144,  -148,  -148,  -148,   241,   225,   228,   242,
    -148,  -148,  -148,  -148,   144,   144,   144,   144,   144,   144,
     144,   144,   144,   144,   144,   144,     7,  -148,  -148,   144,
     144,   144,   262,  -148,   218,   218,   218,   218,   218,   218,
     145,   145,  -148,  -148,  -148,   144,   245,   246,    54,  -148,
    -148,  -148,  -148
};

  /* YYDEFACT[STATE-NUM] -- Default reduction number in state STATE-NUM.
     Performed when YYTABLE does not specify something else to do.  Zero
     means the default is an error.  */
static const yytype_uint8 yydefact[] =
{
       9,     0,     6,     0,     0,    11,     8,     1,    19,     0,
      34,     0,     2,     3,     4,     5,    15,     7,    10,    17,
      18,     0,    73,    34,     0,    35,     0,    33,    28,   155,
       0,    27,     0,    34,    26,    29,    14,     0,    15,    22,
      21,    34,    20,     0,   103,    99,    98,     0,     0,    75,
       0,    78,    83,     0,    80,    81,    82,   100,     0,    55,
      32,    37,     0,     0,   157,     0,    30,    31,     0,     0,
       0,    23,    40,     0,    74,     0,    72,    96,    77,     0,
      95,    93,    94,    79,     0,    36,    64,     0,    57,    58,
       0,   156,    25,    12,     0,    40,     0,     0,     0,    24,
      39,     0,    41,    42,   102,    76,     0,   147,    84,    86,
      92,    88,    89,    90,    91,   101,    97,    56,    54,     0,
       0,    62,    61,     0,     0,    59,    13,    16,     0,    53,
      52,    38,    43,    44,     0,     0,   148,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
      66,    69,    67,    68,     0,    63,    60,    65,     0,     0,
      51,    45,    46,    50,    85,   149,    87,     0,   134,   135,
     136,     0,     0,   139,   140,   141,   142,     0,   153,   154,
     151,   152,   150,   130,     0,     0,     0,   128,     0,   106,
     109,   116,   121,   125,   126,   127,   131,   144,   145,   146,
     129,    71,     0,    48,    49,    47,     0,     0,     0,     0,
     123,   124,   122,   132,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   104,   133,     0,
       0,     0,   107,   108,   110,   111,   112,   113,   114,   115,
     117,   118,   119,   120,    70,     0,     0,     0,     0,   105,
     137,   138,   143
};

  /* YYPGOTO[NTERM-NUM].  */
static const yytype_int16 yypgoto[] =
{
    -148,  -148,  -148,  -148,  -148,  -148,   269,  -148,  -148,  -148,
    -148,  -148,  -148,  -148,  -148,  -148,     8,  -148,   -27,   214,
     213,  -148,  -148,   155,   215,   217,   -53,  -148,  -148,  -148,
    -148,  -148,  -148,  -148,   198,  -101,  -148,  -148,  -148,  -148,
     -42,  -148,  -148,    69,   188,   183,   220,  -148,   -68,  -148,
     -47,   -43,  -148,   -64,    90,  -147,     0,  -148,   -30,  -148,
     -37,    85,  -100,  -148,   -62,  -148,  -148,   -11
};

  /* YYDEFGOTO[NTERM-NUM].  */
static const yytype_uint8 yydefgoto[] =
{
       0,     3,     4,     5,    17,    18,    37,    12,    21,    41,
      42,    13,    14,    33,    34,    15,    26,    27,    60,    99,
     100,   101,   161,   162,   102,   103,    61,    62,   118,    87,
      88,   121,    89,   122,   163,   187,    23,    24,    76,    48,
      49,    50,    51,    78,   108,   109,    79,    80,    52,    53,
      54,    55,   113,    56,   226,   227,   189,   190,   191,   192,
     193,   194,   195,   196,   197,   198,   199,   200
};

  /* YYTABLE[YYPACT[STATE-NUM]] -- What to do in state STATE-NUM.  If
     positive, shift that token.  If negative, reduce the rule whose
     number is the opposite.  If YYTABLE_NINF, syntax error.  */
static const yytype_uint8 yytable[] =
{
      35,     6,   188,    81,    81,    85,    81,    82,    82,    86,
      82,   110,    58,    57,     1,   114,    72,   115,   151,   153,
      90,   201,    39,    67,   207,   208,     7,   151,   153,    40,
     209,    43,   111,   105,   135,   202,   112,    16,    92,    57,
      57,    65,    57,    95,   136,    22,   123,    29,    30,    70,
      96,    57,     2,    66,    29,    30,    97,    98,    32,    81,
     151,   153,    59,    82,    57,    32,   110,   155,    57,   110,
     114,   157,   115,   114,   244,   115,   106,    57,   116,    44,
     245,    25,   246,   247,    45,    29,    30,   111,    19,    20,
     111,   112,    74,    75,   112,    57,    32,    59,   249,    28,
      29,    30,    36,    46,    47,    38,    31,    63,   154,   158,
     159,    32,    71,    45,    29,    77,   119,   154,   120,    64,
      83,   252,    84,    57,   165,    32,    57,   245,   138,   139,
     140,   141,   142,   143,   144,   145,   146,   147,   148,    68,
     216,   217,   218,   219,   220,   221,    44,   210,   211,   212,
     154,    45,    29,    30,   160,    29,    30,   222,   223,   224,
     225,    97,    98,    32,   149,    73,    32,   214,   215,    59,
      46,    47,   178,   179,   138,   139,   140,   141,   142,   143,
     144,   145,   146,   147,   148,    91,   234,   235,   236,   237,
     238,   239,   240,   241,   242,   243,    93,   180,   181,   182,
     183,    29,    30,   107,   152,   184,   185,   104,   224,   225,
     149,    44,    32,   152,   232,   233,    45,    29,    30,   107,
      94,   186,   106,   117,   168,   169,   170,   124,    32,   173,
     174,   175,   176,   125,   126,    46,    47,     8,     9,    10,
      11,   129,   130,   203,   204,    97,   152,   178,   179,   138,
     139,   140,   141,   142,   143,   144,   145,   146,   147,   148,
     128,   138,   139,   140,   141,   142,   143,   144,   145,   146,
     147,   148,   180,   181,   182,   183,    29,    30,   107,   222,
     223,   224,   225,    98,   137,   149,    44,    32,    29,    30,
     167,    45,    29,    30,   156,   149,   206,   149,   229,    32,
     171,   230,   172,    32,   177,   215,   213,    69,   228,   127,
      46,    47,   250,   251,   131,   231,   205,   150,   133,   132,
     166,   248,   164,     0,     0,     0,   134
};

static const yytype_int16 yycheck[] =
{
      11,    57,   149,    50,    51,    58,    53,    50,    51,    62,
      53,    79,    19,    24,     3,    79,    43,    79,   119,   119,
      62,    52,    56,    34,   171,   172,     0,   128,   128,    63,
     177,    23,    79,    75,    50,    66,    79,     4,    65,    50,
      51,    33,    53,    70,    60,    69,    88,    57,    58,    41,
       9,    62,    41,    56,    57,    58,    15,    16,    68,   106,
     161,   161,    69,   106,    75,    68,   134,   120,    79,   137,
     134,   124,   134,   137,    67,   137,    72,    88,    74,    51,
      73,    17,   229,   230,    56,    57,    58,   134,    13,    14,
     137,   134,    70,    71,   137,   106,    68,    69,   245,    56,
      57,    58,    58,    75,    76,     4,    63,    68,   119,    11,
      12,    68,    56,    56,    57,    58,    21,   128,    23,    58,
      51,    67,    53,   134,   135,    68,   137,    73,    30,    31,
      32,    33,    34,    35,    36,    37,    38,    39,    40,    68,
      44,    45,    46,    47,    48,    49,    51,   184,   185,   186,
     161,    56,    57,    58,    56,    57,    58,    61,    62,    63,
      64,    15,    16,    68,    66,    68,    68,    42,    43,    69,
      75,    76,    28,    29,    30,    31,    32,    33,    34,    35,
      36,    37,    38,    39,    40,    58,   216,   217,   218,   219,
     220,   221,   222,   223,   224,   225,    57,    53,    54,    55,
      56,    57,    58,    59,   119,    61,    62,    58,    63,    64,
      66,    51,    68,   128,   214,   215,    56,    57,    58,    59,
      68,    77,    72,    70,   139,   140,   141,    20,    68,   144,
     145,   146,   147,    71,    57,    75,    76,     5,     6,     7,
       8,    55,    55,   158,   159,    15,   161,    28,    29,    30,
      31,    32,    33,    34,    35,    36,    37,    38,    39,    40,
      10,    30,    31,    32,    33,    34,    35,    36,    37,    38,
      39,    40,    53,    54,    55,    56,    57,    58,    59,    61,
      62,    63,    64,    16,    73,    66,    51,    68,    57,    58,
      66,    56,    57,    58,    71,    66,    56,    66,    73,    68,
      66,    73,    66,    68,    66,    43,    67,    38,    67,    95,
      75,    76,    67,    67,   101,    73,   161,   119,   103,   102,
     137,   231,   134,    -1,    -1,    -1,   106
};

  /* YYSTOS[STATE-NUM] -- The (internal number of the) accessing
     symbol of state STATE-NUM.  */
static const yytype_uint8 yystos[] =
{
       0,     3,    41,    79,    80,    81,    57,     0,     5,     6,
       7,     8,    85,    89,    90,    93,     4,    82,    83,    13,
      14,    86,    69,   114,   115,    17,    94,    95,    56,    57,
      58,    63,    68,    91,    92,   145,    58,    84,     4,    56,
      63,    87,    88,    94,    51,    56,    75,    76,   117,   118,
     119,   120,   126,   127,   128,   129,   131,   145,    19,    69,
      96,   104,   105,    68,    58,    94,    56,   145,    68,    84,
      94,    56,    96,    68,    70,    71,   116,    58,   121,   124,
     125,   128,   129,   121,   121,   104,   104,   107,   108,   110,
     118,    58,    96,    57,    68,    96,     9,    15,    16,    97,
      98,    99,   102,   103,    58,   118,    72,    59,   122,   123,
     126,   128,   129,   130,   131,   142,    74,    70,   106,    21,
      23,   109,   111,   118,    20,    71,    57,    97,    10,    55,
      55,    98,   103,   102,   124,    50,    60,    73,    30,    31,
      32,    33,    34,    35,    36,    37,    38,    39,    40,    66,
     112,   113,   139,   140,   145,   104,    71,   104,    11,    12,
      56,   100,   101,   112,   122,   145,   123,    66,   139,   139,
     139,    66,    66,   139,   139,   139,   139,    66,    28,    29,
      53,    54,    55,    56,    61,    62,    77,   113,   133,   134,
     135,   136,   137,   138,   139,   140,   141,   142,   143,   144,
     145,    52,    66,   139,   139,   101,    56,   133,   133,   133,
     138,   138,   138,    67,    42,    43,    44,    45,    46,    47,
      48,    49,    61,    62,    63,    64,   132,   133,    67,    73,
      73,    73,   134,   134,   136,   136,   136,   136,   136,   136,
     136,   136,   136,   136,    67,    73,   133,   133,   132,   133,
      67,    67,    67
};

  /* YYR1[YYN] -- Symbol number of symbol that rule YYN derives.  */
static const yytype_uint8 yyr1[] =
{
       0,    78,    79,    79,    79,    79,    79,    80,    81,    81,
      82,    82,    83,    83,    84,    84,    85,    86,    86,    86,
      87,    87,    88,    88,    89,    90,    91,    91,    92,    92,
      92,    92,    93,    94,    94,    95,    96,    96,    97,    97,
      97,    98,    98,    98,    98,    99,   100,   100,   101,   101,
     101,   101,   102,   103,   104,   105,   106,   107,   107,   108,
     108,   108,   108,   109,   110,   110,   111,   112,   112,   112,
     113,   113,   114,   115,   116,   117,   117,   118,   118,   118,
     119,   119,   119,   120,   121,   121,   122,   122,   123,   123,
     123,   123,   123,   124,   124,   124,   125,   126,   127,   128,
     129,   130,   131,   131,   132,   132,   133,   134,   134,   134,
     135,   135,   135,   135,   135,   135,   135,   136,   136,   136,
     136,   136,   137,   137,   137,   137,   138,   138,   138,   138,
     138,   138,   139,   140,   140,   140,   140,   140,   140,   140,
     140,   140,   140,   140,   141,   141,   141,   142,   142,   142,
     143,   143,   143,   144,   144,   145,   145,   145
};

  /* YYR2[YYN] -- Number of symbols on the right hand side of rule YYN.  */
static const yytype_int8 yyr2[] =
{
       0,     2,     2,     2,     2,     2,     1,     2,     2,     0,
       1,     0,     4,     5,     1,     0,     6,     1,     1,     0,
       1,     1,     1,     2,     5,     4,     1,     1,     1,     1,
       2,     2,     3,     1,     0,     1,     2,     1,     2,     1,
       0,     1,     1,     2,     2,     3,     1,     2,     2,     2,
       1,     1,     2,     2,     3,     1,     1,     1,     1,     2,
       3,     2,     2,     2,     1,     3,     2,     1,     1,     1,
       4,     2,     3,     1,     1,     1,     3,     2,     1,     2,
       1,     1,     1,     1,     2,     4,     1,     3,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     3,     1,     1,
       1,     1,     3,     1,     1,     3,     1,     3,     3,     1,
       3,     3,     3,     3,     3,     3,     1,     3,     3,     3,
       3,     1,     2,     2,     2,     1,     1,     1,     1,     1,
       1,     1,     3,     4,     2,     2,     2,     6,     6,     2,
       2,     2,     2,     6,     1,     1,     1,     1,     2,     3,
       1,     1,     1,     1,     1,     1,     3,     2
};


enum { YYENOMEM = -2 };

#define yyerrok         (yyerrstatus = 0)
#define yyclearin       (yychar = RQEMPTY)

#define YYACCEPT        goto yyacceptlab
#define YYABORT         goto yyabortlab
#define YYERROR         goto yyerrorlab


#define YYRECOVERING()  (!!yyerrstatus)

#define YYBACKUP(Token, Value)                                    \
  do                                                              \
    if (yychar == RQEMPTY)                                        \
      {                                                           \
        yychar = (Token);                                         \
        yylval = (Value);                                         \
        YYPOPSTACK (yylen);                                       \
        yystate = *yyssp;                                         \
        goto yybackup;                                            \
      }                                                           \
    else                                                          \
      {                                                           \
        yyerror (&yylloc, scanner, context, query_graph, YY_("syntax error: cannot back up")); \
        YYERROR;                                                  \
      }                                                           \
  while (0)

/* Backward compatibility with an undocumented macro.
   Use RQerror or RQUNDEF. */
#define YYERRCODE RQUNDEF

/* YYLLOC_DEFAULT -- Set CURRENT to span from RHS[1] to RHS[N].
   If N is 0, then set CURRENT to the empty location which ends
   the previous symbol: RHS[0] (always defined).  */

#ifndef YYLLOC_DEFAULT
# define YYLLOC_DEFAULT(Current, Rhs, N)                                \
    do                                                                  \
      if (N)                                                            \
        {                                                               \
          (Current).first_line   = YYRHSLOC (Rhs, 1).first_line;        \
          (Current).first_column = YYRHSLOC (Rhs, 1).first_column;      \
          (Current).last_line    = YYRHSLOC (Rhs, N).last_line;         \
          (Current).last_column  = YYRHSLOC (Rhs, N).last_column;       \
        }                                                               \
      else                                                              \
        {                                                               \
          (Current).first_line   = (Current).last_line   =              \
            YYRHSLOC (Rhs, 0).last_line;                                \
          (Current).first_column = (Current).last_column =              \
            YYRHSLOC (Rhs, 0).last_column;                              \
        }                                                               \
    while (0)
#endif

#define YYRHSLOC(Rhs, K) ((Rhs)[K])


/* Enable debugging if requested.  */
#if RQDEBUG

# ifndef YYFPRINTF
#  include <stdio.h> /* INFRINGES ON USER NAME SPACE */
#  define YYFPRINTF fprintf
# endif

# define YYDPRINTF(Args)                        \
do {                                            \
  if (yydebug)                                  \
    YYFPRINTF Args;                             \
} while (0)


/* YY_LOCATION_PRINT -- Print the location on the stream.
   This macro was not mandated originally: define only if we know
   we won't break user code: when these are the locations we know.  */

# ifndef YY_LOCATION_PRINT
#  if defined RQLTYPE_IS_TRIVIAL && RQLTYPE_IS_TRIVIAL

/* Print *YYLOCP on YYO.  Private, do not rely on its existence. */

YY_ATTRIBUTE_UNUSED
static int
yy_location_print_ (FILE *yyo, YYLTYPE const * const yylocp)
{
  int res = 0;
  int end_col = 0 != yylocp->last_column ? yylocp->last_column - 1 : 0;
  if (0 <= yylocp->first_line)
    {
      res += YYFPRINTF (yyo, "%d", yylocp->first_line);
      if (0 <= yylocp->first_column)
        res += YYFPRINTF (yyo, ".%d", yylocp->first_column);
    }
  if (0 <= yylocp->last_line)
    {
      if (yylocp->first_line < yylocp->last_line)
        {
          res += YYFPRINTF (yyo, "-%d", yylocp->last_line);
          if (0 <= end_col)
            res += YYFPRINTF (yyo, ".%d", end_col);
        }
      else if (0 <= end_col && yylocp->first_column < end_col)
        res += YYFPRINTF (yyo, "-%d", end_col);
    }
  return res;
 }

#   define YY_LOCATION_PRINT(File, Loc)          \
  yy_location_print_ (File, &(Loc))

#  else
#   define YY_LOCATION_PRINT(File, Loc) ((void) 0)
#  endif
# endif /* !defined YY_LOCATION_PRINT */


# define YY_SYMBOL_PRINT(Title, Kind, Value, Location)                    \
do {                                                                      \
  if (yydebug)                                                            \
    {                                                                     \
      YYFPRINTF (stderr, "%s ", Title);                                   \
      yy_symbol_print (stderr,                                            \
                  Kind, Value, Location, scanner, context, query_graph); \
      YYFPRINTF (stderr, "\n");                                           \
    }                                                                     \
} while (0)


/*-----------------------------------.
| Print this symbol's value on YYO.  |
`-----------------------------------*/

static void
yy_symbol_value_print (FILE *yyo,
                       yysymbol_kind_t yykind, YYSTYPE const * const yyvaluep, YYLTYPE const * const yylocationp, yyscan_t scanner, SPARQLParseContext* context, QueryGraph* query_graph)
{
  FILE *yyoutput = yyo;
  YY_USE (yyoutput);
  YY_USE (yylocationp);
  YY_USE (scanner);
  YY_USE (context);
  YY_USE (query_graph);
  if (!yyvaluep)
    return;
# ifdef YYPRINT
  if (yykind < YYNTOKENS)
    YYPRINT (yyo, yytoknum[yykind], *yyvaluep);
# endif
  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  YY_USE (yykind);
  YY_IGNORE_MAYBE_UNINITIALIZED_END
}


/*---------------------------.
| Print this symbol on YYO.  |
`---------------------------*/

static void
yy_symbol_print (FILE *yyo,
                 yysymbol_kind_t yykind, YYSTYPE const * const yyvaluep, YYLTYPE const * const yylocationp, yyscan_t scanner, SPARQLParseContext* context, QueryGraph* query_graph)
{
  YYFPRINTF (yyo, "%s %s (",
             yykind < YYNTOKENS ? "token" : "nterm", yysymbol_name (yykind));

  YY_LOCATION_PRINT (yyo, *yylocationp);
  YYFPRINTF (yyo, ": ");
  yy_symbol_value_print (yyo, yykind, yyvaluep, yylocationp, scanner, context, query_graph);
  YYFPRINTF (yyo, ")");
}

/*------------------------------------------------------------------.
| yy_stack_print -- Print the state stack from its BOTTOM up to its |
| TOP (included).                                                   |
`------------------------------------------------------------------*/

static void
yy_stack_print (yy_state_t *yybottom, yy_state_t *yytop)
{
  YYFPRINTF (stderr, "Stack now");
  for (; yybottom <= yytop; yybottom++)
    {
      int yybot = *yybottom;
      YYFPRINTF (stderr, " %d", yybot);
    }
  YYFPRINTF (stderr, "\n");
}

# define YY_STACK_PRINT(Bottom, Top)                            \
do {                                                            \
  if (yydebug)                                                  \
    yy_stack_print ((Bottom), (Top));                           \
} while (0)


/*------------------------------------------------.
| Report that the YYRULE is going to be reduced.  |
`------------------------------------------------*/

static void
yy_reduce_print (yy_state_t *yyssp, YYSTYPE *yyvsp, YYLTYPE *yylsp,
                 int yyrule, yyscan_t scanner, SPARQLParseContext* context, QueryGraph* query_graph)
{
  int yylno = yyrline[yyrule];
  int yynrhs = yyr2[yyrule];
  int yyi;
  YYFPRINTF (stderr, "Reducing stack by rule %d (line %d):\n",
             yyrule - 1, yylno);
  /* The symbols being reduced.  */
  for (yyi = 0; yyi < yynrhs; yyi++)
    {
      YYFPRINTF (stderr, "   $%d = ", yyi + 1);
      yy_symbol_print (stderr,
                       YY_ACCESSING_SYMBOL (+yyssp[yyi + 1 - yynrhs]),
                       &yyvsp[(yyi + 1) - (yynrhs)],
                       &(yylsp[(yyi + 1) - (yynrhs)]), scanner, context, query_graph);
      YYFPRINTF (stderr, "\n");
    }
}

# define YY_REDUCE_PRINT(Rule)          \
do {                                    \
  if (yydebug)                          \
    yy_reduce_print (yyssp, yyvsp, yylsp, Rule, scanner, context, query_graph); \
} while (0)

/* Nonzero means print parse trace.  It is left uninitialized so that
   multiple parsers can coexist.  */
int yydebug;
#else /* !RQDEBUG */
# define YYDPRINTF(Args) ((void) 0)
# define YY_SYMBOL_PRINT(Title, Kind, Value, Location)
# define YY_STACK_PRINT(Bottom, Top)
# define YY_REDUCE_PRINT(Rule)
#endif /* !RQDEBUG */


/* YYINITDEPTH -- initial size of the parser's stacks.  */
#ifndef YYINITDEPTH
# define YYINITDEPTH 200
#endif

/* YYMAXDEPTH -- maximum size the stacks can grow to (effective only
   if the built-in stack extension method is used).

   Do not make this value too large; the results are undefined if
   YYSTACK_ALLOC_MAXIMUM < YYSTACK_BYTES (YYMAXDEPTH)
   evaluated with infinite-precision integer arithmetic.  */

#ifndef YYMAXDEPTH
# define YYMAXDEPTH 10000
#endif


/* Context of a parse error.  */
typedef struct
{
  yy_state_t *yyssp;
  yysymbol_kind_t yytoken;
  YYLTYPE *yylloc;
} yypcontext_t;

/* Put in YYARG at most YYARGN of the expected tokens given the
   current YYCTX, and return the number of tokens stored in YYARG.  If
   YYARG is null, return the number of expected tokens (guaranteed to
   be less than YYNTOKENS).  Return YYENOMEM on memory exhaustion.
   Return 0 if there are more than YYARGN expected tokens, yet fill
   YYARG up to YYARGN. */
static int
yypcontext_expected_tokens (const yypcontext_t *yyctx,
                            yysymbol_kind_t yyarg[], int yyargn)
{
  /* Actual size of YYARG. */
  int yycount = 0;
  int yyn = yypact[+*yyctx->yyssp];
  if (!yypact_value_is_default (yyn))
    {
      /* Start YYX at -YYN if negative to avoid negative indexes in
         YYCHECK.  In other words, skip the first -YYN actions for
         this state because they are default actions.  */
      int yyxbegin = yyn < 0 ? -yyn : 0;
      /* Stay within bounds of both yycheck and yytname.  */
      int yychecklim = YYLAST - yyn + 1;
      int yyxend = yychecklim < YYNTOKENS ? yychecklim : YYNTOKENS;
      int yyx;
      for (yyx = yyxbegin; yyx < yyxend; ++yyx)
        if (yycheck[yyx + yyn] == yyx && yyx != YYSYMBOL_YYerror
            && !yytable_value_is_error (yytable[yyx + yyn]))
          {
            if (!yyarg)
              ++yycount;
            else if (yycount == yyargn)
              return 0;
            else
              yyarg[yycount++] = YY_CAST (yysymbol_kind_t, yyx);
          }
    }
  if (yyarg && yycount == 0 && 0 < yyargn)
    yyarg[0] = YYSYMBOL_YYEMPTY;
  return yycount;
}




#ifndef yystrlen
# if defined __GLIBC__ && defined _STRING_H
#  define yystrlen(S) (YY_CAST (YYPTRDIFF_T, strlen (S)))
# else
/* Return the length of YYSTR.  */
static YYPTRDIFF_T
yystrlen (const char *yystr)
{
  YYPTRDIFF_T yylen;
  for (yylen = 0; yystr[yylen]; yylen++)
    continue;
  return yylen;
}
# endif
#endif

#ifndef yystpcpy
# if defined __GLIBC__ && defined _STRING_H && defined _GNU_SOURCE
#  define yystpcpy stpcpy
# else
/* Copy YYSRC to YYDEST, returning the address of the terminating '\0' in
   YYDEST.  */
static char *
yystpcpy (char *yydest, const char *yysrc)
{
  char *yyd = yydest;
  const char *yys = yysrc;

  while ((*yyd++ = *yys++) != '\0')
    continue;

  return yyd - 1;
}
# endif
#endif

#ifndef yytnamerr
/* Copy to YYRES the contents of YYSTR after stripping away unnecessary
   quotes and backslashes, so that it's suitable for yyerror.  The
   heuristic is that double-quoting is unnecessary unless the string
   contains an apostrophe, a comma, or backslash (other than
   backslash-backslash).  YYSTR is taken from yytname.  If YYRES is
   null, do not copy; instead, return the length of what the result
   would have been.  */
static YYPTRDIFF_T
yytnamerr (char *yyres, const char *yystr)
{
  if (*yystr == '"')
    {
      YYPTRDIFF_T yyn = 0;
      char const *yyp = yystr;
      for (;;)
        switch (*++yyp)
          {
          case '\'':
          case ',':
            goto do_not_strip_quotes;

          case '\\':
            if (*++yyp != '\\')
              goto do_not_strip_quotes;
            else
              goto append;

          append:
          default:
            if (yyres)
              yyres[yyn] = *yyp;
            yyn++;
            break;

          case '"':
            if (yyres)
              yyres[yyn] = '\0';
            return yyn;
          }
    do_not_strip_quotes: ;
    }

  if (yyres)
    return yystpcpy (yyres, yystr) - yyres;
  else
    return yystrlen (yystr);
}
#endif


static int
yy_syntax_error_arguments (const yypcontext_t *yyctx,
                           yysymbol_kind_t yyarg[], int yyargn)
{
  /* Actual size of YYARG. */
  int yycount = 0;
  /* There are many possibilities here to consider:
     - If this state is a consistent state with a default action, then
       the only way this function was invoked is if the default action
       is an error action.  In that case, don't check for expected
       tokens because there are none.
     - The only way there can be no lookahead present (in yychar) is if
       this state is a consistent state with a default action.  Thus,
       detecting the absence of a lookahead is sufficient to determine
       that there is no unexpected or expected token to report.  In that
       case, just report a simple "syntax error".
     - Don't assume there isn't a lookahead just because this state is a
       consistent state with a default action.  There might have been a
       previous inconsistent state, consistent state with a non-default
       action, or user semantic action that manipulated yychar.
     - Of course, the expected token list depends on states to have
       correct lookahead information, and it depends on the parser not
       to perform extra reductions after fetching a lookahead from the
       scanner and before detecting a syntax error.  Thus, state merging
       (from LALR or IELR) and default reductions corrupt the expected
       token list.  However, the list is correct for canonical LR with
       one exception: it will still contain any token that will not be
       accepted due to an error action in a later state.
  */
  if (yyctx->yytoken != YYSYMBOL_YYEMPTY)
    {
      int yyn;
      if (yyarg)
        yyarg[yycount] = yyctx->yytoken;
      ++yycount;
      yyn = yypcontext_expected_tokens (yyctx,
                                        yyarg ? yyarg + 1 : yyarg, yyargn - 1);
      if (yyn == YYENOMEM)
        return YYENOMEM;
      else
        yycount += yyn;
    }
  return yycount;
}

/* Copy into *YYMSG, which is of size *YYMSG_ALLOC, an error message
   about the unexpected token YYTOKEN for the state stack whose top is
   YYSSP.

   Return 0 if *YYMSG was successfully written.  Return -1 if *YYMSG is
   not large enough to hold the message.  In that case, also set
   *YYMSG_ALLOC to the required number of bytes.  Return YYENOMEM if the
   required number of bytes is too large to store.  */
static int
yysyntax_error (YYPTRDIFF_T *yymsg_alloc, char **yymsg,
                const yypcontext_t *yyctx)
{
  enum { YYARGS_MAX = 5 };
  /* Internationalized format string. */
  const char *yyformat = YY_NULLPTR;
  /* Arguments of yyformat: reported tokens (one for the "unexpected",
     one per "expected"). */
  yysymbol_kind_t yyarg[YYARGS_MAX];
  /* Cumulated lengths of YYARG.  */
  YYPTRDIFF_T yysize = 0;

  /* Actual size of YYARG. */
  int yycount = yy_syntax_error_arguments (yyctx, yyarg, YYARGS_MAX);
  if (yycount == YYENOMEM)
    return YYENOMEM;

  switch (yycount)
    {
#define YYCASE_(N, S)                       \
      case N:                               \
        yyformat = S;                       \
        break
    default: /* Avoid compiler warnings. */
      YYCASE_(0, YY_("syntax error"));
      YYCASE_(1, YY_("syntax error, unexpected %s"));
      YYCASE_(2, YY_("syntax error, unexpected %s, expecting %s"));
      YYCASE_(3, YY_("syntax error, unexpected %s, expecting %s or %s"));
      YYCASE_(4, YY_("syntax error, unexpected %s, expecting %s or %s or %s"));
      YYCASE_(5, YY_("syntax error, unexpected %s, expecting %s or %s or %s or %s"));
#undef YYCASE_
    }

  /* Compute error message size.  Don't count the "%s"s, but reserve
     room for the terminator.  */
  yysize = yystrlen (yyformat) - 2 * yycount + 1;
  {
    int yyi;
    for (yyi = 0; yyi < yycount; ++yyi)
      {
        YYPTRDIFF_T yysize1
          = yysize + yytnamerr (YY_NULLPTR, yytname[yyarg[yyi]]);
        if (yysize <= yysize1 && yysize1 <= YYSTACK_ALLOC_MAXIMUM)
          yysize = yysize1;
        else
          return YYENOMEM;
      }
  }

  if (*yymsg_alloc < yysize)
    {
      *yymsg_alloc = 2 * yysize;
      if (! (yysize <= *yymsg_alloc
             && *yymsg_alloc <= YYSTACK_ALLOC_MAXIMUM))
        *yymsg_alloc = YYSTACK_ALLOC_MAXIMUM;
      return -1;
    }

  /* Avoid sprintf, as that infringes on the user's name space.
     Don't have undefined behavior even if the translation
     produced a string with the wrong number of "%s"s.  */
  {
    char *yyp = *yymsg;
    int yyi = 0;
    while ((*yyp = *yyformat) != '\0')
      if (*yyp == '%' && yyformat[1] == 's' && yyi < yycount)
        {
          yyp += yytnamerr (yyp, yytname[yyarg[yyi++]]);
          yyformat += 2;
        }
      else
        {
          ++yyp;
          ++yyformat;
        }
  }
  return 0;
}


/*-----------------------------------------------.
| Release the memory associated to this symbol.  |
`-----------------------------------------------*/

static void
yydestruct (const char *yymsg,
            yysymbol_kind_t yykind, YYSTYPE *yyvaluep, YYLTYPE *yylocationp, yyscan_t scanner, SPARQLParseContext* context, QueryGraph* query_graph)
{
  YY_USE (yyvaluep);
  YY_USE (yylocationp);
  YY_USE (scanner);
  YY_USE (context);
  YY_USE (query_graph);
  if (!yymsg)
    yymsg = "Deleting";
  YY_SYMBOL_PRINT (yymsg, yykind, yyvaluep, yylocationp);

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  switch (yykind)
    {
    case YYSYMBOL_DECIMAL: /* DECIMAL  */
#line 158 "sparql_parser.y"
            { free(((*yyvaluep).strval)); }
#line 1581 "sparql_parser.cpp"
        break;

    case YYSYMBOL_DOUBLE: /* DOUBLE  */
#line 158 "sparql_parser.y"
            { free(((*yyvaluep).strval)); }
#line 1587 "sparql_parser.cpp"
        break;

    case YYSYMBOL_INTEGER: /* INTEGER  */
#line 158 "sparql_parser.y"
            { free(((*yyvaluep).strval)); }
#line 1593 "sparql_parser.cpp"
        break;

    case YYSYMBOL_VARIABLE: /* VARIABLE  */
#line 158 "sparql_parser.y"
            { free(((*yyvaluep).strval)); }
#line 1599 "sparql_parser.cpp"
        break;

    case YYSYMBOL_IRI: /* IRI  */
#line 158 "sparql_parser.y"
            { free(((*yyvaluep).strval)); }
#line 1605 "sparql_parser.cpp"
        break;

    case YYSYMBOL_IDENTIFIER: /* IDENTIFIER  */
#line 158 "sparql_parser.y"
            { free(((*yyvaluep).strval)); }
#line 1611 "sparql_parser.cpp"
        break;

    case YYSYMBOL_STRING: /* STRING  */
#line 158 "sparql_parser.y"
            { free(((*yyvaluep).strval)); }
#line 1617 "sparql_parser.cpp"
        break;

    case YYSYMBOL_LANGTAG: /* LANGTAG  */
#line 158 "sparql_parser.y"
            { free(((*yyvaluep).strval)); }
#line 1623 "sparql_parser.cpp"
        break;

    case YYSYMBOL_prefix: /* prefix  */
#line 158 "sparql_parser.y"
            { free(((*yyvaluep).strval)); }
#line 1629 "sparql_parser.cpp"
        break;

    case YYSYMBOL_literal: /* literal  */
#line 158 "sparql_parser.y"
            { free(((*yyvaluep).strval)); }
#line 1635 "sparql_parser.cpp"
        break;

    case YYSYMBOL_rdf_literal: /* rdf_literal  */
#line 158 "sparql_parser.y"
            { free(((*yyvaluep).strval)); }
#line 1641 "sparql_parser.cpp"
        break;

    case YYSYMBOL_numeric_literal: /* numeric_literal  */
#line 158 "sparql_parser.y"
            { free(((*yyvaluep).strval)); }
#line 1647 "sparql_parser.cpp"
        break;

    case YYSYMBOL_boolean_literal: /* boolean_literal  */
#line 158 "sparql_parser.y"
            { free(((*yyvaluep).strval)); }
#line 1653 "sparql_parser.cpp"
        break;

    case YYSYMBOL_iri: /* iri  */
#line 158 "sparql_parser.y"
            { free(((*yyvaluep).strval)); }
#line 1659 "sparql_parser.cpp"
        break;

      default:
        break;
    }
  YY_IGNORE_MAYBE_UNINITIALIZED_END
}






/*----------.
| yyparse.  |
`----------*/

int
yyparse (yyscan_t scanner, SPARQLParseContext* context, QueryGraph* query_graph)
{
/* Lookahead token kind.  */
int yychar;


/* The semantic value of the lookahead symbol.  */
/* Default value used for initialization, for pacifying older GCCs
   or non-GCC compilers.  */
YY_INITIAL_VALUE (static YYSTYPE yyval_default;)
YYSTYPE yylval YY_INITIAL_VALUE (= yyval_default);

/* Location data for the lookahead symbol.  */
static YYLTYPE yyloc_default
# if defined RQLTYPE_IS_TRIVIAL && RQLTYPE_IS_TRIVIAL
  = { 1, 1, 1, 1 }
# endif
;
YYLTYPE yylloc = yyloc_default;

    /* Number of syntax errors so far.  */
    int yynerrs = 0;

    yy_state_fast_t yystate = 0;
    /* Number of tokens to shift before error messages enabled.  */
    int yyerrstatus = 0;

    /* Refer to the stacks through separate pointers, to allow yyoverflow
       to reallocate them elsewhere.  */

    /* Their size.  */
    YYPTRDIFF_T yystacksize = YYINITDEPTH;

    /* The state stack: array, bottom, top.  */
    yy_state_t yyssa[YYINITDEPTH];
    yy_state_t *yyss = yyssa;
    yy_state_t *yyssp = yyss;

    /* The semantic value stack: array, bottom, top.  */
    YYSTYPE yyvsa[YYINITDEPTH];
    YYSTYPE *yyvs = yyvsa;
    YYSTYPE *yyvsp = yyvs;

    /* The location stack: array, bottom, top.  */
    YYLTYPE yylsa[YYINITDEPTH];
    YYLTYPE *yyls = yylsa;
    YYLTYPE *yylsp = yyls;

  int yyn;
  /* The return value of yyparse.  */
  int yyresult;
  /* Lookahead symbol kind.  */
  yysymbol_kind_t yytoken = YYSYMBOL_YYEMPTY;
  /* The variables used to return semantic value and location from the
     action routines.  */
  YYSTYPE yyval;
  YYLTYPE yyloc;

  /* The locations where the error started and ended.  */
  YYLTYPE yyerror_range[3];

  /* Buffer for error messages, and its allocated size.  */
  char yymsgbuf[128];
  char *yymsg = yymsgbuf;
  YYPTRDIFF_T yymsg_alloc = sizeof yymsgbuf;

#define YYPOPSTACK(N)   (yyvsp -= (N), yyssp -= (N), yylsp -= (N))

  /* The number of symbols on the RHS of the reduced rule.
     Keep to zero when no symbol should be popped.  */
  int yylen = 0;

  YYDPRINTF ((stderr, "Starting parse\n"));

  yychar = RQEMPTY; /* Cause a token to be read.  */
  yylsp[0] = yylloc;
  goto yysetstate;


/*------------------------------------------------------------.
| yynewstate -- push a new state, which is found in yystate.  |
`------------------------------------------------------------*/
yynewstate:
  /* In all cases, when you get here, the value and location stacks
     have just been pushed.  So pushing a state here evens the stacks.  */
  yyssp++;


/*--------------------------------------------------------------------.
| yysetstate -- set current state (the top of the stack) to yystate.  |
`--------------------------------------------------------------------*/
yysetstate:
  YYDPRINTF ((stderr, "Entering state %d\n", yystate));
  YY_ASSERT (0 <= yystate && yystate < YYNSTATES);
  YY_IGNORE_USELESS_CAST_BEGIN
  *yyssp = YY_CAST (yy_state_t, yystate);
  YY_IGNORE_USELESS_CAST_END
  YY_STACK_PRINT (yyss, yyssp);

  if (yyss + yystacksize - 1 <= yyssp)
#if !defined yyoverflow && !defined YYSTACK_RELOCATE
    goto yyexhaustedlab;
#else
    {
      /* Get the current used size of the three stacks, in elements.  */
      YYPTRDIFF_T yysize = yyssp - yyss + 1;

# if defined yyoverflow
      {
        /* Give user a chance to reallocate the stack.  Use copies of
           these so that the &'s don't force the real ones into
           memory.  */
        yy_state_t *yyss1 = yyss;
        YYSTYPE *yyvs1 = yyvs;
        YYLTYPE *yyls1 = yyls;

        /* Each stack pointer address is followed by the size of the
           data in use in that stack, in bytes.  This used to be a
           conditional around just the two extra args, but that might
           be undefined if yyoverflow is a macro.  */
        yyoverflow (YY_("memory exhausted"),
                    &yyss1, yysize * YYSIZEOF (*yyssp),
                    &yyvs1, yysize * YYSIZEOF (*yyvsp),
                    &yyls1, yysize * YYSIZEOF (*yylsp),
                    &yystacksize);
        yyss = yyss1;
        yyvs = yyvs1;
        yyls = yyls1;
      }
# else /* defined YYSTACK_RELOCATE */
      /* Extend the stack our own way.  */
      if (YYMAXDEPTH <= yystacksize)
        goto yyexhaustedlab;
      yystacksize *= 2;
      if (YYMAXDEPTH < yystacksize)
        yystacksize = YYMAXDEPTH;

      {
        yy_state_t *yyss1 = yyss;
        union yyalloc *yyptr =
          YY_CAST (union yyalloc *,
                   YYSTACK_ALLOC (YY_CAST (YYSIZE_T, YYSTACK_BYTES (yystacksize))));
        if (! yyptr)
          goto yyexhaustedlab;
        YYSTACK_RELOCATE (yyss_alloc, yyss);
        YYSTACK_RELOCATE (yyvs_alloc, yyvs);
        YYSTACK_RELOCATE (yyls_alloc, yyls);
#  undef YYSTACK_RELOCATE
        if (yyss1 != yyssa)
          YYSTACK_FREE (yyss1);
      }
# endif

      yyssp = yyss + yysize - 1;
      yyvsp = yyvs + yysize - 1;
      yylsp = yyls + yysize - 1;

      YY_IGNORE_USELESS_CAST_BEGIN
      YYDPRINTF ((stderr, "Stack size increased to %ld\n",
                  YY_CAST (long, yystacksize)));
      YY_IGNORE_USELESS_CAST_END

      if (yyss + yystacksize - 1 <= yyssp)
        YYABORT;
    }
#endif /* !defined yyoverflow && !defined YYSTACK_RELOCATE */

  if (yystate == YYFINAL)
    YYACCEPT;

  goto yybackup;


/*-----------.
| yybackup.  |
`-----------*/
yybackup:
  /* Do appropriate processing given the current state.  Read a
     lookahead token if we need one and don't already have one.  */

  /* First try to decide what to do without reference to lookahead token.  */
  yyn = yypact[yystate];
  if (yypact_value_is_default (yyn))
    goto yydefault;

  /* Not known => get a lookahead token if don't already have one.  */

  /* YYCHAR is either empty, or end-of-input, or a valid lookahead.  */
  if (yychar == RQEMPTY)
    {
      YYDPRINTF ((stderr, "Reading a token\n"));
      yychar = yylex (&yylval, &yylloc, scanner);
    }

  if (yychar <= RQEOF)
    {
      yychar = RQEOF;
      yytoken = YYSYMBOL_YYEOF;
      YYDPRINTF ((stderr, "Now at end of input.\n"));
    }
  else if (yychar == RQerror)
    {
      /* The scanner already issued an error message, process directly
         to error recovery.  But do not keep the error token as
         lookahead, it is too special and may lead us to an endless
         loop in error recovery. */
      yychar = RQUNDEF;
      yytoken = YYSYMBOL_YYerror;
      yyerror_range[1] = yylloc;
      goto yyerrlab1;
    }
  else
    {
      yytoken = YYTRANSLATE (yychar);
      YY_SYMBOL_PRINT ("Next token is", yytoken, &yylval, &yylloc);
    }

  /* If the proper action on seeing token YYTOKEN is to reduce or to
     detect an error, take that action.  */
  yyn += yytoken;
  if (yyn < 0 || YYLAST < yyn || yycheck[yyn] != yytoken)
    goto yydefault;
  yyn = yytable[yyn];
  if (yyn <= 0)
    {
      if (yytable_value_is_error (yyn))
        goto yyerrlab;
      yyn = -yyn;
      goto yyreduce;
    }

  /* Count tokens shifted since error; after three, turn off error
     status.  */
  if (yyerrstatus)
    yyerrstatus--;

  /* Shift the lookahead token.  */
  YY_SYMBOL_PRINT ("Shifting", yytoken, &yylval, &yylloc);
  yystate = yyn;
  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  *++yyvsp = yylval;
  YY_IGNORE_MAYBE_UNINITIALIZED_END
  *++yylsp = yylloc;

  /* Discard the shifted token.  */
  yychar = RQEMPTY;
  goto yynewstate;


/*-----------------------------------------------------------.
| yydefault -- do the default action for the current state.  |
`-----------------------------------------------------------*/
yydefault:
  yyn = yydefact[yystate];
  if (yyn == 0)
    goto yyerrlab;
  goto yyreduce;


/*-----------------------------.
| yyreduce -- do a reduction.  |
`-----------------------------*/
yyreduce:
  /* yyn is the number of a rule to reduce with.  */
  yylen = yyr2[yyn];

  /* If YYLEN is nonzero, implement the default value of the action:
     '$$ = $1'.

     Otherwise, the following line sets YYVAL to garbage.
     This behavior is undocumented and Bison
     users should not rely upon it.  Assigning to YYVAL
     unconditionally makes the parser a bit smaller, and it avoids a
     GCC warning that YYVAL may be used uninitialized.  */
  yyval = yyvsp[1-yylen];

  /* Default location. */
  YYLLOC_DEFAULT (yyloc, (yylsp - yylen), yylen);
  yyerror_range[1] = yyloc;
  YY_REDUCE_PRINT (yyn);
  switch (yyn)
    {
  case 2: /* query: prologue select_query  */
#line 186 "sparql_parser.y"
                        { YYACCEPT; }
#line 1963 "sparql_parser.cpp"
    break;

  case 3: /* query: prologue construct_query  */
#line 188 "sparql_parser.y"
                           { YYACCEPT; }
#line 1969 "sparql_parser.cpp"
    break;

  case 4: /* query: prologue describe_query  */
#line 190 "sparql_parser.y"
                          { YYACCEPT; }
#line 1975 "sparql_parser.cpp"
    break;

  case 5: /* query: prologue ask_query  */
#line 192 "sparql_parser.y"
                     { YYACCEPT; }
#line 1981 "sparql_parser.cpp"
    break;

  case 6: /* query: END_OF_FILE  */
#line 194 "sparql_parser.y"
              { YYABORT; }
#line 1987 "sparql_parser.cpp"
    break;

  case 8: /* base_declaration: BASE IRI  */
#line 204 "sparql_parser.y"
           { context->base = (yyvsp[0].strval); free((yyvsp[0].strval)); (yyvsp[0].strval) = nullptr; }
#line 1993 "sparql_parser.cpp"
    break;

  case 12: /* prefix_list: PREFIX prefix ':' IRI  */
#line 215 "sparql_parser.y"
                        {
    context->prefixes[(yyvsp[-2].strval)] = (yyvsp[0].strval);
    free((yyvsp[-2].strval));
    (yyvsp[-2].strval) = nullptr;
    free((yyvsp[0].strval));
    (yyvsp[0].strval) = nullptr;
  }
#line 2005 "sparql_parser.cpp"
    break;

  case 13: /* prefix_list: prefix_list PREFIX prefix ':' IRI  */
#line 223 "sparql_parser.y"
                                    {
    context->prefixes[(yyvsp[-2].strval)] = (yyvsp[0].strval);
    free((yyvsp[-2].strval));
    (yyvsp[-2].strval) = nullptr;
    free((yyvsp[0].strval));
    (yyvsp[0].strval) = nullptr;
  }
#line 2017 "sparql_parser.cpp"
    break;

  case 14: /* prefix: IDENTIFIER  */
#line 233 "sparql_parser.y"
             { (yyval.strval) = (yyvsp[0].strval); }
#line 2023 "sparql_parser.cpp"
    break;

  case 15: /* prefix: %empty  */
#line 235 "sparql_parser.y"
              { (yyval.strval) = strdup(""); }
#line 2029 "sparql_parser.cpp"
    break;

  case 16: /* select_query: SELECT duplicate_modifier select_list dataset_clause where_clause solution_modifier  */
#line 240 "sparql_parser.y"
                                                                                      {
    query_graph->setQueryForm(SelectQuery);
  }
#line 2037 "sparql_parser.cpp"
    break;

  case 17: /* duplicate_modifier: DISTINCT  */
#line 246 "sparql_parser.y"
           { query_graph->setDuplicateModifier(Distinct); }
#line 2043 "sparql_parser.cpp"
    break;

  case 18: /* duplicate_modifier: REDUCED  */
#line 248 "sparql_parser.y"
          { query_graph->setDuplicateModifier(Reduced); }
#line 2049 "sparql_parser.cpp"
    break;

  case 21: /* select_list: '*'  */
#line 256 "sparql_parser.y"
      { context->all_variables = true; }
#line 2055 "sparql_parser.cpp"
    break;

  case 22: /* variables: VARIABLE  */
#line 260 "sparql_parser.y"
           {
    query_graph->addProjection((yyvsp[0].strval));
    free((yyvsp[0].strval));
    (yyvsp[0].strval) = nullptr;
  }
#line 2065 "sparql_parser.cpp"
    break;

  case 23: /* variables: variables VARIABLE  */
#line 266 "sparql_parser.y"
                     {
    query_graph->addProjection((yyvsp[0].strval));
    free((yyvsp[0].strval));
    (yyvsp[0].strval) = nullptr;
  }
#line 2075 "sparql_parser.cpp"
    break;

  case 24: /* construct_query: CONSTRUCT construct_template dataset_clause where_clause solution_modifier  */
#line 275 "sparql_parser.y"
                                                                             {
    query_graph->setQueryForm(ConstructQuery);
  }
#line 2083 "sparql_parser.cpp"
    break;

  case 25: /* describe_query: DESCRIBE describe_list dataset_clause where_clause  */
#line 282 "sparql_parser.y"
                                                     {
    query_graph->setQueryForm(DescribeQuery);
  }
#line 2091 "sparql_parser.cpp"
    break;

  case 27: /* describe_list: '*'  */
#line 290 "sparql_parser.y"
      { context->all_variables = true; }
#line 2097 "sparql_parser.cpp"
    break;

  case 28: /* var_or_iri_list: VARIABLE  */
#line 294 "sparql_parser.y"
           { free((yyvsp[0].strval)); (yyvsp[0].strval) = nullptr; }
#line 2103 "sparql_parser.cpp"
    break;

  case 29: /* var_or_iri_list: iri  */
#line 296 "sparql_parser.y"
      { free((yyvsp[0].strval)); (yyvsp[0].strval) = nullptr; }
#line 2109 "sparql_parser.cpp"
    break;

  case 30: /* var_or_iri_list: var_or_iri_list VARIABLE  */
#line 298 "sparql_parser.y"
                           { free((yyvsp[0].strval)); (yyvsp[0].strval) = nullptr; }
#line 2115 "sparql_parser.cpp"
    break;

  case 31: /* var_or_iri_list: var_or_iri_list iri  */
#line 300 "sparql_parser.y"
                      { free((yyvsp[0].strval)); (yyvsp[0].strval) = nullptr; }
#line 2121 "sparql_parser.cpp"
    break;

  case 32: /* ask_query: ASK dataset_clause where_clause  */
#line 305 "sparql_parser.y"
                                  {
    query_graph->setQueryForm(AskQuery);
  }
#line 2129 "sparql_parser.cpp"
    break;

  case 35: /* from_clause: FROM  */
#line 318 "sparql_parser.y"
       {
    yyerror (&yylloc, scanner, context, query_graph, YY_("syntax error, FROM clause is currently not supported, expecting ':' or 'a'"));
    YYERROR;
  }
#line 2138 "sparql_parser.cpp"
    break;

  case 36: /* where_clause: WHERE group_graph_pattern  */
#line 326 "sparql_parser.y"
                            { query_graph->pattern = (yyvsp[0].pattern); }
#line 2144 "sparql_parser.cpp"
    break;

  case 37: /* where_clause: group_graph_pattern  */
#line 328 "sparql_parser.y"
                      { query_graph->pattern = (yyvsp[0].pattern); }
#line 2150 "sparql_parser.cpp"
    break;

  case 40: /* solution_modifier: %empty  */
#line 337 "sparql_parser.y"
              { yyunget(scanner); }
#line 2156 "sparql_parser.cpp"
    break;

  case 48: /* order_condition: ASC bracketted_expression  */
#line 363 "sparql_parser.y"
                            {
    Order order;
    order.ascending = true;
    order.condition = (yyvsp[0].expression);
    query_graph->addOrder(order);
  }
#line 2167 "sparql_parser.cpp"
    break;

  case 49: /* order_condition: DESC bracketted_expression  */
#line 370 "sparql_parser.y"
                             {
    Order order;
    order.ascending = false;
    order.condition = (yyvsp[0].expression);
    query_graph->addOrder(order);
  }
#line 2178 "sparql_parser.cpp"
    break;

  case 50: /* order_condition: constraint  */
#line 377 "sparql_parser.y"
             {
    Order order;
    order.ascending = true;
    order.condition = (yyvsp[0].expression);
    query_graph->addOrder(order);
  }
#line 2189 "sparql_parser.cpp"
    break;

  case 51: /* order_condition: VARIABLE  */
#line 384 "sparql_parser.y"
           {
    Order order;
    order.ascending = true;
    order.condition = query_graph->createExpression();
    order.condition->type = QueryExpression::Variable;
    order.condition->value = (yyvsp[0].strval);
    query_graph->addOrder(order);
    free((yyvsp[0].strval));
    (yyvsp[0].strval) = nullptr;
  }
#line 2204 "sparql_parser.cpp"
    break;

  case 52: /* limit_clause: LIMIT INTEGER  */
#line 398 "sparql_parser.y"
                { query_graph->setLimit(atol((yyvsp[0].strval))); free((yyvsp[0].strval)); (yyvsp[0].strval) = nullptr; }
#line 2210 "sparql_parser.cpp"
    break;

  case 53: /* offset_clause: OFFSET INTEGER  */
#line 403 "sparql_parser.y"
                 { query_graph->setOffset(atol((yyvsp[0].strval))); free((yyvsp[0].strval)); (yyvsp[0].strval) = nullptr; }
#line 2216 "sparql_parser.cpp"
    break;

  case 54: /* group_graph_pattern: pattern_lbracket graph_pattern pattern_rbracket  */
#line 408 "sparql_parser.y"
                                                  {
    (yyval.pattern) = (yyvsp[-1].pattern);
  }
#line 2224 "sparql_parser.cpp"
    break;

  case 55: /* pattern_lbracket: '{'  */
#line 414 "sparql_parser.y"
      {
    context->parent_pattern = context->pattern;
    context->pattern = query_graph->createPattern();
    context->pattern->type = QueryPattern::Basic;
  }
#line 2234 "sparql_parser.cpp"
    break;

  case 56: /* pattern_rbracket: '}'  */
#line 422 "sparql_parser.y"
      {
    context->pattern = context->parent_pattern;
  }
#line 2242 "sparql_parser.cpp"
    break;

  case 57: /* graph_pattern: basic_graph_pattern  */
#line 428 "sparql_parser.y"
                      { (yyval.pattern) = context->pattern; }
#line 2248 "sparql_parser.cpp"
    break;

  case 58: /* graph_pattern: union_graph_pattern  */
#line 430 "sparql_parser.y"
                      { (yyval.pattern) = context->pattern; }
#line 2254 "sparql_parser.cpp"
    break;

  case 63: /* optional_graph_pattern: OPTIONAL group_graph_pattern  */
#line 446 "sparql_parser.y"
                               {  context->pattern->optionals.push_back((yyvsp[0].pattern)); }
#line 2260 "sparql_parser.cpp"
    break;

  case 64: /* union_graph_pattern: group_graph_pattern  */
#line 451 "sparql_parser.y"
                      {
    context->pattern->sub_patterns.push_back((yyvsp[0].pattern));
    context->pattern->type = QueryPattern::Union;
  }
#line 2269 "sparql_parser.cpp"
    break;

  case 65: /* union_graph_pattern: union_graph_pattern UNION group_graph_pattern  */
#line 456 "sparql_parser.y"
                                                {
    context->pattern->sub_patterns.push_back((yyvsp[0].pattern));
  }
#line 2277 "sparql_parser.cpp"
    break;

  case 66: /* filter: FILTER constraint  */
#line 463 "sparql_parser.y"
                    { context->pattern->filters.push_back((yyvsp[0].expression)); }
#line 2283 "sparql_parser.cpp"
    break;

  case 67: /* constraint: bracketted_expression  */
#line 468 "sparql_parser.y"
                        { (yyval.expression) = (yyvsp[0].expression); }
#line 2289 "sparql_parser.cpp"
    break;

  case 68: /* constraint: built_in_call  */
#line 470 "sparql_parser.y"
                { (yyval.expression) = (yyvsp[0].expression); }
#line 2295 "sparql_parser.cpp"
    break;

  case 69: /* constraint: function_call  */
#line 472 "sparql_parser.y"
                { (yyval.expression) = (yyvsp[0].expression); }
#line 2301 "sparql_parser.cpp"
    break;

  case 70: /* function_call: iri '(' expression_list ')'  */
#line 477 "sparql_parser.y"
                              {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::Function;
    exp->value = (yyvsp[-3].strval);
    exp->arg_list.insert(exp->arg_list.end(), (yyvsp[-1].expression)->arg_list.begin(), (yyvsp[-1].expression)->arg_list.end());
    free((yyvsp[-3].strval));
    (yyvsp[-3].strval) = nullptr;
    query_graph->removeExpression((yyvsp[-1].expression));
  }
#line 2315 "sparql_parser.cpp"
    break;

  case 71: /* function_call: iri NIL  */
#line 487 "sparql_parser.y"
          {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::Function;
    exp->value = (yyvsp[-1].strval);
    free((yyvsp[-1].strval));
    (yyvsp[-1].strval) = nullptr;
  }
#line 2327 "sparql_parser.cpp"
    break;

  case 72: /* construct_template: construct_lbracket construct_triples construct_rbracket  */
#line 498 "sparql_parser.y"
                                                          {
    query_graph->construct_pattern = context->pattern;
  }
#line 2335 "sparql_parser.cpp"
    break;

  case 73: /* construct_lbracket: '{'  */
#line 504 "sparql_parser.y"
      {
    context->pattern = query_graph->createPattern();
    context->pattern->type = QueryPattern::Basic;
  }
#line 2344 "sparql_parser.cpp"
    break;

  case 77: /* triples_same_subject: subject1 property_list  */
#line 523 "sparql_parser.y"
                         {
    QueryResource *subject = context->subjects.back();
    context->resource_pool.free(subject);
    context->subjects.pop_back();
  }
#line 2354 "sparql_parser.cpp"
    break;

  case 78: /* triples_same_subject: subject2  */
#line 529 "sparql_parser.y"
           {
    QueryResource *subject = context->subjects.back();
    context->resource_pool.free(subject);
    context->subjects.pop_back();
  }
#line 2364 "sparql_parser.cpp"
    break;

  case 79: /* triples_same_subject: subject2 property_list  */
#line 535 "sparql_parser.y"
                         {
    QueryResource *subject = context->subjects.back();
    context->resource_pool.free(subject);
    context->subjects.pop_back();
  }
#line 2374 "sparql_parser.cpp"
    break;

  case 80: /* subject1: var_element  */
#line 543 "sparql_parser.y"
              { context->subjects.push_back((yyvsp[0].resource)); }
#line 2380 "sparql_parser.cpp"
    break;

  case 81: /* subject1: iri_element  */
#line 545 "sparql_parser.y"
              { context->subjects.push_back((yyvsp[0].resource)); }
#line 2386 "sparql_parser.cpp"
    break;

  case 82: /* subject1: blank_node_element  */
#line 547 "sparql_parser.y"
                     { context->subjects.push_back((yyvsp[0].resource)); }
#line 2392 "sparql_parser.cpp"
    break;

  case 83: /* subject2: blank_node_property_list  */
#line 551 "sparql_parser.y"
                           { context->subjects.push_back((yyvsp[0].resource)); }
#line 2398 "sparql_parser.cpp"
    break;

  case 84: /* property_list: verb object_list  */
#line 556 "sparql_parser.y"
                   {
    QueryResource *predicate = context->predicates.back();
    context->resource_pool.free(predicate);
    context->predicates.pop_back();
  }
#line 2408 "sparql_parser.cpp"
    break;

  case 85: /* property_list: property_list ';' verb object_list  */
#line 562 "sparql_parser.y"
                                     {
    QueryResource *predicate = context->predicates.back();
    context->resource_pool.free(predicate);
    context->predicates.pop_back();
  }
#line 2418 "sparql_parser.cpp"
    break;

  case 86: /* object_list: object  */
#line 571 "sparql_parser.y"
         {
    QueryResource *object = (yyvsp[0].resource);
    int node = query_graph->addNode(QueryNode(*context->subjects.back(), *context->predicates.back(), *object));
    context->pattern->nodes.push_back(node);
    context->resource_pool.free(object);
  }
#line 2429 "sparql_parser.cpp"
    break;

  case 87: /* object_list: object_list ',' object  */
#line 578 "sparql_parser.y"
                         {
    QueryResource *object = (yyvsp[0].resource);
    int node = query_graph->addNode(QueryNode(*context->subjects.back(), *context->predicates.back(), *object));
    context->pattern->nodes.push_back(node);
    context->resource_pool.free(object);
  }
#line 2440 "sparql_parser.cpp"
    break;

  case 88: /* object: var_element  */
#line 588 "sparql_parser.y"
              { (yyval.resource) = (yyvsp[0].resource); }
#line 2446 "sparql_parser.cpp"
    break;

  case 89: /* object: iri_element  */
#line 590 "sparql_parser.y"
              { (yyval.resource) = (yyvsp[0].resource); }
#line 2452 "sparql_parser.cpp"
    break;

  case 90: /* object: literal_element  */
#line 592 "sparql_parser.y"
                  { (yyval.resource) = (yyvsp[0].resource); }
#line 2458 "sparql_parser.cpp"
    break;

  case 91: /* object: blank_node_element  */
#line 594 "sparql_parser.y"
                     { (yyval.resource) = (yyvsp[0].resource); }
#line 2464 "sparql_parser.cpp"
    break;

  case 92: /* object: blank_node_property_list  */
#line 596 "sparql_parser.y"
                           { (yyval.resource) = (yyvsp[0].resource); }
#line 2470 "sparql_parser.cpp"
    break;

  case 93: /* verb: var_element  */
#line 601 "sparql_parser.y"
              { context->predicates.push_back((yyvsp[0].resource)); }
#line 2476 "sparql_parser.cpp"
    break;

  case 94: /* verb: iri_element  */
#line 603 "sparql_parser.y"
              { context->predicates.push_back((yyvsp[0].resource)); }
#line 2482 "sparql_parser.cpp"
    break;

  case 95: /* verb: type_element  */
#line 605 "sparql_parser.y"
               { context->predicates.push_back((yyvsp[0].resource)); }
#line 2488 "sparql_parser.cpp"
    break;

  case 96: /* type_element: IDENTIFIER  */
#line 609 "sparql_parser.y"
             {
    if(strcmp((yyvsp[0].strval), "a") != 0 && strcmp((yyvsp[0].strval), "A") != 0){
      yyerror (&yylloc, scanner, context, query_graph, YY_("syntax error, unexpected IDENTIFIER, expecting ':' or 'a'"));
      YYERROR;
    }
    free((yyvsp[0].strval));
    (yyvsp[0].strval) = nullptr;
    QueryResource *resource = context->resource_pool.alloc();
    resource->type = QueryResource::IRI;
    resource->value = RDFVocabulary::RDF_TYPE;
    (yyval.resource) = resource;
  }
#line 2505 "sparql_parser.cpp"
    break;

  case 97: /* blank_node_property_list: open_square_bracket property_list ']'  */
#line 625 "sparql_parser.y"
                                        {
    QueryResource *blankNode = context->subjects.back();
    context->subjects.pop_back();
    (yyval.resource) = blankNode;
  }
#line 2515 "sparql_parser.cpp"
    break;

  case 98: /* open_square_bracket: '['  */
#line 633 "sparql_parser.y"
      {
    QueryResource *subject = context->resource_pool.alloc();
    subject->type = QueryResource::Variable;
    subject->value = context->bknode_id_gen.generate();
    context->subjects.push_back(subject);
  }
#line 2526 "sparql_parser.cpp"
    break;

  case 99: /* var_element: VARIABLE  */
#line 642 "sparql_parser.y"
           {
    QueryResource *resource = context->resource_pool.alloc();
    resource->type = QueryResource::Variable;
    resource->value = (yyvsp[0].strval);
    if(context->all_variables){
      if(!context->variables.count(resource->value)){
        context->variables.insert(resource->value);
        query_graph->addProjection(resource->value);
      }
    }
    free((yyvsp[0].strval));
    (yyvsp[0].strval) = nullptr;
    (yyval.resource) = resource;
  }
#line 2545 "sparql_parser.cpp"
    break;

  case 100: /* iri_element: iri  */
#line 659 "sparql_parser.y"
      {
    QueryResource *resource = context->resource_pool.alloc();
    resource->type = QueryResource::IRI;
    resource->value = (yyvsp[0].strval);
    free((yyvsp[0].strval));
    (yyvsp[0].strval) = nullptr;
    (yyval.resource) = resource;
  }
#line 2558 "sparql_parser.cpp"
    break;

  case 101: /* literal_element: rdf_literal  */
#line 670 "sparql_parser.y"
              {
    QueryResource *resource = context->resource_pool.alloc();
    resource->type = QueryResource::Literal;
    resource->value = (yyvsp[0].strval);
    free((yyvsp[0].strval));
    (yyvsp[0].strval) = nullptr;
    (yyval.resource) = resource;
  }
#line 2571 "sparql_parser.cpp"
    break;

  case 102: /* blank_node_element: '_' ':' IDENTIFIER  */
#line 681 "sparql_parser.y"
                     {
    std::string bknode_id;
    if(context->blank_nodes.count((yyvsp[0].strval))) {
      bknode_id = context->blank_nodes[(yyvsp[0].strval)];
    }
    else {
      bknode_id = context->bknode_id_gen.generate();
      context->blank_nodes[(yyvsp[0].strval)] = bknode_id;
    }
    free((yyvsp[0].strval));
    (yyvsp[0].strval) = nullptr;
    QueryResource *resource = context->resource_pool.alloc();
    resource->type = QueryResource::Variable;
    resource->value = bknode_id;
    (yyval.resource) = resource;
  }
#line 2592 "sparql_parser.cpp"
    break;

  case 103: /* blank_node_element: ANON  */
#line 698 "sparql_parser.y"
       {
    QueryResource *resource = context->resource_pool.alloc();
    resource->type = QueryResource::Variable;
    resource->value = context->bknode_id_gen.generate();
    (yyval.resource) = resource;
  }
#line 2603 "sparql_parser.cpp"
    break;

  case 104: /* expression_list: expression  */
#line 707 "sparql_parser.y"
             {
    QueryExpression* exp = query_graph->createExpression();;
    exp->type = QueryExpression::ArgumentList;
    exp->arg_list.push_back((yyvsp[0].expression));
    (yyval.expression) = exp;
  }
#line 2614 "sparql_parser.cpp"
    break;

  case 105: /* expression_list: expression_list ',' expression  */
#line 714 "sparql_parser.y"
                                 {
    (yyvsp[-2].expression)->arg_list.push_back((yyvsp[0].expression));
    (yyval.expression) = (yyvsp[-2].expression);
  }
#line 2623 "sparql_parser.cpp"
    break;

  case 106: /* expression: conditional_expression  */
#line 722 "sparql_parser.y"
                         { (yyval.expression) = (yyvsp[0].expression); }
#line 2629 "sparql_parser.cpp"
    break;

  case 107: /* conditional_expression: conditional_expression OP_OR conditional_expression  */
#line 727 "sparql_parser.y"
                                                      {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::Or;
    exp->arg_list.push_back((yyvsp[-2].expression));
    exp->arg_list.push_back((yyvsp[0].expression));
    (yyval.expression) = exp;
  }
#line 2641 "sparql_parser.cpp"
    break;

  case 108: /* conditional_expression: conditional_expression OP_AND conditional_expression  */
#line 735 "sparql_parser.y"
                                                       {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::And;
    exp->arg_list.push_back((yyvsp[-2].expression));
    exp->arg_list.push_back((yyvsp[0].expression));
    (yyval.expression) = exp;
  }
#line 2653 "sparql_parser.cpp"
    break;

  case 109: /* conditional_expression: relation_expression  */
#line 743 "sparql_parser.y"
                      { (yyval.expression) = (yyvsp[0].expression); }
#line 2659 "sparql_parser.cpp"
    break;

  case 110: /* relation_expression: numeric_expression OP_EQ numeric_expression  */
#line 748 "sparql_parser.y"
                                              {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::Equal;
    exp->arg_list.push_back((yyvsp[-2].expression));
    exp->arg_list.push_back((yyvsp[0].expression));
    (yyval.expression) = exp;
  }
#line 2671 "sparql_parser.cpp"
    break;

  case 111: /* relation_expression: numeric_expression OP_NE numeric_expression  */
#line 756 "sparql_parser.y"
                                              {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::NotEqual;
    exp->arg_list.push_back((yyvsp[-2].expression));
    exp->arg_list.push_back((yyvsp[0].expression));
    (yyval.expression) = exp;
  }
#line 2683 "sparql_parser.cpp"
    break;

  case 112: /* relation_expression: numeric_expression OP_LT numeric_expression  */
#line 764 "sparql_parser.y"
                                              {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::Less;
    exp->arg_list.push_back((yyvsp[-2].expression));
    exp->arg_list.push_back((yyvsp[0].expression));
    (yyval.expression) = exp;
  }
#line 2695 "sparql_parser.cpp"
    break;

  case 113: /* relation_expression: numeric_expression OP_GT numeric_expression  */
#line 772 "sparql_parser.y"
                                              {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::Greater;
    exp->arg_list.push_back((yyvsp[-2].expression));
    exp->arg_list.push_back((yyvsp[0].expression));
    (yyval.expression) = exp;
  }
#line 2707 "sparql_parser.cpp"
    break;

  case 114: /* relation_expression: numeric_expression OP_LE numeric_expression  */
#line 780 "sparql_parser.y"
                                              {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::LessOrEqual;
    exp->arg_list.push_back((yyvsp[-2].expression));
    exp->arg_list.push_back((yyvsp[0].expression));
    (yyval.expression) = exp;
  }
#line 2719 "sparql_parser.cpp"
    break;

  case 115: /* relation_expression: numeric_expression OP_GE numeric_expression  */
#line 788 "sparql_parser.y"
                                              {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::GreaterOrEqual;
    exp->arg_list.push_back((yyvsp[-2].expression));
    exp->arg_list.push_back((yyvsp[0].expression));
    (yyval.expression) = exp;
  }
#line 2731 "sparql_parser.cpp"
    break;

  case 116: /* relation_expression: numeric_expression  */
#line 796 "sparql_parser.y"
                     { (yyval.expression) = (yyvsp[0].expression); }
#line 2737 "sparql_parser.cpp"
    break;

  case 117: /* numeric_expression: numeric_expression '+' numeric_expression  */
#line 801 "sparql_parser.y"
                                            {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::Plus;
    exp->arg_list.push_back((yyvsp[-2].expression));
    exp->arg_list.push_back((yyvsp[0].expression));
    (yyval.expression) = exp;
  }
#line 2749 "sparql_parser.cpp"
    break;

  case 118: /* numeric_expression: numeric_expression '-' numeric_expression  */
#line 809 "sparql_parser.y"
                                            {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::Minus;
    exp->arg_list.push_back((yyvsp[-2].expression));
    exp->arg_list.push_back((yyvsp[0].expression));
    (yyval.expression) = exp;
  }
#line 2761 "sparql_parser.cpp"
    break;

  case 119: /* numeric_expression: numeric_expression '*' numeric_expression  */
#line 817 "sparql_parser.y"
                                            {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::Mul;
    exp->arg_list.push_back((yyvsp[-2].expression));
    exp->arg_list.push_back((yyvsp[0].expression));
    (yyval.expression) = exp;
  }
#line 2773 "sparql_parser.cpp"
    break;

  case 120: /* numeric_expression: numeric_expression '/' numeric_expression  */
#line 825 "sparql_parser.y"
                                            {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::Div;
    exp->arg_list.push_back((yyvsp[-2].expression));
    exp->arg_list.push_back((yyvsp[0].expression));
    (yyval.expression) = exp;
  }
#line 2785 "sparql_parser.cpp"
    break;

  case 121: /* numeric_expression: unary_expression  */
#line 833 "sparql_parser.y"
                   { (yyval.expression) = (yyvsp[0].expression); }
#line 2791 "sparql_parser.cpp"
    break;

  case 122: /* unary_expression: '!' primary_expression  */
#line 838 "sparql_parser.y"
                                      {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::Not;
    exp->arg_list.push_back((yyvsp[0].expression));
    (yyval.expression) = exp;
  }
#line 2802 "sparql_parser.cpp"
    break;

  case 123: /* unary_expression: '+' primary_expression  */
#line 845 "sparql_parser.y"
                                      {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::UnaryPlus;
    exp->arg_list.push_back((yyvsp[0].expression));
    (yyval.expression) = exp;
  }
#line 2813 "sparql_parser.cpp"
    break;

  case 124: /* unary_expression: '-' primary_expression  */
#line 852 "sparql_parser.y"
                                      {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::UnaryMinus;
    exp->arg_list.push_back((yyvsp[0].expression));
    (yyval.expression) = exp;
  }
#line 2824 "sparql_parser.cpp"
    break;

  case 125: /* unary_expression: primary_expression  */
#line 859 "sparql_parser.y"
                     { (yyval.expression) = (yyvsp[0].expression); }
#line 2830 "sparql_parser.cpp"
    break;

  case 126: /* primary_expression: bracketted_expression  */
#line 864 "sparql_parser.y"
                        { (yyval.expression) = (yyvsp[0].expression); }
#line 2836 "sparql_parser.cpp"
    break;

  case 127: /* primary_expression: built_in_call  */
#line 866 "sparql_parser.y"
                { (yyval.expression) = (yyvsp[0].expression); }
#line 2842 "sparql_parser.cpp"
    break;

  case 128: /* primary_expression: function_call  */
#line 868 "sparql_parser.y"
                { (yyval.expression) = (yyvsp[0].expression); }
#line 2848 "sparql_parser.cpp"
    break;

  case 129: /* primary_expression: iri  */
#line 870 "sparql_parser.y"
      {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::IRI;
    exp->value = (yyvsp[0].strval);
    free((yyvsp[0].strval));
    (yyvsp[0].strval) = nullptr;
    (yyval.expression) = exp;
  }
#line 2861 "sparql_parser.cpp"
    break;

  case 130: /* primary_expression: VARIABLE  */
#line 879 "sparql_parser.y"
           {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::Variable;
    exp->value = (yyvsp[0].strval);
    free((yyvsp[0].strval));
    (yyvsp[0].strval) = nullptr;
    (yyval.expression) = exp;
  }
#line 2874 "sparql_parser.cpp"
    break;

  case 131: /* primary_expression: literal  */
#line 888 "sparql_parser.y"
          {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::Literal;
    exp->value = (yyvsp[0].strval);
    free((yyvsp[0].strval));
    (yyvsp[0].strval) = nullptr;
    (yyval.expression) = exp;
  }
#line 2887 "sparql_parser.cpp"
    break;

  case 132: /* bracketted_expression: '(' expression ')'  */
#line 900 "sparql_parser.y"
                     { (yyval.expression) = (yyvsp[-1].expression); }
#line 2893 "sparql_parser.cpp"
    break;

  case 133: /* built_in_call: BIC_BOUND '(' VARIABLE ')'  */
#line 905 "sparql_parser.y"
                             {
    QueryExpression* arg = query_graph->createExpression();
    arg->type = QueryExpression::Variable;
    arg->value = (yyvsp[-1].strval);
    free((yyvsp[-1].strval));
    (yyvsp[-1].strval) = nullptr;
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::Builtin_bound;
    exp->arg_list.push_back(arg);
    (yyval.expression) = exp;
  }
#line 2909 "sparql_parser.cpp"
    break;

  case 134: /* built_in_call: BIC_STR bracketted_expression  */
#line 917 "sparql_parser.y"
                                {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::Builtin_str;
    exp->arg_list.push_back((yyvsp[0].expression));
    (yyval.expression) = exp;
  }
#line 2920 "sparql_parser.cpp"
    break;

  case 135: /* built_in_call: BIC_LANG bracketted_expression  */
#line 924 "sparql_parser.y"
                                 {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::Builtin_lang;
    exp->arg_list.push_back((yyvsp[0].expression));
    (yyval.expression) = exp;
  }
#line 2931 "sparql_parser.cpp"
    break;

  case 136: /* built_in_call: BIC_DATATYPE bracketted_expression  */
#line 931 "sparql_parser.y"
                                     {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::Builtin_datatype;
    exp->arg_list.push_back((yyvsp[0].expression));
    (yyval.expression) = exp;
  }
#line 2942 "sparql_parser.cpp"
    break;

  case 137: /* built_in_call: BIC_SAMETERM '(' expression ',' expression ')'  */
#line 938 "sparql_parser.y"
                                                 {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::Builtin_sameterm;
    exp->arg_list.push_back((yyvsp[-3].expression));
    exp->arg_list.push_back((yyvsp[-1].expression));
    (yyval.expression) = exp;
  }
#line 2954 "sparql_parser.cpp"
    break;

  case 138: /* built_in_call: BIC_LANGMATCHES '(' expression ',' expression ')'  */
#line 946 "sparql_parser.y"
                                                    {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::Builtin_langmatches;
    exp->arg_list.push_back((yyvsp[-3].expression));
    exp->arg_list.push_back((yyvsp[-1].expression));
    (yyval.expression) = exp;
  }
#line 2966 "sparql_parser.cpp"
    break;

  case 139: /* built_in_call: BIC_ISIRI bracketted_expression  */
#line 954 "sparql_parser.y"
                                  {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::Builtin_isiri;
    exp->arg_list.push_back((yyvsp[0].expression));
    (yyval.expression) = exp;
  }
#line 2977 "sparql_parser.cpp"
    break;

  case 140: /* built_in_call: BIC_ISURI bracketted_expression  */
#line 961 "sparql_parser.y"
                                  {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::Builtin_isuri;
    exp->arg_list.push_back((yyvsp[0].expression));
    (yyval.expression) = exp;
  }
#line 2988 "sparql_parser.cpp"
    break;

  case 141: /* built_in_call: BIC_ISBLANK bracketted_expression  */
#line 968 "sparql_parser.y"
                                    {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::Builtin_isblank;
    exp->arg_list.push_back((yyvsp[0].expression));
    (yyval.expression) = exp;
  }
#line 2999 "sparql_parser.cpp"
    break;

  case 142: /* built_in_call: BIC_ISLITERAL bracketted_expression  */
#line 975 "sparql_parser.y"
                                      {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::Builtin_isliteral;
    exp->arg_list.push_back((yyvsp[0].expression));
    (yyval.expression) = exp;
  }
#line 3010 "sparql_parser.cpp"
    break;

  case 143: /* built_in_call: BIC_REGEX '(' expression ',' expression_list ')'  */
#line 982 "sparql_parser.y"
                                                    {
    QueryExpression* exp = query_graph->createExpression();
    exp->type = QueryExpression::Builtin_regex;
    exp->arg_list.push_back((yyvsp[-3].expression));
    exp->arg_list.insert(exp->arg_list.end(), (yyvsp[-1].expression)->arg_list.begin(), (yyvsp[-1].expression)->arg_list.end());
    query_graph->removeExpression((yyvsp[-1].expression));
    (yyval.expression) = exp;
  }
#line 3023 "sparql_parser.cpp"
    break;

  case 144: /* literal: rdf_literal  */
#line 993 "sparql_parser.y"
              { (yyval.strval) = (yyvsp[0].strval); }
#line 3029 "sparql_parser.cpp"
    break;

  case 145: /* literal: numeric_literal  */
#line 995 "sparql_parser.y"
                  { (yyval.strval) = (yyvsp[0].strval); }
#line 3035 "sparql_parser.cpp"
    break;

  case 146: /* literal: boolean_literal  */
#line 997 "sparql_parser.y"
                  { (yyval.strval) = (yyvsp[0].strval); }
#line 3041 "sparql_parser.cpp"
    break;

  case 147: /* rdf_literal: STRING  */
#line 1002 "sparql_parser.y"
         {
    std::stringstream ss;
    ss << "\"" << (yyvsp[0].strval) << "\"" << XSDVocabulary::XSD_STRING;
    free((yyvsp[0].strval));
    (yyvsp[0].strval) = nullptr;
    (yyval.strval) = strdup(ss.str().c_str());
  }
#line 3053 "sparql_parser.cpp"
    break;

  case 148: /* rdf_literal: STRING LANGTAG  */
#line 1010 "sparql_parser.y"
                 {
    std::stringstream ss;
    ss << "\"" << (yyvsp[-1].strval) << "\"" << (yyvsp[0].strval);
    free((yyvsp[-1].strval));
    (yyvsp[-1].strval) = nullptr;
    free((yyvsp[0].strval));
    (yyvsp[0].strval) = nullptr;
    (yyval.strval) = strdup(ss.str().c_str());
  }
#line 3067 "sparql_parser.cpp"
    break;

  case 149: /* rdf_literal: STRING TYPE iri  */
#line 1020 "sparql_parser.y"
                  {
    std::stringstream ss;
    ss << "\"" << (yyvsp[-2].strval) << "\"" << "^^" << (yyvsp[0].strval);
    free((yyvsp[-2].strval));
    (yyvsp[-2].strval) = nullptr;
    free((yyvsp[0].strval));
    (yyvsp[0].strval) = nullptr;
    (yyval.strval) = strdup(ss.str().c_str());
  }
#line 3081 "sparql_parser.cpp"
    break;

  case 150: /* numeric_literal: INTEGER  */
#line 1033 "sparql_parser.y"
          {
    std::stringstream ss;
    ss << "\"" << (yyvsp[0].strval) << "\"" << "^^" << XSDVocabulary::XSD_INTEGER;
    (yyval.strval) = strdup(ss.str().c_str());
  }
#line 3091 "sparql_parser.cpp"
    break;

  case 151: /* numeric_literal: DECIMAL  */
#line 1039 "sparql_parser.y"
          {
    std::stringstream ss;
    ss << "\"" << (yyvsp[0].strval) << "\"" << "^^" << XSDVocabulary::XSD_DECIMAL;
    (yyval.strval) = strdup(ss.str().c_str());
  }
#line 3101 "sparql_parser.cpp"
    break;

  case 152: /* numeric_literal: DOUBLE  */
#line 1045 "sparql_parser.y"
         {
    std::stringstream ss;
    ss << "\"" << (yyvsp[0].strval) << "\"" << "^^" << XSDVocabulary::XSD_DOUBLE;
    (yyval.strval) = strdup(ss.str().c_str());
  }
#line 3111 "sparql_parser.cpp"
    break;

  case 153: /* boolean_literal: BOOL_TRUE  */
#line 1054 "sparql_parser.y"
            {
    std::stringstream ss;
    ss << "\"true\"" << "^^" << XSDVocabulary::XSD_BOOLEAN;
    (yyval.strval) = strdup(ss.str().c_str());
  }
#line 3121 "sparql_parser.cpp"
    break;

  case 154: /* boolean_literal: BOOL_FALSE  */
#line 1060 "sparql_parser.y"
             {
    std::stringstream ss;
    ss << "\"false\"" << "^^" << XSDVocabulary::XSD_BOOLEAN;
    (yyval.strval) = strdup(ss.str().c_str());
  }
#line 3131 "sparql_parser.cpp"
    break;

  case 155: /* iri: IRI  */
#line 1069 "sparql_parser.y"
      {
    std::string str = (yyvsp[0].strval);
    if(!context->base.empty() && str.find("://")==std::string::npos){
      str.insert(str.begin()+1, context->base.begin()+1, context->base.end()-1);
    }
    free((yyvsp[0].strval));
    (yyvsp[0].strval) = nullptr;
    (yyval.strval) = strdup(str.c_str());
  }
#line 3145 "sparql_parser.cpp"
    break;

  case 156: /* iri: IDENTIFIER ':' IDENTIFIER  */
#line 1079 "sparql_parser.y"
                            {
    std::string str = context->prefixes[(yyvsp[-2].strval)];
    str.insert(str.length()-1, (yyvsp[0].strval), strlen((yyvsp[0].strval)));
    free((yyvsp[-2].strval));
    (yyvsp[-2].strval) = nullptr;
    free((yyvsp[0].strval));
    (yyvsp[0].strval) = nullptr;
    (yyval.strval) = strdup(str.c_str());
  }
#line 3159 "sparql_parser.cpp"
    break;

  case 157: /* iri: ':' IDENTIFIER  */
#line 1089 "sparql_parser.y"
                {
    std::string str = context->prefixes[""];
    str.insert(str.length()-1, (yyvsp[0].strval), strlen((yyvsp[0].strval)));
    free((yyvsp[0].strval));
    (yyvsp[0].strval) = nullptr;
    (yyval.strval) = strdup(str.c_str());
  }
#line 3171 "sparql_parser.cpp"
    break;


#line 3175 "sparql_parser.cpp"

      default: break;
    }
  /* User semantic actions sometimes alter yychar, and that requires
     that yytoken be updated with the new translation.  We take the
     approach of translating immediately before every use of yytoken.
     One alternative is translating here after every semantic action,
     but that translation would be missed if the semantic action invokes
     YYABORT, YYACCEPT, or YYERROR immediately after altering yychar or
     if it invokes YYBACKUP.  In the case of YYABORT or YYACCEPT, an
     incorrect destructor might then be invoked immediately.  In the
     case of YYERROR or YYBACKUP, subsequent parser actions might lead
     to an incorrect destructor call or verbose syntax error message
     before the lookahead is translated.  */
  YY_SYMBOL_PRINT ("-> $$ =", YY_CAST (yysymbol_kind_t, yyr1[yyn]), &yyval, &yyloc);

  YYPOPSTACK (yylen);
  yylen = 0;

  *++yyvsp = yyval;
  *++yylsp = yyloc;

  /* Now 'shift' the result of the reduction.  Determine what state
     that goes to, based on the state we popped back to and the rule
     number reduced by.  */
  {
    const int yylhs = yyr1[yyn] - YYNTOKENS;
    const int yyi = yypgoto[yylhs] + *yyssp;
    yystate = (0 <= yyi && yyi <= YYLAST && yycheck[yyi] == *yyssp
               ? yytable[yyi]
               : yydefgoto[yylhs]);
  }

  goto yynewstate;


/*--------------------------------------.
| yyerrlab -- here on detecting error.  |
`--------------------------------------*/
yyerrlab:
  /* Make sure we have latest lookahead translation.  See comments at
     user semantic actions for why this is necessary.  */
  yytoken = yychar == RQEMPTY ? YYSYMBOL_YYEMPTY : YYTRANSLATE (yychar);
  /* If not already recovering from an error, report this error.  */
  if (!yyerrstatus)
    {
      ++yynerrs;
      {
        yypcontext_t yyctx
          = {yyssp, yytoken, &yylloc};
        char const *yymsgp = YY_("syntax error");
        int yysyntax_error_status;
        yysyntax_error_status = yysyntax_error (&yymsg_alloc, &yymsg, &yyctx);
        if (yysyntax_error_status == 0)
          yymsgp = yymsg;
        else if (yysyntax_error_status == -1)
          {
            if (yymsg != yymsgbuf)
              YYSTACK_FREE (yymsg);
            yymsg = YY_CAST (char *,
                             YYSTACK_ALLOC (YY_CAST (YYSIZE_T, yymsg_alloc)));
            if (yymsg)
              {
                yysyntax_error_status
                  = yysyntax_error (&yymsg_alloc, &yymsg, &yyctx);
                yymsgp = yymsg;
              }
            else
              {
                yymsg = yymsgbuf;
                yymsg_alloc = sizeof yymsgbuf;
                yysyntax_error_status = YYENOMEM;
              }
          }
        yyerror (&yylloc, scanner, context, query_graph, yymsgp);
        if (yysyntax_error_status == YYENOMEM)
          goto yyexhaustedlab;
      }
    }

  yyerror_range[1] = yylloc;
  if (yyerrstatus == 3)
    {
      /* If just tried and failed to reuse lookahead token after an
         error, discard it.  */

      if (yychar <= RQEOF)
        {
          /* Return failure if at end of input.  */
          if (yychar == RQEOF)
            YYABORT;
        }
      else
        {
          yydestruct ("Error: discarding",
                      yytoken, &yylval, &yylloc, scanner, context, query_graph);
          yychar = RQEMPTY;
        }
    }

  /* Else will try to reuse lookahead token after shifting the error
     token.  */
  goto yyerrlab1;


/*---------------------------------------------------.
| yyerrorlab -- error raised explicitly by YYERROR.  |
`---------------------------------------------------*/
yyerrorlab:
  /* Pacify compilers when the user code never invokes YYERROR and the
     label yyerrorlab therefore never appears in user code.  */
  if (0)
    YYERROR;

  /* Do not reclaim the symbols of the rule whose action triggered
     this YYERROR.  */
  YYPOPSTACK (yylen);
  yylen = 0;
  YY_STACK_PRINT (yyss, yyssp);
  yystate = *yyssp;
  goto yyerrlab1;


/*-------------------------------------------------------------.
| yyerrlab1 -- common code for both syntax error and YYERROR.  |
`-------------------------------------------------------------*/
yyerrlab1:
  yyerrstatus = 3;      /* Each real token shifted decrements this.  */

  /* Pop stack until we find a state that shifts the error token.  */
  for (;;)
    {
      yyn = yypact[yystate];
      if (!yypact_value_is_default (yyn))
        {
          yyn += YYSYMBOL_YYerror;
          if (0 <= yyn && yyn <= YYLAST && yycheck[yyn] == YYSYMBOL_YYerror)
            {
              yyn = yytable[yyn];
              if (0 < yyn)
                break;
            }
        }

      /* Pop the current state because it cannot handle the error token.  */
      if (yyssp == yyss)
        YYABORT;

      yyerror_range[1] = *yylsp;
      yydestruct ("Error: popping",
                  YY_ACCESSING_SYMBOL (yystate), yyvsp, yylsp, scanner, context, query_graph);
      YYPOPSTACK (1);
      yystate = *yyssp;
      YY_STACK_PRINT (yyss, yyssp);
    }

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  *++yyvsp = yylval;
  YY_IGNORE_MAYBE_UNINITIALIZED_END

  yyerror_range[2] = yylloc;
  ++yylsp;
  YYLLOC_DEFAULT (*yylsp, yyerror_range, 2);

  /* Shift the error token.  */
  YY_SYMBOL_PRINT ("Shifting", YY_ACCESSING_SYMBOL (yyn), yyvsp, yylsp);

  yystate = yyn;
  goto yynewstate;


/*-------------------------------------.
| yyacceptlab -- YYACCEPT comes here.  |
`-------------------------------------*/
yyacceptlab:
  yyresult = 0;
  goto yyreturn;


/*-----------------------------------.
| yyabortlab -- YYABORT comes here.  |
`-----------------------------------*/
yyabortlab:
  yyresult = 1;
  goto yyreturn;


#if 1
/*-------------------------------------------------.
| yyexhaustedlab -- memory exhaustion comes here.  |
`-------------------------------------------------*/
yyexhaustedlab:
  yyerror (&yylloc, scanner, context, query_graph, YY_("memory exhausted"));
  yyresult = 2;
  goto yyreturn;
#endif


/*-------------------------------------------------------.
| yyreturn -- parsing is finished, clean up and return.  |
`-------------------------------------------------------*/
yyreturn:
  if (yychar != RQEMPTY)
    {
      /* Make sure we have latest lookahead translation.  See comments at
         user semantic actions for why this is necessary.  */
      yytoken = YYTRANSLATE (yychar);
      yydestruct ("Cleanup: discarding lookahead",
                  yytoken, &yylval, &yylloc, scanner, context, query_graph);
    }
  /* Do not reclaim the symbols of the rule whose action triggered
     this YYABORT or YYACCEPT.  */
  YYPOPSTACK (yylen);
  YY_STACK_PRINT (yyss, yyssp);
  while (yyssp != yyss)
    {
      yydestruct ("Cleanup: popping",
                  YY_ACCESSING_SYMBOL (+*yyssp), yyvsp, yylsp, scanner, context, query_graph);
      YYPOPSTACK (1);
    }
#ifndef yyoverflow
  if (yyss != yyssa)
    YYSTACK_FREE (yyss);
#endif
  if (yymsg != yymsgbuf)
    YYSTACK_FREE (yymsg);
  return yyresult;
}

#line 1098 "sparql_parser.y"



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
