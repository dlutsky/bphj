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
#define YYSTYPE         TTLSTYPE
#define YYLTYPE         TTLLTYPE
/* Substitute the variable and function names.  */
#define yyparse         ttlparse
#define yylex           ttllex
#define yyerror         ttlerror
#define yydebug         ttldebug
#define yynerrs         ttlnerrs

/* First part of user prologue.  */
#line 17 "turtle_parser.y"


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



#line 117 "turtle_parser.cpp"

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

#include "turtle_lexer.h"
/* Symbol kind.  */
enum yysymbol_kind_t
{
  YYSYMBOL_YYEMPTY = -2,
  YYSYMBOL_YYEOF = 0,                      /* "end of file"  */
  YYSYMBOL_YYerror = 1,                    /* error  */
  YYSYMBOL_YYUNDEF = 2,                    /* "invalid token"  */
  YYSYMBOL_BASE = 3,                       /* BASE  */
  YYSYMBOL_SPARQL_BASE = 4,                /* SPARQL_BASE  */
  YYSYMBOL_PREFIX = 5,                     /* PREFIX  */
  YYSYMBOL_SPARQL_PREFIX = 6,              /* SPARQL_PREFIX  */
  YYSYMBOL_BOOL_TRUE = 7,                  /* BOOL_TRUE  */
  YYSYMBOL_BOOL_FALSE = 8,                 /* BOOL_FALSE  */
  YYSYMBOL_END_OF_FILE = 9,                /* END_OF_FILE  */
  YYSYMBOL_DECIMAL = 10,                   /* DECIMAL  */
  YYSYMBOL_DOUBLE = 11,                    /* DOUBLE  */
  YYSYMBOL_INTEGER = 12,                   /* INTEGER  */
  YYSYMBOL_IRI = 13,                       /* IRI  */
  YYSYMBOL_IDENTIFIER = 14,                /* IDENTIFIER  */
  YYSYMBOL_STRING = 15,                    /* STRING  */
  YYSYMBOL_LANGTAG = 16,                   /* LANGTAG  */
  YYSYMBOL_TYPE = 17,                      /* TYPE  */
  YYSYMBOL_ANON = 18,                      /* ANON  */
  YYSYMBOL_19_ = 19,                       /* ':'  */
  YYSYMBOL_20_ = 20,                       /* '.'  */
  YYSYMBOL_21_ = 21,                       /* ';'  */
  YYSYMBOL_22_ = 22,                       /* ','  */
  YYSYMBOL_23_ = 23,                       /* ']'  */
  YYSYMBOL_24_ = 24,                       /* '['  */
  YYSYMBOL_25___ = 25,                     /* '_'  */
  YYSYMBOL_26_ = 26,                       /* '('  */
  YYSYMBOL_27_ = 27,                       /* ')'  */
  YYSYMBOL_YYACCEPT = 28,                  /* $accept  */
  YYSYMBOL_statement = 29,                 /* statement  */
  YYSYMBOL_directive = 30,                 /* directive  */
  YYSYMBOL_prefix = 31,                    /* prefix  */
  YYSYMBOL_prefix_id = 32,                 /* prefix_id  */
  YYSYMBOL_base = 33,                      /* base  */
  YYSYMBOL_triples = 34,                   /* triples  */
  YYSYMBOL_predicate_object_list = 35,     /* predicate_object_list  */
  YYSYMBOL_object_list = 36,               /* object_list  */
  YYSYMBOL_verb = 37,                      /* verb  */
  YYSYMBOL_subject1 = 38,                  /* subject1  */
  YYSYMBOL_subject2 = 39,                  /* subject2  */
  YYSYMBOL_predicate = 40,                 /* predicate  */
  YYSYMBOL_type = 41,                      /* type  */
  YYSYMBOL_object = 42,                    /* object  */
  YYSYMBOL_blank_node_property_list = 43,  /* blank_node_property_list  */
  YYSYMBOL_open_square_bracket = 44,       /* open_square_bracket  */
  YYSYMBOL_blank_node = 45,                /* blank_node  */
  YYSYMBOL_collection = 46,                /* collection  */
  YYSYMBOL_item_list = 47,                 /* item_list  */
  YYSYMBOL_item_list_not_empty = 48,       /* item_list_not_empty  */
  YYSYMBOL_literal = 49,                   /* literal  */
  YYSYMBOL_numeric_literal = 50,           /* numeric_literal  */
  YYSYMBOL_rdf_literal = 51,               /* rdf_literal  */
  YYSYMBOL_boolean_literal = 52,           /* boolean_literal  */
  YYSYMBOL_iri = 53                        /* iri  */
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
typedef yytype_int8 yy_state_t;

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
         || (defined TTLLTYPE_IS_TRIVIAL && TTLLTYPE_IS_TRIVIAL \
             && defined TTLSTYPE_IS_TRIVIAL && TTLSTYPE_IS_TRIVIAL)))

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
#define YYFINAL  50
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   76

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  28
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  26
/* YYNRULES -- Number of rules.  */
#define YYNRULES  55
/* YYNSTATES -- Number of states.  */
#define YYNSTATES  83

/* YYMAXUTOK -- Last valid token kind.  */
#define YYMAXUTOK   273


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
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
      26,    27,     2,     2,    22,     2,    20,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,    19,    21,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,    24,     2,    23,     2,    25,     2,     2,     2,     2,
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
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     1,     2,     3,     4,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15,    16,    17,    18
};

#if TTLDEBUG
  /* YYRLINE[YYN] -- Source line where rule number YYN was defined.  */
static const yytype_int16 yyrline[] =
{
       0,   145,   145,   147,   149,   155,   157,   163,   171,   181,
     183,   188,   190,   195,   201,   207,   216,   222,   231,   237,
     247,   249,   254,   256,   258,   262,   267,   271,   284,   286,
     288,   290,   292,   297,   305,   312,   326,   334,   338,   345,
     355,   364,   379,   381,   383,   389,   395,   401,   409,   417,
     427,   439,   445,   453,   463,   473
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
  "\"end of file\"", "error", "\"invalid token\"", "BASE", "SPARQL_BASE",
  "PREFIX", "SPARQL_PREFIX", "BOOL_TRUE", "BOOL_FALSE", "END_OF_FILE",
  "DECIMAL", "DOUBLE", "INTEGER", "IRI", "IDENTIFIER", "STRING", "LANGTAG",
  "TYPE", "ANON", "':'", "'.'", "';'", "','", "']'", "'['", "'_'", "'('",
  "')'", "$accept", "statement", "directive", "prefix", "prefix_id",
  "base", "triples", "predicate_object_list", "object_list", "verb",
  "subject1", "subject2", "predicate", "type", "object",
  "blank_node_property_list", "open_square_bracket", "blank_node",
  "collection", "item_list", "item_list_not_empty", "literal",
  "numeric_literal", "rdf_literal", "boolean_literal", "iri", YY_NULLPTR
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
     265,   266,   267,   268,   269,   270,   271,   272,   273,    58,
      46,    59,    44,    93,    91,    95,    40,    41
};
#endif

#define YYPACT_NINF (-13)

#define yypact_value_is_default(Yyn) \
  ((Yyn) == YYPACT_NINF)

#define YYTABLE_NINF (-1)

#define yytable_value_is_error(Yyn) \
  0

  /* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
     STATE-NUM.  */
static const yytype_int8 yypact[] =
{
      11,    -1,    14,   -12,   -12,   -13,   -13,    12,   -13,    26,
     -13,    35,    31,    51,   -13,   -13,   -13,   -13,    -8,   -10,
     -13,    -8,   -13,   -13,   -13,    38,   -13,   -13,    40,    41,
      47,   -13,    48,   -13,   -13,   -13,   -13,   -13,     6,   -13,
     -13,   -13,   -13,    36,    31,   -13,   -13,   -13,   -13,   -13,
     -13,    12,    27,    31,   -13,   -13,   -13,   -13,    32,     5,
     -13,    52,    54,   -13,   -13,   -13,    -6,   -13,   -13,   -13,
      -8,    42,   -13,   -13,   -13,    49,   -13,   -13,    31,    31,
     -13,    42,   -13
};

  /* YYDEFACT[STATE-NUM] -- Default reduction number in state STATE-NUM.
     Performed when YYTABLE does not specify something else to do.  Zero
     means the default is an error.  */
static const yytype_int8 yydefact[] =
{
       0,     0,     0,    10,    10,     4,    53,     0,    36,     0,
      34,     0,    39,     0,     2,     5,     6,     3,     0,     0,
      25,     0,    23,    24,    22,     0,    12,     9,     0,     0,
       0,    55,     0,    51,    52,    46,    47,    45,    48,    40,
      31,    30,    32,     0,    38,    29,    43,    42,    44,    28,
       1,    27,     0,     0,    20,    21,    26,    15,     0,     0,
      11,     0,     0,    54,    35,    49,     0,    37,    41,    13,
       0,    16,    18,    14,    33,     0,     8,    50,     0,     0,
       7,    17,    19
};

  /* YYPGOTO[NTERM-NUM].  */
static const yytype_int8 yypgoto[] =
{
     -13,   -13,   -13,   -13,    67,   -13,   -13,    13,    -5,     2,
     -13,   -13,   -13,   -13,   -11,    74,   -13,    75,    76,   -13,
     -13,   -13,   -13,   -13,   -13,     0
};

  /* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int8 yydefgoto[] =
{
       0,    13,    14,    15,    28,    16,    17,    52,    71,    53,
      18,    19,    54,    55,    72,    40,    21,    41,    42,    43,
      44,    45,    46,    47,    48,    49
};

  /* YYTABLE[YYPACT[STATE-NUM]] -- What to do in state STATE-NUM.  If
     positive, shift that token.  If negative, reduce the rule whose
     number is the opposite.  If YYTABLE_NINF, syntax error.  */
static const yytype_int8 yytable[] =
{
      24,    39,    27,     6,    51,     6,    51,     6,     7,     9,
      57,     9,    25,     9,     1,     2,     3,     4,    56,    56,
       5,    56,    65,    66,     6,     7,    70,    26,    74,     8,
       9,    30,    58,    68,    59,    10,    11,    12,    33,    34,
      31,    35,    36,    37,     6,     7,    38,    69,    70,     8,
       9,    50,    73,    70,    32,    10,    11,    12,    60,    61,
      62,    63,    64,    67,    79,    75,    77,    76,    82,    80,
      56,    29,    78,    81,    20,    22,    23
};

static const yytype_int8 yycheck[] =
{
       0,    12,    14,    13,    14,    13,    14,    13,    14,    19,
      20,    19,    13,    19,     3,     4,     5,     6,    18,    19,
       9,    21,    16,    17,    13,    14,    21,    13,    23,    18,
      19,    19,    19,    44,    21,    24,    25,    26,     7,     8,
      14,    10,    11,    12,    13,    14,    15,    20,    21,    18,
      19,     0,    20,    21,    19,    24,    25,    26,    20,    19,
      19,    14,    14,    27,    22,    13,    66,    13,    79,    20,
      70,     4,    70,    78,     0,     0,     0
};

  /* YYSTOS[STATE-NUM] -- The (internal number of the) accessing
     symbol of state STATE-NUM.  */
static const yytype_int8 yystos[] =
{
       0,     3,     4,     5,     6,     9,    13,    14,    18,    19,
      24,    25,    26,    29,    30,    31,    33,    34,    38,    39,
      43,    44,    45,    46,    53,    13,    13,    14,    32,    32,
      19,    14,    19,     7,     8,    10,    11,    12,    15,    42,
      43,    45,    46,    47,    48,    49,    50,    51,    52,    53,
       0,    14,    35,    37,    40,    41,    53,    20,    35,    35,
      20,    19,    19,    14,    14,    16,    17,    27,    42,    20,
      21,    36,    42,    20,    23,    13,    13,    53,    37,    22,
      20,    36,    42
};

  /* YYR1[YYN] -- Symbol number of symbol that rule YYN derives.  */
static const yytype_int8 yyr1[] =
{
       0,    28,    29,    29,    29,    30,    30,    31,    31,    32,
      32,    33,    33,    34,    34,    34,    35,    35,    36,    36,
      37,    37,    38,    38,    38,    39,    40,    41,    42,    42,
      42,    42,    42,    43,    44,    45,    45,    46,    47,    47,
      48,    48,    49,    49,    49,    50,    50,    50,    51,    51,
      51,    52,    52,    53,    53,    53
};

  /* YYR2[YYN] -- Number of symbols on the right hand side of rule YYN.  */
static const yytype_int8 yyr2[] =
{
       0,     2,     1,     1,     1,     1,     1,     5,     4,     1,
       0,     3,     2,     3,     3,     2,     2,     4,     1,     3,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     3,     1,     3,     1,     3,     1,     0,
       1,     2,     1,     1,     1,     1,     1,     1,     1,     2,
       3,     1,     1,     1,     3,     2
};


enum { YYENOMEM = -2 };

#define yyerrok         (yyerrstatus = 0)
#define yyclearin       (yychar = TTLEMPTY)

#define YYACCEPT        goto yyacceptlab
#define YYABORT         goto yyabortlab
#define YYERROR         goto yyerrorlab


#define YYRECOVERING()  (!!yyerrstatus)

#define YYBACKUP(Token, Value)                                    \
  do                                                              \
    if (yychar == TTLEMPTY)                                        \
      {                                                           \
        yychar = (Token);                                         \
        yylval = (Value);                                         \
        YYPOPSTACK (yylen);                                       \
        yystate = *yyssp;                                         \
        goto yybackup;                                            \
      }                                                           \
    else                                                          \
      {                                                           \
        yyerror (&yylloc, scanner, context, triples, YY_("syntax error: cannot back up")); \
        YYERROR;                                                  \
      }                                                           \
  while (0)

/* Backward compatibility with an undocumented macro.
   Use TTLerror or TTLUNDEF. */
#define YYERRCODE TTLUNDEF

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
#if TTLDEBUG

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
#  if defined TTLLTYPE_IS_TRIVIAL && TTLLTYPE_IS_TRIVIAL

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
                  Kind, Value, Location, scanner, context, triples); \
      YYFPRINTF (stderr, "\n");                                           \
    }                                                                     \
} while (0)


/*-----------------------------------.
| Print this symbol's value on YYO.  |
`-----------------------------------*/

static void
yy_symbol_value_print (FILE *yyo,
                       yysymbol_kind_t yykind, YYSTYPE const * const yyvaluep, YYLTYPE const * const yylocationp, yyscan_t scanner, TurtleParseContext& context, std::vector<std::tuple<std::string, std::string, std::string>>& triples)
{
  FILE *yyoutput = yyo;
  YY_USE (yyoutput);
  YY_USE (yylocationp);
  YY_USE (scanner);
  YY_USE (context);
  YY_USE (triples);
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
                 yysymbol_kind_t yykind, YYSTYPE const * const yyvaluep, YYLTYPE const * const yylocationp, yyscan_t scanner, TurtleParseContext& context, std::vector<std::tuple<std::string, std::string, std::string>>& triples)
{
  YYFPRINTF (yyo, "%s %s (",
             yykind < YYNTOKENS ? "token" : "nterm", yysymbol_name (yykind));

  YY_LOCATION_PRINT (yyo, *yylocationp);
  YYFPRINTF (yyo, ": ");
  yy_symbol_value_print (yyo, yykind, yyvaluep, yylocationp, scanner, context, triples);
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
                 int yyrule, yyscan_t scanner, TurtleParseContext& context, std::vector<std::tuple<std::string, std::string, std::string>>& triples)
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
                       &(yylsp[(yyi + 1) - (yynrhs)]), scanner, context, triples);
      YYFPRINTF (stderr, "\n");
    }
}

# define YY_REDUCE_PRINT(Rule)          \
do {                                    \
  if (yydebug)                          \
    yy_reduce_print (yyssp, yyvsp, yylsp, Rule, scanner, context, triples); \
} while (0)

/* Nonzero means print parse trace.  It is left uninitialized so that
   multiple parsers can coexist.  */
int yydebug;
#else /* !TTLDEBUG */
# define YYDPRINTF(Args) ((void) 0)
# define YY_SYMBOL_PRINT(Title, Kind, Value, Location)
# define YY_STACK_PRINT(Bottom, Top)
# define YY_REDUCE_PRINT(Rule)
#endif /* !TTLDEBUG */


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
            yysymbol_kind_t yykind, YYSTYPE *yyvaluep, YYLTYPE *yylocationp, yyscan_t scanner, TurtleParseContext& context, std::vector<std::tuple<std::string, std::string, std::string>>& triples)
{
  YY_USE (yyvaluep);
  YY_USE (yylocationp);
  YY_USE (scanner);
  YY_USE (context);
  YY_USE (triples);
  if (!yymsg)
    yymsg = "Deleting";
  YY_SYMBOL_PRINT (yymsg, yykind, yyvaluep, yylocationp);

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  switch (yykind)
    {
    case YYSYMBOL_DECIMAL: /* DECIMAL  */
#line 119 "turtle_parser.y"
            { free(((*yyvaluep).strval)); }
#line 1308 "turtle_parser.cpp"
        break;

    case YYSYMBOL_DOUBLE: /* DOUBLE  */
#line 119 "turtle_parser.y"
            { free(((*yyvaluep).strval)); }
#line 1314 "turtle_parser.cpp"
        break;

    case YYSYMBOL_INTEGER: /* INTEGER  */
#line 119 "turtle_parser.y"
            { free(((*yyvaluep).strval)); }
#line 1320 "turtle_parser.cpp"
        break;

    case YYSYMBOL_IRI: /* IRI  */
#line 119 "turtle_parser.y"
            { free(((*yyvaluep).strval)); }
#line 1326 "turtle_parser.cpp"
        break;

    case YYSYMBOL_IDENTIFIER: /* IDENTIFIER  */
#line 119 "turtle_parser.y"
            { free(((*yyvaluep).strval)); }
#line 1332 "turtle_parser.cpp"
        break;

    case YYSYMBOL_STRING: /* STRING  */
#line 119 "turtle_parser.y"
            { free(((*yyvaluep).strval)); }
#line 1338 "turtle_parser.cpp"
        break;

    case YYSYMBOL_LANGTAG: /* LANGTAG  */
#line 119 "turtle_parser.y"
            { free(((*yyvaluep).strval)); }
#line 1344 "turtle_parser.cpp"
        break;

    case YYSYMBOL_prefix_id: /* prefix_id  */
#line 119 "turtle_parser.y"
            { free(((*yyvaluep).strval)); }
#line 1350 "turtle_parser.cpp"
        break;

    case YYSYMBOL_predicate: /* predicate  */
#line 119 "turtle_parser.y"
            { free(((*yyvaluep).strval)); }
#line 1356 "turtle_parser.cpp"
        break;

    case YYSYMBOL_type: /* type  */
#line 119 "turtle_parser.y"
            { free(((*yyvaluep).strval)); }
#line 1362 "turtle_parser.cpp"
        break;

    case YYSYMBOL_object: /* object  */
#line 119 "turtle_parser.y"
            { free(((*yyvaluep).strval)); }
#line 1368 "turtle_parser.cpp"
        break;

    case YYSYMBOL_blank_node_property_list: /* blank_node_property_list  */
#line 119 "turtle_parser.y"
            { free(((*yyvaluep).strval)); }
#line 1374 "turtle_parser.cpp"
        break;

    case YYSYMBOL_blank_node: /* blank_node  */
#line 119 "turtle_parser.y"
            { free(((*yyvaluep).strval)); }
#line 1380 "turtle_parser.cpp"
        break;

    case YYSYMBOL_collection: /* collection  */
#line 119 "turtle_parser.y"
            { free(((*yyvaluep).strval)); }
#line 1386 "turtle_parser.cpp"
        break;

    case YYSYMBOL_item_list: /* item_list  */
#line 119 "turtle_parser.y"
            { free(((*yyvaluep).strval)); }
#line 1392 "turtle_parser.cpp"
        break;

    case YYSYMBOL_item_list_not_empty: /* item_list_not_empty  */
#line 119 "turtle_parser.y"
            { free(((*yyvaluep).strval)); }
#line 1398 "turtle_parser.cpp"
        break;

    case YYSYMBOL_literal: /* literal  */
#line 119 "turtle_parser.y"
            { free(((*yyvaluep).strval)); }
#line 1404 "turtle_parser.cpp"
        break;

    case YYSYMBOL_numeric_literal: /* numeric_literal  */
#line 119 "turtle_parser.y"
            { free(((*yyvaluep).strval)); }
#line 1410 "turtle_parser.cpp"
        break;

    case YYSYMBOL_rdf_literal: /* rdf_literal  */
#line 119 "turtle_parser.y"
            { free(((*yyvaluep).strval)); }
#line 1416 "turtle_parser.cpp"
        break;

    case YYSYMBOL_boolean_literal: /* boolean_literal  */
#line 119 "turtle_parser.y"
            { free(((*yyvaluep).strval)); }
#line 1422 "turtle_parser.cpp"
        break;

    case YYSYMBOL_iri: /* iri  */
#line 119 "turtle_parser.y"
            { free(((*yyvaluep).strval)); }
#line 1428 "turtle_parser.cpp"
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
yyparse (yyscan_t scanner, TurtleParseContext& context, std::vector<std::tuple<std::string, std::string, std::string>>& triples)
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
# if defined TTLLTYPE_IS_TRIVIAL && TTLLTYPE_IS_TRIVIAL
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

  yychar = TTLEMPTY; /* Cause a token to be read.  */
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
  if (yychar == TTLEMPTY)
    {
      YYDPRINTF ((stderr, "Reading a token\n"));
      yychar = yylex (&yylval, &yylloc, scanner);
    }

  if (yychar <= TTLEOF)
    {
      yychar = TTLEOF;
      yytoken = YYSYMBOL_YYEOF;
      YYDPRINTF ((stderr, "Now at end of input.\n"));
    }
  else if (yychar == TTLerror)
    {
      /* The scanner already issued an error message, process directly
         to error recovery.  But do not keep the error token as
         lookahead, it is too special and may lead us to an endless
         loop in error recovery. */
      yychar = TTLUNDEF;
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
  yychar = TTLEMPTY;
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
  case 2: /* statement: directive  */
#line 145 "turtle_parser.y"
            { YYACCEPT; }
#line 1732 "turtle_parser.cpp"
    break;

  case 3: /* statement: triples  */
#line 147 "turtle_parser.y"
          { YYACCEPT; }
#line 1738 "turtle_parser.cpp"
    break;

  case 4: /* statement: END_OF_FILE  */
#line 149 "turtle_parser.y"
              { YYABORT; }
#line 1744 "turtle_parser.cpp"
    break;

  case 7: /* prefix: PREFIX prefix_id ':' IRI '.'  */
#line 163 "turtle_parser.y"
                               {
    context.prefixes[(yyvsp[-3].strval)] = (yyvsp[-1].strval);
    free((yyvsp[-3].strval));
    (yyvsp[-3].strval) = nullptr;
    free((yyvsp[-1].strval));
    (yyvsp[-1].strval) = nullptr;
  }
#line 1756 "turtle_parser.cpp"
    break;

  case 8: /* prefix: SPARQL_PREFIX prefix_id ':' IRI  */
#line 171 "turtle_parser.y"
                                  {
    context.prefixes[(yyvsp[-2].strval)] = (yyvsp[0].strval);
    free((yyvsp[-2].strval));
    (yyvsp[-2].strval) = nullptr;
    free((yyvsp[0].strval));
    (yyvsp[0].strval) = nullptr;
  }
#line 1768 "turtle_parser.cpp"
    break;

  case 9: /* prefix_id: IDENTIFIER  */
#line 181 "turtle_parser.y"
             { (yyval.strval) = (yyvsp[0].strval); }
#line 1774 "turtle_parser.cpp"
    break;

  case 10: /* prefix_id: %empty  */
#line 183 "turtle_parser.y"
              { (yyval.strval) = strdup(""); }
#line 1780 "turtle_parser.cpp"
    break;

  case 11: /* base: BASE IRI '.'  */
#line 188 "turtle_parser.y"
                { context.base = (yyvsp[-1].strval); free((yyvsp[-1].strval)); (yyvsp[-1].strval) = nullptr; }
#line 1786 "turtle_parser.cpp"
    break;

  case 12: /* base: SPARQL_BASE IRI  */
#line 190 "turtle_parser.y"
                   { context.base = (yyvsp[0].strval); free((yyvsp[0].strval)); (yyvsp[0].strval) = nullptr; }
#line 1792 "turtle_parser.cpp"
    break;

  case 13: /* triples: subject1 predicate_object_list '.'  */
#line 195 "turtle_parser.y"
                                     {
    char* subject = context.subjects.back();
    free(subject);
    context.subjects.pop_back();
  }
#line 1802 "turtle_parser.cpp"
    break;

  case 14: /* triples: subject2 predicate_object_list '.'  */
#line 201 "turtle_parser.y"
                                     {
    char* subject = context.subjects.back();
    free(subject);
    context.subjects.pop_back();
  }
#line 1812 "turtle_parser.cpp"
    break;

  case 15: /* triples: subject2 '.'  */
#line 207 "turtle_parser.y"
               {
    char* subject = context.subjects.back();
    free(subject);
    context.subjects.pop_back();
  }
#line 1822 "turtle_parser.cpp"
    break;

  case 16: /* predicate_object_list: verb object_list  */
#line 216 "turtle_parser.y"
                   {
    char* predicate = context.predicates.back();
    free(predicate);
    context.predicates.pop_back();
  }
#line 1832 "turtle_parser.cpp"
    break;

  case 17: /* predicate_object_list: predicate_object_list ';' verb object_list  */
#line 222 "turtle_parser.y"
                                             {
    char* predicate = context.predicates.back();
    free(predicate);
    context.predicates.pop_back();
  }
#line 1842 "turtle_parser.cpp"
    break;

  case 18: /* object_list: object  */
#line 231 "turtle_parser.y"
         {
    triples.push_back(std::make_tuple(context.subjects.back(), context.predicates.back(), (yyvsp[0].strval)));
    free((yyvsp[0].strval));
    (yyvsp[0].strval) = nullptr;
  }
#line 1852 "turtle_parser.cpp"
    break;

  case 19: /* object_list: object_list ',' object  */
#line 237 "turtle_parser.y"
                         {
    triples.push_back(std::make_tuple(context.subjects.back(), context.predicates.back(), (yyvsp[0].strval)));
    free((yyvsp[0].strval));
    (yyvsp[0].strval) = nullptr;
  }
#line 1862 "turtle_parser.cpp"
    break;

  case 20: /* verb: predicate  */
#line 247 "turtle_parser.y"
            { context.predicates.push_back((yyvsp[0].strval)); }
#line 1868 "turtle_parser.cpp"
    break;

  case 21: /* verb: type  */
#line 249 "turtle_parser.y"
       { context.predicates.push_back((yyvsp[0].strval)); }
#line 1874 "turtle_parser.cpp"
    break;

  case 22: /* subject1: iri  */
#line 254 "turtle_parser.y"
      { context.subjects.push_back((yyvsp[0].strval)); }
#line 1880 "turtle_parser.cpp"
    break;

  case 23: /* subject1: blank_node  */
#line 256 "turtle_parser.y"
             { context.subjects.push_back((yyvsp[0].strval)); }
#line 1886 "turtle_parser.cpp"
    break;

  case 24: /* subject1: collection  */
#line 258 "turtle_parser.y"
             { context.subjects.push_back((yyvsp[0].strval)); }
#line 1892 "turtle_parser.cpp"
    break;

  case 25: /* subject2: blank_node_property_list  */
#line 262 "turtle_parser.y"
                           { context.subjects.push_back((yyvsp[0].strval)); }
#line 1898 "turtle_parser.cpp"
    break;

  case 26: /* predicate: iri  */
#line 267 "turtle_parser.y"
      { (yyval.strval) = (yyvsp[0].strval); }
#line 1904 "turtle_parser.cpp"
    break;

  case 27: /* type: IDENTIFIER  */
#line 271 "turtle_parser.y"
             {
    if(strcmp((yyvsp[0].strval), "a") != 0){
      yyerror (&yylloc, scanner, context, triples, YY_("syntax error, unexpected IDENTIFIER, expecting ':' or 'a'"));
      YYERROR;
    }
    free((yyvsp[0].strval));
    (yyvsp[0].strval) = nullptr;
    (yyval.strval) = strdup(RDFVocabulary::RDF_TYPE.c_str());
  }
#line 1918 "turtle_parser.cpp"
    break;

  case 28: /* object: iri  */
#line 284 "turtle_parser.y"
      { (yyval.strval) = (yyvsp[0].strval); }
#line 1924 "turtle_parser.cpp"
    break;

  case 29: /* object: literal  */
#line 286 "turtle_parser.y"
          { (yyval.strval) = (yyvsp[0].strval); }
#line 1930 "turtle_parser.cpp"
    break;

  case 30: /* object: blank_node  */
#line 288 "turtle_parser.y"
             { (yyval.strval) = (yyvsp[0].strval); }
#line 1936 "turtle_parser.cpp"
    break;

  case 31: /* object: blank_node_property_list  */
#line 290 "turtle_parser.y"
                           { (yyval.strval) = (yyvsp[0].strval); }
#line 1942 "turtle_parser.cpp"
    break;

  case 32: /* object: collection  */
#line 292 "turtle_parser.y"
             { (yyval.strval) = (yyvsp[0].strval); }
#line 1948 "turtle_parser.cpp"
    break;

  case 33: /* blank_node_property_list: open_square_bracket predicate_object_list ']'  */
#line 297 "turtle_parser.y"
                                                {
    char *blankNode = context.subjects.back();
    context.subjects.pop_back();
    (yyval.strval) = blankNode;
  }
#line 1958 "turtle_parser.cpp"
    break;

  case 34: /* open_square_bracket: '['  */
#line 305 "turtle_parser.y"
      {
    std::string bknode_id = context.bknode_id_gen.generate();
    context.subjects.push_back(strdup(bknode_id.c_str()));
  }
#line 1967 "turtle_parser.cpp"
    break;

  case 35: /* blank_node: '_' ':' IDENTIFIER  */
#line 312 "turtle_parser.y"
                     {
    std::string bknode_id;
    if(context.blank_nodes.count((yyvsp[0].strval))) {
      bknode_id = context.blank_nodes[(yyvsp[0].strval)];
    }
    else {
      bknode_id = context.bknode_id_gen.generate();
      context.blank_nodes[(yyvsp[0].strval)] = bknode_id;
    }
    free((yyvsp[0].strval));
    (yyvsp[0].strval) = nullptr;
    (yyval.strval) = strdup(bknode_id.c_str());
  }
#line 1985 "turtle_parser.cpp"
    break;

  case 36: /* blank_node: ANON  */
#line 326 "turtle_parser.y"
       {
    std::string bknode_id = context.bknode_id_gen.generate();
    (yyval.strval) = strdup(bknode_id.c_str());
  }
#line 1994 "turtle_parser.cpp"
    break;

  case 37: /* collection: '(' item_list ')'  */
#line 334 "turtle_parser.y"
                    { (yyval.strval) = (yyvsp[-1].strval); }
#line 2000 "turtle_parser.cpp"
    break;

  case 38: /* item_list: item_list_not_empty  */
#line 338 "turtle_parser.y"
                      {
    triples.push_back(std::make_tuple(context.subjects.back(), RDFVocabulary::RDF_REST, RDFVocabulary::RDF_NIL));
    free(context.subjects.back());
    context.subjects.pop_back();
    (yyval.strval) = (yyvsp[0].strval);
  }
#line 2011 "turtle_parser.cpp"
    break;

  case 39: /* item_list: %empty  */
#line 345 "turtle_parser.y"
  {
    std::string bknode_id = context.bknode_id_gen.generate();
    char* item = strdup(bknode_id.c_str());
    triples.push_back(std::make_tuple(item, RDFVocabulary::RDF_FIRST, RDFVocabulary::RDF_NIL));
    triples.push_back(std::make_tuple(item, RDFVocabulary::RDF_REST, RDFVocabulary::RDF_NIL));
    (yyval.strval) = item;
  }
#line 2023 "turtle_parser.cpp"
    break;

  case 40: /* item_list_not_empty: object  */
#line 355 "turtle_parser.y"
         {
    std::string bknode_id = context.bknode_id_gen.generate();
    char* item = strdup(bknode_id.c_str());
    triples.push_back(std::make_tuple(item, RDFVocabulary::RDF_FIRST, (yyvsp[0].strval)));
    free((yyvsp[0].strval));
    context.subjects.push_back(strdup(bknode_id.c_str()));
    (yyval.strval) = item;
  }
#line 2036 "turtle_parser.cpp"
    break;

  case 41: /* item_list_not_empty: item_list_not_empty object  */
#line 364 "turtle_parser.y"
                             {
    std::string bknode_id = context.bknode_id_gen.generate();
    char* item = strdup(bknode_id.c_str());
    triples.push_back(std::make_tuple(context.subjects.back(), RDFVocabulary::RDF_REST, item));
    triples.push_back(std::make_tuple(item, RDFVocabulary::RDF_FIRST, (yyvsp[0].strval)));
    free((yyvsp[0].strval));
    free(context.subjects.back());
    context.subjects.pop_back();
    context.subjects.push_back(item);
    (yyval.strval) = (yyvsp[-1].strval);
  }
#line 2052 "turtle_parser.cpp"
    break;

  case 42: /* literal: rdf_literal  */
#line 379 "turtle_parser.y"
              { (yyval.strval) = (yyvsp[0].strval); }
#line 2058 "turtle_parser.cpp"
    break;

  case 43: /* literal: numeric_literal  */
#line 381 "turtle_parser.y"
                  { (yyval.strval) = (yyvsp[0].strval); }
#line 2064 "turtle_parser.cpp"
    break;

  case 44: /* literal: boolean_literal  */
#line 383 "turtle_parser.y"
                  { (yyval.strval) = (yyvsp[0].strval); }
#line 2070 "turtle_parser.cpp"
    break;

  case 45: /* numeric_literal: INTEGER  */
#line 389 "turtle_parser.y"
          {
    std::stringstream ss;
    ss << "\"" << (yyvsp[0].strval) << "\"" << "^^" << XSDVocabulary::XSD_INTEGER;
    (yyval.strval) = strdup(ss.str().c_str());
  }
#line 2080 "turtle_parser.cpp"
    break;

  case 46: /* numeric_literal: DECIMAL  */
#line 395 "turtle_parser.y"
          {
    std::stringstream ss;
    ss << "\"" << (yyvsp[0].strval) << "\"" << "^^" << XSDVocabulary::XSD_DECIMAL;
    (yyval.strval) = strdup(ss.str().c_str());
  }
#line 2090 "turtle_parser.cpp"
    break;

  case 47: /* numeric_literal: DOUBLE  */
#line 401 "turtle_parser.y"
         {
    std::stringstream ss;
    ss << "\"" << (yyvsp[0].strval) << "\"" << "^^" << XSDVocabulary::XSD_DOUBLE;
    (yyval.strval) = strdup(ss.str().c_str());
  }
#line 2100 "turtle_parser.cpp"
    break;

  case 48: /* rdf_literal: STRING  */
#line 409 "turtle_parser.y"
         {
    std::stringstream ss;
    ss << "\"" << (yyvsp[0].strval) << "\"" << "^^" << XSDVocabulary::XSD_STRING;
    free((yyvsp[0].strval));
    (yyvsp[0].strval) = nullptr;
    (yyval.strval) = strdup(ss.str().c_str());
  }
#line 2112 "turtle_parser.cpp"
    break;

  case 49: /* rdf_literal: STRING LANGTAG  */
#line 417 "turtle_parser.y"
                 {
    std::stringstream ss;
    ss << "\"" << (yyvsp[-1].strval) << "\"" << (yyvsp[0].strval);
    free((yyvsp[-1].strval));
    (yyvsp[-1].strval) = nullptr;
    free((yyvsp[0].strval));
    (yyvsp[0].strval) = nullptr;
    (yyval.strval) = strdup(ss.str().c_str());
  }
#line 2126 "turtle_parser.cpp"
    break;

  case 50: /* rdf_literal: STRING TYPE iri  */
#line 427 "turtle_parser.y"
                  {
    std::stringstream ss;
    ss << "\"" << (yyvsp[-2].strval) << "\"" << "^^" << (yyvsp[0].strval);
    free((yyvsp[-2].strval));
    (yyvsp[-2].strval) = nullptr;
    free((yyvsp[0].strval));
    (yyvsp[0].strval) = nullptr;
    (yyval.strval) = strdup(ss.str().c_str());
  }
#line 2140 "turtle_parser.cpp"
    break;

  case 51: /* boolean_literal: BOOL_TRUE  */
#line 439 "turtle_parser.y"
            {
    std::stringstream ss;
    ss << "\"true\"" << "^^" << XSDVocabulary::XSD_BOOLEAN;
    (yyval.strval) = strdup(ss.str().c_str());
  }
#line 2150 "turtle_parser.cpp"
    break;

  case 52: /* boolean_literal: BOOL_FALSE  */
#line 445 "turtle_parser.y"
             {
    std::stringstream ss;
    ss << "\"false\"" << "^^" << XSDVocabulary::XSD_BOOLEAN;
    (yyval.strval) = strdup(ss.str().c_str());
  }
#line 2160 "turtle_parser.cpp"
    break;

  case 53: /* iri: IRI  */
#line 453 "turtle_parser.y"
      {
    std::string str = (yyvsp[0].strval);
    if(!context.base.empty() && str.find("://")==std::string::npos){
      str.insert(str.begin()+1, context.base.begin()+1, context.base.end()-1);
    }
    free((yyvsp[0].strval));
    (yyvsp[0].strval) = nullptr;
    (yyval.strval) = strdup(str.c_str());
  }
#line 2174 "turtle_parser.cpp"
    break;

  case 54: /* iri: IDENTIFIER ':' IDENTIFIER  */
#line 463 "turtle_parser.y"
                            {
    std::string str = context.prefixes[(yyvsp[-2].strval)];
    str.insert(str.length()-1, (yyvsp[0].strval), strlen((yyvsp[0].strval)));
    free((yyvsp[-2].strval));
    (yyvsp[-2].strval) = nullptr;
    free((yyvsp[0].strval));
    (yyvsp[0].strval) = nullptr;
    (yyval.strval) = strdup(str.c_str());
  }
#line 2188 "turtle_parser.cpp"
    break;

  case 55: /* iri: ':' IDENTIFIER  */
#line 473 "turtle_parser.y"
                {
    std::string str = context.prefixes[""];
    str.insert(str.length()-1, (yyvsp[0].strval), strlen((yyvsp[0].strval)));
    free((yyvsp[0].strval));
    (yyvsp[0].strval) = nullptr;
    (yyval.strval) = strdup(str.c_str());
  }
#line 2200 "turtle_parser.cpp"
    break;


#line 2204 "turtle_parser.cpp"

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
  yytoken = yychar == TTLEMPTY ? YYSYMBOL_YYEMPTY : YYTRANSLATE (yychar);
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
        yyerror (&yylloc, scanner, context, triples, yymsgp);
        if (yysyntax_error_status == YYENOMEM)
          goto yyexhaustedlab;
      }
    }

  yyerror_range[1] = yylloc;
  if (yyerrstatus == 3)
    {
      /* If just tried and failed to reuse lookahead token after an
         error, discard it.  */

      if (yychar <= TTLEOF)
        {
          /* Return failure if at end of input.  */
          if (yychar == TTLEOF)
            YYABORT;
        }
      else
        {
          yydestruct ("Error: discarding",
                      yytoken, &yylval, &yylloc, scanner, context, triples);
          yychar = TTLEMPTY;
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
                  YY_ACCESSING_SYMBOL (yystate), yyvsp, yylsp, scanner, context, triples);
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
  yyerror (&yylloc, scanner, context, triples, YY_("memory exhausted"));
  yyresult = 2;
  goto yyreturn;
#endif


/*-------------------------------------------------------.
| yyreturn -- parsing is finished, clean up and return.  |
`-------------------------------------------------------*/
yyreturn:
  if (yychar != TTLEMPTY)
    {
      /* Make sure we have latest lookahead translation.  See comments at
         user semantic actions for why this is necessary.  */
      yytoken = YYTRANSLATE (yychar);
      yydestruct ("Cleanup: discarding lookahead",
                  yytoken, &yylval, &yylloc, scanner, context, triples);
    }
  /* Do not reclaim the symbols of the rule whose action triggered
     this YYABORT or YYACCEPT.  */
  YYPOPSTACK (yylen);
  YY_STACK_PRINT (yyss, yyssp);
  while (yyssp != yyss)
    {
      yydestruct ("Cleanup: popping",
                  YY_ACCESSING_SYMBOL (+*yyssp), yyvsp, yylsp, scanner, context, triples);
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

#line 482 "turtle_parser.y"




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
