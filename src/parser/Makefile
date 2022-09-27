LEX = flex
YACC = bison

all: sparql_parser.cpp sparql_lexer.cpp turtle_parser.cpp turtle_lexer.cpp

sparql_parser.cpp: sparql_parser.y
	${YACC} --defines=sparql_lexer.h --output=sparql_parser.cpp sparql_parser.y

sparql_lexer.cpp: sparql_lexer.l
	${LEX} -o sparql_lexer.cpp sparql_lexer.l

turtle_parser.cpp: turtle_parser.y
	${YACC} --defines=turtle_lexer.h --output=turtle_parser.cpp turtle_parser.y

turtle_lexer.cpp: turtle_lexer.l
	${LEX} -o turtle_lexer.cpp turtle_lexer.l

clean:
	rm -f sparql_parser.cpp sparql_lexer.h sparql_lexer.cpp turtle_parser.cpp turtle_lexer.h turtle_lexer.cpp
