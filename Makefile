#Makefile

ROOT_DIR = .

SRC_DIR = $(ROOT_DIR)/src
TOOL_DIR = $(ROOT_DIR)/tools
TEST_DIR = $(ROOT_DIR)/test
TP_DIR = $(ROOT_DIR)/third_party


OBJ_DIR = $(ROOT_DIR)/obj
LIB_DIR = $(ROOT_DIR)/lib
BIN_DIR = $(ROOT_DIR)/bin


HEADER_PATH = /usr/local/include/bphj
LIB_PATH = /usr/local/lib
CONF_PATH = /etc/bphj/
DATA_PATH = /var/lib/bphj

CXX = g++

CXXFLAGS = -std=c++2a -g -Wall -Wextra -pthread -w

CPPFLAGS = -isystem $(ROOT_DIR)/include -isystem $(GTEST_DIR)/include -isystem $(TP_DIR)

LDFLAGS = -L/usr/local/lib

LDLIBS = -lpthread -lrocksdb -ldl -lsnappy -lbz2 -lrt -lz -llz4 -lzstd -lyaml-cpp -lserd-0 -lroaring -ldb_cxx-5.3

# LDLIBS = -lpthread -lrocksdb -lorc -lsnappy -lbz2 -lrt -lz -llz4 -lzstd -lyaml-cpp -lserd-0 \
         -lprotobuf -lprotoc -lhdfspp_static -lsasl2 -lcrypto -lroaring -ldb_cxx-5.3


MPICXX = mpicxx


BMP_OBJS = $(OBJ_DIR)/bitvector.o $(OBJ_DIR)/roaring_bitvector.o
BUF_OBJS = $(OBJ_DIR)/buffer_manager.o $(OBJ_DIR)/buffer_page.o
DB_OBJS = $(OBJ_DIR)/config.o $(OBJ_DIR)/database.o $(OBJ_DIR)/database_builder.o \
          $(OBJ_DIR)/statistics_manager.o
KVS_OBJS = $(OBJ_DIR)/rocksdb_store.o
OPR_OBJS = $(OBJ_DIR)/operator.o \
           $(OBJ_DIR)/table_scan.o $(OBJ_DIR)/filter.o \
           $(OBJ_DIR)/hash_join.o $(OBJ_DIR)/backprobe_hash_join.o \
					 $(OBJ_DIR)/results_printer.o
#$(OBJ_DIR)/bitmap_index_scan.o
PARSER_OBJS = $(OBJ_DIR)/rdf_util.o $(OBJ_DIR)/sparql_lexer.o $(OBJ_DIR)/sparql_parser.o \
              $(OBJ_DIR)/turtle_lexer.o $(OBJ_DIR)/turtle_parser.o
PLAN_OBJS = $(OBJ_DIR)/query_plan.o $(OBJ_DIR)/query_planner.o
QUERY_OBJS = $(OBJ_DIR)/query_graph.o $(OBJ_DIR)/semantic_analyzer.o
RTM_OBJS = $(OBJ_DIR)/code_generator.o $(OBJ_DIR)/runtime.o
STG_OBJS = $(OBJ_DIR)/dictionary.o $(OBJ_DIR)/triple_table.o
THRD_OBJS = $(OBJ_DIR)/condition.o $(OBJ_DIR)/mutex.o $(OBJ_DIR)/rw_latch.o $(OBJ_DIR)/thread.o
UTIL_OBJS = $(OBJ_DIR)/bdb_file.o $(OBJ_DIR)/bit_set.o $(OBJ_DIR)/byte_buffer.o \
            $(OBJ_DIR)/file_directory.o $(OBJ_DIR)/math_functions.o $(OBJ_DIR)/string_util.o

ALL_OBJS = $(BMP_OBJS) $(BUF_OBJS) $(DB_OBJS) $(KVS_OBJS) $(OPR_OBJS) $(PARSER_OBJS) $(PLAN_OBJS) \
           $(QUERY_OBJS) $(RTM_OBJS) $(STG_OBJS) $(THRD_OBJS) $(UTIL_OBJS)

TEST_OBJS = $(OBJ_DIR)/test_main.o $(OBJ_DIR)/bitvector_test.o \
						$(OBJ_DIR)/hash_table_test.o $(OBJ_DIR)/memory_pool_test.o $(OBJ_DIR)/static_vector_test.o \
            $(OBJ_DIR)/sparql_parser_test.o $(OBJ_DIR)/turtle_parser_test.o


TP_OBJS = $(OBJ_DIR)/murmur_hash3.o


TARGET = $(BIN_DIR)/dbtool


all: build


build: dirs $(TARGET)
	@ echo "Build finished."


test: dirs $(LIB_DIR)/libbphj.a $(BIN_DIR)/dbtest
	@ echo "Tests starting..."
	@ $(BIN_DIR)/dbtest


install: build
	@ mkdir -p $(HEADER_PATH) $(CONF_PATH) $(DATA_PATH)
	@ chmod 777 $(DATA_PATH)
	@ # cp $(ROOT_DIR)/include/*.h $(HEADER_PATH)
	@ echo "Installation finished."


clean:
	@ rm -f $(OBJ_DIR)/*.o $(LIB_DIR)/*.a $(BIN_DIR)/*


dirs:
	@ mkdir -p $(OBJ_DIR) $(LIB_DIR) $(BIN_DIR)



#DB Tools
$(BIN_DIR)/dbtool: $(OBJ_DIR)/command.o $(OBJ_DIR)/db_tool.o $(LIB_DIR)/libbphj.a
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDLIBS)


$(OBJ_DIR)/command.o: $(TOOL_DIR)/command.h $(TOOL_DIR)/command.cpp
	$(CXX) $(CPPFLAGS) -I$(SRC_DIR) $(CXXFLAGS) -c -o $@ $(TOOL_DIR)/command.cpp

$(OBJ_DIR)/db_tool.o: $(TOOL_DIR)/db_tool.cpp
	$(CXX) $(CPPFLAGS) -I$(SRC_DIR) $(CXXFLAGS) -c -o $@ $(TOOL_DIR)/db_tool.cpp

#DB Library
$(LIB_DIR)/libbphj.a: $(ALL_OBJS) $(TP_OBJS)
	$(AR) $(ARFLAGS) $@ $^



$(OBJ_DIR)/bitvector.o: $(SRC_DIR)/bitmap/bitvector.h $(SRC_DIR)/bitmap/bitvector.cpp
	$(CXX) $(CPPFLAGS) -I$(SRC_DIR) $(CXXFLAGS) -c -o $@ $(SRC_DIR)/bitmap/bitvector.cpp

$(OBJ_DIR)/roaring_bitvector.o: $(SRC_DIR)/bitmap/roaring_bitvector.h $(SRC_DIR)/bitmap/roaring_bitvector.cpp
	$(CXX) $(CPPFLAGS) -I$(SRC_DIR) $(CXXFLAGS) -c -o $@ $(SRC_DIR)/bitmap/roaring_bitvector.cpp

$(OBJ_DIR)/config.o: $(SRC_DIR)/database/config.h $(SRC_DIR)/database/config.cpp
	$(CXX) $(CPPFLAGS) -I$(SRC_DIR) $(CXXFLAGS) -c -o $@ $(SRC_DIR)/database/config.cpp

$(OBJ_DIR)/database.o: $(SRC_DIR)/database/database.h $(SRC_DIR)/database/database.cpp
	$(CXX) $(CPPFLAGS) -I$(SRC_DIR) $(CXXFLAGS) -c -o $@ $(SRC_DIR)/database/database.cpp

$(OBJ_DIR)/database_builder.o: $(SRC_DIR)/database/database_builder.h $(SRC_DIR)/database/database_builder.cpp
	$(CXX) $(CPPFLAGS) -I$(SRC_DIR) $(CXXFLAGS) -c -o $@ $(SRC_DIR)/database/database_builder.cpp

$(OBJ_DIR)/statistics_manager.o: $(SRC_DIR)/database/statistics_manager.h $(SRC_DIR)/database/statistics_manager.cpp
	$(CXX) $(CPPFLAGS) -I$(SRC_DIR) $(CXXFLAGS) -c -o $@ $(SRC_DIR)/database/statistics_manager.cpp

$(OBJ_DIR)/rocksdb_store.o: $(SRC_DIR)/kvstore/rocksdb_store.h $(SRC_DIR)/kvstore/rocksdb_store.cpp
	$(CXX) $(CPPFLAGS) -I$(SRC_DIR) $(CXXFLAGS) -c -o $@ $(SRC_DIR)/kvstore/rocksdb_store.cpp

$(OBJ_DIR)/operator.o: $(SRC_DIR)/operator/operator.h $(SRC_DIR)/operator/operator.cpp
	$(CXX) $(CPPFLAGS) -I$(SRC_DIR) $(CXXFLAGS) -c -o $@ $(SRC_DIR)/operator/operator.cpp

$(OBJ_DIR)/table_scan.o: $(SRC_DIR)/operator/table_scan.h $(SRC_DIR)/operator/table_scan.cpp
	$(CXX) $(CPPFLAGS) -I$(SRC_DIR) $(CXXFLAGS) -c -o $@ $(SRC_DIR)/operator/table_scan.cpp

$(OBJ_DIR)/bitmap_index_scan.o: $(SRC_DIR)/operator/bitmap_index_scan.h $(SRC_DIR)/operator/bitmap_index_scan.cpp
	$(CXX) $(CPPFLAGS) -I$(SRC_DIR) $(CXXFLAGS) -c -o $@ $(SRC_DIR)/operator/bitmap_index_scan.cpp

$(OBJ_DIR)/filter.o: $(SRC_DIR)/operator/filter.h $(SRC_DIR)/operator/filter.cpp
	$(CXX) $(CPPFLAGS) -I$(SRC_DIR) $(CXXFLAGS) -c -o $@ $(SRC_DIR)/operator/filter.cpp

$(OBJ_DIR)/hash_join.o: $(SRC_DIR)/operator/hash_join.h $(SRC_DIR)/operator/hash_join.cpp
	$(CXX) $(CPPFLAGS) -I$(SRC_DIR) $(CXXFLAGS) -c -o $@ $(SRC_DIR)/operator/hash_join.cpp

$(OBJ_DIR)/backprobe_hash_join.o: $(SRC_DIR)/operator/backprobe_hash_join.h $(SRC_DIR)/operator/backprobe_hash_join.cpp
	$(CXX) $(CPPFLAGS) -I$(SRC_DIR) $(CXXFLAGS) -c -o $@ $(SRC_DIR)/operator/backprobe_hash_join.cpp

$(OBJ_DIR)/results_printer.o: $(SRC_DIR)/operator/results_printer.h $(SRC_DIR)/operator/results_printer.cpp
	$(CXX) $(CPPFLAGS) -I$(SRC_DIR) $(CXXFLAGS) -c -o $@ $(SRC_DIR)/operator/results_printer.cpp

$(OBJ_DIR)/rdf_util.o: $(SRC_DIR)/parser/rdf_util.h $(SRC_DIR)/parser/rdf_util.cpp
	$(CXX) $(CPPFLAGS) -I$(SRC_DIR) $(CXXFLAGS) -c -o $@ $(SRC_DIR)/parser/rdf_util.cpp

$(OBJ_DIR)/sparql_lexer.o: $(SRC_DIR)/parser/sparql_lexer.h $(SRC_DIR)/parser/sparql_lexer.cpp
	$(CXX) $(CPPFLAGS) -I$(SRC_DIR) $(CXXFLAGS) -c -o $@ $(SRC_DIR)/parser/sparql_lexer.cpp

$(OBJ_DIR)/sparql_parser.o: $(SRC_DIR)/parser/sparql_parser.h $(SRC_DIR)/parser/sparql_parser.cpp
	$(CXX) $(CPPFLAGS) -I$(SRC_DIR) $(CXXFLAGS) -c -o $@ $(SRC_DIR)/parser/sparql_parser.cpp

$(OBJ_DIR)/turtle_lexer.o: $(SRC_DIR)/parser/turtle_lexer.h $(SRC_DIR)/parser/turtle_lexer.cpp
	$(CXX) $(CPPFLAGS) -I$(SRC_DIR) $(CXXFLAGS) -c -o $@ $(SRC_DIR)/parser/turtle_lexer.cpp

$(OBJ_DIR)/turtle_parser.o: $(SRC_DIR)/parser/turtle_parser.h $(SRC_DIR)/parser/turtle_parser.cpp
	$(CXX) $(CPPFLAGS) -I$(SRC_DIR) $(CXXFLAGS) -c -o $@ $(SRC_DIR)/parser/turtle_parser.cpp

$(OBJ_DIR)/query_plan.o: $(SRC_DIR)/plan/query_plan.h $(SRC_DIR)/plan/query_plan.cpp
	$(CXX) $(CPPFLAGS) -I$(SRC_DIR) $(CXXFLAGS) -c -o $@ $(SRC_DIR)/plan/query_plan.cpp

$(OBJ_DIR)/query_planner.o: $(SRC_DIR)/plan/query_planner.h $(SRC_DIR)/plan/query_planner.cpp
	$(CXX) $(CPPFLAGS) -I$(SRC_DIR) $(CXXFLAGS) -c -o $@ $(SRC_DIR)/plan/query_planner.cpp

$(OBJ_DIR)/query_graph.o: $(SRC_DIR)/query/query_graph.h $(SRC_DIR)/query/query_graph.cpp
	$(CXX) $(CPPFLAGS) -I$(SRC_DIR) $(CXXFLAGS) -c -o $@ $(SRC_DIR)/query/query_graph.cpp

$(OBJ_DIR)/semantic_analyzer.o: $(SRC_DIR)/query/semantic_analyzer.h $(SRC_DIR)/query/semantic_analyzer.cpp
	$(CXX) $(CPPFLAGS) -I$(SRC_DIR) $(CXXFLAGS) -c -o $@ $(SRC_DIR)/query/semantic_analyzer.cpp

$(OBJ_DIR)/code_generator.o: $(SRC_DIR)/runtime/code_generator.h $(SRC_DIR)/runtime/code_generator.cpp
	$(CXX) $(CPPFLAGS) -I$(SRC_DIR) $(CXXFLAGS) -c -o $@ $(SRC_DIR)/runtime/code_generator.cpp

$(OBJ_DIR)/runtime.o: $(SRC_DIR)/runtime/runtime.h $(SRC_DIR)/runtime/runtime.cpp
	$(CXX) $(CPPFLAGS) -I$(SRC_DIR) $(CXXFLAGS) -c -o $@ $(SRC_DIR)/runtime/runtime.cpp

$(OBJ_DIR)/buffer_manager.o: $(SRC_DIR)/buffer/buffer_manager.h $(SRC_DIR)/buffer/buffer_manager.cpp
	$(CXX) $(CPPFLAGS) -I$(SRC_DIR) $(CXXFLAGS) -c -o $@ $(SRC_DIR)/buffer/buffer_manager.cpp

$(OBJ_DIR)/buffer_page.o: $(SRC_DIR)/buffer/buffer_page.h $(SRC_DIR)/buffer/buffer_page.cpp
	$(CXX) $(CPPFLAGS) -I$(SRC_DIR) $(CXXFLAGS) -c -o $@ $(SRC_DIR)/buffer/buffer_page.cpp

$(OBJ_DIR)/dictionary.o: $(SRC_DIR)/storage/dictionary.h $(SRC_DIR)/storage/dictionary.cpp
	$(CXX) $(CPPFLAGS) -I$(SRC_DIR) $(CXXFLAGS) -c -o $@ $(SRC_DIR)/storage/dictionary.cpp

$(OBJ_DIR)/triple_table.o: $(SRC_DIR)/storage/triple_table.h $(SRC_DIR)/storage/triple_table.cpp
	$(CXX) $(CPPFLAGS) -I$(SRC_DIR) $(CXXFLAGS) -c -o $@ $(SRC_DIR)/storage/triple_table.cpp

$(OBJ_DIR)/condition.o: $(SRC_DIR)/thread/condition.h $(SRC_DIR)/thread/condition.cpp
	$(CXX) $(CPPFLAGS) -I$(SRC_DIR) $(CXXFLAGS) -c -o $@ $(SRC_DIR)/thread/condition.cpp

$(OBJ_DIR)/mutex.o: $(SRC_DIR)/thread/mutex.h $(SRC_DIR)/thread/mutex.cpp
	$(CXX) $(CPPFLAGS) -I$(SRC_DIR) $(CXXFLAGS) -c -o $@ $(SRC_DIR)/thread/mutex.cpp

$(OBJ_DIR)/rw_latch.o: $(SRC_DIR)/thread/rw_latch.h $(SRC_DIR)/thread/rw_latch.cpp
	$(CXX) $(CPPFLAGS) -I$(SRC_DIR) $(CXXFLAGS) -c -o $@ $(SRC_DIR)/thread/rw_latch.cpp

$(OBJ_DIR)/thread.o: $(SRC_DIR)/thread/thread.h $(SRC_DIR)/thread/thread.cpp
	$(CXX) $(CPPFLAGS) -I$(SRC_DIR) $(CXXFLAGS) -c -o $@ $(SRC_DIR)/thread/thread.cpp

$(OBJ_DIR)/bdb_file.o: $(SRC_DIR)/util/bdb_file.h $(SRC_DIR)/util/bdb_file.cpp
	$(CXX) $(CPPFLAGS) -I$(SRC_DIR) $(CXXFLAGS) -c -o $@ $(SRC_DIR)/util/bdb_file.cpp

$(OBJ_DIR)/bit_set.o: $(SRC_DIR)/util/bit_set.h $(SRC_DIR)/util/bit_set.cpp
	$(CXX) $(CPPFLAGS) -I$(SRC_DIR) $(CXXFLAGS) -c -o $@ $(SRC_DIR)/util/bit_set.cpp

$(OBJ_DIR)/byte_buffer.o: $(SRC_DIR)/util/byte_buffer.h $(SRC_DIR)/util/byte_buffer.cpp
	$(CXX) $(CPPFLAGS) -I$(SRC_DIR) $(CXXFLAGS) -c -o $@ $(SRC_DIR)/util/byte_buffer.cpp

$(OBJ_DIR)/file_reader_writer.o: $(SRC_DIR)/util/file_reader_writer.h $(SRC_DIR)/util/file_reader_writer.cpp
	$(CXX) $(CPPFLAGS) -I$(SRC_DIR) $(CXXFLAGS) -c -o $@ $(SRC_DIR)/util/file_reader_writer.cpp

$(OBJ_DIR)/file_directory.o: $(SRC_DIR)/util/file_directory.h $(SRC_DIR)/util/file_directory.cpp
	$(CXX) $(CPPFLAGS) -I$(SRC_DIR) $(CXXFLAGS) -c -o $@ $(SRC_DIR)/util/file_directory.cpp

$(OBJ_DIR)/math_functions.o: $(SRC_DIR)/util/math_functions.h $(SRC_DIR)/util/math_functions.cpp
	$(CXX) $(CPPFLAGS) -I$(SRC_DIR) $(CXXFLAGS) -c -o $@ $(SRC_DIR)/util/math_functions.cpp

$(OBJ_DIR)/string_util.o: $(SRC_DIR)/util/string_util.h $(SRC_DIR)/util/string_util.cpp
	$(CXX) $(CPPFLAGS) -I$(SRC_DIR) $(CXXFLAGS) -c -o $@ $(SRC_DIR)/util/string_util.cpp



#DB Test
$(BIN_DIR)/dbtest: $(OBJ_DIR)/gtest-all.o $(TEST_OBJS) $(LIB_DIR)/libbphj.a
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDLIBS)


$(OBJ_DIR)/test_main.o: $(TEST_DIR)/test_main.cpp
	$(CXX) $(CPPFLAGS) -I$(SRC_DIR) $(CXXFLAGS) -c -o $@ $(TEST_DIR)/test_main.cpp

$(OBJ_DIR)/bitvector_test.o: $(TEST_DIR)/bitmap/bitvector_test.cpp
	$(CXX) $(CPPFLAGS) -I$(SRC_DIR) $(CXXFLAGS) -c -o $@ $(TEST_DIR)/bitmap/bitvector_test.cpp

$(OBJ_DIR)/sparql_parser_test.o: $(TEST_DIR)/parser/sparql_parser_test.cpp
	$(CXX) $(CPPFLAGS) -I$(SRC_DIR) $(CXXFLAGS) -c -o $@ $(TEST_DIR)/parser/sparql_parser_test.cpp

$(OBJ_DIR)/turtle_parser_test.o: $(TEST_DIR)/parser/turtle_parser_test.cpp
	$(CXX) $(CPPFLAGS) -I$(SRC_DIR) $(CXXFLAGS) -c -o $@ $(TEST_DIR)/parser/turtle_parser_test.cpp

$(OBJ_DIR)/hash_table_test.o: $(TEST_DIR)/util/hash_table_test.cpp
	$(CXX) $(CPPFLAGS) -I$(SRC_DIR) $(CXXFLAGS) -c -o $@ $(TEST_DIR)/util/hash_table_test.cpp

$(OBJ_DIR)/memory_pool_test.o: $(TEST_DIR)/util/memory_pool_test.cpp
	$(CXX) $(CPPFLAGS) -I$(SRC_DIR) $(CXXFLAGS) -c -o $@ $(TEST_DIR)/util/memory_pool_test.cpp

$(OBJ_DIR)/static_vector_test.o: $(TEST_DIR)/util/static_vector_test.cpp
	$(CXX) $(CPPFLAGS) -I$(SRC_DIR) $(CXXFLAGS) -c -o $@ $(TEST_DIR)/util/static_vector_test.cpp



#Third Party

#Smhasher
$(OBJ_DIR)/murmur_hash3.o: $(TP_DIR)/smhasher/MurmurHash3.h $(TP_DIR)/smhasher/MurmurHash3.cpp
	$(CXX) $(CXXFLAGS) -c -o $@ $(TP_DIR)/smhasher/MurmurHash3.cpp


#Gtest
GTEST_DIR = $(ROOT_DIR)/third_party/googletest

GTEST_HEADERS = $(GTEST_DIR)/include/gtest/*.h \
                $(GTEST_DIR)/include/gtest/internal/*.h

GTEST_SRCS = $(GTEST_DIR)/src/*.cc $(GTEST_DIR)/src/*.h $(GTEST_HEADERS)

$(OBJ_DIR)/gtest-all.o: $(GTEST_SRCS)
	$(CXX) -isystem $(GTEST_DIR)/include -I$(GTEST_DIR) $(CXXFLAGS) -c -o $@ $(GTEST_DIR)/src/gtest-all.cc
