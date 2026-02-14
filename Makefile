CC ?= clang
CFLAGS ?= -std=c11 -Wall -Wextra -pedantic -I include

TEST_BIN := test_runner
SRC := src/holons.c
TEST_SRC := test/holons_test.c

.PHONY: test clean

test: $(TEST_BIN)
	./$(TEST_BIN)

$(TEST_BIN): $(SRC) $(TEST_SRC) include/holons/holons.h
	$(CC) $(CFLAGS) $(SRC) $(TEST_SRC) -o $(TEST_BIN)

clean:
	rm -f $(TEST_BIN)
