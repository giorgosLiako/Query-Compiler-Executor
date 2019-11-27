#Kostas Chasialis Makefile (project 1)

CFLAGS=-g3 -O2 -Wall -Wchkp -Wextra -D_FORTIFY_SOURCE=2 -DDEBUG $(OPTFLAGS)
LDFLAGS=$(OPTLIBS)
PREFIX?=/usr/local

SOURCES=$(wildcard src/**/*.c src/*.c)
HEADERS=$(wildcard src/**/*.h src/*.h)
OBJECTS=$(patsubst %.c,%.o,$(SOURCES))

TEST_SRC=$(wildcard tests/*_main.c)
TESTS=$(patsubst %.c,%,$(TEST_SRC))

MAIN_SRC=$(wildcard main/*_main.c)

TARGET=build/sort_merge.a
SO_TARGET=$(patsubst %.a,%.so,$(TARGET))



UNAME_S := $(shell uname -s)

ifeq ($(UNAME_S), Linux)
	LDLIBS=-L./build -lm
endif

# The Target Build
all: $(TARGET) $(SO_TARGET) main

dev: CFLAGS=-g -Wall -Isrc -Wall -Wextra $(OPTFLAGS)
dev: all

$(TARGET): CFLAGS += -fPIC
$(TARGET): build $(OBJECTS)
	ar rcs $@ $(OBJECTS)
	ranlib $@

$(SO_TARGET): $(TARGET) $(OBJECTS)
	$(CC) -shared -o queries $(OBJECTS)

build:
	@mkdir -p build
	@mkdir -p bin

# The Unit Tests
.PHONY: tests
main: $(TARGET)
	$(CC) $(CFLAGS) -o queries $(MAIN_SRC) $(TARGET) 


valgrind:
	VALGRIND="valgrind --log-file=/tmp/valgrind-%p.log" $(MAKE)

# The Cleaner
clean:
	rm -rf sort_merge
	rm -rf build $(OBJECTS) $(TESTS)
	rm -f tests/tests.log
	find . -name "*.gc*" -exec rm {} \;
	rm -rf `find . -name "*.dSYM" -print`

# The Install
install: all
	install -d $(DESTDIR)/$(PREFIX)/lib/
	install $(TARGET) $(DESTDIR)/$(PREFIX)/lib/

# The Checker
BADFUNCS='[^_.>a-zA-Z0-9](str(n?cpy|n?cat|xfrm|n?dup|str|pbrk|tok|_)\
|stpn?cpy|a?sn?printf|byte_)'

check:
	@echo Files with potentially dangerous function
	@egrep $(BADFUNCS) $(SOURCES) || true

count:
	wc src/*.c src/*.h main/*.c -l
