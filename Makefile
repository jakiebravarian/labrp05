# Define compiler
CC=gcc

# Define compiler flags
CFLAGS=-Wall

# Target executable
TARGET = a.out

# Source file
SRC = brabante_t6l_labrp05.c

# Default target to build the executable
all: $(TARGET)

# Rule to build the executable
$(TARGET): $(SRC)
	$(CC) $(CFLAGS) -o $(TARGET) $(SRC) -lpthread -lm

# Clean up the generated files
clean:
	rm -f $(TARGET) $(OUTPUT)

.PHONY: all run clean