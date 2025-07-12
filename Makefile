# ====== Variables ======
CC      := gcc
CFLAGS  := -Wall -Wextra -Wpedantic -std=c11 -g
SRC_DIR := main
BIN_DIR := bin
TARGET  := db/db

# ====== Sources and Objects ======
SRCS := $(wildcard $(SRC_DIR)/*.c)
OBJS := $(SRCS:$(SRC_DIR)/%.c=$(BIN_DIR)/%.o)
DEPS := $(OBJS:.o=.d)

# ====== Default rule ======
all: $(TARGET)

# ====== Linking ======
$(TARGET): $(OBJS)
	@mkdir -p $(BIN_DIR)
	$(CC) $(CFLAGS) -o $@ $^

# ====== Compilation with dependency generation ======
$(BIN_DIR)/%.o: $(SRC_DIR)/%.c
	@mkdir -p $(BIN_DIR)
	$(CC) $(CFLAGS) -MMD -c $< -o $@

# ====== Include dependencies ======
-include $(DEPS)

# ====== Clean ======
clean:
	rm -rf $(BIN_DIR)/*.o $(BIN_DIR)/*.d $(TARGET) db/mydb.db

# ====== Run the program ======
run: $(TARGET)
	$(TARGET) db/mydb.db

.PHONY: all clean run