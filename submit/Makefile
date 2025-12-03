CC      = gcc
CFLAGS  = -std=c11 -Wall -Wextra -pedantic -g
LDFLAGS = -pthread -lrt
TARGET  = procx
SRCS    = procx.c

.PHONY: all clean run

all: $(TARGET)

$(TARGET): $(SRCS)
	@echo "[CC] Compiling $(SRCS)..."
	@$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)
	@echo "[LINK] Creating $(TARGET) ..."
	@echo ""
	@echo "Build successful!"
	@echo "Run with: ./$(TARGET)"

run: $(TARGET)
	./$(TARGET)

clean:
	rm -f $(TARGET)
