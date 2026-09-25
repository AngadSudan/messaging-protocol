CC = gcc
CFLAGS = -Wall -Wextra -Iinclude -pthread

TARGET = build/protocol

SRC = src/main.c \
      src/consumer.c \
      src/config.c \
      src/producer.c \
      src/queue.c \
      src/wal.c \
      src/server.c \
      src/message.c

$(TARGET): $(SRC)
	mkdir -p build
	$(CC) $(CFLAGS) $(SRC) -o $(TARGET)

clean:
	rm -rf build