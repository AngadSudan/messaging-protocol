#include "ampq/wal.h"
#include "ampq/message.h"
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>

#define WAL_PATH "./WAL.log"
#define MESSAGE_SIZE 4096

static int write_all(int fd, const char *buffer, size_t length)
{
    size_t written = 0;
    while (written < length)
    {
        ssize_t count = write(fd, buffer + written, length - written);
        if (count <= 0)
            return -1;
        written += (size_t)count;
    }
    return 0;
}

static int send_message(int client_fd, const char *message)
{
    char buffer[MESSAGE_SIZE];
    int length = snprintf(buffer, sizeof(buffer), "%s\n", message);
    if (length < 0 || length >= (int)sizeof(buffer))
        return -1;

    return write_all(client_fd, buffer, (size_t)length);
}

int append_wal(const char *message_content)
{
    Message msg = message_create(message_content);
    const char *serialized = message_serialize(&msg);
    if (serialized == NULL)
        return -1;

    int fd = open(WAL_PATH, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd < 0)
        return -1;

    size_t length = strlen(serialized);
    int result = write_all(fd, serialized, length) == 0 &&
                         write_all(fd, "\n", 1) == 0
                     ? 0
                     : -1;
    fsync(fd);
    close(fd);
    return result;
}

int replay_wal(int client_fd)
{
    int input_fd = open(WAL_PATH, O_RDONLY);
    if (input_fd < 0)
        return errno == ENOENT ? 0 : -1;

    char line[MESSAGE_SIZE + 32];
    size_t line_length = 0;
    char character;
    ssize_t bytes_read;
    int result = 0;

    while ((bytes_read = read(input_fd, &character, 1)) > 0)
    {
        if (line_length >= sizeof(line) - 1)
        {
            result = -1;
            break;
        }

        if (character == '\n')
        {
            line[line_length] = '\0';
            if (line_length > 0)
            {
                Message msg = message_deserialize(line);
                if (send_message(client_fd, msg.content) < 0)
                {
                    result = -1;
                    break;
                }
            }
            line_length = 0;
        }
        else
        {
            line[line_length++] = character;
        }
    }

    if (bytes_read < 0 || line_length != 0)
        result = -1;
    close(input_fd);

    if (result < 0)
        return -1;

    int truncate_fd = open(WAL_PATH, O_WRONLY | O_TRUNC);
    if (truncate_fd < 0)
        return -1;
    fsync(truncate_fd);
    close(truncate_fd);
    return 0;
}

int increment_wal_message_retention(const char *message_content, int max_retention)
{
    int input_fd = open(WAL_PATH, O_RDONLY);
    if (input_fd < 0)
        return -1;

    char temporary_path[sizeof(WAL_PATH) + 16];
    snprintf(temporary_path, sizeof(temporary_path), "%s.tmp", WAL_PATH);
    int output_fd = open(temporary_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (output_fd < 0)
    {
        close(input_fd);
        return -1;
    }

    char line[MESSAGE_SIZE + 32];
    size_t line_length = 0;
    int found = 0;
    char buffer[512];
    ssize_t bytes_read;
    int result = 0;

    while ((bytes_read = read(input_fd, buffer, sizeof(buffer))) > 0)
    {
        for (ssize_t i = 0; i < bytes_read; i++)
        {
            if (line_length >= sizeof(line) - 1)
            {
                result = -1;
                break;
            }

            line[line_length++] = buffer[i];
            if (buffer[i] != '\n')
                continue;

            line[line_length - 1] = '\0';
            Message msg = message_deserialize(line);

            if (!found && strcmp(msg.content, message_content) == 0)
            {
                found = 1;
                message_increment_retention(&msg);
            }

            if (!message_should_delete(&msg, max_retention))
            {
                const char *serialized = message_serialize(&msg);
                if (serialized == NULL || write_all(output_fd, serialized, strlen(serialized)) < 0 ||
                    write_all(output_fd, "\n", 1) < 0)
                    result = -1;
            }

            line_length = 0;

            if (result < 0)
                break;
        }
        if (result < 0)
            break;
    }

    if (bytes_read < 0 || line_length != 0)
        result = -1;

    close(input_fd);
    if (close(output_fd) != 0 || result < 0 || rename(temporary_path, WAL_PATH) != 0)
    {
        unlink(temporary_path);
        return -1;
    }
    return found ? 0 : -1;
}

int remove_wal_message(const char *message_content, int max_retention)
{
    return increment_wal_message_retention(message_content, max_retention);
}
