#include "ampq/queue.h"
#include "ampq/config.h"
#include <stdio.h>
#include <errno.h>
#include <netdb.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <pthread.h>
#include <fcntl.h>
#include <signal.h>

#define WAL_PATH "./WAL.log"
#define MESSAGE_SIZE 4096

typedef struct
{
    int fd;
    QueueConfig config;
    int producer_count;
    int consumer_count;
    int consumers[100];
} Queue;

Queue q = {.fd = -1};

int start_queue(void);
void kill_queue(void);
static void *handle_client(void *argument);
static int append_wal(const char *message);
static int remove_wal_message(const char *message);
static int replay_wal(int client_fd);
static int broadcast_message(const char *message);
static int send_message(int client_fd, const char *message);
static int write_all(int fd, const char *buffer, size_t length);

static int connect_to_queue(const char *role)
{
    QueueConfig config = load_config();
    int fd = socket(AF_INET, SOCK_STREAM, 0);

    if (fd < 0)
    {
        perror("socket");
        return -1;
    }

    struct sockaddr_in address = {
        .sin_family = AF_INET,
        .sin_addr.s_addr = htonl(INADDR_LOOPBACK),
        .sin_port = htons(config.port)};

    if (connect(fd, (struct sockaddr *)&address, sizeof(address)) < 0)
    {
        perror("connect");
        close(fd);
        return -1;
    }

    if (dprintf(fd, "%s\n", role) < 0)
    {
        close(fd);
        return -1;
    }

    return fd;
}

int connect_queue(void)
{
    return connect_to_queue("CONSUMER");
}

int publish_message(const char *message)
{
    int fd = connect_to_queue("PRODUCER");
    if (fd < 0)
        return -1;

    int result = dprintf(fd, "%s\n", message) < 0 ? -1 : 0;
    shutdown(fd, SHUT_WR);
    close(fd);
    return result;
}

void consumer_queue_operation(int argc, char *argv[])
{
    if (argc < 3)
    {
        printf("Operations allowed are -  configure|start|stop\n");
        exit(-1);
    }

    char *operation = argv[2];

    if (strcmp(operation, "configure") == 0)
    {
        initialize_config(argc, argv);
    }
    else if (strcmp(operation, "start") == 0)
    {
        start_queue();
    }
    else if (strcmp(operation, "stop") == 0)
    {
        kill_queue();
    }
    else
    {
        printf("Operations allowed are - configure|start|stop\n");
        exit(-1);
    }
}

int start_queue(void)
{
    q.config = load_config();

    if (q.config.max_consumers > (int)(sizeof(q.consumers) / sizeof(q.consumers[0])))
        q.config.max_consumers = sizeof(q.consumers) / sizeof(q.consumers[0]);

    signal(SIGPIPE, SIG_IGN);

    q.fd = socket(AF_INET, SOCK_STREAM, 0);

    if (q.fd < 0)
    {
        perror("socket");
        return -1;
    }

    int opt = 1;

    if (setsockopt(q.fd, SOL_SOCKET, SO_REUSEADDR,
                   &opt, sizeof(opt)) < 0)
    {
        perror("setsockopt");
        close(q.fd);
        return -1;
    }

    struct sockaddr_in address = {
        .sin_family = AF_INET,
        .sin_addr.s_addr = htonl(INADDR_ANY),
        .sin_port = htons(q.config.port)};

    if (bind(q.fd, (struct sockaddr *)&address,
             sizeof(address)) < 0)
    {
        perror("bind");
        close(q.fd);
        return -1;
    }

    if (listen(q.fd, q.config.max_consumers) < 0)
    {
        perror("listen");
        close(q.fd);
        return -1;
    }

    while (1)
    {
        struct sockaddr_in client_address;
        socklen_t client_len = sizeof(client_address);

        int client_fd = accept(
            q.fd,
            (struct sockaddr *)&client_address,
            &client_len);

        if (client_fd < 0)
        {
            perror("accept");
            continue;
        }

        pthread_t thread;
        int *client = malloc(sizeof(*client));
        if (client == NULL)
        {
            close(client_fd);
            continue;
        }

        *client = client_fd;
        if (pthread_create(&thread, NULL, handle_client, client) != 0)
        {
            free(client);
            close(client_fd);
            continue;
        }
        pthread_detach(thread);
    }

    return 0;
}

void kill_queue(void)
{
    if (q.fd >= 0)
        close(q.fd);
    q.fd = -1;
}

static void *handle_client(void *argument)
{
    int client_fd = *(int *)argument;
    free(argument);

    char buffer[MESSAGE_SIZE];
    ssize_t received = 0;
    while (received < (ssize_t)sizeof(buffer) - 1)
    {
        ssize_t count = recv(client_fd, buffer + received, 1, 0);
        if (count <= 0)
        {
            close(client_fd);
            return NULL;
        }
        if (buffer[received++] == '\n')
            break;
    }
    buffer[received] = '\0';

    if (strcmp(buffer, "CONSUMER\n") == 0)
    {
        if (q.consumer_count >= q.config.max_consumers)
        {
            close(client_fd);
            return NULL;
        }

        if (replay_wal(client_fd) < 0)
        {
            close(client_fd);
            return NULL;
        }

        q.consumers[q.consumer_count++] = client_fd;

        while (recv(client_fd, buffer, sizeof(buffer), 0) > 0)
        {
        }

        for (int i = 0; i < q.consumer_count; i++)
        {
            if (q.consumers[i] == client_fd)
            {
                q.consumers[i] = q.consumers[--q.consumer_count];
                break;
            }
        }
        close(client_fd);
        return NULL;
    }

    if (strcmp(buffer, "PRODUCER\n") == 0)
    {
        while (1)
        {
            ssize_t count = recv(client_fd, buffer, sizeof(buffer) - 1, 0);
            if (count <= 0)
                break;
            buffer[count] = '\0';
            char *line = buffer;
            char *newline;
            while ((newline = strchr(line, '\n')) != NULL)
            {
                *newline = '\0';
                if (*line != '\0' && append_wal(line) == 0)
                {
                    if (broadcast_message(line) > 0)
                        remove_wal_message(line);
                }
                line = newline + 1;
            }
        }
    }

    close(client_fd);
    return NULL;
}

static int append_wal(const char *message)
{
    int fd = open(WAL_PATH, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd < 0)
        return -1;

    size_t length = strlen(message);
    int result = write_all(fd, message, length) == 0 &&
                         write_all(fd, "\n", 1) == 0
                     ? 0
                     : -1;
    fsync(fd);
    close(fd);
    return result;
}

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

static int broadcast_message(const char *message)
{
    int delivered = 0;

    for (int i = 0; i < q.consumer_count;)
    {
        if (send_message(q.consumers[i], message) < 0)
        {
            close(q.consumers[i]);
            q.consumers[i] = q.consumers[--q.consumer_count];
            continue;
        }
        delivered++;
        i++;
    }
    return delivered;
}

static int replay_wal(int client_fd)
{
    int input_fd = open(WAL_PATH, O_RDONLY);
    if (input_fd < 0)
        return errno == ENOENT ? 0 : -1;

    char line[MESSAGE_SIZE];
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
            if (line_length > 0 && send_message(client_fd, line) < 0)
            {
                result = -1;
                break;
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

static int remove_wal_message(const char *message)
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

    char line[MESSAGE_SIZE];
    size_t line_length = 0;
    int removed = 0;
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
            if (!removed && strcmp(line, message) == 0)
                removed = 1;
            else if (write_all(output_fd, line, line_length - 1) < 0 ||
                     write_all(output_fd, "\n", 1) < 0)
                result = -1;
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
    return removed ? 0 : -1;
}