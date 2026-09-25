#include "ampq/server.h"
#include "ampq/config.h"
#include "ampq/wal.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>

#define MESSAGE_SIZE 4096

typedef struct
{
    int fd;
    QueueConfig config;
    int producer_count;
    int consumer_count;
    int consumers[100];
} Queue;

static Queue q = {.fd = -1};

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
