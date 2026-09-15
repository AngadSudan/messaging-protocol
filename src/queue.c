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
typedef struct
{
    int fd;
    QueueConfig config;
    int producer_count;
    int consumer_count;
} Queue;

Queue q = {0};

int start_queue(void);
void kill_queue(void);

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

        printf("Client connected: fd=%d\n", client_fd);

        close(client_fd);
    }

    return 0;
}

void kill_queue(void)
{
    close(q.fd);
    q.fd = -1;
}