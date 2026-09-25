#include "ampq/queue.h"
#include "ampq/config.h"
#include "ampq/server.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>

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
