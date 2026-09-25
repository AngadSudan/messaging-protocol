#include "ampq/consumer.h"
#include "ampq/queue.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

void consumer_consumer_operation(int argc, char *argv[])
{
    if (argc < 3)
    {
        printf("Operations allowed are - register|unregister\n");
        exit(-1);
    }

    char *operation = argv[2];

    if (strcmp(operation, "register") == 0)
    {
        int fd = connect_queue();
        if (fd < 0)
            exit(EXIT_FAILURE);

        char message[4096];
        ssize_t received;
        while ((received = read(fd, message, sizeof(message) - 1)) > 0)
        {
            message[received] = '\0';
            fputs(message, stdout);
            fflush(stdout);
        }
        close(fd);
    }
    else
    {
        printf("Operations allowed are - register\n");
        exit(-1);
    }
}
