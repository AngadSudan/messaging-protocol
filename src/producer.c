#include "ampq/producer.h"
#include "ampq/queue.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void consumer_producer_operation(int argc, char *argv[])
{
    if (argc < 3)
    {
        printf("Operations allowed are - register|unregister\n");
        exit(-1);
    }

    char *operation = argv[2];

    if (strcmp(operation, "register") == 0)
    {
        if (argc < 4)
        {
            fprintf(stderr, "Usage: %s producer register <message>\n", argv[0]);
            exit(EXIT_FAILURE);
        }

        if (publish_message(argv[3]) < 0)
            exit(EXIT_FAILURE);
    }
    else
    {
        printf("Operations allowed are - register\n");
        exit(-1);
    }
}
