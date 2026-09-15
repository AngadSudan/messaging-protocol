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
        start_queue();
    }
    else if (strcmp(operation, "unregister") == 0)
    {
        kill_queue();
    }
    else
    {
        printf("Operations allowed are - register|unregister\n");
        exit(-1);
    }
}
