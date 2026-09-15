#include "ampq/queue.h"
#include "ampq/config.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void start_queue() {}
void kill_queue() {}

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
