#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ampq/consumer.h>
#include <ampq/producer.h>
#include <ampq/queue.h>

int main(int argc, char *argv[])
{
    if (argc < 2)
    {
        printf("command usage is: %s [entity]{producer|consumer|queue} [operation]{start|enable|disable}\n", argv[0]);
        exit(-1);
    }

    char *entity = argv[1];

    if (strcmp(entity, "producer") == 0)
    {
        consumer_producer_operation(argc, argv);
    }
    else if (strcmp(entity, "consumer") == 0)
    {
        consumer_consumer_operation(argc, argv);
    }
    else if (strcmp(entity, "queue") == 0)
    {
        consumer_queue_operation(argc, argv);
    }
    else
    {
        printf("valid entities are - producer,consumer,entity\n");
        exit(-1);
    }

    return 0;
}