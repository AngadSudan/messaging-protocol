#ifndef AMQP_MESSAGE_H
#define AMQP_MESSAGE_H

#include <time.h>

typedef struct
{
    char content[4096];
    char timestamp[32];
    int retention_count;
} Message;

Message message_create(const char *content);
char *message_serialize(const Message *msg);
Message message_deserialize(const char *line);
int message_increment_retention(Message *msg);
int message_should_delete(const Message *msg, int max_retention);
void message_set_timestamp(Message *msg);

#endif

