#include "ampq/message.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>

void message_set_timestamp(Message *msg)
{
    time_t now = time(NULL);
    struct tm *tm_info = gmtime(&now);
    strftime(msg->timestamp, sizeof(msg->timestamp), "%Y-%m-%dT%H:%M:%SZ", tm_info);
}

Message message_create(const char *content)
{
    Message msg = {0};
    strncpy(msg.content, content, sizeof(msg.content) - 1);
    msg.content[sizeof(msg.content) - 1] = '\0';
    msg.retention_count = 0;
    message_set_timestamp(&msg);
    return msg;
}

char *message_serialize(const Message *msg)
{
    static char buffer[4096 + 64];
    int len = snprintf(buffer, sizeof(buffer), "%s|%s|%d",
                       msg->timestamp, msg->content, msg->retention_count);
    if (len < 0 || len >= (int)sizeof(buffer))
        return NULL;
    return buffer;
}

Message message_deserialize(const char *line)
{
    Message msg = {0};
    char temp[4096 + 64];
    strncpy(temp, line, sizeof(temp) - 1);
    temp[sizeof(temp) - 1] = '\0';

    char *last_pipe = strrchr(temp, '|');
    if (last_pipe == NULL)
    {
        strncpy(msg.content, temp, sizeof(msg.content) - 1);
        msg.content[sizeof(msg.content) - 1] = '\0';
        strcpy(msg.timestamp, "1970-01-01T00:00:00Z");
        msg.retention_count = 0;
        return msg;
    }

    msg.retention_count = atoi(last_pipe + 1);
    *last_pipe = '\0';

    char *second_pipe = strrchr(temp, '|');
    if (second_pipe == NULL)
    {
        strncpy(msg.content, temp, sizeof(msg.content) - 1);
        msg.content[sizeof(msg.content) - 1] = '\0';
        strcpy(msg.timestamp, "1970-01-01T00:00:00Z");
        return msg;
    }

    strncpy(msg.content, second_pipe + 1, sizeof(msg.content) - 1);
    msg.content[sizeof(msg.content) - 1] = '\0';
    *second_pipe = '\0';

    strncpy(msg.timestamp, temp, sizeof(msg.timestamp) - 1);
    msg.timestamp[sizeof(msg.timestamp) - 1] = '\0';

    return msg;
}

int message_increment_retention(Message *msg)
{
    msg->retention_count++;
    return msg->retention_count;
}

int message_should_delete(const Message *msg, int max_retention)
{
    return msg->retention_count >= max_retention;
}
