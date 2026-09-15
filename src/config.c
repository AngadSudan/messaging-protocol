#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include "ampq/config.h"

#define QUEUE_PATH "./data/queue.conf"

void put_config(QueueConfig *config);
QueueConfig load_config(void);

QueueConfig initialize_config(int argc, char *argv[])
{
    int use_default = strcmp(argv[argc - 1], "-y");
    QueueConfig config = {0};

    if (use_default == 0)
    {
        config = config_default();
    }

    config = custom_config();
    put_config(&config);
    return config;
}

QueueConfig config_default(void)
{
    QueueConfig config = {
        .port = 9294,
        .max_consumers = 100,
        .max_producers = 100};

    return config;
}

QueueConfig custom_config(void)
{
    int port;
    int max_consumers;
    int max_producers;

    char input[32];

    printf("Port number (default - 9294): ");
    fgets(input, sizeof(input), stdin);

    if (input[0] == '\n')
        port = 9294;
    else
        port = atoi(input);

    printf("Max Producers (default - 100): ");
    fgets(input, sizeof(input), stdin);

    if (input[0] == '\n')
        max_producers = 100;
    else
        max_producers = atoi(input);

    printf("Max Consumers (default - 100): ");
    fgets(input, sizeof(input), stdin);

    if (input[0] == '\n')
        max_consumers = 100;
    else
        max_consumers = atoi(input);

    QueueConfig config = {
        .port = port,
        .max_consumers = max_consumers,
        .max_producers = max_producers};

    return config;
}

QueueConfig load_config(void)
{
    int fd;

    if ((fd = open(QUEUE_PATH, O_RDONLY)) < 0)
    {
        printf("Error reading config file\n");
        exit(-1);
    }

    QueueConfig config = {0};

    char line[128];
    int i = 0;
    char ch;

    while (read(fd, &ch, 1) == 1)
    {
        if (ch == '\n')
        {
            line[i] = '\0';

            char *key = strtok(line, "=");
            char *value = strtok(NULL, "=");

            if (key != NULL && value != NULL)
            {
                if (strcmp(key, "port") == 0)
                    config.port = atoi(value);

                else if (strcmp(key, "max_producers") == 0)
                    config.max_producers = atoi(value);

                else if (strcmp(key, "max_consumers") == 0)
                    config.max_consumers = atoi(value);
            }

            i = 0;
        }
        else
        {
            if (i < sizeof(line) - 1)
                line[i++] = ch;
        }
    }

    close(fd);
    return config;
}

void put_config(QueueConfig *config)
{
    int fd;

    if ((fd = open(QUEUE_PATH, O_WRONLY | O_CREAT | O_TRUNC, 0644)) < 0)
    {
        printf("Error writing config file\n");
        exit(-1);
    }

    char buffer[128];

    int len = sprintf(buffer, "port=%d\n", config->port);
    write(fd, buffer, len);

    len = sprintf(buffer, "max_producers=%d\n", config->max_producers);
    write(fd, buffer, len);

    len = sprintf(buffer, "max_consumers=%d\n", config->max_consumers);
    write(fd, buffer, len);

    close(fd);
}