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
    int use_default = (argc > 1 && strcmp(argv[argc - 1], "-y") == 0);
    QueueConfig config = {0};

    if (use_default)
    {
        config = config_default();
        put_config(&config);
        return config;
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
        .max_producers = 100,
        .message_retention = 1,
        .logging_interval = 100};

    strcpy(config.log_file, "./queue.log");
    strcpy(config.server_address, "localhost");
    return config;
}

QueueConfig custom_config(void)
{
    int port;
    int max_consumers;
    int max_producers;
    int message_retention;
    int logging_interval;
    char log_file[256];
    char server_address[256];

    char input[256];

    printf("Server address (default - localhost): ");
    fgets(input, sizeof(input), stdin);

    if (input[0] == '\n')
        strcpy(server_address, "localhost");
    else {
        input[strcspn(input, "\n")] = '\0';
        strcpy(server_address, input);
    }

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

    printf("Max Message retention (default - 1): ");
    fgets(input, sizeof(input), stdin);

    if (input[0] == '\n')
        message_retention = 1;
    else
        message_retention = atoi(input);

    printf("Logging interval in ms (default - 100): ");
    fgets(input, sizeof(input), stdin);

    if (input[0] == '\n')
        logging_interval = 100;
    else
        logging_interval = atoi(input);

    printf("Log file path (default - ./queue.log): ");
    fgets(input, sizeof(input), stdin);

    if (input[0] == '\n')
        strcpy(log_file, "./queue.log");
    else {
        input[strcspn(input, "\n")] = '\0';
        strcpy(log_file, input);
    }

    QueueConfig config = {
        .port = port,
        .max_consumers = max_consumers,
        .message_retention = message_retention,
        .max_producers = max_producers,
        .logging_interval = logging_interval};

    strcpy(config.log_file, log_file);
    strcpy(config.server_address, server_address);
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

    char line[128] = {0};
    size_t i = 0;
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

                else if (strcmp(key, "message_retention") == 0)
                    config.message_retention = atoi(value);

                else if (strcmp(key, "logging_interval") == 0)
                    config.logging_interval = atoi(value);

                else if (strcmp(key, "log_file") == 0)
                    strncpy(config.log_file, value, sizeof(config.log_file) - 1);

                else if (strcmp(key, "server_address") == 0)
                    strncpy(config.server_address, value, sizeof(config.server_address) - 1);
            }

            i = 0;
        }
        else
        {
            if (i < sizeof(line) - 1U)
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

    char buffer[512];

    int len = sprintf(buffer, "server_address=%s\n", config->server_address);
    write(fd, buffer, len);

    len = sprintf(buffer, "port=%d\n", config->port);
    write(fd, buffer, len);

    len = sprintf(buffer, "max_producers=%d\n", config->max_producers);
    write(fd, buffer, len);

    len = sprintf(buffer, "max_consumers=%d\n", config->max_consumers);
    write(fd, buffer, len);

    len = sprintf(buffer, "message_retention=%d\n", config->message_retention);
    write(fd, buffer, len);

    len = sprintf(buffer, "logging_interval=%d\n", config->logging_interval);
    write(fd, buffer, len);

    len = sprintf(buffer, "log_file=%s\n", config->log_file);
    write(fd, buffer, len);

    close(fd);
}