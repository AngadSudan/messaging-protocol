#ifndef AMQP_CONFIG_H
#define AMQP_CONFIG_H

typedef struct
{
    int port;
    int max_producers;
    int max_consumers;
} QueueConfig;

QueueConfig initialize_config(int argc, char *argv[]);
QueueConfig config_default(void);
QueueConfig custom_config(void);
QueueConfig load_config(void);
#endif
