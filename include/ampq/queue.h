#ifndef AMQP_QUEUE_H
#define AMQP_QUEUE_H

void start_queue(void);
void kill_queue(void);
void consumer_queue_operation(int argc, char *argv[]);

#endif
