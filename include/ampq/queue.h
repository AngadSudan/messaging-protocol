#ifndef AMQP_QUEUE_H
#define AMQP_QUEUE_H

int start_queue(void);
void kill_queue(void);
void consumer_queue_operation(int argc, char *argv[]);
int connect_queue(void);
int publish_message(const char *message);

#endif
