#ifndef AMQP_DISPLAY_H
#define AMQP_DISPLAY_H

void display_server_header(void);
void display_consumer_status(int consumer_count, int max_consumers);
void display_connection_event(const char *event_type, int consumer_count, int max_consumers);
void display_message_event(const char *event_type, const char *message);

#endif
