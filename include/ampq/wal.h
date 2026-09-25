#ifndef AMQP_WAL_H
#define AMQP_WAL_H

int append_wal(const char *message);
int replay_wal(int client_fd);
int increment_wal_message_retention(const char *message_content, int max_retention);
int remove_wal_message(const char *message_content, int max_retention);

#endif
