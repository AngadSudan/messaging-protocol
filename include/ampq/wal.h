#ifndef AMQP_WAL_H
#define AMQP_WAL_H

int append_wal(const char *message);
int remove_wal_message(const char *message);
int replay_wal(int client_fd);

#endif
