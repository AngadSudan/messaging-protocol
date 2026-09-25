#ifndef AMQP_LOGGER_H
#define AMQP_LOGGER_H

#include <time.h>

typedef enum {
    LOG_INFO,
    LOG_EVENT,
    LOG_ERROR,
    LOG_MESSAGE
} LogLevel;

void logger_init(const char *log_file, int logging_interval_ms);
void logger_log(LogLevel level, const char *category, const char *format, ...);
void logger_flush(void);
void logger_flush_unlocked(void);
void logger_cleanup(void);

#endif
