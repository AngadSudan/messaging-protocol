#include "ampq/logger.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <time.h>
#include <pthread.h>
#include <unistd.h>

#define LOG_BUFFER_SIZE 65536

typedef struct {
    char *log_file;
    int logging_interval_ms;
    char buffer[LOG_BUFFER_SIZE];
    size_t buffer_pos;
    pthread_mutex_t buffer_lock;
    int running;
    pthread_t flush_thread;
} Logger;

static Logger logger = {0};

static const char *log_level_str(LogLevel level)
{
    switch (level) {
        case LOG_INFO:    return "INFO";
        case LOG_EVENT:   return "EVENT";
        case LOG_ERROR:   return "ERROR";
        case LOG_MESSAGE: return "MESSAGE";
        default:          return "UNKNOWN";
    }
}

static char *get_timestamp(void)
{
    static char timestamp[32];
    time_t now = time(NULL);
    struct tm *tm_info = localtime(&now);
    strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", tm_info);
    return timestamp;
}

static void *flush_thread_func(void *arg)
{
    (void)arg;
    while (logger.running) {
        usleep(logger.logging_interval_ms * 1000);
        logger_flush();
    }
    return NULL;
}

void logger_init(const char *log_file, int logging_interval_ms)
{
    if (logger.log_file != NULL)
        return;

    logger.log_file = malloc(strlen(log_file) + 1);
    strcpy(logger.log_file, log_file);
    logger.logging_interval_ms = logging_interval_ms;
    logger.buffer_pos = 0;
    logger.running = 1;
    pthread_mutex_init(&logger.buffer_lock, NULL);

    pthread_create(&logger.flush_thread, NULL, flush_thread_func, NULL);
    pthread_detach(logger.flush_thread);

    logger_log(LOG_INFO, "SERVER", "Logger initialized - file: %s, interval: %dms",
               log_file, logging_interval_ms);
}

void logger_log(LogLevel level, const char *category, const char *format, ...)
{
    if (logger.log_file == NULL)
        return;

    char message[2048];
    va_list args;
    va_start(args, format);
    vsnprintf(message, sizeof(message), format, args);
    va_end(args);

    char log_entry[2048];
    int len = snprintf(log_entry, sizeof(log_entry), "[%s] %s | %s | %s\n",
                       get_timestamp(), log_level_str(level), category, message);

    pthread_mutex_lock(&logger.buffer_lock);

    if (logger.buffer_pos + len >= LOG_BUFFER_SIZE) {
        logger_flush_unlocked();
    }

    memcpy(logger.buffer + logger.buffer_pos, log_entry, len);
    logger.buffer_pos += len;

    pthread_mutex_unlock(&logger.buffer_lock);
}

void logger_flush_unlocked(void)
{
    if (logger.buffer_pos == 0)
        return;

    FILE *f = fopen(logger.log_file, "a");
    if (f == NULL)
        return;

    fwrite(logger.buffer, 1, logger.buffer_pos, f);
    fclose(f);

    logger.buffer_pos = 0;
}

void logger_flush(void)
{
    pthread_mutex_lock(&logger.buffer_lock);
    logger_flush_unlocked();
    pthread_mutex_unlock(&logger.buffer_lock);
}

void logger_cleanup(void)
{
    if (logger.log_file == NULL)
        return;

    logger.running = 0;
    sleep(1);

    logger_flush();

    pthread_mutex_destroy(&logger.buffer_lock);
    free(logger.log_file);
    memset(&logger, 0, sizeof(logger));
}
