#include "ampq/display.h"
#include <stdio.h>
#include <time.h>

#define BOLD "\x1b[1m"
#define GREEN "\x1b[32m"
#define CYAN "\x1b[36m"
#define YELLOW "\x1b[33m"
#define RESET "\x1b[0m"
#define CLEAR "\x1b[2J\x1b[H"

static const char *get_timestamp(void)
{
    static char timestamp[32];
    time_t now = time(NULL);
    struct tm *tm_info = localtime(&now);
    strftime(timestamp, sizeof(timestamp), "%H:%M:%S", tm_info);
    return timestamp;
}

void display_server_header(void)
{
    printf("%s%s", CLEAR, RESET);
    printf("%s╔════════════════════════════════════════════════════╗%s\n", CYAN, RESET);
    printf("%s║        AMQP Queue Server - Dashboard               ║%s\n", BOLD CYAN, RESET);
    printf("%s╚════════════════════════════════════════════════════╝%s\n", CYAN, RESET);
    printf("\n");
}

void display_consumer_status(int consumer_count, int max_consumers)
{
    printf("%s┌─ Consumer Status ──────────────────────────────────┐%s\n", CYAN, RESET);
    printf("%s│%s Connected: %s%d/%d%s %*s\n",
           CYAN, RESET,
           GREEN, consumer_count, max_consumers, RESET,
           (int)(36 - snprintf(NULL, 0, "Connected: %d/%d", consumer_count, max_consumers)), "");
    printf("%s│%s Capacity:  %s%.1f%%%s %*s\n",
           CYAN, RESET,
           (consumer_count * 100 / max_consumers > 80) ? YELLOW : GREEN,
           (float)(consumer_count * 100) / max_consumers, RESET,
           33, "");
    printf("%s│%s Available: %s%d%s %*s\n",
           CYAN, RESET,
           GREEN, max_consumers - consumer_count, RESET,
           36, "");
    printf("%s└────────────────────────────────────────────────────┘%s\n", CYAN, RESET);
}

void display_connection_event(const char *event_type, int consumer_count, int max_consumers)
{
    const char *symbol = (event_type[0] == 'C') ? "+" : "-";
    const char *color = (event_type[0] == 'C') ? GREEN : YELLOW;

    printf("%s[%s]%s %s%s%s │ Active: %d/%d\n",
           CYAN, get_timestamp(), RESET,
           color, symbol, RESET,
           consumer_count, max_consumers);
}

void display_message_event(const char *event_type, const char *message)
{
    printf("%s[%s]%s %s●%s │ %s: %s\n",
           CYAN, get_timestamp(), RESET,
           BOLD GREEN, RESET,
           event_type, message);
}
