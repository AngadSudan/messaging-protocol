#include "ampq/fileutil.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

int ensure_directory_exists(const char *path)
{
    struct stat st;

    if (stat(path, &st) == 0 && S_ISDIR(st.st_mode))
        return 0;

    if (mkdir(path, 0755) < 0)
    {
        if (errno != EEXIST)
        {
            perror("mkdir");
            return -1;
        }
    }

    return 0;
}

int ensure_parent_directory_exists(const char *path)
{
    char temp[512];
    char *last_slash;

    if (strlen(path) >= sizeof(temp))
        return -1;

    strcpy(temp, path);
    last_slash = strrchr(temp, '/');

    if (last_slash == NULL)
        return 0;

    *last_slash = '\0';

    if (temp[0] == '\0')
        return 0;

    return ensure_directory_exists(temp);
}

int ensure_file_exists(const char *path)
{
    int fd;
    struct stat st;

    if (stat(path, &st) == 0)
        return 0;

    if (ensure_parent_directory_exists(path) < 0)
        return -1;

    fd = open(path, O_WRONLY | O_CREAT, 0644);
    if (fd < 0)
    {
        perror("open");
        return -1;
    }

    close(fd);
    return 0;
}
