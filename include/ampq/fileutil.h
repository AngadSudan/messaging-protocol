#ifndef AMQP_FILEUTIL_H
#define AMQP_FILEUTIL_H

int ensure_directory_exists(const char *path);
int ensure_file_exists(const char *path);
int ensure_parent_directory_exists(const char *path);

#endif
