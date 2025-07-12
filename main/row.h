#ifndef DB_ROW_H
#define DB_ROW_H

#include "common.h"

#define ID_FIELD_SIZE       size_of_attribute(UserRow, id)
#define USERNAME_FIELD_SIZE size_of_attribute(UserRow, username)
#define EMAIL_FIELD_SIZE    size_of_attribute(UserRow, email)

typedef struct {
    uint32_t id;
    char     username[USERNAME_MAX_LENGTH + 1];
    char     email[EMAIL_MAX_LENGTH + 1];
} UserRow;

typedef struct {
    uint32_t id;
    char     field_to_update[USERNAME_MAX_LENGTH];
    char     new_value[EMAIL_MAX_LENGTH + 1];
} UpdatePayload;

void serialize_user_row(UserRow* source, void* destination);
void deserialize_user_row(void* source, UserRow* destination);
void print_user_row(UserRow* user);

#endif