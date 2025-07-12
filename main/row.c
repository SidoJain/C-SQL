#include "row.h"

void serialize_user_row(UserRow* source, void* destination) {
    memcpy((char*)destination + ID_FIELD_OFFSET, &(source->id), ID_FIELD_SIZE);
    memcpy((char*)destination + USERNAME_FIELD_OFFSET, &(source->username), USERNAME_FIELD_SIZE);
    memcpy((char*)destination + EMAIL_FIELD_OFFSET, &(source->email), EMAIL_FIELD_SIZE);
}

void deserialize_user_row(void* source, UserRow* destination) {
    memcpy(&(destination->id), (char*)source + ID_FIELD_OFFSET, ID_FIELD_SIZE);
    memcpy(&(destination->username), (char*)source + USERNAME_FIELD_OFFSET, USERNAME_FIELD_SIZE);
    memcpy(&(destination->email), (char*)source + EMAIL_FIELD_OFFSET, EMAIL_FIELD_SIZE);
}

void print_user_row(UserRow* user) {
    printf("(%d, %s, %s)\n", user->id, user->username, user->email);
}