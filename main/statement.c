#include "statement.h"

PrepareResult prepare_statement(InputBuffer* input_buffer, Statement* statement) {
    if (strncmp(input_buffer->buffer, "insert", 6) == 0)
        return prepare_insert(input_buffer, statement);
    
    if (strncmp(input_buffer->buffer, "select", 6) == 0) {
        int id;
        int args_assigned = sscanf(input_buffer->buffer, "select %d", &id);

        if (args_assigned == 1) {
            if (id < 0)
                return PREPARE_NEGATIVE_ID;
            statement->type = STATEMENT_SPECIFIC_SELECT;
            statement->payload.user_to_insert.id = id;

            char extra[2];
            if (sscanf(input_buffer->buffer, "select %d %1s", &id, extra) == 2)
                return PREPARE_SYNTAX_ERROR;

            return PREPARE_SUCCESS;
        }
        else {
            char extra[2];
            if (sscanf(input_buffer->buffer, "select %1s", extra) == 1)
                return PREPARE_SYNTAX_ERROR;

            statement->type = STATEMENT_SELECT;
            return PREPARE_SUCCESS;
        }
    }

    if (strncmp(input_buffer->buffer, "drop", 4) == 0)
        return prepare_drop(input_buffer, statement);

    if (strncmp(input_buffer->buffer, "update", 6) == 0)
        return prepare_update(input_buffer, statement);

    if (strncmp(input_buffer->buffer, "import", 6) == 0)
        return prepare_import(input_buffer, statement);

    if (strncmp(input_buffer->buffer, "export", 6) == 0)
        return prepare_export(input_buffer, statement);

    return PREPARE_UNRECOGNIZED_STATEMENT;
}

PrepareResult prepare_insert(InputBuffer* input_buffer, Statement* statement) {
    statement->type = STATEMENT_INSERT;

    char username[USERNAME_MAX_LENGTH + 2];
    char email[EMAIL_MAX_LENGTH + 2];
    int id;

    int chars_consumed = 0;
    int args_assigned = sscanf(input_buffer->buffer, "insert %d %40s %260s %n", &id, username, email, &chars_consumed);
    if (args_assigned != 3)
        return PREPARE_SYNTAX_ERROR;

    if (id < 0)
        return PREPARE_NEGATIVE_ID;
    if (strlen(username) > USERNAME_MAX_LENGTH)
        return PREPARE_STRING_TOO_LONG;
    if (strlen(email) > EMAIL_MAX_LENGTH )
            return PREPARE_STRING_TOO_LONG;

    statement->payload.user_to_insert.id = id;
    strcpy(statement->payload.user_to_insert.username, username);
    strcpy(statement->payload.user_to_insert.email, email);
    return PREPARE_SUCCESS;
}

PrepareResult prepare_drop(InputBuffer* input_buffer, Statement* statement) {
    statement->type = STATEMENT_DROP;
    int id;
    int args_assigned = sscanf(input_buffer->buffer, "drop %d", &id);
    if (args_assigned != 1)
        return PREPARE_SYNTAX_ERROR;
    if (id < 0)
        return PREPARE_NEGATIVE_ID;

    statement->payload.user_to_insert.id = id;
    return PREPARE_SUCCESS;
}

PrepareResult prepare_import(InputBuffer* input_buffer, Statement* statement) {
    statement->type = STATEMENT_IMPORT;
    char filename[FILENAME_MAX_LENGTH + 2];
    int args_assigned = sscanf(input_buffer->buffer, "import '%255[^']'", filename);

    if (args_assigned != 1) {
        args_assigned = sscanf(input_buffer->buffer, "import %s", filename);
        if (args_assigned == 1)
            printf(ANSI_COLOR_RED "Syntax Error: Filename must be enclosed in single quotes (e.g., import 'file.csv').\n" ANSI_COLOR_RESET);
        return PREPARE_SYNTAX_ERROR;
    }

    if (strlen(filename) > FILENAME_MAX_LENGTH)
        return PREPARE_STRING_TOO_LONG;

    strcpy(statement->payload.filename, filename);
    return PREPARE_SUCCESS;
}

PrepareResult prepare_export(InputBuffer* input_buffer, Statement* statement) {
    statement->type = STATEMENT_EXPORT;
    char filename[FILENAME_MAX_LENGTH + 2];
    int args_assigned = sscanf(input_buffer->buffer, "export '%255[^']'", filename);
    
    if (args_assigned != 1) {
        args_assigned = sscanf(input_buffer->buffer, "export %s", filename);
        if (args_assigned == 1)
            printf(ANSI_COLOR_RED "Syntax Error: Filename must be enclosed in single quotes (e.g., export 'file.csv').\n" ANSI_COLOR_RESET);
        return PREPARE_SYNTAX_ERROR;
    }

    if (strlen(filename) > FILENAME_MAX_LENGTH)
        return PREPARE_STRING_TOO_LONG;

    strcpy(statement->payload.filename, filename);
    return PREPARE_SUCCESS;
}

PrepareResult prepare_update(InputBuffer* input_buffer, Statement* statement) {
    statement->type = STATEMENT_UPDATE;

    int id;
    char field[USERNAME_MAX_LENGTH + 2];
    char value[EMAIL_MAX_LENGTH + 2];
    int args_assigned = sscanf(input_buffer->buffer, "update %d set %32[a-zA-Z]=%256[^ \n]", &id, field, value);

    if (args_assigned != 3)
        return PREPARE_SYNTAX_ERROR;
    if (id < 0)
        return PREPARE_NEGATIVE_ID;

    if (strcmp(field, "username") != 0 && strcmp(field, "email") != 0) {
        printf(ANSI_COLOR_RED "Unrecognized field '%s' for update.\n" ANSI_COLOR_RESET, field);
        printf(ANSI_COLOR_RED "Only fields 'username' & 'email' can be updated\n" ANSI_COLOR_RESET);
        return PREPARE_SYNTAX_ERROR;
    }

    if (strcmp(field, "username") == 0 && strlen(value) > USERNAME_MAX_LENGTH)
        return PREPARE_STRING_TOO_LONG;
    if (strcmp(field, "email") == 0 && strlen(value) > EMAIL_MAX_LENGTH)
        return PREPARE_STRING_TOO_LONG;

    statement->payload.update_payload.id = id;
    strcpy(statement->payload.update_payload.field_to_update, field);
    strcpy(statement->payload.update_payload.new_value, value);

    return PREPARE_SUCCESS;
}