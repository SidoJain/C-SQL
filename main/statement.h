#ifndef DB_STATEMENT_H
#define DB_STATEMENT_H

#include "common.h"
#include "input.h"
#include "row.h"

typedef struct {
    StatementType type;
    union {
        UserRow       user_to_insert;
        char          filename[FILENAME_MAX_LENGTH + 1];
        UpdatePayload update_payload;
    } payload;
} Statement;

PrepareResult prepare_statement(InputBuffer* input_buffer, Statement* statement);
PrepareResult prepare_insert(InputBuffer* input_buffer, Statement* statement);
PrepareResult prepare_drop(InputBuffer* input_buffer, Statement* statement);
PrepareResult prepare_import(InputBuffer* input_buffer, Statement* statement);
PrepareResult prepare_export(InputBuffer* input_buffer, Statement* statement);
PrepareResult prepare_update(InputBuffer* input_buffer, Statement* statement);

#endif