#include "execution.h"

ExecuteResult execute_statement(Statement* statement, DbTable* table) {
    switch (statement->type) {
        case (STATEMENT_INSERT):
            return execute_insert(statement, table);
        case (STATEMENT_SELECT):
        case (STATEMENT_SPECIFIC_SELECT):
            return execute_select(statement, table);
        case (STATEMENT_DROP):
            return execute_drop(statement, table);
        case (STATEMENT_UPDATE):
            return execute_update(statement, table);
        case (STATEMENT_IMPORT):
            return execute_import(statement, table);
        case (STATEMENT_EXPORT):
            return execute_export(statement, table);
    }

    return EXECUTE_SILENT_ERROR;
}

ExecuteResult execute_insert(Statement* statement, DbTable* table) {
    UserRow* user_to_insert = &(statement->payload.user_to_insert);
    uint32_t key_to_insert = user_to_insert->id;
    TableCursor* cursor = table_find(table, key_to_insert);

    void* node = get_page(table->db_pager, cursor->page_idx);
    uint32_t num_cells = *leaf_node_num_cells(node);
    if (cursor->cell_idx < num_cells) {
        uint32_t key_at_index = *leaf_node_key(node, cursor->cell_idx);
        if (key_at_index == key_to_insert)
            return EXECUTE_DUPLICATE_KEY;
    }
    leaf_node_insert(cursor, user_to_insert->id, user_to_insert);

    free(cursor);
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement* statement, DbTable* table) {
    UserRow user;
    if (statement->type == STATEMENT_SPECIFIC_SELECT) {
        uint32_t key_to_find = statement->payload.user_to_insert.id;
        TableCursor* cursor = table_find(table, key_to_find);
        void* node = get_page(table->db_pager, cursor->page_idx);
        uint32_t num_cells = *leaf_node_num_cells(node);

        if (cursor->cell_idx < num_cells) {
            uint32_t key_at_index = *leaf_node_key(node, cursor->cell_idx);
            if (key_at_index == key_to_find) {
                deserialize_user_row(cursor_value(cursor), &user);
                print_user_row(&user);
                printf(ANSI_COLOR_YELLOW "(Fetched 1 row)\n" ANSI_COLOR_RESET);
            }
            else
                printf(ANSI_COLOR_RED "Error: Record with ID %u not found.\n" ANSI_COLOR_RESET, key_to_find);
        }
        else
            printf(ANSI_COLOR_RED "Error: Record with ID %u not found.\n" ANSI_COLOR_RESET, key_to_find);
        
        free(cursor);

    }
    else {
        TableCursor* cursor = table_start(table);
        uint32_t row_count = 0;
        while (!(cursor->end_of_table)) {
            deserialize_user_row(cursor_value(cursor), &user);
            print_user_row(&user);
            cursor_advance(cursor);
            row_count++;
        }

        printf(ANSI_COLOR_YELLOW "(Fetched %u rows)\n" ANSI_COLOR_RESET, row_count);
        free(cursor);
    }
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_drop(Statement* statement, DbTable* table) {
    uint32_t key_to_delete = statement->payload.user_to_insert.id;
    TableCursor* cursor = table_find(table, key_to_delete);
    void* node = get_page(table->db_pager, cursor->page_idx);

    if (cursor->cell_idx >= *leaf_node_num_cells(node) || *leaf_node_key(node, cursor->cell_idx) != key_to_delete) {
        printf(ANSI_COLOR_RED "Error: Record with ID %u not found.\n" ANSI_COLOR_RESET, key_to_delete);
        free(cursor);
        return EXECUTE_SUCCESS;
    }

    uint32_t page_idx_to_adjust = cursor->page_idx;
    leaf_node_remove_cell(node, cursor->cell_idx);
    adjust_tree_after_delete(table, page_idx_to_adjust);

    free(cursor);
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_import(Statement* statement, DbTable* table) {
    char* filename = statement->payload.filename;
    FILE* file = fopen(filename, "r");
    if (!file) {
        perror(ANSI_COLOR_RED "Error opening file" ANSI_COLOR_RESET);
        return EXECUTE_SILENT_ERROR;
    }

    char line_buffer[512];
    int line_num = 0;
    int success_count = 0;
    int fail_count = 0;

    printf("Importing data from '%s'...\n", filename);

    while (fgets(line_buffer, sizeof(line_buffer), file)) {
        line_num++;
        line_buffer[strcspn(line_buffer, "\r\n")] = 0;

        Statement insert_statement;
        insert_statement.type = STATEMENT_INSERT;

        char username[USERNAME_MAX_LENGTH + 2];
        char email[EMAIL_MAX_LENGTH + 2];
        int id;

        int args_assigned = sscanf(line_buffer, "%d,%32[^,],%255s", &id, username, email);
        if (args_assigned != 3) {
            printf("Line malformed. Skipping...\n");
            continue;
        }

        if (id < 0 || strlen(username) > USERNAME_MAX_LENGTH || strlen(email) > EMAIL_MAX_LENGTH )   {
            fprintf(stderr, ANSI_COLOR_RED "Error on line %d: Invalid data.\n" ANSI_COLOR_RESET, line_num);
            fail_count++;
            continue;
        }

        insert_statement.payload.user_to_insert.id = id;
        strcpy(insert_statement.payload.user_to_insert.username, username);
        strcpy(insert_statement.payload.user_to_insert.email, email);

        if (execute_insert(&insert_statement, table) == EXECUTE_SUCCESS) {
            success_count++;
        } else {
            fprintf(stderr, ANSI_COLOR_YELLOW "Skipping line %d: Could not insert row with ID %d (likely a duplicate key).\n" ANSI_COLOR_RESET, line_num, id);
            fail_count++;
        }
    }

    fclose(file);
    printf(ANSI_COLOR_GREEN "Import complete.\n" ANSI_COLOR_RESET);
    printf(ANSI_COLOR_YELLOW "Successfully inserted: %d rows.\n" ANSI_COLOR_RESET, success_count);
    printf(ANSI_COLOR_YELLOW "Failed or skipped: %d rows.\n" ANSI_COLOR_RESET, fail_count);

    return EXECUTE_SUCCESS;
}

ExecuteResult execute_export(Statement* statement, DbTable* table) {
    const char* filename = statement->payload.filename;
    FILE* file = fopen(filename, "w");
    if (!file) {
        perror(ANSI_COLOR_RED "Error opening file for writing" ANSI_COLOR_RESET);
        return EXECUTE_SUCCESS;
    }

    TableCursor* cursor = table_start(table);
    uint32_t row_count = 0;
    UserRow user;

    while (!(cursor->end_of_table)) {
        deserialize_user_row(cursor_value(cursor), &user);
        fprintf(file, "%u,%s,%s\n", user.id, user.username, user.email);
        cursor_advance(cursor);
        row_count++;
    }
    
    free(cursor);
    fclose(file);

    printf(ANSI_COLOR_YELLOW "Exported %u rows to '%s'.\n" ANSI_COLOR_RESET, row_count, filename);
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_update(Statement* statement, DbTable* table) {
    uint32_t id_to_update = statement->payload.update_payload.id;
    TableCursor* cursor = table_find(table, id_to_update);
    void* node = get_page(table->db_pager, cursor->page_idx);
    if (cursor->cell_idx >= *leaf_node_num_cells(node) || *leaf_node_key(node, cursor->cell_idx) != id_to_update) {
        printf(ANSI_COLOR_RED "Error: Record with ID %u not found.\n" ANSI_COLOR_RESET, id_to_update);
        free(cursor);
        return EXECUTE_SILENT_ERROR;
    }

    void* row_location = cursor_value(cursor);
    UserRow existing_row;
    deserialize_user_row(row_location, &existing_row);

    char* field = statement->payload.update_payload.field_to_update;
    char* value = statement->payload.update_payload.new_value;

    if (strcmp(field, "username") == 0)
        strcpy(existing_row.username, value);
    else if (strcmp(field, "email") == 0)
        strcpy(existing_row.email, value);

    serialize_user_row(&existing_row, row_location);
    free(cursor);
    return EXECUTE_SUCCESS;
}