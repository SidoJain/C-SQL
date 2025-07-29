#include "meta_command.h"

MetaCommandResult do_meta_command(InputBuffer* input_buffer, DbTable* table) {
    if (strncmp(input_buffer->buffer, ".exit", 5) == 0) {
        close_input_buffer(input_buffer);
        db_close(table);
        exit(EXIT_SUCCESS);
    }
    else if (strncmp(input_buffer->buffer, ".btree", 6) == 0) {
        printf("Tree:\n");
        print_tree(table->db_pager, 0, 0);
        return META_COMMAND_SUCCESS;
    }
    else if (strncmp(input_buffer->buffer, ".constants", 10) == 0) {
        printf("Constants:\n");
        print_constants();
        return META_COMMAND_SUCCESS;
    }
    else if (strncmp(input_buffer->buffer, ".commands", 9) == 0) {
        printf("Commands:\n");
        print_commands();
        return META_COMMAND_SUCCESS;
    }
    else
        return META_COMMAND_UNRECOGNIZED_COMMAND;
}

void print_constants() {
    printf("USER_ROW_SIZE: %d\n", USER_ROW_SIZE);
    printf("COMMON_NODE_HEADER_SIZE: %d\n", COMMON_NODE_HEADER_SIZE);
    printf("LEAF_NODE_HEADER_SIZE: %d\n", LEAF_NODE_HEADER_SIZE);
    printf("LEAF_NODE_CELL_SIZE: %d\n", LEAF_NODE_CELL_SIZE);
    printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n", LEAF_NODE_SPACE_FOR_CELLS);
    printf("LEAF_NODE_MAX_CELLS: %d\n", LEAF_NODE_MAX_CELLS);
    printf("INTERNAL_NODE_MAX_KEYS: %d\n", INTERNAL_NODE_MAX_KEYS);
}

void print_commands() {
    printf("insert {num} {name} {email}\n");
    printf("select\n");
    printf("select {id}\n");
    printf("update {id} set {param}={value}\n");
    printf("drop {id}\n");
    printf("import '{file.csv}'\n");
    printf("export '{file.csv}'\n");
    printf(".btree\n");
    printf(".commands\n");
    printf(".constants\n");
    printf(".exit\n");
}

void indent(uint32_t level) {
    for (uint32_t i = 0; i < level; i++)
        printf("  ");
}

void print_tree(DbPager* db_pager, uint32_t page_idx, uint32_t indentation_level) {
    void* node = get_page(db_pager, page_idx);
    uint32_t num_keys, child_page_idx;

    switch (get_node_type(node)) {
        case (NODE_LEAF):
            num_keys = *leaf_node_num_cells(node);
            indent(indentation_level);
            printf("- leaf (size %d)\n", num_keys);
            for (uint32_t i = 0; i < num_keys; i++) {
                indent(indentation_level + 1);
                printf("- %d\n", *leaf_node_key(node, i));
            }
            break;
        case (NODE_INTERNAL):
            num_keys = *internal_node_num_keys(node);
            indent(indentation_level);
            printf("- internal (size %d)\n", num_keys);

            for (uint32_t i = 0; i < num_keys; i++) {
                child_page_idx = *internal_node_child(node, i);
                print_tree(db_pager, child_page_idx, indentation_level + 1);

                indent(indentation_level + 1);
                printf("- key %d\n", *internal_node_key(node, i));
            }
            
            child_page_idx = *internal_node_right_child(node);
            if (child_page_idx != INVALID_PAGE_IDX)
                print_tree(db_pager, child_page_idx, indentation_level + 1);
            break;
    }
}