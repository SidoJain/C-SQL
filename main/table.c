#include "table.h"

DbTable* db_open(const char* db_filename) {
    DbPager* db_pager = pager_open(db_filename);
    DbTable* table = malloc(sizeof(DbTable));
    table->db_pager = db_pager;
    table->root_page_idx = 0;
    if (db_pager->num_pages == 0) {
        void* root_node = get_page(db_pager, 0);
        initialize_leaf_node(root_node);
        set_node_root(root_node, true);
    }

    return table;
}

void db_close(DbTable* table) {
    DbPager* db_pager = table->db_pager;

    uint32_t max_page_used = 0;
    for (uint32_t i = 0; i < MAX_PAGES; i++) {
        if (db_pager->pages[i] != NULL) {
            pager_flush(db_pager, i);
            free(db_pager->pages[i]);
            db_pager->pages[i] = NULL;
            if (i > max_page_used)
                max_page_used = i;
        }
    }

    off_t expected_size = (max_page_used + 1) * PAGE_SIZE_BYTES;
    if (ftruncate(db_pager->file_descriptor, expected_size) != 0) {
        printf(ANSI_COLOR_RED "Error truncating db file.\n" ANSI_COLOR_RESET);
        exit(EXIT_FAILURE);
    }

    if (close(db_pager->file_descriptor) == -1) {
        printf(ANSI_COLOR_RED "Error closing db file.\n" ANSI_COLOR_RESET);
        exit(EXIT_FAILURE);
    }

    free(db_pager);
    free(table);
}

TableCursor* table_start(DbTable* table) {
    TableCursor* cursor = table_find(table, 0);
    void* node = get_page(table->db_pager, cursor->page_idx);
    uint32_t num_cells = *leaf_node_num_cells(node);
    cursor->end_of_table = (num_cells == 0);

    return cursor;
}

TableCursor* table_find(DbTable* table, uint32_t key) {
    uint32_t root_page_idx = table->root_page_idx;
    void* root_node = get_page(table->db_pager, root_page_idx);

    if (get_node_type(root_node) == NODE_LEAF)
        return leaf_node_find(table, root_page_idx, key);
    else
        return internal_node_find(table, root_page_idx, key);
}

void* cursor_value(TableCursor* cursor) {
    uint32_t page_idx = cursor->page_idx;
    void* page = get_page(cursor->table->db_pager, page_idx);

    return leaf_node_value(page, cursor->cell_idx);
}

void cursor_advance(TableCursor* cursor) {
    uint32_t page_idx = cursor->page_idx;
    void* node = get_page(cursor->table->db_pager, page_idx);

    cursor->cell_idx++;
    if (cursor->cell_idx >= (*leaf_node_num_cells(node))) {
        uint32_t next_page_idx = *leaf_node_next_leaf(node);
        if (next_page_idx == 0)
            cursor->end_of_table = true;
        else {
            cursor->page_idx = next_page_idx;
            cursor->cell_idx = 0;
        }
    }
}