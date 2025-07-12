#include "node.h"

NodeType get_node_type(void* node) {
    uint8_t value = *((uint8_t*)((char*)node + NODE_TYPE_OFFSET));
    return (NodeType)value;
}

void set_node_type(void* node, NodeType type) {
    uint8_t value = type;
    *((uint8_t*)((char*)node + NODE_TYPE_OFFSET)) = value;
}

bool is_node_root(void* node) {
    uint8_t value = *((uint8_t*)((char*)node + IS_ROOT_OFFSET));
    return (bool)value;
}

void set_node_root(void* node, bool is_root) {
    uint8_t value = is_root;
    *((uint8_t*)((char*)node + IS_ROOT_OFFSET)) = value;
}

uint32_t* node_parent(void* node) {
    return (uint32_t*)((uint8_t*)node + PARENT_POINTER_OFFSET);
}

uint32_t get_node_max_key(DbPager* db_pager, void* node) {
    if (get_node_type(node) == NODE_LEAF)
        return *leaf_node_key(node, *leaf_node_num_cells(node) - 1);
    void* right_child = get_page(db_pager, *internal_node_right_child(node));

    return get_node_max_key(db_pager, right_child);
}

uint32_t* leaf_node_num_cells(void* node) {
    return (uint32_t*)((uint8_t*)node + LEAF_NODE_NUM_CELLS_OFFSET);
}

uint32_t* leaf_node_next_leaf(void* node) {
    return (uint32_t*)((uint8_t*)node + LEAF_NODE_NEXT_LEAF_OFFSET);
}

void* leaf_node_cell(void* node, uint32_t cell_idx) {
    return (uint8_t*)node + LEAF_NODE_HEADER_SIZE + cell_idx * LEAF_NODE_CELL_SIZE;
}

uint32_t* leaf_node_key(void* node, uint32_t cell_idx) {
    return (uint32_t*)leaf_node_cell(node, cell_idx);
}

void* leaf_node_value(void* node, uint32_t cell_idx) {
    return (uint8_t*)leaf_node_cell(node, cell_idx) + LEAF_NODE_KEY_SIZE;
}

void initialize_leaf_node(void* node) {
    set_node_type(node, NODE_LEAF);
    set_node_root(node, false);
    *leaf_node_num_cells(node) = 0;
    *leaf_node_next_leaf(node) = 0;
    *node_parent(node) = 0;
}

void leaf_node_insert(TableCursor* cursor, uint32_t key, UserRow* value) {
    void* node = get_page(cursor->table->db_pager, cursor->page_idx);
    uint32_t num_cells = *leaf_node_num_cells(node);
    if (num_cells >= LEAF_NODE_MAX_CELLS) {
        leaf_node_split_and_insert(cursor, key, value);
        return;
    }

    if (cursor->cell_idx < num_cells)
        for (uint32_t i = num_cells; i > cursor->cell_idx; i--)
            memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i - 1), LEAF_NODE_CELL_SIZE);

    *(leaf_node_num_cells(node)) += 1;
    *(leaf_node_key(node, cursor->cell_idx)) = key;
    serialize_user_row(value, leaf_node_value(node, cursor->cell_idx));
}

void leaf_node_split_and_insert(TableCursor* cursor, uint32_t key, UserRow* value) {
    void* old_node = get_page(cursor->table->db_pager, cursor->page_idx);
    uint32_t new_page_idx = get_unused_page_num(cursor->table->db_pager);
    void* new_node = get_page(cursor->table->db_pager, new_page_idx);

    if (!new_node) {
        fprintf(stderr, "FATAL: new_node is NULL!\n");
        exit(EXIT_FAILURE);
    }

    initialize_leaf_node(new_node);
    *node_parent(new_node) = *node_parent(old_node);
    *leaf_node_next_leaf(new_node) = *leaf_node_next_leaf(old_node);
    *leaf_node_next_leaf(old_node) = new_page_idx;

    UserRow temp_rows[LEAF_NODE_MAX_CELLS + 1];
    uint32_t temp_keys[LEAF_NODE_MAX_CELLS + 1];

    uint32_t old_num_cells = *leaf_node_num_cells(old_node);
    for (uint32_t i = 0, j = 0; i < old_num_cells; i++, j++) {
        if (j == cursor->cell_idx)
            j++;
        deserialize_user_row(leaf_node_value(old_node, i), &temp_rows[j]);
        temp_keys[j] = *leaf_node_key(old_node, i);
    }

    temp_rows[cursor->cell_idx] = *value;
    temp_keys[cursor->cell_idx] = key;

    *leaf_node_num_cells(old_node) = 0;
    for (uint32_t i = 0; i < LEAF_NODE_LEFT_SPLIT_COUNT; i++) {
        *(leaf_node_key(old_node, i)) = temp_keys[i];
        serialize_user_row(&temp_rows[i], leaf_node_value(old_node, i));
        (*leaf_node_num_cells(old_node))++;
    }

    *leaf_node_num_cells(new_node) = 0;
    for (uint32_t i = 0; i < LEAF_NODE_RIGHT_SPLIT_COUNT; i++) {
        uint32_t idx = i + LEAF_NODE_LEFT_SPLIT_COUNT;
        *(leaf_node_key(new_node, i)) = temp_keys[idx];
        serialize_user_row(&temp_rows[idx], leaf_node_value(new_node, i));
        (*leaf_node_num_cells(new_node))++;
    }

    if (is_node_root(old_node))
        create_new_root(cursor->table, new_page_idx);
    else {
        void* parent = get_page(cursor->table->db_pager, *node_parent(old_node));
        uint32_t old_max = get_node_max_key(cursor->table->db_pager, old_node);
        uint32_t new_max = get_node_max_key(cursor->table->db_pager, new_node);
        update_internal_node_key(parent, old_max, new_max);
        internal_node_insert(cursor->table, *node_parent(old_node), new_page_idx);
    }
}

TableCursor* leaf_node_find(DbTable* table, uint32_t page_idx, uint32_t key) {
    void* node = get_page(table->db_pager, page_idx);
    uint32_t num_cells = *leaf_node_num_cells(node);

    TableCursor* cursor = malloc(sizeof(TableCursor));
    cursor->table = table;
    cursor->page_idx = page_idx;
    cursor->end_of_table = false;

    uint32_t min_index = 0;
    uint32_t one_past_max_index = num_cells;
    while (one_past_max_index != min_index) {
        uint32_t index = (min_index + one_past_max_index) / 2;
        uint32_t key_at_index = *leaf_node_key(node, index);
        if (key == key_at_index) {
            cursor->cell_idx = index;
            return cursor;
        }
        if (key < key_at_index)
            one_past_max_index = index;
        else
            min_index = index + 1;
    }

    cursor->cell_idx = min_index;
    return cursor;
}

uint32_t* internal_node_num_keys(void* node) {
    return (uint32_t*)((uint8_t*)node + INTERNAL_NODE_NUM_KEYS_OFFSET);
}

uint32_t* internal_node_right_child(void* node) {
    return (uint32_t*)((uint8_t*)node + INTERNAL_NODE_RIGHT_CHILD_OFFSET);
}

uint32_t* internal_node_cell(void* node, uint32_t cell_idx) {
    return (uint32_t*)((uint8_t*)node + INTERNAL_NODE_HEADER_SIZE + cell_idx * INTERNAL_NODE_CELL_SIZE);
}

uint32_t* internal_node_child(void* node, uint32_t child_num) {
    uint32_t num_keys = *internal_node_num_keys(node);
    if (child_num > num_keys) {
        printf(ANSI_COLOR_RED "Tried to access child_num %d > num_keys %d\n" ANSI_COLOR_RESET, child_num, num_keys);
        exit(EXIT_FAILURE);
    }
    else if (child_num == num_keys) {
        uint32_t* right_child = internal_node_right_child(node);
        if (*right_child == INVALID_PAGE_IDX) {
            printf(ANSI_COLOR_RED "Tried to access right child of node, but was invalid page\n" ANSI_COLOR_RESET);
            exit(EXIT_FAILURE);
        }
        return right_child;
    }
    else {
        uint32_t* child = internal_node_cell(node, child_num);
        if (*child == INVALID_PAGE_IDX) {
            printf(ANSI_COLOR_RED "Tried to access child %d of node, but was invalid page\n" ANSI_COLOR_RESET, child_num);
            exit(EXIT_FAILURE);
        }
        return child;
    }
}

uint32_t* internal_node_key(void* node, uint32_t key_num) {
    return (uint32_t*)((uint8_t*)internal_node_cell(node, key_num) + INTERNAL_NODE_CHILD_SIZE);
}

void initialize_internal_node(void* node) {
    set_node_type(node, NODE_INTERNAL);
    set_node_root(node, false);
    *internal_node_num_keys(node) = 0;
    *internal_node_right_child(node) = INVALID_PAGE_IDX;
    *node_parent(node) = 0;
}

uint32_t internal_node_find_child(void* node, uint32_t key) {
    uint32_t num_keys = *internal_node_num_keys(node);

    uint32_t min_index = 0;
    uint32_t max_index = num_keys;
    while (min_index != max_index) {
        uint32_t index = (min_index + max_index) / 2;
        uint32_t key_to_right = *internal_node_key(node, index);
        if (key_to_right >= key)
            max_index = index;
        else
            min_index = index + 1;
    }

    return min_index;
}

void update_internal_node_key(void* node, uint32_t old_key, uint32_t new_key) {
    uint32_t old_child_index = internal_node_find_child(node, old_key);
    *internal_node_key(node, old_child_index) = new_key;
}

void internal_node_insert(DbTable* table, uint32_t parent_page_idx, uint32_t child_page_idx) {
    void* parent = get_page(table->db_pager, parent_page_idx);
    void* child = get_page(table->db_pager, child_page_idx);
    uint32_t child_max_key = get_node_max_key(table->db_pager, child);
    uint32_t index = internal_node_find_child(parent, child_max_key);
    uint32_t original_num_keys = *internal_node_num_keys(parent);
    if (original_num_keys >= INTERNAL_NODE_MAX_KEYS) {
        internal_node_split_and_insert(table, parent_page_idx, child_page_idx);
        return;
    }

    uint32_t right_child_page_idx = *internal_node_right_child(parent);
    if (right_child_page_idx == INVALID_PAGE_IDX) {
        *internal_node_right_child(parent) = child_page_idx;
        return;
    }

    void* right_child = get_page(table->db_pager, right_child_page_idx);
    *internal_node_num_keys(parent) = original_num_keys + 1;
    if (child_max_key > get_node_max_key(table->db_pager, right_child)) {
        *internal_node_child(parent, original_num_keys) = right_child_page_idx;
        *internal_node_key(parent, original_num_keys) =
            get_node_max_key(table->db_pager, right_child);
        *internal_node_right_child(parent) = child_page_idx;
    }
    else {
        for (uint32_t i = original_num_keys; i > index; i--) {
            void* destination = internal_node_cell(parent, i);
            void* source = internal_node_cell(parent, i - 1);
            memcpy(destination, source, INTERNAL_NODE_CELL_SIZE);
        }
        *internal_node_child(parent, index) = child_page_idx;
        *internal_node_key(parent, index) = child_max_key;
    }
}

void internal_node_split_and_insert(DbTable* table, uint32_t parent_page_idx, uint32_t child_page_idx) {
    uint32_t old_page_idx = parent_page_idx;
    void* old_node = get_page(table->db_pager, parent_page_idx);
    uint32_t old_max_key = get_node_max_key(table->db_pager, old_node);
    void* child = get_page(table->db_pager, child_page_idx); 
    uint32_t child_max_key = get_node_max_key(table->db_pager, child);
    uint32_t new_page_idx = get_unused_page_num(table->db_pager);
    uint32_t splitting_root = is_node_root(old_node);

    void* parent;
    void* new_node;
    if (splitting_root) {
        create_new_root(table, new_page_idx);
        parent = get_page(table->db_pager, table->root_page_idx);
        old_page_idx = *internal_node_child(parent, 0);
        old_node = get_page(table->db_pager, old_page_idx);
    }
    else {
        parent = get_page(table->db_pager, *node_parent(old_node));
        new_node = get_page(table->db_pager, new_page_idx);
        initialize_internal_node(new_node);
    }
    
    uint32_t* old_num_keys = internal_node_num_keys(old_node);
    uint32_t cur_page_num = *internal_node_right_child(old_node);
    void* cur = get_page(table->db_pager, cur_page_num);

    internal_node_insert(table, new_page_idx, cur_page_num);
    *node_parent(cur) = new_page_idx;
    *internal_node_right_child(old_node) = INVALID_PAGE_IDX;
    for (uint32_t i = INTERNAL_NODE_MAX_KEYS - 1; i > INTERNAL_NODE_MAX_KEYS / 2; i--) {
        cur_page_num = *internal_node_child(old_node, i);
        cur = get_page(table->db_pager, cur_page_num);
        internal_node_insert(table, new_page_idx, cur_page_num);
        *node_parent(cur) = new_page_idx;
        (*old_num_keys)--;
    }

    *internal_node_right_child(old_node) = *internal_node_child(old_node, (*old_num_keys)--);
    uint32_t max_after_split = get_node_max_key(table->db_pager, old_node);
    uint32_t destination_page_num = child_max_key < max_after_split ? old_page_idx : new_page_idx;

    internal_node_insert(table, destination_page_num, child_page_idx);
    *node_parent(child) = destination_page_num;
    update_internal_node_key(parent, old_max_key, get_node_max_key(table->db_pager, old_node));
    if (!splitting_root) {
        internal_node_insert(table, *node_parent(old_node), new_page_idx);
        *node_parent(new_node) = *node_parent(old_node);
    }
}

TableCursor* internal_node_find(DbTable* table, uint32_t page_idx, uint32_t key) {
    void* node = get_page(table->db_pager, page_idx);

    uint32_t child_index = internal_node_find_child(node, key);
    uint32_t child_num = *internal_node_child(node, child_index);
    void* child = get_page(table->db_pager, child_num);
    switch (get_node_type(child)) {
        case NODE_LEAF:
            return leaf_node_find(table, child_num, key);
        case NODE_INTERNAL:
            return internal_node_find(table, child_num, key);
    }

    return internal_node_find(table, child_num, key);
}

void create_new_root(DbTable* table, uint32_t right_child_page_idx) {
    void* root = get_page(table->db_pager, table->root_page_idx);
    void* right_child = get_page(table->db_pager, right_child_page_idx);
    uint32_t left_child_page_idx = get_unused_page_num(table->db_pager);
    void* left_child = get_page(table->db_pager, left_child_page_idx);
    if (get_node_type(root) == NODE_INTERNAL) {
        initialize_internal_node(right_child);
        initialize_internal_node(left_child);
    }

    memcpy(left_child, root, PAGE_SIZE_BYTES);
    set_node_root(left_child, false);
    if (get_node_type(left_child) == NODE_INTERNAL) {
        void* child;
        for (uint32_t i = 0; i < *internal_node_num_keys(left_child); i++) {
            child = get_page(table->db_pager, *internal_node_child(left_child, i));
            *node_parent(child) = left_child_page_idx;
        }
        child = get_page(table->db_pager, *internal_node_right_child(left_child));
        *node_parent(child) = left_child_page_idx;
    }

    initialize_internal_node(root);
    set_node_root(root, true);
    *internal_node_num_keys(root) = 1;
    *internal_node_child(root, 0) = left_child_page_idx;
    uint32_t left_child_max_key = get_node_max_key(table->db_pager, left_child);
    *internal_node_key(root, 0) = left_child_max_key;
    *internal_node_right_child(root) = right_child_page_idx;
    *node_parent(left_child) = table->root_page_idx;
    *node_parent(right_child) = table->root_page_idx;
}

void leaf_node_remove_cell(void* node, uint32_t cell_idx) {
    uint32_t num_cells = *leaf_node_num_cells(node);
    for (uint32_t i = cell_idx; i < num_cells - 1; i++)
        memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i + 1), LEAF_NODE_CELL_SIZE);
    (*leaf_node_num_cells(node))--;
}

uint32_t get_node_child_index(void* parent_node, uint32_t child_page_idx) {
    uint32_t num_keys = *internal_node_num_keys(parent_node);
    for (uint32_t i = 0; i < num_keys; i++)
        if (*internal_node_child(parent_node, i) == child_page_idx)
            return i;

    if (*internal_node_right_child(parent_node) == child_page_idx)
        return num_keys;
    printf(ANSI_COLOR_RED "Could not find child %d in parent.\n" ANSI_COLOR_RESET, child_page_idx);
    exit(EXIT_FAILURE);
}

void merge_nodes(DbTable* table, uint32_t parent_page_idx, uint32_t node_page_idx, uint32_t sibling_page_idx) {
    void* parent_node = get_page(table->db_pager, parent_page_idx);
    void* node = get_page(table->db_pager, node_page_idx);
    void* sibling_node = get_page(table->db_pager, sibling_page_idx);
    uint32_t sibling_child_index_in_parent = get_node_child_index(parent_node, sibling_page_idx);

    if (get_node_type(node) == NODE_LEAF) {
        uint32_t node_num_cells = *leaf_node_num_cells(node);
        uint32_t sibling_num_cells = *leaf_node_num_cells(sibling_node);

        memcpy(leaf_node_cell(node, node_num_cells), leaf_node_cell(sibling_node, 0), sibling_num_cells * LEAF_NODE_CELL_SIZE);
        *leaf_node_num_cells(node) += sibling_num_cells;

        *leaf_node_next_leaf(node) = *leaf_node_next_leaf(sibling_node);
    }
    else {
        uint32_t node_num_keys = *internal_node_num_keys(node);
        uint32_t sibling_num_keys = *internal_node_num_keys(sibling_node);

        uint32_t key_from_parent = *internal_node_key(parent_node, sibling_child_index_in_parent - 1);
        *internal_node_key(node, node_num_keys) = key_from_parent;
        
        memcpy(internal_node_cell(node, node_num_keys + 1), internal_node_cell(sibling_node, 0), sibling_num_keys * INTERNAL_NODE_CELL_SIZE);
        *internal_node_right_child(node) = *internal_node_right_child(sibling_node);
        *internal_node_num_keys(node) += sibling_num_keys + 1;

        uint32_t total_keys = *internal_node_num_keys(node);
        for(uint32_t i = node_num_keys + 1; i < total_keys + 1; i++) {
            uint32_t child_page_idx = *internal_node_child(node, i);
            void* child = get_page(table->db_pager, child_page_idx);
            *node_parent(child) = node_page_idx;
        }
    }

    uint32_t num_parent_keys = *internal_node_num_keys(parent_node);
    for (uint32_t i = sibling_child_index_in_parent - 1; i < num_parent_keys - 1; i++)
        memcpy(internal_node_cell(parent_node, i), internal_node_cell(parent_node, i + 1), INTERNAL_NODE_CELL_SIZE);

    if (sibling_child_index_in_parent == num_parent_keys)
        *internal_node_right_child(parent_node) = *internal_node_child(parent_node, num_parent_keys - 1);
    *internal_node_num_keys(parent_node) -= 1;
    
    uint32_t parent_of_parent_idx = *node_parent(parent_node);
    if (parent_of_parent_idx != 0) {
        void* parent_of_parent = get_page(table->db_pager, parent_of_parent_idx);
        uint32_t old_max = *internal_node_key(parent_of_parent, get_node_child_index(parent_of_parent, parent_page_idx));
        uint32_t new_max = get_node_max_key(table->db_pager, parent_node);
        update_internal_node_key(parent_of_parent, old_max, new_max);
    }

    adjust_tree_after_delete(table, parent_page_idx);
}

void redistribute_cells(DbTable* table, uint32_t parent_page_idx, uint32_t node_page_idx, uint32_t sibling_page_idx) {
    void* parent_node = get_page(table->db_pager, parent_page_idx);
    void* node = get_page(table->db_pager, node_page_idx);
    void* sibling_node = get_page(table->db_pager, sibling_page_idx);
    uint32_t node_child_index = get_node_child_index(parent_node, node_page_idx);

    if (node_child_index < get_node_child_index(parent_node, sibling_page_idx)) {
        uint32_t num_cells_node = *leaf_node_num_cells(node);
        memcpy(leaf_node_cell(node, num_cells_node), leaf_node_cell(sibling_node, 0), LEAF_NODE_CELL_SIZE);
        (*leaf_node_num_cells(node))++;

        uint32_t num_cells_sibling = *leaf_node_num_cells(sibling_node);
        for (uint32_t i = 0; i < num_cells_sibling - 1; i++)
            memcpy(leaf_node_cell(sibling_node, i), leaf_node_cell(sibling_node, i + 1), LEAF_NODE_CELL_SIZE);
        (*leaf_node_num_cells(sibling_node))--;

        *internal_node_key(parent_node, node_child_index) = *leaf_node_key(node, num_cells_node);
    }
    else {
        for (uint32_t i = *leaf_node_num_cells(node); i > 0; i--)
            memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i - 1), LEAF_NODE_CELL_SIZE);

        uint32_t num_cells_sibling = *leaf_node_num_cells(sibling_node);
        memcpy(leaf_node_cell(node, 0), leaf_node_cell(sibling_node, num_cells_sibling - 1), LEAF_NODE_CELL_SIZE);
        (*leaf_node_num_cells(node))++;
        (*leaf_node_num_cells(sibling_node))--;

        *internal_node_key(parent_node, node_child_index - 1) = *leaf_node_key(sibling_node, *leaf_node_num_cells(sibling_node) - 1);
    }
}

void adjust_tree_after_delete(DbTable* table, uint32_t page_idx) {
    void* node = get_page(table->db_pager, page_idx);
    uint32_t num_cells = (get_node_type(node) == NODE_LEAF) ? *leaf_node_num_cells(node) : *internal_node_num_keys(node);
    uint32_t min_cells = (get_node_type(node) == NODE_LEAF) ? LEAF_NODE_MIN_CELLS : INTERNAL_NODE_MIN_KEYS;

    if (is_node_root(node)) {
        handle_root_shrink(table);
        return;
    }
    if (num_cells >= min_cells)
        return;

    uint32_t parent_page_idx = *node_parent(node);
    void* parent_node = get_page(table->db_pager, parent_page_idx);
    uint32_t child_index = get_node_child_index(parent_node, page_idx);

    uint32_t sibling_page_idx;
    if (child_index == *internal_node_num_keys(parent_node))
        sibling_page_idx = *internal_node_child(parent_node, child_index - 1);
    else
        sibling_page_idx = *internal_node_child(parent_node, child_index + 1);

    void* sibling_node = get_page(table->db_pager, sibling_page_idx);
    uint32_t sibling_num_cells = (get_node_type(sibling_node) == NODE_LEAF) ? *leaf_node_num_cells(sibling_node) : *internal_node_num_keys(sibling_node);
    
    if (sibling_num_cells > min_cells)
        redistribute_cells(table, parent_page_idx, page_idx, sibling_page_idx);
    else {
        if (child_index > get_node_child_index(parent_node, sibling_page_idx))
            merge_nodes(table, parent_page_idx, sibling_page_idx, page_idx);
        else
            merge_nodes(table, parent_page_idx, page_idx, sibling_page_idx);
    }
}

void handle_root_shrink(DbTable* table) {
    uint32_t root_page_idx = table->root_page_idx;
    void* root_node = get_page(table->db_pager, root_page_idx);

    if (get_node_type(root_node) == NODE_INTERNAL && *internal_node_num_keys(root_node) == 0) {
        uint32_t new_root_page_idx = *internal_node_child(root_node, 0);
        void* new_root_node = get_page(table->db_pager, new_root_page_idx);

        table->root_page_idx = new_root_page_idx;
        set_node_root(new_root_node, true);
        *node_parent(new_root_node) = 0;
    }
}