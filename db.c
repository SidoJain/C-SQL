#include <fcntl.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>

#define size_of_attribute(Struct, Attribute) (sizeof(((Struct*)0)->Attribute))

#define USERNAME_MAX_LENGTH     32
#define EMAIL_MAX_LENGTH         255
#define FILENAME_MAX_LENGTH     255

#define ID_FIELD_SIZE           size_of_attribute(UserRow, id)
#define USERNAME_FIELD_SIZE     size_of_attribute(UserRow, username)
#define EMAIL_FIELD_SIZE        size_of_attribute(UserRow, email)
#define ID_FIELD_OFFSET         0
#define USERNAME_FIELD_OFFSET   (ID_FIELD_OFFSET + ID_FIELD_SIZE)
#define EMAIL_FIELD_OFFSET      (USERNAME_FIELD_OFFSET + USERNAME_FIELD_SIZE)
#define USER_ROW_SIZE           (ID_FIELD_SIZE + USERNAME_FIELD_SIZE + EMAIL_FIELD_SIZE)

#define PAGE_SIZE_BYTES         4096
#define MAX_PAGES               100
#define INVALID_PAGE_IDX        UINT32_MAX

#define NODE_TYPE_SIZE              sizeof(uint8_t)
#define NODE_TYPE_OFFSET            0
#define IS_ROOT_SIZE                sizeof(uint8_t)
#define IS_ROOT_OFFSET              NODE_TYPE_SIZE
#define PARENT_POINTER_SIZE         sizeof(uint32_t)
#define PARENT_POINTER_OFFSET       (IS_ROOT_OFFSET + IS_ROOT_SIZE)
#define COMMON_NODE_HEADER_SIZE     (NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE)

#define LEAF_NODE_KEY_SIZE          sizeof(uint32_t)
#define LEAF_NODE_KEY_OFFSET        0
#define LEAF_NODE_VALUE_SIZE        USER_ROW_SIZE
#define LEAF_NODE_CELL_SIZE         (LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE)
#define LEAF_NODE_NUM_CELLS_SIZE    sizeof(uint32_t)
#define LEAF_NODE_NUM_CELLS_OFFSET  COMMON_NODE_HEADER_SIZE
#define LEAF_NODE_NEXT_LEAF_SIZE    sizeof(uint32_t)
#define LEAF_NODE_NEXT_LEAF_OFFSET  (LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE)
#define LEAF_NODE_HEADER_SIZE       (COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE + LEAF_NODE_NEXT_LEAF_SIZE)
#define LEAF_NODE_SPACE_FOR_CELLS   (PAGE_SIZE_BYTES - LEAF_NODE_HEADER_SIZE)
#define LEAF_NODE_MAX_CELLS         (LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE)
#define LEAF_NODE_RIGHT_SPLIT_COUNT ((LEAF_NODE_MAX_CELLS + 1) / 2)
#define LEAF_NODE_LEFT_SPLIT_COUNT  ((LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT)
#define LEAF_NODE_MIN_CELLS         (LEAF_NODE_LEFT_SPLIT_COUNT - 1)

#define INTERNAL_NODE_NUM_KEYS_SIZE         sizeof(uint32_t)
#define INTERNAL_NODE_NUM_KEYS_OFFSET       COMMON_NODE_HEADER_SIZE
#define INTERNAL_NODE_RIGHT_CHILD_SIZE      sizeof(uint32_t)
#define INTERNAL_NODE_RIGHT_CHILD_OFFSET    (INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE)
#define INTERNAL_NODE_HEADER_SIZE           (COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE)
#define INTERNAL_NODE_KEY_SIZE              sizeof(uint32_t)
#define INTERNAL_NODE_CHILD_SIZE            sizeof(uint32_t)
#define INTERNAL_NODE_CELL_SIZE             (INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE)
#define INTERNAL_NODE_MAX_KEYS              ((PAGE_SIZE_BYTES - INTERNAL_NODE_HEADER_SIZE) / INTERNAL_NODE_CELL_SIZE)
#define INTERNAL_NODE_MIN_KEYS      (INTERNAL_NODE_MAX_KEYS / 2)

#define ANSI_COLOR_GREEN    "\x1b[32m"
#define ANSI_COLOR_YELLOW   "\x1b[33m"
#define ANSI_COLOR_RED      "\x1b[31m"
#define ANSI_COLOR_RESET    "\x1b[0m"

typedef enum {
    EXECUTE_SUCCESS,
    EXECUTE_DUPLICATE_KEY,
    EXECUTE_SILENT_ERROR
} ExecuteResult;

typedef enum {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

typedef enum {
    PREPARE_SUCCESS,
    PREPARE_NEGATIVE_ID,
    PREPARE_STRING_TOO_LONG,
    PREPARE_SYNTAX_ERROR,
    PREPARE_UNRECOGNIZED_STATEMENT
} PrepareResult;

typedef enum {
    STATEMENT_INSERT,
    STATEMENT_SELECT,
    STATEMENT_SPECIFIC_SELECT,
    STATEMENT_DROP,
    STATEMENT_IMPORT,
    STATEMENT_EXPORT,
    STATEMENT_UPDATE
} StatementType;

typedef enum {
    NODE_INTERNAL,
    NODE_LEAF
} NodeType;

typedef struct {
    char* buffer;
    size_t buffer_length;
    ssize_t input_length;
} InputBuffer;

typedef struct {
    uint32_t id;
    char username[USERNAME_MAX_LENGTH + 1];
    char email[EMAIL_MAX_LENGTH + 1];
} UserRow;

typedef struct {
    uint32_t id;
    char field_to_update[USERNAME_MAX_LENGTH];
    char new_value[EMAIL_MAX_LENGTH + 1];
} UpdatePayload;

typedef struct {
    StatementType type;
    union {
        UserRow user_to_insert;
        char filename[FILENAME_MAX_LENGTH + 1];
        UpdatePayload update_payload;
    } payload;
} Statement;

typedef struct {
    int file_descriptor;
    uint32_t file_length;
    uint32_t num_pages;
    void* pages[MAX_PAGES];
} DbPager;

typedef struct {
    DbPager* db_pager;
    uint32_t root_page_idx;
} DbTable;

typedef struct {
    DbTable* table;
    uint32_t page_idx;
    uint32_t cell_idx;
    bool end_of_table;
} TableCursor;

void serialize_user_row(UserRow* source, void* destination);
void deserialize_user_row(void* source, UserRow* destination);
void print_user_row(UserRow* user);

NodeType get_node_type(void* node);
void set_node_type(void* node, NodeType type);
bool is_node_root(void* node);
void set_node_root(void* node, bool is_root);
uint32_t* node_parent(void* node);

uint32_t* leaf_node_num_cells(void* node);
uint32_t* leaf_node_next_leaf(void* node);
void* leaf_node_cell(void* node, uint32_t cell_idx);
uint32_t* leaf_node_key(void* node, uint32_t cell_idx);
void* leaf_node_value(void* node, uint32_t cell_idx);
void initialize_leaf_node(void* node);
void leaf_node_insert(TableCursor* cursor, uint32_t key, UserRow* value);
void leaf_node_split_and_insert(TableCursor* cursor, uint32_t key, UserRow* value);

uint32_t* internal_node_num_keys(void* node);
uint32_t* internal_node_right_child(void* node);
uint32_t* internal_node_cell(void* node, uint32_t cell_idx);
uint32_t* internal_node_child(void* node, uint32_t child_num);
uint32_t* internal_node_key(void* node, uint32_t key_num);
void initialize_internal_node(void* node);
void update_internal_node_key(void* node, uint32_t old_key, uint32_t new_key);
uint32_t internal_node_find_child(void* node, uint32_t key);
void internal_node_insert(DbTable* table, uint32_t parent_page_idx, uint32_t child_page_idx);
void internal_node_split_and_insert(DbTable* table, uint32_t parent_page_idx, uint32_t child_page_idx);

void* get_page(DbPager* db_pager, uint32_t page_idx);
uint32_t get_unused_page_num(DbPager* db_pager);
uint32_t get_node_max_key(DbPager* db_pager, void* node);

TableCursor* table_start(DbTable* table);
TableCursor* table_find(DbTable* table, uint32_t key);
TableCursor* leaf_node_find(DbTable* table, uint32_t page_idx, uint32_t key);
TableCursor* internal_node_find(DbTable* table, uint32_t page_idx, uint32_t key);
void* cursor_value(TableCursor* cursor);
void cursor_advance(TableCursor* cursor);

DbPager* pager_open(const char* db_filename);
void pager_flush(DbPager* db_pager, uint32_t page_idx);
DbTable* db_open(const char* db_filename);
void db_close(DbTable* table);

InputBuffer* new_input_buffer();
void read_input(InputBuffer* input_buffer);
void close_input_buffer(InputBuffer* input_buffer);
void print_prompt();
MetaCommandResult do_meta_command(InputBuffer* input_buffer, DbTable* table);
void print_constants();
void print_commands();
void indent(uint32_t level);
void print_tree(DbPager* db_pager, uint32_t page_idx, uint32_t indentation_level);

PrepareResult prepare_statement(InputBuffer* input_buffer, Statement* statement);
PrepareResult prepare_insert(InputBuffer* input_buffer, Statement* statement);
PrepareResult prepare_drop(InputBuffer* input_buffer, Statement* statement);
PrepareResult prepare_import(InputBuffer* input_buffer, Statement* statement);
PrepareResult prepare_export(InputBuffer* input_buffer, Statement* statement);
PrepareResult prepare_update(InputBuffer* input_buffer, Statement* statement);
ExecuteResult execute_statement(Statement* statement, DbTable* table);
ExecuteResult execute_insert(Statement* statement, DbTable* table);
ExecuteResult execute_select(Statement* statement, DbTable* table);
ExecuteResult execute_drop(Statement* statement, DbTable* table);
ExecuteResult execute_import(Statement* statement, DbTable* table);
ExecuteResult execute_export(Statement* statement, DbTable* table);
ExecuteResult execute_update(Statement* statement, DbTable* table);

void create_new_root(DbTable* table, uint32_t right_child_page_idx);
void leaf_node_remove_cell(void* node, uint32_t cell_idx);
uint32_t get_node_child_index(void* parent_node, uint32_t child_page_idx);
void merge_nodes(DbTable* table, uint32_t parent_page_idx, uint32_t node_page_idx, uint32_t sibling_page_idx);
void redistribute_cells(DbTable* table, uint32_t parent_page_idx, uint32_t node_page_idx, uint32_t sibling_page_idx);
void adjust_tree_after_delete(DbTable* table, uint32_t page_idx);
void handle_root_shrink(DbTable* table);

int main(int argc, char* argv[]) {
    if (argc < 2) {
        printf(ANSI_COLOR_RED "Must supply a database filename.\n" ANSI_COLOR_RESET);
        exit(EXIT_FAILURE);
    }

    char* db_filename = argv[1];
    DbTable* db_table = db_open(db_filename);

    printf(ANSI_COLOR_GREEN "Use .commands for help\n" ANSI_COLOR_RESET);

    InputBuffer* input_buffer = new_input_buffer();
    while (true) {
        print_prompt();
        read_input(input_buffer);

        if (input_buffer->buffer[0] == '.') {
            switch (do_meta_command(input_buffer, db_table)) {
                case META_COMMAND_SUCCESS:
                    continue;
                case META_COMMAND_UNRECOGNIZED_COMMAND:
                    printf(ANSI_COLOR_RED "Unrecognized command '%s'\n" ANSI_COLOR_RESET, input_buffer->buffer);
                    continue;
            }
        }

        Statement statement;
        switch (prepare_statement(input_buffer, &statement)) {
            case PREPARE_SUCCESS:
                break;
            case PREPARE_NEGATIVE_ID:
                printf(ANSI_COLOR_RED "ID must be positive.\n" ANSI_COLOR_RESET);
                continue;
            case PREPARE_STRING_TOO_LONG:
                printf(ANSI_COLOR_RED "String is too long.\n" ANSI_COLOR_RESET);
                continue;
            case PREPARE_SYNTAX_ERROR:
                printf(ANSI_COLOR_RED "Syntax Error. Could not parse statement.\n" ANSI_COLOR_RESET);
                continue;
            case PREPARE_UNRECOGNIZED_STATEMENT:
                printf(ANSI_COLOR_RED "Unrecognized keyword at start of '%s'.\n" ANSI_COLOR_RESET, input_buffer->buffer);
                continue;
        }

        switch (execute_statement(&statement, db_table)) {
            case EXECUTE_SUCCESS:
                printf(ANSI_COLOR_YELLOW "Executed.\n" ANSI_COLOR_RESET);
                break;
            case EXECUTE_DUPLICATE_KEY:
                printf(ANSI_COLOR_RED "Error: Duplicate key.\n" ANSI_COLOR_RESET);
                break;
            case EXECUTE_SILENT_ERROR:
                break;
        }
    }
}

void serialize_user_row(UserRow* source, void* destination) {
    memcpy(destination + ID_FIELD_OFFSET, &(source->id), ID_FIELD_SIZE);
    memcpy(destination + USERNAME_FIELD_OFFSET, &(source->username), USERNAME_FIELD_SIZE);
    memcpy(destination + EMAIL_FIELD_OFFSET, &(source->email), EMAIL_FIELD_SIZE);
}

void deserialize_user_row(void* source, UserRow* destination) {
    memcpy(&(destination->id), source + ID_FIELD_OFFSET, ID_FIELD_SIZE);
    memcpy(&(destination->username), source + USERNAME_FIELD_OFFSET, USERNAME_FIELD_SIZE);
    memcpy(&(destination->email), source + EMAIL_FIELD_OFFSET, EMAIL_FIELD_SIZE);
}

void print_user_row(UserRow* user) {
    printf("(%d, %s, %s)\n", user->id, user->username, user->email);
}

NodeType get_node_type(void* node) {
    uint8_t value = *((uint8_t*)(node + NODE_TYPE_OFFSET));
    return (NodeType)value;
}

void set_node_type(void* node, NodeType type) {
    uint8_t value = type;
    *((uint8_t*)(node + NODE_TYPE_OFFSET)) = value;
}

bool is_node_root(void* node) {
    uint8_t value = *((uint8_t*)(node + IS_ROOT_OFFSET));
    return (bool)value;
}

void set_node_root(void* node, bool is_root) {
    uint8_t value = is_root;
    *((uint8_t*)(node + IS_ROOT_OFFSET)) = value;
}

uint32_t* node_parent(void* node) {
    return node + PARENT_POINTER_OFFSET;
}

uint32_t* leaf_node_num_cells(void* node) {
    return node + LEAF_NODE_NUM_CELLS_OFFSET;
}

uint32_t* leaf_node_next_leaf(void* node) {
    return node + LEAF_NODE_NEXT_LEAF_OFFSET;
}

void* leaf_node_cell(void* node, uint32_t cell_idx) {
    return node + LEAF_NODE_HEADER_SIZE + cell_idx * LEAF_NODE_CELL_SIZE;
}

uint32_t* leaf_node_key(void* node, uint32_t cell_idx) {
    return leaf_node_cell(node, cell_idx);
}

void* leaf_node_value(void* node, uint32_t cell_idx) {
    return leaf_node_cell(node, cell_idx) + LEAF_NODE_KEY_SIZE;
}

void initialize_leaf_node(void* node) {
    set_node_type(node, NODE_LEAF);
    set_node_root(node, false);
    *leaf_node_num_cells(node) = 0;
    *leaf_node_next_leaf(node) = 0;
    *node_parent(node) = 0;
}

void leaf_node_split_and_insert(TableCursor* cursor, uint32_t key, UserRow* value) {
    void* old_node = get_page(cursor->table->db_pager, cursor->page_idx);
    uint32_t old_max_key = get_node_max_key(cursor->table->db_pager, old_node);
    uint32_t new_page_idx = get_unused_page_num(cursor->table->db_pager);
    void* new_node = get_page(cursor->table->db_pager, new_page_idx);
    initialize_leaf_node(new_node);
    *node_parent(new_node) = *node_parent(old_node);
    *leaf_node_next_leaf(new_node) = *leaf_node_next_leaf(old_node);
    *leaf_node_next_leaf(old_node) = new_page_idx;

    for (int32_t i = LEAF_NODE_MAX_CELLS; i >= 0; i--) {
        void* destination_node;
        if (i >= LEAF_NODE_LEFT_SPLIT_COUNT)
            destination_node = new_node;
        else
            destination_node = old_node;

        uint32_t index_within_node = i % LEAF_NODE_LEFT_SPLIT_COUNT;
        void* destination = leaf_node_cell(destination_node, index_within_node);
        if (i == cursor->cell_idx) {
            serialize_user_row(value, leaf_node_value(destination_node, index_within_node));
            *leaf_node_key(destination_node, index_within_node) = key;
        }
        else if (i > cursor->cell_idx)
            memcpy(destination, leaf_node_cell(old_node, i - 1), LEAF_NODE_CELL_SIZE);
        else
            memcpy(destination, leaf_node_cell(old_node, i), LEAF_NODE_CELL_SIZE);
    }

    *(leaf_node_num_cells(old_node)) = LEAF_NODE_LEFT_SPLIT_COUNT;
    *(leaf_node_num_cells(new_node)) = LEAF_NODE_RIGHT_SPLIT_COUNT;
    if (is_node_root(old_node))
        return create_new_root(cursor->table, new_page_idx);
    else {
        uint32_t parent_page_idx = *node_parent(old_node);
        uint32_t new_max_key = get_node_max_key(cursor->table->db_pager, old_node);
        void* parent = get_page(cursor->table->db_pager, parent_page_idx);

        update_internal_node_key(parent, old_max_key, new_max_key);
        internal_node_insert(cursor->table, parent_page_idx, new_page_idx);
        return;
    }
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

uint32_t* internal_node_num_keys(void* node) {
    return node + INTERNAL_NODE_NUM_KEYS_OFFSET;
}

uint32_t* internal_node_right_child(void* node) {
    return node + INTERNAL_NODE_RIGHT_CHILD_OFFSET;
}

uint32_t* internal_node_cell(void* node, uint32_t cell_idx) {
    return node + INTERNAL_NODE_HEADER_SIZE + cell_idx * INTERNAL_NODE_CELL_SIZE;
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
    return (void*)internal_node_cell(node, key_num) + INTERNAL_NODE_CHILD_SIZE;
}

void initialize_internal_node(void* node) {
    set_node_type(node, NODE_INTERNAL);
    set_node_root(node, false);
    *internal_node_num_keys(node) = 0;
    *internal_node_right_child(node) = INVALID_PAGE_IDX;
    *node_parent(node) = 0;
}

void update_internal_node_key(void* node, uint32_t old_key, uint32_t new_key) {
    uint32_t old_child_index = internal_node_find_child(node, old_key);
    *internal_node_key(node, old_child_index) = new_key;
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

void internal_node_split_and_insert(DbTable* table, uint32_t parent_page_idx, uint32_t child_page_idx) {
    uint32_t old_page_idx = parent_page_idx;
    void* old_node = get_page(table->db_pager,parent_page_idx);
    uint32_t old_max_key = get_node_max_key(table->db_pager, old_node);
    void* child = get_page(table->db_pager, child_page_idx); 
    uint32_t child_max_key = get_node_max_key(table->db_pager, child);
    uint32_t new_page_idx = get_unused_page_num(table->db_pager);
    uint32_t splitting_root = is_node_root(old_node);

    void* parent;
    void* new_node;
    if (splitting_root) {
        create_new_root(table, new_page_idx);
        parent = get_page(table->db_pager,table->root_page_idx);
        old_page_idx = *internal_node_child(parent,0);
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
    for (int i = INTERNAL_NODE_MAX_KEYS - 1; i > INTERNAL_NODE_MAX_KEYS / 2; i--) {
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

void* get_page(DbPager* db_pager, uint32_t page_idx) {
    if (page_idx > MAX_PAGES) {
        printf(ANSI_COLOR_RED "Tried to fetch page number out of bounds. %d > %d\n" ANSI_COLOR_RESET, page_idx, MAX_PAGES);
        exit(EXIT_FAILURE);
    }

    if (db_pager->pages[page_idx] == NULL) {
        void* page = malloc(PAGE_SIZE_BYTES);
        uint32_t num_pages = db_pager->file_length / PAGE_SIZE_BYTES;
        if (db_pager->file_length % PAGE_SIZE_BYTES)
            num_pages++;

        if (page_idx <= num_pages) {
            lseek(db_pager->file_descriptor, page_idx * PAGE_SIZE_BYTES, SEEK_SET);
            ssize_t bytes_read = read(db_pager->file_descriptor, page, PAGE_SIZE_BYTES);
            if (bytes_read == -1) {
                printf(ANSI_COLOR_RED "Error reading file: %d\n" ANSI_COLOR_RESET, errno);
                exit(EXIT_FAILURE);
            }
        }

        db_pager->pages[page_idx] = page;

        if (page_idx >= db_pager->num_pages)
            db_pager->num_pages = page_idx + 1;
    }

    return db_pager->pages[page_idx];
}

uint32_t get_unused_page_num(DbPager* db_pager) {
    return db_pager->num_pages;
}

uint32_t get_node_max_key(DbPager* db_pager, void* node) {
    if (get_node_type(node) == NODE_LEAF)
        return *leaf_node_key(node, *leaf_node_num_cells(node) - 1);
    void* right_child = get_page(db_pager, *internal_node_right_child(node));

    return get_node_max_key(db_pager, right_child);
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

DbPager* pager_open(const char* db_filename) {
    int fd = open(db_filename,
                    O_RDWR |      // Read/Write mode
                        O_CREAT,  // Create file if it does not exist
                    S_IWUSR |     // User write permission
                        S_IRUSR   // User read permission
                    );
    if (fd == -1) {
        printf(ANSI_COLOR_RED "Unable to open file\n" ANSI_COLOR_RESET);
        exit(EXIT_FAILURE);
    }

    off_t file_length = lseek(fd, 0, SEEK_END);
    DbPager* db_pager = malloc(sizeof(DbPager));
    db_pager->file_descriptor = fd;
    db_pager->file_length = file_length;
    db_pager->num_pages = (file_length / PAGE_SIZE_BYTES);
    if (file_length % PAGE_SIZE_BYTES != 0) {
        printf(ANSI_COLOR_RED "Db file is not a whole number of pages. Corrupt file.\n" ANSI_COLOR_RESET);
        exit(EXIT_FAILURE);
    }

    for (uint32_t i = 0; i < MAX_PAGES; i++)
        db_pager->pages[i] = NULL;
    return db_pager;
}

void pager_flush(DbPager* db_pager, uint32_t page_idx) {
    if (db_pager->pages[page_idx] == NULL) {
        printf(ANSI_COLOR_RED "Tried to flush null page\n" ANSI_COLOR_RESET);
        exit(EXIT_FAILURE);
    }

    off_t offset = lseek(db_pager->file_descriptor, page_idx * PAGE_SIZE_BYTES, SEEK_SET);
    if (offset == -1) {
        printf(ANSI_COLOR_RED "Error seeking: %d\n" ANSI_COLOR_RESET, errno);
        exit(EXIT_FAILURE);
    }

    ssize_t bytes_written = write(db_pager->file_descriptor, db_pager->pages[page_idx], PAGE_SIZE_BYTES);
    if (bytes_written == -1) {
        printf(ANSI_COLOR_RED "Error writing: %d\n" ANSI_COLOR_RESET, errno);
        exit(EXIT_FAILURE);
    }
}

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
    for (uint32_t i = 0; i < db_pager->num_pages; i++) {
        if (db_pager->pages[i] == NULL)
            continue;
        pager_flush(db_pager, i);
        free(db_pager->pages[i]);
        db_pager->pages[i] = NULL;
    }

    off_t expected_size = db_pager->num_pages * PAGE_SIZE_BYTES;
    if (ftruncate(db_pager->file_descriptor, expected_size) != 0) {
        printf(ANSI_COLOR_RED "Error truncating db file.\n" ANSI_COLOR_RESET);
        exit(EXIT_FAILURE);
    }

    int result = close(db_pager->file_descriptor);
    if (result == -1) {
        printf(ANSI_COLOR_RED "Error closing db file.\n" ANSI_COLOR_RESET);
        exit(EXIT_FAILURE);
    }

    free(db_pager);
    free(table);
}

InputBuffer* new_input_buffer() {
    InputBuffer* input_buffer = malloc(sizeof(InputBuffer));
    input_buffer->buffer = NULL;
    input_buffer->buffer_length = 0;
    input_buffer->input_length = 0;

    return input_buffer;
}

void read_input(InputBuffer* input_buffer) {
    ssize_t bytes_read = getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);
    if (bytes_read <= 0) {
        printf(ANSI_COLOR_RED "Error reading input\n" ANSI_COLOR_RESET);
        exit(EXIT_FAILURE);
    }

    input_buffer->input_length = bytes_read - 1;
    input_buffer->buffer[bytes_read - 1] = 0;
}

void close_input_buffer(InputBuffer* input_buffer) {
    free(input_buffer->buffer);
    free(input_buffer);
}

void print_prompt() {
    printf(ANSI_COLOR_GREEN "db > " ANSI_COLOR_RESET);
    fflush(stdout);
}

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
    printf("drop {id}\n");
    printf("update {id} set {param}={value}\n");
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
            fprintf(stderr, ANSI_COLOR_RED "Error parsing line %d: Malformed CSV\n" ANSI_COLOR_RESET, line_num);
            fail_count++;
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
        for (int i = 0; i < *internal_node_num_keys(left_child); i++) {
            child = get_page(table->db_pager, *internal_node_child(left_child,i));
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