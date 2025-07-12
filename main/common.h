#ifndef DB_COMMON_H
#define DB_COMMON_H

#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>

#define size_of_attribute(Struct, Attribute) (sizeof(((Struct*)0)->Attribute))

#define USERNAME_MAX_LENGTH     32
#define EMAIL_MAX_LENGTH        255
#define FILENAME_MAX_LENGTH     255

#define ID_FIELD_OFFSET         0
#define USERNAME_FIELD_OFFSET   (ID_FIELD_OFFSET + sizeof(uint32_t))
#define EMAIL_FIELD_OFFSET      (USERNAME_FIELD_OFFSET + USERNAME_MAX_LENGTH + 1)
#define USER_ROW_SIZE           (sizeof(uint32_t) + USERNAME_MAX_LENGTH + 1 + EMAIL_MAX_LENGTH + 1)

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
#define INTERNAL_NODE_MIN_KEYS              (INTERNAL_NODE_MAX_KEYS / 2)

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
    int       file_descriptor;
    uint32_t  file_length;
    uint32_t  num_pages;
    void*     pages[MAX_PAGES];
} DbPager;

typedef struct {
    DbPager* db_pager;
    uint32_t root_page_idx;
} DbTable;

typedef struct {
    DbTable* table;
    uint32_t page_idx;
    uint32_t cell_idx;
    bool     end_of_table;
} TableCursor;

#endif