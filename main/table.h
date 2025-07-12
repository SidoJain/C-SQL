#ifndef DB_TABLE_H
#define DB_TABLE_H

#include <unistd.h>
#include "common.h"
#include "pager.h"
#include "row.h"
#include "node.h"

DbTable*     db_open(const char* filename);
void         db_close(DbTable* table);

TableCursor* table_start(DbTable* table);
TableCursor* table_find(DbTable* table, uint32_t key);
void*        cursor_value(TableCursor* cursor);
void         cursor_advance(TableCursor* cursor);

#endif