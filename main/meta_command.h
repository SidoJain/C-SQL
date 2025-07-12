#ifndef DB_META_COMMAND_H
#define DB_META_COMMAND_H

#include "common.h"
#include "input.h"
#include "table.h"
#include "pager.h"

MetaCommandResult do_meta_command(InputBuffer* input_buffer, DbTable* table);

void print_constants();
void print_commands();
void indent(uint32_t level);
void print_tree(DbPager* pager, uint32_t page_idx, uint32_t indentation_level);

#endif