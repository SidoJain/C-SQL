#ifndef DB_NODE_H
#define DB_NODE_H

#include "common.h"
#include "table.h"
#include "pager.h"

NodeType     get_node_type(void* node);
void         set_node_type(void* node, NodeType type);
bool         is_node_root(void* node);
void         set_node_root(void* node, bool is_root);
uint32_t*    node_parent(void* node);
uint32_t     get_node_max_key(DbPager* pager, void* node);

uint32_t*    leaf_node_num_cells(void* node);
uint32_t*    leaf_node_next_leaf(void* node);
void*        leaf_node_cell(void* node, uint32_t cell_idx);
uint32_t*    leaf_node_key(void* node, uint32_t cell_idx);
void*        leaf_node_value(void* node, uint32_t cell_idx);
void         initialize_leaf_node(void* node);
void         leaf_node_insert(TableCursor* cursor, uint32_t key, UserRow* value);
void         leaf_node_split_and_insert(TableCursor* cursor, uint32_t key, UserRow* value);
TableCursor* leaf_node_find(DbTable* table, uint32_t page_idx, uint32_t key);

uint32_t*    internal_node_num_keys(void* node);
uint32_t*    internal_node_right_child(void* node);
uint32_t*    internal_node_cell(void* node, uint32_t cell_idx);
uint32_t*    internal_node_child(void* node, uint32_t child_num);
uint32_t*    internal_node_key(void* node, uint32_t key_num);
void         initialize_internal_node(void* node);
uint32_t     internal_node_find_child(void* node, uint32_t key);
void         update_internal_node_key(void* node, uint32_t old_key, uint32_t new_key);
void         internal_node_insert(DbTable* table, uint32_t parent_page_idx, uint32_t child_page_idx);
void         internal_node_split_and_insert(DbTable* table, uint32_t parent_page_idx, uint32_t child_page_idx);
TableCursor* internal_node_find(DbTable* table, uint32_t page_idx, uint32_t key);

void         create_new_root(DbTable* table, uint32_t right_child_page_idx);
void         leaf_node_remove_cell(void* node, uint32_t cell_idx);
uint32_t     get_node_child_index(void* parent_node, uint32_t child_page_idx);
void         merge_nodes(DbTable* table, uint32_t parent_page_idx, uint32_t node_page_idx, uint32_t sibling_page_idx);
void         redistribute_cells(DbTable* table, uint32_t parent_page_idx, uint32_t node_page_idx, uint32_t sibling_page_idx);
void         adjust_tree_after_delete(DbTable* table, uint32_t page_idx);
void         handle_root_shrink(DbTable* table);

#endif