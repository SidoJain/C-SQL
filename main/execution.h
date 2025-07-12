#ifndef DB_EXECUTION_H
#define DB_EXECUTION_H

#include "common.h"
#include "table.h"
#include "statement.h"
#include "node.h"

ExecuteResult execute_statement(Statement* statement, DbTable* table);
ExecuteResult execute_insert(Statement* statement, DbTable* table);
ExecuteResult execute_select(Statement* statement, DbTable* table);
ExecuteResult execute_drop(Statement* statement, DbTable* table);
ExecuteResult execute_import(Statement* statement, DbTable* table);
ExecuteResult execute_export(Statement* statement, DbTable* table);
ExecuteResult execute_update(Statement* statement, DbTable* table);

#endif