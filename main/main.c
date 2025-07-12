#include <stdio.h>
#include <stdlib.h>

#include "input.h"
#include "meta_command.h"
#include "statement.h"
#include "execution.h"
#include "table.h"
#include "common.h"

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