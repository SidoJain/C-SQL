#include "pager.h"

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
    if (page_idx >= MAX_PAGES) {
        fprintf(stderr, "Tried to flush page number out of bounds: %d\n", page_idx);
        exit(1);
    }

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

void* get_page(DbPager* db_pager, uint32_t page_idx) {
    if (page_idx >= MAX_PAGES) {
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