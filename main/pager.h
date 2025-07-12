#ifndef DB_PAGER_H
#define DB_PAGER_H

#include <fcntl.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <unistd.h>
#include "common.h"

DbPager*  pager_open(const char* db_filename);
void      pager_flush(DbPager* pager, uint32_t page_idx);
void*     get_page(DbPager* pager, uint32_t page_idx);
uint32_t  get_unused_page_num(DbPager* pager);

#endif