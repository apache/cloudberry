#pragma once

#include <string>

extern "C" {
#include "postgres.h"
#include "fmgr.h"
}

extern "C" {
/* CREATE TABLE yagpcc.__log (...); */
void init_log();

/* TRUNCATE yagpcc.__log */
void truncate_log();
}

/* INSERT INTO yagpcc.__log VALUES (...) */
void insert_log(const yagpcc::SetQueryReq &req);
