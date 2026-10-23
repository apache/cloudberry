# Copyright (c) 2022-2023, PostgreSQL Global Development Group

main_file = files('main.c')

# main.c is deliberately NOT added to backend_sources. src/backend/Makefile
# filters main/main.o out of libpostgres.so and links it into the postgres
# executable instead, so it is added directly to that target.
