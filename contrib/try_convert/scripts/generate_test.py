import re

from general import supported_types, string_types, typmod_types, typmod_lens

def get_typemod_type(t, l):
    if l is None:
        return t
    else:
        return f'{t}({l})'

def get_typemod_table(t, l):
    if l is None:
        return f'tt_{t}'
    else:
        return f'tt_{t}_{l}'
    

from general import uncomparable_types, has_corrupt_data

from general import supported_extensions


print('Supported types:', ' '.join(supported_types))


def remove_empty_lines(t):
    return "\n".join([s for s in t.split("\n") if s])

### GET FUNCTION IDs

from find_casts import get_pg_proc

func_id_name = get_pg_proc()


### GET TYPE IDs

from find_casts import get_pg_type

type_name_id, type_id_name, _ = get_pg_type()

supported_types_count = 0

print(f'Types found: {len(type_id_name)}, supported: {supported_types_count}')


### GET CONVERTS

from find_casts import get_pg_cast

casts = get_pg_cast(type_id_name, func_id_name)

supported_cast_count = 0

print(f'Casts found: {len(casts)}, supported: {supported_cast_count}')


### HEADER & FOOTER

test_header = \
    f'-- SCRIPT-GENERATED TEST for TRY_CONVERT\n' \
    f'-- Tests {supported_types_count} types of {len(type_id_name)} from pg_types.h\n' \
    f'-- Tests {supported_cast_count} cast of {len(casts)} from pg_cast.h\n' \
    f'create schema tryconvert;\n' \
    f'set search_path = tryconvert;\n' \
    f'-- start_ignore\n' \
    f'CREATE EXTENSION IF NOT EXISTS try_convert;\n' \
    f'-- end_ignore\n'
    
for extension in supported_extensions:
    test_header += \
        f'-- start_ignore\n' \
        f'CREATE EXTENSION IF NOT EXISTS {extension};\n' \
        f'-- end_ignore\n'
        
test_header_out = test_header

for type_name in supported_types:
    add = \
        f'select add_type_for_try_convert(\'{type_name}\'::regtype);\n'
    
    out = \
        ' add_type_for_try_convert \n' \
        '--------------------------\n' \
        ' \n' \
        '(1 row)\n' \
        '\n' 
    
    test_header += add

    test_header_out += add + out

test_header_out = test_header_out[:-1]

test_footer = 'reset search_path;'


### TRY_CONVERT_BY_SQL

test_funcs = ''

func_text = \
    f'CREATE FUNCTION try_convert_by_sql_text(_in text, INOUT _out ANYELEMENT, source_type text)\n' \
    f'  LANGUAGE plpgsql AS\n' \
    f'$func$\n' \
    f'    BEGIN\n' \
    f'        EXECUTE format(\'SELECT %L::%s::%s\', $1, source_type, pg_typeof(_out))\n' \
    f'        INTO  _out;\n' \
    f'        EXCEPTION WHEN others THEN\n' \
    f'        -- do nothing: _out already carries default\n' \
    f'    END\n' \
    f'$func$;\n'

test_funcs += func_text

func_text = \
    f'CREATE FUNCTION try_convert_by_sql_text_with_len_out(_in text, INOUT _out ANYELEMENT, source_type text, len_out int)\n' \
    f'  LANGUAGE plpgsql AS\n' \
    f'$func$\n' \
    f'    BEGIN\n' \
    f'        EXECUTE format(\'SELECT %L::%s::%s(%s)\', $1, source_type, pg_typeof(_out), len_out::text)\n' \
    f'        INTO  _out;\n' \
    f'        EXCEPTION WHEN others THEN\n' \
    f'        -- do nothing: _out already carries default\n' \
    f'    END\n' \
    f'$func$;\n'

test_funcs += func_text

for type_name in supported_types:

    func_text = \
        f'CREATE FUNCTION try_convert_by_sql_with_len_out(_in {type_name}, INOUT _out ANYELEMENT, len_out int)\n' \
        f'  LANGUAGE plpgsql AS\n' \
        f'$func$\n' \
        f'    BEGIN\n' \
        f'        EXECUTE format(\'SELECT %L::{type_name}::%s(%s)\', $1, pg_typeof(_out), len_out::text)\n' \
        f'        INTO  _out;\n' \
        f'        EXCEPTION WHEN others THEN\n' \
        f'        -- do nothing: _out already carries default\n' \
        f'    END\n' \
        f'$func$;\n'

    test_funcs += func_text

    func_text = \
        f'CREATE FUNCTION try_convert_by_sql(_in {type_name}, INOUT _out ANYELEMENT)\n' \
        f'  LANGUAGE plpgsql AS\n' \
        f'$func$\n' \
        f'    BEGIN\n' \
        f'        EXECUTE format(\'SELECT %L::{type_name}::%s\', $1, pg_typeof(_out))\n' \
        f'        INTO  _out;\n' \
        f'        EXCEPTION WHEN others THEN\n' \
        f'        -- do nothing: _out already carries default\n' \
        f'    END\n' \
        f'$func$;\n'

    test_funcs += func_text

for source_type in supported_types:
    for target_type in supported_types:
        for source_typmod in typmod_lens:
            if source_type not in typmod_types and source_typmod is not None:
                continue
            for target_typmod in typmod_lens:
                if target_type not in typmod_types and target_typmod is not None:
                    continue

                source_name = source_type
                if source_typmod is not None:
                    source_name += f'({source_typmod})'
                
                target_name = target_type
                if target_typmod is not None:
                    target_name += f'({target_typmod})'

        
                func_text = \
                    f'CREATE FUNCTION try_convert_by_exception_{source_typmod}_{target_typmod}(_in {source_name}, d {target_name}) RETURNS {target_name}\n' \
                    f'  LANGUAGE plpgsql AS\n' \
                    f'$func$\n' \
                    f'    BEGIN\n' \
                    f'        RETURN CAST(_in AS {target_name});\n' \
                    f'        EXCEPTION WHEN others THEN\n' \
                    f'        RETURN d;\n' \
                    f'    END\n' \
                    f'$func$;\n'

                test_funcs += func_text

### CREATE DATA

test_load_data = '-- LOAD DATA\n'

test_load_data += f'CREATE TABLE tt_temp (v text) DISTRIBUTED BY (v);\n'

def copy_data(table_name, filename, type_name):
    return  f'DELETE FROM tt_temp;\n' \
            f'COPY tt_temp from \'@abs_srcdir@/{filename}\';\n' \
            f'INSERT INTO {table_name}(id, v) SELECT row_number() OVER(), v::{type_name} from tt_temp;'  

type_tables = {}

def create_table(type_name, varlen=None):
    table_name = get_typemod_table(type_name, varlen)
    field_type = get_typemod_type(type_name, varlen)

    type_tables[type_name] = table_name

    load_data = f'CREATE TABLE {table_name} (id serial, v {field_type}) DISTRIBUTED BY (id);\n'

    filename = f'data/tt_{type_name}.data'

    load_data += copy_data(table_name, filename, field_type) + '\n'

    # load_data += f'SELECT * FROM {table_name};'

    return load_data

def get_string_table(type_name, string_type, type_varlen=None, string_varlen=None):

    if type_varlen is not None and string_varlen is not None:
        return f'tt_{string_type}_{string_varlen}_of_{type_name}_{type_varlen}'
    elif type_varlen is not None:
        return f'tt_{string_type}_of_{type_name}_{type_varlen}'
    elif string_varlen is not None:
        return f'tt_{string_type}_{string_varlen}_of_{type_name}'
    
    return f'tt_{string_type}_of_{type_name}'

for type_name in supported_types:

    for type_varlen in typmod_lens:
        if type_varlen is not None and type_name not in typmod_types:
            continue

        test_load_data += create_table(type_name, type_varlen)
        
        for string_type in string_types:
            for string_varlen in typmod_lens:
                if string_varlen is not None and string_type not in typmod_types:
                    continue

                field_type = get_typemod_type(type_name, type_varlen)
                string_field_type = get_typemod_type(string_type, string_varlen)

                table_name = get_string_table(type_name, string_type, type_varlen, string_varlen)
                
                load_data =  f'CREATE TABLE {table_name} (id serial, v {string_field_type}) DISTRIBUTED BY (id);\n'

                cut = f'::{field_type}' if type_varlen is not None else ''

                load_data += f'INSERT INTO {table_name}(id, v) SELECT row_number() OVER(), v{cut}::{string_field_type} from tt_temp;\n' 

                test_load_data += load_data

        if type_name in has_corrupt_data:

            for string_type in string_types:
                for string_varlen in typmod_lens:
                    if string_varlen is not None and string_type not in typmod_types:
                        continue

                    field_type = get_typemod_type(type_name, type_varlen)
                    string_field_type = get_typemod_type(string_type, string_varlen)

                    corr_table_name = 'corr_' + get_string_table(type_name, string_type, type_varlen, string_varlen)

                    load_data =  f'CREATE TABLE {corr_table_name} (id serial, v {string_field_type}) DISTRIBUTED BY (id);\n'

                    filename = f'data/corr_{type_name}.data'

                    load_data += copy_data(corr_table_name, filename, string_field_type) + '\n'

                    test_load_data += load_data
                    
            


## GET DATA

def get_data(type_name):
    return type_tables[type_name]

def get_len_from_data(type_name):
    f = open(f'data/tt_{type_name}.data')
    return(len(f.read().split('\n')))

def get_len_from_corr_data(type_name):
    f = open(f'data/corr_{type_name}.data')
    return(len(f.read().split('\n')))

def get_from_data(type_name, i = None):
    f = open(f'data/tt_{type_name}.data')
    values = f.read().split('\n')
    if i is None:
        return content
    return values[i]

## TEST

def create_test(source_name, target_name, test_data, default='NULL', source_varlen=None, target_varlen=None, source_count=0):

    test_filter = 'v1 is distinct from v2' if target_name not in uncomparable_types else 'v1::text is distinct from  v2::text'
    test_filter_not = 'v1 is not distinct from v2' if target_name not in uncomparable_types else 'v1::text is not distinct from  v2::text'

    target_name_1 = get_typemod_type(target_name, target_varlen) 

    try_convert_sql = f'try_convert_by_exception_{source_varlen}_{target_varlen}(v, {default}::{target_name_1})'

    query = \
        f'select * from (' \
            f'select ' \
                f'try_convert(v, {default}::{target_name_1}) as v1, ' \
                f'{try_convert_sql} as v2' \
                f', v' \
            f' from {test_data}' \
    f') as t(v1, v2, v) where {test_filter};'
    result = \
        ' v1 | v2 | v \n' \
        '----+----+---\n' \
        '(0 rows)\n'

    query_not = \
        f'select count(*) from (' \
            f'select ' \
                f'try_convert(v, {default}::{target_name_1}) as v1, ' \
                f'{try_convert_sql} as v2' \
            f' from {test_data}' \
    f') as t(v1, v2) where {test_filter_not};'
    result_not = \
        ' count \n' \
        '-------\n' \
        f'    {source_count}\n'   \
        '(1 row)\n'

    input_source = query + '\n' + query_not
    output_source = remove_empty_lines(query) + '\n' + result + '\n' + remove_empty_lines(query_not) + '\n' + result_not

    return input_source, output_source


### CAST to & from text

text_tests_in = []
text_tests_out = []

default_value = 'NULL'

for string_type in string_types:
    for string_varlen in typmod_lens:
        if string_varlen is not None and type_name not in typmod_types:
            continue

        for type_name in supported_types:
            for type_varlen in typmod_lens:
                if type_varlen is not None and type_name not in typmod_types:
                    continue

                test_type_table = get_typemod_table(type_name, type_varlen)

                text_type_table = get_string_table(type_name, string_type, type_varlen, string_varlen)

                test_corrupted_text_data = f'(select (\'!@#%^&*\' || v || \'!@#%^&*\') from {text_type_table}) as t(v)'

                data_count = get_len_from_data(type_name)

                to_text_in, to_text_out = create_test(
                    type_name, string_type, 
                    test_type_table, default_value, 
                    type_varlen, string_varlen, 
                    data_count
                )
                from_text_in, from_text_out = create_test(
                    string_type, type_name, 
                    text_type_table, default_value, 
                    string_varlen, type_varlen, 
                    data_count
                )
                from_corrupted_text_in, from_corrupted_text_out = create_test(
                    string_type, type_name, 
                    test_corrupted_text_data, default_value, 
                    string_varlen, type_varlen, 
                    data_count
                )

                text_tests_in += [to_text_in, from_text_in]
                text_tests_out += [to_text_out, from_text_out]

                text_tests_in += [from_corrupted_text_in]
                text_tests_out += [from_corrupted_text_out]

                data_count = get_len_from_data(type_name)

                if type_name in has_corrupt_data:

                    corr_text_type_table = 'corr_' + text_type_table

                    data_count = get_len_from_corr_data(type_name)

                    from_corr_in, from_corr_out = create_test(
                        string_type, type_name, 
                        corr_text_type_table, default_value, 
                        string_varlen, type_varlen, 
                        data_count
                    )

                    text_tests_in += [from_corr_in]
                    text_tests_out += [from_corr_out]


# print(text_tests_in[0])
# print(text_tests_in[1])


### CAST from pg_cast

function_tests_in = []
function_tests_out = []

type_casts = [(source_name, target_name) for (source_name, target_name, method) in casts]

for source_name, target_name in type_casts:
    if (source_name not in supported_types or target_name not in supported_types):
        continue

    dd = get_from_data(target_name, 0).translate(str.maketrans('', '', '\''))
    d = f'\'{dd}\''
    
    for default in ['NULL', d]:

        for source_varlen in typmod_lens:
            if source_varlen is not None and source_name not in typmod_types:
                continue

            test_table = get_typemod_table(source_name, source_varlen)

            for target_varlen in typmod_lens:
                if target_varlen is not None and target_name not in typmod_types:
                    continue

                data_count = get_len_from_data(source_name)
            
                test_in, test_out = create_test(
                    source_name, target_name, 
                    test_table, default, 
                    source_varlen, target_varlen,
                    data_count
                )

                function_tests_in += [test_in]
                function_tests_out += [test_out]
        

# print(function_tests_in[0])


### DEFAULTS TEST

# for type_name in supported_types:
    
#     query = f'SELECT try_convert({}::{}, {get_from_data(type_name, 0)}::{type_name});'

### ONE MILLION ERRORS

test_million = ''

test_million_data = \
    'DROP TABLE IF EXISTS text_ints; CREATE TABLE text_ints (v text) DISTRIBUTED BY (v);\n' \
    'INSERT INTO text_ints(v) SELECT (random()*1000)::int4::text FROM generate_series(1,1000000);\n' \
    'DROP TABLE IF EXISTS text_error_ints; CREATE TABLE text_error_ints (v text) DISTRIBUTED BY (v);\n' \
    'INSERT INTO text_error_ints(v) SELECT (random()*1000000 + 1000000)::int8::text FROM generate_series(1,1000000);\n' \
    'DROP TABLE IF EXISTS int4_ints; CREATE TABLE int4_ints (v int4) DISTRIBUTED BY (v);\n' \
    'INSERT INTO int4_ints(v) SELECT (random()*1000)::int4 FROM generate_series(1,1000000);\n' \
    'DROP TABLE IF EXISTS int4_error_ints; CREATE TABLE int4_error_ints (v int4) DISTRIBUTED BY (v);\n' \
    'INSERT INTO int4_error_ints(v) SELECT (random()*1000000 + 1000000)::int4 FROM generate_series(1,1000000);\n'

test_million_query1 = \
    'SELECT count(*) FROM (SELECT try_convert(v, NULL::int2) as v FROM text_ints) as t(v) WHERE v IS NOT NULL;\n'
test_million_query2 = \
    'SELECT count(*) FROM (SELECT try_convert(v, NULL::int2) as v FROM text_error_ints) as t(v) WHERE v IS NULL;\n'

test_million_query3 = \
    'SELECT count(*) FROM (SELECT try_convert(v, NULL::int2) as v FROM int4_ints) as t(v) WHERE v IS NOT NULL;\n'
test_million_query4 = \
    'SELECT count(*) FROM (SELECT try_convert(v, NULL::int2) as v FROM int4_error_ints) as t(v) WHERE v IS NULL;\n'

test_million_result = \
    '  count  \n' \
    '---------\n' \
    ' 1000000\n' \
    '(1 row)\n' \

test_million_in = test_million_data + test_million_query1 + test_million_query2 + test_million_query3 + test_million_query4
test_million_out = test_million_data + \
    test_million_query1 + test_million_result + '\n' + \
    test_million_query2 + test_million_result + '\n' + \
    test_million_query3 + test_million_result + '\n' + \
    test_million_query4 + test_million_result

### NESTED CASTS

value = '42::int4'

for level in range(100):
    value = f'try_convert(try_convert({value}, NULL::text), NULL::int4)'


test_nested_query = f'select {value} as v;\n'

test_nested_result = \
    ' v  \n' \
    '----\n' \
    ' 42\n' \
    '(1 row)\n' \

test_nested_in = test_nested_query
test_nested_out = test_nested_query + test_nested_result


### CONSTRUCT TEST

test_str = '\n'.join([
    test_header, \
    # FUNCTIONS
    test_funcs, \
    # CREATE DATA
    test_load_data, \
    '-- TEXT TESTS', \
    '\n'.join(text_tests_in), \
    '-- FUNCTION TESTS', \
    '\n'.join(function_tests_in), \
    '-- MILLION TESTS', \
    test_million_in,
    '-- NESTED TESTS', \
    test_nested_in,
    test_footer
    ]) + '\n'

test_f = open('input/try_convert.source', 'w')
test_f.write(test_str)


test_str = '\n'.join([
    test_header_out, \
    # FUNCTIONS
    remove_empty_lines(test_funcs), \
    # CREATE DATA
    remove_empty_lines(test_load_data), \
    '-- TEXT TESTS', \
    '\n'.join(text_tests_out), \
    '-- FUNCTION TESTS', \
    '\n'.join(function_tests_out), \
    '-- MILLION TESTS', \
    test_million_out,
    '-- NESTED TESTS', \
    test_nested_out,
    remove_empty_lines(test_footer)
    ]) + '\n'

test_f = open('output/try_convert.source', 'w')
test_f.write(test_str)
