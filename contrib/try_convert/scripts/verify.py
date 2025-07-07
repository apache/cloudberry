#####################################################
#                                                   #
#   Verifies no ereport called while convertation   #
#                                                   #
#####################################################

# Need also to verify context forwarding

import re
import os, glob

DEBUG_FLAG = True
PRINT_ALL_FLAG = False

from general import source_filenames
from general import supported_types

def filter_supported_casts(casts):
    supported_casts = []
    for cast in casts:
        if cast[0] in supported_types and cast[1] in supported_types:
            supported_casts += [cast]
    return supported_casts

# Get casts from pg_cast

from find_casts import get_pg_cast, get_pg_proc, get_pg_type

func_id_name = get_pg_proc()
type_name_id, type_id_name, type_io_funcs = get_pg_type()

pg_casts = filter_supported_casts(get_pg_cast(type_id_name, func_id_name))

# Get casts from extensions

from find_casts import get_extensions


extension_casts, extension_sql_functions = get_extensions()

casts = pg_casts + extension_casts


if DEBUG_FLAG:
    print(f'FOUND {len(casts)} CASTs')
    if PRINT_ALL_FLAG:
        for l in sorted(casts):
            print(l)


#
# Get functions used to cast
#

# Get INOUT functions

required_in_funcs = {}
required_out_funcs = {}

for cast in casts:
    if cast[2] == 'WITH INOUT':
        required_out_funcs[cast[0]] = True
        required_in_funcs[cast[1]] = True

if DEBUG_FLAG:
    print(f'REQUIRED {len(required_in_funcs)} _IN FUNCTIONS')
    if PRINT_ALL_FLAG:
        for f in sorted(required_in_funcs):
            print(f)

if DEBUG_FLAG:
    print(f'REQUIRED {len(required_out_funcs)} _OUT FUNCTIONS')
    if PRINT_ALL_FLAG:
        for f in sorted(required_out_funcs):
            print(f)


# Get functions from CREATE CAST

required_funcs = {}

for cast in pg_casts:
    if cast[2] != 'WITH INOUT' and cast[2] != 'WITHOUT FUNCTION':
        required_funcs[cast[2]] = cast


from find_casts import find_create_function_in_text

sql_funcs = extension_sql_functions

# print(sql_funcs) 

for cast in extension_casts:
    if cast[2] != 'WITH INOUT' and cast[2] != 'WITHOUT FUNCTION':
        sql_func_name = cast[2]

        m = re.match('(\w+)\(', sql_func_name)
        if m is not None:
            sql_func_name = m[1]

        c_func = "Not found"

        if sql_func_name in sql_funcs:
            c_func = sql_funcs[sql_func_name]

        # print(sql_func_name, c_func)

        required_funcs[c_func] = sql_func_name

if DEBUG_FLAG:
    print(f'REQUIRED {len(required_funcs)} FUNCTIONS FROM CREATE CAST')
    if PRINT_ALL_FLAG:
        for f in sorted(required_funcs):
            print(f)

required_funcs_list = list(required_funcs) + list(required_in_funcs) + list(required_out_funcs)

for l in type_io_funcs:
    if l in supported_types:
        required_funcs_list += type_io_funcs[l]

#
# Load functions bodies
#

# load convert function

from find_calls import find_functions

convert_functions = []

for root, subdirs, files in os.walk('../..'):
    for filename in files:
        file_path = os.path.join(root, filename)
        if filename[-2:] == '.c':
        # if filename in source_filenames:
            # print(file_path)
            with open(file_path, 'r') as f:
                content = f.read()

                funcs = find_functions(required_funcs_list, content)

                convert_functions += funcs

                # print(file_path, len(funcs))

loaded_convert_functions = {}

from find_calls import remove_comments

for name, return_type, args, body in convert_functions:
    if return_type == 'Datum' and args == 'PG_FUNCTION_ARGS':
        body = remove_comments(body)
        loaded_convert_functions[name] = body

for rf in required_funcs:
    if rf not in loaded_convert_functions:
        print(f'body for {rf}({required_funcs[rf]}) not found')


if DEBUG_FLAG:
    print(f'FOUND {len(loaded_convert_functions)} FUNCTIONS BODIES')

# load safe functions

from find_calls import find_safe_functions

safe_functions = []

null_functions = []

for root, subdirs, files in os.walk('../..'):
    for filename in files:
        file_path = os.path.join(root, filename)
        if filename[-2:] == '.c':
        # if filename in source_filenames:
            # print(file_path)
            with open(file_path, 'r') as f:
                content = f.read()

                funcs = find_safe_functions(content)

                safe_functions += funcs


loaded_safe_functions = {}
loaded_null_functions = {}

for name, return_type, args, body in safe_functions:
    loaded_safe_functions[name] = remove_comments(body)

    if re.search(r'bool', return_type) is None:
        print(f'    WARNING: safe function {name} returns result not bool')
    
    m = re.match('(\w+)Safe', name)
    if m is not None:
        loaded_null_functions[m[1]] = 1

    m = re.match('(\w+)_safe', name)
    if m is not None:
        loaded_null_functions[m[1]] = 1

if DEBUG_FLAG:
    print(f'FOUND {len(loaded_safe_functions)} SAFE FUNCTIONS BODIES')

if DEBUG_FLAG:
    print(f'REQUIRED {len(loaded_null_functions)} SAFE FUNCTIONS VARIANTS')

loaded_functions = dict(list(loaded_convert_functions.items()) + list(loaded_safe_functions.items()))

#
# Check functions don't call unsafe ereport
#

from find_calls import get_all_functions_with

ereport_functions = get_all_functions_with('ereport\(ERROR,')

unsafe_convert_functions = {}

for func_name in loaded_functions:
    body = loaded_functions[func_name]

    pattern_call = '(\w+)\s*\(([\s\S]*?)\)'

    for token_match in re.finditer(pattern_call, body):

        token = token_match[1]

        args = token_match[2]

        # if (token == 'ereport'):
        #     print(token, args[:5])
        
        if token in ereport_functions or ((token == 'ereport' or token == 'elog') and args[:5] == 'ERROR'):
            print(f'    WARNING: call unsafe function {token} in {func_name}')
            unsafe_convert_functions[func_name] = token
            continue

if DEBUG_FLAG:
    print(f'FOUND {len(unsafe_convert_functions)} UNSAFE CONVERT FUNCTIONS')


#
# Check context forwarding don't call unsafe variants
#

unsafe_variants = set(loaded_null_functions.keys())

unsafe_variant_usage = {}
unsafe_variant_usage_count = 0
unwrapped_safe_usage = {}
unwrapped_safe_usage_count = 0
wrong_wrap_usage = {}
wrong_wrap_usage_count = 0

for func_name in loaded_functions:
    body = loaded_functions[func_name]

    pattern_call = '\w+'

    for token_match in re.finditer(pattern_call, body):

        token = token_match[0]

        l = max(0, token_match.start() - 40)
        r = min(token_match.end() + 150, len(body))

        line = body[l:r]
        
        if token in unsafe_variants:
            print(f'    WARNING: call unsafe variant of function {token} in {func_name}')
            unsafe_variant_usage[func_name] = token
            unsafe_variant_usage_count += 1
            continue

        if token == 'ereturn' and re.search(r'ereturn\(fcinfo', line):
            print(f'    WARNING: ereturn cannot be run at fcinfo context in {func_name}, use PG_ERETURN')

        if token == 'return' and func_name in loaded_safe_functions:
            m = re.match(r'return\s*([\s\S]*?);', body[token_match.start():r])
            if m[1] != 'true':
                print(f'    WARNING: in safe function {func_name}: "return" used only with "true", not "{m[1]}"')


        if token in loaded_functions:

            # if_wrapper_pattern = rf'if \(!{token}\([\s\S]+?, (?:escontext|fcinfo->context)\)\)'
            void_wrapper_pattern = rf'(void) {token}\([\s\S]+?, NULL\);'
            safe_call_wrapper_pattern = rf'safe_call\({token}, \([\s\S]+?, (?:escontext|fcinfo->context)\)\);'
            safe_call_with_free_wrapper_pattern = rf'safe_call_with_free\({token}, \([\s\S]+?, (?:escontext|fcinfo->context)\), \{{[\s\S]+?\}}\);'
            pg_safe_call_wrapper_pattern = rf'PG_SAFE_CALL\({token}, \([\s\S]+?, (?:escontext|fcinfo->context)\)\);'
            return_wrapper_pattern = rf'return {token}\([\s\S]+?, (?:escontext|fcinfo->context)\);'
            direct_call_wrapper_pattern = rf'DirectFunctionCall1Safe\({token}, [\s\S]+?, (?:escontext|fcinfo->context)\);'

            def unite_patterns(l):
                return '|'.join(l)

            search_pattern = unite_patterns([
                # if_wrapper_pattern, 
                safe_call_wrapper_pattern,
                safe_call_with_free_wrapper_pattern,
                pg_safe_call_wrapper_pattern,
                return_wrapper_pattern, 
                direct_call_wrapper_pattern
            ])

            if not re.search(search_pattern, line):
                # print(func_name, token_match, line)
                print(f'    WARNING: call unwrapped safe function {token} in {func_name}')
                unwrapped_safe_usage[func_name] = token
                unwrapped_safe_usage_count += 1
                continue

            pg_sc_patterns = unite_patterns([
                pg_safe_call_wrapper_pattern, 
                direct_call_wrapper_pattern,
            ])
            if func_name in loaded_convert_functions and not re.search(pg_sc_patterns, line):
                print(f'    WARNING: wrong wrapped safe function {token} in PG_FUNCTION {func_name}')
                wrong_wrap_usage[func_name] = token
                wrong_wrap_usage_count += 1
                continue

            sc_patterns = unite_patterns([
                safe_call_wrapper_pattern, 
                safe_call_with_free_wrapper_pattern,
                return_wrapper_pattern,
            ])
            if func_name in loaded_safe_functions and not re.search(sc_patterns, line):
                print(f'    WARNING: wrong wrapped safe function {token} in SAFE {func_name}')
                wrong_wrap_usage[func_name] = token
                wrong_wrap_usage_count += 1
                continue



if DEBUG_FLAG:
    print(f'FOUND {unsafe_variant_usage_count} UNSAFE VARIANT USAGES')

if DEBUG_FLAG:
    print(f'FOUND {unwrapped_safe_usage_count} UNWRAPPED SAFE FUNCTION USAGES')

if DEBUG_FLAG:
    print(f'FOUND {wrong_wrap_usage_count} WRONG WRAP USAGES')


# print(loaded_functions['date_in'])
# print(loaded_functions['timestamptz_interval_bound'])
