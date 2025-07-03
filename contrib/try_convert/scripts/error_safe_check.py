import re

import os, glob

p_any = '(?:\S| |\t|\n+\t|\n+ |\n+#|\n+/|\n+\$)+?'
p_spaces = '\s*'

def create_pattern(funcCall):
    return '\n(\w+)\s+?(\w+)' + p_spaces + '\((' + p_any + ')\)' + p_spaces + '(\n\{' + '(' + p_any + ')' + f'({funcCall})' + '\s*' + '(' + p_any + ')' + '\n+\})'

pattern = create_pattern('ereturn')


source_filenames = ['time.c', 'date.c', 'timedate.c', 'int.c', 'float.c', 'bool.c', 'char.c', 'formatting.c', 'json.c', 'jsonb.c', 'nabstime.c', 'numeric.c', 'timestamp.c', 'network.c']


# verify all convert and in&outs(if in converts) are 'soft'-error handling
# safe error handle == sub-calls are not 'ereport' (replaced by 'ereturn')


boolin = '''
	const char *in_str = PG_GETARG_CSTRING(0);
	const char *str;
	size_t		len;
	bool		result;

	/*
	 * Skip leading and trailing whitespace
	 */
	str = in_str;
	while (isspace((unsigned char) *str))
		str++;

	len = strlen(str);
	while (len > 0 && isspace((unsigned char) str[len - 1]))
		len--;

	if (parse_bool_with_len(str, len, &result))
		PG_RETURN_BOOL(result);

	ereturn(fcinfo->context, 0,
			(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
		   errmsg("invalid input syntax for type boolean: \"%s\"", in_str)));

	/* not reached */
	PG_RETURN_BOOL(false);
'''


def find_functions_with_call(functions):
    ddd = '|'.join(functions + ['ereturn'])
    call_pattern = f'(?:{ddd})'
    function_with_call_pattern = create_pattern(call_pattern)

    caller_functions = []
    caller_pg_functions = []

    unadapted_functions = {}
    unadapted_calls = []
    functions_with_unadapted_calls = {}

    for root, subdirs, files in os.walk('../..'):
        for filename in files:
            file_path = os.path.join(root, filename)
            # if filename[-2:] == '.c':
            if filename in source_filenames:
                # print(file_path)
                with open(file_path, 'r') as f:
                    content = f.read()
                    # content = 'lol () {   print(\'sds\')  ereturn(wwe); }'
                    matches = re.findall(function_with_call_pattern, content)
                    if len(matches) > 0:
                        for m in matches:
                            # print('!!!!!!!!!!', file_path, m[0], m[1], m[2], m[3], '$$$$$$$$', m[4])
                            caller_functions += [m[1]]
                            if m[2] == 'PG_FUNCTION_ARGS':
                                caller_pg_functions += [m[1]]

                            if (m[0] != 'bool' or re.search('escontext', m[2]) is None) and m[2] != 'PG_FUNCTION_ARGS':
                                unadapted_functions[m[1]] = True

                            if m[5] != 'ereturn':
                                func_call = m[4][-20:] + m[5] + m[6][:20]
                                safe_call_pattern = f'if{p_spaces}\(!{m[5]}\({p_any}\)\)'
                                safe_call_void_pattern = f'\(void\){p_spaces}{m[5]}\({p_any}\)\)'
                                direct_safe_call_pattern = f'DirectFunctionCall1Safe\({m[5]}'
                                if re.search(safe_call_pattern, func_call) is None and \
                                    re.search(safe_call_void_pattern, func_call) is None and \
                                    re.search(direct_safe_call_pattern, func_call) is None:
                                    unadapted_calls += [m[5]]

                                    if m[1] in functions_with_unadapted_calls:
                                        functions_with_unadapted_calls[m[1]] += [m[5]]
                                    else:
                                        functions_with_unadapted_calls[m[1]] = [m[5]]

    return caller_functions, caller_pg_functions, unadapted_functions, unadapted_calls, functions_with_unadapted_calls

caller_functions = []

# print(re.findall('!' + p_any + '\n*!', f'!{boolin}!'))

while True:
    new_caller_functions, new_caller_pg_functions, unadapted_functions, unadapted_calls, functions_with_unadapted_calls = find_functions_with_call(caller_functions)

    if len(caller_functions) == len(new_caller_functions):
        break

    # for c in sorted(new_caller_functions):
    #     if c not in caller_functions:
    #         print(c)

    for f in unadapted_functions:
        print(f)

    caller_functions = new_caller_functions

    print(len(caller_functions), len(unadapted_functions), len(unadapted_calls), len(functions_with_unadapted_calls))

# replase res = func(intput) -> if (!func(input, &res, escontext)) return false;
# define safe_call(functionCall, input...) if (!functionCall(input)) return false;

for f in sorted(functions_with_unadapted_calls):
    print(f, functions_with_unadapted_calls[f])