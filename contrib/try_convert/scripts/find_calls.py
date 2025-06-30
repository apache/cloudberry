import re
import os, glob

from general import source_filenames

p_any = '(?:\S| |\t|\n+\t|\n+ |\n+#|\n+/|\n+\$)+?'
p_spaces = '\s*'
p_space = '\s+'


def remove_comments(body):
    body = re.sub('".*?"', '""', body)
    body = re.sub('/\*[\s\S]*?\*/', '/* */', body)
    return body

def find_functions(func_names, text):

    func_names_pattern_list = '|'.join(func_names)
    func_names_pattern = f'((?:{func_names_pattern_list}))'

    func_pattern = '\n(\w+)\s+?' + func_names_pattern + p_spaces + '\((' + p_any + ')\)' + p_spaces + '(\n\{' + '(' + p_any + ')' + '\n+\})'

    funcs = []

    for m in re.findall(func_pattern, text):
        # print(m[1])
        funcs += [(m[1], m[0], m[2], m[3])]
    

    return funcs

def find_safe_functions(text):

    func_pattern = '\n((?:\w+\s+)+)(\w+)' + p_spaces + '\((' + p_any + 'Node' + p_spaces + '\*' + p_spaces + 'escontext' + ')\)' + p_spaces + '(\n\{' + '(' + p_any + ')' + '\n+\})'

    funcs = []

    for m in re.findall(func_pattern, text):
        # print(m[1])
        funcs += [(m[1], m[0], m[2], m[3])]
    

    return funcs

def create_pattern(funcCall):
    return '\n((?:\w+\s+)+)(\w+)' + p_spaces + '\((' + p_any + ')\)' + p_spaces + '(\n\{' + '(' + p_any + ')'  + '\W' + f'({funcCall})' + '\W' + '(' + p_any + ')' + '\n+\})'


def find_functions_with_call(functions):
        ddd = '|'.join(functions)
        call_pattern = f'(?:{ddd})'
        function_with_call_pattern = create_pattern(call_pattern)

        caller_functions = []

        for root, subdirs, files in os.walk('../..'):
            for filename in files:
                file_path = os.path.join(root, filename)
                # if filename[-2:] == '.c':
                if filename in source_filenames:
                    # print(file_path)
                    with open(file_path, 'r') as f:
                        content = remove_comments(f.read())
                        # content = 'lol () {   print(\'sds\')  ereturn(wwe); }'
                        matches = re.findall(function_with_call_pattern, content)
                        if len(matches) > 0:
                            for m in matches:
                                # print('!!!!!!!!!!', file_path, m[0], m[1], m[2], m[3], '$$$$$$$$', m[4])
                                # if m[1] == 'DecodeNumberField':
                                #     print(m[1], m[4], m[5])
                                if m[1] not in ['if']:
                                    caller_functions += [m[1]]

        return caller_functions


def get_all_functions_with(function_call):


    caller_functions = {function_call}

    # print(re.findall('!' + p_any + '\n*!', f'!{boolin}!'))

    while True:
        new_caller_functions = find_functions_with_call(list(caller_functions))

        l = len(caller_functions)
            
        for cf in new_caller_functions:
            caller_functions.add(cf)

        if l == len(caller_functions):
            break
        
        # print(len(caller_functions))

    return list(caller_functions)