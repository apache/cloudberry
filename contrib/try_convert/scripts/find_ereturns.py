import re

import os, glob

from general import source_filenames

p_any = '(?:\S| |\t|\n+\t|\n+ |\n+#|\n+/|\n+\$)+?'
p_anys = '(?:\S| |\t|\n+\t|\n+ |\n+#|\n+/|\n+\$)*?'
p_spaces = '\s*'
p_space = '\s+'

ereturn_pattern = 'ereturn\(' + p_any + '\);'

ereturns = {}

for root, subdirs, files in os.walk('../..'):
    for filename in files:
        file_path = os.path.join(root, filename)
        # if filename[-2:] == '.c':
        if filename in source_filenames:
            # print(file_path)
            with open(file_path, 'r') as f:
                content = f.read()
                # content = 'lol () {   print(\'sds\')  ereturn(wwe); }'
                matches = re.findall(ereturn_pattern, content)

                for m in matches:

                    def get_field(field, m):
                        errfield = re.search(f'{field}\("('+ p_anys + '"' + p_anys + ')[\)\n]', m)
                        if errfield is not None:
                            errfield = errfield[1]

                        return errfield

                    errmsg = get_field('errmsg', m)
                    errdetail = get_field('errdetail', m)
                    errhint = get_field('errhint', m)

                    desc = (errmsg, errdetail, errhint)

                    ereturns[filename] = (ereturns[filename] if filename in ereturns else []) + [desc]


for filename in sorted(ereturns):
    print(filename)
    for ereturn in ereturns[filename]:
        print('         ', ereturn)