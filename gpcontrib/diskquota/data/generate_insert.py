# generate insert statement
f = open('insertfile', 'w')
f.write("set search_path to perfs;\n");
for i in range(1,2000):
  f.write("insert into a"+str(i)+" select generate_series(1,3000);\n");
f.close()
