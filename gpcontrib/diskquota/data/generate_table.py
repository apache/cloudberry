#generate create table statement
f = open('tablefile', 'w')
f.write("create schema perfs;\nset search_path to perfs;\n");
sql  = "create table a"
sql2 = " (i int);\n"
for i in range(1,100000):
  f.write(sql+str(i)+sql2+ "insert into a"+str(i)+" values(2);\n");
f.close()
