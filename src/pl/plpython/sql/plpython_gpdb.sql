-- Miscellaneous extra tests added in GPDB.

-- test of exception handling
CREATE OR REPLACE FUNCTION queryexec( query text ) returns boolean
AS $$
try:
  plan = plpy.prepare( query )
  rv = plpy.execute( plan )
except:
  plpy.notice( 'Error Trapped' )
  return False

for r in rv:
  plpy.notice( str( r ) )

return True
$$ LANGUAGE plpython3u;

SELECT queryexec('SELECT 2'); 
SELECT queryexec('SELECT X'); 

--
create or replace function split(input int) returns setof ab_tuple as
$$
       plpy.log("Returning the FIRST tuple")
       yield [input, input + 1]
       plpy.log("Returning the SECOND tuple"); 
       yield [input+2, input + 3]
$$
LANGUAGE plpython3u; 

SELECT (split(10)).*; 

SELECT * FROM split(100); 

-- Test named and unnamed tuples.
CREATE OR REPLACE FUNCTION unnamed_tuple_test() RETURNS VOID LANGUAGE plpython3u AS $$
plpy.execute("SHOW client_min_messages")
$$;
CREATE FUNCTION named_tuple_test() RETURNS VARCHAR LANGUAGE plpython3u AS $$
return plpy.execute("SELECT setting FROM pg_settings WHERE name='client_min_messages'")[0]['setting']
$$;

select unnamed_tuple_test(); 
select named_tuple_test(); 

--
-- These test results will follow the upsteam results
CREATE OR REPLACE FUNCTION oneline() returns text as $$ 
return 'No spaces' 
$$ LANGUAGE plpython3u;

CREATE OR REPLACE FUNCTION oneline2() returns text as $$  
x = "\""
y = ''
z = ""
w = '\'' + 'a string with # and "" inside ' + "another string with #  and '' inside "
return x + y + z + w
$$ LANGUAGE plpython3u;

CREATE OR REPLACE FUNCTION multiline() returns text as $$ 
return """ One space
  Two spaces
   Three spaces
No spaces""" 
$$ LANGUAGE plpython3u;

CREATE OR REPLACE FUNCTION multiline2() returns text as $$
# If there's something in my comment it can mess things up
return '''
The ' in the comment should not cause this line to begin with a tab
''' + 'This is a rather long string containing\n\
    several lines of text just as you would do in C.\n\
     Note that whitespace at the beginning of the line is\
significant. The string can contain both \' and ".\n' + r"This is an another long string containing\n\
two lines of text and defined with the r\"...\" syntax."
$$ LANGUAGE plpython3u; 

CREATE OR REPLACE FUNCTION multiline3() returns text as $$  
# This is a comment
x = """ 
  # This is not a comment so the quotes at the end of the line do end the string """ 
return x
$$ LANGUAGE plpython3u;

select oneline() 
union all 
select oneline2()
union all 
select multiline() 
union all 
select multiline2() 
union all 
select multiline3();
