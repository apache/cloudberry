SELECT XMLPARSE(CONTENT '<abc>x</abc>'::text STRIP WHITESPACE) AS "xmlparse";
SELECT XMLPARSE(CONTENT '<abc>x</abc>'::text PRESERVE WHITESPACE) AS "xmlparse";
