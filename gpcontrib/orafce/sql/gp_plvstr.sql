-- plvstr.normalize() drops leading and trailing whitespace and folds inner
-- whitespace into one space.
SELECT '[' || plvstr.normalize(E'  \t a  b \n c  ') || ']' AS normalized;
SELECT '[' || plvstr.normalize(E' \t\n ') || ']' AS only_whitespace;
SELECT '[' || plvstr.normalize('a') || ']' AS one_char;
