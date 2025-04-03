create table t_update_hash_col_utilitymode(c int, d int) distributed by (c);

-- This works.
1U: update t_update_hash_col_utilitymode set d = d + 1;

-- But this throws an error.
1U: update t_update_hash_col_utilitymode set c = c + 1;

drop table t_update_hash_col_utilitymode;
