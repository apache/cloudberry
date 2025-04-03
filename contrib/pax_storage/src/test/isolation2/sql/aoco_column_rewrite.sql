/**
 * FIXME(jiaqizho): i cherry-pick the commit 17b0aac07bebc771b2d3a32bbd22cc8318201f57
 * (Improve gp_toolkit.gp_check_orphaned_files for more reliable results) without 
 * cherry-pick the prev-commits which is `ALTER TABLE ACCESS METHOD`.
 * 
 * !!!KEEP THIS COMMENTS util we cherry-pick the commit 20f39c76f3dd03b0feb8b05011c1e0637df6c77e
 * (Improve gp_toolkit.gp_check_orphaned_files for more reliable results).
 * 
 * So when we cherry-pick the commit 20f39c76f3dd03b0feb8b05011c1e0637df6c77e, please
 * also bring the  changes in 17b0aac07bebc771b2d3a32bbd22cc8318201f57, or we will lost it.
 */
