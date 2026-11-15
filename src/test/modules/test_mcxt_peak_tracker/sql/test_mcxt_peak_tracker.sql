create extension test_mcxt_peak_tracker;

select * from check_peak_usage_propagation();

select * from check_alloc_and_free_for_context_types();

select * from check_reparent();
