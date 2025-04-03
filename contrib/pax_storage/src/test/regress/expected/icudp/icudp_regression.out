-- UDPIFC receives data in a rx thread, errors in this thread cannot be raised
-- directly, they are recorded in memory and the main thread is responsible to
-- raise it at some proper time.
--
-- It is possible for the rx thread to record an error after last TEARDOWN,
-- technically it should be counted for last query, but there was a bug that
-- the main thread raised it in next SETUP, so the new query failed immediately
-- due to out-of-date errors.
--
-- This is fixed by discarding any rx thread errors at beginning of SETUP.
do $$ begin
    -- The fail rate was low, try enough times to trigger it.
    for i in 1 .. 10 loop
        begin
            -- Turn on fault injection on memory allocation, it can happen in
            -- either malloc() or palloc().
            set gp_udpic_fault_inject_percent = 50;
            set gp_udpic_fault_inject_bitmap = 1048576;

            -- The error only happens on malloc() failures in the rx-thread,
            -- need some lucks to trigger it, so retry is needed.
            for j in 1 .. 10 loop
                begin
                    -- Try to trigger the error with a motion to QD.
                    select * from gp_dist_random('gp_id');
                exception
                    -- Discard the errors.
                    when others then continue;
                end;
            end loop;

            -- Turn off the fault injection
            set gp_udpic_fault_inject_bitmap = 0;

            -- So this motioned query should not fail, without the fix the fail
            -- rate was about 30%.
            execute 'select * from gp_dist_random(''gp_id'')';
        end;
    end loop;
end $$;
