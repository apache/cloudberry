CREATE EXTENSION anser_test;

-- Bloom payload protocol: serialize one part and read it back.
SELECT anser_test_bloom_roundtrip('bf_roundtrip', 42);

-- In-place fold: same-size union mutates the accumulator; a mismatched size is
-- rejected.  This is the coordinator's only combine path (the first part is
-- kept verbatim, every later part folds into it).
SELECT anser_test_bloom_fold_inplace() AS fold_inplace_ok;

-- Safety regression: the consumer rebuilds the filter from its own parameters
-- and requires the received bitset to be exactly the expected size (and the
-- header magic to match); truncated/oversized/corrupt parts are rejected.
SELECT anser_test_bloom_rejects_mismatch() AS reject_mismatch;

-- Producer and consumer driven end to end in one backend: publish a part, let
-- the coordinator side merge it, then receive and query the filter.
SELECT anser_test_node_roundtrip(168);

DROP EXTENSION anser_test;
