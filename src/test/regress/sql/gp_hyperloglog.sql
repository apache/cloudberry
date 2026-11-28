-- A normal aggregate produces a state that can be validated and read.
SELECT round(gp_hyperloglog_get_estimate(
             gp_hyperloglog_accum(i)::text::gp_hyperloglog_estimator)::numeric, 2)
FROM generate_series(1, 10) AS i;

-- Reject a packed state whose register array is too short for its dimensions.
SELECT encode(decode('05040200ffffffff' || repeat('00', 72), 'hex'), 'base64')::gp_hyperloglog_estimator;

-- Reject unsupported dimensions before computing shifts or indexing tables.
SELECT encode(decode('7f040200ffffffff' || repeat('00', 72), 'hex'), 'base64')::gp_hyperloglog_estimator;

-- Reject b=64 specifically before its shift/index calculations.
SELECT encode(decode('40040200ffffffff' || repeat('00', 72), 'hex'), 'base64')::gp_hyperloglog_estimator;

-- Reject malformed compressed input before calling the PGLZ decompressor.
SELECT encode(decode('fb040200ffffffff' || repeat('00', 64) || '0100' || repeat('00', 4), 'hex'), 'base64')::gp_hyperloglog_estimator;

-- Unpacked register values must fit the lookup table's register width.
SELECT encode(decode('04040202ffffffff' || repeat('00', 64) || 'ff' || repeat('00', 19), 'hex'), 'base64')::gp_hyperloglog_estimator;

-- Merging counters with different dimensions would otherwise read past one
-- of the register arrays.
SELECT gp_hyperloglog_merge(
       encode(decode('04040200ffffffff' || repeat('00', 76), 'hex'), 'base64')::gp_hyperloglog_estimator,
       encode(decode('05040200ffffffff' || repeat('00', 84), 'hex'), 'base64')::gp_hyperloglog_estimator);
