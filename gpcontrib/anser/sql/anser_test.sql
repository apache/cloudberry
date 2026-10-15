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

-- ---------------------------------------------------------------------------
-- Sizing, and the decision not to bother.
--
-- Anser can decline to build a filter at four points, and these cases walk the
-- boundary of each.  Declining is always safe: it costs the query its filter,
-- never its correctness.  The payload cap is pinned here because every size
-- below follows from it.
-- ---------------------------------------------------------------------------
SET anser.max_info_size = 68157440;

-- 1. The planner's gate -- the only one that costs nothing, since nothing is
-- injected and no consumer is left waiting.  A 64 MB bitset is 536870912 bits,
-- so 134217728 estimated keys is exactly 4 bits/key and one more key is not.
-- A zero or negative estimate is treated as one key, not as "no filter".
SELECT t.est_rows, s.injected, s.total_elems, s.planned_bytes
FROM (VALUES (0), (-5), (1), (1000), (1000000), (10000000),
             (134217728), (134217729), (150000000), (1000000000))
       AS t(est_rows),
     LATERAL anser_test_rf_size(t.est_rows) AS s;

-- 2. Filter construction.  bloom_create floors every bitset at 1 MB and that
-- floor overrides the work_mem cap, so a cap below "1 MB + header" can only
-- yield a filter too large to send -- which is why a flat 1 MB cap is refused
-- while 1048592 (the floor plus exactly one header) is accepted and serializes
-- to precisely the cap.  fits_cap is the invariant that must never be false.
SELECT t.what, t.elems, t.cap, s.built, s.bits, s.serialized,
       round(s.bits_per_key::numeric, 3) AS bits_per_key,
       (NOT s.built OR s.serialized <= t.cap) AS fits_cap
FROM (VALUES
        ('no keys',                      0::bigint, 1048640::bigint),
        ('negative keys',                       -1,         1048640),
        ('cap equals the header',                32,              16),
        ('cap one byte over the header',         32,              17),
        ('cap is 1 MB, no header room',          32,         1048576),
        ('cap is 1 MB plus a header',            32,         1048592),
        ('cap one byte short of that',           32,         1048591),
        ('a single key',                          1,         1048640),
        ('exactly 4 bits/key',              2097152,         1048640),
        ('one key too many',                2097153,         1048640),
        ('exactly 4 bits/key at 2 MB',      4194304,         2097216),
        ('one key too many at 2 MB',        4194305,         2097216),
        ('a billion keys',               1000000000,        68157440))
       AS t(what, elems, cap),
     LATERAL anser_test_bloom_shape(t.elems, t.cap) AS s;

-- 3. The coordinator's gate on a merged payload.  The fill is fixed exactly by
-- setting whole bytes rather than by inserting keys, which would be both slow
-- and only statistically precise.  The rule is "reject above 95%", so a payload
-- sitting exactly on the limit is still delivered.  Malformed framing is not
-- worth delivering either, since it cannot be a filter at all.
SELECT t.what, t.bitset_bytes, t.bytes_set, t.damage,
       anser_test_worth_delivering(t.bitset_bytes, t.bytes_set, t.damage)
         AS worth_delivering
FROM (VALUES
        ('empty',                1000,    0, ''),
        ('half full',            1000,  500, ''),
        ('just under the limit', 1000,  949, ''),
        ('exactly on the limit', 1000,  950, ''),
        ('just over the limit',  1000,  951, ''),
        ('completely full',      1000, 1000, ''),
        ('no bitset at all',        0,    0, ''),
        ('corrupt magic',        1000,    0, 'magic'),
        ('corrupt version',      1000,    0, 'version'),
        ('zero part count',      1000,    0, 'parts'),
        ('null payload',         1000,    0, 'null'))
       AS t(what, bitset_bytes, bytes_set, damage);

-- 4. The producer driven end to end on the coordinator-local path: build,
-- insert, publish, consume.  "no-filter:cancelled" is the case that matters
-- most -- construction declined, so the cancel goes out before the build side
-- is scanned and no consumer waits for its timeout.  The last row is the one
-- the planner cannot predict: a filter sized for 32 keys that receives three
-- million, which saturates and so publishes a cancel instead of a payload.
SELECT t.what,
       anser_test_producer_decision(t.elems, t.cap, t.n_keys) AS decision
FROM (VALUES
        ('sparse filter',                          32::bigint, 1048640::bigint,     100),
        ('cap with no header room',                        32,         1048576,     100),
        ('no keys declared',                                0,         1048640,       0),
        ('one key past the floor',                    2097153,         1048640,       0),
        ('exactly 4 bits/key',                        2097152,         1048640,  100000),
        ('estimate of 32, three million inserted',         32,         1048640, 3000000))
       AS t(what, elems, cap, n_keys);

-- ---------------------------------------------------------------------------
-- The wire protocol.
--
-- Not exhaustive; a fuzzer would be the right tool for that.  These cover the
-- properties that are cheap to check and expensive to lose: a fixed-width
-- header with the fields where they are documented, a payload that cannot be
-- mistaken for framing, a length cross-check that rejects a message which does
-- not add up, and a checksum that rejects one altered byte anywhere it covers.
-- ---------------------------------------------------------------------------

-- The bytes, exactly.  Session/command/condition are 42/7/3 and part is 1 of 3,
-- so these are golden values: any change to the layout, the field widths or
-- what the checksum covers shows up here as a diff.  Note the third row -- a
-- cancelled message carries no body even though one was passed, and its
-- checksum must be computed over what is actually sent.
--
-- That the message comes back as text is itself the check that it is NUL-free,
-- which a NOTIFY payload has to be: the fourth row's body is 00 ff 00.
SELECT t.what, length(m.msg) AS len, m.msg
FROM (VALUES
        ('part with a 3-byte body',     'P', 'B', 0, '\x616263'::bytea),
        ('subscription, no body',       'S', '-', 0, '\x'::bytea),
        ('cancelled: body suppressed',  'P', 'B', 1, '\x616263'::bytea),
        ('body of NUL and 0xff bytes',  'P', 'B', 0, '\x00ff00'::bytea))
       AS t(what, kind, ptype, flags, body),
     LATERAL (SELECT anser_test_wire_format(t.kind::"char", t.ptype::"char",
                                            t.flags, 'anser_rf_3', t.body)
                AS msg) m;

-- Alter exactly one byte (or one length) and see what the reader does.
--
-- The split matters: "malformed" is rejected by framing, before a byte is
-- allocated or a channel created, while "checksum-mismatch" got past framing
-- and was caught by the CRC.  Corrupting the kind byte is the interesting one
-- -- the message stays perfectly well-formed, and only the checksum notices,
-- which is why the checksum covers the header and not just the payload.
SELECT t.tamper,
       anser_test_wire_roundtrip('anser_rf_3', '\x616263'::bytea, t.tamper,
                                 'B'::"char") AS outcome
FROM (VALUES (''), ('truncate'), ('append'), ('bodylen'), ('tag'), ('kind'),
             ('type'), ('crc'), ('key'), ('body')) AS t(tamper);

-- Keys and bodies that must survive the trip unchanged.  "ok" means every
-- field and the decoded body came back identical, so each of these is a case
-- where a naive framing would have gone wrong: the key travels as raw bytes
-- and is taken by length, which is what lets it contain a newline, a space, or
-- the protocol's own tag without any escaping.
SELECT t.what,
       anser_test_wire_roundtrip(t.key, t.body, '', 'B'::"char") AS outcome
FROM (VALUES
        ('plain key',                     'anser_rf_3'           , '\x616263'::bytea),
        ('key containing a newline',      E'rf:a\nb'             , '\x616263'::bytea),
        ('key that looks like a header',  'anser3 P B 0000000000', '\x616263'::bytea),
        ('key with spaces and quotes',    'rf:"a b".c'           , '\x616263'::bytea),
        ('key at the 63-byte limit',      repeat('k', 63)        , '\x616263'::bytea),
        ('empty key',                     ''                     , '\x616263'::bytea),
        ('empty body',                    'anser_rf_3'           , '\x'::bytea),
        ('body of a single NUL',          'anser_rf_3'           , '\x00'::bytea),
        ('body of framing-hostile bytes', 'anser_rf_3'           , '\x00ff0a0d5c22'::bytea),
        ('1 KB body',                     'anser_rf_3'           , decode(repeat('00ff', 512), 'hex')))
       AS t(what, key, body);

-- The QD -> QE checksum.  The value itself does not matter; which inputs
-- change it does.  Every routing field and the key are always covered, and the
-- body only for a payload type that asks for it -- the fifth column is that
-- deliberate exemption, not an oversight.
SELECT
  anser_test_push_crc('B'::"char", 1, 0, 'k', '\x01')
    <> anser_test_push_crc('B'::"char", 2, 0, 'k', '\x01') AS condition_covered,
  anser_test_push_crc('B'::"char", 1, 0, 'k', '\x01')
    <> anser_test_push_crc('B'::"char", 1, 1, 'k', '\x01') AS flags_covered,
  anser_test_push_crc('B'::"char", 1, 0, 'k', '\x01')
    <> anser_test_push_crc('B'::"char", 1, 0, 'j', '\x01') AS key_covered,
  anser_test_push_crc('B'::"char", 1, 0, 'k', '\x01')
    <> anser_test_push_crc('B'::"char", 1, 0, 'k', '\x02') AS bloom_body_covered,
  anser_test_push_crc('-'::"char", 1, 0, 'k', '\x01')
     = anser_test_push_crc('-'::"char", 1, 0, 'k', '\x02') AS other_body_exempt,
  anser_test_push_crc('B'::"char", 1, 0, 'k', '\x01')
    <> anser_test_push_crc('-'::"char", 1, 0, 'k', '\x01') AS type_covered;

DROP EXTENSION anser_test;
