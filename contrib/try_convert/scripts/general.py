source_filenames = [
        'time.c', 'date.c', 'datetime.c', 'timestamp.c', 'nabstime.c', 'formatting.c', 
        'int.c', 'int8.c', 'float.c', 'float.c', 'bool.c', 'numeric.c', 'numutils.c',
        'format_type.c',
        # 'varbit.c',
        'char.c', 'varchar.c',
        'json.c', 'jsonb.c', 'xml.c', 
        'network.c',
        # 'cash.c',
        'reg_proc.c',
        'postgres.c',
        'stringinfo.c',
    ] + [
        'citext.c', 'oracle_compat.c',
        'hstore_io.c',
    ]

supported_types = [
    'int8',             # NUMBERS
    'int4',
    'int2',
    'float8',
    'float4',
    'numeric',
    # 'complex',

    'bool',

    # 'bit',              # BITSTRING
    # 'varbit',

    'date',             # TIME
    'time',
    'timetz',
    'timestamp',
    'timestamptz',
    'interval',

    # 'point',            # GEOMENTY
    # 'circle',
    # 'line',
    # 'lseg',
    # 'path',
    # 'box',
    # 'polygon',

    # 'cidr',             # IP
    # 'inet',
    # 'macaddr',

    'json',             # OBJ
    'jsonb',
    # 'xml',

    # 'bytea',   

    'char',             # STRINGS
    # 'bpchar',
    'varchar',
    'text',

    # 'money',
    # # 'pg_lsn',
    # # 'tsquery',
    # # 'tsvector',
    # # 'txid_snapshot',
    # 'uuid',

    # 'regtype',          # SYSTEM
    # 'regproc',
    # 'regclass',
    # 'oid',
] + [
    'citext',
    'hstore',
]

supported_extensions = [
    'citext',
    'hstore',
]



string_types = [
    'text',
    'citext',
    'char',
    # 'bpchar',
    'varchar',
]

typmod_types = [
    'bit',
    'varbit',
    'char',
    'varchar',
    # 'bpchar',
]

typmod_lens = [
    None, 1, 5, 10, 20
]



uncomparable_types = [
    'json',
    'xml',
    'point',
]

has_corrupt_data = [
    'time', 'timetz', 'timestamp', 'timestamptz', 'date', 
    'json',
    'int2', 'int4', 'int8', 'float4', 'float8', 'numeric', 
]