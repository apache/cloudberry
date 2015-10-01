--------------------------
-- FIXEDDECIMALAGGSTATE --
-------------------------

CREATE TYPE FIXEDDECIMALAGGSTATE;

CREATE FUNCTION fixeddecimalaggstatein(cstring, oid, int4)
RETURNS FIXEDDECIMALAGGSTATE
AS 'fixeddecimal', 'fixeddecimalaggstatein'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION fixeddecimalaggstateout(fixeddecimalaggstate)
RETURNS cstring
AS 'fixeddecimal', 'fixeddecimalaggstateout'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION fixeddecimalaggstaterecv(internal)
RETURNS FIXEDDECIMALAGGSTATE
AS 'fixeddecimal', 'fixeddecimalaggstaterecv'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION fixeddecimalaggstatesend(FIXEDDECIMALAGGSTATE)
RETURNS bytea
AS 'fixeddecimal', 'fixeddecimalaggstatesend'
LANGUAGE C IMMUTABLE STRICT;


CREATE TYPE FIXEDDECIMALAGGSTATE (
    INPUT          = fixeddecimalaggstatein,
    OUTPUT         = fixeddecimalaggstateout,
    RECEIVE        = fixeddecimalaggstaterecv,
    SEND           = fixeddecimalaggstatesend,
    INTERNALLENGTH = 8,
    ALIGNMENT      = 'double',
    STORAGE        = plain,
    CATEGORY       = 'N',
    PREFERRED      = false,
    COLLATABLE     = false
);

