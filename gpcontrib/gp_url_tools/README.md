# gp_url_tools: Greenplum extension providing functionality for working with URL addresses

### Features
gp_url_tools is an extension for the Greenplum database that gives implementation 
for functions that encode/decode url/uri.

### Installation
Install from source:
```
git clone https://github.com/open-gpdb/gp_url_tools.git
cd gp_url_tools
# Build it. Building would require GP installed nearby and sourcing greenplum_path.sh
source <path_to_gp>/greenplum_path.sh
make && make install
```

### Usage
```
=# create extension gp_url_tools;

=# select url_tools_schema.encode_url('Hello World');
  encode_url
───────────────
 Hello%20World
(1 row)

=# select url_tools_schema.decode_url('Hello%20World');
 decode_url  
─────────────
 Hello World
(1 row)

=# select url_tools_schema.encode_uri('https://ru.wikipedia.org/wiki/Greenplum_(компания)');
                                         encode_uri                  
────────────────────────────────────────────────────────────────────────────────────────────
 https://ru.wikipedia.org/wiki/Greenplum_(%D0%BA%D0%BE%D0%BC%D0%BF%D0%B0%D0%BD%D0%B8%D1%8F)

=# select url_tools_schema.decode_uri('https://ru.wikipedia.org/wiki/Greenplum_(%D0%BA%D0%BE%D0%BC%D0%BF%D0%B0%D0%BD%D0%B8%D1%8F)');
                     decode_uri               
────────────────────────────────────────────────────
 https://ru.wikipedia.org/wiki/Greenplum_(компания)
```

### Acknowledgments
Thank you very much for the extension for postgrsql: https://github.com/okbob/url_encode, its sources were very useful.
