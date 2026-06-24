<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# gp_url_tools: Cloudberry extension providing functionality for working with URL addresses

### Features
gp_url_tools is an extension for the Cloudberry database that gives implementation 
for functions that encode/decode url/uri.

### Usage
```sql
CREATE EXTENSION gp_url_tools;
```
```sql
SELECT url_tools_schema.encode_url('Hello World');
```
```bash
  encode_url
───────────────
 Hello%20World
(1 row)
```
```sql
SELECT url_tools_schema.decode_url('Hello%20World');
```
```bash
 decode_url  
─────────────
 Hello World
(1 row)
```
```sql
SELECT url_tools_schema.encode_uri('https://ru.wikipedia.org/wiki/Greenplum_(компания)');
```
```bash
                                         encode_uri                  
────────────────────────────────────────────────────────────────────────────────────────────
 https://ru.wikipedia.org/wiki/Greenplum_(%D0%BA%D0%BE%D0%BC%D0%BF%D0%B0%D0%BD%D0%B8%D1%8F)
```
```sql
SELECT url_tools_schema.decode_uri('https://ru.wikipedia.org/wiki/Greenplum_(%D0%BA%D0%BE%D0%BC%D0%BF%D0%B0%D0%BD%D0%B8%D1%8F)');
```
```bash
                     decode_uri               
────────────────────────────────────────────────────
 https://ru.wikipedia.org/wiki/Greenplum_(компания)
```

### Acknowledgments
Thank you very much for the extension for PostgreSQL: https://github.com/okbob/url_encode, its sources were very useful.
