# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import time
import math
from functools import wraps

# A simple cache for plpy and provider instances
# The key will be `user::provider_id` to ensure user isolation
CACHE = {}

def get_plpy():
    if "plpy" in CACHE:
        return CACHE["plpy"]

    try:
        import plpy
        CACHE["plpy"] = plpy
        return plpy
    except ImportError:
        raise ImportError("plpy module not available. This code must be run within PostgreSQL PL/Python environment.")

def cosine_similarity(vec1, vec2):
    dot_product = sum(p*q for p,q in zip(vec1, vec2))
    magnitude = math.sqrt(sum([val**2 for val in vec1])) * math.sqrt(sum([val**2 for val in vec2]))
    if not magnitude:
        return 0.0
    return dot_product / magnitude


def parse_jsonb(jsonb_str):
    try:
        import json

        jsonb_str = jsonb_str.strip()
        if jsonb_str.startswith("```json"):
            jsonb_str = jsonb_str.split("```json", 1)[1].split("```", 1)[0]
        else:
            jsonb_str = jsonb_str.split("```", 1)[0]

        return json.loads(jsonb_str)
    except json.JSONDecodeError:
        get_plpy().warning(f"Error parsing JSONB string: {jsonb_str}")
        return None

def retry_with_backoff(retries=3, backoff_in_seconds=1):
    def rwb(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            plpy = get_plpy()
            x = 0
            while True:
                try:
                    return f(*args, **kwargs)
                except Exception as e:
                    # A more robust implementation would check for specific boto3/requests transient errors
                    if "botocore.exceptions" in str(type(e)) or "requests.exceptions" in str(type(e)):
                        if x < retries:
                            x += 1
                            sleep_time = backoff_in_seconds * 2**(x-1)
                            plpy.warning(f"API call failed, retrying in {sleep_time}s... ({x}/{retries})")
                            time.sleep(sleep_time)
                        else:
                            plpy.warning(f"API call failed after {retries} retries.")
                            raise e
                    else:
                        raise e
        return wrapper
    return rwb
