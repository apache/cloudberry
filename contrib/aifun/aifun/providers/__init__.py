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

from .openai import OpenAIProvider
from .anthropic import AnthropicProvider
from .gemini import GeminiProvider
from .aws_bedrock import AWSBedrockProvider

# The central mapping of provider type names to their implementation classes
PROVIDER_MAP = {
    "openai": OpenAIProvider,
    "anthropic": AnthropicProvider,
    "google": GeminiProvider,
    "aws_bedrock": AWSBedrockProvider,
}

def get_provider_class(provider_type):
    """
    Factory function to get the provider class based on its type name.
    """
    provider_class = PROVIDER_MAP.get(provider_type)
    if not provider_class:
        # This error will be caught by the calling function in handler.py
        # and reported as a plpy.error
        raise ValueError(f"Unsupported provider type: {provider_type}")
    return provider_class
