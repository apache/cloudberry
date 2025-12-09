# py_handler/providers/__init__.py
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
