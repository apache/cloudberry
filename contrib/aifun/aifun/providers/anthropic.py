# py_handler/providers/anthropic.py
from .base import RequestsBasedProvider

class AnthropicProvider(RequestsBasedProvider):
    """
    Provider implementation for Anthropic's Claude API.
    
    This class implements the Anthropic API endpoints for text generation
    using Claude models through the Anthropic REST API.
    """
    
    def _get_endpoint(self):
        """
        Get the endpoint URL for Anthropic's messages API.
        
        Returns:
            str: The complete URL for the messages API endpoint
        """
        return self.metadata.get("endpoint", "https://api.anthropic.com/v1/messages")

    def _get_headers(self):
        """
        Get the required headers for Anthropic API requests.
        
        Returns:
            dict: Dictionary containing the required headers including API key and version
        """
        return {
            "x-api-key": self.credentials['api_key'], 
            "anthropic-version": self.metadata.get("anthropic_version", "2023-06-01"), 
            "Content-Type": "application/json"
        }

    def ask(self, model: str, prompt: str, **kwargs):
        """
        Generate a text response using Anthropic's Claude API.
        
        Args:
            model (str): The model identifier (e.g., "claude-3-opus-20240229")
            prompt (str): The input prompt for text generation
            **kwargs: Additional parameters for the Anthropic API (max_tokens, temperature, etc.)
            
        Returns:
            str: The generated text response
            
        Raises:
            requests.HTTPError: If the API request fails
        """
        payload = {
            "model": model, 
            "messages": [
                {
                    "role": "user", 
                    "content": prompt
                }
            ], 
            "max_tokens": 4096, 
            **kwargs
        }
        data = self._make_request(
            "post",
            self._get_endpoint(),
            self._get_headers(),
            payload
        )
        return data["content"][0]["text"].strip()

    def vlm_ask(self, model: str, prompt: str, image: str, **kwargs):
        """
        Generate a text response using Anthropic's Claude API for visual language models.
        
        Args:
            model (str): The model identifier (e.g., "claude-3-opus-20240229")
            prompt (str): The input prompt for text generation
            image (str): The base64-encoded image string for visual context,
                         including the mime type prefix (e.g., "data:image/png;base64,")
            **kwargs: Additional parameters for the Anthropic API
            
        Returns:
            str: The generated text response
            
        Raises:
            requests.HTTPError: If the API request fails
        """
        mime_type = image.split(";")[0].split(":")[-1]
        image = image.split(',')[1]

        payload = {
            "model": model, 
            "messages": [
                {
                    "role": "user", 
                    "content": [
                        {
                            "type": "image",
                            "image": {
                                "data": image,
                                "mime_type": mime_type
                            }
                        },
                        {
                            "type": "text",
                            "text": prompt
                        }
                    ]
                }
            ], 
            "max_tokens": 4096, 
            **kwargs
        }
        data = self._make_request(
            "post",
            self._get_endpoint(),
            self._get_headers(),
            payload
        )
        return data["content"][0]["text"].strip()
    