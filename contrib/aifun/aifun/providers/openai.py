# py_handler/providers/openai.py
from .base import RequestsBasedProvider

class OpenAIProvider(RequestsBasedProvider):
    """
    Provider implementation for OpenAI API.
    
    This class implements the OpenAI API endpoints for text generation, embeddings,
    and multimodal embeddings using the OpenAI REST API.
    """
    
    def _get_endpoint(self, path: str) -> str:
        """
        Get the full endpoint URL for a given API path.
        
        Args:
            path (str): The API path (e.g., "/chat/completions")
            
        Returns:
            str: The complete URL for the API endpoint
        """
        base_url = self.metadata.get("endpoint", "https://api.openai.com/v1")
        return f"{base_url}{path}"

    def _get_headers(self) -> dict:
        """
        Get the required headers for OpenAI API requests.
        
        Returns:
            dict: Dictionary containing the required headers
        """
        return {
            "Authorization": f"Bearer {self.credentials['api_key']}",
            "Content-Type": "application/json"
        }

    def ask(self, model: str, prompt: str, **kwargs) -> str:
        """
        Generate a text response using OpenAI's chat completion API.
        
        Args:
            model (str): The model identifier (e.g., "gpt-4", "gpt-3.5-turbo")
            prompt (str): The input prompt for text generation
            **kwargs: Additional parameters for the OpenAI API (temperature, max_tokens, etc.)
            
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
            **kwargs
        }        
        data = self._make_request(
            "post",
            self._get_endpoint("/chat/completions"),
            self._get_headers(),
            payload
        )
        return data["choices"][0]["message"]["content"].strip()

    def embed(self, model: str, text: str, **kwargs) -> list:
        """
        Generate text embeddings using OpenAI's embeddings API.
        
        Args:
            model (str): The model identifier (e.g., "text-embedding-ada-002")
            text (str): The input text to generate embeddings for
            **kwargs: Additional parameters for the OpenAI API
            
        Returns:
            list: A list of floating-point numbers representing the embedding vector
            
        Raises:
            requests.HTTPError: If the API request fails
        """
        payload = {
            "model": model,
            "input": text,
            **kwargs
        }
        data = self._make_request(
            "post",
            self._get_endpoint("/embeddings"),
            self._get_headers(),
            payload
        )
        return str(data["data"][0]["embedding"])
    
    def multimodal_embed(self, model: str, content: dict, **kwargs) -> list:
        """
        Generate embeddings for multimodal content using OpenAI.
        Supports text and image content for models like CLIP.
        
        Args:
            model (str): The model identifier (e.g., "text-embedding-3-large")
            content (dict): Dictionary with text and/or image data
                           Format: {"text": "some text", "image": "base64_encoded_image"}
            **kwargs: Additional parameters for the embedding model
        
        Note:
            - The input_image should be a base64-encoded string including the mime type prefix (e.g., "data:image/png;base64,")
        
        Returns:
            list: A list of floating-point numbers representing the embedding vector
            
        Raises:
            requests.HTTPError: If the API request fails
        """
        # Extract text and image from content
        input_text = content.get("text", "")
        input_image = content.get("image", "")
        
        if input_image and input_text:
            payload = {
                "model": model,
                "input": [
                    {"text": input_text},
                    {"image": input_image}
                ],
                **kwargs
            }
        elif input_image:
            # Image-only embedding
            payload = {
                "model": model,
                "input": [{"image": input_image}],
                **kwargs
            }
        else:
            payload = {
                "model": model,
                "input": [{"text": input_text}],
                **kwargs
            }
        
        data = self._make_request("post", self._get_endpoint("/embeddings"), self._get_headers(), payload)
        return str(data["data"][0]["embedding"])

    def vlm_ask(self, model: str, prompt: str, image: str, **kwargs) -> str:
        """
        Generate a text response using OpenAI's visual language model API.
        
        Args:
            model (str): The model identifier (e.g., "gpt-4-vision-preview")
            prompt (str): The input prompt for text generation
            image (str): The base64-encoded image string (including the prefix mime type, e.g., "data:image/png;base64,") for visual context
            **kwargs: Additional parameters for the OpenAI API
            
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
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": image
                            }
                        },
                        {
                            "type": "text",
                            "text": prompt
                        }
                    ]
                }
            ],
            **kwargs
        }   
       
        data = self._make_request(
            "post",
            self._get_endpoint("/chat/completions"),
            self._get_headers(),
            payload
        )

        return data["choices"][0]["message"]["content"].strip()
