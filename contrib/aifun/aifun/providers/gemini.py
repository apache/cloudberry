# py_handler/providers/gemini.py
from .base import RequestsBasedProvider

class GeminiProvider(RequestsBasedProvider):
    """
    Provider implementation for Google's Gemini API.
    
    This class implements the Gemini API endpoints for text generation,
    embeddings, and multimodal embeddings using the Google Generative AI REST API.
    """
    
    def _get_endpoint(self, model: str, task: str):
        """
        Get the endpoint URL for a specific model and task.
        
        Args:
            model (str): The model identifier
            task (str): The task type (e.g., "generateContent", "embedContent")
            
        Returns:
            str: The complete URL for the API endpoint
        """
        base_url = self.metadata.get("endpoint", "https://generativelanguage.googleapis.com/v1beta/models")
        return f"{base_url}/{model}:{task}?key={self.credentials['api_key']}"

    def _get_headers(self):
        """
        Get the required headers for Gemini API requests.
        
        Returns:
            dict: Dictionary containing the required headers
        """
        return {"Content-Type": "application/json"}

    def ask(self, model: str, prompt: str, **kwargs):
        """
        Generate a text response using Google's Gemini API.
        
        Args:
            model (str): The model identifier (e.g., "gemini-pro")
            prompt (str): The input prompt for text generation
            **kwargs: Additional parameters for the Gemini API (temperature, max_output_tokens, etc.)
            
        Returns:
            str: The generated text response
            
        Raises:
            requests.HTTPError: If the API request fails
        """
        payload = {
            "contents": [
                {
                    "parts": [
                        {
                            "text": prompt
                        }
                    ]
                }
            ]
        }
        if "temperature" in kwargs or "max_output_tokens" in kwargs:
            payload["generationConfig"] = {
                k: v for k, v in kwargs.items() 
                if k in ["temperature", "max_output_tokens"]
            }
        endpoint = self._get_endpoint(model, "generateContent")
        data = self._make_request(
            "post",
            endpoint,
            self._get_headers(),
            payload
        )
        return data["candidates"][0]["content"]["parts"][0]["text"].strip()

    def embed(self, model: str, text: str, **kwargs):
        """
        Generate text embeddings using Google's Gemini API.
        
        Args:
            model (str): The model identifier (e.g., "embedding-001")
            text (str): The input text to generate embeddings for
            **kwargs: Additional parameters for the Gemini API
            
        Returns:
            str: A string representation of the embedding vector
            
        Raises:
            requests.HTTPError: If the API request fails
        """
        payload = {
            "model": f"models/{model}",
            "content": {
                "parts": [
                    {
                        "text": text
                    }
                ]
            }
        }
        endpoint = self._get_endpoint(model, "embedContent")
        data = self._make_request(
            "post",
            endpoint,
            self._get_headers(),
            payload
        )
        return str(data["embedding"]["values"])
    
    def multimodal_embed(self, model: str, content: dict, **kwargs):
        """
        Generate embeddings for multimodal content using Google Gemini.
        Supports text and image content for Gemini models.
        
        Args:
            model (str): The model identifier (e.g., "multimodalembedding")
            content (dict): Dictionary with text and/or image data
                           Format: {"text": "some text", "image": "base64_encoded_image"}
            **kwargs: Additional parameters for the embedding model (e.g., mime_type)
        
        Note:
            - The input_image should be a base64-encoded string including the mime type prefix (e.g., "data:image/png;base64,")
        
        Returns:
            str: A string representation of the embedding vector
            
        Raises:
            requests.HTTPError: If the API request fails
            Exception: If content doesn't contain at least text or image data
        """
        # Extract text and image from content
        input_text = content.get("text", "")
        input_image = content.get("image", "")
        
        # Build the parts array for the request
        parts = []
        
        if input_text:
            parts.append({"text": input_text})
        
        if input_image:
            mime_type = input_image.split(";")[0].split(":")[-1]
            image = input_image.split(',')[1]
            parts.append({
                "inline_data": {
                    "mime_type": mime_type,
                    "data": image
                }
            })
        
        if not parts:
            self.plpy.error("Content must contain at least text or image data.")
        
        # Build the payload for the Gemini API
        payload = {
            "model": f"models/{model}",
            "content": {"parts": parts}
        }
        
        endpoint = self._get_endpoint(model, "embedContent")
        data = self._make_request(
            "post",
            endpoint,
            self._get_headers(),
            payload
        )
        return str(data["embedding"]["values"])

    def vlm_ask(self, model: str, prompt: str, image: str, **kwargs):
        """
        Generate a text response using Google's Gemini API for visual language models.
        
        Args:
            model (str): The model identifier (e.g., "gemini-pro-vision")
            prompt (str): The input prompt for text generation
            image (str): The base64-encoded image string for visual context,
                         including the mime type prefix (e.g., "data:image/png;base64,")
            **kwargs: Additional parameters for the Gemini API
            
        Returns:
            str: The generated text response
            
        Raises:
            requests.HTTPError: If the API request fails
        """
        mime_type = image.split(";")[0].split(":")[-1]
        image = image.split(',')[1]

        payload = {
            "model": f"models/{model}",
            "contents": [
                {
                    "parts": [
                        {
                            "text": prompt
                        },
                        {
                            "inline_data": {
                                "mime_type": mime_type,
                                "data": image
                            }
                        }
                    ]
                }
            ]
        }
        
        endpoint = self._get_endpoint(model, "generateContent")
        data = self._make_request(
            "post",
            endpoint,
            self._get_headers(),
            payload
        )
        return data["candidates"][0]["content"]["parts"][0]["text"].strip()
