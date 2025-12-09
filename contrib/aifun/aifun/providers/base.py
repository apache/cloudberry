# py_handler/providers/base.py
import requests

from ..utils import get_plpy, retry_with_backoff

class BaseProvider:
    """
    Base class for all AI providers.
    
    This class defines the common interface that all provider implementations must follow.
    It provides basic functionality and abstract methods that concrete providers must implement.
    
    Attributes:
        credentials (dict): Authentication credentials for the provider
        metadata (dict): Additional metadata about the provider configuration
        plpy: PostgreSQL interface for error handling
    """
    
    def __init__(self, credentials: dict, metadata: dict = None):
        """
        Initialize the base provider with credentials and metadata.
        
        Args:
            credentials (dict): Authentication credentials for the provider
            metadata (dict, optional): Additional metadata about the provider configuration.
                                     Defaults to None.
        """
        self.credentials = credentials
        self.metadata = metadata or {}
        self.plpy = get_plpy()

    def ask(self, model: str, prompt: str, **kwargs) -> str:
        """
        Generate a text response from the given prompt.
        
        Args:
            model (str): The model identifier to use for generation
            prompt (str): The input prompt for text generation
            **kwargs: Additional parameters specific to the provider/model
            
        Returns:
            str: The generated text response
            
        Raises:
            NotImplementedError: If the provider doesn't support this method
        """
        raise NotImplementedError(f"{self.__class__.__name__} does not support 'ask'")

    def embed(self, model: str, text: str, **kwargs) -> str:
        """
        Generate text embeddings for the given text.
        
        Args:
            model (str): The model identifier to use for embedding generation
            text (str): The input text to generate embeddings for
            **kwargs: Additional parameters specific to the provider/model
            
        Returns:
            str: A string representation of the embedding vector
            
        Raises:
            NotImplementedError: If the provider doesn't support this method
        """
        raise NotImplementedError(f"{self.__class__.__name__} does not support 'embedding'")
    
    def multimodal_embed(self, model: str, content: dict, **kwargs) -> str:
        """
        Generate embeddings for multimodal content (text, images, etc.)
        
        Args:
            model (str): The model identifier for embedding generation
            content (dict): A dictionary containing the content to embed
                           Expected format: {"text": "...", "image": "..."} or similar
            **kwargs: Additional parameters for the embedding model
        
        Returns:
            str: A string representation of the embedding vector
            
        Raises:
            NotImplementedError: If the provider doesn't support this method
        """
        raise NotImplementedError(f"{self.__class__.__name__} does not support 'multimodal_embedding'")

    def vlm_ask(self, model: str, prompt: str, image: str, **kwargs) -> str:
        """
        Generate a text response from the given prompt using a visual language model.
        
        Args:
            model (str): The model identifier to use for generation
            prompt (str): The input prompt for text generation
            image (str): The base64-encoded image string for visual context
            **kwargs: Additional parameters specific to the provider/model
            
        Returns:
            str: The generated text response
            
        Raises:
            NotImplementedError: If the provider doesn't support this method
        """
        raise NotImplementedError(f"{self.__class__.__name__} does not support 'vlm_ask'")

class RequestsBasedProvider(BaseProvider):
    """
    Base class for providers that use HTTP requests.
    
    This class provides common functionality for providers that communicate
    with their APIs through HTTP requests.
    """
    
    @retry_with_backoff()
    def _make_request(self, method, url, headers, json_payload):
        """
        Make an HTTP request with retry logic.
        
        Args:
            method (str): HTTP method (GET, POST, etc.)
            url (str): The endpoint URL
            headers (dict): HTTP headers to include in the request
            json_payload (dict): JSON payload to send in the request body
            
        Returns:
            dict: The JSON response from the API
            
        Raises:
            requests.HTTPError: If the request fails
        """
        response = requests.request(method, url, headers=headers, json=json_payload, timeout=60)
        response.raise_for_status()
        return response.json()
