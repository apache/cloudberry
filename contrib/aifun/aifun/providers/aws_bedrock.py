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

import json
import boto3

from .base import BaseProvider
from ..utils import retry_with_backoff


class AWSBedrockProvider(BaseProvider):
    """
    Provider implementation for AWS Bedrock service.
    
    This class implements the AWS Bedrock API endpoints for text generation,
    embeddings, and multimodal embeddings using the boto3 SDK.
    """
    
    def __init__(self, credentials: dict, metadata: dict = None):
        """
        Initialize the AWS Bedrock provider with credentials and metadata.
        
        Args:
            credentials (dict): AWS credentials including access key, secret key, and session token
            metadata (dict, optional): Additional metadata including AWS region. Defaults to None.
            
        Raises:
            Exception: If boto3 is not installed
        """
        super().__init__(credentials, metadata)
        if boto3 is None:
            self.plpy.error("boto3 is not installed. Please run 'pip install boto3' to use AWS Bedrock.")
        
        self.client = boto3.client(
            "bedrock-runtime",
            aws_access_key_id=self.credentials.get("aws_access_key_id"),
            aws_secret_access_key=self.credentials.get("aws_secret_access_key"),
            aws_session_token=self.credentials.get("aws_session_token"),
            region_name=self.metadata.get("region")
        )

    @retry_with_backoff()
    def _invoke_model(self, model_id: str, body: str):
        """
        Invoke a model with the given body using AWS Bedrock.
        
        Args:
            model_id (str): The model identifier to invoke
            body (str): JSON string containing the request body
            
        Returns:
            dict: The response from the model invocation
            
        Raises:
            Exception: If the model invocation fails
        """
        response = self.client.invoke_model(
            body=body, 
            modelId=model_id, 
            accept="application/json", 
            contentType="application/json"
        )
        return json.loads(response.get('body').read())

    def ask(self, model: str, prompt: str, **kwargs):
        """
        Generate a text response using AWS Bedrock models.
        
        Args:
            model (str): The model identifier (e.g., "anthropic.claude-3-opus-20240229")
            prompt (str): The input prompt for text generation
            **kwargs: Additional parameters specific to the model
            
        Returns:
            str: The generated text response
            
        Raises:
            Exception: If the model is not supported or the request fails
        """
        body = {}
        if "anthropic.claude" in model:
            if "v3" in model:
                 body = json.dumps({
                     "messages": [
                        {
                            "role": "user",
                            "content": prompt
                        }
                    ], 
                     "max_tokens": kwargs.pop("max_tokens", 4096), 
                     "anthropic_version": "bedrock-2023-05-31", 
                     **kwargs
                 })
                 response_body = self._invoke_model(model, body)
                 return response_body.get('content')[0].get('text')
            else: # Claude v1/v2
                body = json.dumps({
                    "prompt": f"\n\nHuman: {prompt}\n\nAssistant:", 
                    "max_tokens_to_sample": kwargs.pop("max_tokens_to_sample", 4096), 
                    **kwargs
                })
                response_body = self._invoke_model(model, body)
                return response_body.get('completion')
        elif "amazon.titan" in model:
            body = json.dumps({
                "inputText": prompt, 
                "textGenerationConfig": {"maxTokenCount": kwargs.pop("maxTokenCount", 4096), **kwargs}
            })
            response_body = self._invoke_model(model, body)
            return response_body.get('results')[0].get('outputText')
        else:
            self.plpy.error(f"Model '{model}' is not currently supported by the AWSBedrockProvider.")

    def embed(self, model: str, text: str, **kwargs):
        """
        Generate text embeddings using AWS Bedrock models.
        
        Args:
            model (str): The model identifier (e.g., "amazon.titan-embed-text-v1")
            text (str): The input text to generate embeddings for
            **kwargs: Additional parameters specific to the model
            
        Returns:
            str: A string representation of the embedding vector
            
        Raises:
            Exception: If the model is not supported for embeddings or the request fails
        """
        body = {}
        if "amazon.titan" in model:
            body = json.dumps({"inputText": text})
            response_body = self._invoke_model(model, body)
            return str(response_body.get('embedding'))
        else:
            self.plpy.error(f"Model '{model}' is not currently supported for embeddings by the AWSBedrockProvider.")
    
    def multimodal_embed(self, model, content, **kwargs):
        """
        Generate embeddings for multimodal content using AWS Bedrock.
        Supports text and image content for models like Amazon Titan Multimodal Embeddings.
        
        Args:
            model (str): The model identifier (e.g., "amazon.titan-embed-image-v1")
            content (dict): Dictionary with text and/or image data
                           Format: {"text": "some text", "image": "base64_encoded_image"}
            **kwargs: Additional parameters for the embedding model
        
        Notes:
            - The input_image should include the mime type prefix (e.g., "data:image/png;base64,")
            
        Returns:
            str: A string representation of the embedding vector
            
        Raises:
            Exception: If the model is not supported for multimodal embeddings or the request fails
        """
        if "amazon.titan-embed-image" in model:
            # Extract text and image from content
            input_text = content.get("text", "")
            input_image = content.get("image", "")
            
            # Build the request body for Titan Multimodal Embeddings
            body = {
                "inputImage": input_image,
                "inputText": input_text,
                "embeddingConfig": {
                    "outputEmbeddingLength": kwargs.get("embedding_length", 1024)
                }
            }
            
            response_body = self._invoke_model(model, json.dumps(body))
            return str(response_body.get('embedding'))
        else:
            self.plpy.error(f"Model '{model}' is not currently supported for multimodal embeddings by the AWSBedrockProvider.")

    def vlm_ask(self, model, prompt, image, **kwargs):
        """
        Generate a text response using AWS Bedrock models for visual language models.
        
        Args:
            model (str): The model identifier (e.g., "anthropic.claude-3-opus-20240229")
            prompt (str): The input prompt for text generation
            image (str): The base64-encoded image string for visual context,
                         including the mime type prefix (e.g., "data:image/png;base64,")
            **kwargs: Additional parameters specific to the model
            
        Returns:
            str: The generated text response
            
        Raises:
            Exception: If the model is not supported or the request fails
        """
        body = {}
        if "anthropic.claude" in model:
            if "v3" in model:
                 body = json.dumps({
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
                     "max_tokens": kwargs.pop("max_tokens", 4096), 
                     **kwargs
                 })
                 response_body = self._invoke_model(model, body)
                 return response_body.get('content')[0].get('text')
            else: # Claude v1/v2
                body = json.dumps({
                    "prompt": f"\n\nHuman: {prompt}\n\nAssistant:", 
                    "max_tokens_to_sample": 4096, 
                    **kwargs
                })
                response_body = self._invoke_model(model, body)
                return response_body.get('completion')
        else:
            self.plpy.error(f"Model '{model}' is not currently supported for visual language models by the AWSBedrockProvider.")
    