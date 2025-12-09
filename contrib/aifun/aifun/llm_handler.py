from typing import Union
import json

from .utils import get_plpy, cosine_similarity, CACHE, parse_jsonb
from .providers import get_provider_class

plpy = get_plpy()

def _get_provider_instance(provider_id: str):
    """
    Get a provider instance by ID
    
    This is an internal factory function that handles database lookups and provider instantiation.
    Uses Row Level Security (RLS) to ensure users can only access their own providers.
    Caches instances per user per provider_id for performance.
    
    Args:
        provider_id (str): The unique identifier of the provider
        
    Returns:
        object: An instance of the provider class, configured with API key and metadata
        
    Raises:
        Error: If the provider is not found for the current user
        Error: If the provider type is invalid
    """    
    current_user = plpy.execute("SELECT current_user")[0]['current_user']
    cache_key = f"{current_user}::{provider_id}"

    if cache_key in CACHE:
        return CACHE[cache_key]

    plan = plpy.prepare("""
        SELECT
            provider_type,
            api_key,
            metadata
        FROM
            aifun.providers
        WHERE
            owner_role = current_user AND provider_id = $1
    """, ["text"])
    results = plpy.execute(plan, [provider_id])

    if not results:
        plpy.error(f"Provider '{provider_id}' not found for current user.")

    config = results[0]
    provider_type = config['provider_type']
    api_key = config['api_key']
    metadata = json.loads(config['metadata']) if config['metadata'] else {}

    try:
        ProviderClass = get_provider_class(provider_type)
        # Create credentials object with API key for provider compatibility
        credentials = {"api_key": api_key}
        instance = ProviderClass(credentials, metadata)
        CACHE[cache_key] = instance
        return instance
    except ValueError as e:
        plpy.error(str(e))


# --- Public Functions (called by PostgreSQL) ---

def ask(provider_id: str, model: str, prompt: str) -> str:
    """
    Send a question to an AI model and get a response
    
    Uses the specified provider and model to send a question to an AI and get the model's response.
    Uses a default temperature parameter of 0 to ensure consistency in responses.
    
    Args:
        provider_id (str): The unique identifier of the provider
        model (str): The name of the AI model to use
        prompt (str): The question or prompt to send to the AI
        
    Returns:
        str: The response text from the AI model
        
    Example:
        answer = ask("openai_provider", "gpt-4", "What is artificial intelligence?")
    """
    provider = _get_provider_instance(provider_id)
    return provider.ask(model, prompt, temperature=0)

def embed(provider_id: str, model: str, text_to_embed: str) -> list[float]:
    """
    Generate vector embeddings for text
    
    Converts text to vector representation using the specified provider and model.
    Can be used for text similarity comparison, clustering, and other tasks.
    
    Args:
        provider_id (str): The unique identifier of the provider
        model (str): The name of the model to use for generating embeddings
        text_to_embed (str): The text to generate vector embeddings for
        
    Returns:
        list[float]: The vector embedding representation of the text
        
    Example:
        vector = embed("openai_provider", "text-embedding-ada-002", "This is a sample text")
    """
    provider = _get_provider_instance(provider_id)
    return provider.embed(model, text_to_embed)

def multimodal_embed(provider_id: str, model: str, content: dict) -> list[float]:
    """
    Generate vector embeddings for multimodal content
    
    Generates a unified vector representation for content containing multiple modalities such as text and images.
    Supports combinations of different content types for multimodal search, comparison, and other scenarios.
    
    Args:
        provider_id (str): The unique identifier of the provider
        model (str): The name of the model to use for generating multimodal embeddings
        content (dict): The content to embed, must be a dictionary
                               Expected format: {"text": "...", "image": "..."}
                               Image should be base64-encoded strings with mime type prefix (e.g., "data:image/png;base64,")
    
    Returns:
        list[float]: The vector embedding representation of the multimodal content
        
    Raises:
        Error: If the content is not in valid JSON format
        
    Example:
        content = {"text": "A cat sitting on a sofa", "image": "base64-encoded image data"}
        vector = multimodal_embed("openai_provider", "clip-embeddings", content)
    """
    provider = _get_provider_instance(provider_id)
    
    if "image" in content and content["image"] and not content["image"].startswith("data:image/"):
        plpy.error("Image content must include mime type prefix (e.g., 'data:image/png;base64,')")
    
    return provider.multimodal_embed(model, content)

def classify(provider_id: str, model: str, text_to_classify: str, labels: list[str]) -> str:
    """
    Classify text into predefined categories
    
    Uses an AI model to classify given text into one of the specified categories.
    The model will be prompted to return only the category name without any other content.
    
    Args:
        provider_id (str): The unique identifier of the provider
        model (str): The name of the model to use for classification
        text_to_classify (str): The text content to classify
        labels (list): A list of predefined categories
        
    Returns:
        str: The classification result, which is one of the labels
        
    Example:
        categories = ["technology", "sports", "entertainment"]
        result = classify("openai_provider", "gpt-4", "The latest iPhone has been released", categories)
        # Returns "technology"
    """
    provider = _get_provider_instance(provider_id)
    label_list_str = ", ".join([f"'{label}'" for label in labels])
    prompt = f"""Given the following text, classify it into one of the following categories: {label_list_str}.
Respond with only the chosen category name and nothing else.
Text: {text_to_classify}
Category:"""
    
    result = provider.ask(model, prompt, max_tokens=50, temperature=0)
    stripped_result = result.strip().strip("'\"")
    if stripped_result in labels:
        return stripped_result
    return result

def extract(provider_id: str, model: str, text_to_parse: str, json_schema: str) -> dict:
    """
    Extract structured information from text according to a JSON schema
    
    Uses an AI model to extract information from unstructured text and format the output according to a specified JSON schema.
    The function validates that the returned JSON is valid to ensure the output meets the expected format.
    
    Args:
        provider_id (str): The unique identifier of the provider
        model (str): The name of the model to use for information extraction
        text_to_parse (str): The text content to extract information from
        json_schema (str): The JSON schema that defines the output format
        
    Returns:
        dict: The extraction result formatted according to json_schema
        
    Raises:
        Error: If the model returns invalid JSON
        Error: If there is an error processing the JSON response
        
    Example:
        schema = '{"name": "string", "age": "number", "email": "string"}'
        text = "John is 25 years old and his email is john@example.com"
        result = extract("openai_provider", "gpt-4", text, schema)
        # Returns {"name": "John", "age": 25, "email": "john@example.com"}
    """
    provider = _get_provider_instance(provider_id)
    prompt = f"""Extract information from the following text according to the JSON schema.
Respond with only the extracted JSON object.
Schema: {json_schema}
Text: {text_to_parse}
JSON:"""

    # Parse and validate JSON to ensure it's properly formatted
    try:
        result = provider.ask(model, prompt, temperature=0)
        parsed_json = parse_jsonb(result)
        return parsed_json
    except json.JSONDecodeError as e:
        plpy.error(f"LLM returned invalid JSON: {result}. Error: {str(e)}")
    except Exception as e:
        plpy.error(f"Error processing JSON response: {str(e)}")

def summarize(provider_id: str, model: str, text_to_summarize: str, length: int = 50) -> str:
    """
    Generate a concise summary of text
    
    Uses an AI model to generate a concise summary of the given text.
    Uses a medium temperature parameter (0.5) to maintain some creativity while ensuring content accuracy.
    
    Args:
        provider_id (str): The unique identifier of the provider
        model (str): The name of the model to use for generating summaries
        text_to_summarize (str): The text content to summarize
        length (int, optional): The desired length of the summary (default is 50)
        
    Returns:
        str: The summary content of the text
        
    Example:
        long_text = "This is a long text content..."
        summary = summarize("openai_provider", "gpt-4", long_text)
    """
    provider = _get_provider_instance(provider_id)
    prompt = f"Provide a concise summary of the following text. The summary should be {length} words or less.\nText: {text_to_summarize}\nSummary:"
    return provider.ask(model, prompt, temperature=0.3)

def translate(provider_id: str, model: str, text_to_translate: str, target_language: str) -> str:
    """
    Translate text to a target language
    
    Uses an AI model to translate text to the specified target language.
    The model will be prompted to return only the translated text without any other content.
    
    Args:
        provider_id (str): The unique identifier of the provider
        model (str): The name of the model to use for translation
        text_to_translate (str): The text content to translate
        target_language (str): The name of the target language
        
    Returns:
        str: The translated text
        
    Example:
        translated = translate("openai_provider", "gpt-4", "Hello, world!", "Chinese")
        # Returns "你好，世界！"
    """
    provider = _get_provider_instance(provider_id)
    prompt = f"Translate the following text into {target_language}. Respond with only the translated text.\nText: {text_to_translate}\nTranslation:"
    return provider.ask(model, prompt, temperature=0)

def similarity(provider_id: str, model: str, text1: str, text2: str) -> float:
    """
    Calculate the similarity between two texts
    
    Evaluates the semantic similarity between two texts by converting them to vector embeddings and then calculating cosine similarity.
    The return value ranges from 0 to 1, where 1 indicates complete similarity and 0 indicates no similarity.
    
    Args:
        provider_id (str): The unique identifier of the provider
        model (str): The name of the model to use for generating embeddings
        text1 (str): The first text
        text2 (str): The second text
        
    Returns:
        float: The similarity score between the two texts, ranging from 0 to 1
        
    Raises:
        Error: If unable to parse the embedding string returned by the provider
        
    Example:
        score = similarity("openai_provider", "text-embedding-ada-002", 
                          "Artificial intelligence is the future", "AI will change the world")
        # Returns a similarity score between 0 and 1
    """
    provider = _get_provider_instance(provider_id)
    embedding_str1 = provider.embed(model, text1)
    embedding_str2 = provider.embed(model, text2)
    try:
        vec1 = json.loads(embedding_str1)
        vec2 = json.loads(embedding_str2)
    except (json.JSONDecodeError, TypeError):
        plpy.error("Failed to parse embedding string from provider.")
    return cosine_similarity(vec1, vec2)

def fix_grammar(provider_id: str, model: str, text: str) -> str:
    """
    Correct grammar and spelling errors in text
    
    Uses an AI model to detect and correct grammar and spelling errors in text.
    The model will be prompted to return only the corrected text without any other content.
    
    Args:
        provider_id (str): The unique identifier of the provider
        model (str): The name of the model to use for grammar correction
        text (str): The text to correct for grammar and spelling
        
    Returns:
        str: The corrected text
        
    Example:
        corrected = fix_grammar("openai_provider", "gpt-4", "I has a apple.")
        # Returns "I have an apple."
    """
    provider = _get_provider_instance(provider_id)
    prompt = f"Correct the grammar and spelling of the following text. Respond with only the corrected text and nothing else.\n\nText: \"{text}\"\n\nCorrected:"
    return provider.ask(model, prompt, temperature=0)

def chunk(text: str, chunk_size: int = 1000, overlap: int = 200) -> list[str]:
    """
    Split text into chunks of specified size with optional overlap
    
    Splits long text into smaller chunks for easier processing and analysis. Can specify the size of each chunk and the overlap between chunks,
    which helps maintain contextual continuity.
    
    Args:
        text (str): The text content to split
        chunk_size (int, optional): The size of each chunk, default is 1000 characters
        overlap (int, optional): The number of overlapping characters between chunks, default is 200
        
    Returns:
        list: A list containing the text chunks
        
    Raises:
        Error: If input text is not a string
        Error: If chunk_size is not a positive integer
        Error: If overlap is not a non-negative integer
        Error: If overlap is greater than or equal to chunk_size
        
    Example:
        long_text = "This is a long text content..."
        chunks = chunk(long_text, chunk_size=500, overlap=100)
        # Returns a list containing multiple text chunks
    """
    if not isinstance(text, str):
        plpy.error("Input 'text' must be a string.")
    if not isinstance(chunk_size, int) or chunk_size <= 0:
        plpy.error("'chunk_size' must be a positive integer.")
    if not isinstance(overlap, int) or overlap < 0:
        plpy.error("'overlap' must be a non-negative integer.")
    if overlap >= chunk_size:
        plpy.error("'overlap' must be less than 'chunk_size'.")

    chunks = []
    start = 0
    while start < len(text):
        end = start + chunk_size
        chunk_content = text[start:end]
        chunks.append(chunk_content)
        start += (chunk_size - overlap)
        if start >= len(text) - overlap and start < len(text): # Ensure last chunk is not just overlap
            if len(text) - (start + overlap) > 0: # If there's still significant text left
                pass # The loop will handle the last chunk
            else:
                break # Avoid creating tiny chunks at the very end if only overlap is left

    return chunks

def rerank(provider_id: str, model: str, query: str, documents: list[str]) -> list[str]:
    """
    Rerank a list of documents based on a query
    
    Uses an AI model to evaluate the relevance of each document in a list to a given query and reorder them from most to least relevant.
    The model will be prompted to return a JSON array containing the reranked documents.
    
    Args:
        provider_id (str): The unique identifier of the provider
        model (str): The name of the model to use for reranking
        query (str): The query text to evaluate document relevance against
        documents (list): A list of documents to rerank, each element is a string
        
    Returns:
        list: A list of documents ordered from most to least relevant
        
    Raises:
        Error: If input query is not a string
        Error: If input documents is not a list of strings
        Error: If an error occurs during the reranking process
        
    Example:
        docs = ["Article about machine learning", "Cooking recipe", "AI development trends"]
        ranked = rerank("openai_provider", "gpt-4", "AI technology", docs)
        # Returns a reranked list of documents with AI-related documents at the front
    """
    if not isinstance(documents, list) or not all(isinstance(d, str) for d in documents):
        plpy.error("Input 'documents' must be a list of strings.")

    provider = _get_provider_instance(provider_id)

    # Construct a prompt to instruct the LLM to rerank
    documents_str = "\n".join([f"Document {i+1}: {doc}" for i, doc in enumerate(documents)])
    prompt = f"""You are an expert document reranker. Your task is to reorder a list of documents based on their relevance to a given query.
Return a JSON array where each element is one of the original documents, ordered from most relevant to least relevant.
Do NOT include any other text or explanation, just the JSON array.

Query: "{query}"

Documents to rerank:
---
{documents_str}
---

Reranked Documents (JSON array):"""

    try:
        result = provider.ask(model, prompt, temperature=0)
        # Attempt to parse the result as JSON
        reranked_list = parse_jsonb(result)
        if not isinstance(reranked_list, list) or not all(isinstance(d, str) for d in reranked_list):
            plpy.warning(f"LLM returned non-list or non-string elements for reranking: {result}")
            return documents # Fallback to original order if parsing fails
        
        # Basic validation: ensure all returned documents were part of the original set
        # This is a heuristic, LLMs can hallucinate.
        if all(doc in documents for doc in reranked_list):
            return reranked_list
        else:
            plpy.warning(f"LLM returned documents not in original set during reranking. Returning original order. LLM output: {result}")
            return documents # Fallback to original order if validation fails

    except json.JSONDecodeError:
        plpy.warning(f"LLM returned non-JSON response for reranking. Returning original order. LLM output: {result}")
        return documents # Fallback to original order if parsing fails
    except Exception as e:
        plpy.error(f"Failed to rerank documents: {e}")
    return documents # Fallback in case of any other error

def extract_keywords(provider_id: str, model: str, text: str, num_keywords: int = 5) -> list[str]:
    """
    Extract a specified number of keywords from text
    
    Uses an AI model to extract the most important keywords or phrases from a given text.
    Can specify the number of keywords to extract, and the model will return a JSON array containing these keywords.
    
    Args:
        provider_id (str): The unique identifier of the provider
        model (str): The name of the model to use for keyword extraction
        text (str): The text content to extract keywords from
        num_keywords (int, optional): The number of keywords to extract, default is 5
        
    Returns:
        list: A list containing the extracted keywords
        
    Raises:
        Error: If input text is not a string
        Error: If num_keywords is not a positive integer
        Error: If an error occurs during the keyword extraction process
        
    Example:
        article = "Artificial intelligence is a branch of computer science that aims to create systems capable of performing tasks that typically require human intelligence."
        keywords = extract_keywords("openai_provider", "gpt-4", article, 3)
        # Might return ["artificial intelligence", "computer science", "intelligent tasks"]
    """
    if not isinstance(num_keywords, int) or num_keywords <= 0:
        plpy.error("'num_keywords' must be a positive integer.")

    provider = _get_provider_instance(provider_id)

    prompt = f"""Extract exactly {num_keywords} keywords or key phrases from the following text.
Return them as a JSON array of strings. Do NOT include any other text or explanation, just the JSON array.

Text: "{text}"

Keywords (JSON array):"""

    try:
        result = provider.ask(model, prompt, temperature=0)
        keywords_list = parse_jsonb(result)
        if not isinstance(keywords_list, list) or not all(isinstance(k, str) for k in keywords_list):
            plpy.warning(f"LLM returned non-list or non-string elements for keywords: {result}")
            return []
        return keywords_list
    except json.JSONDecodeError:
        plpy.warning(f"LLM returned non-JSON response for keywords. LLM output: {result}")
        return []
    except Exception as e:
        plpy.error(f"Failed to extract keywords: {e}")
    return []


def vlm_parse_pdf(
    provider_id: str,
    model: str,
    file_content: Union[str, bytes],
    prompt: str = "Extract all text and describe any images, charts, or visual elements in this PDF.",
) -> list[str]:
    """
    Parse PDF using Vision Language Model for enhanced text extraction and visual analysis

    This function converts PDF pages to images and uses a VLM to extract text and analyze visual elements.
    It provides more accurate OCR and can describe charts, diagrams, and other visual content.

    Args:
        provider_id (str): The unique identifier of the provider
        model (str): The name of the model to use for VLM processing
        file_content (Union[str, bytes]): PDF file content (base64 string or bytes)
        prompt (str): Custom prompt for text extraction and analysis

    Returns:
        list: List of extracted text/analysis for each page

    Raises:
        Error: If VLM processing fails
        Error: If PDF conversion fails
    """
    plpy = get_plpy()
    provider = _get_provider_instance(provider_id)

    try:
        import io
        import pypdfium2 as pdfium
        import base64

        if isinstance(file_content, str):
            if file_content.startswith("data:application/pdf;base64,"):
                file_content = file_content.split(",")[1]
            file_content = base64.b64decode(file_content)

        pdf = pdfium.PdfDocument(file_content)
        page_nums = len(pdf)
        results = []
        for i in range(page_nums):
            page = pdf[i]
            bitmap = page.render(scale=2)
            pil_image = bitmap.to_pil()

            buf = io.BytesIO()
            pil_image.save(buf, format="PNG")
            base64_image = f"data:image/png;base64,{base64.b64encode(buf.getvalue()).decode('utf-8')}"

            parsed_content = provider.vlm_ask(
                model=model,
                prompt=prompt,
                image=base64_image
            )

            results.append({
                "page_num": i + 1,
                "content": parsed_content
            })
        pdf.close()

        return {
            "metadata": {
                "pageCount": page_nums
            },
            "pages": results
        }

    except Exception as e:
        plpy.error(f"Error processing PDF with VLM: {e}")


def visual_qa(provider_id: str, model: str, image: Union[str, bytes], question: str):
    """
    Perform visual question answering on an image

    This function allows asking specific questions about image content and getting
    detailed answers based on visual analysis of the image.

    Args:
        provider_id (str): The unique identifier of the provider
        model (str): The name of the vision model to use
        image (Union[str, bytes]): Base64 encoded image content or bytes, if string, should include mime type prefix (e.g., "data:image/png;base64,")
        question (str): Question to ask about the image

    Returns:
        str: Answer to the question based on image analysis
    """
    provider = _get_provider_instance(provider_id)

    if isinstance(image, str):
        if not image.startswith("data:image/"):
            raise ValueError("Image string must include mime type prefix (e.g., 'data:image/png;base64,')")

        return provider.vlm_ask(model, question, image)

    from PIL import Image
    import io
    import base64

    image = Image.open(io.BytesIO(image))
    buf = io.BytesIO()
    image.save(buf, format="PNG")
    image = f"data:image/png;base64,{base64.b64encode(buf.getvalue()).decode('utf-8')}"

    return provider.vlm_ask(model, question, image)
