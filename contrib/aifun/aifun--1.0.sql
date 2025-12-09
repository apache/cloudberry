\echo Use "CREATE EXTENSION aifun" to load this file. \quit

-- SECURITY WARNING: This extension handles sensitive API keys and credentials.
-- Row Level Security (RLS) is applied to ensure proper isolation.
-- Each user can only access their own provider configurations.

CREATE SCHEMA IF NOT EXISTS aifun;

-- Table to store provider configurations for each user
-- This table uses Row Level Security (RLS) to ensure users can only access their own providers
CREATE TABLE IF NOT EXISTS aifun.providers (
    owner_role TEXT NOT NULL DEFAULT current_user,
    provider_id TEXT NOT NULL,
    provider_type TEXT NOT NULL,
    api_key TEXT NOT NULL,
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (owner_role, provider_id)
) DISTRIBUTED REPLICATED;

-- Enable Row Level Security on the providers table
ALTER TABLE aifun.providers ENABLE ROW LEVEL SECURITY;

-- Create RLS policy to ensure users can only access their own providers
CREATE POLICY user_isolation_policy ON aifun.providers
    FOR ALL TO public
    USING (owner_role = current_user)
    WITH CHECK (owner_role = current_user);

-- Add or update a provider for the current user
CREATE OR REPLACE FUNCTION aifun.add_provider(
    p_id TEXT,
    p_type TEXT,
    p_api_key TEXT,
    p_metadata JSONB DEFAULT NULL
) RETURNS void AS $$
BEGIN
    INSERT INTO aifun.providers (owner_role, provider_id, provider_type, api_key, metadata)
    VALUES (
        current_user,
        p_id,
        p_type,
        p_api_key,
        p_metadata
    )
    ON CONFLICT (owner_role, provider_id) DO UPDATE
    SET provider_type = EXCLUDED.provider_type,
        api_key = EXCLUDED.api_key,
        metadata = EXCLUDED.metadata;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION aifun.add_provider(TEXT, TEXT, TEXT, JSONB) IS 'Adds a new AI provider configuration or updates an existing one for the current user.

Parameters:
- p_id: Unique provider identifier
- p_type: Provider type (e.g., "openai", "anthropic", "gemini", etc.)
- p_api_key: API key for authentication
- p_metadata: Optional JSON with additional configuration

Each user can only access their own providers due to Row Level Security.';

-- Function to remove a provider configuration for the current user
CREATE OR REPLACE FUNCTION aifun.remove_provider(p_id TEXT)
RETURNS void AS $$
BEGIN
    DELETE FROM aifun.providers 
    WHERE owner_role = current_user AND provider_id = p_id;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION aifun.remove_provider(TEXT) IS 'Removes an AI provider configuration for the current user.

Parameters:
- p_id: The unique identifier of the provider to remove

Users can only remove their own providers due to Row Level Security. This action is irreversible.';

-- Function to get API key for a provider (internal use only)
CREATE OR REPLACE FUNCTION aifun._get_provider_key(p_provider_id TEXT)
RETURNS TEXT AS $$
DECLARE
    v_api_key TEXT;
BEGIN
    SELECT api_key INTO v_api_key 
    FROM aifun.providers 
    WHERE owner_role = current_user AND provider_id = p_provider_id;
    
    IF v_api_key IS NULL THEN
        RAISE EXCEPTION 'Provider not found or access denied';
    END IF;
    
    RETURN v_api_key;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION aifun._get_provider_key(TEXT) IS 'Internal function that retrieves the API key for a specific provider.

Parameters:
- p_provider_id: The unique identifier of the provider

This function is restricted to the current user''s providers due to Row Level Security. Raises an exception if the provider is not found or access is denied.';

-- Function to update provider API key
CREATE OR REPLACE FUNCTION aifun.update_api_key(p_provider_id TEXT, p_api_key TEXT)
RETURNS void AS $$
BEGIN
    UPDATE aifun.providers 
    SET api_key = p_api_key 
    WHERE owner_role = current_user AND provider_id = p_provider_id;
    
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Provider not found or access denied';
    END IF;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION aifun.update_api_key(TEXT, TEXT) IS 'Updates the API key for an existing AI provider configuration.

Parameters:
- p_provider_id: The unique identifier of the provider
- p_api_key: The new API key

Users can only update their own providers due to Row Level Security. Raises an exception if the provider is not found.';

-- Function to check if a provider exists for current user
CREATE OR REPLACE FUNCTION aifun.has_provider(p_provider_id TEXT)
RETURNS BOOLEAN AS $$
BEGIN
    RETURN EXISTS (
        SELECT 1 
        FROM aifun.providers 
        WHERE owner_role = current_user AND provider_id = p_provider_id
    );
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION aifun.has_provider(TEXT) IS 'Checks if a specific AI provider configuration exists for the current user.

Parameters:
- p_provider_id: The unique identifier of the provider to check

Returns true if the provider exists and is accessible, false otherwise. This function respects Row Level Security.';

-- Function to ask a question using a specified provider and model
CREATE OR REPLACE FUNCTION aifun.ask(provider TEXT, model TEXT, prompt TEXT)
RETURNS TEXT AS $$
    try:
        from aifun.llm_handler import ask
        return ask(provider, model, prompt)
    except Exception as e:
        plpy.error(f"Error in AI function: {e}")
$$ LANGUAGE plpython3u;

COMMENT ON FUNCTION aifun.ask(TEXT, TEXT, TEXT) IS 'Sends a question or prompt to an AI model and returns the response.

Parameters:
- provider: The unique identifier of the provider
- model: The specific model to use
- prompt: The question or prompt to send

Returns the AI model''s response as text. Raises an exception if the provider or model is not available.';

-- Function to ask a question with context using a specified provider and model
CREATE OR REPLACE FUNCTION aifun.ask(provider TEXT, model TEXT, prompt TEXT, context TEXT)
RETURNS TEXT AS $$
    try:
        from aifun.llm_handler import ask
        
        if context:
            enhanced_prompt = f"Context: {context}\n\nQuestion: {prompt}"
        else:
            enhanced_prompt = prompt
        
        return ask(provider, model, enhanced_prompt)
    except Exception as e:
        plpy.error(f"Error in AI function: {e}")
$$ LANGUAGE plpython3u;

COMMENT ON FUNCTION aifun.ask(TEXT, TEXT, TEXT, TEXT) IS 'Sends a question or prompt with context to an AI model and returns the response.

Parameters:
- provider: The unique identifier of the provider
- model: The specific model to use
- prompt: The question or prompt to send
- context: Additional context information as plain text

Returns the AI model''s response as text. The context is provided to the model to help with generating more relevant responses. Raises an exception if the provider or model is not available.';

-- Function to have a conversation with an AI model using a specified provider and model
CREATE OR REPLACE FUNCTION aifun.chat(provider TEXT, model TEXT, messages JSONB)
RETURNS TEXT AS $$
    try:
        from aifun.llm_handler import ask
        import json
        
        # Convert messages to a list of dictionaries
        message_list = []
        if messages is not None:
            message_list = list(messages)
        
        # Format messages for the AI model
        formatted_messages = []
        for msg in message_list:
            if isinstance(msg, dict) and 'role' in msg and 'content' in msg:
                formatted_messages.append(msg)
        
        # Build conversation prompt
        conversation_prompt = ""
        system_messages = [msg for msg in formatted_messages if msg['role'] == 'system']
        if system_messages:
            conversation_prompt += f"System: {system_messages[0]['content']}\n\n"
        
        for msg in formatted_messages:
            if msg['role'] == 'user':
                conversation_prompt += f"User: {msg['content']}\n"
            elif msg['role'] == 'assistant':
                conversation_prompt += f"Assistant: {msg['content']}\n"
        
        conversation_prompt += "Assistant: "
        
        return ask(provider, model, conversation_prompt)
    except Exception as e:
        plpy.error(f"Error in AI function: {e}")
$$ LANGUAGE plpython3u;

COMMENT ON FUNCTION aifun.chat(TEXT, TEXT, JSONB) IS 'Has a conversation with an AI model using a specified provider and model.

Parameters:
- provider: The unique identifier of the provider
- model: The specific model to use
- messages: Array of message objects in JSONB format, each with ''role'' and ''content'' fields

Returns the AI model''s response as text. The messages should be an array of objects with role (system/user/assistant) and content fields. Raises an exception if the provider or model is not available.';

-- Function to generate an embedding for a given text using a specified provider and model
CREATE OR REPLACE FUNCTION aifun.embed(provider TEXT, model TEXT, text_to_embed TEXT)
RETURNS vector AS $$
    try:
        from aifun.llm_handler import embed
        return embed(provider, model, text_to_embed)
    except Exception as e:
        plpy.error(f"Error in AI function: {e}")
$$ LANGUAGE plpython3u;

COMMENT ON FUNCTION aifun.embed(TEXT, TEXT, TEXT) IS 'Generates a vector embedding for the given text using an AI model.

Parameters:
- provider: The unique identifier of the provider
- model: The specific embedding model to use
- text_to_embed: The text to embed

Returns the embedding as a vector of floating-point numbers. Raises an exception if the provider or model is not available.';

-- Function to generate an embedding for multimodal content (text, images, etc.) using a specified provider and model
CREATE OR REPLACE FUNCTION aifun.multimodal_embed(provider TEXT, model TEXT, content JSONB)
RETURNS vector AS $$
    try:
        import json
        from aifun.llm_handler import multimodal_embed

        return multimodal_embed(
            provider,
            model,
            json.loads(content)
        )
    except Exception as e:
        plpy.error(f"Error in AI function: {e}")
$$ LANGUAGE plpython3u;

COMMENT ON FUNCTION aifun.multimodal_embed(TEXT, TEXT, JSONB) IS 'Generates a vector embedding for multimodal content (text, images, etc.) using an AI model.

Parameters:
- provider: The unique identifier of the provider
- model: The specific embedding model to use
- content: JSONB object containing the multimodal content to embed,
           expected format: {"text": "...", "image": "data:image/png;base64,..."}

Returns the embedding as a vector of floating-point numbers. Raises an exception if the provider or model is not available.';

-- Function to generate an embedding for multimodal content (text, images, etc.) using a specified provider and model
CREATE OR REPLACE FUNCTION aifun.multimodal_embed(provider TEXT, model TEXT, text TEXT DEFAULT NULL, image TEXT DEFAULT NULL)
RETURNS vector AS $$
    try:
        import json
        from aifun.llm_handler import multimodal_embed

        return multimodal_embed(
            provider,
            model,
            {
                "text": text,
                "image": image
            }
        )
    except Exception as e:
        plpy.error(f"Error in AI function: {e}")
$$ LANGUAGE plpython3u;

COMMENT ON FUNCTION aifun.multimodal_embed(TEXT, TEXT, TEXT, TEXT) IS 'Generates a vector embedding for multimodal content (text, images, etc.) using an AI model.

Parameters:
- provider: The unique identifier of the provider
- model: The specific embedding model to use
- text: The text to embed (optional)
- image: The image to embed (optional, base64-encoded), expected format: "data:image/png;base64,..."

Returns the embedding as a vector of floating-point numbers. Raises an exception if the provider or model is not available.';

-- Function to generate an embedding for multimodal content (text, images, etc.) using a specified provider and model
CREATE OR REPLACE FUNCTION aifun.multimodal_embed(provider TEXT, model TEXT, text TEXT DEFAULT NULL, image BYTEA DEFAULT NULL)
RETURNS vector AS $$
    try:
        import io
        import json
        import base64
        from PIL import Image
        from aifun.llm_handler import multimodal_embed
        
        base64_image = None
        if image is not None:
            pil_image = Image.open(io.BytesIO(image))
            buf = io.BytesIO()
            pil_image.save(buf, format='PNG')
            base64_image = base64.b64encode(buf.getvalue()).decode('utf-8')
            base64_image = f"data:image/png;base64,{base64_image}"

        return multimodal_embed(
            provider,
            model,
            {
                "text": text,
                "image": base64_image
            }
        )
    except Exception as e:
        plpy.error(f"Error in AI function: {e}")
$$ LANGUAGE plpython3u;

COMMENT ON FUNCTION aifun.multimodal_embed(TEXT, TEXT, TEXT, BYTEA) IS 'Generates a vector embedding for multimodal content (text, images, etc.) using an AI model.

Parameters:
- provider: The unique identifier of the provider
- model: The specific embedding model to use
- text: The text to embed (optional)
- image: The image to embed (optional, binary data), expected format: BYTEA

Returns the embedding as a vector of floating-point numbers. Raises an exception if the provider or model is not available.';


-- Function to classify a text using a specified provider and model
CREATE OR REPLACE FUNCTION aifun.classify(provider TEXT, model TEXT, text_to_classify TEXT, labels TEXT[])
RETURNS TEXT AS $$
    try:
        from aifun.llm_handler import classify
        return classify(provider, model, text_to_classify, labels)
    except Exception as e:
        plpy.error(f"Error in AI function: {e}")
$$ LANGUAGE plpython3u;

COMMENT ON FUNCTION aifun.classify(TEXT, TEXT, TEXT, TEXT[]) IS 'Classifies text into predefined categories using an AI model.

Parameters:
- provider: The unique identifier of the provider
- model: The specific model to use
- text_to_classify: The text to classify
- labels: Array of category labels to classify into

Returns the most likely category as text. Raises an exception if the provider or model is not available.';

-- Function to extract structured information from a text using a specified provider and model
CREATE OR REPLACE FUNCTION aifun.extract(provider TEXT, model TEXT, text_to_parse TEXT, json_schema JSONB)
RETURNS JSONB AS $$
    import json
    try:
        from aifun.llm_handler import extract
        data = extract(provider, model, text_to_parse, json_schema)
        return json.dumps(data)
    except Exception as e:
        plpy.error(f"Error in AI function: {e}")
$$ LANGUAGE plpython3u;

COMMENT ON FUNCTION aifun.extract(TEXT, TEXT, TEXT, JSONB) IS 'Extracts structured information from a text using a specified provider and model.

Parameters:
- provider: The unique identifier of the provider
- model: The specific extraction model to use
- text_to_extract: The text to extract information from
- p_options: Optional JSON with additional parameters

Returns a JSON object with the extracted information.';

-- Function to summarize a text using a specified provider and model
CREATE OR REPLACE FUNCTION aifun.summarize(provider TEXT, model TEXT, text_to_summarize TEXT, length INTEGER DEFAULT 50)
RETURNS TEXT AS $$
    try:
        from aifun.llm_handler import summarize
        return summarize(provider, model, text_to_summarize, length)
    except Exception as e:
        plpy.error(f"Error in AI function: {e}")
$$ LANGUAGE plpython3u;

COMMENT ON FUNCTION aifun.summarize(TEXT, TEXT, TEXT, INTEGER) IS 'Generates a concise summary of the given text using an AI model.

Parameters:
- provider: The unique identifier of the provider
- model: The specific model to use
- text_to_summarize: The text to summarize
- length: Optional length of the summary (default is 50)

Returns the summary as text. Raises an exception if the provider or model is not available.';

-- Function to translate a text using a specified provider and model
CREATE OR REPLACE FUNCTION aifun.translate(provider TEXT, model TEXT, text_to_translate TEXT, target_language TEXT)
RETURNS TEXT AS $$
    try:
        from aifun.llm_handler import translate
        return translate(provider, model, text_to_translate, target_language)
    except Exception as e:
        plpy.error(f"Error in AI function: {e}")
$$ LANGUAGE plpython3u;

COMMENT ON FUNCTION aifun.translate(TEXT, TEXT, TEXT, TEXT) IS 'Translates text from one language to another using an AI model.

Parameters:
- provider: The unique identifier of the provider
- model: The specific model to use
- text_to_translate: The text to translate
- target_language: The target language code or name

Returns the translated text. Raises an exception if the provider or model is not available.';

-- Function to calculate the similarity between two texts using a specified provider and model
CREATE OR REPLACE FUNCTION aifun.similarity(provider TEXT, model TEXT, text1 TEXT, text2 TEXT)
RETURNS float8 AS $$
    try:
        from aifun.llm_handler import similarity
        return similarity(provider, model, text1, text2)
    except Exception as e:
        plpy.error(f"Error in AI function: {e}")
$$ LANGUAGE plpython3u;

COMMENT ON FUNCTION aifun.similarity(TEXT, TEXT, TEXT, TEXT) IS 'Calculates the similarity between two texts using a specified provider and model.

Parameters:
- provider: The unique identifier of the provider
- model: The specific similarity model to use
- text1: The first text to compare
- text2: The second text to compare

Returns a similarity score between 0 and 1.';

-- Function to fix grammar in a text using a specified provider and model
CREATE OR REPLACE FUNCTION aifun.fix_grammar(provider TEXT, model TEXT, text TEXT)
RETURNS TEXT AS $$
    try:
        from aifun.llm_handler import fix_grammar
        return fix_grammar(provider, model, text)
    except Exception as e:
        plpy.error(f"Error in AI function: {e}")
$$ LANGUAGE plpython3u;

COMMENT ON FUNCTION aifun.fix_grammar(TEXT, TEXT, TEXT) IS 'Fixes grammar in a text using a specified provider and model.

Parameters:
- provider: The unique identifier of the provider
- model: The specific grammar correction model to use
- text_to_fix: The text with grammar to fix

Returns the text with corrected grammar.';

-- Function to chunk a text into smaller parts using a specified chunk size and overlap
CREATE OR REPLACE FUNCTION aifun.chunk(text TEXT, chunk_size INTEGER DEFAULT 1000, overlap INTEGER DEFAULT 200)
RETURNS TEXT[] AS $$
    try:
        from aifun.llm_handler import chunk
        return chunk(text, chunk_size, overlap)
    except Exception as e:
        plpy.error(f"Error in AI function: {e}")
$$ LANGUAGE plpython3u;

COMMENT ON FUNCTION aifun.chunk(TEXT, INTEGER, INTEGER) IS 'Splits a long text into smaller, overlapping chunks for processing.

Parameters:
- text_to_chunk: The text to chunk
- chunk_size: The size of each chunk
- overlap: The overlap between chunks

Returns an array of text chunks. Useful for processing long documents with models that have context length limitations.';

-- Function to rerank documents based on a query using a specified provider and model
CREATE OR REPLACE FUNCTION aifun.rerank(provider TEXT, model TEXT, query TEXT, documents TEXT[])
RETURNS TEXT[] AS $$
    try:
        from aifun.llm_handler import rerank
        return rerank(provider, model, query, documents)
    except Exception as e:
        plpy.error(f"Error in AI function: {e}")
$$ LANGUAGE plpython3u;

COMMENT ON FUNCTION aifun.rerank(TEXT, TEXT, TEXT, TEXT[]) IS 'Reranks documents based on their relevance to a query using an AI model.

Parameters:
- provider: The unique identifier of the provider
- model: The specific model to use
- query: The search query
- documents: Array of documents to rerank

Returns an array of document indices sorted by relevance. Raises an exception if the provider or model is not available.';

-- Function to extract keywords from a text using a specified provider and model
-- This function uses to ensure proper access control
CREATE OR REPLACE FUNCTION aifun.extract_keywords(provider TEXT, model TEXT, text TEXT, num_keywords INTEGER DEFAULT 5)
RETURNS TEXT[] AS $$
    try:
        from aifun.llm_handler import extract_keywords
        return extract_keywords(provider, model, text, num_keywords)
    except Exception as e:
        plpy.error(f"Error in AI function: {e}")
$$ LANGUAGE plpython3u;

COMMENT ON FUNCTION aifun.extract_keywords(TEXT, TEXT, TEXT, INTEGER) IS 'Extracts keywords from a text using a specified provider and model.

Parameters:
- provider: The unique identifier of the provider
- model: The specific keyword extraction model to use
- text_to_extract: The text to extract keywords from
- num_keywords: The number of keywords to extract (default: 5)

Returns an array of keywords.';


-- Function to parse PDF file content and extract text (TEXT version)
CREATE OR REPLACE FUNCTION aifun.parse_pdf(file_content_base64 TEXT)
RETURNS JSONB AS $$
    try:
        import json
        from aifun.parser import parse_pdf
        return json.dumps(parse_pdf(file_content_base64))
    except Exception as e:
        plpy.error(f"Error parsing PDF file: {e}")
$$ LANGUAGE plpython3u;

COMMENT ON FUNCTION aifun.parse_pdf(TEXT) IS 'Extracts text from a base64-encoded PDF file.

Parameters:
- file_content_base64: Base64-encoded PDF file content

Returns the extracted pages from the PDF file as a JSON array. Uses PyPDF2 library for PDF parsing.
Raises an exception if the file is not a valid PDF or if any error occurs during processing.';

-- Function to parse PDF file content and extract text (BYTEA version)
CREATE OR REPLACE FUNCTION aifun.parse_pdf(file_content_bytea BYTEA)
RETURNS JSONB AS $$
    try:
        import json
        from aifun.parser import parse_pdf
        return json.dumps(parse_pdf(file_content_bytea))
    except Exception as e:
        plpy.error(f"Error parsing PDF file from bytea: {e}")
$$ LANGUAGE plpython3u;


COMMENT ON FUNCTION aifun.parse_pdf(BYTEA) IS 'Extracts text from a binary PDF file (BYTEA).

Parameters:
- file_content_bytea: Binary PDF file content

Returns the extracted pages from the PDF file as a JSON array. Uses PyPDF2 library for PDF parsing.
Raises an exception if the file is not a valid PDF or if any error occurs during processing.';


-- Function to parse DOCX file content and extract text
CREATE OR REPLACE FUNCTION aifun.parse_docx(file_content_base64 TEXT)
RETURNS JSONB AS $$
    try:
        import json
        from aifun.parser import parse_docx
        return json.dumps(parse_docx(file_content_base64))
    except Exception as e:
        plpy.error(f"Error parsing DOCX file: {e}")
$$ LANGUAGE plpython3u;

COMMENT ON FUNCTION aifun.parse_docx(TEXT) IS 'Extracts text from a base64-encoded DOCX file.

Parameters:
- file_content_base64: Base64-encoded DOCX file content

Returns the extracted pages from the DOCX file as a JSON array. Uses python-docx library for DOCX parsing.
Raises an exception if the file is not a valid DOCX or if any error occurs during processing.';

-- Function to parse DOCX file content and extract text (BYTEA version)
CREATE OR REPLACE FUNCTION aifun.parse_docx(file_content_bytea BYTEA)
RETURNS JSONB AS $$
    try:
        import json
        from aifun.parser import parse_docx
        return json.dumps(parse_docx(file_content_bytea))
    except Exception as e:
        plpy.error(f"Error parsing DOCX file from bytea: {e}")
$$ LANGUAGE plpython3u;

COMMENT ON FUNCTION aifun.parse_docx(BYTEA) IS 'Extracts text from a binary DOCX file (BYTEA).

Parameters:
- file_content_bytea: Binary DOCX file content

Returns the extracted pages from the DOCX file as a JSON array. Uses python-docx library for DOCX parsing.
Raises an exception if the file is not a valid DOCX or if any error occurs during processing.';

-- Function to parse PPTX file content and extract text
CREATE OR REPLACE FUNCTION aifun.parse_pptx(file_content_base64 TEXT)
RETURNS JSONB AS $$
    try:
        import json
        from aifun.parser import parse_pptx
        return json.dumps(parse_pptx(file_content_base64))
    except Exception as e:
        plpy.error(f"Error parsing PPTX file from base64: {e}")
$$ LANGUAGE plpython3u;

COMMENT ON FUNCTION aifun.parse_pptx(TEXT) IS 'Extracts text from a base64-encoded PPTX file.

Parameters:
- file_content_base64: Base64-encoded PPTX file content

Returns the extracted slides from the PPTX file as a JSON array. Uses python-pptx library for PPTX parsing.
Raises an exception if the file is not a valid PPTX or if any error occurs during processing.';


-- Function to parse PPTX file content and extract text (BYTEA version)
CREATE OR REPLACE FUNCTION aifun.parse_pptx(file_content_bytea BYTEA)
RETURNS JSONB AS $$
    try:
        import json
        from aifun.parser import parse_pptx
        return json.dumps(parse_pptx(file_content_bytea))
    except Exception as e:
        plpy.error(f"Error parsing PPTX file from bytea: {e}")
$$ LANGUAGE plpython3u;

COMMENT ON FUNCTION aifun.parse_pptx(BYTEA) IS 'Extracts text from a binary PPTX file (BYTEA).

Parameters:
- file_content_bytea: Binary PPTX file content

Returns the extracted slides from the PPTX file as a JSON array. Uses python-pptx library for PPTX parsing.
Raises an exception if the file is not a valid PPTX or if any error occurs during processing.';

-- Universal document parser function
CREATE OR REPLACE FUNCTION aifun.parse_document(file_content_base64 TEXT, file_extension TEXT)
RETURNS JSONB AS $$
    try:
        import json
        from aifun.parser import parse_document
        return json.dumps(parse_document(file_content_base64, file_extension))      
    except Exception as e:
        plpy.error(f"Error parsing document: {e}")
$$ LANGUAGE plpython3u;

COMMENT ON FUNCTION aifun.parse_document(TEXT, TEXT) IS 'Universal document parser that handles different file formats. 

Parameters:
- file_content_base64: Base64-encoded file content
- file_extension: File extension (pdf, docx, pptx, png, etc.)

Automatically detects the file type based on extension and calls the appropriate parser function.
Returns the extracted data from the document.
Raises an exception if the file format is not supported or if any error occurs during processing.';

-- Function to parse document content and extract text (BYTEA version)
CREATE OR REPLACE FUNCTION aifun.parse_document(file_content_bytea BYTEA, file_extension TEXT)
RETURNS JSONB AS $$
    try:
        import json
        from aifun.parser import parse_document
        return json.dumps(parse_document(file_content_bytea, file_extension))
    except Exception as e:
        plpy.error(f"Error parsing document from bytea: {e}")
$$ LANGUAGE plpython3u;

COMMENT ON FUNCTION aifun.parse_document(BYTEA, TEXT) IS 'Universal document parser that handles different file formats. 

Parameters:
- file_content_bytea: Binary file content
- file_extension: File extension (pdf, docx, pptx, png, etc.)

Automatically detects the file type based on extension and calls the appropriate parser function.
Returns the extracted data from the document.
Raises an exception if the file format is not supported or if any error occurs during processing.';

-- Function to get help information for a UDF by reading its comment
CREATE OR REPLACE FUNCTION aifun.help(
    p_function_name TEXT
)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    v_comment TEXT;
BEGIN
    SELECT pg_description.description INTO v_comment
    FROM pg_description
    JOIN pg_proc ON pg_description.objoid = pg_proc.oid
    JOIN pg_namespace ON pg_proc.pronamespace = pg_namespace.oid
    WHERE pg_description.objsubid = 0
      AND pg_namespace.nspname = 'aifun'
      AND pg_proc.proname = p_function_name;
    
    IF v_comment IS NULL THEN
        RETURN 'No help information available for function: aifun.' || p_function_name;
    ELSE
        RETURN v_comment;
    END IF;
END;
$$;

COMMENT ON FUNCTION aifun.help(TEXT) IS 'Retrieves help information for a specified aifun function.

Parameters:
- p_function_name: The name of the function without the "aifun." prefix

Returns the comment/documentation for the function. Example: SELECT aifun.help(''ask''); to get help for the aifun.ask function.';

-- Function to list all available aifun functions
CREATE OR REPLACE FUNCTION aifun.list_all()
RETURNS TABLE(
    function_name TEXT,
    function_description TEXT
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        pg_proc.proname::TEXT AS function_name,
        pg_description.description::TEXT AS function_description
    FROM pg_proc
    JOIN pg_namespace ON pg_proc.pronamespace = pg_namespace.oid
    LEFT JOIN pg_description ON pg_description.objoid = pg_proc.oid AND pg_description.objsubid = 0
    WHERE pg_namespace.nspname = 'aifun'
      AND pg_proc.prokind = 'f'
    ORDER BY pg_proc.proname;
END;
$$;

COMMENT ON FUNCTION aifun.list_all() IS 'Lists all available aifun functions.

Returns a table of all available aifun functions with their names and descriptions.';

-- VLM-based PDF parsing function (TEXT version)
CREATE OR REPLACE FUNCTION aifun.parse_pdf_with_vlm(provider TEXT, model TEXT, file_content_base64 TEXT, prompt TEXT DEFAULT 'Extract all text and describe any images, charts, or visual elements in this PDF.')
RETURNS JSONB AS $$
    try:
        import json
        from aifun.llm_handler import vlm_parse_pdf

        result = vlm_parse_pdf(provider, model, file_content_base64, prompt)
        return json.dumps(result)
    except Exception as e:
        plpy.error(f"Error parsing PDF with VLM: {e}")
$$ LANGUAGE plpython3u;

COMMENT ON FUNCTION aifun.parse_pdf_with_vlm(TEXT, TEXT, TEXT, TEXT) IS 'Parse PDF using Vision Language Model for enhanced text extraction and visual analysis.

This function converts PDF pages to images and uses a VLM to extract text and analyze visual elements.
It provides more accurate OCR and can describe charts, diagrams, and other visual content.

Parameters:
- provider: The unique identifier of the VLM provider
- model: The vision model to use (e.g., "paddleocr-vl")
- file_content_base64: Base64-encoded PDF file content
- prompt: Custom prompt for analysis (optional)

Returns JSON array with extracted text and visual analysis for each page.
Raises an exception if VLM processing fails or if PDF conversion fails.';

-- VLM-based PDF parsing function (BYTEA version)
CREATE OR REPLACE FUNCTION aifun.parse_pdf_with_vlm(provider TEXT, model TEXT, file_content_bytea BYTEA, prompt TEXT DEFAULT 'Extract all text and describe any images, charts, or visual elements in this PDF.')
RETURNS JSONB AS $$
    try:
        import json
        from aifun.llm_handler import vlm_parse_pdf

        result = vlm_parse_pdf(provider, model, file_content_bytea, prompt)
        return json.dumps(result)
    except Exception as e:
        plpy.error(f"Error parsing PDF with VLM from bytea: {e}")
$$ LANGUAGE plpython3u;

COMMENT ON FUNCTION aifun.parse_pdf_with_vlm(TEXT, TEXT, BYTEA, TEXT) IS 'Parse PDF using Vision Language Model for enhanced text extraction and visual analysis.

This function converts PDF pages to images and uses a VLM to extract text and analyze visual elements.
It provides more accurate OCR and can describe charts, diagrams, and other visual content.

Parameters:
- provider: The unique identifier of the VLM provider
- model: The vision model to use (e.g., "paddleocr-vl")
- file_content_bytea: Binary PDF file content (BYTEA)
- prompt: Custom prompt for analysis (optional)

Returns JSON array with extracted text and visual analysis for each page.
Raises an exception if VLM processing fails or if PDF conversion fails.';

-- Visual Question Answering function (TEXT version)
CREATE OR REPLACE FUNCTION aifun.visual_qa(provider TEXT, model TEXT, image TEXT, question TEXT)
RETURNS TEXT AS $$
    try:
        from aifun.llm_handler import visual_qa
        return visual_qa(provider, model, image, question)
    except Exception as e:
        plpy.error(f"Error performing visual QA: {e}")
$$ LANGUAGE plpython3u;

COMMENT ON FUNCTION aifun.visual_qa(TEXT, TEXT, TEXT, TEXT) IS 'Perform visual question answering on an image.

This function allows asking specific questions about image content and getting
 detailed answers based on visual analysis of the image.

Parameters:
- provider: The unique identifier of the VLM provider
- model: The vision model to use (e.g., "paddleocr-vl")
- image: Base64-encoded image file content, including the prefix "data:image/png;base64,"
- question: Question to ask about the image

Returns the answer to the question based on image analysis.
Raises an exception if VLM processing fails or if image processing fails.';

-- Visual Question Answering function (BYTEA version)
CREATE OR REPLACE FUNCTION aifun.visual_qa(provider TEXT, model TEXT, image BYTEA, question TEXT)
RETURNS TEXT AS $$
    try:
        from aifun.llm_handler import visual_qa
        return visual_qa(provider, model, image, question)
    except Exception as e:
        plpy.error(f"Error performing visual QA from bytea: {e}")
$$ LANGUAGE plpython3u;

COMMENT ON FUNCTION aifun.visual_qa(TEXT, TEXT, BYTEA, TEXT) IS 'Perform visual question answering on an image.

This function allows asking specific questions about image content and getting
detailed answers based on visual analysis of the image.

Parameters:
- provider: The unique identifier of the VLM provider
- model: The vision model to use (e.g., "paddleocr-vl")
- file_content_bytea: Binary image file content (BYTEA)
- question: Question to ask about the image

Returns the answer to the question based on image analysis.
Raises an exception if VLM processing fails or if image processing fails.';

-- Grant usage on schema aifun to public
GRANT USAGE ON SCHEMA aifun TO public;

GRANT ALL ON aifun.providers TO public;
