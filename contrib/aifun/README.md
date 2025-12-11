# AIFun PostgreSQL Extension

An Apache Cloudberry/PostgreSQL extension that provides AI-powered functions that can be used with various LLM providers (OpenAI, Anthropic, Google Gemini, AWS Bedrock, any third-party providers that follow the OpenAI API specification, and locally hosted models using vLLM or SGLang).

## Features

- **Multiple AI Providers**: Support for OpenAI, Anthropic, Google Gemini, and AWS Bedrock
- **User Isolation**: Each user manages their own API keys and providers
- **Row Level Security**: PostgreSQL RLS ensures users can only access their own configurations
- **AI Functions**: Ask questions, generate embeddings, classify text, extract structured data, summarize, translate, and more
- **Simple Setup**: No complex encryption keys, users directly manage their API keys

## Installation

```bash
# Install the extension
make install

# In PostgreSQL
CREATE EXTENSION aifun;
```

## Quick Start

### 1. Add Your AI Provider

```sql
-- Add OpenAI-compatible provider
SELECT
    aifun.add_provider(
        p_id => 'local_llm',
        p_type => 'openai',
        p_api_key => 'abc', 
        p_metadata => '{
            "endpoint": "http://10.14.10.1:8800/vllm/v1"
        }'::JSONB
    );

-- Add OpenAI provider
SELECT
    aifun.add_provider(
        p_id => 'local_llm',
        p_type => 'openai',
        p_api_key => 'api-key-openai'
    );

-- Add Anthropic provider  
SELECT
    aifun.add_provider(
        p_id => 'local_llm',
        p_type => 'anthropic',
        p_api_key => 'api-key-anthropic'
    );

-- Add Google Gemini provider
SELECT
    aifun.add_provider(
        p_id => 'local_llm',
        p_type => 'gemini',
        p_api_key => 'api-key-gemini'
    );
```

### 2. Use AI Functions

```sql
-- Ask a question
SELECT
    aifun.ask(
        provider => 'local_llm',
        model => 'zhipu/glm4-9b-chat',
        prompt => 'What is PostgreSQL?'
    );

-- Ask a question with context
SELECT
    aifun.ask(
        provider => 'local_llm',
        model => 'zhipu/glm4-9b-chat',
        prompt => 'What is the main advantage?',
        context => '{"context": "PostgreSQL is a relational database with ACID compliance and extensive extensibility."}'::jsonb
    );

-- Have a conversation with the AI
SELECT
    aifun.chat(
        provider => 'local_llm',
        model => 'zhipu/glm4-9b-chat',
        messages => '[
            {
                "role": "system",
                "content": "You are a helpful assistant."
            },
            {
                "role": "user",
                "content": "What is PostgreSQL?"
            },
            {
                "role": "assistant",
                "content": "PostgreSQL is an open-source relational database management system."
            },
            {
                "role": "user",
                "content": "What are its main features?"
            }
        ]'::jsonb
    );

-- Classify text
SELECT
    aifun.classify(
        provider => 'local_llm',
        model => 'zhipu/glm4-9b-chat',
        text_to_classify => 'I love this product!',
        labels => ARRAY['positive', 'negative', 'neutral']
    );

-- Extract structured data
SELECT
    aifun.extract(
        provider => 'local_llm',
        model => 'zhipu/glm4-9b-chat',
        text_to_parse => 'John is 30 years old and works as a developer', 
        json_schema => '{
            "name": "string",
            "age": "number",
            "job": "string"
        }'::JSONB
    );

-- Summarize text
SELECT
    aifun.summarize(
        provider => 'local_llm',
        model => 'zhipu/glm4-9b-chat',
        text_to_summarize => 'aifun is a PostgreSQL extension that provides AI-powered functions using various LLM providers (OpenAI, Anthropic, Google Gemini, AWS Bedrock). 
        It allows users to ask questions, generate embeddings, classify text, extract structured data, summarize, translate, and more.
        '
);

-- Translate text
SELECT
    aifun.translate(
        provider => 'local_llm',
        model => 'zhipu/glm4-9b-chat',
        text_to_translate => 'Hello world', 
        target_language => 'Spanish'
    );

-- Visual Q&A
SELECT
    aifun.visual_qa(
        provider => 'local_vlm',
        model => 'paddleocr-vl',
        image => 'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAoAAAAKCAIAAAACUFjqAAAAE0lEQVR4nGP8z4APMOGVZRip0gBBLAETee26JgAAAABJRU5ErkJggg==',
        question => 'What is the color of the shape?'
    );

-- Generate embeddings
SELECT
    aifun.embed(
        provider => 'local_embedding',
        model => 'jina', 
        text_to_embed => 'PostgreSQL is awesome'
    );

-- Multimodal embeddings with JSON content
SELECT
    aifun.multimodal_embed(
        provider => 'local_multimodal_embedding',
        model => 'bge', 
        content => '{"text": "A red square", "image": "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAoAAAAKCAIAAAACUFjqAAAAE0lEQVR4nGP8z4APMOGVZRip0gBBLAETee26JgAAAABJRU5ErkJggg=="}'::jsonb
    );

-- Multimodal embeddings with text and image
SELECT
    aifun.multimodal_embed(
        provider => 'local_multimodal_embedding',
        model => 'bge',
        text => 'A red square',
        image => 'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAoAAAAKCAIAAAACUFjqAAAAE0lEQVR4nGP8z4APMOGVZRip0gBBLAETee26JgAAAABJRU5ErkJggg=='
    );

-- Parse PDF (Replace with actual base64-encoded PDF content)
SELECT aifun.parse_pdf(
    file_content_base64 => 'data:application/pdf;base64,JVBERi0xLjQKJdPr6eUQyAAAAAA=='
);

-- Parse DOCX (Replace with actual base64-encoded DOCX content)
SELECT aifun.parse_docx(
    file_content_base64 => 'data:application/vnd.openxmlformats-officedocument.wordprocessingml.document;base64,UEsDBBQACAgIAQwCAAQAAQAAAgICAgICAgAAAAAA=='
);

-- Parse PPTX (Replace with actual base64-encoded PPTX content)
SELECT aifun.parse_pptx(
    file_content_base64 => 'data:application/vnd.openxmlformats-officedocument.presentationml.presentation;base64,UEsDBBQACAgIAQwCAAQAAQAAAgICAgICAgAAAAAA=='
);

-- Parse PDF with VLM
SELECT
    aifun.parse_pdf_with_vlm(
        provider => 'local_vlm',
        model => 'paddleocr-vl',
        file_content_base64 => 'data:application/pdf;base64,JVBERi0xLjQKJdPr6eUQyAAAAAA=='
    );

-- Parse document with specified format (Replace with actual base64-encoded content)
SELECT aifun.parse_document(
    file_content_base64 => 'data:application/pdf;base64,JVBERi0xLjQKJdPr6eUQyAAAAAA==',
    file_extension => 'pdf'
);

-- Parse document with specified format (Replace with actual base64-encoded content)
SELECT aifun.parse_document(
    file_content_base64 => 'data:application/vnd.openxmlformats-officedocument.wordprocessingml.document;base64,UEsDBBQACAgIAQwCAAQAAQAAAgICAgICAgAAAAAA==',
    file_extension => 'docx'
);

-- Parse document with specified format (Replace with actual base64-encoded content)
SELECT aifun.parse_document(
    file_content_base64 => 'data:application/vnd.openxmlformats-officedocument.presentationml.presentation;base64,UEsDBBQACAgIAQwCAAQAAQAAAgICAgICAgAAAAAA==',
    file_extension => 'pptx'
);
```

### 3. Use AI Functions with Tables

```sql
CREATE TABLE IF NOT EXISTS test (
    id SERIAL PRIMARY KEY,
    content TEXT
);

-- Insert some test data
INSERT INTO test (content)
VALUES ('PostgreSQL is a powerful open-source database system.'),
       ('I love using Anthropic for AI-powered chatbots.'),
       ('AIFun is a PostgreSQL extension that provides AI-powered functions using various LL M providers.');

CREATE TABLE test_translation AS
SELECT
    id,
    aifun.translate(
        provider => 'local_llm',
        model => 'zhipu/glm4-9b-chat',
        text_to_translate => content, 
        target_language => 'zh'
    ) AS translation
FROM test;

CREATE TABLE test_embedding AS
SELECT
    id,
    aifun.embed(
        provider => 'local_llm',
        model => 'jina', 
        text_to_embed => content
    ) AS embedding
FROM test;
```

### 4. Use AI Functions with Directory Tables
```sql
-- Create a directory table
CREATE DIRECTORY TABLE test_dirt;

-- Parse PDF files
SELECT
    aifun.parse_pdf(
        file_content_bytea => content
    )  
FROM directory_table('test_dirt')
WHERE relative_path LIKE '%.pdf';

-- Parse DOCX files
SELECT
    aifun.parse_docx(
        file_content_bytea => content
    )  
FROM directory_table('test_dirt')
WHERE relative_path LIKE '%.docx';

-- Parse PPTX files
SELECT
    aifun.parse_pptx(
        file_content_bytea => content
    )  
FROM directory_table('test_dirt')
WHERE relative_path LIKE '%.pptx';

-- Parse all document types
SELECT
    aifun.parse_document(
        file_content_bytea => content,
        file_extension => SPLIT_PART(relative_path, '.', -1)
    )  
FROM directory_table('test_dirt');
```

### 4. Manage Your Providers

```sql
-- List your providers
SELECT * FROM aifun.my_providers;

-- Update API key
SELECT aifun.update_api_key('my_openai', 'new-api-key');

-- Remove provider
SELECT aifun.remove_provider('my_openai');

-- Check if provider exists
SELECT aifun.has_provider('my_openai');
```

## Security Model

This extension uses **Row Level Security (RLS)** to ensure complete user isolation:

- **User Isolation**: Each user can only access their own provider configurations
- **No Shared Secrets**: No master encryption keys that could be compromised
- **Direct API Keys**: Users manage their own API keys directly
- **PostgreSQL Security**: Leverages PostgreSQL's built-in RLS for access control

### How It Works

1. Each provider configuration is tagged with the owner's username
2. RLS policies ensure users can only see/modify their own data
3. API keys are stored as plain text (user's own keys)
4. No central encryption/decryption overhead

## Supported Providers

| Provider | Type | Models |
|----------|------|---------|
| OpenAI | `openai` | gpt-4, gpt-3.5-turbo, text-embedding-ada-002, or compatible models |
| Anthropic | `anthropic` | claude-3-sonnet, claude-3-haiku |
| Google Gemini | `gemini` | gemini-pro, gemini-pro-vision |
| AWS Bedrock | `aws_bedrock` | Various models via AWS Bedrock |

## Appendix - List of Functions

| Schema | Name | Result data type | Argument data types | Type |
|--------|------|------------------|---------------------|------|
| aifun | _get_provider_key | text | p_provider_id text | func |
| aifun | add_provider | void | p_id text, p_type text, p_api_key text, p_metadata jsonb DEFAULT NULL::jsonb | func |
| aifun | ask | text | provider text, model text, prompt text | func |
| aifun | ask | text | provider text, model text, prompt text, context text | func |
| aifun | chat | text | provider text, model text, messages jsonb | func |
| aifun | chunk | text[] | text text, chunk_size integer DEFAULT 1000, overlap integer DEFAULT 200 | func |
| aifun | classify | text | provider text, model text, text_to_classify text, labels text[] | func |
| aifun | embed | vector | provider text, model text, text_to_embed text | func |
| aifun | extract | jsonb | provider text, model text, text_to_parse text, json_schema jsonb | func |
| aifun | extract_keywords | text[] | provider text, model text, text text, num_keywords integer DEFAULT 5 | func |
| aifun | fix_grammar | text | provider text, model text, text text | func |
| aifun | has_provider | boolean | p_provider_id text | func |
| aifun | help | text | p_function_name text | func |
| aifun | list_all | TABLE(function_name text, function_description text) |  | func |
| aifun | multimodal_embed | vector | provider text, model text, content jsonb | func |
| aifun | multimodal_embed | vector | provider text, model text, text text DEFAULT NULL::text, image bytea DEFAULT NULL::bytea | func |
| aifun | multimodal_embed | vector | provider text, model text, text text DEFAULT NULL::text, image text DEFAULT NULL::text | func |
| aifun | parse_document | jsonb | file_content_base64 text, file_extension text | func |
| aifun | parse_document | jsonb | file_content_bytea bytea, file_extension text | func |
| aifun | parse_docx | jsonb | file_content_base64 text | func |
| aifun | parse_docx | jsonb | file_content_bytea bytea | func |
| aifun | parse_pdf | jsonb | file_content_base64 text | func |
| aifun | parse_pdf | jsonb | file_content_bytea bytea | func |
| aifun | parse_pdf_with_vlm | jsonb | provider text, model text, file_content_base64 text, prompt text DEFAULT 'Extract all text and describe any images, charts, or visual elements in this PDF.'::text | func |
| aifun | parse_pdf_with_vlm | jsonb | provider text, model text, file_content_bytea bytea, prompt text DEFAULT 'Extract all text and describe any images, charts, or visual elements in this PDF.'::text | func |
| aifun | parse_pptx | jsonb | file_content_base64 text | func |
| aifun | parse_pptx | jsonb | file_content_bytea bytea | func |
| aifun | remove_provider | void | p_id text | func |
| aifun | rerank | text[] | provider text, model text, query text, documents text[] | func |
| aifun | similarity | double precision | provider text, model text, text1 text, text2 text | func |
| aifun | summarize | text | provider text, model text, text_to_summarize text, length integer DEFAULT 50 | func |
| aifun | translate | text | provider text, model text, text_to_translate text, target_language text | func |
| aifun | update_api_key | void | p_provider_id text, p_api_key text | func |
| aifun | visual_qa | text | provider text, model text, image bytea, question text | func |
| aifun | visual_qa | text | provider text, model text, image text, question text | func |