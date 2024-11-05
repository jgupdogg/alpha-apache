# tests/test_summarizer_func.py

import pytest
from unittest.mock import MagicMock
from dags.utils.components.summaries.summary_func import map_summary

def test_map_summary_success(mocker):
    # Mock the call to OpenAI within LangChain's summarize chain
    mock_chain = MagicMock()
    mock_chain.run.return_value = "Summarized text"
    mocker.patch('dags.utils.summarizer.summary_func.load_summarize_chain', return_value=mock_chain)

    # Mock the Article object
    article = MagicMock()
    article.content = "This is a long article content that needs to be summarized."

    # Mock prompt templates with necessary placeholders
    mocker.patch(
        'dags.utils.summarizer.summary_func.get_map_prompt_template',
        return_value="Map prompt with text: {text}"
    )
    mocker.patch(
        'dags.utils.summarizer.summary_func.get_combine_prompt_template',
        return_value="Combine prompt with text: {text}"
    )

    result = map_summary(article)
    assert result == "Summarized text"

def test_map_summary_text_splitting(mocker):
    # Patch the RecursiveCharacterTextSplitter class in summary_func.py
    mock_text_splitter = mocker.patch('dags.utils.summarizer.summary_func.RecursiveCharacterTextSplitter')

    # Mock the summarize chain
    mock_chain = MagicMock()
    mock_chain.run.return_value = "Summarized text"
    mocker.patch('dags.utils.summarizer.summary_func.load_summarize_chain', return_value=mock_chain)

    # Mock the Article object
    article = MagicMock()
    article.content = "Content " * 1000  # Long content

    # Mock prompt templates with necessary placeholders
    mocker.patch(
        'dags.utils.summarizer.summary_func.get_map_prompt_template',
        return_value="Map prompt with text: {text}"
    )
    mocker.patch(
        'dags.utils.summarizer.summary_func.get_combine_prompt_template',
        return_value="Combine prompt with text: {text}"
    )

    result = map_summary(article, chunk_size=500, chunk_overlap=50)
    assert result == "Summarized text"

    # Assert that RecursiveCharacterTextSplitter was instantiated with correct args
    mock_text_splitter.assert_called_once_with(chunk_size=500, chunk_overlap=50)

def test_map_summary_edge_case_empty_content(mocker):
    # Mock the call to OpenAI within LangChain's summarize chain
    mock_chain = MagicMock()
    mock_chain.run.return_value = ""
    mocker.patch('dags.utils.summarizer.summary_func.load_summarize_chain', return_value=mock_chain)

    # Mock the Article object with empty content
    article = MagicMock()
    article.content = ""

    # Mock prompt templates with necessary placeholders
    mocker.patch(
        'dags.utils.summarizer.summary_func.get_map_prompt_template',
        return_value="Map prompt with text: {text}"
    )
    mocker.patch(
        'dags.utils.summarizer.summary_func.get_combine_prompt_template',
        return_value="Combine prompt with text: {text}"
    )

    result = map_summary(article)
    assert result == ""
