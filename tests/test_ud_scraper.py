# tests/test_ud_scraper.py

import pytest
from unittest.mock import patch, MagicMock
from dags.utils.components.scraper.ud_scraper import UDScraper
from bs4 import BeautifulSoup
import platform

# Constants
TEST_URL = "http://example.com"

@pytest.fixture(scope="module")
def scraper():
    """
    Fixture to initialize the UDScraper instance.
    """
    scraper_instance = UDScraper(use_proxy=False)  # Disable proxy for testing
    yield scraper_instance
    scraper_instance.close_driver()

def test_make_request_success(scraper):
    """
    Test that make_request successfully fetches and parses the page.
    """
    if platform.system() != "Windows":
        pytest.skip("Skipping test_make_request_success on non-Windows systems for demonstration.")
    
    soup = scraper.make_request(TEST_URL)
    assert soup is not None, "Scraper returned None"
    assert isinstance(soup, BeautifulSoup), "Scraper did not return a BeautifulSoup object"
    
    # Check for expected content in the example.com page
    title = soup.find('h1')
    assert title is not None, "Title tag not found in the scraped page"
    assert title.text.strip() == "Example Domain", "Unexpected title text"

def test_make_request_invalid_url(scraper):
    """
    Test that make_request handles invalid URLs gracefully.
    """
    invalid_url = "http://invalid.url.test"
    soup = scraper.make_request(invalid_url, max_retries=1)
    assert soup is None, "Scraper should return None for invalid URLs"

@patch('dags.utils.components.scraper.ud_scraper.UDScraper.setup_driver')
def test_make_request_with_mocked_driver(mock_setup_driver):
    """
    Test make_request with a mocked Selenium driver to avoid actual browser interaction.
    """
    # Create a mock driver with the necessary methods
    mock_driver = MagicMock()
    mock_driver.page_source = "<html><body><h1>Mocked Page</h1></body></html>"
    mock_setup_driver.return_value = mock_driver
    
    # Initialize scraper with proxy disabled
    scraper_instance = UDScraper(use_proxy=False)
    
    # Perform the request
    soup = scraper_instance.make_request(TEST_URL, max_retries=1)
    
    # Assertions
    assert soup is not None, "Scraper returned None with mocked driver"
    assert isinstance(soup, BeautifulSoup), "Scraper did not return a BeautifulSoup object with mocked driver"
    
    title = soup.find('h1')
    assert title is not None, "Title tag not found in the mocked scraped page"
    assert title.text.strip() == "Mocked Page", "Unexpected title text in mocked page"
    
    # Clean up
    scraper_instance.close_driver()

def test_proxy_pool():
    """
    Optional: Test the proxy_pool functions if you wish to include them.
    This example assumes that get_best_proxies is properly implemented.
    """
    from dags.utils.components.scraper.proxy_pool import get_best_proxies
    
    proxies = get_best_proxies()
    assert proxies is not None, "Proxy pool is None"
    assert isinstance(proxies, list), "Proxies should be returned as a list"
    if proxies:
        proxy = proxies[0]
        assert 'ip' in proxy and 'port' in proxy, "Proxy does not contain 'ip' and 'port'"
