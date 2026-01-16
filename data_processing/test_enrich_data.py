import unittest
from unittest.mock import patch, Mock
import pandas as pd
import os 
from dotenv import load_dotenv 
from data_processing.enrich_data import get_movie_details, enrich_dataframe


# ============================================================================
# Mock Data Fixtures
# ============================================================================

FAKE_SEARCH_RESPONSE = {
    "results": [
        {"id": 12345}
    ]
}

FAKE_SEARCH_RESPONSE_EMPTY = {
    "results": []
}

FAKE_DETAILS_RESPONSE = {
    "genres": [{"name": "Science Fiction"}],
    "production_countries": [{"name": "United States of America"}],
    "runtime": 124,
    "overview": "A mind-bending thriller.",
    "vote_average": 8.8,
    "credits": {
        "crew": [
            {"job": "Director", "name": "Christopher Nolan"},
            {"job": "Writer", "name": "Jonathan Nolan"}
        ]
    }
}


# ============================================================================
# Mock Response Builders
# ============================================================================

def _create_mock_response(status_code: int, json_data: dict = None, headers: dict = None) -> Mock:
    """
    Creates a mock HTTP response object.
    
    Args:
        status_code (int): HTTP status code
        json_data (dict): Response JSON data
        headers (dict): Response headers
    
    Returns:
        Mock: Configured mock response
    """
    mock_response = Mock()
    mock_response.status_code = status_code
    mock_response.raise_for_status.return_value = None
    if json_data:
        mock_response.json.return_value = json_data
    if headers:
        mock_response.headers = headers
    else:
        mock_response.headers = {}
    return mock_response


def _create_successful_search_and_details_mocks() -> list:
    """
    Creates successful mock responses for search and details API calls.
    
    Returns:
        list: [search_response, details_response]
    """
    search_mock = _create_mock_response(200, FAKE_SEARCH_RESPONSE)
    details_mock = _create_mock_response(200, FAKE_DETAILS_RESPONSE)
    return [search_mock, details_mock]


def _create_rate_limited_then_success_mocks() -> list:
    """
    Creates mock responses that first return 429 (rate limit), then succeed.
    
    Returns:
        list: [rate_limit_response, search_response, details_response]
    """
    rate_limit_mock = _create_mock_response(429, headers={'Retry-After': '1'})
    search_mock = _create_mock_response(200, FAKE_SEARCH_RESPONSE)
    details_mock = _create_mock_response(200, FAKE_DETAILS_RESPONSE)
    return [rate_limit_mock, search_mock, details_mock]


# ============================================================================
# DataFrame Builders
# ============================================================================

def _create_source_dataframe() -> pd.DataFrame:
    """
    Creates a sample source DataFrame for testing.
    
    Returns:
        pd.DataFrame: Source data with movies
    """
    source_data = {
        'Name': ['Inception', 'The Dark Knight'],
        'Year': [2010, 2008],
        'Letterboxd URI': ['uri1', 'uri2'],
        'Rating': [5, 4],
        'Review': ['Great movie!', 'Awesome film!'],
        'Watched Date': ['2023-01-01', '2023-01-02']
    }
    return pd.DataFrame(source_data)


def _create_dataframe_with_missing_columns() -> pd.DataFrame:
    """
    Creates a DataFrame missing required columns.
    
    Returns:
        pd.DataFrame: Incomplete DataFrame
    """
    source_data = {
        'Name': ['Inception'],
        'Year': [2010],
    }
    return pd.DataFrame(source_data)


def _create_dataframe_with_missing_values() -> pd.DataFrame:
    """
    Creates a DataFrame with missing Name/Year values.
    
    Returns:
        pd.DataFrame: DataFrame with None values
    """
    source_data = {
        'Name': ['Inception', None, 'The Dark Knight'],
        'Year': [2010, 2008, None],
        'Letterboxd URI': ['uri1', 'uri2', 'uri3'],
        'Rating': [5, 4, 3],
    }
    return pd.DataFrame(source_data)


def _create_dataframe_with_nonexistent_movie() -> pd.DataFrame:
    """
    Creates a DataFrame with a non-existent movie.
    
    Returns:
        pd.DataFrame: DataFrame with fake movie data
    """
    source_data = {
        'Name': ['FakeMovieThatDoesntExist'],
        'Year': [2099],
        'Letterboxd URI': ['uri1'],
        'Rating': [5],
    }
    return pd.DataFrame(source_data)


# ============================================================================
# Assertion Helpers
# ============================================================================

def _assert_enriched_dataframe_has_all_columns(enriched_df: pd.DataFrame) -> None:
    """
    Asserts that enriched DataFrame has all expected columns.
    
    Args:
        enriched_df (pd.DataFrame): DataFrame to check
    """
    required_columns = ['MyRating', 'Rating', 'Synopsis', 'Director', 'Genres', 'Country']
    for col in required_columns:
        assert col in enriched_df.columns, f"Missing column: {col}"


def _assert_first_row_enriched_correctly(enriched_df: pd.DataFrame) -> None:
    """
    Asserts that the first row of enriched DataFrame contains correct data.
    
    Args:
        enriched_df (pd.DataFrame): Enriched DataFrame
    """
    first_row = enriched_df.iloc[0]
    assert first_row['Director'] == ['Christopher Nolan'], "Director mismatch"
    assert first_row['Runtime'] == 124, "Runtime mismatch"
    assert first_row['MyRating'] == 5, "MyRating mismatch"
    assert first_row['Rating'] == 8.8, "Rating mismatch"
    assert first_row['Synopsis'] == "A mind-bending thriller.", "Synopsis mismatch"
    assert 'Science Fiction' in first_row['Genres'], "Genre not found"
    assert 'United States of America' in first_row['Country'], "Country not found"
    assert first_row['Review'] == 'Great movie!', "Review mismatch"
    assert first_row['Watched Date'] == '2023-01-01', "Watched Date mismatch"


# ============================================================================
# Test Classes
# ============================================================================

class TestGetMovieDetailsSuccess(unittest.TestCase):
    """Tests for successful get_movie_details calls."""

    @patch('data_processing.enrich_data.API_KEY', 'fake_api_key')
    @patch('requests.get')
    def test_successful_movie_fetch(self, mock_get):
        """Tests successful retrieval of movie details from API."""
        mock_get.side_effect = _create_successful_search_and_details_mocks()

        details = get_movie_details("Inception", 2010)

        self.assertIsNotNone(details)
        self.assertEqual(details['runtime'], 124)
        self.assertIn('Science Fiction', [g['name'] for g in details['genres']])
        self.assertEqual(mock_get.call_count, 2)

    @patch('data_processing.enrich_data.API_KEY', 'fake_api_key')
    @patch('requests.get')
    def test_rate_limit_handling(self, mock_get):
        """Tests proper handling of rate limiting (429) with retry."""
        mock_get.side_effect = _create_rate_limited_then_success_mocks()

        details = get_movie_details("Inception", 2010, retries=3)

        self.assertIsNotNone(details)
        self.assertEqual(details['runtime'], 124)


class TestGetMovieDetailsFailures(unittest.TestCase):
    """Tests for get_movie_details failure scenarios."""

    @patch('data_processing.enrich_data.API_KEY', 'fake_api_key')
    @patch('requests.get')
    def test_no_results_found(self, mock_get):
        """Tests handling of no search results."""
        mock_response = _create_mock_response(200, FAKE_SEARCH_RESPONSE_EMPTY)
        mock_get.return_value = mock_response

        details = get_movie_details("NonexistentMovie", 2099)

        self.assertIsNone(details)


class TestGetMovieDetailsValidation(unittest.TestCase):
    """Tests for input validation in get_movie_details."""

    @patch('data_processing.enrich_data.API_KEY', 'fake_api_key')
    def test_invalid_title_scenarios(self):
        """Tests various invalid title inputs."""
        invalid_titles = [
            ("", "empty string"),
            (None, "None value"),
            (123, "non-string type"),
        ]
        
        for title, description in invalid_titles:
            with self.subTest(description=description):
                details = get_movie_details(title, 2010)
                self.assertIsNone(details, f"Should return None for {description}")

    @patch('data_processing.enrich_data.API_KEY', 'fake_api_key')
    def test_invalid_year_scenarios(self):
        """Tests various invalid year inputs."""
        invalid_years = [
            (1700, "year too old"),
            (2200, "year too new"),
            ("not_a_year", "non-numeric year"),
        ]
        
        for year, description in invalid_years:
            with self.subTest(description=description):
                details = get_movie_details("Inception", year)
                self.assertIsNone(details, f"Should return None for {description}")


class TestEnrichDataframeSuccess(unittest.TestCase):
    """Tests for successful enrich_dataframe operations."""

    @patch('data_processing.enrich_data.get_movie_details')
    def test_enriches_multiple_movies(self, mock_get_details):
        """Tests enrichment of multiple movies in DataFrame."""
        mock_get_details.return_value = FAKE_DETAILS_RESPONSE
        source_df = _create_source_dataframe()

        enriched_df = enrich_dataframe(source_df)

        self.assertEqual(len(enriched_df), 2)
        _assert_enriched_dataframe_has_all_columns(enriched_df)
        _assert_first_row_enriched_correctly(enriched_df)
        self.assertEqual(mock_get_details.call_count, 2)


class TestEnrichDataframeValidation(unittest.TestCase):
    """Tests for validation in enrich_dataframe."""

    @patch('data_processing.enrich_data.get_movie_details')
    def test_missing_required_columns(self, mock_get_details):
        """Tests error when required columns are missing."""
        source_df = _create_dataframe_with_missing_columns()

        with self.assertRaises(ValueError) as context:
            enrich_dataframe(source_df)
        
        self.assertIn("required columns", str(context.exception))

    @patch('data_processing.enrich_data.get_movie_details')
    def test_skips_missing_values(self, mock_get_details):
        """Tests that rows with missing Name/Year are skipped."""
        mock_get_details.return_value = FAKE_DETAILS_RESPONSE
        source_df = _create_dataframe_with_missing_values()

        enriched_df = enrich_dataframe(source_df)

        # Only first row should be enriched (other two have missing values)
        self.assertEqual(len(enriched_df), 1)
        self.assertEqual(enriched_df.iloc[0]['Name'], 'Inception')


class TestEnrichDataframeFailures(unittest.TestCase):
    """Tests for enrich_dataframe failure scenarios."""

    @patch('data_processing.enrich_data.get_movie_details')
    def test_no_movies_enriched(self, mock_get_details):
        """Tests error when no movies can be enriched."""
        mock_get_details.return_value = None
        source_df = _create_dataframe_with_nonexistent_movie()

        with self.assertRaises(ValueError) as context:
            enrich_dataframe(source_df)
        
        self.assertIn("No movies were successfully enriched", str(context.exception))


class TestEnvironmentConfiguration(unittest.TestCase):
    """Tests for environment and configuration."""

    def test_tmdb_api_key_loaded(self):
        """Tests that TMDB_API_KEY is properly loaded from .env file."""
        load_dotenv()
        api_key = os.getenv("TMDB_API_KEY")
        
        self.assertIsNotNone(api_key, "TMDB_API_KEY should be loaded from .env file")
        self.assertIsInstance(api_key, str, "TMDB_API_KEY should be a string")
        self.assertGreater(len(api_key), 0, "TMDB_API_KEY should not be empty")

if __name__ == '__main__':
    unittest.main()
