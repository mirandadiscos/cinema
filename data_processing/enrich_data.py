import os
import requests
import pandas as pd
from time import sleep
from dotenv import load_dotenv
import logging

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

API_KEY = os.environ.get("TMDB_API_KEY")


def _validate_api_key() -> None:
    """
    Validates that TMDB_API_KEY is configured.
    
    Raises:
        RuntimeError: If API_KEY is not available
    """
    if not API_KEY:
        logger.error("TMDB_API_KEY environment variable not set.")
        raise RuntimeError("TMDB_API_KEY not configured")


def _validate_movie_input(title: str, year: int) -> tuple[str, int]:
    """
    Validates and sanitizes movie input parameters.
    
    Args:
        title (str): Movie title
        year (int): Year of release
    
    Returns:
        tuple: (validated_title, validated_year)
    
    Raises:
        ValueError: If inputs are invalid
    """
    if not isinstance(title, str) or not title.strip():
        raise ValueError("Title must be a non-empty string.")
    
    try:
        year = int(year)
        if not (1800 <= year <= 2100):
            raise ValueError(f"Invalid year {year}. Must be between 1800 and 2100.")
    except (ValueError, TypeError) as e:
        raise ValueError(f"Year must be an integer, got {type(year).__name__}.")
    
    return title.strip(), year


def _fetch_from_api(url: str, params: dict, attempt: int, max_retries: int) -> dict | None:
    """
    Generic API fetch with rate limit handling and retry logic.
    
    Args:
        url (str): API endpoint URL
        params (dict): Query parameters
        attempt (int): Current attempt number
        max_retries (int): Maximum number of retries
    
    Returns:
        dict: API response or None if failed
    """
    try:
        response = requests.get(url, params=params, timeout=10)
        
        if response.status_code == 429:
            wait_time = int(response.headers.get('Retry-After', 2 ** attempt))
            logger.warning(f"Rate limited. Waiting {wait_time}s before retry (attempt {attempt + 1}/{max_retries})")
            sleep(wait_time)
            return None
        
        response.raise_for_status()
        return response.json()
        
    except requests.exceptions.Timeout:
        logger.error(f"Timeout on attempt {attempt + 1}/{max_retries}")
        if attempt < max_retries - 1:
            sleep(2 ** attempt)
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {e}")
        if attempt < max_retries - 1:
            sleep(2 ** attempt)
        return None


def _extract_directors(credits: dict) -> list[str]:
    """
    Extracts director names from credits data.
    
    Args:
        credits (dict): Credits data from API response
    
    Returns:
        list: Director names or ['N/A'] if none found
    """
    directors = []
    for member in credits.get('crew', []):
        if member.get('job') == 'Director':
            directors.append(member.get('name'))
    
    return directors if directors else ['N/A']


def get_movie_details(title: str, year: int, retries: int = 3) -> dict | None:
    """
    Fetches movie details from The Movie Database (TMDb) API.
    Includes input validation and retry logic for rate limiting.
    
    Args:
        title (str): Movie title
        year (int): Year of release
        retries (int): Number of retries for rate limit handling
    
    Returns:
        dict: Movie details or None if not found/error
    """
    try:
        _validate_api_key()
        title, year = _validate_movie_input(title, year)
    except (RuntimeError, ValueError) as e:
        logger.error(f"Validation error: {e}")
        return None

    search_url = "https://api.themoviedb.org/3/search/movie"
    search_params = {'api_key': API_KEY, 'query': title, 'year': year}
    
    for attempt in range(retries):
        search_data = _fetch_from_api(search_url, search_params, attempt, retries)
        if search_data is None:
            continue
        
        if not search_data.get('results'):
            logger.info(f"No results found for '{title}' ({year})")
            return None
        
        movie_id = search_data['results'][0]['id']
        
        details_url = f"https://api.themoviedb.org/3/movie/{movie_id}"
        details_params = {
            'api_key': API_KEY,
            'language': 'en-US',
            'append_to_response': 'credits'
        }
        details = _fetch_from_api(details_url, details_params, attempt, retries)
        
        if details is None:
            continue
        
        return details
    
    return None

def enrich_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enriches the DataFrame with details from TMDb.
    Validates required columns exist before processing.
    
    Args:
        df (pd.DataFrame): DataFrame with movie data
    
    Returns:
        pd.DataFrame: Enriched DataFrame
    
    Raises:
        ValueError: If required columns are missing or no data was enriched
    """
    required_columns = ['Name', 'Year', 'Letterboxd URI', 'Rating']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    enriched_data = []
    
    for index, row in df.iterrows():
        if pd.isna(row['Name']) or pd.isna(row['Year']):
            logger.warning(f"Skipping row {index}: missing Name or Year")
            continue
        
        logger.info(f"Fetching details for: {row['Name']} ({row['Year']})")
        details = get_movie_details(row['Name'], row['Year'])
        
        if details:
            enriched_data.append(_build_enriched_row(row, details))
        else:
            logger.warning(f"Could not find details for: {row['Name']}")
        
        sleep(0.5)

    if not enriched_data:
        raise ValueError("No movies were successfully enriched.")
    
    logger.info(f"Successfully enriched {len(enriched_data)} records.")
    return pd.DataFrame(enriched_data)


def _build_enriched_row(source_row: pd.Series, details: dict) -> dict:
    """
    Builds a single enriched row combining source data with API details.
    
    Args:
        source_row (pd.Series): Original row from source DataFrame
        details (dict): Movie details from TMDb API
    
    Returns:
        dict: Enriched row data
    """
    return {
        'Name': source_row['Name'],
        'Year': source_row['Year'],
        'Letterboxd URI': source_row['Letterboxd URI'],
        'MyRating': source_row['Rating'],
        'Review': source_row.get('Review', None),
        'Watched Date': source_row.get('Watched Date', None),
        'Rating': details.get('vote_average'),
        'Synopsis': details.get('overview'),
        'Genres': [genre['name'] for genre in details.get('genres', [])],
        'Director': _extract_directors(details.get('credits', {})),
        'Country': [country['name'] for country in details.get('production_countries', [])],
        'Runtime': details.get('runtime'),
    }


