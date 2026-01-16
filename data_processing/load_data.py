import pandas as pd
import os
import logging
from enrich_data import enrich_dataframe

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
DATA_DIR = os.path.join(PROJECT_ROOT, 'data_input')

SOURCE_DATA_PATH = os.path.join(DATA_DIR, 'reviews.csv')
ENRICHED_DATA_PATH = os.path.join(DATA_DIR, 'enriched_data.csv')


def _check_api_key() -> None:
    """
    Validates that TMDB_API_KEY environment variable is set.
    
    Raises:
        EnvironmentError: If TMDB_API_KEY is not configured
    """
    if not os.environ.get("TMDB_API_KEY"):
        logger.critical("TMDB_API_KEY environment variable is not set.")
        raise EnvironmentError("TMDB_API_KEY not configured")
    logger.info("API key validated successfully.")


def _load_source_data(source_path: str) -> pd.DataFrame:
    """
    Loads the source CSV data.
    
    Args:
        source_path (str): Path to the source CSV file
    
    Returns:
        pd.DataFrame: Loaded data
    
    Raises:
        FileNotFoundError: If source file doesn't exist
    """
    logger.info(f"Loading source data from: {source_path}")
    
    if not os.path.exists(source_path):
        raise FileNotFoundError(f"Source data file not found at {source_path}")
    
    source_df = pd.read_csv(source_path)
    logger.info(f"Successfully loaded {len(source_df)} reviews.")
    
    return source_df


def _save_enriched_data(enriched_df: pd.DataFrame, output_path: str) -> None:
    """
    Saves the enriched DataFrame to a CSV file.
    
    Args:
        enriched_df (pd.DataFrame): Enriched data to save
        output_path (str): Path where to save the CSV
    """
    logger.info(f"Saving enriched data to: {output_path}")
    enriched_df.to_csv(output_path, index=False)
    logger.info(f"Successfully saved {len(enriched_df)} enriched records")


def _load_existing_enriched_data(enriched_path: str) -> pd.DataFrame | None:
    """
    Loads existing enriched data if available.
    
    Args:
        enriched_path (str): Path to the enriched CSV file
    
    Returns:
        pd.DataFrame: Enriched data if file exists, None otherwise
    """
    if not os.path.exists(enriched_path):
        return None
    
    logger.info(f"Loading existing enriched data from: {enriched_path}")
    enriched_df = pd.read_csv(enriched_path)
    logger.info(f"Loaded {len(enriched_df)} existing enriched records")
    return enriched_df

def process_and_save_data() -> None:
    """
    Main orchestration function for loading, enriching, and saving data.
    
    Raises:
        EnvironmentError: If TMDB_API_KEY is not set
        FileNotFoundError: If source data file is not found
        ValueError: If data validation fails
    """
    try:
        _check_api_key()
        source_df = _load_source_data(SOURCE_DATA_PATH)
        
        logger.info("Starting data enrichment process...")
        enriched_df = enrich_dataframe(source_df)
        logger.info("Enrichment complete.")
        
        _save_enriched_data(enriched_df, ENRICHED_DATA_PATH)
        
    except (FileNotFoundError, ValueError, EnvironmentError) as e:
        logger.error(f"Process error: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        raise


def main() -> None:
    """
    Entry point for the data processing application.
    Checks for existing enriched data before running enrichment.
    """
    if _load_existing_enriched_data(ENRICHED_DATA_PATH) is not None:
        logger.info("Using existing enriched data.")
        return
    
    logger.info("Starting data enrichment process.")
    try:
        process_and_save_data()
    except Exception as e:
        logger.error(f"Process failed: {e}", exc_info=True)
        exit(1)

if __name__ == "__main__":
    main()
        