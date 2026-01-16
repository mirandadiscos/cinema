#!/usr/bin/env python
"""
Teste da estratégia incremental de processamento.
Verifica se novos filmes são processados e antigos são reutilizados.
"""

import sys
import pandas as pd
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'data_processing'))

from load_data import _get_new_movies, _combine_enriched_data


def test_incremental_strategy():
    """Testa detecção de novos filmes e combinação com dados antigos."""
    
    old_enriched = pd.DataFrame({
        'Letterboxd URI': ['http://letterboxd.com/film/inception'],
        'Name': ['Inception'],
        'Year': [2010],
        'Rating': [8.8],
        'MyRating': [9]
    })
    
    old_enriched.to_csv('/tmp/enriched_temp.csv', index=False)
    
    new_reviews = pd.DataFrame({
        'Letterboxd URI': [
            'http://letterboxd.com/film/inception',
            'http://letterboxd.com/film/dark-knight'
        ],
        'Name': ['Inception', 'The Dark Knight'],
        'Year': [2010, 2008],
        'Rating': [9, 10]
    })
    
    new_movies = _get_new_movies(new_reviews, '/tmp/enriched_temp.csv')
    
    assert len(new_movies) == 1, f"Esperava 1 novo filme, encontrou {len(new_movies)}"
    assert new_movies.iloc[0]['Name'] == 'The Dark Knight', "Filme novo incorreto"
    
    new_enriched = pd.DataFrame({
        'Letterboxd URI': ['http://letterboxd.com/film/dark-knight'],
        'Name': ['The Dark Knight'],
        'Year': [2008],
        'Rating': [9.0],
        'MyRating': [10]
    })
    
    combined = _combine_enriched_data(new_enriched, '/tmp/enriched_temp.csv')
    
    assert len(combined) == 2, f"Esperava 2 filmes combinados, encontrou {len(combined)}"
    assert set(combined['Name']) == {'Inception', 'The Dark Knight'}, "Nomes incorretos"
    
    os.remove('/tmp/enriched_temp.csv')
    
    print("✓ Teste de incrementalização passou")


if __name__ == '__main__':
    test_incremental_strategy()
