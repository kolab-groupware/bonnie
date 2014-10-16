from caching import CachedDict
from elasticsearch_storage import ElasticSearchStorage

__all__ = [
        'CachedDict',
        'ElasticSearchStorage'
    ]

def list_classes():
    return [
            ElasticSearchStorage
        ]
