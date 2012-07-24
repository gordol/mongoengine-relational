from __future__ import print_function
from __future__ import unicode_literals

from mongoengine import Document
from mongoengine.base import ObjectId
from mongoengine.queryset import QuerySet


class DocumentCache( object ):
    def __init__( self, request ):
        if not hasattr( request, 'cache' ):
            request.cache = self
        else:
            raise RuntimeError( 'A `DocumentCache` already exists; only one should be created per request.' )

        self._documents = {}

    def __iter__( self ):
        return iter( self._documents )

    def __getitem__( self, id ):
        """Dictionary-style field access, return a field's value if present.
        """
        try:
            id = id.pk if isinstance( id, Document ) else id
            return self._documents[ str( id ) ]
        except KeyError:
            return None

    def __setitem__(self, id, value):
        """Dictionary-style field access, set a field's value.
        """
        if isinstance( value, Document ):
            self._documents[ str( id ) ] = value
            return value

    def __delitem__( self, id ):
        return self.remove( id )

    def __contains__( self, id ):
        id = id.pk if isinstance( id, Document ) else id
        value = self[ str( id ) ]
        return value is not None

    def __len__(self):
        return len( self._documents )

    def add( self, documents ):
        '''
        Add one or more documents to the cache.

        @param documents:
        @type documents: Document or list or set or QuerySet
        '''
        if isinstance( documents, Document ):
            if documents.pk:
                self._documents[ str( documents.pk ) ] = documents
        else:
            self._documents.update( ( str( obj.pk ), obj ) for obj in documents if obj.pk )

    def remove( self, documents ):
        '''
        Remove one or more documents from the cache.

        @param documents:
        @type documents: Document or list or set or QuerySet
        '''
        if isinstance( documents, Document ):
            if documents.pk:
                del self._documents[ str( documents.pk ) ]
        elif isinstance( documents, ObjectId ):
            del self._documents[ str( documents ) ]
        else:
            for obj in documents:
                self._documents.pop( str( obj.pk ) )

