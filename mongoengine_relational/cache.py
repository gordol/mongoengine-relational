from __future__ import print_function
from __future__ import unicode_literals

from mongoengine import Document
from mongoengine.base import ObjectId
from mongoengine.queryset import QuerySet
from bson import DBRef


class DocumentCache( object ):
    def __init__( self, request ):
        if not hasattr( request, 'cache' ):
            request.cache = self
        else:
            raise RuntimeError( 'A `DocumentCache` already exists; only one should be created per request.' )

        self.request = request
        self._documents = {}

    def __iter__( self ):
        return iter( self._documents )

    def __getitem__( self, id ):
        """Dictionary-style field access, return a field's value if present.
        """
        # This proxies to `self.get`
        return self.get( id )

    def __setitem__(self, id, value):
        """Dictionary-style field access, set a field's value.
        """
        if isinstance( value, Document ):
            # Set the `request` on the Document
            value._request = self.request

            self._documents[ str( id ) ] = value

            return value

    def __delitem__( self, id ):
        return self.remove( id )

    def __contains__( self, id ):
        id = id.id if isinstance( id, ( DBRef, Document ) ) else id
        value = self[ str( id ) ]
        return value is not None

    def __len__(self):
        return len( self._documents )

    def get( self, id, default=None ):
        try:
            id = id.id if isinstance( id, ( DBRef, Document ) ) else id
            return self._documents[ str( id ) ]
        except KeyError:
            return default

    def add( self, documents ):
        '''
        Add one or more documents to the cache.

        @param documents:
        @type documents: Document or list or set or QuerySet
        '''
        if not documents:
            return

        if isinstance( documents, Document ):
            if documents.pk and not documents in self:
                self[ documents.pk ] = documents
        elif isinstance( documents, ( list, set, QuerySet ) ):
            for obj in documents:
                if obj.pk and not obj in self:
                    self[ obj.pk ] = obj

    def remove( self, documents ):
        '''
        Remove one or more documents from the cache.

        @param documents:
        @type documents: DBRef or Document or ObjectId or list or set or QuerySet
        '''
        if not documents:
            return

        if isinstance( documents, ( DBRef, Document ) ):
            if documents.id:
                del self._documents[ str( documents.id ) ]
        elif isinstance( documents, ObjectId ):
            del self._documents[ str( documents ) ]
        elif isinstance( documents, ( list, set, QuerySet ) ):
            for obj in documents:
                self._documents.pop( str( obj.pk ) )

