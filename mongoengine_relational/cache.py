from __future__ import print_function
from __future__ import unicode_literals

import collections

from mongoengine import Document
from mongoengine.queryset import QuerySet
from bson import DBRef, ObjectId


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
            self._documents[ str( id ) ] = value

            # Set the `request` on the Document, so it can take advantage of the cache itself
            value._request = self.request

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
        @type documents: Document or QuerySet or Iterable
        @rtype Document or Document[]
        '''
        if isinstance( documents, Document ):
            # Set the `request` on the Document, so it can take advantage of the cache itself
            documents._request = self.request

            # If `documents` doesn't have a `pk`, continue.
            # If it does have a `pk`, set it as the cache entry for this doc if there's no entry yet,
            # or return the cache entry.
            if documents.pk:
                if documents in self:
                    documents = self[ documents.pk ]
                else:
                    self[ documents.pk ] = documents

            return documents

        elif isinstance( documents, ( QuerySet, collections.Iterable ) ):
            docs = []
            for obj in documents:
                # Set the `request` on the Document, so it can take advantage of the cache itself
                obj._request = self.request

                if obj.pk:
                    if obj in self:
                        obj = self[ obj.pk ]
                    else:
                        self[ obj.pk ] = obj

                if isinstance( obj, Document ):
                    docs.append( obj )

            return docs

    def remove( self, documents ):
        '''
        Remove one or more documents from the cache.

        @param documents:
        @type documents: DBRef or Document or ObjectId or list or set or QuerySet
        '''
        if isinstance( documents, ( DBRef, Document ) ):
            if documents.id:
                del self._documents[ str( documents.id ) ]
        elif isinstance( documents, ObjectId ):
            del self._documents[ str( documents ) ]
        elif isinstance( documents, ( list, set, QuerySet ) ):
            for obj in documents:
                self._documents.pop( str( obj.pk ) )

