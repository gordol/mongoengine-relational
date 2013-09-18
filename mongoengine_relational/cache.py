from __future__ import print_function
from __future__ import unicode_literals

import collections

from mongoengine import Document
from mongoengine.queryset import QuerySet
from bson import DBRef, ObjectId


class DocumentCache( object ):
    def __init__( self, request=None ):
        if request:
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
            if self.request:
                value._set_request( self.request )

            return value

    def __delitem__( self, id ):
        return self.remove( id )

    def __contains__( self, id ):
        object_id = id.id if isinstance( id, ( DBRef, Document ) ) else id
        doc = self[ str( object_id ) ]

        if not doc and isinstance( id, Document ) and id.pk:
            self[ id.pk ] = id
            doc = id

        return doc is not None

    def __len__(self):
        return len( self._documents )

    def get( self, id, default=None ):
        try:
            object_id = id.id if isinstance( id, ( DBRef, Document ) ) else id

            if not str( object_id ) in self._documents and isinstance( id, Document ) and id.pk:
                self[ id.pk ] = id

            return id if isinstance( id, Document ) else self._documents[ str( object_id ) ]
        except KeyError:
            return default

    def add( self, documents ):
        '''
        Add one or more documents to the cache. Only Documents will be returned.

        @param documents:
        @type documents: Document or QuerySet or Iterable
        @rtype Document or Document[]
        '''
        if isinstance( documents, Document ):
            # Set the `request` on the Document, so it can take advantage of the cache itself
            if self.request:
                documents._set_request( self.request )

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
                if isinstance( obj, Document ):
                    # Set the `request` on the Document, so it can take advantage of the cache itself
                    if self.request:
                        obj._set_request( self.request )

                    if obj.pk:
                        if obj in self:
                            obj = self[ obj.pk ]
                        else:
                            self[ obj.pk ] = obj

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

