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
            if self.request and hasattr( value, '_set_request' ) and callable( value._set_request ):
                value._set_request( self.request, update_relations=False )

            return value

    def __delitem__( self, id ):
        return self.remove( id )

    def __contains__( self, id ):
        object_id = id.id if isinstance( id, ( DBRef, Document ) ) else id
        return str( object_id ) in self._documents

    def __len__(self):
        return len( self._documents )

    def get( self, item, default=None ):
        object_id = None
        doc = None

        if isinstance( item, Document ):
            object_id = item.pk

            # If it's a new document (no pk), just return it. We can't cache it yet
            if not item.pk:
                doc = item
            # If it's an existing document and it's not yet in the cache, add it and return it
            elif str( object_id ) not in self._documents:
                self[ item.pk ] = item
                doc = item

        elif isinstance( item, DBRef ):
            object_id = item.id

        elif isinstance( item, ( ObjectId, basestring ) ):
            object_id = item

        if doc is None and object_id:
            try:
                doc = self._documents[ str( object_id ) ]
            except KeyError:
                pass

        return doc or default

    def add( self, documents ):
        '''
        Add one or more documents to the cache. Only Documents will be returned.

        @param documents:
        @type documents: Document or QuerySet or Iterable
        @rtype Document or Document[]
        '''
        docs = None

        if isinstance( documents, Document ):
            # Set the `request` on the Document, so it can take advantage of the cache itself
            docs = self._add_single_document( documents )

        elif isinstance( documents, ( QuerySet, collections.Iterable ) ):
            docs = []
            for obj in documents:
                if isinstance( obj, Document ):
                    obj = self._add_single_document( obj )
                    docs.append( obj )

        return docs

    def _add_single_document( self, doc ):
        '''
        Add a single document to the cache, or replace it with it's cached duplicate if an instance of that
        document was already present.

        @type doc: Document
        '''
        # Set the `request` on the Document, so it can take advantage of the cache itself
        if self.request and hasattr( doc, '_set_request' ) and callable( doc._set_request ):
            doc._set_request( self.request, update_relations=False )

        # If `doc` doesn't have a `pk`, continue.
        # If it does have a `pk`, set it as the cache entry for this document if there's no entry yet,
        # or return the cache entry.
        if doc.pk:
            if doc in self:
                doc = self._documents[ str( doc.pk ) ]
            else:
                self[ doc.pk ] = doc

        return doc

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
                if obj.pk:
                    del self._documents[ str( obj.pk ) ]