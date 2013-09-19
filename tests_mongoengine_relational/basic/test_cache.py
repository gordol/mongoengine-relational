from __future__ import print_function
from __future__ import unicode_literals

import unittest
import mongoengine

from mongoengine_relational import *

from bson import DBRef, ObjectId

from pyramid import testing
from pyramid.request import Request

from tests_mongoengine_relational.basic.documents import *
from tests_mongoengine_relational.utils import Struct


class CacheTestCase( unittest.TestCase ):

    def setUp( self ):
        mongoengine.register_connection( mongoengine.DEFAULT_CONNECTION_NAME, 'mongoengine_relational_test' )
        c = mongoengine.connection.get_connection()
        c.drop_database( 'mongoengine_relational_test' )

        # Setup application/request config
        self.request = Request.blank( '/api/v1/' )
        self.config = testing.setUp( request=self.request )

        # Setup data
        d = self.data = Struct()

        d.blijdorp = Zoo( name='Blijdorp' )
        d.office = Office( tenant=d.blijdorp )
        d.bear = Animal( name='Baloo', species='bear', zoo=d.blijdorp )

        d.dolphin = Animal( id=ObjectId(), name='Flipper', species='dolphin' )
        d.mammoth = Animal( id=ObjectId(), name='Manny', species='mammoth' )
        d.artis = Zoo( id=ObjectId(), name='Artis', animals=[ d.mammoth ] )
        d.tiger = Animal( id=ObjectId(), name='Shere Khan', species='tiger', zoo=d.artis )

        d.cache = DocumentCache( self.request )


    def tearDown( self ):
        testing.tearDown()

        # Clear our references
        self.data = None


    def test_create_cache( self ):
        d = self.data

        # The `cache` has attached itself to the `request`
        self.assertTrue( hasattr( self.request, 'cache' ) )
        self.assertEqual( d.cache, self.request.cache )

        # Only one `DocumentCache` should be instantiated per request
        self.assertRaises( RuntimeError, DocumentCache, self.request )

        # Documents keep their local cache until they get access to the global cache
        a1 = Animal()
        self.assertTrue( isinstance( a1._cache, DocumentCache ) )
        self.assertFalse( a1._cache == d.cache )
        a1._set_request( self.request )
        self.assertTrue( a1._cache == d.cache )

        a2 = Animal( request=self.request )
        self.assertTrue( a2._cache == d.cache )

    def test_add_remove_documents( self ):
        d = self.data

        # A document isn't cached if it doesn't have an id
        d.cache.add( d.blijdorp )
        self.assertFalse( d.blijdorp in d.cache )

        d.cache.add( d.mammoth )
        self.assertEquals( d.mammoth, d.cache[ d.mammoth.pk ] )
        self.assertEquals( d.mammoth, d.cache[ d.mammoth ] )

        d.cache.add( [ d.artis, d.tiger ] )
        self.assertEquals( d.tiger, d.cache[ d.tiger.id ] )
        self.assertEquals( d.tiger, d.cache[ d.tiger ] )

        # Test contains, len
        self.assertFalse( d.blijdorp in d.cache )
        self.assertTrue( d.tiger in d.cache )

        self.assertEqual( len( d.cache ), 3 )

        # Test removal of single items, then removal of lists
        del d.cache[ d.tiger ]
        self.assertNotIn( d.tiger, d.cache )

        d.cache.remove( d.mammoth.id )
        self.assertNotIn( d.mammoth, d.cache )

        d.cache.remove( [ d.artis ] )
        self.assertNotIn( d.artis, d.cache )

    def test_document_get( self ):
        d = self.data

        # Get something silly, non-relational
        self.assertEqual( d.artis.name, 'Artis' )

        # Trying to get a doc that isn't in the cache yet should add it
        self.assertFalse( d.dolphin in d.cache )
        dolphin = d.cache[ d.dolphin ]
        self.assertTrue( d.dolphin in d.cache )
        self.assertTrue( id( dolphin ) == id( d.dolphin ) )

        # Add `tiger` to the cache; its zoo isn't in there before, but should appear since `tiger` now knows the request
        self.assertFalse( d.artis in d.cache )
        d.cache.add( d.tiger )
        # self.assertTrue( d.artis in d.cache )

        self.assertTrue( d.tiger in d.artis.animals )

    def test_document_get_dbref( self ):
        d = self.data

        # Get a list of docs (contains one DBRef, some Documents)
        lion = DBRef( 'Animal', ObjectId() )
        lion_doc = Animal( id=lion.id, name="Simba" )

        # Add `lion` to `animals`, and `lion_doc` to the cache; the cache should be able to find it
        self.assertIsNone( lion_doc.zoo )
        d.artis.animals.append( lion )
        d.cache.add( lion_doc )

        # Since `artis` doesn't have a `_request` property yet, it can't utilize the cache yet, and thus won't find `lion`
        self.assertTrue( not hasattr( d.artis, '_request' ) or not d.artis._request )

        self.assertFalse( lion_doc in d.artis.animals )
        d.artis._set_request( self.request )
        self.assertTrue( lion_doc in d.artis.animals )

        print( d.artis, lion_doc._data['zoo'])

        # `artis` has latched onto `request`, and can now find the cache
        self.assertTrue( d.artis._request, self.request )
        self.assertEquals( lion_doc.zoo, d.artis )










