from __future__ import print_function
from __future__ import unicode_literals

import unittest

from mongoengine_relational import *

from tests_mongoengine_relational.utils import Struct
from bson import DBRef, ObjectId

from pyramid import testing
from pyramid.response import Response
from pyramid.request import Request

from tests_mongoengine_relational.basic.documents import *


class CacheTestCase( unittest.TestCase ):

    def setUp( self ):
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

        d.node = Node()

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

    def test_add_remove_documents( self ):
        d = self.data

        # A document isn't cached if it doesn't have an id
        d.cache.add( d.blijdorp )
        self.assertEquals( None, d.cache[ d.blijdorp ] )

        d.cache.add( d.mammoth )
        self.assertEquals( d.mammoth, d.cache[ d.mammoth.pk ] )
        self.assertEquals( d.mammoth, d.cache[ d.mammoth ] )

        d.cache.add( [ d.artis, d.tiger ] )
        self.assertEquals( d.tiger, d.cache[ d.tiger.id ] )
        self.assertEquals( d.tiger, d.cache[ d.tiger ] )

        # Test contains, len
        self.assertFalse( d.blijdorp in d.cache )
        self.assertTrue( d.tiger in d.cache._documents.values() )

        self.assertEqual( len( d.cache ), 3 )

        # Test removal of single items, then removal of lists
        del d.cache[ d.tiger ]
        self.assertNotIn( d.tiger, d.cache._documents.values() )

        d.cache.remove( d.mammoth.id )
        self.assertNotIn( d.mammoth, d.cache._documents.values() )

        d.cache.remove( [ d.artis ] )
        self.assertNotIn( d.artis, d.cache._documents.values() )

    def test_document_get( self ):
        d = self.data

        # Get something silly, non-relational
        self.assertEqual( d.artis.name, 'Artis' )

        # Trying to get a doc that isn't in the cache yet should add it
        self.assertFalse( d.dolphin in d.cache._documents.values() )
        self.assertTrue( d.dolphin in d.cache )

        # Add `tiger` to the cache; its zoo isn't in there before, but should appear since `tiger` now knows the request
        self.assertFalse( d.artis in d.cache._documents.values() )
        d.cache.add( d.tiger )
        # self.assertTrue( d.artis in d.cache._documents.values() )

        self.assertEqual( d.artis, d.tiger.zoo )
        self.assertTrue( d.tiger in d.artis.animals )

        # Get a list of docs (contains one DBRef, some Documents)
        lion = DBRef( 'Animal', ObjectId() )
        lion_doc = Animal( id=lion.id, name="Simba" )

        # Add `lion` to `animals`, and `lion_doc` to the cache; the cache should be able to find it for `artis`
        d.artis.animals.append( lion )
        d.cache.add( lion_doc )

        self.assertTrue( lion_doc in d.artis._fetch( self.request, 'animals' ) )










