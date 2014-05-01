from __future__ import print_function
from __future__ import unicode_literals

import unittest
import mongoengine

from bson import DBRef, ObjectId

from pyramid import testing
from pyramid.request import Request

from mongoengine_relational.relationalmixin import set_difference, equals

from tests_mongoengine_relational.basic.documents import *
from tests_mongoengine_relational.utils import Struct


class RelationsTestCase( unittest.TestCase ):

    def setUp( self ):
        mongoengine.register_connection( mongoengine.DEFAULT_CONNECTION_NAME, 'mongoengine_relational_test' )
        c = mongoengine.connection.get_connection()
        c.drop_database( 'mongoengine_relational_test' )

        # Setup application/request config
        self.request = Request.blank( '/api/v1/' )

        # Instantiate a DocumentCache; it will attach itself to `request.cache`.
        DocumentCache( self.request )

        self.config = testing.setUp( request=self.request )

        # Setup data
        d = self.data = Struct()

        d.blijdorp = Zoo( id=ObjectId(), name='Blijdorp' )
        d.office = Office( tenant=d.blijdorp )
        d.bear = Animal( name='Baloo', species='bear', zoo=d.blijdorp )

        d.mammoth = Animal( id=ObjectId(), name='Manny', species='mammoth' )
        d.artis = Zoo( id=ObjectId(), name='Artis', animals=[ d.mammoth ] )
        d.tiger = Animal( id=ObjectId(), name='Shere Khan', species='tiger', zoo=d.artis )

        d.node = Node()

    def tearDown( self ):
        testing.tearDown()

        # Clear our references
        self.data = None

    def test_documents_without_relations( self ):
        book = Book( id=ObjectId(), author=User( name='A' ), name='B' )
        page = Page()

        book.pages.append( page )
        book.author = User( name='B' )

    def test_baselist( self ):
        d = self.data

        # test BaseList del/pop
        del d.artis.animals[ 0 ]
        d.artis.animals.pop()

        self.assertEqual( d.mammoth.zoo, None )
        self.assertEqual( d.tiger.zoo, None )

        # test append / extend
        d.artis.animals.append( d.mammoth )
        d.artis.animals.extend( [ d.tiger ] )

        self.assertEqual( d.mammoth.zoo, d.artis )
        self.assertEqual( d.tiger.zoo, d.artis )

        # test remove/insert
        d.artis.animals.remove( d.tiger )
        d.artis.animals.insert( 0, d.tiger )

    def test_create_document( self ):
        d = self.data

        # Without id
        self.assertEqual( 3, len( d.bear.get_changed_fields() ) )
        d.bear.save( self.request )
        self.assertEqual( 0, len( d.bear.get_changed_fields() ) )

        # With id
        self.assertEqual( 0, len( d.tiger.get_changed_fields() ) )

    def test_relation_initialization( self ):
        d = self.data

        # since `office` doesn't have an id, `update_relations` is not called on init
        self.assertNotEqual( d.office, d.blijdorp.office )

        self.assertEqual( d.blijdorp, d.bear.zoo )

        # relations are known on both sides
        # propagated from hasone to the other side
        self.assertEqual( d.artis, d.tiger.zoo )
        self.assertIn( d.tiger, d.artis.animals )

        # propagated from hasmany to the other side
        self.assertEqual( d.mammoth.zoo, d.artis )
        self.assertIn( d.mammoth, d.artis.animals )

    def test_memo_initialization_no_id( self ):
        d = self.data

        # _memo keys have been created
        self.assertIn( 'zoo', d.bear._memo_hasone )
        self.assertIn( 'animals', d.blijdorp._memo_hasmany )
        self.assertIn( 'office', d.blijdorp._memo_hasone )

        # but since the objects were created without id, _memo shouldn't be populated
        self.assertEqual( None, d.bear._memo_hasone[ 'zoo' ], "no `id`, so no memo contents initially" )
        self.assertItemsEqual( [], d.blijdorp._memo_hasmany[ 'animals' ], "no `id`, so no memo contents initially" )

    def test_memo_initialization_with_id( self ):
        d = self.data

        # the objects were created with ids, so _memo should be populated
        self.assertEqual( d.artis, d.tiger._memo_hasone[ 'zoo' ], "'zoo' should be in 'tiger's memo" )

        self.assertNotIn( d.tiger, d.artis._memo_hasmany[ 'animals' ], "'tiger' should be in 'zoo's memo" )

        d.artis.save( request=self.request )

        self.assertIn( d.tiger, d.artis._memo_hasmany[ 'animals' ], "'tiger' should be in 'zoo's memo" )

    def test_update_hasmany( self ):
        d = self.data

        print( 'artis.get_changed_fields: ', d.artis.get_changed_fields() )
        self.assertEqual( 1, len( d.artis.get_changed_fields() ) )
        d.artis.save( request=self.request )
        self.assertEqual( 0, len( d.artis.get_changed_fields() ) )

        # put 'bear' in 'artis'
        d.artis.animals.append( d.bear )

        self.assertEqual( 1, len( d.artis.get_changed_fields() ) )
        self.assertEqual( d.bear.zoo, d.artis )

        # after saving 'artis', the 'zoo' on 'bear' should be set to 'artis'
        d.bear.save( self.request )
        d.artis.save( self.request )

        self.assertEqual( 0, len( d.artis.get_changed_fields() ) )
        self.assertEqual( d.bear.zoo, d.artis )

        # move the 'bear' to 'blijdorp'. It should be removed from 'artis'
        d.blijdorp.animals.append( d.bear )

        self.assertNotIn( d.bear, d.artis.animals )
        self.assertIn( d.bear, d.blijdorp.animals )

        d.blijdorp.save( request=self.request )

        self.assertEqual( d.bear.zoo, d.blijdorp )
        self.assertNotIn( d.bear, d.artis.animals )
        self.assertIn( d.bear, d.blijdorp.animals )

        # now that 'bear' is in 'blijdorp', 'tiger' wants to move to 'blijdorp' as well
        d.tiger.zoo = d.blijdorp

        self.assertNotIn( d.tiger, d.artis.animals )
        self.assertIn( d.tiger, d.blijdorp.animals )

        d.tiger.save( request=self.request )

        self.assertNotIn( d.tiger, d.artis.animals )
        self.assertIn( d.tiger, d.blijdorp.animals )

        # Reset `d.blijdorp.animals` by assigning it an empty list
        d.blijdorp.animals = []

        self.assertFalse( d.bear.zoo, d.blijdorp )
        self.assertNotIn( d.bear, d.blijdorp.animals )

    def test_update_hasone( self ):
        d = self.data

        # give 'artis' an office
        office = Office( id=ObjectId() )
        d.artis.office = office

        # 'office.tenant' has been set to 'artis' right away
        self.assertEqual( office.tenant, d.artis )

        d.artis.save( request=self.request )

        self.assertEqual( 0, len( d.artis.get_changed_fields() ) )
        self.assertEqual( office.tenant, d.artis )

        # the office decides it'd rather have 'zoo' as a tenant; 'artis' are making a mess of it.
        # 'office' should be added to the 'blijdorp' side, and removed from the 'artis' side after saving.
        office.tenant = d.blijdorp

        self.assertEqual( office.tenant, d.blijdorp )
        self.assertEqual( office, d.blijdorp.office )
        self.assertNotEqual( office, d.artis.office )

        office.save( request=self.request )
        d.artis.save( request=self.request )

        self.assertEqual( office.tenant, d.blijdorp )
        self.assertEqual( office, d.blijdorp.office )
        self.assertNotEqual( office, d.artis.office )

    def get_changed_fields( self ):
        d = self.data

        self.assertIn( 'zoo', d.bear.get_changed_fields() )

        self.assertEqual( 0, len( d.tiger.get_changed_fields() ) )

        d.tiger.zoo = d.blijdorp

        self.assertIn( 'zoo', d.tiger.get_changed_fields() )

        # Test `on_change` for a related field
        self.assertEqual( d.blijdorp.on_change_animals_called, False )
        d.blijdorp.save( request=self.request )
        self.assertEqual( d.blijdorp.on_change_animals_called, True )

        # Test `on_change` for a regular field
        d.artis.save( request=self.request )
        self.assertEqual( d.artis.on_change_name_called, False )
        d.artis.name = 'New Artis'
        self.assertEqual( d.artis.on_change_name_called, False )
        d.artis.save( request=self.request )
        self.assertEqual( d.artis.on_change_name_called, True )

    def test_update_managed_relations( self ):
        d = self.data

        print( d.blijdorp.animals, d.blijdorp._memo_hasmany[ 'animals' ] )

        self.assertNotIn( d.bear, d.blijdorp.animals, "'bear' should not be in 'zoo' yet" )

        self.assertFalse( d.bear.update_relations(), "`update_relations` should return False, since `bear` doesn't have an `id` yet.")

        # "save" bear by giving it an id, and running `update_relations`.
        d.bear.save( request=self.request )

        self.assertEqual( d.blijdorp, d.bear._memo_hasone[ 'zoo' ], "'zoo' memoized in 'bear's _memo_hasone now" )
        self.assertIn( d.bear, d.blijdorp.animals, "'bear' should be in 'zoo' now" )

        try:
            d.blijdorp.validate()
        except ValidationError as e:
            print( e, e.errors )
            raise e

    def test_document_dbref_equality( self ):
        # If an document has been fetched from the database, it's relations can just contain DbRefs,
        # instead of Documents.
        lion = DBRef( 'Animal', ObjectId() )
        lion_doc = Animal( id=lion.id, name="Simba" )
        giraffe = DBRef( 'Animal', ObjectId() )
        giraffe_doc = Animal( id=giraffe.id, name='Giraffe' )
        office = DBRef( 'Office', ObjectId() )
        office_doc = Office( id=office.id )

        self.assertTrue( equals(lion_doc, lion) )

        # No diff; sets are for the same objectIds
        self.assertFalse( set_difference( { lion, giraffe }, { lion_doc, giraffe_doc } ) )

        # removed: `lion`
        diff = set_difference( { lion, giraffe }, { giraffe_doc } )
        self.assertEqual( len( diff ), 1 )
        self.assertIn( lion, diff )

        # removed: `lion`
        diff = set_difference( { lion, office }, { office_doc, giraffe_doc } )
        self.assertEqual( len( diff ), 1 )
        self.assertIn( lion, diff )

        # removed: `giraffe`
        diff = set_difference( { lion, giraffe, office }, { office, lion_doc } )
        self.assertEqual( len( diff ), 1 )
        self.assertIn( giraffe, diff )

        # No diff; second set is a superset of the first set
        diff = set_difference( { lion, office }, { lion_doc, office_doc, giraffe_doc } )
        self.assertEqual( len( diff ), 0 )

        # removed: the new Document
        diff = set_difference( { Animal( name='John Doe' ) }, {} )
        self.assertEqual( len( diff ), 1 )


        # Moving on; substituting DbRef with a Document (dereferencing) shouldn't mark a relation as changed
        zoo = Zoo( id=ObjectId(), name="Dierenpark Emmen", animals=[ lion, giraffe ], office=office )

        self.assertFalse( zoo.get_changed_fields() )

        # dereference a `hasmany`; use `_data` to avoid dereferencing
        zoo._data[ 'animals' ].remove( lion )
        zoo._data[ 'animals' ].append( lion_doc )

        # dereference a `hasone`
        zoo._data[ 'office' ] = office_doc

        self.assertFalse( zoo.get_changed_fields() )

    def test_delete( self ):
        d = self.data

        # Give `artis` an office as well, and persist it for `changed_relations`; it should not have 2 animals and an office
        office = Office( id=ObjectId() )
        d.artis.office = office
        d.artis.save( request=self.request )

        # relations on other models that should point to `d.artis`
        self.assertEqual( d.mammoth.zoo, d.artis )
        self.assertEqual( d.tiger.zoo, d.artis )
        self.assertEqual( office.tenant, d.artis )

        d.artis.clear_relations()

        # relations on other models that pointed to `d.artis` should be cleared
        self.assertEqual( d.mammoth.zoo, None )
        self.assertEqual( d.tiger.zoo, None )
        self.assertEqual( office.tenant, None )

        changes = d.artis.get_changed_fields()

        self.assertIn( 'animals', changes )
        self.assertIn( 'office', changes )

    def test_reload( self ):
        d = self.data

        d.artis.save( self.request )
        d.mammoth.save( self.request )
        d.tiger.save( self.request )

        d.artis.reload()

        # Check if `reload` uses the documents as already present in the cache, or constructs new ones
        self.assertListEqual( d.artis.animals, [ d.mammoth, d.tiger ] )
        self.assertEqual( id( d.artis.animals[ 0 ] ), id( d.mammoth ) )

    def test_memoize_documents( self ):
        pass

    def test_delete_rules( self ):
        pass

    def test_update( self ):
        d = self.data

        self.assertIn( 'animals', d.artis.get_changed_fields() )

        # Updating a relation should memoize it, and thus remove it from `get_changed_relations`
        d.artis.update( self.request, 'animals' )
        self.assertNotIn( 'animals', d.artis.get_changed_fields() )

