from __future__ import print_function
from __future__ import unicode_literals

import unittest

from tests_mongoengine_relational.utils import FauxSave, Struct, get_object_id

from pyramid import testing
from pyramid.response import Response
from pyramid.request import Request

from mongoengine import *
from mongoengine_relational import *
from bson import DBRef

from mongoengine_relational.relationalmixin import set_difference


class User( RelationManagerMixin, Document ):
    name = StringField()


class Office( RelationManagerMixin, Document ):
    name = StringField()
    tenant = GenericReferenceField( related_name='office' ) # one-to-one relation (incl. 1 side generic)


class Zoo( RelationManagerMixin, Document ):
    name = StringField()
    animals = ListField( ReferenceField( 'Animal' ), related_name='zoo' ) # hasmany relation
    office = ReferenceField( 'Office', related_name='tenant' ) # one-to-one relation (incl. 1 side generic)

    def on_change_animals( self, value, old_value, added_docs, removed_docs, **kwargs ):
        print( ('animals updated; new={}, old={}, added={}, removed={}').format( value, old_value, added_docs, removed_docs ) )


class Animal( RelationManagerMixin, Document ):
    name = StringField()
    species = StringField( required=True )
    zoo = ReferenceField( 'Zoo', related_name='animals', required=True ) # hasmany relation

    def on_change_zoo( self, value, old_value, **kwargs ):
        print( ('zoo updated; new={}, old={}').format( value, old_value ) )


class Node( RelationManagerMixin, Document ):
    name = StringField()
    parent = ReferenceField( 'Node', related_name='children' ) # hasone relation
    children = ListField( ReferenceField( 'Node' ), related_name='parent' ) # hasmany relation


class Book( RelationManagerMixin, Document ):
    name = StringField()
    author = ReferenceField( 'User' )
    pages = ListField( ReferenceField( 'Page' ) ) # one-to-many without a related field


class Page( RelationManagerMixin, Document ):
    pass


class RelationsTestCase( unittest.TestCase ):

    def setUp( self ):
        # Setup application/request config
        self.request = Request.blank( '/api/v1/' )
        self.config = testing.setUp( request=self.request )

        # Setup data
        d = self.data = Struct()

        d.blijdorp = Zoo( name='Blijdorp' )
        d.office = Office( tenant=d.blijdorp )
        d.bear = Animal( name='Baloo', species='bear', zoo=d.blijdorp )

        d.mammoth = Animal( id=get_object_id(), name='Manny', species='mammoth' )
        d.artis = Zoo( id=get_object_id(), name='Artis', animals=[ d.mammoth ] )
        d.tiger = Animal( id=get_object_id(), name='Shere Khan', species='tiger', zoo=d.artis )

        d.node = Node()


    def tearDown( self ):
        testing.tearDown()

        # Clear our references
        self.data = None


    def test_documents_without_relations( self ):
        book = Book( id=get_object_id(), author=User( name='A' ), name='B' )
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

        print( 'artis.get_changed_relations: ', d.artis.get_changed_relations() )
        self.assertEqual( 1, len( d.artis.get_changed_relations() ) )
        d.artis.save( request=self.request )
        self.assertEqual( 0, len( d.artis.get_changed_relations() ) )

        # put 'bear' in 'artis'
        d.artis.animals.append( d.bear )

        self.assertEqual( 1, len( d.artis.get_changed_relations() ) )
        self.assertEqual( d.bear.zoo, d.artis )

        # after saving 'artis', the 'zoo' on 'bear' should be set to 'artis'
        d.artis.save( request=self.request )

        self.assertEqual( 0, len( d.artis.get_changed_relations() ) )
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


    def test_update_hasone( self ):
        d = self.data
        
        # give 'artis' an office
        office = Office( id=get_object_id() )
        d.artis.office = office

        # 'office.tenant' has been set to 'artis' right away
        self.assertEqual( office.tenant, d.artis )

        d.artis.save( request=self.request )

        self.assertEqual( 0, len( d.artis.get_changed_relations() ) )
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


    def test_get_changed_relations( self ):
        self.assertEqual( 0, len( self.data.tiger.get_changed_relations() ) )

        changed_relations = self.data.bear.get_changed_relations()
        self.assertIn( 'zoo', changed_relations )


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
        lion = DBRef( 'Animal', get_object_id() )
        lion_doc = Animal( id=lion.id, name="Simba" )
        giraffe = DBRef( 'Animal', get_object_id() )
        giraffe_doc = Animal( id=giraffe.id, name='Giraffe' )
        office = DBRef( 'Office', get_object_id() )
        office_doc = Office( id=office.id )

        self.assertTrue( lion_doc._equals( lion ) )

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
        zoo = Zoo( id=get_object_id(), name="Dierenpark Emmen", animals=[ lion, giraffe ], office=office )

        self.assertFalse( zoo.get_changed_relations() )

        # dereference a `hasmany`; use `_data` to avoid dereferencing
        zoo._data[ 'animals' ].remove( lion )
        zoo._data[ 'animals' ].append( lion_doc )

        # dereference a `hasone`
        zoo._data[ 'office' ] = office_doc

        self.assertFalse( zoo.get_changed_relations() )


    def test_delete( self ):
        d = self.data

        # Give `artis` an office as well, and persist it for `changed_relations`; it should not have 2 animals and an office
        office = Office( id=get_object_id() )
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

        changes = d.artis.get_changed_relations()

        print( changes )


    def test_memoize_documents( self ):
        pass


    def test_delete_rules( self ):
        pass



