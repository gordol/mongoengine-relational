from __future__ import print_function
from __future__ import unicode_literals

# Load `RelationManagerMixin` as early as possible, as it overrides classes on `mongoengine`
from mongoengine_relational import RelationManagerMixin

from mongoengine import *
from mongoengine_relational import *


class User( RelationManagerMixin, Document ):
    name = StringField()


class Office( RelationManagerMixin, Document ):
    name = StringField()
    tenant = GenericReferenceField( related_name='office' ) # one-to-one relation (incl. 1 side generic)


class Zoo( RelationManagerMixin, Document ):
    on_change_name_called = False
    on_change_animals_called = False

    name = StringField()
    animals = ListField( ReferenceField( 'Animal' ), related_name='zoo' ) # hasmany relation
    office = ReferenceField( 'Office', related_name='tenant' ) # one-to-one relation (incl. 1 side generic)

    def on_change_name( self, request, value, prev_value, **kwargs ):
        self.on_change_name_called = True
        print( 'name updated; new={}, old={}'.format( value, prev_value ) )

    def on_change_animals( self, request, added_docs, removed_docs, **kwargs ):
        self.on_change_animals_called = True
        print( ('animals updated; added={}, removed={}').format( added_docs, removed_docs ) )

    def __unicode__( self ):
        return unicode('{}: `{}` (id={})'.format( self.__class__.__name__, self.name, self.pk ))


class Animal( RelationManagerMixin, Document ):
    name = StringField()
    species = StringField( required=True )
    zoo = ReferenceField( 'Zoo', related_name='animals', required=True ) # hasmany relation

    def on_change_zoo( self, request, new_zoo, prev_zoo, **kwargs ):
        print( ('zoo updated; added={}, removed={}').format( new_zoo, prev_zoo ) )

    def __unicode__( self ):
        return unicode('{}: `{}` (id={})'.format( self.__class__.__name__, self.name, self.pk ))


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