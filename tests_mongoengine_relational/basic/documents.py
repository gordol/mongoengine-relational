from __future__ import print_function
from __future__ import unicode_literals

# Load `RelationManagerMixin` as early as possible, as it overrides classes on `mongoengine`
from mongoengine_relational import RelationManagerMixin

from mongoengine import *
from mongoengine_relational import *
from bson import DBRef


class User( RelationManagerMixin, Document ):
    name = StringField()


class Office( RelationManagerMixin, Document ):
    name = StringField()
    tenant = GenericReferenceField( related_name='office' ) # one-to-one relation (incl. 1 side generic)


class Zoo( RelationManagerMixin, Document ):
    name = StringField()
    animals = ListField( ReferenceField( 'Animal' ), related_name='zoo' ) # hasmany relation
    office = ReferenceField( 'Office', related_name='tenant' ) # one-to-one relation (incl. 1 side generic)

    def on_change_animals( self, request, value, prev_value, added_docs, removed_docs, **kwargs ):
        print( ('animals updated; new={}, old={}, added={}, removed={}').format( value, prev_value, added_docs, removed_docs ) )


class Animal( RelationManagerMixin, Document ):
    name = StringField()
    species = StringField( required=True )
    zoo = ReferenceField( 'Zoo', related_name='animals', required=True ) # hasmany relation

    def on_change_zoo( self, request, value, prev_value, **kwargs ):
        print( ('zoo updated; new={}, old={}').format( value, prev_value ) )


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