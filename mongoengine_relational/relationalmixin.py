from __future__ import print_function
from __future__ import unicode_literals

from pyramid.request import Request

from mongoengine import Document, GenericReferenceField, ReferenceField, ListField, ValidationError
from mongoengine.base import ComplexBaseField
from mongoengine.common import _import_class
from mongoengine import base
from mongoengine.queryset import CASCADE, DO_NOTHING, NULLIFY, DENY, PULL
from bson import DBRef, ObjectId, SON

import copy

from .cache import DocumentCache

# from kitchen.text.converters import getwriter
# import sys
# UTF8Writer = getwriter('utf8')
# sys.stdout = UTF8Writer(sys.stdout)

class BaseList( list ):
    '''
    Overridden `BaseList`, so we can track changes made to `toMany` relations.
    '''

    _dereferenced = False
    _instance = None
    _observer = None
    _name = None

    def __init__( self, list_items, instance, name ):
        self._instance = instance
        self._name = name
        if hasattr( self._instance, 'add_hasmany' ) and hasattr( self._instance, 'remove_hasmany' ):
            self._observer = self._instance

        super( BaseList, self ).__init__( list_items )

    def __setitem__( self, key, element ):
        self._mark_as_changed()

        try:
            old_element = self.__getitem__( key )
        except KeyError:
            pass
        else:
            # Only remove when there was no KeyError
            if self._observer:
                self._observer.remove_hasmany( self._name, old_element )

        # Always set the element.
        if self._observer:
            self._observer.add_hasmany( self._name, element )

        return super( BaseList, self ).__setitem__( key, element )

    def __delitem__( self, index ):
        self._mark_as_changed()

        old_element = list.__getitem__( self, index )
        result = super( BaseList, self ).__delitem__( index )

        if self._observer:
            self._observer.remove_hasmany( self._name, old_element )

        return result

    def __getstate__( self ):
        return self

    def __setstate__( self, state ):
        self = state
        return self

    def append( self, element ):
        self._mark_as_changed()

        result = super( BaseList, self ).append( element )
        if self._observer:
            self._observer.add_hasmany( self._name, element )

        return result

    def extend( self, iterable ):
        self._mark_as_changed()

        result = super(BaseList, self).extend( iterable )

        if self._observer:
            for element in iterable:
                self._observer.add_hasmany( self._name, element )

        return result

    def insert( self, index, element ):
        self._mark_as_changed()

        result = super( BaseList, self ).insert( index, element )
        if self._observer: 
            self._observer.add_hasmany( self._name, element )

        return result

    def pop( self, index=None ):
        self._mark_as_changed()

        result = super(BaseList, self).pop( index ) if index else super(BaseList, self).pop()
        if self._observer:
            self._observer.remove_hasmany( self._name, result )

        return result

    def remove( self, element ):
        self._mark_as_changed()

        result = super(BaseList, self).remove( element )
        if self._observer:
            self._observer.remove_hasmany( self._name, element )

        return result

    def reverse( self ):
        self._mark_as_changed()
        return super( BaseList, self ).reverse()

    def sort( self, *args, **kwargs ):
        self._mark_as_changed()
        return super( BaseList, self ).sort( *args, **kwargs )

    def _mark_as_changed( self ):
        if hasattr( self._instance, '_mark_as_changed' ):
            self._instance._mark_as_changed( self._name )

# Assign `BaseList` to `base` in order to override mongengine's default `BaseList`
base.BaseList = BaseList


class RelationalError( Exception ):
    pass


class ReferenceField( ReferenceField ):
    '''
    Adds a `related_name` argument to MongoEngine's `ReferenceField` for use in
    managing reverse relations.

    The `related_name` should point to a `ListField`, `ReferenceField` or
    `GenericReferenceField`.  The corresponding field may or may not have a
    `related_name` argument pointing back here.
    '''
    
    def __init__(self, document_type, **kwargs):
        related_name = kwargs.pop( 'related_name', None )
        super( ReferenceField, self ).__init__( document_type, **kwargs )
        if related_name and isinstance( related_name, basestring ):
            self.related_name = related_name

    def __get__( self, instance, owner ):
        """Descriptor to allow lazy dereferencing.
        """
        if instance is None:
            # Document class being used rather than a document object
            return self

        # Get value from document instance if available
        value = instance._data.get( self.name )
        self._auto_dereference = instance._fields[ self.name ]._auto_dereference
        # Dereference DBRefs
        if self._auto_dereference and isinstance( value, DBRef ):
            result = None

            if hasattr( instance, '_cache' ):
                result = instance._fetch( self.name )

            if value and not result:
                value = self.document_type._get_db().dereference( value )
                if value is not None:
                    result = self.document_type._from_son( value )
                    instance._data[self.name] = result

                    if hasattr( instance, '_cache' ):
                        instance._cache.add( result )

        return super( ReferenceField, self ).__get__( instance, owner )


class GenericReferenceField( GenericReferenceField ):
    '''
    Adds a `related_name` argument to MongoEngine's ``GenericReferenceField``
    for use in managing reverse relations.

    The `related_name`` should point to a `ListField`, `ReferenceField` or
    `GenericReferenceField`.  The corresponding field may or may not have a
    `related_name` argument pointing back here.
    '''

    def __init__(self, **kwargs):
        related_name = kwargs.pop( 'related_name', None )
        super( GenericReferenceField, self ).__init__( **kwargs )
        if related_name and isinstance( related_name, basestring ):
            self.related_name = related_name

    def __get__(self, instance, owner):
        if instance is None:
            return self

        value = instance._data.get( self.name )
        self._auto_dereference = instance._fields[ self.name ]._auto_dereference
        if self._auto_dereference and isinstance( value, (dict, SON) ):
            result = None

            if hasattr( instance, '_cache' ):
                result = instance._fetch( self.name )

            if value and not result:
                result = self.dereference( value )
                instance._data[self.name] = result

                if hasattr( instance, '_cache' ):
                    instance._cache.add( result )

        return super( GenericReferenceField, self ).__get__( instance, owner )


class ListField( ListField ):
    '''
    Adds a `related_name` argument to MongoEngine's `ListField` for use in
    managing reverse relations.

    The `related_name` should point to a `ReferenceField`. The corresponding
    `ReferenceField` may or may not have a `related_name` argument pointing
    back here.
    '''

    def __init__(self, field=None, **kwargs):
        related_name = kwargs.pop('related_name', None)
        super(ListField, self).__init__(field=field, **kwargs)
        if related_name and isinstance(related_name, basestring):
            self.related_name = related_name

    def __get__( self, instance, owner ):
        """Descriptor to automatically dereference references.
        """
        if instance is None:
            # Document class being used rather than a document object
            return self

        # We only care about lists that contain documents/references here.
        # Code is adapted from `ComplexBaseField.__get__`.
        if isinstance( self.field, ( GenericReferenceField, ReferenceField ) ):
            # dereference = self._auto_dereference
            _dereference = _import_class("DeReference")()

            self._auto_dereference = instance._fields[self.name]._auto_dereference
            # If we ever uncomment the piece below, make sure to include something like `if hasattr( instance, '_cache' ) and all( instance._cache[ doc ] for doc in value ): pass`
            # if instance._initialised and dereference:
            #     instance._data[self.name] = _dereference(
            #         instance._data.get(self.name), max_depth=1, instance=instance,
            #         name=self.name
            #     )

            # Skip `ComplexBaseField`, we're modifying that code right here; retrieve document data from `BaseField`
            value = super( ComplexBaseField, self ).__get__( instance, owner )

            # Convert lists to BaseList so we can watch for any changes on them
            if isinstance(value, (list, tuple)) and not isinstance( value, BaseList ):
                value = BaseList( value, instance, self.name )
                instance._data[ self.name ] = value

            # If we have raw values, obtain documents; either from cache, or by dereferencing
            if self._auto_dereference and instance._initialised and isinstance( value, BaseList ) and not value._dereferenced:
                # If we can find all objects in the cache, use it. Otherwise, retrieve all of them.
                if hasattr( instance, '_cache' ) and all( instance._cache[ doc ] for doc in value ):
                    for index, doc in enumerate( value ):
                        super( BaseList, value ).__setitem__( index, instance._cache[ doc ] )
                else:
                    value = _dereference(
                        value, max_depth=1, instance=instance, name=self.name
                    )
                    value._dereferenced = True

                    # For the list of retrieved documents, replace already known entries with cached documents.
                    # Add others to the cache.
                    if hasattr( instance, '_cache' ):
                        for index, doc in enumerate( value ):
                            if doc in instance._cache:
                                doc = instance._cache[ doc ]
                                # Be careful not to trigger `BaseList` append/remove again,
                                # since this'll get us an infinite loop
                                super( BaseList, value ).__setitem__( index, doc )
                            else:
                                instance._cache.add( doc )

                instance._data[self.name] = value
        else:
            # If we're not dealing with documents/references, just call the super
            value = super( ListField, self ).__get__( instance, owner )

        return value


class RelationManagerMixin( object ):
    """ 
    Manages the 'other side' of relations upon changing (saving) a
    :class:`~mongoengine.Document`'s fields that define :attr:`related_name`.

    .. Example:
        class Organization( Document ):
            owner = ReferenceField( 'Person', related_name='organizations', required=True )
            
        class Person( Document ):
            organizations = ListField( ReferenceField( 'Organization' ), related_name='owner' )

    Updates an owner's `organizations` field when an organization's owner changes:
        * remove the organization from any previous owner's `organizations` field
        * add the organization to any new owner's `organizations` field

    Will update all corresponding organization's `owner` fields when the
    `organizations` field of an owner changes:

        * for every added organization, ensure its `owner` points back to us.
          If it doesn't: remove the organization from any previous owner's
          `organizations`

        * for every removed organization, nullify its `owner` if `required` is
          False, or don't remove the organization from this owner's
          `organizations` if an `owner` is required.

    Raises `RelationalError` if both ends of the relation are not of the
    right type or not pointing to each other.

    Makes sure appropriate `delete_rules` are in place, registering them when
    they can be derived.

    Of course this doesn't guarantee any hard consistency due to possible bugs
    in application code or exceptions down the line. 
    
    (Potential) todo: `rebuild` functionality that can repair, or at least
    report, any differences between managed fields.
    """
    def __init__( self, *args, **kwargs ):
        super( RelationManagerMixin, self ).__init__( *args, **kwargs )

        self._supplement_delete_rules()

        self._initialised = False
        self._init_memo()

        if 'request' in kwargs:
            self._set_request( kwargs[ 'request' ] )
        else:
            self._cache = DocumentCache()
            # If initial relations were set, add these to related models
            self.update_relations()

        self._initialised = True

        if self.pk:
            # Sync the memos with the current Document state
            self._memoize_fields()

    def __setattr__( self, key, value ):
        '''
        Overridden to track changes on simple `ReferenceField`s.
        '''
        if self._initialised and key[ 0 ] != '_':
            if key in self._memo_hasone:
                # Duplicate a part of Mongoengine's `base/fields.py`.
                # After https://github.com/MongoEngine/mongoengine/commit/51e50bf0a9b4a6580dd78909b54887e1caeaa179,
                # HasOne fields will not get marked as changed anymore since `update_hasone` already updates the `_data`
                # contents itself.
                # This part mimics https://github.com/MongoEngine/mongoengine/commit/85b81fb12a3e6fd4a1129602c433ce381d45e925
                if self._initialised:
                    try:
                        if ( key not in self._data or self._data[ key ] != value ):
                            self._mark_as_changed( key )
                    except:
                        # Values can't be compared eg: naive and tz datetimes
                        # So mark it as changed
                        self._mark_as_changed( key )

                value = self._cache.get( value, value )
                self.update_hasone( key, value )

            elif key in self._memo_hasmany:
                value = [ self._cache.get( item, item ) for item in value ]
                self.update_hasmany( key, value, self[ key ] )

        return super( RelationManagerMixin, self ).__setattr__( key, value )

    def _init_memo( self ):
        '''
        Memoize reference fields to monitor changes.
        '''
        self._memo_hasone = {}
        self._memo_hasmany = {}
        self._memo_simple = {}

        for name, field in self._fields.iteritems():
            if isinstance( field, ReferenceField ) or isinstance( field, GenericReferenceField ):
                self._memo_hasone[ name ] = None
                related_doc_type = getattr( field, 'document_type', None )
            elif isinstance( field, ListField ):
                # Only memoize the ListField if it contains ReferenceFields.
                if isinstance( field.field, ReferenceField ) or isinstance( field.field, GenericReferenceField ):
                    self._memo_hasmany[ name ] = set()
                    related_doc_type = getattr( field.field, 'document_type', None )
                else:
                    related_doc_type = None
            else:
                default = field.default
                if callable( default ):
                    default = default()
                self._memo_simple[ name ] = default
                related_doc_type = None

            # If 'field' is relational and has a 'related_name', check whether the field
            # we refer to exists on the other document and points back to this Document.
            # Raise an informative Exception if it doesn't exist or point back.
            # NOTE: This only works on normal `ReferenceField`s; 
            # `GenericReferenceField`s go unchecked since any type of document 
            # could potentially end up in one.
            if related_doc_type and hasattr( field, 'related_name' ):
                if field.related_name not in related_doc_type._fields:
                    raise RelationalError("You should add a field `{}` with `related_name='{}'` to the `{}` Document.".format(field.related_name, name, related_doc_type._class_name ) )

                related_field = related_doc_type._fields[ field.related_name ]
                if not hasattr( related_field, 'related_name' ):
                    raise RelationalError( "You should add `related_name={}` to the definition of `{}` on the `{}` Document".format( name, related_field.name, related_doc_type._class_name ) )
                elif related_field.related_name != name:
                    raise RelationalError( "The field `{}` of `{}` has `related_name='{}'`; should this be `related_name='{}'`?".format( related_field.name, related_doc_type._class_name, related_field.related_name, name ) )

                    # print( '  {0} <-> {1}.{2}'.format( name, related_doc_type._class_name, related_field.name ) )

    def _supplement_delete_rules( self ):
        '''
        Every field with a `related_name` should have a `delete_rule`
        registered (other than `DO_NOTHING`) so we can keep relational
        integrity on delete. If this isn't the case, register an appropriate
        one.
        '''
        for field_name, field in self._fields.items():
            related_name = getattr( field, 'related_name', None )
            if not related_name:
                # Skip this field since it is not managed by us.
                continue

            if isinstance( field, ListField ) and hasattr( field, 'field' ):
                field = field.field

            related_doc_type = getattr( field, 'document_type', None )
            related_field = getattr( related_doc_type, related_name, None )
            if related_field:
                if isinstance( related_field, ListField ):
                    new_rule = PULL
                elif not related_field.required:
                    new_rule = NULLIFY
                else:
                    new_rule = DENY

                try:
                    delete_rule = self._meta['delete_rules'].get( ( related_doc_type, related_name ), DO_NOTHING )
                except AttributeError as e:
                    delete_rule = DO_NOTHING

                if delete_rule == DO_NOTHING:
                    self.register_delete_rule( related_doc_type, related_name, new_rule )
                    # print(' ~~ REGISTERING delete rule `{0}` on `{3}.{4}` for relation `{1}.{2}`.'.format(
                    #     'PULL' if new_rule == 4 else 'DENY' if new_rule == 3 else 'NULLIFY', self._class_name, field_name, related_doc_type and related_doc_type._class_name, related_name).encode("utf-8") )

    def _memoize_fields( self, updated_fields=None ):
        '''
        Creates a copy of the items in our fields so we can compare changes.

        @param updated_fields: limit the fields that are memoized to the given fields.
            If not specified, all fields are memoized.
        @type updated_fields: list<string> or set<string>
        @return:
        '''
        if not self.pk:
            return False

        if updated_fields is None:
            updated_fields = set()

        for name in self._memo_hasone.keys():
            # Remember a single reference
            if not updated_fields or name in updated_fields:
                related_doc = self._data[ name ]

                # A `GenericReferenceField` is stored as a dict containing a DBRef as `_ref`,
                # and the Document class as `_cls`.
                if isinstance( related_doc, dict ) and '_ref' in related_doc:
                    related_doc = related_doc[ '_ref' ]

                self._cache.add( related_doc )
                self._memo_hasone[ name ] = related_doc

        for name in self._memo_hasmany.keys():
            # Remember a set of references
            if not updated_fields or name in updated_fields:
                related_docs = set()

                for related_doc in set( self._data[ name ] ):
                    if isinstance( related_doc, dict ) and '_ref' in related_doc:
                        related_docs.add( related_doc[ '_ref' ] )
                    else:
                        related_docs.add( related_doc )

                self._cache.add( related_docs )
                self._memo_hasmany[ name ] = related_docs

        for name in self._memo_simple.keys():
            if not updated_fields or name in updated_fields:
                self._memo_simple[ name ] = copy.copy( self._data[ name ] )

    def save( self, request=None, force_insert=False, validate=True, clean=True, write_concern=None,
              cascade=None, cascade_kwargs=None, _refs=None, **kwargs ):
        '''
        Override `save`. If a document is being saved for the first time,
        it will be given an id (if the save was successful).
        '''
        request = request or ( kwargs and '_request' in kwargs and kwargs[ '_request' ] ) or self._request or None
        self._set_request( request )

        is_new = self.pk is None

        # Trigger `pre_save` hook if it's defined on this Document
        if hasattr( self, 'pre_save' ) and callable( self.pre_save ):
            self.pre_save( request )

        # Trigger `on_change*` callbacks for changed relations, so we can set new privileges
        if not is_new:
            # Remember changed fields for `post_save` before they get reset by `_on_change`.
            changed_fields = self.get_changed_fields()
            self._on_change( request, changed_fields=changed_fields )

        result = super( RelationManagerMixin, self ).save( force_insert=force_insert, validate=validate, clean=clean,
            write_concern=write_concern, cascade=cascade, cascade_kwargs=cascade_kwargs, _refs=_refs, kwargs=kwargs )

        # Update relations after saving if it's a new Document; it should have an id now
        if is_new:
            # Add this doc to the cache, now that it has an id
            request.cache.add( self )

            self.update_relations()

            # Trigger `on_change_pk` if it's present. `pk` is a special case since it isn't a relation,
            # so it won't be triggered through `_on_change`.
            if hasattr( self, 'on_change_pk' ) and callable( self.on_change_pk ):
                self.on_change_pk( request, self.pk, None, updated_fields=self._meta[ 'id_field' ] )

            # Remember changed fields for `post_save` before they get reset by `_on_change`.
            changed_fields = self.get_changed_fields()
            self._on_change( request, changed_fields=changed_fields )

        # Trigger `post_save` hook if it's defined on this Document
        if hasattr( self, 'post_save' ) and callable( self.post_save ):
            self.post_save( request, changed_fields )

        return result

    def reload( self, max_depth=1 ):
        '''
        Override `reload`, to perform an `update_relations` after new data has been fetched.
        '''
        result = super( RelationManagerMixin, self ).reload( max_depth=max_depth )

        # When doing an explicit reload, the relations as fetched from the database should be considered leading.
        self.update_relations() # TODO: add rebuild=True functionality?

        return result

    def delete( self, request, **write_concern ):
        '''
        Override `delete` to clear existing relations before performing the actual delete, to prevent
        lingering references to this document when it's gone.
        @param safe:
        @return:
        '''
        self._set_request( request )

        # Trigger `pre_delete` hook if it's defined on this Document
        if hasattr( self, 'pre_delete' ) and callable( self.pre_delete ):
            self.pre_delete( request )

        self.clear_relations()

        result = super( RelationManagerMixin, self ).delete( write_concern=write_concern )

        # Trigger `post_delete` hook if it's defined on this Document
        if hasattr( self, 'post_delete' ) and callable( self.post_delete ):
            self.post_delete( request )

        return result

    def update( self, request, *args, **kwargs ):
        '''
        Update the Document.

        @param request:
        @type request: pyramid.request.Request
        @param args: (a tuple of) field names that should be updated
        @return:
        '''
        self._set_request( request )

        # Trigger `pre_update` hook if it's defined on this Document
        if hasattr( self, 'pre_update' ) and callable( self.pre_update ):
            self.pre_update( request )

        # Add each `field_name` from args to kwargs, so it will be passed to the `update` call
        for field_name in args:
            kwargs[ 'set__{}'.format( field_name ) ] = self[ field_name ]

        result = super( RelationManagerMixin, self ).update( **kwargs )

        if args:
            self._on_change( request, changed_fields=args, updated_fields=args )

        # Trigger `post_update` hook if it's defined on this Document
        if hasattr( self, 'post_update' ) and callable( self.post_update ):
            self.post_update( request )

        return result

    def clear_relations( self ):
        '''
        Clear relations from this document (both hasOne and hasMany)
        '''
        for field_name, previous_related_doc in self._memo_hasone.iteritems():
            self.update_hasone( field_name, None )

        for field_name, previous_related_docs in self._memo_hasmany.iteritems():
            current_related_docs = set( self[ field_name ] )
            for related_doc in current_related_docs:
                self.remove_hasmany( field_name, related_doc )

    def _on_change( self, request, changed_fields=None, updated_fields=None ):
        '''
        Handle Document changes. Triggers `on_change*` callbacks to handle changes on specific relations.

        @param changed_fields: The set of `changed_fields` to process.
            If not set, will be determined by calling `get_changed_fields`.
        @param updated_fields: Limit the fields that are processed (handlers triggerd, and memoized) to
            the given field names. If not specified, all fields are processed.
        @type updated_fields: list<string> or set<string>
        '''
        fields = set( changed_fields ) if changed_fields is not None else self.get_changed_fields()
        updated_fields = set( updated_fields ) if updated_fields is not None else set()

        # The main `on_change` function should always be called, regardless of `updated_fields`!
        if hasattr( self, 'on_change' ) and callable( self.on_change ):
            self.on_change( request=request, changed_fields=fields, updated_fields=updated_fields )

        for name in fields:
            # Proceed if `field_name` is unset, or we've arrived at the correct `field_name`.
            if updated_fields and name not in updated_fields:
                continue

            # Determine which callback to use. If a callback exists, invoke it if `field_name`
            # is not set, or `field_name` matches `field`
            method = getattr( self, 'on_change_{}'.format( name ), None )

            if callable( method ):
                added_docs, removed_docs = self.get_changes_for_field( name )

                if name in self._memo_hasone or name in self._memo_hasmany or name in self._memo_simple:
                    method( request, added_docs, removed_docs, updated_fields=updated_fields )

        # Sync the memos with the current Document state
        self._memoize_fields( updated_fields )

    def get_changed_fields( self ):
        ''' 
        Get a set listing the names of fields on this document that have been
        modified since the last call to `_memoize_fields` (which is
        called from `_on_change`, which is called from `save`).
        '''
        changed_fields = set()

        # For hasone, simply compare the values.
        for field_name, previous_related_doc in self._memo_hasone.iteritems():
            related_doc = self._data[ field_name ]

            if nequals( related_doc, previous_related_doc ):
                changed_fields.add( field_name )

        # For hasmany, check if different values exist in the old set compared
        # to the new set (using symmetric_difference).
        for field_name, previous_related_docs in self._memo_hasmany.iteritems():
            current_related_docs = set( self._data[ field_name ] )

            if len( set_difference( previous_related_docs, current_related_docs ) ) > 0 or \
                    len( set_difference( current_related_docs, previous_related_docs ) ) > 0:
                changed_fields.add( field_name )

        for field_name, previous_value in self._memo_simple.iteritems():
            if previous_value != self._data[ field_name ]:
                changed_fields.add( field_name )

        return changed_fields

    def get_changes_for_field( self, field_name ):
        '''
        Get the changeset (added and removed Documents) for a single relation.

        @param field_name:
        @return: a tuple containing two values. The first contains a set of added/new Documents;
            the second a set of removed/old Documents.
        @rtype: tuple
        '''
        # Make sure we get actual, (dereferenced) document(s)
        curr_value = self[ field_name ]
        added_docs = set()
        removed_docs = set()

        if field_name in self._memo_simple:
            prev_value = self._memo_simple[ field_name ]
            return curr_value, prev_value

        elif field_name in self._memo_hasone:
            prev_value = self._memo_hasone[ field_name ]
            if prev_value and not equals( prev_value, curr_value ):
                removed_docs.add( prev_value )
            if curr_value and not equals( prev_value, curr_value ):
                added_docs.add( curr_value )

        elif field_name in self._memo_hasmany:
            prev_value = self._memo_hasmany[ field_name ]
            added_docs = set_difference( curr_value, prev_value )
            removed_docs = set_difference( prev_value, curr_value )

        else:
            raise RelationalError( "Can't find _memo entry for field_name={}".format( field_name ) )

        # Try to replace any stray DBFRefs with Documents
        for doc_or_ref in list( added_docs ):
            if isinstance( doc_or_ref, DBRef ):
                try:
                    doc = self._cache[ doc_or_ref ]
                    added_docs.remove( doc_or_ref )
                    added_docs.add( doc )
                except IndexError as e:
                    raise ValidationError( 'Cannot find Document for DBRef={}'.format( doc_or_ref ) )

        for doc_or_ref in list( removed_docs ):
            if isinstance( doc_or_ref, DBRef ):
                try:
                    doc = self._cache[ doc_or_ref ]
                    removed_docs.remove( doc_or_ref )
                    removed_docs.add( doc )
                except IndexError as e:
                    raise ValidationError( 'Cannot find Document for DBRef={}'.format( doc_or_ref ) )

        if field_name in self._memo_hasone:
            return added_docs.pop() if added_docs else None, removed_docs.pop() if removed_docs else None
        else:
            return added_docs, removed_docs

    def get_related_documents_to_update( self ):
        '''
        Determines which related documents should be saved or deleted
        due to changes in their relations to us.
        '''
        to_save = set()
        to_delete = set()
        removed_relations = {}

        changed_fields = self.get_changed_fields()
        for name in changed_fields:
            if hasattr( self._fields[ name ], 'related_name' ):
                # This field is `managed`. Find out the changes.
                added, removed = self.get_changes_for_field( name )
                removed_relations[ name ] = removed if isinstance( removed, set ) else { removed }
                to_save.update( added if isinstance( added, set ) else { added } )

        # What should happen to removed relations depends on the delete rule 
        # they registered with us, which in MongoEngine currently is one of:
        #
        #  * DO_NOTHING  - don't do anything (default).
        #  * NULLIFY     - Updates the reference to null.
        #  * CASCADE     - Deletes the documents associated with the reference.
        #  * DENY        - Prevent the deletion of the reference object.
        #  * PULL        - Pull the reference from a :class:`~mongoengine.ListField` of references
        #
        # For us this means:
        #  - DO_NOTHING's raise an error, since this shouldn't happen for 
        #    managed relations
        #  - NULLIFY'd and PULL'ed relations are added to `to_save`
        #  - CASCADE'd relations are added to `to_delete`
        #  - DENY'd relations raise a ValidationError
        for relation, removed_set in removed_relations.items():
            field = self._fields[ relation ]
            related_name = getattr( field, 'related_name', '' )
            if not related_name:
                # Skip this field; it's not managed by us.
                continue

            if isinstance( field, ListField ):
                field = field.field

            related_doc_type = getattr( field, 'document_type', None )
            if related_doc_type:
                delete_rule = self._meta['delete_rules'].get( (related_doc_type, related_name), DO_NOTHING )

                if delete_rule == DO_NOTHING:
                    raise ValidationError( "Field `{0}` on {1} has no delete rule.".format(related_name, self))
                if delete_rule == DENY:
                    # This object will be updated by other rules.
                    pass
                elif delete_rule == CASCADE:
                    to_delete.update( removed_set )
                elif delete_rule in ( NULLIFY, PULL ):
                    to_save.update( removed_set )

        return to_save, to_delete

    def update_relations( self ):
        '''
        Updates the 'other side' of our managed related fields explicitly, based on the difference between
        the related document(s) stored in the `_memo`s and the current situation.
        '''
        # Do not reciprocate relations when this Document doesn't have an id yet, as this
        # will cause related documents to fail validation and become unsaveable.
        if not self.pk:
            return False

        # Iterate over our `hasone` fields. Since related data can still be
        # DBRefs, access fields by `_data` to avoid getting caught up in an
        # endless `dereferencing` loop.
        for field_name in self._memo_hasone.keys():
            related_doc = self._cache[ self._data[ field_name ] ]
            # print( 'updating `{0}.{1}`, related_doc=`{2}`'.format( self, field_name, related_doc ) )
            if isinstance( related_doc, RelationManagerMixin ):
                self.update_hasone( field_name, related_doc )

        # Iterate over our `hasmany` fields
        for field_name in self._memo_hasmany:
            self.update_hasmany( field_name, self._data[ field_name ] )

        return True

    def update_hasone( self, field_name, new_value ):
        '''
        Update a `hasOne` relation, both on the side of this document, and the opposite related document.

        @param field_name:
        @param new_value:
        @return:
        '''
        if field_name in self._memo_hasone:
            field = self._fields[ field_name ]
            related_doc = self[ field_name ]
            self._cache.add( [ related_doc, new_value ] )

            if hasattr( field, 'related_name' ):
                # Remove old value
                if related_doc and isinstance( related_doc, RelationManagerMixin ):
                    related_data = getattr( related_doc, field.related_name )

                    if isinstance( related_data, ( list, tuple ) ):
                        if self in related_data:
                            related_data.remove( self )
                            # print( 'Removed `{0}` from `{1}` of {2} `{3}`'.format( self, field.related_name, related_doc._class_name, related_doc ).encode("utf-8") )
                    elif related_data == self:
                        related_doc._data[ field.related_name ] = None
                        # print( 'Cleared `{0}` of {1}'.format( field.related_name, related_doc ).encode("utf-8") )

                # Set new value
                related_doc = new_value

                if related_doc and isinstance( related_doc, RelationManagerMixin ):
                    related_data = getattr( related_doc, field.related_name )

                    if isinstance( related_data, ( list, tuple ) ):
                        if self not in related_data:
                            related_data.append( self )
                            # print( 'Appended `{0}` to `{1}` of {2} `{3}`'.format( self, field.related_name, related_doc._class_name, related_doc ).encode("utf-8") )
                    elif related_data != self:
                        related_doc._data[ field.related_name ] = self
                        # print( 'Set `{0}` of `{1}` to `{2}`'.format( field.related_name, related_doc, self ).encode("utf-8") )

            self._data[ field_name ] = new_value

    def update_hasmany( self, field_name, current_related_docs, previous_related_docs=None ):
        '''

        @param field_name:
        @param current_related_docs:
        @param previous_related_docs:
        @return:
        '''
        if field_name in self._memo_hasmany:
            field = self._fields[ field_name ]

            if previous_related_docs is None:
                previous_related_docs = set( self._memo_hasmany[ field_name ] )
            else:
                previous_related_docs = set( previous_related_docs )

            # Only compare actual documents; DBRefs get ignored here
            previous_related_docs = filter( None, [ self._cache[ doc ] for doc in previous_related_docs ] )
            current_related_docs = filter( None, [ self._cache[ doc ] for doc in set( current_related_docs ) ] )

            # Only process fields that have a related_name set.
            if hasattr( field, 'related_name' ):
                added_docs = set_difference( current_related_docs, previous_related_docs )
                removed_docs = set_difference( previous_related_docs, current_related_docs )

                # print( 'update_hasmany on `{}`: current_related_docs=`{}`, previous_related_docs=`{}`, added_docs=`{}`, removed_docs=`{}'.format( self, current_related_docs, previous_related_docs, added_docs, removed_docs ) )

                for related_doc in removed_docs:
                    if isinstance( related_doc, RelationManagerMixin ):
                        related_doc.update_hasone( field.related_name, None )

                for related_doc in added_docs:
                    if isinstance( related_doc, RelationManagerMixin ):
                        related_doc.update_hasone( field.related_name, self )

    def add_hasmany( self, field_name, value ):
        '''

        @param field_name:
        @param value:
        @return:
        '''
        if not isinstance( value, Document ):
            return

        self._cache.add( value )

        if field_name in self._memo_hasmany:
            field = self._fields[ field_name ]

            if hasattr( field, 'related_name' ):
                # the other side of the relation is always a 'hasone'
                value.update_hasone( field.related_name, self )

    def remove_hasmany( self, field_name, value ):
        '''

        @param field_name:
        @param value:
        @return:
        '''
        if not isinstance( value, Document ):
            return

        self._cache.add( value )

        if field_name in self._memo_hasmany:
            field = self._fields[ field_name ]

            if hasattr( field, 'related_name' ):
                # the other side of the relation is always a 'hasone'
                value.update_hasone( field.related_name, None )

    def _find( self, doc_or_ref ):
        '''
        Attempt to find a document (or dbref) in either the local memo, or the global request cache

        @param doc_or_ref:
        @type doc_or_ref: Document or DBRef
        @return:
        '''
        pass

    def _fetch( self, field_name ):
        '''
        Attempt to retrieve documents for a relation from cache.

        @param field_name:
        @type field_name: string
        @param
        '''
        data = self._data[ field_name ]
        field = self._fields[ field_name ]
        result = None

        # A `GenericReferenceField` is stored as a dict containing a DBRef as `_ref`, and the Document class as `_cls`.
        if isinstance( data, dict ) and '_ref' in data:
            data = data[ '_ref' ]

        if isinstance( data, DBRef ):
            result = self._cache[ data ]
            if isinstance( result, Document ):
                self._data[ field_name ] = result
        elif isinstance( data, list ) and hasattr( field, 'field' ):
            # Only fetch documents from our document cache if all data items can be found
            if all( self._cache[ obj ] for obj in data ):
                result = [ self._cache[ obj ] for obj in data ]
                self._data[ field_name ] = result

        return result

    def _set_request( self, request, update_relations=True ):
        if not isinstance( request, Request ):
            raise ValueError( 'request={} should be an instance of `pyramid.request.Request`'.format( request ) )
        elif not hasattr( self, '_request' ):
            self._request = request

            request.cache.add( self )

            if hasattr( self, '_cache' ):
                request.cache.add( self._cache._documents.values() )
                self._cache._documents.clear()

            self._cache = request.cache

            if update_relations:
                self.update_relations()


def set_difference( first_set, second_set ):
    '''
    Determine the difference between two sets containing a (possible) mixture of Documents and DBRefs.
    The `second_set` is subtracted from the `first_set`.
    @param first_set:
    @type first_set: set
    @param second_set:
    @return:
    '''
    second_set_ids = set()
    for doc_or_ref in second_set:
        if doc_or_ref:
            if isinstance( doc_or_ref, Document ):
                second_set_ids.add(doc_or_ref.pk)
            elif isinstance( doc_or_ref, DBRef ):
                second_set_ids.add(doc_or_ref.id)

    diff = set()
    for doc_or_ref in first_set:
        if doc_or_ref:
            match = False
            if isinstance(doc_or_ref, Document):
                if doc_or_ref.pk in second_set_ids:
                    match = True
            elif isinstance( doc_or_ref, DBRef ):
                if doc_or_ref.id in second_set_ids:
                    match = True

            if not match:
                diff.add( doc_or_ref )

    return diff


def equals( doc_or_ref1, doc_or_ref2=False ):
    '''
    Determine if two Documents (or DBRefs representing documents) are equal.
    A DBRef pointing to a Document is considered to be equal to that Document.
    '''

    # If either one is an ObjectId or DBRef, compare ids.
    # (if the other object doesn't have a pk yet, they can't be equal).
    if doc_or_ref1 and doc_or_ref2:
        # A `GenericReferenceField` is stored as a dict containing a DBRef as `_ref`,
        # and the Document class as `_cls`.
        if isinstance( doc_or_ref1, dict ) and '_ref' in doc_or_ref1:
            doc_or_ref1 = doc_or_ref1[ '_ref' ]

        if isinstance( doc_or_ref2, dict ) and '_ref' in doc_or_ref2:
            doc_or_ref2 = doc_or_ref2[ '_ref' ]

        if isinstance( doc_or_ref1, (ObjectId, DBRef) ) or isinstance( doc_or_ref2, (ObjectId, DBRef) ):
            doc_or_ref1 = doc_or_ref1 if isinstance( doc_or_ref1, ObjectId ) else doc_or_ref1.id if isinstance( doc_or_ref1, DBRef ) else doc_or_ref1.pk
            doc_or_ref2 = doc_or_ref2 if isinstance( doc_or_ref2, ObjectId ) else doc_or_ref2.id if isinstance( doc_or_ref2, DBRef ) else doc_or_ref2.pk

    return doc_or_ref1 == doc_or_ref2


def nequals( doc_or_ref1, doc_or_ref2=None ):
    return not equals( doc_or_ref1, doc_or_ref2 )
