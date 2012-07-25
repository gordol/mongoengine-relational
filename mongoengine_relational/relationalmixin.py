from __future__ import print_function
from __future__ import unicode_literals

from mongoengine import Document, GenericReferenceField, ReferenceField, ListField, ValidationError
from mongoengine import base
from mongoengine.queryset import CASCADE, DO_NOTHING, NULLIFY, DENY, PULL
from bson import DBRef, ObjectId


class BaseList( list ):
    '''
    Overridden `BaseList`, so we can track changes made to `toMany` relations.
    '''

    _dereferenced = False
    _instance = None
    _observer = None
    _name = None

    def __init__(self, list_items, instance, name):
        self._instance = instance
        self._name = name
        if hasattr( self._instance, 'add_hasmany' ) and hasattr( self._instance, 'remove_hasmany' ):
            self._observer = self._instance

        super( BaseList, self ).__init__( list_items )


    def __setitem__(self, key, element ):
        self._mark_as_changed()

        try:
            old_element = self.__getitem__( key )
        except KeyError:
            pass
        else:
            # Only remove when there was no KeyError
            if self._observer:
                self._observer.remove_hasmany( self._name, key, old_element )

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

    def pop(self, index=None ):
        self._mark_as_changed()

        result = super(BaseList, self).pop( index ) if index else super(BaseList, self).pop()
        if self._observer:
            self._observer.remove_hasmany( self._name, result )

        return result

    def remove(self, element ):
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

        if self.pk:
            # If initial relations were set, add these to related models
            self.update_relations()

        self._initialised = True

        if self.pk:
            # Sync the memos with the current Document state
            self._memoize_related_fields()


    def __setattr__( self, key, value ):
        '''
        Overridden to track changes on simple `ReferenceField`s.
        '''
        if self._initialised and key[ 0 ] != '_':
            if key in self._memo_hasone:
                self.update_hasone( key, value )
            elif key in self._memo_hasmany:
                self.update_hasmany( key, value, self[ key ] )

        return super( RelationManagerMixin, self ).__setattr__( key, value )


    def _init_memo( self ):
        '''
        Memoize reference fields to monitor changes.
        '''
        if not hasattr( self, '_memo_hasmany' ):
            self._memo_hasmany = {}
        if not hasattr( self, '_memo_hasone' ):
            self._memo_hasone = {}

        # Remember previously related models, so DbRefs returned by functions
        # like `get_changes_for_relation` can  be substituted for the actual
        # (modified) Documents
        self._memo_related_docs = set()

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

                    #print( '  %s <-> %s.%s' % ( name, related_doc_type._class_name, related_field.name ) )


    def _supplement_delete_rules( self ):
        '''
        Every field with a `related_name` should have a `delete_rule` registered (other than `DO_NOTHING`)
        so we can keep relational integrity on delete. If this isn't the case, register an appropriate one.
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

                delete_rule = self._meta['delete_rules'].get( ( related_doc_type, related_name ), DO_NOTHING )
                if delete_rule == DO_NOTHING:
                    self.register_delete_rule( related_doc_type, related_name, new_rule )
                    print(' ~~ REGISTERING delete rule `{0}` on `{3}.{4}` for relation `{1}.{2}`.'.format(
                        'PULL' if new_rule == 4 else 'DENY' if new_rule == 3 else 'NULLIFY', self._class_name, field_name, related_doc_type and related_doc_type._class_name, related_name) )


    def _memoize_related_fields( self ):
        '''
        Creates a copy of the items in our managed fields so we can compare changes.
        '''
        if not self.pk:
            return False

        for field_name in self._memo_hasone.keys():
            # Remember a single reference
            related_doc = self._data[ field_name ]
            self._memoize_documents( related_doc )
            self._memo_hasone[ field_name ] = related_doc

        for field_name in self._memo_hasmany.keys():
            # Remember a set of references
            related_docs = set( self._data[ field_name ] )
            self._memoize_documents( related_docs )
            self._memo_hasmany[ field_name ] = related_docs


    def _memoize_documents( self, docs ):
        '''
        Store the given document(s) in a memo

        @param docs:
        @type docs: Document or list or set or tuple
        '''
        if isinstance( docs, Document ):
            docs = [ docs ]

        if isinstance( docs, ( list, set, tuple ) ):
            documents = { doc for doc in docs if isinstance( doc, Document ) }
            self._memo_related_docs.update( documents )

    def get( self, field_name, request ):
        '''
        Get documents for a relation; retrieves documents from cache if possible.

        @param name:
        @type name: basestring
        @param
        '''
        data = self._data[ field_name ]
        field = self._fields[ field_name ]
        result = None

        if isinstance( data, DBRef ):
            result = request.cache[ data ]
            if isinstance( result, Document ):
                self._data[ field_name ] = result
        elif isinstance( data, list ) and hasattr( field, 'field' ):
            # Only fetch documents from our document cache if all data items can be found
            if all( str(obj.id) in request.cache for obj in data ):
                result = [ request.cache[ str(obj.id) ] for obj in data ]
                self._data[ field_name ] = result

        if not result:
            result = getattr( self, field_name )

            # Add fetched documents to the cache
            request.cache.add( result )

        return result

    def save( self, safe=True, force_insert=False, validate=True, write_options=None, cascade=None, cascade_kwargs=None, _refs=None, request=None ):
        ''' 
        Override `save`. If a document is being saved for the first time,
        it will be given an id (if the save was successful).  
        '''
        request = request or ( cascade_kwargs and cascade_kwargs[ 'request' ] ) or None

        if not request:
            raise ValueError( '`save` needs a `request` parameter (in order to properly invoke `on_change*` callbacks)' )

        # Stuff `request` in `cascade_kwargs`, so `cascade_save` will receive it as a kwarg
        cascade_kwargs = cascade_kwargs or {}
        cascade_kwargs.setdefault( 'request', request )

        is_new = self.pk is None

        # Trigger `on_change*` callbacks for changed relations, so we can set new privileges
        if not is_new:
            self._on_change( request )

        result = super( RelationManagerMixin, self ).save( safe=safe, force_insert=force_insert, validate=validate,
                write_options=write_options, cascade=cascade, cascade_kwargs=cascade_kwargs, _refs=_refs )

        # Update relations after saving if it's a new Document; it should have an id now
        if is_new:
            self.update_relations()

            if hasattr( self, 'on_change_pk' ):
                self.on_change_pk( value=self.pk, old_value=None, request=request, field_name=self._meta[ 'id_field' ] )

            # Trigger `on_change*` callbacks for changed relations, so we can set new privileges
            self._on_change( request )

        return result


    def cascade_save(self, *args, **kwargs):
        '''
        Overridden to propagate `request` for cascade saves.
        '''
        if ( kwargs[ 'request' ] ):
            if 'cascade_kwargs' in kwargs:
                kwargs[ 'cascade_kwargs' ].update( { 'request': kwargs.get( 'request' ) } )
            else:
                kwargs['cascade_kwargs'] = { 'request': kwargs.get( 'request' ) } 

            del kwargs[ 'request' ]

        return super( RelationManagerMixin, self ).cascade_save( *args, **kwargs )


    def reload( self, max_depth=1 ):
        '''
        Override `reload`, to perform an `update_relations` after new data has been fetched.
        '''
        result = super( RelationManagerMixin, self ).reload( max_depth=max_depth )

        # When doing an explicit reload, the relations as fetched from the database should be considered leading.
        self.update_relations( rebuild=True )

        return result


    def delete( self, safe=False ):
        '''
        Over `delete` to clear existing relations before performing the actual delete, to prevent
        lingering references to this document when it's gone.
        @param safe:
        @return:
        '''
        self.clear_relations()

        return super( RelationManagerMixin, self ).delete( safe=safe )


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


    def _on_change( self, request ):
        '''
        Handle Document changes. Triggers `on_change*` callbacks to handle changes on specific relations.
        '''
        changed_relations = self.get_changed_relations()

        if hasattr( self, 'on_change' ):
            self.on_change( changed_relations=changed_relations, request=request )

        for field_name in changed_relations:
            # Determine which callback to use. If a callback exists, invoke it.
            method = getattr( self, 'on_change_{}'.format( field_name ), None )
            if callable( method ):
                added_docs, removed_docs = self.get_changes_for_relation( field_name )

                if field_name in self._memo_hasone:
                    new_value = added_docs.pop() if len( added_docs ) else None
                    old_value = removed_docs.pop() if len( removed_docs ) else None
                    method( value=new_value, old_value=old_value, request=request, field_name=field_name, 
                            added_docs=None, removed_docs=None )
                elif field_name in self._memo_hasmany:
                    method( value=None, old_value=None, request=request, field_name=field_name, 
                            added_docs=added_docs, removed_docs=removed_docs )

        # Sync the memos with the current Document state
        self._memoize_related_fields()


    def get_changed_relations( self ): 
        ''' 
        Get a set listing the names of fields on this document that have been
        modified since the last call to `_memoize_related_fields` (which is
        called from `_on_change`, which is called from `save`).  
        ''' 
        changed_fields = set()

        # For hasone, simply compare the values.
        for field_name, previous_related_doc in self._memo_hasone.iteritems():
            related_doc = self._data[ field_name ]

            if self._nequals( related_doc, previous_related_doc ):
                changed_fields.add( field_name )

        # For hasmany, check if different values exist in the old set compared
        # to the new set (using symmetric_difference).
        for field_name, previous_related_docs in self._memo_hasmany.iteritems():
            current_related_docs = set( self._data[ field_name ] )

            if len( set_difference( previous_related_docs, current_related_docs ) ) > 0 or \
                    len( set_difference( current_related_docs, previous_related_docs ) ) > 0:
                changed_fields.add( field_name )

        return changed_fields


    def get_changes_for_relation( self, field_name ):
        '''
        Get the changeset (added and removed Documents)for a single relation.

        @param field_name:
        @return: a tuple containing two values. The first contains a set of added/new Documents;
            the second a set of removed/old Documents.
        @rtype: tuple
        '''
        # Make sure we get actual, (dereferenced) document(s)
        new_value = self[ field_name ]
        added_docs = set()
        removed_docs = set()

        if field_name in self._memo_hasone:
            old_value = self._memo_hasone[ field_name ]
            if old_value:
                removed_docs.add( old_value )
            if new_value:
                added_docs.add( new_value )

        elif field_name in self._memo_hasmany:
            old_value = self._memo_hasmany[ field_name ]
            added_docs = set_difference( new_value, old_value )
            removed_docs = set_difference( old_value, new_value )

        else:
            raise RelationalError( "Can't find _memo entry for field_name={}".format( field_name ) )

        # Try to replace any stray DBFRefs with Documents
        for doc_or_ref in added_docs:
            if isinstance( doc_or_ref, DBRef ):
                try:
                    doc = [ doc for doc in self._memo_related_docs if doc._equals( doc_or_ref ) ][ 0 ]
                    added_docs.remove( doc_or_ref )
                    added_docs.add( doc )
                except IndexError as e:
                    raise ValidationError( 'Cannot find Document for DBRef={}'.format( doc_or_ref ) )

        for doc_or_ref in removed_docs:
            if isinstance( doc_or_ref, DBRef ):
                try:
                    doc = [ doc for doc in self._memo_related_docs if doc._equals( doc_or_ref ) ][ 0 ]
                    removed_docs.remove( doc_or_ref )
                    removed_docs.add( doc )
                except IndexError as e:
                    raise ValidationError( 'Cannot find Document for DBRef={}'.format( doc_or_ref ) )

        return added_docs, removed_docs


    def get_related_documents_to_update( self ):
        '''
        Determines which related documents should be saved or deleted
        due to changes in their relations to us.
        '''
        to_save = set()
        to_delete = set()
        removed_relations = {}

        changed_relations = self.get_changed_relations()
        for rel in changed_relations:
            # Now find out the changes
            added_relations, removed_relations[ rel ] = self.get_changes_for_relation( rel )
            to_save.update( added_relations )

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
            fld = self._fields[ relation ]
            related_name = getattr( fld, 'related_name', '' )
            if not related_name:
                # Skip this field; it's not managed by us.
                continue

            if isinstance( fld, ListField ):
                fld = fld.field

            delete_rule = self._meta['delete_rules'].get((fld.document_type, related_name), DO_NOTHING)

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


    def update_relations( self, rebuild=False ):
        '''
        Updates the 'other side' of our managed related fields explicitly, based on the difference between
        the related document(s) stored in the `_memo`s and the current situation.
        '''

        # Do not reciprocate relations when we don't have an id yet, as this
        # will cause related documents to fail validation and become
        # unsaveable.
        if not self.pk:
            return False

        # Iterate over our `hasone` fields. Since related data can still be
        # DBRefs, access fields by `_data` to avoid getting caught up in an
        # endless `dereferencing` loop.
        for field_name in self._memo_hasone.keys():
            related_doc = self._data[ field_name ]
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
            self._memoize_documents( related_doc )

            if hasattr( field, 'related_name' ):
                # Remove old value
                # TODO: this was changed from `self._data[ field_name ]` to self[ field_name ];
                # verify this doesn't cause (way) too much queries..


                if related_doc and isinstance( related_doc, RelationManagerMixin ):
                    related_data = getattr( related_doc, field.related_name )

                    if isinstance( related_data, ( list, tuple ) ):
                        if self in related_data:
                            related_doc[ field.related_name ].remove( self )
                            print( 'Removed `%s` from `%s` of %s `%s`' % ( self, field.related_name, related_doc._class_name, related_doc ) )
                    elif related_data == self:
                        related_doc._data[ field.related_name ] = None
                        print( 'Cleared `%s` of %s' % ( field.related_name, related_doc ) )

                # Set new value
                related_doc = new_value

                if related_doc and isinstance( related_doc, RelationManagerMixin ):
                    self._memoize_documents( related_doc )
                    related_data = getattr( related_doc, field.related_name )

                    if isinstance( related_data, ( list, tuple ) ):
                        if self not in related_data:
                            related_doc[ field.related_name ].append( self )
                            print( 'Appended `%s` to `%s` of %s `%s`' % ( self, field.related_name, related_doc._class_name, related_doc ) )
                    elif related_data != self:
                        related_doc._data[ field.related_name ] = self
                        print( 'Set `%s` of `%s` to `%s`' % ( field.related_name, related_doc, self ) )

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

            current_related_docs = set( current_related_docs )

            self._memoize_documents( previous_related_docs )
            self._memoize_documents( current_related_docs )

            # Only process fields that have a related_name set.
            if hasattr( field, 'related_name' ):
                added_docs = set_difference( current_related_docs, previous_related_docs )
                removed_docs = set_difference( previous_related_docs, current_related_docs )

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

        self._memoize_documents( value )

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

        self._memoize_documents( value )

        if field_name in self._memo_hasmany:
            field = self._fields[ field_name ]

            if hasattr( field, 'related_name' ):
                # the other side of the relation is always a 'hasone'
                value.update_hasone( field.related_name, None )


    #
    # Utility comparison functions, to compare a mix of DBRefs and Documents
    #

    def _equals( self, doc_or_ref1, doc_or_ref2=False ):
        # `None` is also valid input, so only replace `doc_or_ref2` with self if it's exactly `False`
        if doc_or_ref2 is False:
            doc_or_ref2 = self

        return equals( doc_or_ref1, doc_or_ref2 )


    def _nequals( self, doc_or_ref1, doc_or_ref2=False ):
        return nequals( doc_or_ref1, doc_or_ref2 )


def set_difference( first_set, second_set ):
    '''
    Determine the difference between two sets containing a (possible) mixture of Documents and DBRefs.
    The `second_set` is subtracted from the `first_set`.
    @param first_set:
    @param second_set:
    @return:
    '''
    diff = set()
    for doc_or_ref in first_set:
        match = False
        for other_doc_or_ref in second_set:
            if equals( doc_or_ref, other_doc_or_ref ):
                match = True
                break

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
        if isinstance( doc_or_ref1, (ObjectId, DBRef) ) or isinstance( doc_or_ref2, (ObjectId, DBRef) ):
            doc_or_ref1 = doc_or_ref1.id if isinstance( doc_or_ref1, DBRef ) else doc_or_ref1 if isinstance( doc_or_ref1, ObjectId ) else doc_or_ref1.pk
            doc_or_ref2 = doc_or_ref2.id if isinstance( doc_or_ref2, DBRef ) else doc_or_ref2 if isinstance( doc_or_ref2, ObjectId ) else doc_or_ref2.pk

    return doc_or_ref1 == doc_or_ref2


def nequals( doc_or_ref1, doc_or_ref2=None ):
    return not equals( doc_or_ref1, doc_or_ref2 )
