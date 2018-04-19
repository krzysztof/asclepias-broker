# -*- coding: utf-8 -*-
#
# Copyright (C) 2018 CERN.
#
# Asclepias Broker is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
"""Elasticsearch indexing module."""

from collections import defaultdict
from copy import deepcopy
from itertools import chain
from typing import Iterable
from uuid import UUID
from invenio_db import db
from sqlalchemy.orm import aliased

from invenio_search import current_search_client

from elasticsearch_dsl import Search, Q
import sqlalchemy as sa

from .mappings.dsl import DB_RELATION_TO_ES, ObjectDoc, ObjectRelationshipsDoc
from .models import Group, GroupRelationship, GroupType, Identifier, Relation, GroupRelationshipM2M, GroupM2M

def build_group_metadata(group: Group) -> dict:
    """Build the metadata for a group object."""
    if group.type == GroupType.Version:
        # Identifiers of the first identity group from all versions
        id_group = group.groups[0]
        ids = id_group.identifiers
        doc = deepcopy((id_group.data and id_group.data.json) or {})
    else:
        doc = deepcopy((group.data and group.data.json) or {})
        ids = group.identifiers

    doc['Identifier'] = [{'ID': i.value, 'IDScheme': i.scheme} for i in ids]
    # TODO: generate IDURL using idutils
    doc['ID'] = str(group.id)
    return doc


def build_relationship_metadata(rel: GroupRelationship) -> dict:
    """Build the metadata for a relationship."""
    if rel.type == GroupType.Version:
        # History of the first group relationship from all versions
        # TODO: Maybe concatenate all histories?
        id_rel = rel.relationships[0]
        return deepcopy((id_rel.data and id_rel.data.json) or {})
    else:
        return deepcopy((rel.data and rel.data.json) or {})


def index_documents(docs):
    """Index a list of documents into ES."""
    for doc in docs:
        current_search_client.index(
            index='relationships', doc_type='doc', body=doc)


def index_identity_group_relationships(ig_id: str, vg_id: str):
    """Build the relationship docs for Identity relations."""
    # Build the documents for incoming Version2Identity relations

    import ipdb; ipdb.set_trace()
    ver_grp_cls = aliased(Group, name='ver_grp_cls')
    id_grp_cls = aliased(Group, name='id_grp_cls')
    relationships = (
        db.session.query(GroupRelationship, ver_grp_cls)
        .join(id_grp_cls, GroupRelationship.source_id == id_grp_cls.id)
        .join(GroupM2M, sa.and_(
            GroupM2M.group_id == ver_grp_cls.id,
            GroupM2M.subgroup_id == id_grp_cls.id
            ))
        .filter(
            ver_grp_cls.type == GroupType.Version,
            GroupRelationship.type == GroupType.Identity,
            GroupRelationship.target_id == ig_id,
        )
    )
    docs = []
    for rel, src_vg in relationships:
        src_meta = build_group_metadata(src_vg)
        trg_meta = build_group_metadata(rel.target)  # TODO: rel.target -> ig_id
        rel_meta = build_relationship_metadata(rel)
        doc = {
            # "ID":  TODO: push Rel ID or RelM2M ID
            "Grouping": "identity",
            "RelationshipType": rel.relation.name,
            "History": rel_meta,
            "Source": src_meta,
            "Target": trg_meta,
        }
        docs.append(doc)

    # Build the documents for outgoing Version2Identity relations
    ver_grprel_cls = aliased(GroupRelationship, name='ver_grprel_cls')
    id_grprel_cls = aliased(GroupRelationship, name='id_grprel_cls')
    relationships = (
        db.session.query(id_grprel_cls, Group)
        .join(ver_grprel_cls, ver_grprel_cls.source_id == vg_id)
        .join(GroupRelationshipM2M, sa.and_(
            GroupRelationshipM2M.relationship_id == ver_grprel_cls.id,
            GroupRelationshipM2M.subrelationship_id == id_grprel_cls.id))
        .join(Group, id_grprel_cls.target_id == Group.id)
        .filter(
            Group.type == GroupType.Identity,
            ver_grprel_cls.type == GroupType.Version,
            id_grprel_cls.type == GroupType.Identity,
        )
    )

    # vg_id_obj = TODO Resolve object
    vg_id_obj = Group.query.get(vg_id)
    for rel, trg_ig in relationships:
        src_meta = build_group_metadata(vg_id_obj)
        trg_meta = build_group_metadata(rel.target)
        rel_meta = build_relationship_metadata(rel)
        doc = {
            # "ID":  TODO: push Rel ID or RelM2M ID
            "Grouping": "identity",
            "RelationshipType": rel.relation.name,
            "History": rel_meta,
            "Source": src_meta,
            "Target": trg_meta,
        }
        docs.append(doc)
    index_documents(docs)


def index_version_group_relationships(group_id: str):
    """Build the relationship docs for Version relations."""
    relationships = GroupRelationship.query.filter(
        GroupRelationship.type == GroupType.Version,
        sa.or_(
            GroupRelationship.source_id == group_id,
            GroupRelationship.target_id == group_id),
    )
    docs = []
    for rel in relationships:
        src_meta = build_group_metadata(rel.source)
        trg_meta = build_group_metadata(rel.target)
        rel_meta = build_relationship_metadata(rel)
        doc = {
            # "ID":  TODO: push Rel ID or RelM2M ID
            "Grouping": "version",
            "RelationshipType": rel.relation.name,
            "History": rel_meta,
            "Source": src_meta,
            "Target": trg_meta,
        }
        docs.append(doc)
    index_documents(docs)


def delete_group_relations(group_id):
    """Delete all relations for given group ID from ES."""
    q = Search(index='relationships').query('term', Source__ID=group_id)
    q.delete()

    q = Search(index='relationships').query('term', Target__ID=group_id)
    q.delete()


def update_indices(src_ig, trg_ig, mrg_ig, src_vg, trg_vg, mrg_vg):
    """Updates Elasticsearch indices with the updated groups."""
    # `src_group` and `trg_group` were merged into `merged_group`.
    for grp_id in [src_ig, trg_ig, src_vg, trg_vg]:
        delete_group_relations(grp_id)

    if mrg_vg:
        index_version_group_relationships(mrg_vg)
    else:
        index_version_group_relationships(src_vg)
        index_version_group_relationships(trg_vg)

    if mrg_ig:
        index_identity_group_relationships(mrg_ig, mrg_vg)
    else:
        import ipdb; ipdb.set_trace()
        index_identity_group_relationships(src_ig, src_vg)
        index_identity_group_relationships(trg_ig, trg_vg)
