
def _group_documents_by_bundle(document_ids, _get_relation_data):
    """Agrupa `document_ids` em grupos
    """
    groups = {}
    for doc_id in document_ids:
        bundle_id, doc = _get_relation_data(doc_id)
        if bundle_id:
            groups[bundle_id] = groups.get(bundle_id) or []
            groups[bundle_id].append(doc_id)
    return groups
