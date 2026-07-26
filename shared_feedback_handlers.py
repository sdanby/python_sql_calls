def build_feedback_list_response(
    *,
    FeedbackRequest,
    feedback_payload_factory,
):
    rows = FeedbackRequest.query.order_by(FeedbackRequest.id.asc()).all()
    return [feedback_payload_factory(row) for row in rows], 200


def build_feedback_create_response(
    payload,
    *,
    FeedbackRequest,
    db,
    user,
    feedback_payload_factory,
    utcnow,
    set_updated_at=False,
):
    request_type_raw = str(payload.get('type') or '').strip().lower()
    title = str(payload.get('title') or '').strip()
    details = str(payload.get('details') or '').strip()

    if request_type_raw not in ('error', 'suggestion'):
        return {'error': 'type must be "error" or "suggestion"'}, 400
    if not title:
        return {'error': 'title is required'}, 400
    if not details:
        return {'error': 'details are required'}, 400

    row_kwargs = {
        'request_type': request_type_raw,
        'title': title,
        'details': details,
        'status': 'logged',
        'created_by_user_id': user.id,
        'created_by_display_name': (user.display_name or '').strip() or None,
        'created_by_email': user.email,
        'created_at': utcnow(),
    }
    if set_updated_at:
        row_kwargs['updated_at'] = utcnow()

    row = FeedbackRequest(**row_kwargs)
    db.session.add(row)
    db.session.commit()
    return feedback_payload_factory(row), 201


def build_feedback_update_response(
    request_id,
    payload,
    *,
    FeedbackRequest,
    db,
    is_admin,
    feedback_payload_factory,
    utcnow,
    status_if_missing=None,
    allowed_statuses=None,
    delete_status=None,
    touch_updated_at=False,
):
    if not is_admin:
        return {'error': 'Forbidden'}, 403

    request_type_raw = str(payload.get('type') or '').strip().lower()
    title = str(payload.get('title') or '').strip()
    details = str(payload.get('details') or '').strip()
    if status_if_missing is None:
        status_raw = str(payload.get('status') or '').strip().lower() if 'status' in payload else None
    else:
        status_raw = str(payload.get('status') or status_if_missing).strip().lower()

    if request_type_raw not in ('error', 'suggestion'):
        return {'error': 'type must be "error" or "suggestion"'}, 400
    if not title:
        return {'error': 'title is required'}, 400
    if not details:
        return {'error': 'details are required'}, 400
    if allowed_statuses is not None and status_raw not in allowed_statuses:
        return {'error': 'status is invalid'}, 400

    row = FeedbackRequest.query.filter_by(id=request_id).first()
    if not row:
        return {'error': 'feedback request not found'}, 404

    if delete_status and status_raw == delete_status:
        db.session.delete(row)
        db.session.commit()
        return {'id': request_id, 'deleted': True}, 200

    row.request_type = request_type_raw
    row.title = title
    row.details = details
    if status_raw:
        row.status = status_raw
    if touch_updated_at:
        row.updated_at = utcnow()
    db.session.commit()

    return feedback_payload_factory(row), 200