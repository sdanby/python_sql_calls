def build_chat_message_payload(row, *, created_at_formatter):
    display_name = str(row.created_by_display_name or '').strip()
    if display_name:
        created_by = display_name
    else:
        email = str(row.created_by_email or '').strip()
        if email:
            created_by = email
        else:
            athlete_code = str(row.athlete_code or '').strip()
            created_by = athlete_code or 'Unknown'

    return {
        'id': row.id,
        'messageText': row.message_text,
        'createdAt': created_at_formatter(row.created_at),
        'createdBy': created_by,
        'athleteCode': str(row.athlete_code or '').strip() or None,
    }


def get_latest_chat_message_id(*, db, ChatMessage):
    row = db.session.query(db.func.max(ChatMessage.id).label('latest_chat_message_id')).first()
    if not row:
        return None
    latest_chat_message_id = getattr(row, 'latest_chat_message_id', None)
    if latest_chat_message_id is None and isinstance(row, (tuple, list)) and row:
        latest_chat_message_id = row[0]
    return int(latest_chat_message_id) if latest_chat_message_id is not None else None


def mark_chat_read(
    user,
    *,
    db,
    ChatMessage=None,
    latest_chat_message_id=None,
    latest_chat_message_id_factory=None,
):
    if not user:
        return None
    if latest_chat_message_id_factory is None:
        latest_chat_message_id_factory = lambda: get_latest_chat_message_id(db=db, ChatMessage=ChatMessage)

    resolved_latest_chat_message_id = latest_chat_message_id
    if resolved_latest_chat_message_id is None:
        resolved_latest_chat_message_id = latest_chat_message_id_factory()
    if resolved_latest_chat_message_id is None:
        return None

    current_last_read = user.last_read_chat_message_id
    if current_last_read == resolved_latest_chat_message_id:
        return resolved_latest_chat_message_id

    user.last_read_chat_message_id = resolved_latest_chat_message_id
    db.session.commit()
    return resolved_latest_chat_message_id


def build_chat_unread_payload(user, *, latest_chat_message_id=None, latest_chat_message_id_factory):
    resolved_latest_chat_message_id = latest_chat_message_id
    if resolved_latest_chat_message_id is None:
        resolved_latest_chat_message_id = latest_chat_message_id_factory()

    last_read_chat_message_id = int(user.last_read_chat_message_id) if user and user.last_read_chat_message_id is not None else None
    has_unread = bool(
        user
        and resolved_latest_chat_message_id is not None
        and (last_read_chat_message_id is None or last_read_chat_message_id < resolved_latest_chat_message_id)
    )
    return {
        'hasUnread': has_unread,
        'lastReadChatMessageId': last_read_chat_message_id,
        'latestChatMessageId': resolved_latest_chat_message_id,
    }


def build_chat_messages_response(
    user,
    *,
    limit_raw,
    mark_read_raw,
    ChatMessage,
    chat_message_payload_factory,
    mark_chat_read_handler,
):
    try:
        limit = max(1, min(int(limit_raw), 500))
    except Exception:
        limit = 200

    should_mark_read = str(mark_read_raw or '').strip().lower() in {'1', 'true', 'yes', 'y'}
    rows = ChatMessage.query.order_by(ChatMessage.created_at.desc(), ChatMessage.id.desc()).limit(limit).all()
    latest_chat_message_id = rows[0].id if rows else None
    if should_mark_read and latest_chat_message_id is not None:
        mark_chat_read_handler(user, latest_chat_message_id=latest_chat_message_id)

    return [chat_message_payload_factory(row) for row in reversed(rows)], 200


def build_chat_unread_status_response(user, *, chat_unread_payload_factory):
    return chat_unread_payload_factory(user), 200


def build_chat_mark_read_response(user, *, mark_chat_read_handler, chat_unread_payload_factory):
    latest_chat_message_id = mark_chat_read_handler(user)
    return chat_unread_payload_factory(user, latest_chat_message_id), 200


def build_chat_create_message_response(
    payload,
    *,
    ChatMessage,
    db,
    user,
    utcnow,
    chat_message_payload_factory,
    mark_chat_read_handler,
):
    message_text = str(payload.get('messageText') or '').strip()
    if not message_text:
        return {'error': 'messageText is required'}, 400
    if len(message_text) > 2000:
        return {'error': 'messageText is too long'}, 400

    row = ChatMessage(
        created_by_user_id=user.id,
        created_by_display_name=(user.display_name or '').strip() or None,
        created_by_email=user.email,
        athlete_code=(user.athlete_code or '').strip() or None,
        message_text=message_text,
        created_at=utcnow(),
    )
    db.session.add(row)
    db.session.commit()
    mark_chat_read_handler(user, latest_chat_message_id=row.id)
    return chat_message_payload_factory(row), 201