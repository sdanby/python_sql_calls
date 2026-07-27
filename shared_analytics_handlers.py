def build_page_visit_response(
    payload,
    *,
    session_token_resolver,
    duration_ms_transformer=None,
    entered_at_resolver,
    left_at_resolver,
    referrer_path_resolver,
    user_agent,
    persist_page_visit,
):
    page_path = str(payload.get('path') or '').strip()
    if not page_path:
        return {'error': 'path is required'}, 400

    duration_ms = payload.get('durationMs')
    try:
        duration_ms = int(duration_ms) if duration_ms is not None else None
    except Exception:
        duration_ms = None

    if duration_ms_transformer is not None:
        duration_ms = duration_ms_transformer(duration_ms)

    persist_page_visit(
        session_token=session_token_resolver(payload),
        page_path=page_path[:512],
        entered_at=entered_at_resolver(payload),
        left_at=left_at_resolver(payload),
        duration_ms=duration_ms,
        referrer_path=referrer_path_resolver(payload),
        user_agent=user_agent,
    )
    return {'ok': True}, 200