import re


EMAIL_PATTERN = re.compile(r'^[^@\s]+@[^@\s]+\.[^@\s]+$')


def build_auth_register_response(
    payload,
    *,
    AuthUser,
    AuthSession,
    db,
    normalize_email,
    resolve_athlete_code,
    resolve_default_course,
    session_token_factory,
    record_login_event,
    user_payload_factory,
    generate_password_hash,
    utcnow,
):
    email = normalize_email(payload.get('email'))
    password = str(payload.get('password') or '')
    display_name = (payload.get('displayName') or '').strip() or None
    athlete_code = resolve_athlete_code(payload.get('athleteCode'))
    default_course_code, default_course_name = resolve_default_course(
        payload.get('defaultCourseCode'),
        payload.get('defaultCourseName')
    )

    if not EMAIL_PATTERN.match(email):
        return {'error': 'Valid email is required.'}, 400
    if len(password) < 8:
        return {'error': 'Password must be at least 8 characters.'}, 400

    existing = AuthUser.query.filter_by(email=email).first()
    if existing:
        return {'error': 'Email already registered.'}, 409

    user = AuthUser(
        email=email,
        password_hash=generate_password_hash(password),
        display_name=display_name,
        athlete_code=athlete_code,
        default_course_code=default_course_code,
        default_course_name=default_course_name,
        last_login_at=utcnow(),
    )
    db.session.add(user)
    db.session.commit()

    token = session_token_factory()
    db.session.add(AuthSession(token=token, user_id=user.id, provider='email'))
    db.session.commit()
    record_login_event(user.id, 'email', True)

    return {'token': token, 'user': user_payload_factory(user)}, 200


def build_auth_login_response(
    payload,
    *,
    AuthUser,
    AuthSession,
    db,
    normalize_email,
    resolve_athlete_code,
    resolve_default_course,
    session_token_factory,
    record_login_event,
    user_payload_factory,
    check_password_hash,
    utcnow,
):
    email = normalize_email(payload.get('email'))
    password = str(payload.get('password') or '')

    user = AuthUser.query.filter_by(email=email).first()
    if not user or not user.password_hash or not check_password_hash(user.password_hash, password):
        record_login_event(user.id if user else None, 'email', False)
        return {'error': 'Invalid email or password.'}, 401

    previous_last_login_at = user.last_login_at
    token = session_token_factory()
    if 'athleteCode' in payload:
        user.athlete_code = resolve_athlete_code(payload.get('athleteCode'))
    if 'defaultCourseCode' in payload or 'defaultCourseName' in payload:
        dc_code, dc_name = resolve_default_course(payload.get('defaultCourseCode'), payload.get('defaultCourseName'))
        user.default_course_code = dc_code
        user.default_course_name = dc_name
    user.last_login_at = utcnow()
    db.session.add(AuthSession(token=token, user_id=user.id, provider='email'))
    db.session.commit()
    record_login_event(user.id, 'email', True)

    payload_user = user_payload_factory(user)
    payload_user['previousLoginAt'] = previous_last_login_at.isoformat() if previous_last_login_at else None
    return {'token': token, 'user': payload_user}, 200


def build_auth_google_response(
    payload,
    *,
    AuthUser,
    AuthSession,
    db,
    google_dependencies_ready,
    verify_google_credential,
    google_client_id,
    normalize_email,
    resolve_athlete_code,
    resolve_default_course,
    session_token_factory,
    record_login_event,
    user_payload_factory,
    utcnow,
):
    if not google_dependencies_ready():
        return {'error': 'Google auth dependencies are not installed on backend.'}, 501

    credential = payload.get('credential') or payload.get('idToken')
    athlete_code = resolve_athlete_code(payload.get('athleteCode'))
    default_course_code, default_course_name = resolve_default_course(
        payload.get('defaultCourseCode'),
        payload.get('defaultCourseName')
    )
    if not credential:
        return {'error': 'Google credential is required.'}, 400

    client_id = google_client_id()
    if not client_id:
        return {'error': 'GOOGLE_CLIENT_ID is not configured on backend.'}, 500

    try:
        claims = verify_google_credential(credential, client_id)
    except Exception as exc:
        if record_login_event is not None:
            record_login_event(None, 'google', False)
        return {'error': f'Invalid Google token: {exc}'}, 401

    google_sub = claims.get('sub')
    email = normalize_email(claims.get('email'))
    display_name = claims.get('name')
    if not google_sub or not email:
        return {'error': 'Google token missing required claims.'}, 400

    user = AuthUser.query.filter_by(google_sub=google_sub).first()
    if not user:
        user = AuthUser.query.filter_by(email=email).first()

    previous_last_login_at = user.last_login_at if user else None

    if not user:
        user = AuthUser(
            email=email,
            google_sub=google_sub,
            display_name=display_name,
            athlete_code=athlete_code,
            default_course_code=default_course_code,
            default_course_name=default_course_name,
        )
        db.session.add(user)
    else:
        if not user.google_sub:
            user.google_sub = google_sub
        if display_name and not user.display_name:
            user.display_name = display_name
        if 'athleteCode' in payload:
            user.athlete_code = athlete_code
        if 'defaultCourseCode' in payload or 'defaultCourseName' in payload:
            user.default_course_code = default_course_code
            user.default_course_name = default_course_name

    user.last_login_at = utcnow()
    db.session.commit()

    token = session_token_factory()
    db.session.add(AuthSession(token=token, user_id=user.id, provider='google'))
    db.session.commit()
    if record_login_event is not None:
        record_login_event(user.id, 'google', True)

    payload_user = user_payload_factory(user)
    payload_user['previousLoginAt'] = previous_last_login_at.isoformat() if previous_last_login_at else None
    return {'token': token, 'user': payload_user}, 200


def build_auth_logout_response(
    payload,
    *,
    AuthSession,
    db,
    extract_bearer_token,
):
    session_token = payload.get('token') or extract_bearer_token()
    if not session_token:
        return {'ok': True}, 200

    sess = AuthSession.query.filter_by(token=session_token, revoked=False).first()
    if sess:
        sess.revoked = True
        db.session.commit()

    return {'ok': True}, 200


def build_auth_me_response(
    *,
    extract_bearer_token,
    resolve_session,
    user_payload_factory,
):
    session_token = extract_bearer_token()
    _sess, user = resolve_session(session_token)
    if not user:
        return {'error': 'Unauthorized'}, 401

    return {'user': user_payload_factory(user)}, 200


def build_auth_link_athlete_response(
    payload,
    *,
    db,
    extract_bearer_token,
    resolve_session,
    normalize_athlete_code,
    resolve_athlete_code,
    resolve_default_course,
    user_payload_factory,
):
    session_token = payload.get('token') or extract_bearer_token()
    _sess, user = resolve_session(session_token)
    if not user:
        return {'error': 'Unauthorized'}, 401

    requested_athlete_code = normalize_athlete_code(payload.get('athleteCode'))
    resolved_athlete_code = resolve_athlete_code(requested_athlete_code)
    requested_default_course_code = payload.get('defaultCourseCode')
    requested_default_course_name = payload.get('defaultCourseName')
    resolved_default_course_code, resolved_default_course_name = resolve_default_course(
        requested_default_course_code,
        requested_default_course_name
    )

    if requested_athlete_code and not resolved_athlete_code:
        user.athlete_code = None
        db.session.commit()
        return {
            'ok': True,
            'user': user_payload_factory(user),
            'message': 'athleteCode not found in athletes; stored as NULL.'
        }, 200

    user.athlete_code = resolved_athlete_code
    if 'defaultCourseCode' in payload or 'defaultCourseName' in payload:
        user.default_course_code = resolved_default_course_code
        user.default_course_name = resolved_default_course_name
    db.session.commit()

    return {'ok': True, 'user': user_payload_factory(user)}, 200