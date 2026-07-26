def build_admin_user_set_admin_response(
    user_id,
    payload,
    *,
    AuthUser,
    db,
    user_payload_factory,
    supports_explicit_admin_flag=True,
    admin_count_factory=None,
    bootstrap_state_factory=None,
):
    if not supports_explicit_admin_flag:
        return {'error': 'Admin role editing is not available in this local schema.'}, 400

    desired_flag = bool(payload.get('isAdmin'))
    target = AuthUser.query.filter_by(id=user_id).first()
    if not target:
        return {'error': 'User not found'}, 404

    if (
        admin_count_factory is not None
        and not desired_flag
        and bool(getattr(target, 'is_admin', False))
        and int(admin_count_factory()) <= 1
    ):
        return {'error': 'At least one admin is required.'}, 400

    target.is_admin = desired_flag
    db.session.commit()

    response_body = {
        'ok': True,
        'user': user_payload_factory(target),
    }
    if bootstrap_state_factory is not None:
        admin_count, bootstrap_open = bootstrap_state_factory()
        response_body['adminCount'] = int(admin_count)
        response_body['bootstrapOpen'] = bool(bootstrap_open)
    return response_body, 200


def build_admin_status_response(
    user,
    *,
    user_payload_factory,
    admin_count,
    bootstrap_open,
    can_access_admin,
):
    if not user:
        return {'error': 'Unauthorized'}, 401

    return {
        'adminCount': int(admin_count),
        'bootstrapOpen': bool(bootstrap_open),
        'canAccessAdmin': bool(can_access_admin),
        'user': user_payload_factory(user),
    }, 200


def build_admin_users_list_response(
    user,
    *,
    AuthUser,
    user_row_payload_factory,
    admin_count,
    bootstrap_open,
    can_access_admin,
):
    if not user:
        return {'error': 'Unauthorized'}, 401
    if not can_access_admin:
        return {'error': 'Forbidden'}, 403

    rows = AuthUser.query.order_by(AuthUser.created_at.desc()).all()
    return {
        'users': [user_row_payload_factory(row) for row in rows],
        'adminCount': int(admin_count),
        'bootstrapOpen': bool(bootstrap_open),
    }, 200


def build_admin_activity_response(
    user,
    *,
    can_access_admin,
    limit_raw,
    since_raw,
    parse_since,
    activity_loader,
    min_limit=1,
    max_limit=5000,
    default_limit=300,
):
    if not user:
        return {'error': 'Unauthorized'}, 401
    if not can_access_admin:
        return {'error': 'Forbidden'}, 403

    try:
        limit = int(limit_raw)
    except Exception:
        limit = int(default_limit)
    limit = max(int(min_limit), min(limit, int(max_limit)))

    activity = activity_loader(limit, parse_since(since_raw))
    return {
        'activity': activity,
        'limit': limit,
    }, 200


def build_admin_user_set_default_course_response(
    user_id,
    payload,
    *,
    AuthUser,
    db,
    user_payload_factory,
    resolve_default_course,
):
    target = AuthUser.query.filter_by(id=user_id).first()
    if not target:
        return {'error': 'User not found'}, 404

    default_course_code, default_course_name = resolve_default_course(
        payload.get('defaultCourseCode'),
        payload.get('defaultCourseName'),
    )
    if not default_course_code and not default_course_name:
        return {'error': 'Course not found. Please check the course code or name.'}, 400

    target.default_course_code = default_course_code
    target.default_course_name = default_course_name
    db.session.commit()

    return {
        'ok': True,
        'user': user_payload_factory(target),
    }, 200


def build_admin_user_set_athlete_code_response(
    user_id,
    payload,
    *,
    AuthUser,
    db,
    user_payload_factory,
    normalize_athlete_code,
    resolve_athlete_code,
):
    target = AuthUser.query.filter_by(id=user_id).first()
    if not target:
        return {'error': 'User not found'}, 404

    requested_athlete_code = normalize_athlete_code(payload.get('athleteCode'))
    resolved_athlete_code = resolve_athlete_code(requested_athlete_code)

    if requested_athlete_code and not resolved_athlete_code:
        return {'error': 'Athlete not found. Please choose a valid athlete from search.'}, 400

    target.athlete_code = resolved_athlete_code
    db.session.commit()

    return {
        'ok': True,
        'user': user_payload_factory(target),
    }, 200