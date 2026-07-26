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