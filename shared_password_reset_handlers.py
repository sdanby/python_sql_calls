import re


EMAIL_PATTERN = re.compile(r'^[^@\s]+@[^@\s]+\.[^@\s]+$')


def build_auth_config_response(*, google_client_id, password_reset_enabled):
    return {
        'googleClientId': google_client_id or '',
        'passwordResetEnabled': bool(password_reset_enabled),
    }, 200


def build_password_reset_request_response(
    payload,
    *,
    AuthUser,
    AuthPasswordResetToken,
    db,
    normalize_email,
    password_reset_email_enabled,
    invalidate_active_password_reset_tokens,
    password_reset_token_factory,
    password_reset_ttl_minutes,
    hash_password_reset_token,
    password_reset_link_factory,
    send_email,
    utcnow,
):
    if not password_reset_email_enabled():
        return {'error': 'Password reset email is not configured.'}, 503

    email = normalize_email(payload.get('email'))
    if not EMAIL_PATTERN.match(email):
        return {'error': 'Valid email is required.'}, 400

    user = AuthUser.query.filter_by(email=email).first()
    if user:
        invalidate_active_password_reset_tokens(user.id)
        plain_token = password_reset_token_factory()
        expires_at = utcnow() + password_reset_ttl_minutes()
        db.session.add(AuthPasswordResetToken(
            user_id=user.id,
            token_hash=hash_password_reset_token(plain_token),
            expires_at=expires_at,
        ))
        db.session.commit()
        reset_link = password_reset_link_factory(plain_token)
        ttl_minutes = int(password_reset_ttl_minutes().total_seconds() // 60)
        send_email(
            'Reset your parkrun project password',
            (
                f"Hello,\n\n"
                f"We received a request to reset your parkrun project password.\n\n"
                f"Open this link to choose a new password:\n{reset_link}\n\n"
                f"This link expires in {ttl_minutes} minutes.\n\n"
                f"If you did not request this, you can ignore this email."
            ),
            user.email,
        )

    return {
        'ok': True,
        'message': 'If that email address is registered, a password reset link has been sent.'
    }, 200


def build_password_reset_validate_response(token, *, get_password_reset_token_row, password_reset_status_payload):
    token_row = get_password_reset_token_row(token)
    return password_reset_status_payload(token_row), 200


def build_password_reset_confirm_response(
    payload,
    *,
    AuthUser,
    AuthSession,
    db,
    get_password_reset_token_row,
    password_reset_status_payload,
    generate_password_hash,
    utcnow,
):
    token = str(payload.get('token') or '').strip()
    password = str(payload.get('password') or '')

    if len(password) < 8:
        return {'error': 'Password must be at least 8 characters.'}, 400

    token_row = get_password_reset_token_row(token)
    status_payload = password_reset_status_payload(token_row)
    if not status_payload['valid']:
        return {'error': 'This password reset link is invalid or has expired.'}, 400

    user = AuthUser.query.filter_by(id=token_row.user_id).first()
    if not user:
        return {'error': 'This password reset link is invalid or has expired.'}, 400

    user.password_hash = generate_password_hash(password)
    token_row.used_at = utcnow()
    AuthSession.query.filter_by(user_id=user.id, revoked=False).update({'revoked': True}, synchronize_session=False)
    db.session.commit()

    return {'ok': True, 'message': 'Your password has been updated. Please sign in.'}, 200