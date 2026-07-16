# Add missing _require_authenticated_user function
def _require_authenticated_user():
    session_token = _extract_bearer_token()
    _sess, user = _resolve_session(session_token)
    return _sess, user

from datetime import datetime, timedelta, timezone
from collections import deque
from contextlib import redirect_stderr, redirect_stdout
import json
import importlib
import os
import re
import socketserver
import threading
import time
import uuid
from flask import Flask, jsonify, request
from flask_cors import CORS
from werkzeug.security import generate_password_hash, check_password_hash
from wsgiref.simple_server import WSGIRequestHandler, WSGIServer, make_server

# NOTE FOR MAINTAINERS:
# This file is used for local/backend workflows, but the Render-hosted production API is driven
# by `app.py` (and the Render-linked copy under `python_sql_calls_repo/`).
# Do not assume changes made only in `backendAPI.py` will affect the live site.
# If an endpoint, SQL payload, auth flow, or any Python/Postgres API response needs to change
# for production, make that change in `app.py` first and keep the Render-linked repo in sync.

app = Flask(__name__)
CORS(app, origins=["http://localhost:3000"])   # production: use exact origins
@app.after_request
def add_cors_headers(response):
    response.headers["Access-Control-Allow-Origin"] = "http://localhost:3000"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS, PUT, DELETE"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    # If you send credentials from client:
    # response.headers["Access-Control-Allow-Credentials"] = "true"
    return response
from database import connections,update_parkrun_events,fetch_coefficients_for_all_events,get_most_recent_date_with_coeff_not_one
from process import process_event_url,process_parkrun_history
from scraper import create_webdriver
from consistency import fetch_data, fetch_max_position, create_table, get_parkrun_data
from sqlalchemy import text, inspect
from dateutil.parser import parse as parse_date
from analytics import fetch_event_data,get_transformed_event_data, optimize_event_times_logic, coeffStartDate as get_coeff_start_date,normalize_coefficients, check_and_update_events, update_eligible_times_for_all_weeks,timeToSeconds
from parkrunAPI import parkrun_api
from lists_api import lists_bp


import logging


# Set Flask logging level to ERROR
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

from flask_sqlalchemy import SQLAlchemy

try:
    id_token = importlib.import_module('google.oauth2.id_token')
    google_requests = importlib.import_module('google.auth.transport.requests')
except Exception:
    id_token = None
    google_requests = None

app = Flask(__name__)
app.register_blueprint(parkrun_api)
app.register_blueprint(lists_bp)
#app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///processing_status.db'
#app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
database_url = os.getenv('DATABASE_URL') or os.getenv('RENDER_POSTGRES_URL')
if database_url and database_url.startswith('postgres://'):
    database_url = database_url.replace('postgres://', 'postgresql://', 1)
app.config['SQLALCHEMY_DATABASE_URI'] = database_url or 'sqlite:///C:/Users/stevi/flask-backend/myapp/parkrun.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

CORS(app)


class AuthUser(db.Model):
    __tablename__ = 'auth_users'
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(255), unique=True, nullable=False, index=True)
    password_hash = db.Column(db.String(255), nullable=True)
    google_sub = db.Column(db.String(255), unique=True, nullable=True, index=True)
    display_name = db.Column(db.String(255), nullable=True)
    athlete_code = db.Column(db.String(32), nullable=True, index=True)
    default_course_code = db.Column(db.String(32), nullable=True, index=True)
    default_course_name = db.Column(db.String(255), nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    last_login_at = db.Column(db.DateTime, nullable=True)


class AuthSession(db.Model):
    __tablename__ = 'auth_sessions'
    token = db.Column(db.String(96), primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('auth_users.id'), nullable=False, index=True)
    provider = db.Column(db.String(32), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    last_seen_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    revoked = db.Column(db.Boolean, default=False, nullable=False)


class AuthLoginEvent(db.Model):
    __tablename__ = 'auth_login_events'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, nullable=True, index=True)
    provider = db.Column(db.String(32), nullable=False)
    success = db.Column(db.Boolean, nullable=False)
    ip_address = db.Column(db.String(64), nullable=True)
    user_agent = db.Column(db.String(512), nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)


class PageUsageEvent(db.Model):
    __tablename__ = 'page_usage_events'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, nullable=True, index=True)
    session_token = db.Column(db.String(96), nullable=True, index=True)
    page_path = db.Column(db.String(512), nullable=False)
    entered_at = db.Column(db.DateTime, nullable=True)
    left_at = db.Column(db.DateTime, nullable=True)
    duration_ms = db.Column(db.Integer, nullable=True)
    referrer_path = db.Column(db.String(512), nullable=True)
    user_agent = db.Column(db.String(512), nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)


class FeedbackRequest(db.Model):
    __tablename__ = 'feedback_requests'
    id = db.Column(db.Integer, primary_key=True)
    request_type = db.Column(db.String(32), nullable=False)
    title = db.Column(db.String(255), nullable=False)
    details = db.Column(db.Text, nullable=False)
    status = db.Column(db.String(32), nullable=False, default='Logged')
    created_by_user_id = db.Column(db.Integer, nullable=True, index=True)
    created_by_display_name = db.Column(db.String(255), nullable=True)
    created_by_email = db.Column(db.String(255), nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class ChatMessage(db.Model):
    __tablename__ = 'chat_messages'
    id = db.Column(db.Integer, primary_key=True)
    created_by_user_id = db.Column(db.Integer, nullable=True, index=True)
    created_by_display_name = db.Column(db.String(255), nullable=True)
    created_by_email = db.Column(db.String(255), nullable=True)
    athlete_code = db.Column(db.String(32), nullable=True)
    message_text = db.Column(db.Text, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False, index=True)


with app.app_context():
    AuthUser.__table__.create(bind=db.engine, checkfirst=True)
    AuthSession.__table__.create(bind=db.engine, checkfirst=True)
    AuthLoginEvent.__table__.create(bind=db.engine, checkfirst=True)
    PageUsageEvent.__table__.create(bind=db.engine, checkfirst=True)
    FeedbackRequest.__table__.create(bind=db.engine, checkfirst=True)
    ChatMessage.__table__.create(bind=db.engine, checkfirst=True)
    inspector = inspect(db.engine)
    auth_user_columns = {column['name'] for column in inspector.get_columns('auth_users')}
    if 'athlete_code' not in auth_user_columns:
        db.session.execute(text("ALTER TABLE auth_users ADD COLUMN athlete_code VARCHAR(32)"))
        db.session.commit()
    if 'default_course_code' not in auth_user_columns:
        db.session.execute(text("ALTER TABLE auth_users ADD COLUMN default_course_code VARCHAR(32)"))
        db.session.commit()
    if 'default_course_name' not in auth_user_columns:
        db.session.execute(text("ALTER TABLE auth_users ADD COLUMN default_course_name VARCHAR(255)"))
        db.session.commit()
    feedback_request_columns = {column['name'] for column in inspector.get_columns('feedback_requests')}
    if 'created_by_user_id' not in feedback_request_columns:
        db.session.execute(text("ALTER TABLE feedback_requests ADD COLUMN created_by_user_id INTEGER"))
        db.session.commit()
    if 'created_by_display_name' not in feedback_request_columns:
        db.session.execute(text("ALTER TABLE feedback_requests ADD COLUMN created_by_display_name VARCHAR(255)"))
        db.session.commit()
    if 'created_by_email' not in feedback_request_columns:
        db.session.execute(text("ALTER TABLE feedback_requests ADD COLUMN created_by_email VARCHAR(255)"))
        db.session.commit()
    if 'updated_at' not in feedback_request_columns:
        db.session.execute(text("ALTER TABLE feedback_requests ADD COLUMN updated_at DATETIME"))
        db.session.execute(text("UPDATE feedback_requests SET updated_at = COALESCE(updated_at, created_at, CURRENT_TIMESTAMP)"))
        db.session.commit()


def _normalize_email(value):
    return (value or '').strip().lower()


def _normalize_athlete_code(value):
    normalized = str(value or '').strip()
    return normalized or None


def _resolve_athlete_code(value):
    athlete_code = _normalize_athlete_code(value)
    if not athlete_code:
        return None
    row = db.session.execute(
        text("""
            SELECT CAST(athlete_code AS TEXT) AS athlete_code
            FROM athletes
            WHERE CAST(athlete_code AS TEXT) = :athlete_code
            LIMIT 1
        """),
        {'athlete_code': athlete_code}
    ).fetchone()
    return athlete_code if row else None


def _session_token():
    return f"{uuid.uuid4().hex}{uuid.uuid4().hex}"


def _resolve_default_course(event_code_value, event_name_value):
    code = str(event_code_value or '').strip()
    name = str(event_name_value or '').strip()

    if code:
        row = db.session.execute(
            text("""
                SELECT CAST(event_code AS TEXT) AS event_code,
                       COALESCE(NULLIF(display_name, ''), event_name) AS event_name
                FROM events
                WHERE CAST(event_code AS TEXT) = :event_code
                LIMIT 1
            """),
            {'event_code': code}
        ).mappings().first()
        if row:
            return str(row.get('event_code')), str(row.get('event_name') or '')
        return None, None

    if name:
        row = db.session.execute(
            text("""
                SELECT CAST(event_code AS TEXT) AS event_code,
                       COALESCE(NULLIF(display_name, ''), event_name) AS event_name
                FROM events
                WHERE LOWER(COALESCE(NULLIF(display_name, ''), event_name)) = LOWER(:event_name)
                LIMIT 1
            """),
            {'event_name': name}
        ).mappings().first()
        if row:
            return str(row.get('event_code')), str(row.get('event_name') or '')
        return None, None

    return None, None


def _extract_bearer_token():
    auth_header = request.headers.get('Authorization', '')
    if auth_header.lower().startswith('bearer '):
        return auth_header.split(' ', 1)[1].strip()
    return None


def _parse_dt(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    raw = str(value).strip()
    if not raw:
        return None
    if raw.endswith('Z'):
        raw = raw[:-1] + '+00:00'
    try:
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is not None:
            return dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt
    except Exception:
        return None


def _resolve_session(session_token):
    if not session_token:
        return None, None
    sess = AuthSession.query.filter_by(token=session_token, revoked=False).first()
    if not sess:
        return None, None
    user = AuthUser.query.filter_by(id=sess.user_id).first()
    if not user:
        return None, None
    sess.last_seen_at = datetime.utcnow()
    db.session.commit()
    return sess, user


def _is_local_request():
    host = str(request.host or '').split(':', 1)[0].strip().lower()
    forwarded_for = str(request.headers.get('X-Forwarded-For', '') or '')
    remote_addr = str(request.remote_addr or '').strip().lower()

    candidate_ips = [remote_addr]
    candidate_ips.extend(part.strip().lower() for part in forwarded_for.split(',') if part.strip())

    if host in {'localhost', '127.0.0.1'}:
        return True
    return any(ip in {'127.0.0.1', '::1', 'localhost'} for ip in candidate_ips)


def _weekly_upload_dev_access_enabled():
    raw_value = str(os.getenv('WEEKLY_UPLOAD_DEV_ACCESS', '1')).strip().lower()
    return raw_value not in {'0', 'false', 'no', 'off'}


def _authorize_weekly_upload_request():
    if _weekly_upload_dev_access_enabled() and _is_local_request():
        return None
    _sess, user = _require_authenticated_user()
    if user and _is_admin_user(user):
        return None
    if not user:
        return jsonify({'error': 'Unauthorized'}), 401
    return jsonify({'error': 'Forbidden'}), 403


def _user_payload(user):
    return {
        'id': user.id,
        'email': user.email,
        'displayName': user.display_name,
        'athleteCode': user.athlete_code,
        'defaultCourseCode': user.default_course_code,
        'defaultCourseName': user.default_course_name,
        'isAdmin': _is_admin_user(user),
        'lastLoginAt': user.last_login_at.isoformat() if user.last_login_at else None,
    }


def _record_login_event(user_id, provider, success):
    evt = AuthLoginEvent(
        user_id=user_id,
        provider=provider,
        success=success,
        ip_address=request.headers.get('X-Forwarded-For', request.remote_addr),
        user_agent=request.headers.get('User-Agent')
    )
    db.session.add(evt)
    db.session.commit()


def _feedback_creator_label(row):
    display_name = str(row.created_by_display_name or '').strip()
    if display_name:
        return display_name
    email = str(row.created_by_email or '').strip()
    if email:
        return email
    return 'Unknown'


def _feedback_payload(row):
    return {
        'id': row.id,
        'type': 'error' if str(row.request_type).lower() == 'error' else 'suggestion',
        'title': row.title,
        'details': row.details,
        'dateLogged': (row.created_at or datetime.utcnow()).strftime('%Y-%m-%d'),
        'status': (row.status or 'logged').lower(),
        'createdBy': _feedback_creator_label(row)
    }


def _chat_creator_label(row):
    display_name = str(row.created_by_display_name or '').strip()
    if display_name:
        return display_name
    email = str(row.created_by_email or '').strip()
    if email:
        return email
    athlete_code = str(row.athlete_code or '').strip()
    if athlete_code:
        return athlete_code
    return 'Unknown'


def _chat_message_payload(row):
    return {
        'id': row.id,
        'messageText': row.message_text,
        'createdAt': _datetime_to_api_string(row.created_at),
        'createdBy': _chat_creator_label(row),
        'athleteCode': str(row.athlete_code or '').strip() or None,
    }


def _datetime_to_api_string(value):
    if not value:
        return None
    if value.tzinfo is not None:
        return value.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')
    return value.isoformat() + 'Z'


def _build_admin_activity_feed(limit, since=None):
    login_query = AuthLoginEvent.query
    page_query = PageUsageEvent.query

    if since:
        login_query = login_query.filter(AuthLoginEvent.created_at >= since)
        page_query = page_query.filter(PageUsageEvent.created_at >= since)

    login_rows = login_query.order_by(AuthLoginEvent.created_at.desc()).limit(limit).all()
    page_rows = page_query.order_by(PageUsageEvent.created_at.desc()).limit(limit).all()

    user_ids = {
        int(row.user_id)
        for row in list(login_rows) + list(page_rows)
        if getattr(row, 'user_id', None) is not None
    }
    user_map = {}
    if user_ids:
        users = AuthUser.query.filter(AuthUser.id.in_(user_ids)).all()
        user_map = {int(user.id): user for user in users}

    activity = []

    for row in login_rows:
        user = user_map.get(int(row.user_id)) if row.user_id is not None else None
        activity.append({
            'activityType': 'login',
            'activityAt': _datetime_to_api_string(row.created_at),
            'userId': int(row.user_id) if row.user_id is not None else None,
            'email': user.email if user else None,
            'displayName': user.display_name if user else None,
            'provider': row.provider,
            'success': bool(row.success),
            'pagePath': None,
            'durationMs': None,
            'referrerPath': None,
            'userAgent': row.user_agent,
            'ipAddress': row.ip_address,
            '_sortAt': row.created_at or datetime.min
        })

    for row in page_rows:
        user = user_map.get(int(row.user_id)) if row.user_id is not None else None
        sort_at = row.entered_at or row.created_at or datetime.min
        activity.append({
            'activityType': 'page_visit',
            'activityAt': _datetime_to_api_string(sort_at),
            'userId': int(row.user_id) if row.user_id is not None else None,
            'email': user.email if user else None,
            'displayName': user.display_name if user else None,
            'provider': None,
            'success': None,
            'pagePath': row.page_path,
            'durationMs': int(row.duration_ms) if row.duration_ms is not None else None,
            'referrerPath': row.referrer_path,
            'userAgent': row.user_agent,
            'ipAddress': None,
            '_sortAt': sort_at
        })

    activity.sort(key=lambda row: row.get('_sortAt') or datetime.min, reverse=True)
    trimmed = activity[:limit]
    for row in trimmed:
        row.pop('_sortAt', None)
    return trimmed


WEEKLY_UPLOAD_LOG_LIMIT = 600
_weekly_upload_state_lock = threading.RLock()
_curve_reference_state_lock = threading.RLock()
_admin_worker_registry_lock = threading.RLock()


def _current_saturday_iso(reference_dt=None):
    current = reference_dt or datetime.now()
    start_of_week = current - timedelta(days=current.weekday())
    saturday = start_of_week + timedelta(days=5)
    return saturday.strftime('%Y-%m-%d')


def _latest_parkrun_saturday_iso(reference_dt=None):
    current = reference_dt or datetime.now()
    days_since_saturday = (current.weekday() - 5) % 7
    saturday = current - timedelta(days=days_since_saturday)
    return saturday.strftime('%Y-%m-%d')


def _coerce_bool(value, default=False):
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in ('1', 'true', 'yes', 'on'):
        return True
    if text in ('0', 'false', 'no', 'off'):
        return False
    return bool(default)


def _coerce_optional_int(value, field_name):
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(text)
    except Exception as exc:
        raise ValueError(f'{field_name} must be a whole number.') from exc


def _default_weekly_sql_pipeline_options():
    return {
        'startDate': _current_saturday_iso(),
        'rebuild': True,
        'runSqlPipeline': True,
        'buildAthletes': True,
        'skipCoeffUpdates': False,
        'noParkrunPostgres': False,
        'eventCode': None,
        'scraper': True,
        'allAthletes': True,
        'leaveAthletePostgres': False,
        'noVolunteers': False,
        'refreshMaterializedView': True,
        'rebuildMaterializedViewsFromDefinitions': False,
        'rebuildHistoricAfterRun': False,
        'resumeCurveFromStage2': False,
        'resumeCurveFromAllHistory': False,
        'forceFreshStart': False,
    }


def _parse_weekly_sql_pipeline_options(raw_options):
    defaults = _default_weekly_sql_pipeline_options()
    options = raw_options if isinstance(raw_options, dict) else {}
    start_date = str(options.get('startDate') or defaults['startDate']).strip()

    if not re.match(r'^\d{4}-\d{2}-\d{2}$', start_date):
        raise ValueError('startDate must be in YYYY-MM-DD format.')

    return {
        'startDate': start_date,
        'rebuild': _coerce_bool(options.get('rebuild'), defaults['rebuild']),
        'runSqlPipeline': _coerce_bool(options.get('runSqlPipeline'), defaults['runSqlPipeline']),
        'buildAthletes': _coerce_bool(options.get('buildAthletes'), defaults['buildAthletes']),
        'skipCoeffUpdates': _coerce_bool(options.get('skipCoeffUpdates'), defaults['skipCoeffUpdates']),
        'noParkrunPostgres': _coerce_bool(options.get('noParkrunPostgres'), defaults['noParkrunPostgres']),
        'eventCode': _coerce_optional_int(options.get('eventCode'), 'eventCode'),
        'scraper': _coerce_bool(options.get('scraper'), defaults['scraper']),
        'allAthletes': _coerce_bool(options.get('allAthletes'), defaults['allAthletes']),
        'leaveAthletePostgres': _coerce_bool(options.get('leaveAthletePostgres'), defaults['leaveAthletePostgres']),
        'noVolunteers': _coerce_bool(options.get('noVolunteers'), defaults['noVolunteers']),
        'refreshMaterializedView': _coerce_bool(options.get('refreshMaterializedView'), defaults['refreshMaterializedView']),
        'rebuildMaterializedViewsFromDefinitions': _coerce_bool(options.get('rebuildMaterializedViewsFromDefinitions'), defaults['rebuildMaterializedViewsFromDefinitions']),
        'rebuildHistoricAfterRun': _coerce_bool(options.get('rebuildHistoricAfterRun'), defaults['rebuildHistoricAfterRun']),
        'resumeCurveFromStage2': _coerce_bool(options.get('resumeCurveFromStage2'), defaults['resumeCurveFromStage2']),
        'resumeCurveFromAllHistory': _coerce_bool(options.get('resumeCurveFromAllHistory'), defaults['resumeCurveFromAllHistory']),
        'forceFreshStart': _coerce_bool(options.get('forceFreshStart'), defaults['forceFreshStart']),
    }


def _iso_to_slash_date(iso_date):
    text_value = str(iso_date or '').strip()
    if not re.match(r'^\d{4}-\d{2}-\d{2}$', text_value):
        raise ValueError('Date must be in YYYY-MM-DD format.')
    year, month, day = text_value.split('-')
    return f'{day}/{month}/{year}'


def _delete_uploaded_event_scope(event_code, iso_date):
    sqlite_conn = None
    render_db_conn = None
    deleted = {
        'sqlite': {'eventpositions': 0, 'parkrunEvents': 0},
        'postgres': {'eventpositions': 0, 'parkrunEvents': 0}
    }

    slash_date = _iso_to_slash_date(iso_date)

    try:
        sqlite_conn, sqlite_cursor, render_db_conn, render_cursor = connections()

        sqlite_cursor.execute(
            "SELECT COUNT(*) FROM eventpositions WHERE event_code = ? AND (event_date = ? OR (substr(event_date, 7, 4) || '-' || substr(event_date, 4, 2) || '-' || substr(event_date, 1, 2)) = ?)",
            (event_code, slash_date, iso_date)
        )
        deleted['sqlite']['eventpositions'] = int((sqlite_cursor.fetchone() or [0])[0] or 0)
        sqlite_cursor.execute(
            "DELETE FROM eventpositions WHERE event_code = ? AND (event_date = ? OR (substr(event_date, 7, 4) || '-' || substr(event_date, 4, 2) || '-' || substr(event_date, 1, 2)) = ?)",
            (event_code, slash_date, iso_date)
        )

        sqlite_cursor.execute(
            "SELECT COUNT(*) FROM parkrun_events WHERE event_code = ? AND (event_date = ? OR (substr(event_date, 7, 4) || '-' || substr(event_date, 4, 2) || '-' || substr(event_date, 1, 2)) = ?)",
            (event_code, slash_date, iso_date)
        )
        deleted['sqlite']['parkrunEvents'] = int((sqlite_cursor.fetchone() or [0])[0] or 0)
        sqlite_cursor.execute(
            "DELETE FROM parkrun_events WHERE event_code = ? AND (event_date = ? OR (substr(event_date, 7, 4) || '-' || substr(event_date, 4, 2) || '-' || substr(event_date, 1, 2)) = ?)",
            (event_code, slash_date, iso_date)
        )

        render_cursor.execute(
            """
            SELECT COUNT(*)
            FROM eventpositions
            WHERE event_code = %s
              AND (
                    event_date::text = %s
                 OR event_date::text = %s
                 OR CASE
                        WHEN event_date::text ~ '^\\d{2}/\\d{2}/\\d{4}$' THEN to_char(to_date(event_date::text, 'DD/MM/YYYY'), 'YYYY-MM-DD')
                        WHEN event_date::text ~ '^\\d{4}-\\d{2}-\\d{2}$' THEN event_date::text
                        ELSE NULL
                    END = %s
              )
            """,
            (event_code, slash_date, iso_date, iso_date)
        )
        deleted['postgres']['eventpositions'] = int((render_cursor.fetchone() or [0])[0] or 0)
        render_cursor.execute(
            """
            DELETE FROM eventpositions
            WHERE event_code = %s
              AND (
                    event_date::text = %s
                 OR event_date::text = %s
                 OR CASE
                        WHEN event_date::text ~ '^\\d{2}/\\d{2}/\\d{4}$' THEN to_char(to_date(event_date::text, 'DD/MM/YYYY'), 'YYYY-MM-DD')
                        WHEN event_date::text ~ '^\\d{4}-\\d{2}-\\d{2}$' THEN event_date::text
                        ELSE NULL
                    END = %s
              )
            """,
            (event_code, slash_date, iso_date, iso_date)
        )

        render_cursor.execute(
            """
            SELECT COUNT(*)
            FROM parkrun_events
            WHERE event_code = %s
              AND (
                    event_date::text = %s
                 OR event_date::text = %s
                 OR CASE
                        WHEN event_date::text ~ '^\\d{2}/\\d{2}/\\d{4}$' THEN to_char(to_date(event_date::text, 'DD/MM/YYYY'), 'YYYY-MM-DD')
                        WHEN event_date::text ~ '^\\d{4}-\\d{2}-\\d{2}$' THEN event_date::text
                        ELSE NULL
                    END = %s
              )
            """,
            (event_code, slash_date, iso_date, iso_date)
        )
        deleted['postgres']['parkrunEvents'] = int((render_cursor.fetchone() or [0])[0] or 0)
        render_cursor.execute(
            """
            DELETE FROM parkrun_events
            WHERE event_code = %s
              AND (
                    event_date::text = %s
                 OR event_date::text = %s
                 OR CASE
                        WHEN event_date::text ~ '^\\d{2}/\\d{2}/\\d{4}$' THEN to_char(to_date(event_date::text, 'DD/MM/YYYY'), 'YYYY-MM-DD')
                        WHEN event_date::text ~ '^\\d{4}-\\d{2}-\\d{2}$' THEN event_date::text
                        ELSE NULL
                    END = %s
              )
            """,
            (event_code, slash_date, iso_date, iso_date)
        )

        sqlite_conn.commit()
        render_db_conn.commit()
        return deleted
    except Exception:
        if sqlite_conn:
            try:
                sqlite_conn.rollback()
            except Exception:
                pass
        if render_db_conn:
            try:
                render_db_conn.rollback()
            except Exception:
                pass
        raise
    finally:
        if sqlite_conn:
            try:
                sqlite_conn.close()
            except Exception:
                pass
        if render_db_conn:
            try:
                render_db_conn.close()
            except Exception:
                pass


_weekly_upload_state = {
    'running': False,
    'status': 'idle',
    'runMode': 'standard',
    'runToken': 0,
    'stopRequested': False,
    'startedAt': None,
    'finishedAt': None,
    'totalCourses': 0,
    'processedCourses': 0,
    'currentCourse': '',
    'currentCode': '',
    'loopEvents': True,
    'loadHistory': False,
    'parkrunName': '',
    'sqlPipelineOptions': _default_weekly_sql_pipeline_options(),
    'previousSqlRun': None,
    'error': None,
    'logs': deque(maxlen=WEEKLY_UPLOAD_LOG_LIMIT)
}

_curve_reference_state = {
    'running': False,
    'status': 'idle',
    'runToken': 0,
    'startedAt': None,
    'finishedAt': None,
    'referenceDate': '',
    'error': None,
    'logs': deque(maxlen=WEEKLY_UPLOAD_LOG_LIMIT)
}

_admin_worker_registry = {}

WEEKLY_SQL_HISTORY_FILE = os.path.join(os.path.dirname(__file__), 'curve_progress', 'weekly_sql_history.json')
WEEKLY_SQL_STAGE_HISTORY_DEFINITIONS = (
    {'id': 'setup', 'completePatterns': ('[timing] process.full_pipeline.sql_pipeline:',)},
    {'id': 'scraper', 'completePatterns': ('[timing] process.full_pipeline.scraper:',)},
    {'id': 'athletes', 'completePatterns': ('[timing] process.full_pipeline.copy_athletes:',)},
    {'id': 'curve-stage-1', 'completePatterns': ('[curved_ranks][timing] weekly.stage1:', '[curved_ranks][timing] pipeline.stage1:',)},
    {'id': 'curve-stage-2-build', 'completePatterns': ('[curved_ranks][timing] weekly.stage2.current_snapshot:', '[curved_ranks][timing] stage2.build (',)},
    {'id': 'curve-all-history-reference', 'completePatterns': ('[curved_ranks][timing] weekly.curve_time_ranks_reference.rebuild_all_history:', '[curved_ranks] reusing existing all-history curve_time_ranks_reference for weekly athlete history build',)},
    {'id': 'curve-athlete-history', 'completePatterns': ('[curved_ranks][timing] curve_athlete_best_rank_history.fast_build',)},
    {'id': 'curve-history-sync', 'completePatterns': ('[timing] process.full_pipeline.curve_rank_updates_with_copy:', '[timing] process.resume_curve_stage2.curve_rank_updates_from_stage2', '[timing] process.resume_curve.curve_rank_updates_from_all_history',)},
    {'id': 'verify-summary', 'completePatterns': ('[timing] process.full_pipeline.curve_rank_range_summary_upload:',)},
    {'id': 'copy-results', 'completePatterns': ('[timing] process.full_pipeline.copy_parkrun_events:',)},
    {'id': 'mv-backup', 'completePatterns': ('[mv-backup] saved', '[mv-backup] no materialized views found for schema')},
    {'id': 'mv-refresh-foundation', 'completePatterns': ('refreshed mv_participant_run_filters in',)},
    {'id': 'mv-refresh-current', 'completePatterns': ('refreshed mv_best_curve in',)},
    {'id': 'mv-refresh-1y', 'completePatterns': ('refreshed mv_best_1y_curve in',)},
    {'id': 'mv-refresh-caches', 'completePatterns': ('refreshed mv_club_members_cache in', 'materialized views refreshed successfully.')},
    {'id': 'finish', 'completePatterns': ('completed weekly sql pipeline run.',)},
)


def _weekly_sql_scope_key(start_date, event_code):
    start_part = str(start_date or '').strip()
    event_part = 'all' if event_code is None or str(event_code).strip() == '' else str(event_code).strip()
    return f'{start_part}|{event_part}'


def _weekly_sql_history_summary_for_logs(options, status, logs, started_at=None, finished_at=None, error=None):
    start_date = str((options or {}).get('startDate') or '').strip()
    if not start_date:
        return None
    event_code = (options or {}).get('eventCode')
    stage_completed_at = {}
    completed_stage_ids = []
    normalized_logs = list(logs or [])

    for definition in WEEKLY_SQL_STAGE_HISTORY_DEFINITIONS:
        matched_at = None
        for entry in reversed(normalized_logs):
            message = str((entry or {}).get('message') or '').lower()
            if any(pattern.lower() in message for pattern in definition['completePatterns']):
                matched_at = (entry or {}).get('at')
                break
        if matched_at:
            stage_completed_at[definition['id']] = matched_at
            completed_stage_ids.append(definition['id'])

    auto_resume_mode = None
    if 'finish' not in completed_stage_ids:
        if any(stage_id in completed_stage_ids for stage_id in ('curve-all-history-reference', 'curve-athlete-history', 'curve-history-sync')):
            auto_resume_mode = 'allHistory'
        elif 'curve-stage-1' in completed_stage_ids:
            auto_resume_mode = 'stage2'

    last_message = ''
    if normalized_logs:
        last_message = str((normalized_logs[-1] or {}).get('message') or '')

    return {
        'scopeKey': _weekly_sql_scope_key(start_date, event_code),
        'startDate': start_date,
        'eventCode': event_code,
        'status': str(status or 'idle'),
        'startedAt': started_at,
        'finishedAt': finished_at,
        'error': error,
        'updatedAt': datetime.utcnow().isoformat() + 'Z',
        'completedStageIds': completed_stage_ids,
        'stageCompletedAt': stage_completed_at,
        'lastMessage': last_message,
        'autoResumeMode': auto_resume_mode,
    }


def _load_weekly_sql_history():
    try:
        with open(WEEKLY_SQL_HISTORY_FILE, 'r', encoding='utf-8') as handle:
            data = json.load(handle)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_weekly_sql_history(data):
    try:
        os.makedirs(os.path.dirname(WEEKLY_SQL_HISTORY_FILE), exist_ok=True)
        temp_path = WEEKLY_SQL_HISTORY_FILE + '.tmp'
        with open(temp_path, 'w', encoding='utf-8') as handle:
            json.dump(data, handle, ensure_ascii=True, indent=2)
        os.replace(temp_path, WEEKLY_SQL_HISTORY_FILE)
    except Exception:
        pass


def _get_weekly_sql_history_summary(options):
    start_date = str((options or {}).get('startDate') or '').strip()
    if not start_date:
        return None
    event_code = (options or {}).get('eventCode')
    history = _load_weekly_sql_history()
    return history.get(_weekly_sql_scope_key(start_date, event_code))


def _persist_weekly_sql_history_summary(summary):
    if not summary or not summary.get('scopeKey'):
        return
    history = _load_weekly_sql_history()
    history[str(summary['scopeKey'])] = summary
    _save_weekly_sql_history(history)


def _weekly_sql_history_summary_from_state_locked():
    if str(_weekly_upload_state.get('runMode') or 'standard') != 'sqlPipeline':
        return None
    options = dict(_weekly_upload_state.get('sqlPipelineOptions') or {})
    return _weekly_sql_history_summary_for_logs(
        options=options,
        status=_weekly_upload_state.get('status'),
        logs=list(_weekly_upload_state.get('logs') or []),
        started_at=_weekly_upload_state.get('startedAt'),
        finished_at=_weekly_upload_state.get('finishedAt'),
        error=_weekly_upload_state.get('error'),
    )


def _is_admin_user(user):
    if not user:
        return False
    admin_value = getattr(user, 'is_admin', None)
    # Local legacy schemas may not yet expose `is_admin`; treat authenticated users as admin there.
    if admin_value is None:
        return True
    return bool(admin_value)


def _weekly_upload_log(message, level='info', event_code=None, event_name=None, athletes=None, volunteers=None, run_token=None):
    entry = {
        'at': datetime.utcnow().isoformat() + 'Z',
        'level': str(level or 'info').lower(),
        'message': str(message or ''),
        'eventCode': str(event_code) if event_code is not None else '',
        'eventName': str(event_name) if event_name is not None else '',
        'athletes': int(athletes) if athletes is not None and str(athletes).isdigit() else None,
        'volunteers': int(volunteers) if volunteers is not None and str(volunteers).isdigit() else None,
    }
    sql_history_summary = None
    with _weekly_upload_state_lock:
        if run_token is not None and int(_weekly_upload_state.get('runToken') or 0) != int(run_token):
            return
        _weekly_upload_state['logs'].append(entry)
        sql_history_summary = _weekly_sql_history_summary_from_state_locked()
    if sql_history_summary is not None:
        _persist_weekly_sql_history_summary(sql_history_summary)


class _WeeklyUploadLogStream:
    def __init__(self, level='info', run_token=None):
        self.level = str(level or 'info').lower()
        self.run_token = run_token
        self._buffer = ''

    def write(self, data):
        text = str(data or '')
        if not text:
            return 0
        self._buffer += text
        while '\n' in self._buffer:
            line, self._buffer = self._buffer.split('\n', 1)
            line = line.strip('\r').strip()
            if line and not _is_local_request_log_line(line):
                _weekly_upload_log(line, level=self.level, run_token=self.run_token)
        return len(text)

    def flush(self):
        line = self._buffer.strip('\r').strip()
        if line and not _is_local_request_log_line(line):
            _weekly_upload_log(line, level=self.level, run_token=self.run_token)
        self._buffer = ''


def _weekly_upload_reset_state_locked(next_status='idle', error=None):
    _weekly_upload_state['running'] = False
    _weekly_upload_state['status'] = next_status
    _weekly_upload_state['stopRequested'] = False
    _weekly_upload_state['finishedAt'] = datetime.utcnow().isoformat() + 'Z'
    _weekly_upload_state['currentCourse'] = ''
    _weekly_upload_state['currentCode'] = ''
    _weekly_upload_state['error'] = error


def _weekly_upload_clear_view_state_locked():
    _weekly_upload_state['running'] = False
    _weekly_upload_state['status'] = 'idle'
    _weekly_upload_state['runMode'] = 'standard'
    _weekly_upload_state['stopRequested'] = False
    _weekly_upload_state['startedAt'] = None
    _weekly_upload_state['finishedAt'] = None
    _weekly_upload_state['totalCourses'] = 0
    _weekly_upload_state['processedCourses'] = 0
    _weekly_upload_state['currentCourse'] = ''
    _weekly_upload_state['currentCode'] = ''
    _weekly_upload_state['loopEvents'] = True
    _weekly_upload_state['loadHistory'] = False
    _weekly_upload_state['parkrunName'] = ''
    _weekly_upload_state['sqlPipelineOptions'] = dict(_default_weekly_sql_pipeline_options())
    _weekly_upload_state['previousSqlRun'] = None
    _weekly_upload_state['error'] = None
    _weekly_upload_state['logs'] = deque(maxlen=WEEKLY_UPLOAD_LOG_LIMIT)


def _weekly_upload_snapshot(scope_options=None):
    acquired = _weekly_upload_state_lock.acquire(timeout=2.0)
    if not acquired:
        snapshot = {
            'running': False,
            'status': 'unavailable',
            'runMode': 'standard',
            'stopRequested': False,
            'startedAt': None,
            'finishedAt': None,
            'totalCourses': 0,
            'processedCourses': 0,
            'currentCourse': '',
            'currentCode': '',
            'loopEvents': True,
            'loadHistory': False,
            'parkrunName': '',
            'sqlPipelineOptions': dict(_default_weekly_sql_pipeline_options()),
            'previousSqlRun': None,
            'error': 'Weekly upload state lock timed out. Reset the weekly SQL pipeline state or restart the local backend.',
            'logs': [],
            'currentCourseElapsedSeconds': 0,
            'isStalled': True,
        }
    else:
        try:
            state_previous_sql_run = _weekly_upload_state.get('previousSqlRun')
            snapshot = {
                'running': bool(_weekly_upload_state['running']),
                'status': str(_weekly_upload_state['status'] or 'idle'),
                'runMode': str(_weekly_upload_state.get('runMode') or 'standard'),
                'stopRequested': bool(_weekly_upload_state.get('stopRequested')),
                'startedAt': _weekly_upload_state['startedAt'],
                'finishedAt': _weekly_upload_state['finishedAt'],
                'totalCourses': int(_weekly_upload_state['totalCourses'] or 0),
                'processedCourses': int(_weekly_upload_state['processedCourses'] or 0),
                'currentCourse': str(_weekly_upload_state['currentCourse'] or ''),
                'currentCode': str(_weekly_upload_state['currentCode'] or ''),
                'loopEvents': bool(_weekly_upload_state['loopEvents']),
                'loadHistory': bool(_weekly_upload_state['loadHistory']),
                'parkrunName': str(_weekly_upload_state['parkrunName'] or ''),
                'sqlPipelineOptions': dict(_weekly_upload_state.get('sqlPipelineOptions') or _default_weekly_sql_pipeline_options()),
                'previousSqlRun': state_previous_sql_run,
                'error': _weekly_upload_state['error'],
                'logs': list(_weekly_upload_state['logs'])
            }
            current_started_at = _weekly_upload_state.get('currentCourseStartedAt')
            if snapshot['running'] and current_started_at:
                elapsed = max(0, int(time.time() - float(current_started_at)))
                snapshot['currentCourseElapsedSeconds'] = elapsed
                snapshot['isStalled'] = elapsed >= int(os.getenv('WEEKLY_UPLOAD_STALL_SECONDS', '300'))
            else:
                snapshot['currentCourseElapsedSeconds'] = 0
                snapshot['isStalled'] = False
        finally:
            _weekly_upload_state_lock.release()
    if scope_options:
        requested_key = _weekly_sql_scope_key(scope_options.get('startDate'), scope_options.get('eventCode'))
        if not snapshot.get('previousSqlRun') or str(snapshot['previousSqlRun'].get('scopeKey') or '') != requested_key:
            snapshot['previousSqlRun'] = _get_weekly_sql_history_summary(scope_options)
    return snapshot


def _prune_admin_worker_registry_locked():
    inactive_keys = []
    for registry_key, metadata in _admin_worker_registry.items():
        worker = metadata.get('thread')
        if worker is None or not worker.is_alive():
            inactive_keys.append(registry_key)
    for registry_key in inactive_keys:
        _admin_worker_registry.pop(registry_key, None)


def _register_admin_worker(worker_type, run_token, worker, label=''):
    registry_key = f'{str(worker_type)}:{int(run_token)}'
    with _admin_worker_registry_lock:
        _prune_admin_worker_registry_locked()
        _admin_worker_registry[registry_key] = {
            'type': str(worker_type),
            'runToken': int(run_token),
            'thread': worker,
            'label': str(label or ''),
            'startedAt': datetime.utcnow().isoformat() + 'Z',
        }


def _unregister_admin_worker(worker_type, run_token):
    registry_key = f'{str(worker_type)}:{int(run_token)}'
    with _admin_worker_registry_lock:
        _admin_worker_registry.pop(registry_key, None)
        _prune_admin_worker_registry_locked()


def _active_admin_worker_summaries():
    with _admin_worker_registry_lock:
        _prune_admin_worker_registry_locked()
        summaries = []
        for metadata in _admin_worker_registry.values():
            worker = metadata.get('thread')
            summaries.append({
                'type': str(metadata.get('type') or ''),
                'runToken': int(metadata.get('runToken') or 0),
                'label': str(metadata.get('label') or ''),
                'startedAt': metadata.get('startedAt'),
                'alive': bool(worker and worker.is_alive()),
            })
        return summaries


def _background_jobs_running_error():
    active_workers = _active_admin_worker_summaries()
    if not active_workers:
        return None
    parts = []
    for worker in active_workers:
        worker_type = str(worker.get('type') or 'worker')
        label = str(worker.get('label') or '').strip()
        parts.append(f'{worker_type}{f" ({label})" if label else ""}')
    joined = ', '.join(parts)
    return {
        'error': f'Another admin background job is still running: {joined}. Wait for it to finish or restart the local backend before starting a new process.',
        'backgroundJobs': active_workers,
    }


class _ThreadingWSGIServer(socketserver.ThreadingMixIn, WSGIServer):
    daemon_threads = True


class _QuietWSGIRequestHandler(WSGIRequestHandler):
    def log_message(self, format, *args):
        return


def _is_local_request_log_line(message):
    text = str(message or '').strip()
    return bool(re.match(r'^(127\.0\.0\.1|localhost)\s+-\s+-\s+\[', text, flags=re.IGNORECASE))


def _curve_reference_log(message, level='info', run_token=None):
    entry = {
        'at': datetime.utcnow().isoformat() + 'Z',
        'level': str(level or 'info').lower(),
        'message': str(message or ''),
        'eventCode': '',
        'eventName': '',
        'athletes': None,
        'volunteers': None,
    }
    with _curve_reference_state_lock:
        if run_token is not None and int(_curve_reference_state.get('runToken') or 0) != int(run_token):
            return
        _curve_reference_state['logs'].append(entry)


class _CurveReferenceLogStream:
    def __init__(self, level='info', run_token=None):
        self.level = str(level or 'info').lower()
        self.run_token = run_token
        self._buffer = ''

    def write(self, data):
        text = str(data or '')
        if not text:
            return 0
        self._buffer += text
        while '\n' in self._buffer:
            line, self._buffer = self._buffer.split('\n', 1)
            line = line.strip('\r').strip()
            if line and not _is_local_request_log_line(line):
                _curve_reference_log(line, level=self.level, run_token=self.run_token)
        return len(text)

    def flush(self):
        line = self._buffer.strip('\r').strip()
        if line and not _is_local_request_log_line(line):
            _curve_reference_log(line, level=self.level, run_token=self.run_token)
        self._buffer = ''


def _curve_reference_reset_state_locked(next_status='idle', error=None):
    _curve_reference_state['running'] = False
    _curve_reference_state['status'] = next_status
    _curve_reference_state['finishedAt'] = datetime.utcnow().isoformat() + 'Z'
    _curve_reference_state['error'] = error


def _curve_reference_clear_view_state_locked():
    _curve_reference_state['running'] = False
    _curve_reference_state['status'] = 'idle'
    _curve_reference_state['startedAt'] = None
    _curve_reference_state['finishedAt'] = None
    _curve_reference_state['referenceDate'] = ''
    _curve_reference_state['error'] = None
    _curve_reference_state['logs'] = deque(maxlen=WEEKLY_UPLOAD_LOG_LIMIT)


def _curve_reference_snapshot():
    acquired = _curve_reference_state_lock.acquire(timeout=2.0)
    if not acquired:
        return {
            'running': False,
            'status': 'unavailable',
            'referenceDate': '',
            'startedAt': None,
            'finishedAt': None,
            'error': 'Curve reference state lock timed out. Reset the curve reference task state or restart the local backend.',
            'logs': []
        }

    try:
        return {
            'running': bool(_curve_reference_state['running']),
            'status': str(_curve_reference_state.get('status') or 'idle'),
            'referenceDate': str(_curve_reference_state.get('referenceDate') or ''),
            'startedAt': _curve_reference_state.get('startedAt'),
            'finishedAt': _curve_reference_state.get('finishedAt'),
            'error': _curve_reference_state.get('error'),
            'logs': list(_curve_reference_state.get('logs') or [])
        }
    finally:
        _curve_reference_state_lock.release()


def _run_curve_reference_publish_task(reference_date, run_token):
    try:
        from curved_ranks import run_curve_time_ranks_reference_one_off
    except Exception as exc:
        _curve_reference_log(f'Unable to import curved_ranks.run_curve_time_ranks_reference_one_off: {exc}', level='error', run_token=run_token)
        with _curve_reference_state_lock:
            if int(_curve_reference_state.get('runToken') or 0) == int(run_token):
                _curve_reference_reset_state_locked(next_status='failed', error=str(exc))
        return

    stream = _CurveReferenceLogStream(level='info', run_token=run_token)
    selected_date = str(reference_date or '').strip()
    selected_label = selected_date or 'ALL'

    try:
        _curve_reference_log(
            f'Started curve_time_ranks_reference publish run for snapshot={selected_label}.',
            level='info',
            run_token=run_token,
        )

        with redirect_stdout(stream), redirect_stderr(stream):
            processed = run_curve_time_ranks_reference_one_off(current_date=selected_date or None)
            print(f"[curve_time_ranks_reference] sqlite one-off build complete: {processed}")
            upload_result = _upload_curve_time_ranks_reference_to_postgres()
            print(
                "[curve_time_ranks_reference] postgres upload complete: "
                f"version={upload_result['referenceVersion']}, rows={upload_result['rowsUploaded']}"
            )

        stream.flush()
        with _curve_reference_state_lock:
            if int(_curve_reference_state.get('runToken') or 0) == int(run_token):
                _curve_reference_state['status'] = 'completed'
                _curve_reference_state['error'] = None
        _curve_reference_log('Completed curve_time_ranks_reference publish run.', level='success', run_token=run_token)
    except Exception as exc:
        stream.flush()
        _curve_reference_log(f'curve_time_ranks_reference publish failed: {exc}', level='error', run_token=run_token)
        with _curve_reference_state_lock:
            if int(_curve_reference_state.get('runToken') or 0) == int(run_token):
                _curve_reference_state['status'] = 'failed'
                _curve_reference_state['error'] = str(exc)
    finally:
        with _curve_reference_state_lock:
            if int(_curve_reference_state.get('runToken') or 0) == int(run_token):
                _curve_reference_reset_state_locked(
                    next_status=str(_curve_reference_state.get('status') or 'idle'),
                    error=_curve_reference_state.get('error')
                )
        _unregister_admin_worker('curveReferencePublish', run_token)


def _fetch_weekly_loop_events(cursor, target_date=None):
    target_iso = str(target_date or _latest_parkrun_saturday_iso())
    iso_expr = "substr(event_date, 7, 4) || '-' || substr(event_date, 4, 2) || '-' || substr(event_date, 1, 2)"

    cursor.execute(f"""
        WITH latest_events AS (
            SELECT event_code, MAX({iso_expr}) AS latest_date
            FROM parkrun_events
            GROUP BY event_code
        )
        SELECT e.event_code, e.event_name
        FROM events e
        LEFT JOIN latest_events le ON le.event_code = e.event_code
        WHERE le.latest_date IS NULL OR le.latest_date < ?
        ORDER BY e.event_name
    """, (target_iso,))
    return cursor.fetchall()


def _latest_event_stats(cursor, event_code):
    try:
        cursor.execute('''
            SELECT event_date, last_position, volunteers, event_number
            FROM parkrun_events
            WHERE event_code = ?
            ORDER BY substr(event_date, 7, 4) || '-' || substr(event_date, 4, 2) || '-' || substr(event_date, 1, 2) DESC
            LIMIT 1
        ''', (event_code,))
        row = cursor.fetchone()
        if not row:
            return None
        return {
            'eventDate': row[0],
            'athletes': row[1],
            'volunteers': row[2],
            'eventNumber': row[3]
        }
    except Exception:
        return None


def _next_event_results_url(cursor, event_code, event_name):
    try:
        cursor.execute(
            '''
            SELECT MAX(event_number)
            FROM parkrun_events
            WHERE event_code = ? AND event_number IS NOT NULL AND event_number < 10000
            ''',
            (event_code,)
        )
        row = cursor.fetchone()
        latest_event_number = int(row[0]) if row and row[0] is not None else None
        if latest_event_number is None:
            return None
        next_event_number = latest_event_number + 1
        return f'https://www.parkrun.org.uk/{str(event_name).lower()}/results/{next_event_number}/'
    except Exception:
        return None


def _course_has_latest_upload(cursor, event_code, target_date=None):
    try:
        iso_expr = "substr(event_date, 7, 4) || '-' || substr(event_date, 4, 2) || '-' || substr(event_date, 1, 2)"
        target_iso = str(target_date or _latest_parkrun_saturday_iso())

        cursor.execute(f"SELECT MAX({iso_expr}) FROM parkrun_events WHERE event_code = ?", (event_code,))
        course_latest = cursor.fetchone()
        course_latest_date = course_latest[0] if course_latest else None
        if not course_latest_date:
            return False

        return str(course_latest_date) >= target_iso
    except Exception:
        return False


def _run_weekly_upload_worker(loop_events, load_history, parkrun_name, run_token=None):
    conn = None
    render_db_conn = None
    driver = None
    had_errors = False
    previous_human_check_mode = os.getenv('PARKRUN_HUMAN_CHECK_MODE')

    try:
        os.environ['PARKRUN_HUMAN_CHECK_MODE'] = 'skip'
        conn, cursor, render_db_conn, render_cursor = connections()
        driver = create_webdriver()

        if loop_events:
            target_upload_date = _latest_parkrun_saturday_iso()
            events = _fetch_weekly_loop_events(cursor, target_upload_date)
            total = len(events)
            with _weekly_upload_state_lock:
                _weekly_upload_state['totalCourses'] = total
            _weekly_upload_log(f'Started weekly upload for {total} courses targeting {target_upload_date}.', level='info')

            if total == 0:
                _weekly_upload_log(f'No courses need upload for {target_upload_date}.', level='info')

            for index, event in enumerate(events, start=1):
                with _weekly_upload_state_lock:
                    stop_requested = bool(_weekly_upload_state.get('stopRequested'))
                if stop_requested:
                    _weekly_upload_log('Weekly upload stop requested. Halting before the next course.', level='warning')
                    with _weekly_upload_state_lock:
                        _weekly_upload_state['processedCourses'] = index - 1
                        _weekly_upload_state['status'] = 'stopped'
                        _weekly_upload_state['error'] = None
                    break

                event_code = event[0]
                event_name = event[1]

                # In weekly latest-results mode, skip courses that already have the target parkrun date.
                if not load_history and _course_has_latest_upload(cursor, event_code, target_upload_date):
                    _weekly_upload_log(
                        f'[{index}/{total}] Skipping {event_name} (already has {target_upload_date}).',
                        level='info',
                        event_code=event_code,
                        event_name=event_name
                    )
                    with _weekly_upload_state_lock:
                        _weekly_upload_state['processedCourses'] = index
                    continue

                with _weekly_upload_state_lock:
                    _weekly_upload_state['processedCourses'] = index - 1
                    _weekly_upload_state['currentCourse'] = str(event_name or '')
                    _weekly_upload_state['currentCode'] = str(event_code or '')

                _weekly_upload_log(
                    f'[{index}/{total}] Processing {event_name} (code {event_code})',
                    level='info',
                    event_code=event_code,
                    event_name=event_name
                )

                try:
                    if load_history:
                        process_parkrun_history(driver, cursor, render_cursor, conn, render_db_conn, event_code, event_name)
                    else:
                        url = f'https://www.parkrun.org.uk/{str(event_name).lower()}/results/latestresults/'
                        upload_succeeded = bool(process_event_url(driver, cursor, render_cursor, url, event_code, event_name, conn, render_db_conn))

                        if not upload_succeeded:
                            fallback_url = _next_event_results_url(cursor, event_code, event_name)
                            if fallback_url:
                                _weekly_upload_log(
                                    f'latestresults failed for {event_name}. Retrying explicit event URL {fallback_url}',
                                    level='warning',
                                    event_code=event_code,
                                    event_name=event_name
                                )
                                upload_succeeded = bool(process_event_url(driver, cursor, render_cursor, fallback_url, event_code, event_name, conn, render_db_conn))

                        if not upload_succeeded:
                            raise RuntimeError('Upload fetch failed for latestresults and explicit next-event URL.')

                    stats = _latest_event_stats(cursor, event_code)
                    _weekly_upload_log(
                        f'Completed {event_name}',
                        level='success',
                        event_code=event_code,
                        event_name=event_name,
                        athletes=stats.get('athletes') if stats else None,
                        volunteers=stats.get('volunteers') if stats else None
                    )
                except Exception as course_exc:
                    had_errors = True
                    _weekly_upload_log(
                        f'Error on {event_name}: {course_exc}',
                        level='error',
                        event_code=event_code,
                        event_name=event_name
                    )

            with _weekly_upload_state_lock:
                if _weekly_upload_state.get('status') != 'stopped':
                    _weekly_upload_state['processedCourses'] = total
        else:
            with _weekly_upload_state_lock:
                _weekly_upload_state['totalCourses'] = 1
                _weekly_upload_state['processedCourses'] = 0
                _weekly_upload_state['currentCourse'] = str(parkrun_name or '')
                _weekly_upload_state['currentCode'] = ''

            _weekly_upload_log(f'Processing single parkrun history: {parkrun_name}', level='info', event_name=parkrun_name)
            with _weekly_upload_state_lock:
                stop_requested = bool(_weekly_upload_state.get('stopRequested'))
            if stop_requested:
                _weekly_upload_log('Weekly upload stop requested before single-parkrun history load started.', level='warning', event_name=parkrun_name)
                with _weekly_upload_state_lock:
                    _weekly_upload_state['status'] = 'stopped'
                    _weekly_upload_state['error'] = None
            else:
                process_parkrun_history(driver, cursor, render_cursor, conn, render_db_conn, None, parkrun_name)
                with _weekly_upload_state_lock:
                    _weekly_upload_state['processedCourses'] = 1
                _weekly_upload_log('Completed single-parkrun history load.', level='success', event_name=parkrun_name)

        if _weekly_upload_state.get('status') != 'stopped':
            conn.commit()
            render_db_conn.commit()

        with _weekly_upload_state_lock:
            if _weekly_upload_state.get('status') != 'stopped':
                _weekly_upload_state['status'] = 'completed_with_errors' if had_errors else 'completed'
                _weekly_upload_state['error'] = 'One or more courses failed. See log.' if had_errors else None
    except Exception as exc:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        if render_db_conn:
            try:
                render_db_conn.rollback()
            except Exception:
                pass

        _weekly_upload_log(f'Weekly upload failed: {exc}', level='error')
        with _weekly_upload_state_lock:
            _weekly_upload_state['status'] = 'failed'
            _weekly_upload_state['error'] = str(exc)
    finally:
        if previous_human_check_mode is None:
            os.environ.pop('PARKRUN_HUMAN_CHECK_MODE', None)
        else:
            os.environ['PARKRUN_HUMAN_CHECK_MODE'] = previous_human_check_mode

        with _weekly_upload_state_lock:
            stopped_run = str(_weekly_upload_state.get('status') or '') == 'stopped'

        with _weekly_upload_state_lock:
            _weekly_upload_state['running'] = False
            _weekly_upload_state['stopRequested'] = False
            _weekly_upload_state['finishedAt'] = datetime.utcnow().isoformat() + 'Z'
            _weekly_upload_state['currentCourse'] = ''
            _weekly_upload_state['currentCode'] = ''

        if stopped_run:
            _weekly_upload_log('Weekly upload stopped. The backend process has finished.', level='warning')

        if conn:
            try:
                conn.close()
            except Exception:
                pass
        if render_db_conn:
            try:
                render_db_conn.close()
            except Exception:
                pass
        if driver:
            try:
                driver.quit()
            except Exception:
                pass
        if run_token is not None:
            _unregister_admin_worker('standard', run_token)


def _run_weekly_sql_pipeline_worker(sql_pipeline_options, run_token):
    try:
        from newAnalytics import run_simple_sql_loop
    except Exception as exc:
        _weekly_upload_log(f'Unable to import newAnalytics.run_simple_sql_loop: {exc}', level='error', run_token=run_token)
        with _weekly_upload_state_lock:
            if int(_weekly_upload_state.get('runToken') or 0) == int(run_token):
                _weekly_upload_reset_state_locked(next_status='failed', error=str(exc))
        return

    stream = _WeeklyUploadLogStream(level='info', run_token=run_token)
    options = _parse_weekly_sql_pipeline_options(sql_pipeline_options)

    try:
        with _weekly_upload_state_lock:
            if int(_weekly_upload_state.get('runToken') or 0) == int(run_token):
                _weekly_upload_state['totalCourses'] = 1
                _weekly_upload_state['processedCourses'] = 0
                _weekly_upload_state['currentCourse'] = str(options.get('startDate') or 'run_simple_sql_loop')
                _weekly_upload_state['currentCode'] = str(options.get('eventCode') or '')
                _weekly_upload_state['sqlPipelineOptions'] = dict(options)

        _weekly_upload_log(
            f"Started weekly SQL pipeline run for startDate={options['startDate']}"
            + (f", eventCode={options['eventCode']}" if options.get('eventCode') is not None else ', eventCode=all'),
            level='info',
            run_token=run_token,
        )

        with redirect_stdout(stream), redirect_stderr(stream):
            run_simple_sql_loop(
                start_date=options['startDate'],
                rebuild=options['rebuild'],
                run_sql_pipeline=options['runSqlPipeline'],
                buildAthletes=options['buildAthletes'],
                skip_coeff_updates=options['skipCoeffUpdates'],
                no_parkrun_postgres=options['noParkrunPostgres'],
                event_code=options['eventCode'],
                Scraper=options['scraper'],
                all_athletes=options['allAthletes'],
                leave_athlete_postgres=options['leaveAthletePostgres'],
                no_volunteers=options['noVolunteers'],
                refresh_materialized_view=options['refreshMaterializedView'],
                rebuild_materialized_views_from_definitions=options['rebuildMaterializedViewsFromDefinitions'],
                rebuild_historic_after_run=options['rebuildHistoricAfterRun'],
                resume_curve_from_stage2=options['resumeCurveFromStage2'],
                resume_curve_from_all_history=options['resumeCurveFromAllHistory'],
            )

        stream.flush()
        with _weekly_upload_state_lock:
            if int(_weekly_upload_state.get('runToken') or 0) == int(run_token):
                _weekly_upload_state['processedCourses'] = 1
                _weekly_upload_state['status'] = 'completed'
                _weekly_upload_state['error'] = None
        _weekly_upload_log('Completed weekly SQL pipeline run.', level='success', run_token=run_token)
    except Exception as exc:
        stream.flush()
        _weekly_upload_log(f'Weekly SQL pipeline failed: {exc}', level='error', run_token=run_token)
        with _weekly_upload_state_lock:
            if int(_weekly_upload_state.get('runToken') or 0) == int(run_token):
                _weekly_upload_state['status'] = 'failed'
                _weekly_upload_state['error'] = str(exc)
                summary = _weekly_sql_history_summary_from_state_locked()
            else:
                summary = None
        if summary is not None:
            _persist_weekly_sql_history_summary(summary)
    finally:
        with _weekly_upload_state_lock:
            if int(_weekly_upload_state.get('runToken') or 0) == int(run_token):
                _weekly_upload_reset_state_locked(
                    next_status=str(_weekly_upload_state.get('status') or 'idle'),
                    error=_weekly_upload_state.get('error')
                )
        _unregister_admin_worker('sqlPipeline', run_token)


def _ensure_curve_time_ranks_reference_postgres_table(render_cursor):
    render_cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS curve_time_ranks_reference (
            metric_type TEXT NOT NULL,
            curve_rank_group INTEGER NOT NULL,
            curve_rank_reference_version DATE NOT NULL,
            min_seconds INTEGER,
            max_seconds INTEGER,
            min_time TEXT,
            max_time TEXT,
            target_group_cnt DOUBLE PRECISION,
            actual_group_cnt INTEGER,
            score_upper DOUBLE PRECISION,
            score_lower DOUBLE PRECISION,
            created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (metric_type, curve_rank_group, curve_rank_reference_version)
        )
        """
    )
    render_cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_curve_time_ranks_reference_version
        ON curve_time_ranks_reference (curve_rank_reference_version DESC, metric_type, curve_rank_group DESC)
        """
    )


def _upload_curve_time_ranks_reference_to_postgres():
    sqlite_conn = None
    render_db_conn = None
    try:
        sqlite_conn, sqlite_cursor, render_db_conn, render_cursor = connections()
        _ensure_curve_time_ranks_reference_postgres_table(render_cursor)

        sqlite_cursor.execute(
            """
            SELECT
                metric_type,
                curve_rank_group,
                curve_rank_reference_version,
                min_seconds,
                max_seconds,
                min_time,
                max_time,
                target_group_cnt,
                actual_group_cnt,
                score_upper,
                score_lower
            FROM curve_time_ranks_reference
            ORDER BY metric_type, curve_rank_group DESC
            """
        )
        columns = [str(col[0]) for col in (sqlite_cursor.description or [])]
        rows = [dict(zip(columns, row)) for row in sqlite_cursor.fetchall()]
        if not rows:
            raise RuntimeError('curve_time_ranks_reference is empty in SQLite after the one-off rebuild.')

        versions = sorted({str(row.get('curve_rank_reference_version') or '').strip() for row in rows if str(row.get('curve_rank_reference_version') or '').strip()})
        if not versions:
            raise RuntimeError('curve_time_ranks_reference rows do not contain a curve_rank_reference_version value.')
        if len(versions) != 1:
            raise RuntimeError(f'Expected one active SQLite reference version, found {len(versions)}: {versions}')

        reference_version = versions[0]
        payload_rows = [
            (
                row.get('metric_type'),
                row.get('curve_rank_group'),
                reference_version,
                row.get('min_seconds'),
                row.get('max_seconds'),
                row.get('min_time'),
                row.get('max_time'),
                row.get('target_group_cnt'),
                row.get('actual_group_cnt'),
                row.get('score_upper'),
                row.get('score_lower'),
            )
            for row in rows
        ]

        render_cursor.executemany(
            """
            INSERT INTO curve_time_ranks_reference (
                metric_type,
                curve_rank_group,
                curve_rank_reference_version,
                min_seconds,
                max_seconds,
                min_time,
                max_time,
                target_group_cnt,
                actual_group_cnt,
                score_upper,
                score_lower
            )
            VALUES (%s, %s, CAST(%s AS DATE), %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (metric_type, curve_rank_group, curve_rank_reference_version)
            DO UPDATE SET
                min_seconds = EXCLUDED.min_seconds,
                max_seconds = EXCLUDED.max_seconds,
                min_time = EXCLUDED.min_time,
                max_time = EXCLUDED.max_time,
                target_group_cnt = EXCLUDED.target_group_cnt,
                actual_group_cnt = EXCLUDED.actual_group_cnt,
                score_upper = EXCLUDED.score_upper,
                score_lower = EXCLUDED.score_lower
            """,
            payload_rows,
        )
        render_db_conn.commit()
        return {
            'referenceVersion': reference_version,
            'rowsUploaded': len(payload_rows),
        }
    except Exception:
        if render_db_conn:
            try:
                render_db_conn.rollback()
            except Exception:
                pass
        raise
    finally:
        if sqlite_conn:
            try:
                sqlite_conn.close()
            except Exception:
                pass
        if render_db_conn:
            try:
                render_db_conn.close()
            except Exception:
                pass


def _run_curve_reference_publish_worker(reference_date, run_token):
    try:
        from curved_ranks import run_curve_time_ranks_reference_one_off
    except Exception as exc:
        _weekly_upload_log(f'Unable to import curved_ranks.run_curve_time_ranks_reference_one_off: {exc}', level='error', run_token=run_token)
        with _weekly_upload_state_lock:
            if int(_weekly_upload_state.get('runToken') or 0) == int(run_token):
                _weekly_upload_reset_state_locked(next_status='failed', error=str(exc))
        return

    stream = _WeeklyUploadLogStream(level='info', run_token=run_token)
    selected_date = str(reference_date or '').strip()
    selected_label = selected_date or 'ALL'

    try:
        with _weekly_upload_state_lock:
            if int(_weekly_upload_state.get('runToken') or 0) == int(run_token):
                _weekly_upload_state['totalCourses'] = 1
                _weekly_upload_state['processedCourses'] = 0
                _weekly_upload_state['currentCourse'] = selected_label
                _weekly_upload_state['currentCode'] = 'curve-reference'

        _weekly_upload_log(
            f'Started curve_time_ranks_reference publish run for snapshot={selected_label}.',
            level='info',
            run_token=run_token,
        )

        with redirect_stdout(stream), redirect_stderr(stream):
            processed = run_curve_time_ranks_reference_one_off(
                current_date=selected_date or None,
            )
            print(f"[curve_time_ranks_reference] sqlite one-off build complete: {processed}")
            upload_result = _upload_curve_time_ranks_reference_to_postgres()
            print(
                "[curve_time_ranks_reference] postgres upload complete: "
                f"version={upload_result['referenceVersion']}, rows={upload_result['rowsUploaded']}"
            )

        stream.flush()
        with _weekly_upload_state_lock:
            if int(_weekly_upload_state.get('runToken') or 0) == int(run_token):
                _weekly_upload_state['processedCourses'] = 1
                _weekly_upload_state['status'] = 'completed'
                _weekly_upload_state['error'] = None
        _weekly_upload_log('Completed curve_time_ranks_reference publish run.', level='success', run_token=run_token)
    except Exception as exc:
        stream.flush()
        _weekly_upload_log(f'curve_time_ranks_reference publish failed: {exc}', level='error', run_token=run_token)
        with _weekly_upload_state_lock:
            if int(_weekly_upload_state.get('runToken') or 0) == int(run_token):
                _weekly_upload_state['status'] = 'failed'
                _weekly_upload_state['error'] = str(exc)
    finally:
        with _weekly_upload_state_lock:
            if int(_weekly_upload_state.get('runToken') or 0) == int(run_token):
                _weekly_upload_reset_state_locked(
                    next_status=str(_weekly_upload_state.get('status') or 'idle'),
                    error=_weekly_upload_state.get('error')
                )
        _unregister_admin_worker('curveReferencePublish', run_token)


@app.route('/api/admin/weekly-upload/status', methods=['GET'])
def admin_weekly_upload_status():
    auth_error = _authorize_weekly_upload_request()
    if auth_error is not None:
        return auth_error
    scope_options = None
    requested_start_date = str(request.args.get('startDate') or '').strip()
    requested_event_code_raw = str(request.args.get('eventCode') or '').strip()
    if requested_start_date and re.match(r'^\d{4}-\d{2}-\d{2}$', requested_start_date):
        requested_event_code = int(requested_event_code_raw) if requested_event_code_raw.isdigit() else None
        scope_options = {
            'startDate': requested_start_date,
            'eventCode': requested_event_code,
        }
    return jsonify(_weekly_upload_snapshot(scope_options=scope_options)), 200


@app.route('/api/admin/weekly-upload/start', methods=['POST'])
def admin_weekly_upload_start():
    auth_error = _authorize_weekly_upload_request()
    if auth_error is not None:
        return auth_error

    payload = request.get_json(silent=True) or {}
    run_mode = str(payload.get('runMode') or 'standard').strip() or 'standard'
    loop_events = bool(payload.get('loopEvents', True))
    load_history = bool(payload.get('loadHistory', False))
    parkrun_name = str(payload.get('parkrunName') or 'default_parkrun').strip() or 'default_parkrun'
    sql_pipeline_options = _default_weekly_sql_pipeline_options()

    if run_mode == 'sqlPipeline' or run_mode == 'curveReferencePublish':
        try:
            sql_pipeline_options = _parse_weekly_sql_pipeline_options(payload.get('sqlPipelineOptions'))
        except ValueError as exc:
            return jsonify({'error': str(exc)}), 400
    if run_mode == 'sqlPipeline':
        matched_previous_sql_run = _get_weekly_sql_history_summary(sql_pipeline_options)
        if (
            not sql_pipeline_options.get('forceFreshStart')
            and not sql_pipeline_options.get('resumeCurveFromStage2')
            and not sql_pipeline_options.get('resumeCurveFromAllHistory')
            and matched_previous_sql_run
            and str(matched_previous_sql_run.get('autoResumeMode') or '') in ('stage2', 'allHistory')
        ):
            if str(matched_previous_sql_run.get('autoResumeMode') or '') == 'allHistory':
                sql_pipeline_options['resumeCurveFromAllHistory'] = True
            else:
                sql_pipeline_options['resumeCurveFromStage2'] = True
    else:
        matched_previous_sql_run = None

    background_jobs_error = _background_jobs_running_error()
    if background_jobs_error is not None:
        return jsonify(background_jobs_error), 409

    current_state = None
    with _weekly_upload_state_lock:
        if _weekly_upload_state['running']:
            current_state = {
                'running': bool(_weekly_upload_state['running']),
                'status': str(_weekly_upload_state['status'] or 'running')
            }
    if current_state is not None:
        return jsonify({'error': 'Weekly upload is already running.', 'state': current_state}), 409

    with _weekly_upload_state_lock:
        _weekly_upload_state['running'] = True
        _weekly_upload_state['status'] = 'running'
        _weekly_upload_state['runMode'] = run_mode
        _weekly_upload_state['runToken'] = int(_weekly_upload_state.get('runToken') or 0) + 1
        _weekly_upload_state['stopRequested'] = False
        _weekly_upload_state['startedAt'] = datetime.utcnow().isoformat() + 'Z'
        _weekly_upload_state['finishedAt'] = None
        _weekly_upload_state['totalCourses'] = 0
        _weekly_upload_state['processedCourses'] = 0
        _weekly_upload_state['currentCourse'] = ''
        _weekly_upload_state['currentCode'] = ''
        _weekly_upload_state['loopEvents'] = loop_events
        _weekly_upload_state['loadHistory'] = load_history
        _weekly_upload_state['parkrunName'] = parkrun_name
        _weekly_upload_state['sqlPipelineOptions'] = dict(sql_pipeline_options)
        _weekly_upload_state['previousSqlRun'] = matched_previous_sql_run
        _weekly_upload_state['error'] = None
        _weekly_upload_state['logs'] = deque(maxlen=WEEKLY_UPLOAD_LOG_LIMIT)
        run_token = int(_weekly_upload_state['runToken'])

    if run_mode == 'sqlPipeline':
        queued_message = 'Weekly SQL pipeline queued.'
    elif run_mode == 'curveReferencePublish':
        queued_message = 'curve_time_ranks_reference rebuild and Postgres publish queued.'
    else:
        queued_message = 'Weekly upload queued.'

    _weekly_upload_log(queued_message, level='info')
    if run_mode == 'sqlPipeline' and matched_previous_sql_run and sql_pipeline_options.get('resumeCurveFromAllHistory') and not sql_pipeline_options.get('forceFreshStart'):
        _weekly_upload_log(
            'Auto-resume checkpoint found for this date. The run will resume from Rebuild All-History Time Reference unless you force a fresh start.',
            level='info'
        )
    elif run_mode == 'sqlPipeline' and matched_previous_sql_run and sql_pipeline_options.get('resumeCurveFromStage2') and not sql_pipeline_options.get('forceFreshStart'):
        _weekly_upload_log(
            'Auto-resume checkpoint found for this date. The run will resume from Curve Stage 2 Current Snapshot unless you force a fresh start.',
            level='info'
        )

    if run_mode == 'sqlPipeline':
        worker = threading.Thread(
            target=_run_weekly_sql_pipeline_worker,
            args=(sql_pipeline_options, run_token),
            daemon=True
        )
    elif run_mode == 'curveReferencePublish':
        worker = threading.Thread(
            target=_run_curve_reference_publish_worker,
            args=(sql_pipeline_options.get('startDate'), run_token),
            daemon=True
        )
    else:
        worker = threading.Thread(
            target=_run_weekly_upload_worker,
            args=(loop_events, load_history, parkrun_name, run_token),
            daemon=True
        )
    _register_admin_worker(run_mode, run_token, worker, label=sql_pipeline_options.get('startDate') if run_mode != 'standard' else parkrun_name)
    worker.start()

    return jsonify({'ok': True, 'state': _weekly_upload_snapshot()}), 202


@app.route('/api/admin/curve-reference/publish', methods=['POST'])
def admin_curve_reference_publish():
    auth_error = _authorize_weekly_upload_request()
    if auth_error is not None:
        return auth_error

    payload = request.get_json(silent=True) or {}
    try:
        sql_pipeline_options = _parse_weekly_sql_pipeline_options(payload.get('sqlPipelineOptions'))
    except ValueError as exc:
        return jsonify({'error': str(exc)}), 400

    background_jobs_error = _background_jobs_running_error()
    if background_jobs_error is not None:
        return jsonify(background_jobs_error), 409

    current_state = None
    with _curve_reference_state_lock:
        if _curve_reference_state['running']:
            current_state = {
                'running': bool(_curve_reference_state['running']),
                'status': str(_curve_reference_state['status'] or 'running')
            }
    if current_state is not None:
        return jsonify({'error': 'Curve reference publish is already running.', 'state': current_state}), 409

    with _curve_reference_state_lock:
        _curve_reference_state['running'] = True
        _curve_reference_state['status'] = 'running'
        _curve_reference_state['runToken'] = int(_curve_reference_state.get('runToken') or 0) + 1
        _curve_reference_state['startedAt'] = datetime.utcnow().isoformat() + 'Z'
        _curve_reference_state['finishedAt'] = None
        _curve_reference_state['referenceDate'] = str(sql_pipeline_options.get('startDate') or '')
        _curve_reference_state['error'] = None
        _curve_reference_state['logs'] = deque(maxlen=WEEKLY_UPLOAD_LOG_LIMIT)
        run_token = int(_curve_reference_state['runToken'])

    _curve_reference_log('curve_time_ranks_reference rebuild and Postgres publish queued.', level='info')

    worker = threading.Thread(
        target=_run_curve_reference_publish_task,
        args=(sql_pipeline_options.get('startDate'), run_token),
        daemon=True
    )
    _register_admin_worker('curveReferencePublish', run_token, worker, label=sql_pipeline_options.get('startDate'))
    worker.start()

    return jsonify({'ok': True, 'state': _curve_reference_snapshot()}), 202


@app.route('/api/admin/curve-reference/status', methods=['GET'])
def admin_curve_reference_status():
    auth_error = _authorize_weekly_upload_request()
    if auth_error is not None:
        return auth_error
    return jsonify(_curve_reference_snapshot()), 200


@app.route('/api/admin/curve-reference/reset', methods=['POST'])
def admin_curve_reference_reset():
    auth_error = _authorize_weekly_upload_request()
    if auth_error is not None:
        return auth_error

    with _curve_reference_state_lock:
        has_state_to_clear = bool(_curve_reference_state['running']) \
            or bool(_curve_reference_state.get('logs')) \
            or str(_curve_reference_state.get('status') or '') not in ('', 'idle', 'reset') \
            or bool(_curve_reference_state.get('error'))
        if not has_state_to_clear:
            return jsonify({'error': 'No curve reference task state is currently active.'}), 409

        previous_reference_date = str(_curve_reference_state.get('referenceDate') or '')
        _curve_reference_state['runToken'] = int(_curve_reference_state.get('runToken') or 0) + 1
        _curve_reference_clear_view_state_locked()

    _curve_reference_log(
        f'Admin reset the curve reference task state{f" for {previous_reference_date}" if previous_reference_date else ""}. Any stale worker output will be ignored.',
        level='warning'
    )
    return jsonify({'ok': True, 'state': _curve_reference_snapshot()}), 202


@app.route('/api/admin/weekly-upload/stop', methods=['POST'])
def admin_weekly_upload_stop():
    auth_error = _authorize_weekly_upload_request()
    if auth_error is not None:
        return auth_error

    already_requested = False
    with _weekly_upload_state_lock:
        if not _weekly_upload_state['running']:
            return jsonify({'error': 'No weekly upload is currently running.'}), 409
        if str(_weekly_upload_state.get('runMode') or 'standard') != 'standard':
            return jsonify({'error': 'Stop is only supported for the standard weekly upload run.'}), 409
        if _weekly_upload_state.get('stopRequested'):
            already_requested = True
        else:
            _weekly_upload_state['stopRequested'] = True
            _weekly_upload_state['status'] = 'stopping'

    if already_requested:
        return jsonify({'ok': True, 'state': _weekly_upload_snapshot()}), 202

    _weekly_upload_log('Stop requested. The weekly upload will halt after the current course finishes.', level='warning')
    return jsonify({'ok': True, 'state': _weekly_upload_snapshot()}), 202


@app.route('/api/admin/weekly-upload/reset', methods=['POST'])
def admin_weekly_upload_reset():
    auth_error = _authorize_weekly_upload_request()
    if auth_error is not None:
        return auth_error

    with _weekly_upload_state_lock:
        current_run_mode = str(_weekly_upload_state.get('runMode') or 'standard')
        if current_run_mode == 'standard':
            return jsonify({'error': 'Reset is only supported for non-standard admin tasks.'}), 409
        has_state_to_clear = bool(_weekly_upload_state['running']) \
            or bool(_weekly_upload_state.get('logs')) \
            or str(_weekly_upload_state.get('status') or '') not in ('', 'idle', 'reset') \
            or bool(_weekly_upload_state.get('error'))
        if not has_state_to_clear:
            return jsonify({'error': 'No non-standard admin task state is currently active.'}), 409

        previous_course = str(_weekly_upload_state.get('currentCourse') or '')
        _weekly_upload_state['runToken'] = int(_weekly_upload_state.get('runToken') or 0) + 1
        _weekly_upload_clear_view_state_locked()

    _weekly_upload_log(
        f'Admin reset the {current_run_mode} task state{f" while {previous_course} was active" if previous_course else ""}. Any stale worker output will be ignored.',
        level='warning'
    )
    return jsonify({'ok': True, 'state': _weekly_upload_snapshot()}), 202


@app.route('/api/admin/weekly-upload/delete-event', methods=['POST'])
def admin_weekly_upload_delete_event():
    auth_error = _authorize_weekly_upload_request()
    if auth_error is not None:
        return auth_error

    payload = request.get_json(silent=True) or {}
    try:
        event_code = int(str(payload.get('eventCode') or '').strip())
    except Exception:
        return jsonify({'error': 'eventCode must be a whole number.'}), 400

    event_name = str(payload.get('eventName') or '').strip()
    start_date = str(payload.get('startDate') or '').strip()

    if not start_date:
        return jsonify({'error': 'startDate is required.'}), 400

    with _weekly_upload_state_lock:
        if _weekly_upload_state['running']:
            return jsonify({'error': 'Cannot delete an event while a weekly task is running.'}), 409

    try:
        deleted = _delete_uploaded_event_scope(event_code, start_date)
    except ValueError as exc:
        return jsonify({'error': str(exc)}), 400
    except Exception as exc:
        return jsonify({'error': f'Failed to delete uploaded event: {exc}'}), 500

    _weekly_upload_log(
        f"Deleted uploaded event data for {event_name or 'selected course'} on {start_date}",
        level='warning',
        event_code=event_code,
        event_name=event_name
    )
    _weekly_upload_log(
        'Delete summary: '
        f"sqlite eventpositions={deleted['sqlite']['eventpositions']}, sqlite parkrun_events={deleted['sqlite']['parkrunEvents']}, "
        f"postgres eventpositions={deleted['postgres']['eventpositions']}, postgres parkrun_events={deleted['postgres']['parkrunEvents']}",
        level='warning',
        event_code=event_code,
        event_name=event_name
    )

    return jsonify({'ok': True, 'deleted': deleted, 'state': _weekly_upload_snapshot()}), 200


def _supports_explicit_admin_flag():
    return hasattr(AuthUser, 'is_admin')


def _count_admins_and_bootstrap_state():
    if _supports_explicit_admin_flag():
        admin_count = int(AuthUser.query.filter_by(is_admin=True).count())
        return admin_count, admin_count == 0

    # Legacy schema fallback: no explicit admin column exists locally.
    total_users = int(AuthUser.query.count())
    return total_users, total_users == 0


@app.route('/api/admin/status', methods=['GET'])
def admin_status():
    _sess, user = _require_authenticated_user()
    if not user:
        return jsonify({'error': 'Unauthorized'}), 401

    admin_count, bootstrap_open = _count_admins_and_bootstrap_state()
    can_access = _is_admin_user(user) or bootstrap_open

    return jsonify({
        'canAccessAdmin': bool(can_access),
        'adminCount': int(admin_count),
        'bootstrapOpen': bool(bootstrap_open),
        'user': _user_payload(user)
    }), 200


@app.route('/api/admin/users', methods=['GET'])
def admin_users():
    _sess, user = _require_authenticated_user()
    if not user:
        return jsonify({'error': 'Unauthorized'}), 401

    admin_count, bootstrap_open = _count_admins_and_bootstrap_state()
    if not (_is_admin_user(user) or bootstrap_open):
        return jsonify({'error': 'Forbidden'}), 403

    rows = AuthUser.query.order_by(AuthUser.created_at.desc()).all()
    users_payload = []
    for row in rows:
        explicit_admin_value = getattr(row, 'is_admin', None)
        users_payload.append({
            'id': row.id,
            'email': row.email,
            'displayName': row.display_name,
            'athleteCode': row.athlete_code,
            'defaultCourseCode': row.default_course_code,
            'defaultCourseName': row.default_course_name,
            'isAdmin': bool(explicit_admin_value) if explicit_admin_value is not None else True,
            'createdAt': row.created_at.isoformat() if row.created_at else None,
            'lastLoginAt': row.last_login_at.isoformat() if row.last_login_at else None,
        })

    return jsonify({
        'users': users_payload,
        'adminCount': int(admin_count),
        'bootstrapOpen': bool(bootstrap_open)
    }), 200


@app.route('/api/admin/activity', methods=['GET'])
def admin_activity():
    _sess, user = _require_authenticated_user()
    if not user:
        return jsonify({'error': 'Unauthorized'}), 401

    admin_count, bootstrap_open = _count_admins_and_bootstrap_state()
    if not (_is_admin_user(user) or bootstrap_open):
        return jsonify({'error': 'Forbidden'}), 403

    try:
        limit = int(request.args.get('limit', 300))
    except Exception:
        limit = 300
    limit = max(1, min(limit, 5000))

    since = _parse_dt(request.args.get('since'))
    activity = _build_admin_activity_feed(limit, since)
    return jsonify({'activity': activity, 'limit': limit}), 200


@app.route('/api/admin/users/<int:user_id>/admin', methods=['POST'])
def admin_set_user_admin(user_id):
    _sess, user = _require_authenticated_user()
    if not user:
        return jsonify({'error': 'Unauthorized'}), 401

    admin_count, bootstrap_open = _count_admins_and_bootstrap_state()
    if not (_is_admin_user(user) or bootstrap_open):
        return jsonify({'error': 'Forbidden'}), 403

    if not _supports_explicit_admin_flag():
        return jsonify({'error': 'Admin role editing is not available in this local schema.'}), 400

    payload = request.get_json(silent=True) or {}
    desired_flag = bool(payload.get('isAdmin'))

    target = AuthUser.query.filter_by(id=user_id).first()
    if not target:
        return jsonify({'error': 'User not found'}), 404

    target.is_admin = desired_flag
    db.session.commit()

    updated_admin_count, updated_bootstrap_open = _count_admins_and_bootstrap_state()

    return jsonify({
        'ok': True,
        'user': {
            'id': target.id,
            'email': target.email,
            'displayName': target.display_name,
            'athleteCode': target.athlete_code,
            'defaultCourseCode': target.default_course_code,
            'defaultCourseName': target.default_course_name,
            'isAdmin': bool(getattr(target, 'is_admin', False)),
            'createdAt': target.created_at.isoformat() if target.created_at else None,
            'lastLoginAt': target.last_login_at.isoformat() if target.last_login_at else None,
        },
        'adminCount': int(updated_admin_count),
        'bootstrapOpen': bool(updated_bootstrap_open)
    }), 200


@app.route('/api/feedback-requests', methods=['GET'])
def get_feedback_requests():
    _sess, user = _require_authenticated_user()
    if not user:
        return jsonify({'error': 'Unauthorized'}), 401

    rows = FeedbackRequest.query.order_by(FeedbackRequest.id.asc()).all()
    payload = []
    for row in rows:
        payload.append(_feedback_payload(row))
    return jsonify(payload), 200


@app.route('/api/feedback-requests', methods=['POST'])
def create_feedback_request():
    _sess, user = _require_authenticated_user()
    if not user:
        return jsonify({'error': 'Unauthorized'}), 401

    payload = request.get_json(silent=True) or {}
    request_type_raw = str(payload.get('type') or '').strip().lower()
    title = str(payload.get('title') or '').strip()
    details = str(payload.get('details') or '').strip()
    status_raw = str(payload.get('status') or '').strip().lower() if 'status' in payload else None

    if request_type_raw not in ('error', 'suggestion'):
        return jsonify({'error': 'type must be "error" or "suggestion"'}), 400
    if not title:
        return jsonify({'error': 'title is required'}), 400
    if not details:
        return jsonify({'error': 'details are required'}), 400

    row = FeedbackRequest(
        request_type=request_type_raw,
        title=title,
        details=details,
        status='logged',
        created_by_user_id=user.id,
        created_by_display_name=(user.display_name or '').strip() or None,
        created_by_email=user.email,
        created_at=datetime.utcnow()
    )
    db.session.add(row)
    db.session.commit()

    return jsonify(_feedback_payload(row)), 201


@app.route('/api/feedback-requests/<int:request_id>', methods=['PUT'])
def update_feedback_request(request_id):
    _sess, user = _require_authenticated_user()
    if not user:
        return jsonify({'error': 'Unauthorized'}), 401
    if not bool(user.is_admin):
        return jsonify({'error': 'Forbidden'}), 403

    payload = request.get_json(silent=True) or {}
    request_type_raw = str(payload.get('type') or '').strip().lower()
    title = str(payload.get('title') or '').strip()
    details = str(payload.get('details') or '').strip()
    status_raw = str(payload.get('status') or '').strip().lower() if 'status' in payload else None

    if request_type_raw not in ('error', 'suggestion'):
        return jsonify({'error': 'type must be "error" or "suggestion"'}), 400
    if not title:
        return jsonify({'error': 'title is required'}), 400
    if not details:
        return jsonify({'error': 'details are required'}), 400

    row = FeedbackRequest.query.filter_by(id=request_id).first()
    if not row:
        return jsonify({'error': 'feedback request not found'}), 404

    row.request_type = request_type_raw
    row.title = title
    row.details = details
    if status_raw:
        row.status = status_raw
    db.session.commit()

    return jsonify(_feedback_payload(row)), 200


@app.route('/api/chat/messages', methods=['GET'])
def get_chat_messages():
    _sess, user = _require_authenticated_user()
    if not user:
        return jsonify({'error': 'Unauthorized'}), 401

    limit_raw = request.args.get('limit', 200)
    try:
        limit = max(1, min(int(limit_raw), 500))
    except Exception:
        limit = 200

    rows = ChatMessage.query.order_by(ChatMessage.created_at.desc(), ChatMessage.id.desc()).limit(limit).all()
    payload = [_chat_message_payload(row) for row in reversed(rows)]
    return jsonify(payload), 200


@app.route('/api/chat/messages', methods=['POST'])
def create_chat_message():
    _sess, user = _require_authenticated_user()
    if not user:
        return jsonify({'error': 'Unauthorized'}), 401

    payload = request.get_json(silent=True) or {}
    message_text = str(payload.get('messageText') or '').strip()
    if not message_text:
        return jsonify({'error': 'messageText is required'}), 400
    if len(message_text) > 2000:
        return jsonify({'error': 'messageText is too long'}), 400

    row = ChatMessage(
        created_by_user_id=user.id,
        created_by_display_name=(user.display_name or '').strip() or None,
        created_by_email=user.email,
        athlete_code=(user.athlete_code or '').strip() or None,
        message_text=message_text,
        created_at=datetime.utcnow()
    )
    db.session.add(row)
    db.session.commit()

    return jsonify(_chat_message_payload(row)), 201


@app.route('/api/auth/register', methods=['POST'])
def auth_register():
    payload = request.get_json(silent=True) or {}
    email = _normalize_email(payload.get('email'))
    password = str(payload.get('password') or '')
    display_name = (payload.get('displayName') or '').strip() or None
    athlete_code = _resolve_athlete_code(payload.get('athleteCode'))
    default_course_code, default_course_name = _resolve_default_course(
        payload.get('defaultCourseCode'),
        payload.get('defaultCourseName')
    )

    if not re.match(r'^[^@\s]+@[^@\s]+\.[^@\s]+$', email):
        return jsonify({'error': 'Valid email is required.'}), 400
    if len(password) < 8:
        return jsonify({'error': 'Password must be at least 8 characters.'}), 400

    existing = AuthUser.query.filter_by(email=email).first()
    if existing:
        return jsonify({'error': 'Email already registered.'}), 409

    user = AuthUser(
        email=email,
        password_hash=generate_password_hash(password),
        display_name=display_name,
        athlete_code=athlete_code,
        default_course_code=default_course_code,
        default_course_name=default_course_name,
        last_login_at=datetime.utcnow(),
    )
    db.session.add(user)
    db.session.commit()

    token = _session_token()
    db.session.add(AuthSession(token=token, user_id=user.id, provider='email'))
    db.session.commit()
    _record_login_event(user.id, 'email', True)

    return jsonify({'token': token, 'user': _user_payload(user)}), 200


@app.route('/api/auth/login', methods=['POST'])
def auth_login():
    payload = request.get_json(silent=True) or {}
    email = _normalize_email(payload.get('email'))
    password = str(payload.get('password') or '')

    user = AuthUser.query.filter_by(email=email).first()
    if not user or not user.password_hash or not check_password_hash(user.password_hash, password):
        _record_login_event(user.id if user else None, 'email', False)
        return jsonify({'error': 'Invalid email or password.'}), 401

    previous_last_login_at = user.last_login_at
    token = _session_token()
    if 'athleteCode' in payload:
        user.athlete_code = _resolve_athlete_code(payload.get('athleteCode'))
    if 'defaultCourseCode' in payload or 'defaultCourseName' in payload:
        dc_code, dc_name = _resolve_default_course(payload.get('defaultCourseCode'), payload.get('defaultCourseName'))
        user.default_course_code = dc_code
        user.default_course_name = dc_name
    user.last_login_at = datetime.utcnow()
    db.session.add(AuthSession(token=token, user_id=user.id, provider='email'))
    db.session.commit()
    _record_login_event(user.id, 'email', True)
    payload_user = _user_payload(user)
    payload_user['previousLoginAt'] = previous_last_login_at.isoformat() if previous_last_login_at else None
    return jsonify({'token': token, 'user': payload_user}), 200


@app.route('/api/auth/google', methods=['POST'])
def auth_google():
    if id_token is None or google_requests is None:
        return jsonify({'error': 'Google auth dependencies are not installed on backend.'}), 501

    payload = request.get_json(silent=True) or {}
    credential = payload.get('credential') or payload.get('idToken')
    athlete_code = _resolve_athlete_code(payload.get('athleteCode'))
    default_course_code, default_course_name = _resolve_default_course(
        payload.get('defaultCourseCode'),
        payload.get('defaultCourseName')
    )
    if not credential:
        return jsonify({'error': 'Google credential is required.'}), 400

    client_id = os.getenv('GOOGLE_CLIENT_ID')
    if not client_id:
        return jsonify({'error': 'GOOGLE_CLIENT_ID is not configured on backend.'}), 500

    try:
        # Allow small local clock drift to avoid intermittent "Token used too early" failures.
        claims = id_token.verify_oauth2_token(
            credential,
            google_requests.Request(),
            client_id,
            clock_skew_in_seconds=10
        )
    except Exception as exc:
        _record_login_event(None, 'google', False)
        return jsonify({'error': f'Invalid Google token: {exc}'}), 401

    google_sub = claims.get('sub')
    email = _normalize_email(claims.get('email'))
    display_name = claims.get('name')
    if not google_sub or not email:
        return jsonify({'error': 'Google token missing required claims.'}), 400

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
            default_course_name=default_course_name
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

    user.last_login_at = datetime.utcnow()
    db.session.commit()

    token = _session_token()
    db.session.add(AuthSession(token=token, user_id=user.id, provider='google'))
    db.session.commit()
    _record_login_event(user.id, 'google', True)
    payload_user = _user_payload(user)
    payload_user['previousLoginAt'] = previous_last_login_at.isoformat() if previous_last_login_at else None
    return jsonify({'token': token, 'user': payload_user}), 200


@app.route('/api/auth/logout', methods=['POST'])
def auth_logout():
    payload = request.get_json(silent=True) or {}
    session_token = payload.get('token') or _extract_bearer_token()
    if not session_token:
        return jsonify({'ok': True}), 200
    sess = AuthSession.query.filter_by(token=session_token, revoked=False).first()
    if sess:
        sess.revoked = True
        db.session.commit()
    return jsonify({'ok': True}), 200


@app.route('/api/events/options', methods=['GET'])
def get_event_options():
    rows = db.session.execute(text("""
        SELECT CAST(event_code AS TEXT) AS event_code,
               COALESCE(NULLIF(display_name, ''), event_name) AS event_name
        FROM events
        ORDER BY COALESCE(NULLIF(display_name, ''), event_name)
    """)).mappings().all()
    payload = [
        {
            'eventCode': str(row.get('event_code') or ''),
            'eventName': str(row.get('event_name') or '')
        }
        for row in rows
        if row.get('event_code') is not None and row.get('event_name') is not None
    ]
    return jsonify(payload), 200


@app.route('/api/auth/me', methods=['GET'])
def auth_me():
    session_token = _extract_bearer_token()
    _sess, user = _resolve_session(session_token)
    if not user:
        return jsonify({'error': 'Unauthorized'}), 401
    return jsonify({'user': _user_payload(user)}), 200


@app.route('/api/auth/link-athlete', methods=['POST'])
def auth_link_athlete():
    payload = request.get_json(silent=True) or {}
    session_token = payload.get('token') or _extract_bearer_token()
    _sess, user = _resolve_session(session_token)
    if not user:
        return jsonify({'error': 'Unauthorized'}), 401

    requested_athlete_code = _normalize_athlete_code(payload.get('athleteCode'))
    resolved_athlete_code = _resolve_athlete_code(requested_athlete_code)
    requested_default_course_code = payload.get('defaultCourseCode')
    requested_default_course_name = payload.get('defaultCourseName')
    resolved_default_course_code, resolved_default_course_name = _resolve_default_course(
        requested_default_course_code,
        requested_default_course_name
    )

    if requested_athlete_code and not resolved_athlete_code:
        user.athlete_code = None
        db.session.commit()
        return jsonify({
            'ok': True,
            'user': _user_payload(user),
            'message': 'athleteCode not found in athletes; stored as NULL.'
        }), 200

    user.athlete_code = resolved_athlete_code
    if 'defaultCourseCode' in payload or 'defaultCourseName' in payload:
        user.default_course_code = resolved_default_course_code
        user.default_course_name = resolved_default_course_name
    db.session.commit()
    return jsonify({'ok': True, 'user': _user_payload(user)}), 200


@app.route('/api/auth/config', methods=['GET'])
def auth_config():
    return jsonify({
        'googleClientId': os.getenv('GOOGLE_CLIENT_ID') or ''
    }), 200


@app.route('/api/analytics/page-visit', methods=['POST'])
def track_page_visit():
    payload = request.get_json(silent=True) or {}
    session_token = payload.get('token') or _extract_bearer_token()
    _sess, user = _resolve_session(session_token)

    page_path = (payload.get('path') or '').strip()
    if not page_path:
        return jsonify({'error': 'path is required'}), 400

    duration_ms = payload.get('durationMs')
    try:
        duration_ms = int(duration_ms) if duration_ms is not None else None
    except Exception:
        duration_ms = None

    if duration_ms is not None:
        duration_ms = max(0, min(duration_ms, 7 * 24 * 60 * 60 * 1000))

    event = PageUsageEvent(
        user_id=user.id if user else None,
        session_token=session_token,
        page_path=page_path[:512],
        entered_at=_parse_dt(payload.get('enteredAt')),
        left_at=_parse_dt(payload.get('leftAt')),
        duration_ms=duration_ms,
        referrer_path=(payload.get('referrer') or payload.get('referrerPath') or '')[:512] or None,
        user_agent=request.headers.get('User-Agent')
    )
    db.session.add(event)
    db.session.commit()
    return jsonify({'ok': True}), 200

@app.route('/', methods=['GET', 'POST'])
def home():
    if request.method == 'POST':
        # handle POST logic here
        return jsonify(message="Received a POST request"), 200
    return jsonify(message="Welcome to the API!"), 200
@app.route('/zz', methods=['GET', 'POST'])
def zz():
    if request.method == 'POST':
        # handle POST logic here
        print("In ZZ!!")
        return jsonify(message="Received a POST request"), 200
        
    return jsonify(message="Welcome to the API!"), 200
# Set up logging 
logging.basicConfig(level=logging.INFO)
@app.route('/get_parkrun_data', methods=['POST'])
def get_parkrun_data_route():
    try:
        print("GPDR")
        event_codes = request.json.get('eventCodes')
        print("GPDR",event_codes)
        logging.info(f"Received eventCodes: {event_codes}")
        result = get_parkrun_data(event_codes)
        #logging.info(f"Returning result: {result}")
        return jsonify(result) # Ensure the response is JSON
    except Exception as e:
        logging.error(f"Error fetching parkrun data: {e}")
        return jsonify({"error": "An error occurred while fetching the data."}), 500


@app.route('/api/eventinfo', methods=['GET'])
def get_event_info():
    """Return event_name and event_number for a supplied event_number/event_code/event_name and event_date.
       Accepts query params: event_number (int) OR event_code (int) OR event_name (str), plus event_date (str: DD/MM/YYYY or YYYY-MM-DD).
    """
    event_number = request.args.get('event_number', default=None, type=int)
    event_code = request.args.get('event_code', default=None, type=int)
    event_name = request.args.get('display_name', default=None, type=str)
    event_date = request.args.get('event_date', default=None, type=str)

    # Validate minimal inputs
    if event_date is None or (event_number is None and event_code is None and event_name is None):
        return jsonify({"error": "Provide event_date and one of event_number, event_code or event_name"}), 400

    try:
        conn, cursor, render_db_conn, render_cursor = connections()
        record = None
        # Prepare event_date variants to match DB formats (DD/MM/YYYY or YYYY-MM-DD)
        dates_to_try = [event_date]
        try:
            # If input is YYYY-MM-DD convert to DD/MM/YYYY
            import re
            if re.match(r'^\d{4}-\d{2}-\d{2}$', event_date or ''):
                parts = event_date.split('-')
                alt = f"{parts[2]}/{parts[1]}/{parts[0]}"
                if alt not in dates_to_try:
                    dates_to_try.append(alt)
            # If input is DD/MM/YYYY convert to YYYY-MM-DD
            if re.match(r'^\d{2}/\d{2}/\d{4}$', event_date or ''):
                p = event_date.split('/')
                alt2 = f"{p[2]}-{p[1]}-{p[0]}"
                if alt2 not in dates_to_try:
                    dates_to_try.append(alt2)
        except Exception:
            pass

        # Helper to execute a query with date variants
        def try_query(sql, params_base):
            for d in dates_to_try:
                params = list(params_base) + [d]
                cursor.execute(sql, tuple(params))
                r = cursor.fetchone()
                if r:
                    return r
            return None

        # Prefer event_number lookup when provided
        if event_number is not None:
            sql = '''
                SELECT pe.event_number, e.display_name, pe.event_code
                FROM parkrun_events pe
                LEFT JOIN events e ON pe.event_code = e.event_code
                WHERE pe.event_number = ? AND pe.event_date = ?
                LIMIT 1
            '''
            record = try_query(sql, [event_number])

        # Fallback to event_code + event_date
        if record is None and event_code is not None:
            sql = '''
                SELECT pe.event_number, e.display_name, pe.event_code
                FROM parkrun_events pe
                LEFT JOIN events e ON pe.event_code = e.event_code
                WHERE pe.event_code = ? AND pe.event_date = ?
                LIMIT 1
            '''
            record = try_query(sql, [event_code])

        # Fallback to event_name + event_date (case-insensitive exact match)
        if record is None and event_name is not None:
            sql = '''
                SELECT pe.event_number, e.display_name, pe.event_code
                FROM parkrun_events pe
                LEFT JOIN events e ON pe.event_code = e.event_code
                WHERE LOWER(e.event_name) = LOWER(?) AND pe.event_date = ?
                LIMIT 1
            '''
            record = try_query(sql, [event_name])

        if record:
            ev_number, ev_name, ev_code = record
            return jsonify({
                'event_number': ev_number,
                'event_name': ev_name,
                'event_code': ev_code
            }), 200
        return jsonify({"error": "Event not found"}), 404
    except Exception as e:
        logging.error(f"Error in get_event_info: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        try:
            conn.close()
        except Exception:
            pass
@app.route('/api/eventby_number', methods=['GET'])
def get_event_by_number():
    """Return event_date and optional event_name for a supplied event_code + event_number.
       Query params: event_code (int), event_number (int)
    """
    event_code = request.args.get('event_code', default=None, type=int)
    event_number = request.args.get('event_number', default=None, type=int)

    if event_code is None or event_number is None:
        return jsonify({"error": "Provide event_code and event_number"}), 400

    try:
        conn, cursor, render_db_conn, render_cursor = connections()
        cursor.execute('SELECT event_date FROM parkrun_events WHERE event_code = ? AND event_number = ? LIMIT 1', (event_code, event_number))
        row = cursor.fetchone()
        if not row:
            return jsonify({"error": "Event not found"}), 404
        event_date = row[0]
        # Try to fetch a friendly display name if available
        cursor.execute('SELECT display_name FROM events WHERE event_code = ? LIMIT 1', (event_code,))
        r2 = cursor.fetchone()
        display_name = r2[0] if r2 else None
        return jsonify({
            'event_code': event_code,
            'event_number': event_number,
            'event_date': event_date,
            'event_name': display_name
        }), 200
    except Exception as e:
        logging.error(f"Error in get_event_by_number: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        try:
            conn.close()
        except Exception:
            pass
@app.route('/reconnect', methods=['GET']) 
def reconnectDB(): 
    try: 
        conn, cursor, render_db_conn, render_cursor = connections() 
        return jsonify({"message": "Database reconnected successfully."}), 200
    except Exception as e: 
        logging.error(f"Database reconnecting error: {e}") 
        return jsonify({"error": "An error occurred while reconnecting to the database."}), 500
@app.route('/api/delete_eventpositions', methods=['DELETE'])
def delete_event_positions_local():
    data = request.get_json()  # Get the JSON payload
    event_code = data.get('event_code')
    event_date = data.get('event_date')

    # Validate input
    if not event_code or not event_date:
        return jsonify({"error": "event_code and event_date are required"}), 400

    try:
        conn,cursor,render_db_conn,render_cursor=connections()
        # Delete from the eventpositions table in SQLite
 
        cursor.execute('DELETE FROM eventpositions WHERE event_code = ? AND event_date = ?', (event_code, event_date))
        conn.commit()  # Commit changes to the SQLite database    
        return jsonify({"message": f"Record deleted from eventpositions."}), 200


    except Exception as e:
        conn.rollback()  # Rollback in case of error
        return jsonify({"error": str(e)}), 500   
@app.route('/api/parkrun_events', methods=['DELETE'])
def delete_parkrun_events():
    data = request.get_json()  # Get the JSON payload
    event_code = data.get('event_code')
    event_date = data.get('event_date')


    # Validate input
    if not event_code or not event_date:
        return jsonify({"error": "event_code and event_date are required"}), 400

    try:
        conn,cursor,render_db_conn,render_cursor=connections()
        # Delete from parkrun_events table

        cursor.execute('SELECT * FROM parkrun_events  WHERE event_code = ? AND event_date = ?', (event_code, event_date))
        record = cursor.fetchone()
        print("before",record);

        cursor.execute('DELETE FROM parkrun_events WHERE event_code = ? AND event_date = ?', (event_code, event_date))
        conn.commit()  # Commit changes to the SQLite database
    
        cursor.execute('SELECT * FROM parkrun_events  WHERE event_code = ? AND event_date = ?', (event_code, event_date))
        record = cursor.fetchone()
        print("after",record);

    
        return jsonify({"message": f"Record deleted from parkrun_events."}), 200

    except Exception as e:
        conn.rollback()  # Rollback in case of error
        return jsonify({"error": str(e)}), 500  
@app.route('/api/parkrun_event', methods=['GET'])
def get_parkrun_event():
    event_code = request.args.get('event_code', default=None, type=int)  # Get event_code from request URL
    event_date = request.args.get('event_date', default=None, type=str)  # Get event_date from request URL

    # Validate input
    if not event_code or not event_date:
        return jsonify({"error": "event_code and event_date are required"}), 400
    try:
        conn, cursor, render_db_conn, render_cursor = connections()
        # Fetch the event based on event_code and event_date
        cursor.execute('''
            SELECT event_number, last_position, volunteers 
            FROM parkrun_events 
            WHERE event_code = ? AND event_date = ?
        ''', (event_code, event_date))

        record = cursor.fetchone()

        if record:
            event_number, last_position, volunteers = record  # Unpack the record
            return jsonify({
                "event_code": event_code,
                "event_date": event_date,
                "event_number": event_number,
                "last_position": last_position,
                "volunteers": volunteers
            }), 200  # Return the fetched record as JSON
        else:
            return jsonify({"error": "Event not found"}), 404

    except Exception as e:
        print(f"Error occurred: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()  # Close the database connection
@app.route('/add-empty-event', methods=['POST'])
def add_empty_event():
    data = request.json
    print("Received data:", data)  # Debug statement
    event_code = data['event_code']
    event_date = data['event_date']
    last_position = data['last_position']
    event_number = data['event_number']
    volunteers = data['volunteers']
    parkrunName = data['parkrunName']

    conn, cursor, render_db_conn, render_cursor = connections()

    try:
        # Call the existing update_parkrun_events function with the parameters
        update_parkrun_events(cursor, render_cursor, event_code, event_date, last_position, event_number, volunteers, parkrunName)
        return jsonify({"message": "Empty event record added successfully!"}), 200
    except Exception as e:
        print(f"Error adding empty event record: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()    
@app.route('/process-event/<int:event_number>', methods=['POST'])
def process_event(event_number):
    print("enter process_event",event_number)
    conn = None
    driver = None
    try:
        # Connect to the database
        conn, cursor, render_db_conn, render_cursor = connections()
        driver = create_webdriver()

        # Get incoming JSON data
        data = request.get_json()
        print('data:', data)
        provided_event_name = data.get('event_name') if data else None
        provided_event_code = data.get('event_code') if data else None

        # Initialize variables for event_code and event_name
        event_code = provided_event_code
        event_name = provided_event_name  # Use provided event_name if available

        # If we have an event_code but not an event_name, look up the name from events table
        if event_code and not event_name:
            cursor.execute('SELECT event_name FROM events WHERE event_code = ? LIMIT 1', (event_code,))
            row = cursor.fetchone()
            if row:
                event_name = row[0]

        # If we still don't have an event_name, fall back to finding by event_number (legacy behavior)
        if not event_name:
            cursor.execute('''
                SELECT pe.event_code, e.event_name
                FROM parkrun_events pe
                LEFT JOIN events e ON pe.event_code = e.event_code
                WHERE pe.event_number = ?
                LIMIT 1
            ''', (event_number,))
            event_record = cursor.fetchone()

            if event_record:
                event_code = event_record[0]
                event_name = event_record[1]
            else:
                return jsonify({"error": "Event not found"}), 404  # Early return if event is not in the database

        # Construct the URL for the event
        url = f'https://www.parkrun.org.uk/{event_name.lower()}/results/{event_number}/'
        print('In here: process_event')

        # Call your function to process the event
        process_event_url(driver, cursor, render_cursor, url, event_number, event_name, conn, render_db_conn)
        return jsonify(message=f"Processed event number {event_number} successfully!"), 200

    except Exception as e:
        print(f"Error occurred while processing event {event_number}: {e}")
        return jsonify({"error": str(e)}), 500

    finally:
        if conn:
            conn.close()
        if driver:
            driver.quit()
@app.route('/process-event-date', methods=['POST'])
def process_event_date():
    print("enter PED")
    data = request.json
    print("enter PED1",data)
    event_code = data['event_code']
    event_date = data.get('event_date', None)
    EvNumber = data.get('EvNumber', None)  # Get EvNumber from the request (optional)

    event_name=""
    print("PED",event_code,event_date,EvNumber)
    # Ensure event_date is not None and well formatted
    if not event_date and not EvNumber:
        return jsonify({"error": "Event date is required."}), 400

    # Attempt to parse the date; handle possible value errors
    try:
        if event_date:
            # Replace Z with UTC offset to correctly parse
            parsed_event_date = datetime.fromisoformat(event_date.replace("Z", "+00:00"))  # Handles the Z as UTC
            formatted_event_date = parsed_event_date.strftime('%d/%m/%Y')  # Reformat to DD/MM/YYYY
            print('Processing code1=', event_code, 'date=', formatted_event_date)
    except ValueError as e:
        return jsonify({"error": "Invalid date format."}), 400  # Return error for bad date formats

    # Connect to the database
    conn, cursor, render_db_conn, render_cursor = connections()
    driver = create_webdriver()

    try:
        #print('Processing code=', event_code, 'date=', formatted_event_date)
        
       # Build initial event number based on EvNumber being provided
        if EvNumber is not None:
            event_number = EvNumber
            print('Using provided event number:', event_number)

            query = f'SELECT * FROM events WHERE event_code = {event_code};' # Use %s for PostgreSQL, ? for SQLite 
            cursor.execute(query)    
            eventN = cursor.fetchall()  # Fetch all records from events table
            event_name = eventN[0][1]
        else:
            print("PED- before")
            cursor.execute('''
                SELECT pe.event_number, e.event_name 
                FROM parkrun_events pe
                LEFT JOIN events e ON pe.event_code = e.event_code
                WHERE pe.event_code = ? AND pe.event_date = ?
                    AND pe.event_number < 10000
            ''', (event_code, formatted_event_date))
            event_number=None
            record = cursor.fetchone()
            print("PED- after",record)
            if not record:
                #print("PED- before2")
                result = get_event_number(event_code, event_date)
                print("PED- after2",result)
                if result is None or len(result) != 2:
                    return jsonify({"error": "Failed to retrieve event number and name."}), 500
                event_number, event_name = result                
                print("check event",event_number," ",event_name)
            if record or event_number>0:
                if event_number==None:
                    event_number = record[0]
                    event_name = record[1]
        print('Post-Processing code=', event_code, 'date=', event_date, 'name=', event_name,'number=', event_number)
        numeric_event_number = int(event_number)
        if numeric_event_number<10000:
            # Construct the URL for the specific event
            url = f'https://www.parkrun.org.uk/{event_name.lower()}/results/{event_number}/'
            print('In here: process_event_date')
            process_event_url(driver, cursor, render_cursor, url, event_number, event_name, conn, render_db_conn)
            return jsonify(message=f"Processed event date {event_date} for event code {event_code} successfully!"), 200
        else:
            return jsonify({"error": "Non-event."}), 404

    except Exception as e:
        print(f"Error occurred while processing event date {event_date}: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()
        driver.quit()
@app.route('/start-scraping', methods=['POST'])
def start_scraping():
    data = request.json  # Get the original request payload
    print("Received data:", data)  # Debugging line to see incoming request data

    loopEvents = request.json.get('loopEvents', True)  # Get loopEvents from the JSON payload
    loadHistory = request.json.get('loadHistory', False)  # Get loadHistory from the JSON payload
    parkrunName = request.json.get('parkrunName', "default_parkrun")  # Get the parkrun name from the payload, with default
    #return jsonify(message="Scraping started!"), 200
    print(f"Parkrun selected: {parkrunName}")
    print(f"Loop Event Type: {loopEvents}")

    # Connect to the SQLite database
    conn,cursor,render_db_conn,render_cursor=connections()
    driver = create_webdriver()

    ##################################
    #parkrunName = "rodingvalley"
    #updateFlag = False  # Default value; set to True when you want to update
    #loopEvents = False # use this to run a different loop to collect at the beginning of the week
    ##################################
    try:
        if loopEvents:
            # Query to select all events
            cursor.execute("""
                WITH max_date_cte AS (
                SELECT MAX(formatted_date) AS max_event_date
                FROM parkrun_events_view),
                latest_events AS (
                SELECT event_code,
                        MAX(formatted_date) AS latest_date
                FROM parkrun_events_view
                GROUP BY event_code),
                filtered_events AS (
                SELECT le.event_code
                FROM latest_events le
                JOIN max_date_cte md ON le.latest_date < md.max_event_date AND le.latest_date> date(md.max_event_date,'-7 days'))
                SELECT e.event_code, e.event_name
                FROM events e
                JOIN filtered_events fe ON e.event_code = fe.event_code;
            """)
            events = cursor.fetchall()  # Fetch all records from events 
            if not events:
                cursor.execute("""
                    WITH max_date_cte AS (
                    SELECT MAX(substr(event_date, 7, 4) || '-' || substr(event_date, 4, 2) || '-' || substr(event_date, 1, 2)) AS max_event_date
                    FROM parkrun_events),
                    latest_events AS (
                    SELECT event_code,
                            MAX(substr(event_date, 7, 4) || '-' || substr(event_date, 4, 2) || '-' || substr(event_date, 1, 2)) AS latest_date
                    FROM parkrun_events
                    GROUP BY event_code),
                    filtered_events AS (
                    SELECT le.event_code
                    FROM latest_events le
                    JOIN max_date_cte md ON le.latest_date <= md.max_event_date)
                    SELECT e.event_code, e.event_name
                    FROM events e
                    JOIN filtered_events fe ON e.event_code = fe.event_code;
                """)
                events = cursor.fetchall()  # Fetch all records from events again
            # Loop through each event for current period
            for event in events:
                event_code = event[0]  # Assuming the first column is event_code
                event_name = event[1]  # Assuming the second column is event_name
                print(f"Processing Event: {event_name} (Code: {event_code})")

                if loadHistory:
                    # Call the function to process the history of events
                    process_parkrun_history(driver, cursor, render_cursor, conn, render_db_conn, event_code, event_name)
                else:
                    # Construct the URL for the current event using event_name
                    url = f'https://www.parkrun.org.uk/{event_name.lower()}/results/latestresults/'
                    print(f"In here: start_scraping-loopEvents for {event_name}")
                    process_event_url(driver, cursor, render_cursor, url, event_code, event_name, conn, render_db_conn)

        else:
            # Process a specific parkrun if loopEvents is False
            process_parkrun_history(driver, cursor, render_cursor, conn, render_db_conn, None, parkrunName)

        
        # Commit the changes and close the database connection
        conn.commit()
        render_db_conn.commit()

        return jsonify(message="Scraping process completed successfully!"), 200  # Ensure a valid return
    
    except Exception as e:
        print(f"Error occurred: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()
        # Close the browser
        driver.quit()
class ProcessingStatus(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    status = db.Column(db.String(10), nullable=False)
    updated_at = db.Column(db.DateTime, server_default=db.func.current_timestamp(), onupdate=db.func.current_timestamp())
@app.route('/build', methods=['GET'])
def create_tables():
    db.create_all()  # 
@app.route('/status', methods=['GET'])
def get_status():
    """Get the current processing status."""
    print (f"get_status -1")
    status_entry = ProcessingStatus.query.first()
    print (f"get_status -2 {status_entry.status}")
    if status_entry:
        return jsonify({'status': status_entry.status}), 200
    else:
        return jsonify({'status': 'not set'}), 404
@app.route('/start', methods=['POST'])
def start_processing():
    """Set the processing status to 'running'."""
    status_entry = ProcessingStatus.query.first()
    print (f"start_processing -2 {status_entry.status}")
    
    if status_entry:
        status_entry.status = 'running'
    else:
        status_entry = ProcessingStatus(status='running')
        db.session.add(status_entry)
    
    db.session.commit()
    print (f"start_processing -3 status = started")
    return jsonify({'status': 'started'}), 200
@app.route('/stop', methods=['POST'])
def stop_processing(): 
    """Set the processing status to 'stopped'."""
    status_entry = ProcessingStatus.query.first()
    
    if status_entry:
        status_entry.status = 'stopped'
        db.session.commit()
        return jsonify({'status': 'stopped'}), 200
    else:
        return jsonify({'status': 'not set'}), 404      
class ParkrunEvent(db.Model):                           # Define your ParRunEvents model
    __tablename__ = 'parkrun_events'
    event_code = db.Column(db.Integer, primary_key=True)
    event_date = db.Column(db.String, nullable=False)
    last_position = db.Column(db.Integer)
    volunteers = db.Column(db.Integer)
    event_number = db.Column(db.Integer)

    __table_args__ = (db.UniqueConstraint('event_code', 'event_date', name='unique_event'),)
@app.route('/delete_duplicates', methods=['POST']) 
def delete_duplicates(): 
    try: 
        # SQL query to delete rows with position > 10000 and duplicate event_code and event_date 
        delete_query = """ 
        DELETE FROM parkrun_events 
        WHERE event_number > 10000 
        AND (event_code, event_date) IN ( 
            SELECT event_code, event_date 
            FROM parkrun_events 
            GROUP BY event_code, event_date 
            HAVING COUNT(*) > 1 ); 
            """ 
        result = db.session.execute(text(delete_query)) 
        print("delete dupes:",result)
        db.session.commit() 
        return jsonify({'message': 'Duplicate rows deleted successfully', 'deleted_rows': result.rowcount}), 200 
    except Exception as e: 
        db.session.rollback() 
        return jsonify({'error': str(e)}), 500
def get_event_number(event_code, event_date): 
    try:
        # Parse the event_date string to a datetime object 
        #print(event_date)
        event_date_obj = parse_date(event_date)
        #print(event_date_obj)
        # Check if the record exists 
        formatted_event_date = event_date_obj.strftime('%d/%m/%Y') 
        #print(formatted_event_date)
    except ValueError: 
        return None
    event = ParkrunEvent.query.filter_by(event_code=event_code, event_date=formatted_event_date).first() 

    conn,cursor,render_db_conn,render_cursor=connections()
    #driver = create_webdriver()
    query = f'SELECT * FROM events WHERE event_code = {event_code};' # Use %s for PostgreSQL, ? for SQLite 
    #print(query)
    #cursor.execute(query, (event_code,))  
    cursor.execute(query)    
    eventN = cursor.fetchall()  # Fetch all records from events table
    #print(eventN)
    event_name = eventN[0][1]

    if event: 
        return event.event_number,event_name
    
    # Calculate one week before and after 
    one_week_before = (event_date_obj - timedelta(weeks=1)).strftime('%d/%m/%Y') 
    one_week_after = (event_date_obj + timedelta(weeks=1)).strftime('%d/%m/%Y') 
    #print(one_week_before)
    #print(one_week_after)
    # Fetch events for one week before and one week after 
    before_event = db.session.execute( 
        text("SELECT * FROM parkrun_events WHERE event_code = :event_code AND event_date = :event_date"), 
        {"event_code": event_code, "event_date": one_week_before} ).fetchone() 
    after_event = db.session.execute( 
        text("SELECT * FROM parkrun_events WHERE event_code = :event_code AND event_date = :event_date"), 
        {"event_code": event_code, "event_date": one_week_after} ).fetchone()   
    #print(before_event)
    #print(after_event)
    #print(event_name)
    if before_event and after_event: 
        if before_event.event_number < 10000 and after_event.event_number == before_event.event_number + 2: 
            return before_event.event_number + 1,event_name
        
    return None 
with app.app_context(): 
    inspector = inspect(db.engine)
    tables = inspector.get_table_names()
    print("Tables in the database:", tables)
@app.route('/process_events', methods=['POST'])
def process_events():
    data = request.get_json() 
    event_code = data.get('event_code')    
    print(f"process_events -1 event_code = {event_code}")
    print(event_code)
    events = []
    # Direct SQL Query
    if event_code is not None:
        with db.engine.connect() as connection:
            result = connection.execute(text("SELECT * FROM parkrun_events WHERE event_code = :event_code"), {"event_code": event_code})
            events = [row._mapping for row in result]
            #print(f"Direct SQL Query Fetched events: {events}")
    #print(f"events = {events}")
    if not events:
        return jsonify({'error': 'No events found for the specified event code'}), 404
    # Fetch all records in the table for debugging
    #all_events = parkrun_events.query.all()
    #print(f"All events in the table: {all_events}")

    # Convert fetched events to a list of dictionaries for easier manipulation
    events_data = [{'event_date': event['event_date'], 'event_number': event['event_number']} for event in events]
    #print(f"process_events -2 event_data count: {len(events_data)}")
    #print(f"Events Data: {events_data}")

    # Sort events to ensure they are in the correct order
    events_data.sort(key=lambda x: datetime.strptime(x['event_date'], '%d/%m/%Y'))

    deleted_records = 0
    events_to_delete = set()  # Use a set to prevent duplicates
    print (f"length of events to delete: {events_to_delete}")
    for i in range(len(events_data)):
        current_event = events_data[i]
        #print(f"current_event_number {current_event['event_number']}")
        if current_event['event_number'] > 10000:
            # Case 1: Check if it has the earliest date
            print(f"test {i}, {events_data[i - 1]['event_number']}, {current_event['event_number']}, {events_data[i + 1]['event_number']},{current_event['event_date']}")
            #if i == 0 or datetime.strptime(current_event['event_date'], '%d/%m/%Y') < datetime.strptime(events_data[0]['event_date'], '%d/%m/%Y'):
            #    events_to_delete.add(tuple(current_event.items()))

            # Case 2: Check if the previous event number is correct
            if i > 0 and i < len(events_data) - 1 and int(events_data[i - 1]['event_number']) + 2 == int(events_data[i + 1]['event_number']):
                events_to_delete.add(tuple(current_event.items()))

    # Convert set back to list of dictionaries
    events_to_delete = [dict(event) for event in events_to_delete]
    print(f"Events to delete: {events_to_delete}")

    # Deleting the identified records
    for event in events_to_delete:
        ParkrunEvent.query.filter_by(event_number=event['event_number'], event_code=event_code).delete()
        deleted_records += 1

    db.session.commit()  # Submit the changes to the database

    return jsonify({'message': 'Processing complete', 'deleted_records': deleted_records}), 200
@app.route('/event-data', methods=['GET'])
def get_event_data():
    """API endpoint to fetch event data."""
    startDate = request.args.get('startDate', default=None, type=str)   
    if not startDate:
        return jsonify({"error": "startDate is required"}), 400  # Return an error if startDate is missing
    return fetch_event_data(startDate)  # Pass startDate to the fetch_event_data function
@app.route('/coeff-startDate', methods=['GET'])
def coeffStartDate():
    try:
        start_date = get_coeff_start_date()  # Call the function from analytics.py
        if start_date:
            return jsonify({"startDate": start_date}), 200
        else:
            return jsonify({"error": "No data found"}), 404
    except Exception as e:
        print(f"Error in API /coeff-startDate: {e}")
        return jsonify({"error": str(e)}), 500
@app.route('/transformed-event-data', methods=['POST'])
def transformed_event_data():
    """API endpoint to fetch and transform event data."""
    try:
        # Get the cutoff value from query parameters
 
        data = request.get_json()
        pivot = data.get('pivot')
        lowest_times = data.get('lowest_times')
        counts = data.get('counts')
        cutoff = data.get('cutoff')  # Get the cutoff value from the request body
        # Validate the input data
        if not pivot or not lowest_times or not counts:
            return jsonify({'error': 'Missing required data (pivot, lowest_times, counts)'}), 400
        # Transform the data with the cutoff
        result = get_transformed_event_data(pivot, lowest_times, counts, cutoff)

        # Return the transformed data as a JSON response
        return jsonify(result), 200
    except Exception as e:
        print(f"Error in /transformed-event-data: {e}")
        return jsonify({'error': str(e)}), 500
@app.route('/optimize-event-times', methods=['POST'])  # Updated to POST for sending larger data
def optimize_event_times():
    """API endpoint to optimize event times."""
    print("In here: optimize_event_times")
    try:
        # Get the JSON data sent from the client
        data = request.get_json()
        pivot_data = data['pivot']
        change_value = data['changeValue']
        event_date = data['eventDate']
        #event_code = data['eventCode']
        #coeff=data['coeff']
        counts = data['counts']

        #print("event_Date=",event_date)
        # Establish database connections
        conn, sqlite_cursor, render_db_conn, render_cursor = connections()
        # Call the logic function from analytics.py
        result = optimize_event_times_logic(pivot_data, change_value, event_date, counts, sqlite_cursor, render_cursor)
        normalize_coefficients(event_date)
        # Commit changes to both databases
        conn.commit()
        render_db_conn.commit()

        # Return the result as a JSON response
        return jsonify(result), 200
    except Exception as e:
        print(f"Error in /optimize-event-times: {e}")  # Log the error

        # Roll back changes in case of an error
        if 'conn' in locals():
            conn.rollback()
        if 'render_db_conn' in locals():
            render_db_conn.rollback()

        return jsonify({'error': str(e)}), 500

    finally:
        # Close database connections
        if 'conn' in locals():
            conn.close()
        if 'render_db_conn' in locals():
            render_db_conn.close()
@app.route('/get-coefficients', methods=['GET'])
def get_coefficients():
    """API endpoint to fetch coefficients for all event_codes and event_dates."""
    try:
        # Call the function from database.py
        coefficients = fetch_coefficients_for_all_events()
        return jsonify(coefficients), 200
    except Exception as e:
        print(f"Error in /get-coefficients: {e}")
        return jsonify({'error': str(e)}), 500
@app.route('/reset-coefficients', methods=['POST'])
def reset_coefficients():
    try:
        data = request.json
        event_date = data.get('eventDate')

        if not event_date:
            return jsonify({'error': 'Event date is required'}), 400

        # Reset coefficients in the database
        conn, cursor, _, _ = connections()
        cursor.execute('''
            UPDATE parkrun_events
            SET coeff = 1.0
            WHERE event_date = ?;
        ''', (event_date,))
        conn.commit()
        conn.close()

        return jsonify({'message': f'Coefficients for {event_date} reset to 1.0'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500
@app.route('/api/most_recent_date', methods=['GET'])
def most_recent_date():
    """
    API endpoint to get the most recent event_date where coeff is not equal to 1.
    """
    try:
        # Connect to the SQLite database
        conn, sqlite_cursor, _, _ = connections()

        # Call the function to get the most recent date
        most_recent_date = get_most_recent_date_with_coeff_not_one(sqlite_cursor)

        # Close the database connection
        conn.close()

        # Return the result as JSON
        if most_recent_date:
            return jsonify({"most_recent_date": most_recent_date}), 200
        else:
            return jsonify({"message": "No rows found where coeff <> 1"}), 404

    except Exception as e:
        # Handle errors and return a 500 response
        return jsonify({"error": str(e)}), 500
@app.route('/update-coeffs', methods=['POST'])
def update_coeffs():
    try:
        check_and_update_events() 
        return jsonify({'message': 'Coefficients updated successfully!'}), 200
    except Exception as e:
        print(f"Error in /update-coeffs: {e}")
        return jsonify({'error': str(e)}), 500
@app.route('/update-eligible', methods=['POST'])
def update_eligible():
    try:
        update_eligible_times_for_all_weeks()  # Pass True to update eligible times
        return jsonify({'message': 'Eligible times updated successfully!'}), 200
    except Exception as e:
        print(f"Error in /update-eligible: {e}")
        return jsonify({'error': str(e)}), 500
@app.route('/eventpositions', methods=['GET'])
def api_get_eventpositions():
    # Accept either event_code or event_name; event_date is required
    event_code = request.args.get('event_code', default=None, type=int)
    event_name = request.args.get('event_name', default=None, type=str)
    event_date = request.args.get('event_date', default=None, type=str)

    if not event_date:
        return jsonify({"error": "event_date is required"}), 400

    conn, cursor, render_db_conn, render_cursor = connections()
    try:
        # If event_name provided, look up the code
        if event_code is None and event_name:
            cursor.execute('SELECT event_code FROM events WHERE event_name = ? LIMIT 1', (event_name,))
            row = cursor.fetchone()
            if not row:
                return jsonify({"error": "event_name not found"}), 404
            event_code = row[0]

        if event_code is None:
            return jsonify({"error": "event_code or event_name is required"}), 400

        # Import locally to avoid modifying top-of-file imports if you prefer
        from database import get_single_section_sql

        # Render SQL for the section (this will substitute params safely using debug_sql_render_named)
        sql = get_single_section_sql(
            'get_eventpositions',
            filename='newSQL.sql',
            params={'event_code': event_code, 'event_date': event_date}
        )

        # Execute and return results
        cursor.execute(sql)
        rows = cursor.fetchall()
        cols = [d[0] for d in cursor.description] if cursor.description else []
        result = [dict(zip(cols, r)) for r in rows]
        return jsonify(result), 200

    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()    


@app.route('/api/clubs/members', methods=['GET'])
def get_club_members():
    club = (request.args.get('club') or '').strip()
    limit = request.args.get('limit', default=1000, type=int)
    limit = max(1, min(limit, 5000))

    if not club:
        return jsonify({'error': 'Missing required parameter: club'}), 400

    cache_exists = bool(db.session.execute(
        text("SELECT to_regclass('public.mv_club_members_cache') IS NOT NULL")
    ).scalar())

    if cache_exists:
        sql = text("""
            SELECT
                athlete_code,
                name,
                club_key,
                current_club,
                latest_age_group,
                club_runs_total,
                club_runs_last_year,
                first_club_run_date,
                last_club_run_date,
                fastest_time,
                fastest_time_seconds,
                best_event_adj_time,
                best_event_adj_time_seconds,
                best_age_event_adj_time,
                best_age_event_adj_time_seconds,
                best_age_sex_event_adj_time,
                best_age_sex_event_adj_time_seconds,
                CASE
                    WHEN COALESCE(club_runs_last_year, 0) > 0 THEN best_curve_ranking_current
                    ELSE NULL
                END AS best_curve_ranking_current,
                COALESCE(best_curve_ranking_historic, best_curve_ranking_current) AS best_curve_ranking_historic,
                best_curve_ranking_current_type,
                total_runs_all_clubs
            FROM mv_club_members_cache
            WHERE club_key = regexp_replace(LOWER(BTRIM(:club)), '\s+ac$', '')
            ORDER BY club_runs_total DESC, name
            LIMIT :limit
        """)
    else:
        sql = text("""
            WITH club_runs AS (
                SELECT
                    regexp_replace(LOWER(BTRIM(m.club)), '\s+ac$', '') AS club_key,
                    m.athlete_code,
                    COUNT(*)::int AS club_runs_total,
                    COUNT(*) FILTER (
                        WHERE m.event_dt >= CURRENT_DATE - INTERVAL '1 year'
                    )::int AS club_runs_last_year,
                    MIN(m.event_dt) AS first_club_run_date,
                    MAX(m.event_dt) AS last_club_run_date,
                    MIN(m.time_seconds) AS fastest_time_seconds,
                    (ARRAY_AGG(m.time ORDER BY m.time_seconds ASC NULLS LAST, m.event_dt DESC NULLS LAST))[1] AS fastest_time,
                    MIN(m.event_adj_time_seconds) AS best_event_adj_time_seconds,
                    (ARRAY_AGG(m.event_adj_time ORDER BY m.event_adj_time_seconds ASC NULLS LAST, m.event_dt DESC NULLS LAST))[1] AS best_event_adj_time,
                    MIN(m.age_event_adj_time_seconds) AS best_age_event_adj_time_seconds,
                    (ARRAY_AGG(m.age_event_adj_time ORDER BY m.age_event_adj_time_seconds ASC NULLS LAST, m.event_dt DESC NULLS LAST))[1] AS best_age_event_adj_time,
                    MIN(m.age_sex_event_adj_time_seconds) AS best_age_sex_event_adj_time_seconds,
                    (ARRAY_AGG(m.age_sex_event_adj_time ORDER BY m.age_sex_event_adj_time_seconds ASC NULLS LAST, m.event_dt DESC NULLS LAST))[1] AS best_age_sex_event_adj_time
                FROM mv_extend_runs m
                WHERE regexp_replace(LOWER(BTRIM(m.club)), '\s+ac$', '') = regexp_replace(LOWER(BTRIM(:club)), '\s+ac$', '')
                  AND m.club IS NOT NULL
                  AND BTRIM(m.club) <> ''
                  AND m.athlete_code IS NOT NULL
                  AND BTRIM(m.athlete_code) <> ''
                GROUP BY regexp_replace(LOWER(BTRIM(m.club)), '\s+ac$', ''), m.athlete_code
            ),
            latest_age AS (
                SELECT DISTINCT ON (m.athlete_code)
                    m.athlete_code,
                    m.age_group AS latest_age_group
                FROM mv_extend_runs m
                JOIN club_runs cr ON cr.athlete_code = m.athlete_code
                WHERE m.athlete_code IS NOT NULL
                  AND BTRIM(m.athlete_code) <> ''
                ORDER BY m.athlete_code, m.event_dt DESC NULLS LAST, m.event_code DESC
            ),
            latest_1y_row AS (
                SELECT DISTINCT ON (m.athlete_code)
                    m.athlete_code,
                    m.current_best_rank_b,
                    m.current_best_rank_e,
                    m.current_best_rank_ae,
                    m.current_best_rank_es,
                    m.current_best_rank_aes
                FROM mv_extend_runs m
                JOIN club_runs cr ON cr.athlete_code = m.athlete_code
                WHERE m.athlete_code IS NOT NULL
                  AND BTRIM(m.athlete_code) <> ''
                  AND m.event_dt >= CURRENT_DATE - INTERVAL '1 year'
                ORDER BY m.athlete_code, m.event_dt DESC NULLS LAST, m.event_code DESC
            ),
            latest_1y_rank_candidates AS (
                SELECT athlete_code, 'B'::text AS metric_type, current_best_rank_b::numeric AS rank, 1 AS metric_order
                FROM latest_1y_row
                WHERE current_best_rank_b IS NOT NULL

                UNION ALL

                SELECT athlete_code, 'E'::text AS metric_type, current_best_rank_e::numeric AS rank, 2 AS metric_order
                FROM latest_1y_row
                WHERE current_best_rank_e IS NOT NULL

                UNION ALL

                SELECT athlete_code, 'AE'::text AS metric_type, current_best_rank_ae::numeric AS rank, 3 AS metric_order
                FROM latest_1y_row
                WHERE current_best_rank_ae IS NOT NULL

                UNION ALL

                SELECT athlete_code, 'ES'::text AS metric_type, current_best_rank_es::numeric AS rank, 4 AS metric_order
                FROM latest_1y_row
                WHERE current_best_rank_es IS NOT NULL

                UNION ALL

                SELECT athlete_code, 'AES'::text AS metric_type, current_best_rank_aes::numeric AS rank, 5 AS metric_order
                FROM latest_1y_row
                WHERE current_best_rank_aes IS NOT NULL
            ),
            current_rank_best AS (
                SELECT
                    athlete_code,
                    metric_type,
                    rank,
                    ROW_NUMBER() OVER (
                        PARTITION BY athlete_code
                        ORDER BY rank DESC, metric_order ASC
                    ) AS rn
                FROM latest_1y_rank_candidates
            ),
            historic_rank AS (
                SELECT
                    m.athlete_code,
                    GREATEST(
                        COALESCE(MAX(m.current_best_rank_b)::numeric, 0),
                        COALESCE(MAX(m.current_best_rank_e)::numeric, 0),
                        COALESCE(MAX(m.current_best_rank_ae)::numeric, 0),
                        COALESCE(MAX(m.current_best_rank_es)::numeric, 0),
                        COALESCE(MAX(m.current_best_rank_aes)::numeric, 0)
                    ) AS best_curve_ranking_historic
                FROM mv_extend_runs m
                JOIN club_runs cr ON cr.athlete_code = m.athlete_code
                WHERE m.athlete_code IS NOT NULL
                  AND BTRIM(m.athlete_code) <> ''
                  AND (
                      m.current_best_rank_b IS NOT NULL
                      OR m.current_best_rank_e IS NOT NULL
                      OR m.current_best_rank_ae IS NOT NULL
                      OR m.current_best_rank_es IS NOT NULL
                      OR m.current_best_rank_aes IS NOT NULL
                  )
                GROUP BY m.athlete_code
            )
            SELECT
                cr.athlete_code,
                COALESCE(a.name, cr.athlete_code) AS name,
                cr.club_key,
                a.club AS current_club,
                la.latest_age_group,
                cr.club_runs_total,
                cr.club_runs_last_year,
                cr.first_club_run_date,
                cr.last_club_run_date,
                cr.fastest_time,
                cr.fastest_time_seconds,
                cr.best_event_adj_time,
                cr.best_event_adj_time_seconds,
                cr.best_age_event_adj_time,
                cr.best_age_event_adj_time_seconds,
                cr.best_age_sex_event_adj_time,
                cr.best_age_sex_event_adj_time_seconds,
                CASE WHEN cr.club_runs_last_year > 0 THEN crb.rank ELSE NULL::numeric END AS best_curve_ranking_current,
                COALESCE(hr.best_curve_ranking_historic, CASE WHEN cr.club_runs_last_year > 0 THEN crb.rank ELSE NULL::numeric END) AS best_curve_ranking_historic,
                CASE WHEN cr.club_runs_last_year > 0 THEN crb.metric_type ELSE NULL::text END AS best_curve_ranking_current_type,
                COALESCE(a.total_runs, 0) AS total_runs_all_clubs
            FROM club_runs cr
            LEFT JOIN athletes a ON a.athlete_code = cr.athlete_code
            LEFT JOIN latest_age la ON la.athlete_code = cr.athlete_code
            LEFT JOIN current_rank_best crb ON crb.athlete_code = cr.athlete_code AND crb.rn = 1
            LEFT JOIN historic_rank hr ON hr.athlete_code = cr.athlete_code
            ORDER BY cr.club_runs_total DESC, name
            LIMIT :limit
        """)

    result = db.session.execute(sql, {'club': club, 'limit': limit})
    rows = [dict(row) for row in result.fetchall()]
    return jsonify(rows), 200

if __name__ == '__main__':
#    for rule in app.url_map.iter_rules():
#        print(f"{rule.endpoint}: {rule}")
    conn, cursor, render_db_conn, *_ = connections()
    try:
        conn.create_function("time_to_seconds", 1, timeToSeconds)
        cursor.execute("SELECT time_to_seconds('1:23:45')")
        print(cursor.fetchone())
    finally:
        try:
            conn.close()
        except Exception:
            pass
        if render_db_conn:
            try:
                render_db_conn.close()
            except Exception:
                pass

    httpd = make_server(
        '127.0.0.1',
        5050,
        app,
        server_class=_ThreadingWSGIServer,
        handler_class=_QuietWSGIRequestHandler,
    )
    print('Serving backendAPI on http://127.0.0.1:5050')
    httpd.serve_forever()
