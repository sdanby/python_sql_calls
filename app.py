from flask import Flask, jsonify, request
from flask_cors import CORS  # Make sure to import CORS
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Date
from datetime import datetime, timezone
from sqlalchemy import func
from sqlalchemy import text # Import text from SQLAlchemy
from sqlalchemy import inspect
from lists_api import lists_bp, get_adjustment_fields_sql 
import traceback
import re
import os
import uuid
from google.oauth2 import id_token
from google.auth.transport import requests as google_requests
from werkzeug.security import generate_password_hash, check_password_hash
#from consistency import get_parkrun_data

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Replace the following credentials with your actual database credentials
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://parkrundata_user:m3UE0JWilwRNS1MBVgN2kr0BnIOVZUmH@dpg-cs2r25dsvqrc73dpgdd0-a.frankfurt-postgres.render.com:5432/parkrundata'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False


# Add these engine options to manage the connection pool
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
    'pool_size': 10,
    'pool_recycle': 280,
    'pool_pre_ping': True
}

db = SQLAlchemy(app)


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

# Add this block to automatically close sessions
@app.teardown_appcontext
def shutdown_session(exception=None):
    db.session.remove()

# 4. Register the blueprint with your main app
app.register_blueprint(lists_bp)

class ProcessingStatus(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    status = db.Column(db.String(10), nullable=False)
    updated_at = db.Column(db.DateTime, server_default=db.func.current_timestamp(), onupdate=db.func.current_timestamp())

class Event(db.Model):
    __tablename__ = 'events'
    event_code = db.Column(db.Integer, primary_key=True)
    event_name = db.Column(db.String, nullable=False)
    display_name = db.Column(db.String, nullable=False)

class EventPosition(db.Model):
    __tablename__ = 'eventpositions'
    event_code = db.Column(db.Integer, db.ForeignKey('events.event_code'), primary_key=True)
    event_date = db.Column(db.String, primary_key=True)
    position = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String)
    male_position = db.Column(db.Integer)
    male_count = db.Column(db.Integer)
    age_group = db.Column(db.String)
    age_grade = db.Column(db.String)
    time = db.Column(db.String)
    club = db.Column(db.String)
    comment = db.Column(db.String)
    athlete_code = db.Column(db.String)
    event_eligible_appearances = db.Column(db.Integer)
    time_ratio= db.Column(db.Float)
    adj_time_seconds= db.Column(db.Float)
    adj_time_ratio= db.Column(db.Float)
    event_code_count= db.Column(db.Integer)
    tourist_flag=db.Column(db.String)
    last_event_code_count= db.Column(db.Integer)
    age_ratio_male= db.Column(db.Float)
    age_ratio_sex= db.Column(db.Float)
    super_tourist= db.Column(db.Integer)
    local_time_ratio= db.Column(db.Float)
    adj2_time_seconds= db.Column(db.Float)
    adj2_time_ratio= db.Column(db.Float)
    distinct_courses_long= db.Column(db.Integer)
    last_event_code_count_long= db.Column(db.Integer)
    total_runs_long= db.Column(db.Integer)
    regular=db.Column(db.String)
    returner=db.Column(db.String)
    super_returner=db.Column(db.String)
    best_curve_ranking_current= db.Column(db.Integer)
    best_curve_ranking_historic= db.Column(db.Integer)
    best_curve_ranking_current_type= db.Column(db.String)
    event_rank_b = db.Column(db.Float)
    event_rank_e = db.Column(db.Float)
    event_rank_es = db.Column(db.Float)
    event_rank_ae = db.Column(db.Float)
    event_rank_aes = db.Column(db.Float)

class ParkrunEvent(db.Model):
    __tablename__ = 'parkrun_events'
    event_code = db.Column(db.Integer, primary_key=True)
    event_date = db.Column(db.String, nullable=False)  # Use String for storing date
    last_position = db.Column(db.Integer)
    volunteers = db.Column(db.Integer)
    event_number = db.Column(db.Integer,primary_key=True)
    coeff = db.Column(db.Float)
    obs = db.Column(db.Integer)
    coeff_event = db.Column(db.Float)
    avg_time = db.Column(db.Float)
    avgtimelim12 = db.Column(db.Float)
    avgtimelim5 = db.Column(db.Float)
    tourist_count = db.Column(db.Integer)
    super_tourist_count = db.Column(db.Integer)
    regulars = db.Column(db.Integer)
    avg_age = db.Column(db.Float)
    first_timers_count = db.Column(db.Integer)
    returners_count = db.Column(db.Integer)
    club_count = db.Column(db.Integer)
    pb_count = db.Column(db.Integer)
    recentbest_count = db.Column(db.Integer)
    eligible_time_count = db.Column(db.Integer)
    unknown_count = db.Column(db.Integer)
    super_returner_count= db.Column(db.Integer)

    def to_dict(self):
       return {
            'event_code': self.event_code,
            'event_date': self.event_date,  # Return the string directly
            'last_position': self.last_position,
            'volunteers': self.volunteers,
            'event_number' : self.event_number,
            'coeff' : self.coeff,
            'obs' : self.obs,
            'coeff_event' : self.coeff_event,
            'avg_time' : self.avg_time,            
            'avgtimelim12' : self.avgtimelim12,           
            'avgtimelim5' : self.avgtimelim5,
            'tourist_count' : self.tourist_count,
            'super_tourist_count' : self.tourist_count,
            'regulars' : self.regulars,
            'avg_age' : self.avg_age,
            'first_timers_count' : self.first_timers_count,
            'returners_count' : self.returners_count,
            'club_count' : self.club_count,
            'pb_count' : self.pb_count,
            'recentbest_count' : self.recentbest_count,
            'eligible_time_count' : self.eligible_time_count,
            'unknown_count' : self.unknown_count,
            'super_returner_count' : self.super_returner_count
        }


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
    is_admin = db.Column(db.Boolean, default=False, nullable=False)
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
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


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
    if 'is_admin' not in auth_user_columns:
        db.session.execute(text("ALTER TABLE auth_users ADD COLUMN is_admin BOOLEAN NOT NULL DEFAULT FALSE"))
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
    if 'updated_at' not in feedback_request_columns:
        db.session.execute(text("ALTER TABLE feedback_requests ADD COLUMN updated_at DATETIME"))
        db.session.execute(text("UPDATE feedback_requests SET updated_at = COALESCE(updated_at, created_at, CURRENT_TIMESTAMP)"))
        db.session.commit()


def _normalize_email(value):
    return (value or '').strip().lower()


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


def _user_payload(user):
    return {
        'id': user.id,
        'email': user.email,
        'displayName': user.display_name,
        'athleteCode': user.athlete_code,
        'defaultCourseCode': user.default_course_code,
        'defaultCourseName': user.default_course_name,
        'isAdmin': bool(user.is_admin),
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


def _admin_count():
    return int(AuthUser.query.filter_by(is_admin=True).count())


def _is_admin_bootstrap_open():
    return _admin_count() == 0


def _can_access_admin(user):
    if not user:
        return False
    return bool(user.is_admin) or _is_admin_bootstrap_open()


def _require_authenticated_user():
    session_token = _extract_bearer_token()
    _sess, user = _resolve_session(session_token)
    return _sess, user


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
        'lastUpdated': (row.updated_at or row.created_at or datetime.utcnow()).strftime('%Y-%m-%d'),
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
        'createdAt': row.created_at.isoformat() if row.created_at else None,
        'createdBy': _chat_creator_label(row),
        'athleteCode': str(row.athlete_code or '').strip() or None,
    }


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
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow()
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
    status_raw = str(payload.get('status') or 'updated').strip().lower()
    allowed_statuses = {'logged', 'updated', 'in-progress', 'prioritised', 'rejected', 'on-hold', 'completed', 'deleted'}

    if request_type_raw not in ('error', 'suggestion'):
        return jsonify({'error': 'type must be "error" or "suggestion"'}), 400
    if not title:
        return jsonify({'error': 'title is required'}), 400
    if not details:
        return jsonify({'error': 'details are required'}), 400
    if status_raw not in allowed_statuses:
        return jsonify({'error': 'status is invalid'}), 400

    row = FeedbackRequest.query.filter_by(id=request_id).first()
    if not row:
        return jsonify({'error': 'feedback request not found'}), 404

    if status_raw == 'deleted':
        db.session.delete(row)
        db.session.commit()
        return jsonify({'id': request_id, 'deleted': True}), 200

    row.request_type = request_type_raw
    row.title = title
    row.details = details
    row.status = status_raw
    row.updated_at = datetime.utcnow()
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


def _format_db_datetime(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)

#@app.route('/get_parkrun_data', methods=['GET']) 
#def get_parkrun_data_route(): 
#    return get_parkrun_data()

@app.route('/api/auth/config', methods=['GET'])
def auth_config():
    return jsonify({
        'googleClientId': os.getenv('GOOGLE_CLIENT_ID') or ''
    }), 200


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
    payload = request.get_json(silent=True) or {}
    credential = payload.get('credential') or payload.get('idToken')
    athlete_code = _resolve_athlete_code(payload.get('athleteCode'))
    default_course_code, default_course_name = _resolve_default_course(
        payload.get('defaultCourseCode'),
        payload.get('defaultCourseName')
    )
    if not credential:
        return jsonify({'error': 'Google credential is required'}), 400

    client_id = os.getenv('GOOGLE_CLIENT_ID')
    if not client_id:
        return jsonify({'error': 'GOOGLE_CLIENT_ID is not configured on backend'}), 500

    try:
        claims = id_token.verify_oauth2_token(
            credential,
            google_requests.Request(),
            client_id
        )
    except Exception as exc:
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

    session_token = _session_token()
    db.session.add(AuthSession(token=session_token, user_id=user.id, provider='google'))
    db.session.commit()
    _record_login_event(user.id, 'google', True)

    payload_user = _user_payload(user)
    payload_user['previousLoginAt'] = previous_last_login_at.isoformat() if previous_last_login_at else None
    return jsonify({
        'token': session_token,
        'user': payload_user
    }), 200


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


@app.route('/api/auth/me', methods=['GET'])
def auth_me():
    session_token = _extract_bearer_token()
    _sess, user = _resolve_session(session_token)
    if not user:
        return jsonify({'error': 'Unauthorized'}), 401
    return jsonify({'user': _user_payload(user)}), 200


@app.route('/api/admin/status', methods=['GET'])
def admin_status():
    _sess, user = _require_authenticated_user()
    if not user:
        return jsonify({'error': 'Unauthorized'}), 401

    admin_count = _admin_count()
    bootstrap_open = admin_count == 0
    can_access_admin = bool(user.is_admin) or bootstrap_open

    return jsonify({
        'adminCount': admin_count,
        'bootstrapOpen': bootstrap_open,
        'canAccessAdmin': can_access_admin,
        'user': _user_payload(user)
    }), 200


@app.route('/api/admin/users', methods=['GET'])
def admin_users_list():
    _sess, user = _require_authenticated_user()
    if not user:
        return jsonify({'error': 'Unauthorized'}), 401
    if not _can_access_admin(user):
        return jsonify({'error': 'Forbidden'}), 403

    rows = AuthUser.query.order_by(AuthUser.created_at.desc()).all()
    payload = []
    for row in rows:
        payload.append({
            'id': row.id,
            'email': row.email,
            'displayName': row.display_name,
            'athleteCode': row.athlete_code,
            'defaultCourseCode': row.default_course_code,
            'defaultCourseName': row.default_course_name,
            'isAdmin': bool(row.is_admin),
            'createdAt': _format_db_datetime(row.created_at),
            'lastLoginAt': _format_db_datetime(row.last_login_at)
        })

    return jsonify({
        'users': payload,
        'adminCount': _admin_count(),
        'bootstrapOpen': _is_admin_bootstrap_open()
    }), 200


@app.route('/api/admin/users/<int:user_id>/admin', methods=['POST'])
def admin_user_set_admin(user_id):
    _sess, user = _require_authenticated_user()
    if not user:
        return jsonify({'error': 'Unauthorized'}), 401
    if not _can_access_admin(user):
        return jsonify({'error': 'Forbidden'}), 403

    payload = request.get_json(silent=True) or {}
    is_admin = bool(payload.get('isAdmin', False))

    target = AuthUser.query.filter_by(id=user_id).first()
    if not target:
        return jsonify({'error': 'User not found'}), 404

    if not is_admin and bool(target.is_admin):
        admin_count_before = _admin_count()
        if admin_count_before <= 1:
            return jsonify({'error': 'At least one admin is required.'}), 400

    target.is_admin = is_admin
    db.session.commit()

    return jsonify({
        'ok': True,
        'user': {
            'id': target.id,
            'email': target.email,
            'displayName': target.display_name,
            'athleteCode': target.athlete_code,
            'defaultCourseCode': target.default_course_code,
            'defaultCourseName': target.default_course_name,
            'isAdmin': bool(target.is_admin),
            'createdAt': _format_db_datetime(target.created_at),
            'lastLoginAt': _format_db_datetime(target.last_login_at)
        },
        'adminCount': _admin_count(),
        'bootstrapOpen': _is_admin_bootstrap_open()
    }), 200


@app.route('/api/admin/users/<int:user_id>/default-course', methods=['POST'])
def admin_user_set_default_course(user_id):
    _sess, user = _require_authenticated_user()
    if not user:
        return jsonify({'error': 'Unauthorized'}), 401
    if not _can_access_admin(user):
        return jsonify({'error': 'Forbidden'}), 403

    payload = request.get_json(silent=True) or {}
    target = AuthUser.query.filter_by(id=user_id).first()
    if not target:
        return jsonify({'error': 'User not found'}), 404

    dc_code, dc_name = _resolve_default_course(payload.get('defaultCourseCode'), payload.get('defaultCourseName'))
    if not dc_code and not dc_name:
        return jsonify({'error': 'Course not found. Please check the course code or name.'}), 400
    target.default_course_code = dc_code
    target.default_course_name = dc_name
    db.session.commit()

    return jsonify({
        'ok': True,
        'user': {
            'id': target.id,
            'email': target.email,
            'displayName': target.display_name,
            'athleteCode': target.athlete_code,
            'defaultCourseCode': target.default_course_code,
            'defaultCourseName': target.default_course_name,
            'isAdmin': bool(target.is_admin),
            'createdAt': _format_db_datetime(target.created_at),
            'lastLoginAt': _format_db_datetime(target.last_login_at)
        }
    }), 200


@app.route('/api/admin/activity', methods=['GET'])
def admin_activity_list():
    _sess, user = _require_authenticated_user()
    if not user:
        return jsonify({'error': 'Unauthorized'}), 401
    if not _can_access_admin(user):
        return jsonify({'error': 'Forbidden'}), 403

    limit = request.args.get('limit', default=300, type=int)
    limit = max(20, min(limit, 5000))
    since_raw = request.args.get('since', default='', type=str)
    since_dt = _parse_dt(since_raw)

    sql = text("""
        WITH page_events AS (
            SELECT
                'page_visit'::text AS activity_type,
                p.created_at AS activity_at,
                au.id AS user_id,
                au.email AS email,
                au.display_name AS display_name,
                NULL::text AS provider,
                NULL::boolean AS success,
                p.page_path AS page_path,
                p.duration_ms AS duration_ms,
                p.referrer_path AS referrer_path,
                p.user_agent AS user_agent,
                NULL::text AS ip_address
            FROM page_usage_events p
            LEFT JOIN auth_sessions s ON s.token = p.session_token
            LEFT JOIN auth_users au ON au.id = s.user_id
        ),
        login_events AS (
            SELECT
                'login'::text AS activity_type,
                l.created_at AS activity_at,
                au.id AS user_id,
                au.email AS email,
                au.display_name AS display_name,
                l.provider AS provider,
                l.success AS success,
                NULL::text AS page_path,
                NULL::integer AS duration_ms,
                NULL::text AS referrer_path,
                l.user_agent AS user_agent,
                l.ip_address AS ip_address
            FROM auth_login_events l
            LEFT JOIN auth_users au ON au.id = l.user_id
        ),
        combined AS (
            SELECT * FROM page_events
            UNION ALL
            SELECT * FROM login_events
        )
        SELECT
            activity_type,
            activity_at,
            user_id,
            email,
            display_name,
            provider,
            success,
            page_path,
            duration_ms,
            referrer_path,
            user_agent,
            ip_address
        FROM combined
        WHERE (:since_dt IS NULL OR activity_at >= :since_dt)
        ORDER BY activity_at DESC NULLS LAST
        LIMIT :limit
    """)

    rows = db.session.execute(sql, {'limit': limit, 'since_dt': since_dt}).mappings().all()
    payload = []
    for row in rows:
        payload.append({
            'activityType': row.get('activity_type'),
            'activityAt': _format_db_datetime(row.get('activity_at')),
            'userId': row.get('user_id'),
            'email': row.get('email'),
            'displayName': row.get('display_name'),
            'provider': row.get('provider'),
            'success': row.get('success'),
            'pagePath': row.get('page_path'),
            'durationMs': row.get('duration_ms'),
            'referrerPath': row.get('referrer_path'),
            'userAgent': row.get('user_agent'),
            'ipAddress': row.get('ip_address')
        })

    return jsonify({'activity': payload, 'limit': limit}), 200


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


@app.route('/api/analytics/page-visit', methods=['POST', 'OPTIONS'])
def track_page_visit():
    if request.method == 'OPTIONS':
        return ('', 204)

    payload = request.get_json(silent=True) or {}
    token = payload.get('token')
    path = (payload.get('path') or '').strip()
    if not path:
        return jsonify({'error': 'path is required'}), 400

    duration_ms = payload.get('durationMs')
    try:
        duration_ms = int(duration_ms) if duration_ms is not None else None
    except Exception:
        duration_ms = None

    db.session.execute(text("""
        INSERT INTO page_usage_events
            (session_token, page_path, entered_at, left_at, duration_ms, referrer_path, user_agent, created_at)
        VALUES
            (:session_token, :page_path, :entered_at, :left_at, :duration_ms, :referrer_path, :user_agent, NOW())
    """), {
        'session_token': token,
        'page_path': path[:512],
        'entered_at': payload.get('enteredAt'),
        'left_at': payload.get('leftAt'),
        'duration_ms': duration_ms,
        'referrer_path': (payload.get('referrer') or '')[:512] or None,
        'user_agent': request.headers.get('User-Agent'),
    })
    db.session.commit()
    return jsonify({'ok': True}), 200

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
        db.session.commit() 
        return jsonify({'message': 'Duplicate rows deleted successfully', 'deleted_rows': result.rowcount}), 200 
    except Exception as e: 
        db.session.rollback() 
        return jsonify({'error': str(e)}), 500
    
@app.route('/api/eventpositions', methods=['GET'])
def get_event_positions():
    event_code = request.args.get('event_code', default=None, type=int)
    event_date = request.args.get('event_date', default=None, type=str)

    print(f"Received event_code: {event_code}, event_date: {event_date}")

    sql = text("""
    SELECT ep.*, a.total_runs
    FROM eventpositions ep
    LEFT JOIN athletes a ON a.athlete_code = ep.athlete_code
    WHERE (:event_code IS NULL OR ep.event_code = :event_code)
      AND (:event_date IS NULL OR ep.event_date = :event_date)
    ORDER BY ep.position
    """)

    params = {'event_code': event_code, 'event_date': event_date}
    result = db.session.execute(sql, params)

    rows = [dict(r) for r in result.fetchall()]

    return jsonify([{
        'event_code': r.get('event_code'),
        'event_date': r.get('event_date'),
        'position': r.get('position'),
        'name': r.get('name'),
        'male_position': r.get('male_position'),
        'male_count': r.get('male_count'),
        'age_group': r.get('age_group'),
        'age_grade': r.get('age_grade'),
        'time': r.get('time'),
        'club': r.get('club'),
        'comment': r.get('comment'),
        'athlete_code': r.get('athlete_code'),
        'event_eligible_appearances': r.get('event_eligible_appearances'),
        'time_ratio': r.get('time_ratio'),
        'adj_time_seconds': r.get('adj_time_seconds'),
        'adj_time_ratio': r.get('adj_time_ratio'),
        'event_code_count': r.get('event_code_count'),
        'tourist_flag': r.get('tourist_flag'),
        'last_event_code_count': r.get('last_event_code_count'),
        'age_ratio_male': r.get('age_ratio_male'),
        'age_ratio_sex': r.get('age_ratio_sex'),
        'super_tourist': r.get('super_tourist'),
        'local_time_ratio': r.get('local_time_ratio'),
        'adj2_time_seconds': r.get('adj2_time_seconds'),
        'adj2_time_ratio': r.get('adj2_time_ratio'),
        'distinct_courses_long': r.get('distinct_courses_long'),
        'last_event_code_count_long': r.get('last_event_code_count_long'),
        'total_runs_long': r.get('total_runs_long'),
        'regular': r.get('regular'),
        'returner': r.get('returner'),
        'super_returner': r.get('super_returner'),
        # new field from athletes table:
		'total_runs': r.get('total_runs'),
		'best_curve_ranking_current': r.get('best_curve_ranking_current'),
		'best_curve_ranking_historic': r.get('best_curve_ranking_historic'),
        'best_curve_ranking_current_type': r.get('best_curve_ranking_current_type'),
        'event_rank_b': r.get('event_rank_b'),
        'event_rank_e': r.get('event_rank_e'),
        'event_rank_es': r.get('event_rank_es'),
        'event_rank_ae': r.get('event_rank_ae'),
        'event_rank_aes': r.get('event_rank_aes')
    } for r in rows])

@app.route('/api/eventpositions', methods=['DELETE'])
def delete_event_positions():
    data = request.get_json()  # Get the JSON payload
    event_code = data.get('event_code')
    event_date = data.get('event_date')

    # Validate input
    if not event_code or not event_date:
        return jsonify({"error": "event_code and event_date are required"}), 400

    try:
        # Delete from eventpositions table
        rows_deleted = db.session.query(EventPosition).filter(
            EventPosition.event_code == event_code,
            EventPosition.event_date == event_date
        ).delete()
        
        db.session.commit()  # Commit changes to the database
        return jsonify({"message": f"{rows_deleted} record(s) deleted from eventpositions."}), 200

    except Exception as e:
        db.session.rollback()  # Rollback in case of error
        return jsonify({"error": str(e)}), 500


@app.route('/api/eventpositions/monthly-cascade', methods=['GET'])
def get_event_positions_monthly_cascade():
    event_code = request.args.get('event_code', default=None, type=int)

    if event_code is None:
        return jsonify({"error": "event_code is required"}), 400

    sql = text("""
    WITH base AS (
        SELECT
            ep.event_code,
            ep.event_date,
            COALESCE(ep.total_runs, a.total_runs, 0) AS total_runs,
            COALESCE(ep.age_group, '') AS age_group,
            COALESCE(ep.comment, '') AS comment,
            COALESCE(CAST(ep.super_tourist AS TEXT), '') AS super_tourist,
            COALESCE(ep.tourist_flag, '') AS tourist_flag,
            COALESCE(ep.returner, '') AS returner,
            COALESCE(ep.super_returner, '') AS super_returner,
            COALESCE(ep.regular, '') AS regular,
            COALESCE(ep.last_event_code_count_long, 0) AS last_event_code_count_long,
            COALESCE(pe.last_position, 0) AS last_position,
            to_date(ep.event_date, 'DD/MM/YYYY') AS event_dt
        FROM eventpositions ep
        LEFT JOIN athletes a ON a.athlete_code = ep.athlete_code
        LEFT JOIN parkrun_events pe ON pe.event_code = ep.event_code AND pe.event_date = ep.event_date
        WHERE ep.event_code = :event_code
          AND (pe.event_number IS NULL OR pe.event_number <= 10000)
    ),
    classified AS (
        SELECT
            event_date,
            EXTRACT(MONTH FROM event_dt)::int AS month_idx,
            last_position,
            UPPER(BTRIM(age_group)) AS age_group_norm,
            COALESCE(NULLIF(substring(UPPER(BTRIM(age_group)) from '([0-9]+)'), ''), '0')::int AS age_start,
            CASE
                WHEN total_runs = 1 THEN 'g1_first_first_timer'
                WHEN comment = 'First Timer!' THEN 'g2_first_timer_comment'
                WHEN super_tourist IN ('1', 'T', 'True', 'true') THEN 'g3_super_tourist'
                WHEN tourist_flag = 'T' THEN 'g4_tourist'
                WHEN returner = 'T' OR super_returner = 'T' THEN 'g5_returner_or_super_returner'
                WHEN regular = 'T' THEN 'g6_super_regular'
                WHEN last_event_code_count_long > 10 THEN 'g7_last_event_code_count_long_gt10'
                ELSE 'g8_rest'
            END AS grp,
            CASE
                WHEN UPPER(BTRIM(age_group)) LIKE 'JM%' THEN 'a1_younger_men'
                WHEN UPPER(BTRIM(age_group)) LIKE 'YM%' THEN 'a2_adult_men'
                WHEN UPPER(BTRIM(age_group)) LIKE 'AM%' THEN 'a3_senior_men'
                WHEN UPPER(BTRIM(age_group)) LIKE 'SM%' THEN 'a2_adult_men'
                WHEN UPPER(BTRIM(age_group)) LIKE 'VM%' THEN
                    CASE
                        WHEN COALESCE(NULLIF(substring(UPPER(BTRIM(age_group)) from '([0-9]+)'), ''), '0')::int >= 65 THEN 'a5_super_vet_men'
                        WHEN COALESCE(NULLIF(substring(UPPER(BTRIM(age_group)) from '([0-9]+)'), ''), '0')::int >= 50 THEN 'a4_veteran_men'
                        ELSE 'a3_senior_men'
                    END
                WHEN UPPER(BTRIM(age_group)) LIKE 'JW%' THEN 'a6_younger_women'
                WHEN UPPER(BTRIM(age_group)) LIKE 'YW%' THEN 'a7_adult_women'
                WHEN UPPER(BTRIM(age_group)) LIKE 'AW%' THEN 'a8_senior_women'
                WHEN UPPER(BTRIM(age_group)) LIKE 'SW%' THEN 'a7_adult_women'
                WHEN UPPER(BTRIM(age_group)) LIKE 'VW%' THEN
                    CASE
                        WHEN COALESCE(NULLIF(substring(UPPER(BTRIM(age_group)) from '([0-9]+)'), ''), '0')::int >= 65 THEN 'a10_super_vet_women'
                        WHEN COALESCE(NULLIF(substring(UPPER(BTRIM(age_group)) from '([0-9]+)'), ''), '0')::int >= 50 THEN 'a9_veteran_women'
                        ELSE 'a8_senior_women'
                    END
                ELSE 'a11_unclassified'
            END AS age_super_grp
        FROM base
        WHERE event_dt IS NOT NULL
    ),
    per_event AS (
        SELECT
            event_date,
            month_idx,
            GREATEST(MAX(last_position) - COUNT(*), 0)::float AS unknown_count,
            SUM(CASE WHEN grp = 'g1_first_first_timer' THEN 1 ELSE 0 END) AS g1,
            SUM(CASE WHEN grp = 'g2_first_timer_comment' THEN 1 ELSE 0 END) AS g2,
            SUM(CASE WHEN grp = 'g3_super_tourist' THEN 1 ELSE 0 END) AS g3,
            SUM(CASE WHEN grp = 'g4_tourist' THEN 1 ELSE 0 END) AS g4,
            SUM(CASE WHEN grp = 'g5_returner_or_super_returner' THEN 1 ELSE 0 END) AS g5,
            SUM(CASE WHEN grp = 'g6_super_regular' THEN 1 ELSE 0 END) AS g6,
            SUM(CASE WHEN grp = 'g7_last_event_code_count_long_gt10' THEN 1 ELSE 0 END) AS g7,
            SUM(CASE WHEN grp = 'g8_rest' THEN 1 ELSE 0 END) AS g8,
            SUM(CASE WHEN age_super_grp = 'a1_younger_men' THEN 1 ELSE 0 END) AS a1,
            SUM(CASE WHEN age_super_grp = 'a2_adult_men' THEN 1 ELSE 0 END) AS a2,
            SUM(CASE WHEN age_super_grp = 'a3_senior_men' THEN 1 ELSE 0 END) AS a3,
            SUM(CASE WHEN age_super_grp = 'a4_veteran_men' THEN 1 ELSE 0 END) AS a4,
            SUM(CASE WHEN age_super_grp = 'a5_super_vet_men' THEN 1 ELSE 0 END) AS a5,
            SUM(CASE WHEN age_super_grp = 'a6_younger_women' THEN 1 ELSE 0 END) AS a6,
            SUM(CASE WHEN age_super_grp = 'a7_adult_women' THEN 1 ELSE 0 END) AS a7,
            SUM(CASE WHEN age_super_grp = 'a8_senior_women' THEN 1 ELSE 0 END) AS a8,
            SUM(CASE WHEN age_super_grp = 'a9_veteran_women' THEN 1 ELSE 0 END) AS a9,
            SUM(CASE WHEN age_super_grp = 'a10_super_vet_women' THEN 1 ELSE 0 END) AS a10,
            SUM(CASE WHEN age_super_grp = 'a11_unclassified' THEN 1 ELSE 0 END) AS a11
        FROM classified
        GROUP BY event_date, month_idx
    ),
    per_month AS (
        SELECT
            month_idx,
            COUNT(*)::int AS events_in_month,
            AVG(unknown_count)::float AS unknown_avg,
            AVG(g1)::float AS g1_avg,
            AVG(g2)::float AS g2_avg,
            AVG(g3)::float AS g3_avg,
            AVG(g4)::float AS g4_avg,
            AVG(g5)::float AS g5_avg,
            AVG(g6)::float AS g6_avg,
            AVG(g7)::float AS g7_avg,
            AVG(g8)::float AS g8_avg,
            AVG(a1)::float AS a1_avg,
            AVG(a2)::float AS a2_avg,
            AVG(a3)::float AS a3_avg,
            AVG(a4)::float AS a4_avg,
            AVG(a5)::float AS a5_avg,
            AVG(a6)::float AS a6_avg,
            AVG(a7)::float AS a7_avg,
            AVG(a8)::float AS a8_avg,
            AVG(a9)::float AS a9_avg,
            AVG(a10)::float AS a10_avg,
            AVG(a11)::float AS a11_avg
        FROM per_event
        GROUP BY month_idx
    ),
    months AS (
        SELECT generate_series(1, 12) AS month_idx
    )
    SELECT
        m.month_idx,
        to_char(make_date(2000, m.month_idx, 1), 'Mon') AS month_label,
        COALESCE(pm.events_in_month, 0) AS events_in_month,
        COALESCE(pm.unknown_avg, 0) AS unknown_avg,
        COALESCE(pm.g1_avg, 0) AS first_first_timer_avg,
        COALESCE(pm.g2_avg, 0) AS first_timer_comment_avg,
        COALESCE(pm.g3_avg, 0) AS super_tourist_avg,
        COALESCE(pm.g4_avg, 0) AS tourist_avg,
        COALESCE(pm.g5_avg, 0) AS returner_or_super_returner_avg,
        COALESCE(pm.g6_avg, 0) AS super_regular_avg,
        COALESCE(pm.g7_avg, 0) AS regular_avg,
        COALESCE(pm.g7_avg, 0) AS last_event_code_count_long_gt10_avg,
        COALESCE(pm.g8_avg, 0) AS rest_avg,
        COALESCE(pm.a1_avg, 0) AS younger_men_avg,
        COALESCE(pm.a2_avg, 0) AS adult_men_avg,
        COALESCE(pm.a3_avg, 0) AS senior_men_avg,
        COALESCE(pm.a4_avg, 0) AS veteran_men_avg,
        COALESCE(pm.a5_avg, 0) AS super_vet_men_avg,
        COALESCE(pm.a6_avg, 0) AS younger_women_avg,
        COALESCE(pm.a7_avg, 0) AS adult_women_avg,
        COALESCE(pm.a8_avg, 0) AS senior_women_avg,
        COALESCE(pm.a9_avg, 0) AS veteran_women_avg,
        COALESCE(pm.a10_avg, 0) AS super_vet_women_avg,
        COALESCE(pm.a11_avg, 0) AS unclassified_avg
    FROM months m
    LEFT JOIN per_month pm ON pm.month_idx = m.month_idx
    ORDER BY m.month_idx;
    """)

    try:
        result = db.session.execute(sql, {'event_code': event_code})
        rows = [dict(r) for r in result.fetchall()]
        return jsonify(rows), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/eventTimeAdjustment', methods=['GET'])
def get_event_time_adjustment():
    event_code = request.args.get('event_code', default=None, type=int)
    event_date = request.args.get('event_date', default=None, type=str)

    print(f"Received event_code: {event_code}, event_date: {event_date}")

    sql = text(f"""
    WITH tmp_time_adjustment AS (
        SELECT 
            e.event_date,
            e.event_code,
            time,
            athlete_code,
            age_ratio_male,
            age_ratio_sex,
            substring(e.event_date, 7, 4) || chr(45) || substring(e.event_date, 4, 2) || chr(45) || substring(e.event_date, 1, 2) AS formatted_date,
            CASE
                WHEN length(time) - length(replace(time, ':', '')) = 2 THEN
                    CAST(substring(time, 1, strpos(time, ':') - 1) AS INTEGER) * 3600 +
                    CAST(substring(time, strpos(time, ':') + 1, strpos(substring(time, strpos(time, ':') + 1), ':') - 1) AS INTEGER) * 60 +
                    CAST(substring(time, length(time) - 1, 2) AS INTEGER)
                ELSE
                    CAST(substring(time, 1, strpos(time, ':') - 1) AS INTEGER) * 60 +
                    CAST(substring(time, strpos(time, ':') + 1) AS INTEGER)
            END AS time_seconds,
            adj_time_seconds,
            adj2_time_seconds,
            coeff,
            coeff_event
        FROM eventpositions e
        JOIN parkrun_events p ON e.event_code = p.event_code AND e.event_date = p.event_date
        WHERE (:event_code IS NULL OR e.event_code = :event_code)
          AND (:event_date IS NULL OR e.event_date = :event_date)
    )
    SELECT 
        formatted_date,
        event_code,
        athlete_code,
        coeff AS season_adj,
        coeff + coeff_event - 1 AS event_adj,
        age_ratio_male AS age_adj,
        age_ratio_sex / age_ratio_male AS sex_adj,
        time,
        {get_adjustment_fields_sql()}
    FROM tmp_time_adjustment
    ORDER BY age_event_adj_time
    """)
    
    # accept either min_sec or min_seconds query param; default 12:49 -> 769 seconds
    min_param = request.args.get('min_sec')
    if min_param is None:
        min_param = request.args.get('min_seconds')
    try:
        min_seconds = int(min_param) if min_param is not None else 12 * 60 + 49
    except (TypeError, ValueError):
        min_seconds = 12 * 60 + 49
        
    params = {'event_code': event_code, 'event_date': event_date, 'min_sec': min_seconds}
    result = db.session.execute(sql, params)

    rows = [dict(r) for r in result.fetchall()]

    return jsonify(rows)


@app.route('/api/parkrun_events', methods=['GET'])
def get_parkrun_events():
    event_code = request.args.get('event_code', default=None, type=int)
    
    if event_code is not None:
        try:
            event_code = int(event_code)  # Ensure it's an integer
        except ValueError:
            return jsonify({"error": "Invalid event_code"}), 400

    events = ParkrunEvent.query.filter_by(event_code=event_code).all()
    print(f"Filtered event_code: {event_code}, Found {len(events)} events.")  # Debugging line
    
    formatted_events = [event.to_dict() for event in events]
    return jsonify(formatted_events)

@app.route('/api/parkrun_event', methods=['GET'])
def get_parkrun_event():
    # Retrieve event_code, event_date, and event_number from query parameters
    event_code = request.args.get('event_code', default=None, type=int)  # Get event_code
    event_date = request.args.get('event_date', default=None, type=str)  # Get event_date
    event_number = request.args.get('event_number', default=None, type=int)  # Get event_number

    # Validate input
    if event_code is None:
        return jsonify({"error": "event_code is required"}), 400

    if event_date is None and event_number is None:
        return jsonify({"error": "Either event_date or event_number is required"}), 400
    try:
        # Fetch the specific event based on event_code and event_date or event_number
        if event_number is not None:
            event_record = ParkrunEvent.query.filter_by(event_code=event_code, event_number=event_number).first()
        else:
            #event_record = ParkrunEvent.query.filter_by(event_code=event_code, event_date=formatted_event_date).first()
            event_record = ParkrunEvent.query.filter_by(event_code=event_code, event_date=event_date).first()

        if event_record:
            return jsonify(event_record.to_dict()), 200  # Return the found record
            
        else:
            return jsonify({"error": "Event not found for the given code and date/number."}), 404

    except Exception as e:
        print(f"Error occurred: {e}")
        return jsonify({"error": str(e)}), 500


from flask import jsonify, request
from sqlalchemy import func

@app.route('/api/last_positions', methods=['GET'])
def get_last_positions():
    event_code = request.args.get('event_code', default=None, type=int)

    if event_code is None:
        return jsonify({"error": "event_code is required"}), 400

    # Query to get last positions for the specified event_code
    try:
        last_positions_query = (
            db.session.query(
                EventPosition.event_code,
                EventPosition.event_date,  # Get the event date directly
                func.max(EventPosition.position).label('last_position')  # Find the last position for the week
            )
            .filter(EventPosition.event_code == event_code)  # Filter by the given event_code
            .group_by(
                EventPosition.event_code,
                EventPosition.event_date  # Group by event code and event date
            )
            .all()
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    if not last_positions_query:
        return jsonify({"message": "No records found for this event code"}), 404

    # Helper function to convert dd/mm/yyyy to ISO format (yyyy-mm-dd)
    def format_date_to_iso(date_str):
        try:
            if isinstance(date_str, str) and '/' in date_str:
                # Assume dd/mm/yyyy format
                parts = date_str.split('/')
                if len(parts) == 3:
                    day, month, year = parts
                    return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
            return str(date_str)  # Return as-is if not in expected format
        except:
            return str(date_str)  # Return as-is if conversion fails

    # Prepare the response with both original and formatted dates
    last_positions = [{
        'event_code': code,
        'event_date': date,  # Use event_date directly (original dd/mm/yyyy format)
        'formatted_date': format_date_to_iso(str(date)),  # ISO format (yyyy-mm-dd) for proper sorting
        'last_position': last_position
    } for code, date, last_position in last_positions_query]

    return jsonify(last_positions)  # Return the retrieved last positions as JSON

@app.route('/api/parkrun_events', methods=['DELETE'])
def delete_parkrun_events():
    data = request.get_json()  # Get the JSON payload
    event_code = data.get('event_code')
    event_date = data.get('event_date')

    # Validate input
    if not event_code or not event_date:
        return jsonify({"error": "event_code and event_date are required"}), 400

    try:
        # Delete from parkrun_events table
        rows_deleted = db.session.query(ParkrunEvent).filter(
            ParkrunEvent.event_code == event_code,
            ParkrunEvent.event_date == event_date
        ).delete()

        db.session.commit()  # Commit changes to the database
        return jsonify({"message": f"{rows_deleted} record(s) deleted from parkrun_events."}), 200

    except Exception as e:
        db.session.rollback()  # Rollback in case of error
        return jsonify({"error": str(e)}), 500

@app.route('/process_events', methods=['POST'])
def process_events():
    data = request.get_json() 
    if data is None: 
        return jsonify({'error': 'No JSON payload received'}), 400 
    event_code = data.get('event_code') 
    if event_code is None: 
        return jsonify({'error': 'event_code not provided'}), 400
    print(f"process_events -1 event_code = {event_code}")
    print(event_code)
    events = [] 
    if event_code is not None: 
        with db.engine.connect() as connection: 
            result = connection.execute(text("SELECT * FROM parkrun_events WHERE event_code = :event_code"), {"event_code": event_code}) 
            events = [dict(row.items()) for row in result.mappings()] 
            #print(f"Direct SQL Query Fetched events: {events}"
    if not events:
        return jsonify({'error': 'No events found for the specified event code'}), 404

    # Convert fetched events to a list of dictionaries for easier manipulation
    events_data = [{'event_date': event['event_date'], 'event_number': event['event_number']} for event in events]


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

@app.route('/api/events', methods=['GET'])
def get_events():
    events = Event.query.all()  # Fetching event names and codes
    return jsonify([{
        'event_code': e.event_code,
        'event_name': e.event_name
    } for e in events])  # Return as JSON


@app.route('/api/status', methods=['GET'])
def get_status():
    """Get the current processing status."""
    status_entry = ProcessingStatus.query.first()
    if status_entry:
        return jsonify({'status': status_entry.status}), 200
    else:
        return jsonify({'status': 'not set'}), 404

@app.route('/api/start', methods=['POST'])
def start_processing():
    """Set the processing status to 'running'."""
    status_entry = ProcessingStatus.query.first()
    
    if status_entry:
        status_entry.status = 'running'
    else:
        status_entry = ProcessingStatus(status='running')
        db.session.add(status_entry)
    
    db.session.commit()
    return jsonify({'status': 'started'}), 200

@app.route('/api/stop', methods=['POST'])
def stop_processing():
    """Set the processing status to 'stopped'."""
    status_entry = ProcessingStatus.query.first()
    
    if status_entry:
        status_entry.status = 'stopped'
        db.session.commit()
        return jsonify({'status': 'stopped'}), 200
    else:
        return jsonify({'status': 'not set'}), 404   

@app.route('/')
def hello():
    return 'How quickly will this update the front-end?'

@app.route('/build', methods=['GET'])
def create_tables():
    db.create_all()  # 

@app.route('/api/event-data', methods=['GET'])
def fetch_event_data():
    """Fetch event data from the database and return it as JSON."""
    try:
        query = text('''
            WITH first_15_dates AS (
                SELECT DISTINCT event_date
                FROM parkrun_events
                ORDER BY to_date(event_date, 'DD/MM/YYYY')
                LIMIT 15
            )
            SELECT event_code, event_date, time, athlete_code
            FROM eventpositions
            WHERE event_date IN (SELECT event_date FROM first_15_dates)
            ORDER BY athlete_code;
        ''')

        rows = db.session.execute(query).mappings().all()
        result = [dict(row) for row in rows]

        return jsonify(result), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/results', methods=['GET'])
def get_results():
    """Get most recent results or all results from a supplied start date (inclusive)."""
    try:
        print("Fetching results from the database...")

        # Get limit from query params, default to 15, clamp to max 100
        limit = request.args.get('limit', default=15, type=int)
        limit = max(1, min(limit, 100))  # Prevent abuse

        # Optional start date (YYYY-MM-DD). If provided we'll return all records from that date (inclusive).
        start_date = request.args.get('date', default=None, type=str)
        params = {'limit': limit}

        if start_date:
            # validate format
            try:
                datetime.strptime(start_date, '%Y-%m-%d')
            except ValueError:
                return jsonify({'error': 'date must be YYYY-MM-DD'}), 400

            query = """
                SELECT 
                  fe.event_code,
                  e.event_name,
                  fe.event_date,
                  fe.last_position,
                  fe.volunteers,
                  fe.event_number,
                  fe.coeff,
                  fe.obs,
                  fe.coeff_event,
                  fe.avg_time,
                  fe.avgtimelim12,
                  fe.avgtimelim5,
                  fe.tourist_count,
                  fe.super_tourist_count,
                  fe.regulars,
                  fe.avg_age,
                  fe.first_timers_count,
                  fe.returners_count,
                  fe.club_count,
                  fe.pb_count,
                  fe.recentbest_count,
                  fe.eligible_time_count,
                  fe.unknown_count,
                  fe.super_returner_count
                FROM (
                  SELECT *,
                         substr(event_date, 7, 4) || '-' || substr(event_date, 4, 2) || '-' || substr(event_date, 1, 2) AS formatted_date
                  FROM parkrun_events
                ) fe
                JOIN events e ON fe.event_code = e.event_code
                WHERE fe.formatted_date >= :start_date
                ORDER BY fe.formatted_date DESC, fe.event_code;
            """
            params['start_date'] = start_date
        else:
            # original latest-n-dates query
            query = """
                WITH formatted_events AS (
                  SELECT *,
                         substr(event_date, 7, 4) || '-' || substr(event_date, 4, 2) || '-' || substr(event_date, 1, 2) AS formatted_date
                  FROM parkrun_events
                ),
                
                latest_dates AS (
                  SELECT DISTINCT formatted_date
                  FROM formatted_events
                  ORDER BY formatted_date DESC
                  LIMIT :limit
                )
                
                SELECT 
                  fe.event_code,
                  e.event_name,
                  fe.event_date,
                  fe.last_position,
                  fe.volunteers,
                  fe.event_number,
                  fe.coeff,
                  fe.obs,
                  fe.coeff_event,
                  fe.avg_time,
                  fe.avgtimelim12,
                  fe.avgtimelim5,
                  fe.tourist_count,
                  fe.super_tourist_count,
                  fe.regulars,
                  fe.avg_age,
                  fe.first_timers_count,
                  fe.returners_count,
                  fe.club_count,
                  fe.pb_count,
                  fe.recentbest_count,
                  fe.eligible_time_count,
                  fe.unknown_count,
                  fe.super_returner_count
                FROM formatted_events fe
                JOIN events e ON fe.event_code = e.event_code
                WHERE fe.formatted_date IN (SELECT formatted_date FROM latest_dates)
                ORDER BY fe.formatted_date DESC, fe.event_code;
            """

        result_proxy = db.session.execute(query, params)
        rows = result_proxy.fetchall()
        columns = result_proxy.keys()
        result = [dict(zip(columns, row)) for row in rows]

        print(f"Fetched {len(result)} results from the database.")
        return jsonify(result), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/resultsAll', methods=['GET'])
def get_resultsAll():
    """Get most recent results."""
    try:
        print("Fetching results from the database...")

        query = """
            WITH formatted_events AS (
              SELECT *,
                     substr(event_date, 7, 4) || '-' || substr(event_date, 4, 2) || '-' || substr(event_date, 1, 2) AS formatted_date
              FROM parkrun_events
            )
            
            SELECT 
              fe.event_code,
              e.event_name,
              fe.event_date,
              fe.last_position,
              fe.volunteers,
              fe.event_number,
              fe.coeff,
              fe.obs,
              fe.coeff_event,
              fe.avg_time,
              fe.avgtimelim12,
              fe.avgtimelim5,
              fe.tourist_count,
              fe.super_tourist_count,
              fe.regulars,
              fe.avg_age,
              fe.first_timers_count,
              fe.returners_count,
              fe.club_count,
              fe.pb_count,
              fe.recentbest_count,
              fe.eligible_time_count,
              fe.unknown_count,
              fe.super_returner_count
            FROM formatted_events fe
            JOIN events e ON fe.event_code = e.event_code
            ORDER BY fe.formatted_date DESC, fe.event_code;
        """

        result_proxy = db.session.execute(query)
        rows = result_proxy.fetchall()
        columns = result_proxy.keys()
        result = [dict(zip(columns, row)) for row in rows]

        print(f"Fetched {len(result)} results from the database.")
        return jsonify(result), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/eventinfo', methods=['GET'])
def get_event_info():
    event_number = request.args.get('event_number', type=int)
    event_code = request.args.get('event_code', type=int)
    # accept both event_name and display_name param names
    event_name = request.args.get('event_name', type=str) or request.args.get('display_name', type=str)
    event_date = request.args.get('event_date', type=str)

    if not event_date or (event_number is None and event_code is None and not event_name):
        return jsonify({"error": "Provide event_date and one of event_number, event_code or event_name"}), 400

    # Build date variants to try (keep original first)
    dates_to_try = [event_date]
    try:
        if re.match(r'^\d{4}-\d{2}-\d{2}$', event_date):
            y, m, d = event_date.split('-')
            alt = f"{d}/{m}/{y}"
            if alt not in dates_to_try:
                dates_to_try.append(alt)
        if re.match(r'^\d{2}/\d{2}/\d{4}$', event_date):
            d, m, y = event_date.split('/')
            alt2 = f"{y}-{m}-{d}"
            if alt2 not in dates_to_try:
                dates_to_try.append(alt2)
    except Exception:
        pass

    try:
        # Base query joins parkrun_events with events to get display_name
        q = db.session.query(ParkrunEvent, Event).join(Event, ParkrunEvent.event_code == Event.event_code)

        record = None
        if event_number is not None:
            record = q.filter(ParkrunEvent.event_number == event_number, ParkrunEvent.event_date.in_(dates_to_try)).first()

        if record is None and event_code is not None:
            record = q.filter(ParkrunEvent.event_code == event_code, ParkrunEvent.event_date.in_(dates_to_try)).first()

        if record is None and event_name:
            # case-insensitive match on event_name
            record = q.filter(func.lower(Event.event_name) == func.lower(event_name), ParkrunEvent.event_date.in_(dates_to_try)).first()

        if not record:
            return jsonify({"error": "Event not found"}), 404

        pe, ev = record  # tuple: (ParkrunEvent, Event)
        display = ev.display_name or ev.event_name
        return jsonify({
            "event_number": pe.event_number,
            "event_name": display,
            "event_code": pe.event_code
        }), 200

    except Exception as e:
        app.logger.exception("get_event_info error")
        return jsonify({"error": str(e)}), 500

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
        # Use SQLAlchemy to query parkrun_events joined to events for display_name
        q = db.session.query(ParkrunEvent, Event).join(Event, ParkrunEvent.event_code == Event.event_code)
        rec = q.filter(ParkrunEvent.event_code == event_code, ParkrunEvent.event_number == event_number).first()

        if not rec:
            return jsonify({"error": "Event not found"}), 404

        pe, ev = rec  # ParkrunEvent, Event
        display_name = ev.display_name or ev.event_name if ev is not None else None

        return jsonify({
            'event_code': pe.event_code,
            'event_number': pe.event_number,
            'event_date': pe.event_date,
            'event_name': display_name
        }), 200

    except Exception as e:
        app.logger.exception("get_event_by_number error")
        return jsonify({"error": str(e)}), 500

@app.route('/api/curve-rank-reference', methods=['GET'])
def get_curve_rank_reference():
    rank_type = (request.args.get('rank_type') or 'B').strip().upper()
    allowed_rank_types = {'B', 'E', 'ES', 'AE', 'AES'}
    if rank_type not in allowed_rank_types:
        rank_type = 'B'

    requested_reference_version = (request.args.get('reference_version') or '').strip()

    def _seconds_to_time_label(value):
        if value is None:
            return None
        total_seconds = max(0, int(round(float(value))))
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60
        if hours > 0:
            return f"{hours}:{minutes:02d}:{seconds:02d}"
        return f"{minutes}:{seconds:02d}"

    def _load_reference_from_postgres_table():
        table_exists = bool(
            db.session.execute(text("SELECT to_regclass('public.curve_time_ranks_reference') IS NOT NULL")).scalar()
        )
        if not table_exists:
            return None

        latest_version_sql = text("""
            SELECT MAX(curve_rank_reference_version) AS curve_rank_reference_version
            FROM curve_time_ranks_reference
            WHERE metric_type IN ('B', 'E', 'ES', 'AE', 'AES')
        """)
        latest_version = db.session.execute(latest_version_sql).scalar()
        if latest_version is None:
            return None

        versions_sql = text("""
            SELECT DISTINCT curve_rank_reference_version
            FROM curve_time_ranks_reference
            WHERE metric_type IN ('B', 'E', 'ES', 'AE', 'AES')
            ORDER BY curve_rank_reference_version DESC
        """)
        version_rows = db.session.execute(versions_sql).fetchall()
        available_versions = [row[0].isoformat() for row in version_rows if row[0] is not None]
        selected_version = requested_reference_version if requested_reference_version in available_versions else latest_version.isoformat()

        rows_sql = text("""
            SELECT
                curve_rank_group AS curved_rank_group,
                curve_rank_reference_version,
                min_seconds,
                max_seconds,
                min_time,
                max_time,
                actual_group_cnt,
                score_lower,
                score_upper
            FROM curve_time_ranks_reference
            WHERE metric_type = :rank_type
              AND curve_rank_reference_version = CAST(:reference_version AS DATE)
            ORDER BY curve_rank_group DESC
        """)
        result = db.session.execute(rows_sql, {
            'rank_type': rank_type,
            'reference_version': selected_version,
        })

        rows = []
        for row in result.fetchall():
            record = dict(row._mapping)
            min_seconds = record.get('min_seconds')
            max_seconds = record.get('max_seconds')
            snapshot_date = record.get('curve_rank_reference_version')
            rows.append({
                'curved_rank_group': record.get('curved_rank_group'),
                'curve_rank_reference_version': snapshot_date.isoformat() if snapshot_date is not None else selected_version,
                'min_seconds': min_seconds,
                'max_seconds': max_seconds,
                'min_time': record.get('min_time') or _seconds_to_time_label(min_seconds),
                'max_time': record.get('max_time') or _seconds_to_time_label(max_seconds),
                'score_lower': record.get('score_lower'),
                'score_upper': record.get('score_upper'),
                'actual_group_cnt': record.get('actual_group_cnt'),
            })

        return {
            'rank_type': rank_type,
            'curve_rank_reference_version': selected_version,
            'latest_curve_rank_reference_version': latest_version.isoformat(),
            'available_curve_rank_reference_versions': available_versions,
            'rows': rows
        }

    postgres_reference_payload = _load_reference_from_postgres_table()
    if postgres_reference_payload is not None:
        return jsonify(postgres_reference_payload), 200

    latest_version_sql = text("""
        SELECT MAX(snapshot_date) AS snapshot_date
        FROM curve_rank_range_summary
        WHERE period_type = 'ALL'
          AND metric_type IN ('B', 'E', 'ES', 'AE', 'AES')
    """)
    latest_version = db.session.execute(latest_version_sql).scalar()
    if latest_version is None:
        return jsonify({
            'rank_type': rank_type,
            'curve_rank_reference_version': None,
            'latest_curve_rank_reference_version': None,
            'available_curve_rank_reference_versions': [],
            'rows': []
        }), 200

    versions_sql = text("""
        SELECT DISTINCT snapshot_date
        FROM curve_rank_range_summary
        WHERE period_type = 'ALL'
          AND metric_type IN ('B', 'E', 'ES', 'AE', 'AES')
        ORDER BY snapshot_date DESC
    """)
    version_rows = db.session.execute(versions_sql).fetchall()
    available_versions = [row[0].isoformat() for row in version_rows if row[0] is not None]
    selected_version = requested_reference_version if requested_reference_version in available_versions else latest_version.isoformat()

    rows_sql = text("""
        SELECT
            rank AS curved_rank_group,
            CAST(ROUND(min_best_metric_seconds) AS INTEGER) AS min_seconds,
            CAST(ROUND(max_best_metric_seconds) AS INTEGER) AS max_seconds,
            source_rows AS actual_group_cnt,
            CASE
                WHEN rank = 100 THEN 100.0
                ELSE rank + 0.5
            END AS score_upper,
            CASE
                WHEN rank = 0 THEN 0.0
                ELSE rank - 0.5
            END AS score_lower,
            snapshot_date
        FROM curve_rank_range_summary
        WHERE period_type = 'ALL'
          AND metric_type = :rank_type
          AND snapshot_date = CAST(:reference_version AS DATE)
        ORDER BY rank DESC
    """)
    result = db.session.execute(rows_sql, {
        'rank_type': rank_type,
        'reference_version': selected_version,
    })

    rows = []
    for row in result.fetchall():
        record = dict(row._mapping)
        min_seconds = record.get('min_seconds')
        max_seconds = record.get('max_seconds')
        snapshot_date = record.get('snapshot_date')
        rows.append({
            'curved_rank_group': record.get('curved_rank_group'),
            'curve_rank_reference_version': snapshot_date.isoformat() if snapshot_date is not None else selected_version,
            'min_seconds': min_seconds,
            'max_seconds': max_seconds,
            'min_time': _seconds_to_time_label(min_seconds),
            'max_time': _seconds_to_time_label(max_seconds),
            'score_lower': record.get('score_lower'),
            'score_upper': record.get('score_upper'),
            'actual_group_cnt': record.get('actual_group_cnt'),
        })

    return jsonify({
        'rank_type': rank_type,
        'curve_rank_reference_version': selected_version,
        'latest_curve_rank_reference_version': latest_version.isoformat(),
        'available_curve_rank_reference_versions': available_versions,
        'rows': rows
    }), 200

@app.route('/api/athlete_runs', methods=['GET'])
def get_athlete_runs():
    athlete_code = request.args.get('athlete_code', type=str)
    if not athlete_code:
        return jsonify({'error': 'athlete_code is required'}), 400

    sql = text("""
        SELECT
            ep.event_code,
            e.event_name AS event_name,
            e.display_name AS event_display,
            ep.event_date,
            ep.position,
            ep.name,
            ep.male_position,
            ep.male_count,
            ep.age_group,
            ep.age_grade,
            ep.time,
            ep.club,
            ep.comment,
            ep.athlete_code,
            ep.event_eligible_appearances,
            ep.time_ratio,
            ep.adj_time_seconds,
            ep.adj_time_ratio,
            ep.event_code_count,
            ep.tourist_flag,
            ep.last_event_code_count,
            ep.total_runs,
            ep.age_ratio_male,
            ep.age_ratio_sex,
            ep.super_tourist,
            ep.local_time_ratio,
            ep.adj2_time_seconds,
            ep.adj2_time_ratio,
            ep.distinct_courses_long,
            ep.last_event_code_count_long,
            ep.total_runs_long,
            ep.current_age_estimate,
            ep.regular,
            ep.returner,
            ep.super_returner,
            a.name AS athlete_name,
            a.club AS athlete_club,
            a.min_dob,
            a.last_age_estimate,
            a.max_dob,
            a.last_updated,
            a.current_age_estimate AS athlete_current_age_estimate,            
            a.sex,
            a.total_runs,
			p.coeff,
			p.coeff_event,
			p.event_number,
			p.last_position,
            ep.best_curve_ranking_current,
		    ep.best_curve_ranking_historic,
		    ep.best_curve_ranking_current_type,
            ep.event_rank_b,
            ep.event_rank_e,
            ep.event_rank_es,
            ep.event_rank_ae,
            ep.event_rank_aes
        FROM eventpositions ep
        JOIN athletes a ON a.athlete_code = ep.athlete_code
        LEFT JOIN events e ON e.event_code = ep.event_code
		LEFT JOIN parkrun_events p ON ep.event_code=p.event_code and ep.event_date=p.event_date
        WHERE ep.athlete_code = :athlete_code
        ORDER BY substr(ep.event_date, 7, 4) || '-' || substr(ep.event_date, 4, 2) || '-' || substr(ep.event_date, 1, 2), ep.position
    """)

    result = db.session.execute(sql, {'athlete_code': athlete_code})
    rows = [dict(row) for row in result.fetchall()]
    return jsonify(rows), 200

@app.route('/api/athletes', methods=['GET'])
def get_athletes():
    sql = text("SELECT athlete_code, name FROM athletes")
    result = db.session.execute(sql)
    rows = [dict(row) for row in result.fetchall()]
    return jsonify(rows), 200

@app.route('/api/athletes/search', methods=['GET'])
def search_athletes():
    q = request.args.get('q', default='', type=str).strip()
    limit = request.args.get('limit', default=25, type=int)
    if not q:
        return jsonify([]), 200

    # Prefer prefix match on athlete_code and substring (case-insensitive) on name
    pattern_code = f'{q}%'
    pattern_name = f'%{q.lower()}%'

    sql = text("""
        SELECT athlete_code, name, club, current_age_estimate
        FROM athletes
        WHERE athlete_code LIKE :pattern_code
           OR LOWER(name) LIKE :pattern_name
        ORDER BY CASE WHEN LOWER(name) LIKE :pattern_name THEN 0 ELSE 1 END, name
        LIMIT :limit
    """)
    result = db.session.execute(sql, {'pattern_code': pattern_code, 'pattern_name': pattern_name, 'limit': limit})
    rows = [dict(row) for row in result.fetchall()]
    return jsonify(rows), 200

@app.route('/api/clubs/search', methods=['GET'])
def search_clubs():
    q = request.args.get('q', default='', type=str).strip().lower()
    limit = request.args.get('limit', default=25, type=int)
    limit = max(1, min(limit, 200))

    sql = text("""
        SELECT
            club,
            COUNT(*)::int AS athlete_count
        FROM athletes
        WHERE club IS NOT NULL
          AND btrim(club) <> ''
          AND (:q = '' OR LOWER(club) LIKE :pattern)
        GROUP BY club
        ORDER BY club
        LIMIT :limit
    """)
    result = db.session.execute(sql, {'q': q, 'pattern': f'%{q}%', 'limit': limit})
    rows = [dict(row) for row in result.fetchall()]
    return jsonify(rows), 200


@app.route('/api/clubs/members', methods=['GET'])
def get_club_members():
    club = (request.args.get('club') or '').strip()
    limit = request.args.get('limit', default=1000, type=int)
    limit = max(1, min(limit, 5000))

    if not club:
        return jsonify({'error': 'Missing required parameter: club'}), 400
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
        WHERE club_key = regexp_replace(LOWER(BTRIM(:club)), '\\s+ac$', '')
        ORDER BY club_runs_total DESC, name
        LIMIT :limit
    """)

    result = db.session.execute(sql, {'club': club, 'limit': limit})
    rows = [dict(row) for row in result.fetchall()]
    return jsonify(rows), 200


@app.route('/api/clubs/course-summary', methods=['GET'])
def get_club_course_summary():
    club = (request.args.get('club') or '').strip()
    limit = request.args.get('limit', default=1000, type=int)
    limit = max(1, min(limit, 5000))

    if not club:
        return jsonify({'error': 'Missing required parameter: club'}), 400

    sql = text("""
        WITH params AS (
            SELECT
                BTRIM(:club) AS target_club,
                MAX(CAST(formatted_date AS date)) AS anchor_date
            FROM eventpositions
            WHERE formatted_date IS NOT NULL
        ),
        base AS (
            SELECT
                ep.event_code,
                COALESCE(NULLIF(ev.display_name, ''), ev.event_name) AS event_name,
                CAST(ep.formatted_date AS date) AS event_day,
                ep.athlete_code
            FROM eventpositions ep
            JOIN events ev
              ON ev.event_code = ep.event_code
            CROSS JOIN params p
                        WHERE ep.formatted_date IS NOT NULL
              AND LOWER(BTRIM(COALESCE(ep.club, ''))) = LOWER(p.target_club)
        ),
        all_history AS (
            SELECT
                b.event_code,
                b.event_name,
                COUNT(DISTINCT b.event_day)::int AS events_held_all_history,
                COUNT(*)::int AS club_runs_all_history,
                COUNT(DISTINCT b.athlete_code)::int AS athletes_all_history
            FROM base b
            GROUP BY b.event_code, b.event_name
        ),
        last_year AS (
            SELECT
                b.event_code,
                b.event_name,
                COUNT(DISTINCT b.event_day)::int AS events_held_last_year,
                COUNT(*)::int AS club_runs_last_year,
                COUNT(DISTINCT b.athlete_code)::int AS athletes_last_year
            FROM base b
            CROSS JOIN params p
            WHERE p.anchor_date IS NOT NULL
              AND b.event_day >= (p.anchor_date - INTERVAL '1 year')
            GROUP BY b.event_code, b.event_name
        )
        SELECT
            a.event_code,
            a.event_name,
            a.events_held_all_history,
            a.club_runs_all_history,
            a.athletes_all_history,
            RANK() OVER (
                ORDER BY
                    a.club_runs_all_history DESC,
                    a.events_held_all_history DESC,
                    a.athletes_all_history DESC,
                    a.event_name ASC
            )::int AS rank_all_history,
            COALESCE(l.events_held_last_year, 0)::int AS events_held_last_year,
            COALESCE(l.club_runs_last_year, 0)::int AS club_runs_last_year,
            COALESCE(l.athletes_last_year, 0)::int AS athletes_last_year,
            RANK() OVER (
                ORDER BY
                    COALESCE(l.club_runs_last_year, 0) DESC,
                    COALESCE(l.events_held_last_year, 0) DESC,
                    COALESCE(l.athletes_last_year, 0) DESC,
                    a.event_name ASC
            )::int AS rank_last_year
        FROM all_history a
        LEFT JOIN last_year l
          ON l.event_code = a.event_code
        ORDER BY
            a.club_runs_all_history DESC,
            a.events_held_all_history DESC,
            a.event_name ASC
        LIMIT :limit
    """)

    result = db.session.execute(sql, {'club': club, 'limit': limit})
    rows = [dict(row) for row in result.fetchall()]
    return jsonify(rows), 200

@app.route('/api/athlete_best_summary', methods=['GET'])
def get_athlete_best_summary():
        """
        API endpoint to get best-run ranks (all-time and last-year) plus total/recent runs
        for a single athlete.
        Query param:
            - athlete_code (required)
        """
        from app import db  # Import db here to avoid circular import
        try:
                athlete_code = (request.args.get('athlete_code') or '').strip()
                if not athlete_code:
                        return jsonify({
                                'error': 'Missing required parameter',
                                'required': ['athlete_code']
                        }), 400

                history_table_exists = bool(
                    db.session.execute(text("SELECT to_regclass('public.curve_rank_mapping_history') IS NOT NULL")).scalar()
                )
                summary_table_exists = bool(
                    db.session.execute(text("SELECT to_regclass('public.curve_rank_range_summary') IS NOT NULL")).scalar()
                )

                if history_table_exists:
                    latest_snapshot_sql = """
                            SELECT MAX(snapshot_date) AS snapshot_date
                            FROM curve_rank_mapping_history
                            WHERE period_type = '1Y'
                    """
                    mapping_lateral_sql = """
                                SELECT c.rank, c.best_metric_seconds AS mapped_seconds
                                FROM curve_rank_mapping_history c
                                JOIN latest_1y_snapshot ls ON ls.snapshot_date = c.snapshot_date
                                WHERE c.period_type = '1Y'
                                    AND c.metric_type = b.metric_type
                                    AND c.rank IS NOT NULL
                                ORDER BY ABS(c.best_metric_seconds - b.metric_seconds) ASC, c.best_metric_seconds ASC
                                LIMIT 1
                    """
                elif summary_table_exists:
                    latest_snapshot_sql = """
                            SELECT MAX(snapshot_date) AS snapshot_date
                            FROM curve_rank_range_summary
                            WHERE period_type = '1Y'
                    """
                    mapping_lateral_sql = """
                                SELECT
                                    c.rank,
                                    COALESCE(
                                        (c.min_best_metric_seconds + c.max_best_metric_seconds) / 2.0,
                                        c.min_best_metric_seconds,
                                        c.max_best_metric_seconds
                                    ) AS mapped_seconds
                                FROM curve_rank_range_summary c
                                JOIN latest_1y_snapshot ls ON ls.snapshot_date = c.snapshot_date
                                WHERE c.period_type = '1Y'
                                    AND c.metric_type = b.metric_type
                                    AND c.rank IS NOT NULL
                                ORDER BY
                                    CASE
                                    WHEN b.metric_seconds IS NULL THEN 1000000000.0
                                    WHEN c.min_best_metric_seconds IS NOT NULL
                                         AND b.metric_seconds < c.min_best_metric_seconds
                                        THEN c.min_best_metric_seconds - b.metric_seconds
                                    WHEN c.max_best_metric_seconds IS NOT NULL
                                         AND b.metric_seconds > c.max_best_metric_seconds
                                        THEN b.metric_seconds - c.max_best_metric_seconds
                                    ELSE 0.0
                                    END ASC,
                                    c.rank DESC
                                LIMIT 1
                    """
                else:
                    latest_snapshot_sql = "SELECT NULL::date AS snapshot_date"
                    mapping_lateral_sql = "SELECT NULL::numeric AS rank, NULL::double precision AS mapped_seconds WHERE FALSE"

                sql = text(f"""
                        WITH
                        params AS (
                            SELECT CAST(:athlete_code AS text) AS athlete_code
                        ),
                        athlete_curve_source AS (
                            SELECT
                                ep.athlete_code::text AS athlete_code,
                                ep.event_date::text AS event_date,
                                to_date(ep.event_date, 'DD/MM/YYYY') AS event_dt,
                                ep.time::text AS time,
                                ep.event_rank_b,
                                ep.event_rank_e,
                                ep.event_rank_es,
                                ep.event_rank_ae,
                                ep.event_rank_aes,
                                ep.current_best_rank_b,
                                ep.current_best_rank_e,
                                ep.current_best_rank_es,
                                ep.current_best_rank_ae,
                                ep.current_best_rank_aes,
                                ep.age_ratio_male,
                                ep.age_ratio_sex,
                                p.coeff,
                                p.coeff_event,
                                CASE
                                    WHEN ep.time IS NULL OR btrim(ep.time) = '' THEN NULL
                                    WHEN length(ep.time) - length(replace(ep.time, ':', '')) = 2 THEN
                                        CAST(substring(ep.time, 1, strpos(ep.time, ':') - 1) AS INTEGER) * 3600 +
                                        CAST(substring(ep.time, strpos(ep.time, ':') + 1, strpos(substring(ep.time, strpos(ep.time, ':') + 1), ':') - 1) AS INTEGER) * 60 +
                                        CAST(substring(ep.time, length(ep.time) - 1, 2) AS INTEGER)
                                    WHEN strpos(ep.time, ':') > 0 THEN
                                        CAST(substring(ep.time, 1, strpos(ep.time, ':') - 1) AS INTEGER) * 60 +
                                        CAST(substring(ep.time, strpos(ep.time, ':') + 1) AS INTEGER)
                                    ELSE NULL
                                END AS time_seconds
                            FROM eventpositions ep
                            LEFT JOIN parkrun_events p ON ep.event_code = p.event_code AND ep.event_date = p.event_date
                            JOIN params prm ON ep.athlete_code::text = prm.athlete_code
                        ),
                        athlete_curve_rows AS (
                            SELECT
                                athlete_code,
                                event_date,
                                event_dt,
                                time,
                                event_rank_b,
                                event_rank_e,
                                event_rank_es,
                                event_rank_ae,
                                event_rank_aes,
                                current_best_rank_b,
                                current_best_rank_e,
                                current_best_rank_es,
                                current_best_rank_ae,
                                current_best_rank_aes,
                                {get_adjustment_fields_sql()}
                            FROM athlete_curve_source
                            WHERE time_seconds IS NOT NULL
                        ),
                        athlete_curve_rows_1y AS (
                            SELECT *
                            FROM athlete_curve_rows
                            WHERE event_dt >= (CURRENT_DATE - INTERVAL '1 year')::date
                        ),
                        total_runs_ranked AS (
                            SELECT
                                a.athlete_code::text AS athlete_code,
                                COALESCE(a.total_runs, 0) AS total_runs,
                                COALESCE(a.recent_runs, 0) AS recent_runs
                            FROM athletes a
                        ),
                        best_metric_candidates AS (
                            SELECT athlete_code, 'B'::text AS metric_type, MIN(time_seconds)::double precision AS metric_seconds
                            FROM athlete_curve_rows_1y
                            GROUP BY athlete_code

                            UNION ALL

                            SELECT athlete_code, 'E'::text AS metric_type, MIN(event_adj_time_seconds)::double precision AS metric_seconds
                            FROM athlete_curve_rows_1y
                            GROUP BY athlete_code

                            UNION ALL

                            SELECT athlete_code, 'AE'::text AS metric_type, MIN(age_event_adj_time_seconds)::double precision AS metric_seconds
                            FROM athlete_curve_rows_1y
                            GROUP BY athlete_code

                            UNION ALL

                            SELECT athlete_code, 'ES'::text AS metric_type, MIN(sex_event_adj_time_seconds)::double precision AS metric_seconds
                            FROM athlete_curve_rows_1y
                            GROUP BY athlete_code

                            UNION ALL

                            SELECT athlete_code, 'AES'::text AS metric_type, MIN(age_sex_event_adj_time_seconds)::double precision AS metric_seconds
                            FROM athlete_curve_rows_1y
                            GROUP BY athlete_code
                        ),
                        latest_1y_snapshot AS (
                            {latest_snapshot_sql}
                        ),
                        est_rank_candidates AS (
                            SELECT
                                b.athlete_code,
                                b.metric_type,
                                b.metric_seconds,
                                cm.rank::numeric AS rank,
                                CASE b.metric_type
                                    WHEN 'B' THEN 1
                                    WHEN 'E' THEN 2
                                    WHEN 'AE' THEN 3
                                    WHEN 'ES' THEN 4
                                    WHEN 'AES' THEN 5
                                    ELSE 99
                                END AS metric_order
                            FROM best_metric_candidates b
                            LEFT JOIN LATERAL (
                                                                {mapping_lateral_sql}
                            ) cm ON TRUE
                        ),
                        est_rank_best AS (
                            SELECT
                                athlete_code,
                                metric_type,
                                metric_seconds,
                                rank,
                                ROW_NUMBER() OVER (
                                    PARTITION BY athlete_code
                                    ORDER BY rank DESC NULLS LAST, metric_order ASC
                                ) AS rn
                            FROM est_rank_candidates
                            WHERE rank IS NOT NULL
                        ),
                        best_1y_metric_rows AS (
                            SELECT athlete_code, 'best_1y'::text AS best_type, 'B'::text AS metric_type, event_date, event_dt, event_rank_b AS carried_rank, time::text AS time, time_seconds::double precision AS sort_seconds, time_seconds::double precision AS metric_seconds
                            FROM athlete_curve_rows_1y
                            WHERE time_seconds IS NOT NULL

                            UNION ALL

                            SELECT athlete_code, 'event_1y'::text AS best_type, 'E'::text AS metric_type, event_date, event_dt, event_rank_e AS carried_rank, event_adj_time::text AS time, event_adj_time_seconds::double precision AS sort_seconds, event_adj_time_seconds::double precision AS metric_seconds
                            FROM athlete_curve_rows_1y
                            WHERE event_adj_time_seconds IS NOT NULL

                            UNION ALL

                            SELECT athlete_code, 'age_event_1y'::text AS best_type, 'AE'::text AS metric_type, event_date, event_dt, event_rank_ae AS carried_rank, age_event_adj_time::text AS time, age_event_adj_time_seconds::double precision AS sort_seconds, age_event_adj_time_seconds::double precision AS metric_seconds
                            FROM athlete_curve_rows_1y
                            WHERE age_event_adj_time_seconds IS NOT NULL

                            UNION ALL

                            SELECT athlete_code, 'sex_event_1y'::text AS best_type, 'ES'::text AS metric_type, event_date, event_dt, event_rank_es AS carried_rank, sex_event_adj_time::text AS time, sex_event_adj_time_seconds::double precision AS sort_seconds, sex_event_adj_time_seconds::double precision AS metric_seconds
                            FROM athlete_curve_rows_1y
                            WHERE sex_event_adj_time_seconds IS NOT NULL

                            UNION ALL

                            SELECT athlete_code, 'age_sex_event_1y'::text AS best_type, 'AES'::text AS metric_type, event_date, event_dt, event_rank_aes AS carried_rank, age_sex_event_adj_time::text AS time, age_sex_event_adj_time_seconds::double precision AS sort_seconds, age_sex_event_adj_time_seconds::double precision AS metric_seconds
                            FROM athlete_curve_rows_1y
                            WHERE age_sex_event_adj_time_seconds IS NOT NULL
                        ),
                        picked_1y_metric_rows AS (
                            SELECT
                                athlete_code,
                                best_type,
                                metric_type,
                                event_date,
                                event_dt,
                                carried_rank,
                                time,
                                sort_seconds,
                                metric_seconds,
                                ROW_NUMBER() OVER (
                                    PARTITION BY athlete_code, best_type
                                    ORDER BY metric_seconds ASC NULLS LAST, event_dt ASC NULLS LAST, event_date ASC NULLS LAST
                                ) AS rn
                            FROM best_1y_metric_rows
                        ),
                        resolved_1y_metric_rows AS (
                            SELECT
                                p.athlete_code,
                                p.best_type,
                                p.event_date,
                                                                p.carried_rank AS rank,
                                p.time,
                                p.sort_seconds,
                                p.event_dt
                            FROM picked_1y_metric_rows p
                            WHERE p.rn = 1
                                                            AND p.carried_rank IS NOT NULL
                        ),
                        stacked AS (
                                                        SELECT athlete_code, 'best_all_time'::text AS best_type, event_date, current_best_rank_b AS rank, time::text AS time, time_seconds AS sort_seconds, event_dt
                            FROM athlete_curve_rows
                            WHERE current_best_rank_b IS NOT NULL

                            UNION ALL
                                                        SELECT athlete_code, 'event_all_time'::text AS best_type, event_date, current_best_rank_e AS rank, event_adj_time::text AS time, event_adj_time_seconds AS sort_seconds, event_dt
                            FROM athlete_curve_rows
                            WHERE current_best_rank_e IS NOT NULL

                            UNION ALL
                                                        SELECT athlete_code, 'age_event_all_time'::text AS best_type, event_date, current_best_rank_ae AS rank, age_event_adj_time::text AS time, age_event_adj_time_seconds AS sort_seconds, event_dt
                            FROM athlete_curve_rows
                            WHERE current_best_rank_ae IS NOT NULL

                            UNION ALL
                                                        SELECT athlete_code, 'sex_event_all_time'::text AS best_type, event_date, current_best_rank_es AS rank, sex_event_adj_time::text AS time, sex_event_adj_time_seconds AS sort_seconds, event_dt
                            FROM athlete_curve_rows
                            WHERE current_best_rank_es IS NOT NULL

                            UNION ALL
                                                        SELECT athlete_code, 'age_sex_event_all_time'::text AS best_type, event_date, current_best_rank_aes AS rank, age_sex_event_adj_time::text AS time, age_sex_event_adj_time_seconds AS sort_seconds, event_dt
                            FROM athlete_curve_rows
                            WHERE current_best_rank_aes IS NOT NULL

                            UNION ALL
                                                        SELECT athlete_code, best_type, event_date, rank, time, sort_seconds, event_dt
                            FROM resolved_1y_metric_rows

                            UNION ALL
                                                        SELECT tr.athlete_code, 'total_runs'::text AS best_type, CURRENT_DATE::text AS event_date, 0 AS rank, tr.total_runs::text AS time, NULL::numeric AS sort_seconds, CURRENT_DATE::date AS event_dt
                            FROM total_runs_ranked tr JOIN params p ON tr.athlete_code = p.athlete_code

                            UNION ALL
                                                        SELECT tr.athlete_code, 'recent_runs'::text AS best_type, CURRENT_DATE::text AS event_date, 0 AS rank, tr.recent_runs::text AS time, NULL::numeric AS sort_seconds, CURRENT_DATE::date AS event_dt
                            FROM total_runs_ranked tr JOIN params p ON tr.athlete_code = p.athlete_code

                            UNION ALL
                                                        SELECT e.athlete_code, 'estimated_rank_1y'::text AS best_type, NULL::text AS event_date, e.rank, e.metric_type::text AS time, e.metric_seconds AS sort_seconds, NULL::date AS event_dt
                            FROM est_rank_best e
                            WHERE e.rn = 1
                        ),
                        picked AS (
                            SELECT *,
                                   ROW_NUMBER() OVER (
                                       PARTITION BY athlete_code, best_type
                                                                             ORDER BY rank DESC NULLS LAST, sort_seconds ASC NULLS LAST, event_dt DESC NULLS LAST, event_date DESC NULLS LAST
                                   ) AS rn
                            FROM stacked
                        )
                        SELECT
                            athlete_code,
                            best_type,
                            event_date,
                            rank,
                            time
                        FROM picked
                        WHERE rn = 1
                        ORDER BY best_type;
                """)

                result_proxy = db.session.execute(sql, {'athlete_code': athlete_code, 'min_sec': 12 * 60 + 49})
                column_names = result_proxy.keys()
                results = [dict(zip(column_names, row)) for row in result_proxy.fetchall()]

                db.session.commit()
                return jsonify(results)

        except Exception as e:
                try:
                        db.session.rollback()
                except Exception:
                        pass
                tb = traceback.format_exc()
                print(f"Database error in get_athlete_best_summary: {e}\n{tb}")
                return jsonify({
                        "error": "Failed to fetch data from the database",
                        "exception": str(e),
                        "traceback": tb
                }), 500

@app.route('/api/next_ext_similar', methods=['GET'])
def get_next_ext_similar():
        athlete_code = request.args.get('athlete_code', type=str)
        if not athlete_code:
                return jsonify({'error': 'athlete_code is required'}), 400

        above_count = request.args.get('above', default=10, type=int)
        below_count = request.args.get('below', default=10, type=int)
        above_count = max(0, min(50, above_count if above_count is not None else 10))
        below_count = max(0, min(50, below_count if below_count is not None else 10))

        sql = text("""
            WITH athlete_curve_rows_1y AS (
                SELECT
                    ep.athlete_code::text AS athlete_code,
                    COALESCE(NULLIF(BTRIM(ep.name), ''), NULLIF(BTRIM(a.name), ''), ep.athlete_code::text) AS athlete_name,
                    COALESCE(NULLIF(BTRIM(ep.club), ''), NULLIF(BTRIM(a.club), ''), '') AS club,
                    ep.event_date::date AS event_dt,
                    NULLIF(BTRIM(ep.age_group), '') AS age_group,
                    NULLIF(BTRIM(ep.age_grade::text), '') AS age_grade,
                    time_to_seconds(ep.time)::numeric AS raw_seconds,
                    ep.event_rank_b::numeric AS rank_b,
                    ep.event_rank_e::numeric AS rank_e,
                    ep.event_rank_ae::numeric AS rank_ae,
                    ep.event_rank_es::numeric AS rank_es,
                    ep.event_rank_aes::numeric AS rank_aes,
                    NULLIF((COALESCE(pe.coeff, 1)::numeric + COALESCE(pe.coeff_event, 1)::numeric - 1), 0) AS coeff_product,
                    NULLIF(ep.age_ratio_male::numeric, 0) AS age_ratio_male,
                    NULLIF(ep.age_ratio_sex::numeric, 0) AS age_ratio_sex
                FROM eventpositions ep
                JOIN athletes a
                  ON a.athlete_code = ep.athlete_code
                LEFT JOIN parkrun_events pe
                  ON pe.event_code = ep.event_code
                 AND pe.event_date = ep.event_date
                WHERE ep.event_date::date >= CURRENT_DATE - INTERVAL '1 year'
            ),
            metric_rows AS (
                SELECT
                    athlete_code,
                    athlete_name,
                    club,
                    event_dt,
                    age_group,
                    age_grade,
                    'B'::text AS rank_metric,
                    '*'::text AS rank_suffix,
                    1 AS metric_order,
                    rank_b AS exact_rank,
                    ROUND(rank_b)::int AS display_rank,
                    raw_seconds AS metric_seconds
                FROM athlete_curve_rows_1y
                WHERE rank_b IS NOT NULL
                  AND raw_seconds IS NOT NULL

                UNION ALL

                SELECT
                    athlete_code,
                    athlete_name,
                    club,
                    event_dt,
                    age_group,
                    age_grade,
                    'E'::text AS rank_metric,
                    'E'::text AS rank_suffix,
                    2 AS metric_order,
                    rank_e AS exact_rank,
                    ROUND(rank_e)::int AS display_rank,
                    CASE
                        WHEN coeff_product IS NULL THEN NULL
                        ELSE raw_seconds / coeff_product
                    END AS metric_seconds
                FROM athlete_curve_rows_1y
                WHERE rank_e IS NOT NULL
                  AND raw_seconds IS NOT NULL

                UNION ALL

                SELECT
                    athlete_code,
                    athlete_name,
                    club,
                    event_dt,
                    age_group,
                    age_grade,
                    'AE'::text AS rank_metric,
                    'AE'::text AS rank_suffix,
                    3 AS metric_order,
                    rank_ae AS exact_rank,
                    ROUND(rank_ae)::int AS display_rank,
                    CASE
                        WHEN coeff_product IS NULL OR age_ratio_male IS NULL THEN NULL
                        ELSE raw_seconds / (coeff_product * age_ratio_male)
                    END AS metric_seconds
                FROM athlete_curve_rows_1y
                WHERE rank_ae IS NOT NULL
                  AND raw_seconds IS NOT NULL

                UNION ALL

                SELECT
                    athlete_code,
                    athlete_name,
                    club,
                    event_dt,
                    age_group,
                    age_grade,
                    'ES'::text AS rank_metric,
                    'ES'::text AS rank_suffix,
                    4 AS metric_order,
                    rank_es AS exact_rank,
                    ROUND(rank_es)::int AS display_rank,
                    CASE
                        WHEN coeff_product IS NULL OR age_ratio_male IS NULL OR age_ratio_sex IS NULL THEN NULL
                        ELSE raw_seconds / (coeff_product * (age_ratio_sex / age_ratio_male))
                    END AS metric_seconds
                FROM athlete_curve_rows_1y
                WHERE rank_es IS NOT NULL
                  AND raw_seconds IS NOT NULL

                UNION ALL

                SELECT
                    athlete_code,
                    athlete_name,
                    club,
                    event_dt,
                    age_group,
                    age_grade,
                    'AES'::text AS rank_metric,
                    'AES'::text AS rank_suffix,
                    5 AS metric_order,
                    rank_aes AS exact_rank,
                    ROUND(rank_aes)::int AS display_rank,
                    CASE
                        WHEN coeff_product IS NULL OR age_ratio_sex IS NULL THEN NULL
                        ELSE raw_seconds / (coeff_product * age_ratio_sex)
                    END AS metric_seconds
                FROM athlete_curve_rows_1y
                WHERE rank_aes IS NOT NULL
                  AND raw_seconds IS NOT NULL
            ),
            ranked_metric_rows AS (
                SELECT
                    metric_rows.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY athlete_code
                        ORDER BY display_rank DESC, exact_rank DESC, metric_seconds ASC NULLS LAST, event_dt DESC, metric_order ASC
                    ) AS athlete_pick_rn
                FROM metric_rows
                WHERE display_rank IS NOT NULL
                  AND metric_seconds IS NOT NULL
            ),
            best_rows AS (
                SELECT
                    athlete_code,
                    athlete_name,
                    club,
                    event_dt,
                    age_group,
                    age_grade,
                    rank_metric,
                    rank_suffix,
                    exact_rank,
                    display_rank,
                    metric_seconds,
                    CONCAT(display_rank::text, rank_suffix) AS rank_display
                FROM ranked_metric_rows
                WHERE athlete_pick_rn = 1
            ),
            selected AS (
                SELECT *
                FROM best_rows
                WHERE athlete_code = :athlete_code
            ),
            peer_pool AS (
                SELECT
                    best_rows.*,
                    ROW_NUMBER() OVER (
                        ORDER BY metric_seconds ASC, exact_rank DESC, LOWER(athlete_name) ASC, athlete_code ASC
                    ) AS peer_rn
                FROM best_rows
                JOIN selected
                  ON selected.display_rank = best_rows.display_rank
            ),
            selected_position AS (
                SELECT peer_rn AS selected_peer_rn
                FROM peer_pool
                WHERE athlete_code = :athlete_code
            )
            SELECT
                peer_pool.athlete_code,
                peer_pool.athlete_name,
                peer_pool.club,
                peer_pool.age_group,
                peer_pool.age_grade,
                TO_CHAR(peer_pool.event_dt, 'DDMonYY') AS event_date,
                peer_pool.rank_metric,
                peer_pool.rank_suffix,
                peer_pool.display_rank AS rank_score,
                ROUND(peer_pool.exact_rank::numeric, 1) AS exact_rank,
                peer_pool.rank_display,
                ROUND(peer_pool.metric_seconds)::int AS best_time_seconds,
                peer_pool.peer_rn,
                selected_position.selected_peer_rn,
                (peer_pool.athlete_code = :athlete_code) AS is_selected
            FROM peer_pool
            CROSS JOIN selected_position
            WHERE peer_pool.peer_rn BETWEEN GREATEST(selected_position.selected_peer_rn - :above_count, 1)
                                        AND selected_position.selected_peer_rn + :below_count
            ORDER BY peer_pool.peer_rn
        """)

        try:
                result_proxy = db.session.execute(sql, {
                        'athlete_code': athlete_code,
                        'above_count': above_count,
                        'below_count': below_count
                })
                column_names = result_proxy.keys()
                rows = [dict(zip(column_names, row)) for row in result_proxy.fetchall()]

                selected_row = next((row for row in rows if row.get('is_selected')), None)

                db.session.commit()
                return jsonify({
                        'selectedAthleteCode': athlete_code,
                        'selectedRankScore': selected_row.get('rank_score') if selected_row else None,
                        'selectedRankDisplay': selected_row.get('rank_display') if selected_row else None,
                        'rows': rows
                })

        except Exception as e:
                try:
                        db.session.rollback()
                except Exception:
                        pass
                tb = traceback.format_exc()
                print(f"Database error in get_next_ext_similar: {e}\n{tb}")
                return jsonify({
                        'error': 'Failed to fetch data from the database',
                        'exception': str(e),
                        'traceback': tb
                }), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)