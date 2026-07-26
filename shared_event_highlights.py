import re


def build_date_variants(raw_value):
    variants = []
    if raw_value:
        variants.append(str(raw_value).strip())
    if raw_value and re.match(r'^\d{4}-\d{2}-\d{2}$', str(raw_value).strip()):
        yyyy, mm, dd = str(raw_value).strip().split('-')
        variants.append(f'{dd}/{mm}/{yyyy}')
    if raw_value and re.match(r'^\d{2}/\d{2}/\d{4}$', str(raw_value).strip()):
        dd, mm, yyyy = str(raw_value).strip().split('/')
        variants.append(f'{yyyy}-{mm}-{dd}')

    ordered = []
    seen = set()
    for value in variants:
        if value and value not in seen:
            ordered.append(value)
            seen.add(value)
    return ordered


def parse_age_grade_value(raw_value):
    if raw_value is None:
        return None

    text_value = str(raw_value).strip().replace('%', '')
    if not text_value:
        return None

    try:
        return float(text_value)
    except Exception:
        return None


def parse_time_to_seconds(raw_value):
    if raw_value is None:
        return None

    text_value = str(raw_value).strip()
    if not text_value:
        return None

    try:
        if re.match(r'^-?\d+(?:\.\d+)?$', text_value):
            return float(text_value)
        parts = [int(part) for part in text_value.split(':')]
        if len(parts) == 3:
            return (parts[0] * 3600) + (parts[1] * 60) + parts[2]
        if len(parts) == 2:
            return (parts[0] * 60) + parts[1]
        if len(parts) == 1:
            return parts[0]
    except Exception:
        return None

    return None


def classify_sex(age_group_value):
    normalized = str(age_group_value or '').strip().upper()
    if normalized.startswith(('VW', 'SW', 'JW', 'W')):
        return 'female'
    if normalized.startswith(('VM', 'SM', 'JM', 'M')):
        return 'male'
    return 'unknown'


def milestone_label(level, suffix):
    return f'{level} {suffix}'


def build_event_highlights_payload(event_row, position_rows, volunteer_rows, resolved_event_name):
    milestone_levels = [10, 25, 50, 100, 250, 500, 1000]

    gender_counts = {'male': 0, 'female': 0, 'unknown': 0}
    pb_breakdown = {'male': 0, 'female': 0, 'unknown': 0}
    first_timer_breakdown = {'ever': 0, 'here': 0}
    milestone_counts = {str(level): 0 for level in milestone_levels}
    volunteer_milestone_counts = {str(level): 0 for level in milestone_levels}
    first_finishers = {'male': None, 'female': None}
    first_finisher = None
    top_age_grade = None
    first_timers = []
    first_timer_names = []
    pb_roster = []
    milestone_people = []
    volunteer_people = []
    volunteer_names = []
    volunteer_milestones = []
    minute_counts = {}

    for row in position_rows:
        position = row.get('position')
        athlete_code_value = row.get('athlete_code')
        safe_name = str(row.get('name') or athlete_code_value or '').strip()
        safe_comment = str(row.get('comment') or '').strip()
        lower_comment = safe_comment.lower()
        time_value = row.get('time')
        age_group_value = row.get('age_group')
        sex = classify_sex(age_group_value)
        total_runs_value = int(row.get('total_runs') or 0)

        gender_counts[sex] = gender_counts.get(sex, 0) + 1

        person_payload = {
            'position': position,
            'name': safe_name,
            'athlete_code': athlete_code_value,
            'time': time_value,
            'comment': safe_comment,
            'age_group': age_group_value,
            'age_grade': row.get('age_grade'),
            'sex': sex,
            'total_runs': total_runs_value,
        }

        if first_finisher is None:
            first_finisher = {
                'position': position,
                'name': safe_name,
                'athlete_code': athlete_code_value,
                'time': time_value,
                'age_group': age_group_value,
                'age_grade': row.get('age_grade'),
                'sex': sex,
            }

        if sex in ('male', 'female') and first_finishers[sex] is None:
            first_finishers[sex] = {
                'position': position,
                'name': safe_name,
                'athlete_code': athlete_code_value,
                'time': time_value,
                'age_group': age_group_value,
                'age_grade': row.get('age_grade'),
            }

        age_grade_value = parse_age_grade_value(row.get('age_grade'))
        if age_grade_value is not None:
            if top_age_grade is None or age_grade_value > top_age_grade['age_grade_value']:
                top_age_grade = {
                    'position': position,
                    'name': safe_name,
                    'athlete_code': athlete_code_value,
                    'time': time_value,
                    'age_group': age_group_value,
                    'age_grade': row.get('age_grade'),
                    'age_grade_value': age_grade_value,
                }

        is_pb = lower_comment.startswith('new pb')
        if is_pb:
            pb_roster.append(person_payload)
            pb_breakdown[sex] = pb_breakdown.get(sex, 0) + 1

        is_first_timer_here = 'first timer' in lower_comment
        is_first_timer_ever = total_runs_value == 1
        if is_first_timer_here or is_first_timer_ever:
            timer_type = 'ever' if is_first_timer_ever else 'here'
            first_timer_breakdown[timer_type] += 1
            first_timers.append({
                **person_payload,
                'first_timer_type': timer_type,
            })
            first_timer_names.append(safe_name)

        for level in milestone_levels:
            if total_runs_value == level:
                milestone_counts[str(level)] += 1
                milestone_people.append({
                    **person_payload,
                    'milestone_level': level,
                    'milestone_label': milestone_label(level, 'runs'),
                })
                break

        time_seconds = parse_time_to_seconds(time_value)
        if time_seconds is not None:
            minute_bucket = int(time_seconds // 60)
            minute_counts[minute_bucket] = minute_counts.get(minute_bucket, 0) + 1

    for row in volunteer_rows:
        safe_name = str(row.get('athlete_name') or row.get('athlete_code') or '').strip()
        athlete_code_value = row.get('athlete_code')
        total_vols_value = int(row.get('total_vols') or 0)
        volunteer_person = {
            'name': safe_name,
            'athlete_code': athlete_code_value,
            'role': row.get('volunteer_role'),
            'total_vols': total_vols_value,
        }
        volunteer_people.append(volunteer_person)
        volunteer_names.append(safe_name)

        for level in milestone_levels:
            if total_vols_value == level:
                volunteer_milestone_counts[str(level)] += 1
                volunteer_milestones.append({
                    **volunteer_person,
                    'milestone_level': level,
                    'milestone_label': milestone_label(level, 'vols'),
                })
                break

    participants = int(event_row.get('last_position') or len(position_rows) or 0)
    known_gender_total = int(gender_counts.get('male', 0) or 0) + int(gender_counts.get('female', 0) or 0) + int(gender_counts.get('unknown', 0) or 0)
    if participants > known_gender_total:
        gender_counts['unknown'] = gender_counts.get('unknown', 0) + (participants - known_gender_total)

    finishers_by_minute = [
        {
            'minute': minute,
            'label': f'{minute}',
            'count': minute_counts[minute],
        }
        for minute in sorted(minute_counts.keys())
    ]

    volunteers_count = int(event_row.get('volunteers') or len(volunteer_people) or 0)
    return {
        'event_code': event_row.get('event_code'),
        'event_date': event_row.get('event_date'),
        'event_number': event_row.get('event_number'),
        'event_name': event_row.get('event_name') or resolved_event_name,
        'last_position': event_row.get('last_position'),
        'avg_age': event_row.get('avg_age'),
        'first_timers_count': event_row.get('first_timers_count'),
        'pb_count': event_row.get('pb_count'),
        'tourist_count': event_row.get('tourist_count'),
        'regulars': event_row.get('regulars'),
        'returners_count': event_row.get('returners_count'),
        'club_count': event_row.get('club_count'),
        'participants': participants,
        'distance_covered_km': round(participants * 5, 0),
        'volunteers': volunteers_count,
        'top_age_grade': top_age_grade,
        'first_finisher': first_finisher,
        'first_finishers': first_finishers,
        'gender_breakdown': gender_counts,
        'pb_breakdown': {
            **pb_breakdown,
            'total': len(pb_roster),
        },
        'first_timer_breakdown': {
            **first_timer_breakdown,
            'total': len(first_timers),
        },
        'milestone_breakdown': {
            **milestone_counts,
            'total': len(milestone_people),
        },
        'first_timers': first_timers,
        'first_timer_names': first_timer_names,
        'pb_roster': pb_roster,
        'milestone_people': milestone_people,
        'milestone_names': [person.get('name') for person in milestone_people],
        'finishers_by_minute': finishers_by_minute,
        'volunteer_roster': volunteer_people,
        'volunteer_names': volunteer_names,
        'volunteer_milestone_breakdown': {
            **volunteer_milestone_counts,
            'total': len(volunteer_milestones),
        },
        'volunteer_milestones': volunteer_milestones,
        'provisional_notes': [
            'Milestones are inferred from exact current totals on the event date.',
            'First timers are split into first ever parkrun versus first time here using totals plus event comments.',
        ],
    }