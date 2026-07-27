def build_event_by_number_response(event_code, event_number, *, event_lookup):
    if event_code is None or event_number is None:
        return {'error': 'Provide event_code and event_number'}, 400

    record = event_lookup(event_code, event_number)
    if not record:
        return {'error': 'Event not found'}, 404

    return {
        'event_code': record.get('event_code'),
        'event_number': record.get('event_number'),
        'event_date': record.get('event_date'),
        'event_name': record.get('event_name'),
    }, 200


def build_event_date_variants(event_date):
    dates_to_try = [event_date]
    try:
        import re

        if re.match(r'^\d{4}-\d{2}-\d{2}$', event_date or ''):
            year, month, day = event_date.split('-')
            alt = f"{day}/{month}/{year}"
            if alt not in dates_to_try:
                dates_to_try.append(alt)
        if re.match(r'^\d{2}/\d{2}/\d{4}$', event_date or ''):
            day, month, year = event_date.split('/')
            alt = f"{year}-{month}-{day}"
            if alt not in dates_to_try:
                dates_to_try.append(alt)
    except Exception:
        pass
    return dates_to_try


def build_event_info_response(event_number, event_code, event_name, event_date, *, event_lookup, event_name_label='event_name'):
    if not event_date or (event_number is None and event_code is None and not event_name):
        return {'error': 'Provide event_date and one of event_number, event_code or event_name'}, 400

    record = event_lookup(
        event_number=event_number,
        event_code=event_code,
        event_name=event_name,
        dates_to_try=build_event_date_variants(event_date),
    )
    if not record:
        return {'error': 'Event not found'}, 404

    return {
        'event_number': record.get('event_number'),
        event_name_label: record.get('event_name'),
        'event_code': record.get('event_code'),
    }, 200