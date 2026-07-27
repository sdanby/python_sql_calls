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