"""STO-55: completion evidence for one owned housekeeping work-probe read."""


def completed_probe(events, cursor, session, query):
    """Return an exclusive cursor only after this send's successful C -> Z.

    Parse/close ReadyForQuery replies do not prove execution. The caller arms a
    send gate after cursor, then supplies its exact session and query. A failed
    or superseded send cannot borrow completion from another execution.
    """
    sent = command = None
    for index in range(cursor, len(events)):
        event = events[index]
        if event.get('session') != session:
            continue
        kind = event.get('event')
        if kind == 'send_requested':
            if sent is not None:
                return None
            if event.get('query') == query and event.get('protocol') in ('P', 'Q'):
                sent = index
        elif sent is not None:
            if kind in ('server_error_forwarded', 'transport_closed'):
                return None
            if kind != 'protocol_response':
                continue
            if event.get('query') != query or event.get('tag') == 'E':
                return None
            if event.get('tag') == 'C':
                if event.get('command_tag') != 'SELECT 1' or command is not None:
                    return None
                command = index
            elif event.get('tag') == 'Z' and command is not None:
                if event.get('ready_status') != 'I':
                    return None
                return dict(session=session, send_cursor=sent, command_cursor=command,
                            ready_cursor=index, next_cursor=index + 1)
    return None
