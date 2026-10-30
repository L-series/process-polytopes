"""Parse Callgrind costs; compressed function IDs are identities, not filenames."""
import collections
import re


def normalize(name):
    name = re.sub(r"'\d+$", '', name)
    return re.sub(r'\.(?:constprop|isra|part)\.\d+', '', name)


def parse_callgrind(path):
    names = {'fn': {}, 'fl': {}, 'ob': {}}
    nodes = {}
    edges = collections.defaultdict(lambda: {'calls': 0, 'instructions': 0})
    current = pending = None
    source = '???'
    event_count = ir_index = summary = None

    def decode(kind, value):
        match = re.match(r'^\((\d+)\)(?: (.*))?$', value)
        if not match:
            return value, value
        key, full = match.groups()
        if full is not None:
            names[kind][key] = full
        if key not in names[kind]:
            raise ValueError(f'Undefined Callgrind {kind} ID {key}')
        return key, names[kind][key]

    def node(value, definition=False):
        key, name = decode('fn', value)
        record = nodes.setdefault(key, {'name': name, 'file': '???', 'self_instructions': 0,
                                       'inclusive_instructions': 0, 'calls': 0})
        # cfn may precede fn and omit its file. Resolve the source at definition.
        if definition:
            record['file'] = source
        return key

    for raw in path.read_text().splitlines():
        line = raw.strip()
        if line.startswith('events:'):
            events = line.split()[1:]
            event_count, ir_index = len(events), events.index('Ir')
        elif line.startswith('summary:'):
            if ir_index is None:
                raise ValueError('Callgrind summary before events')
            summary = int(line.split()[1 + ir_index])
        elif line.startswith('fl='):
            _, source = decode('fl', line[3:])
        elif line.startswith(('fi=', 'fe=')):
            decode('fl', line[3:])  # Inline/include source positions, not fn identity.
        elif line.startswith('fn='):
            current, pending = node(line[3:], True), None
        elif line.startswith(('cfl=', 'cfi=')):
            decode('fl', line[4:])
        elif line.startswith('cfn='):
            pending = node(line[4:])
        elif line.startswith('calls='):
            if current is None or pending is None:
                raise ValueError('Callgrind call without endpoints')
            count = int(line[6:].split()[0])
            nodes[pending]['calls'] += count
            edges[current, pending]['calls'] += count
        elif line.startswith(('ob=', 'cob=')):
            decode('ob', line.split('=', 1)[1])
        elif current is not None and re.match(r'^[+*\-\d]', line):
            if event_count is None:
                raise ValueError('Callgrind costs before events')
            cost = int(line.split()[-event_count + ir_index])
            nodes[current]['inclusive_instructions'] += cost
            if pending is not None:
                edges[current, pending]['instructions'] += cost
                pending = None
            else:
                nodes[current]['self_instructions'] += cost
    if summary is None:
        raise ValueError('Callgrind output has no summary')
    if sum(f['self_instructions'] for f in nodes.values()) != summary:
        raise ValueError('Callgrind self-instruction accounting mismatch')
    # Contexts/clones are separate during parsing; merge their invocation costs
    # only after resolving definitions. Inclusive recursive costs overlap by design.
    merged = {}
    keys = {}
    for identity, record in nodes.items():
        key = record['file'], normalize(record['name'])
        keys[identity] = key
        target = merged.setdefault(key, {'file': key[0], 'name': key[1], 'self_instructions': 0,
                                         'inclusive_instructions': 0, 'calls': 0})
        for field in ['self_instructions', 'inclusive_instructions', 'calls']:
            target[field] += record[field]
    combined_edges = collections.defaultdict(lambda: {'calls': 0, 'instructions': 0})
    for (a, b), counts in edges.items():
        target = combined_edges[keys[a], keys[b]]
        for field in counts:
            target[field] += counts[field]
    result = {'total_instructions': summary,
              'functions': [f for f in merged.values() if f['inclusive_instructions'] or f['self_instructions']],
              'edges': [{'caller': {'file': a[0], 'name': a[1]},
                         'callee': {'file': b[0], 'name': b[1]}, **counts}
                        for (a, b), counts in combined_edges.items() if counts['instructions']]}
    if sum(f['self_instructions'] for f in result['functions']) != summary:
        raise ValueError('Merged Callgrind instruction accounting mismatch')
    return result


def normalize_instructions(profile, rows):
    for function in profile['functions']:
        for scope in ['self', 'inclusive']:
            value = function[f'{scope}_instructions']
            function[f'{scope}_instructions_per_cws'] = value / rows
            function[f'{scope}_instructions_per_call'] = value / function['calls'] if function['calls'] else None
    return profile
