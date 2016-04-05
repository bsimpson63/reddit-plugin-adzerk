# Polls Adzerk for current list of flights and saves the current targeting
# information to zookeeper (to be run periodically with upstart)

import adzerk_api
import json
from pylons import app_globals as g

KEYWORD_NODE = "/keyword-targets"
    
def update_global_keywords():
    active_flights = adzerk_api.Flight.list(is_active=True)

    keyword_target = set()

    # Count the number of flights targeting each sub/keyword
    for flight in active_flights:
        for keyword_list in flight.Keywords.split('\n'):
            for keyword in keyword_list.split(','):
                ks = keyword.strip()
                if ks.startswith('k.') or ks.startswith('!k.'):
                    keyword_target.add(ks)

    # Store results in zookeeper
    if g.zookeeper:
        g.zookeeper.ensure_path(KEYWORD_NODE)
        g.zookeeper.set(KEYWORD_NODE, json.dumps(list(keyword_target)))
