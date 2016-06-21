from pylons import app_globals as g

from r2.models import PromoCampaign

class PromoCampaignByFlightIdCache():
    @classmethod
    def _cache_key(cls, flight_id):
        return "flightid:%s" % flight_id

    @classmethod
    def add(cls, campaign):
        key = cls._cache_key(campaign.external_flight_id)
        g.gencache.set(key, campaign._fullname, time=60*60*24)

    @classmethod
    def get(cls, flight_id):
        fullname = g.gencache.get(cls._cache_key(flight_id), stale=True)

        if not fullname:
            q = PromoCampaign._query(
                PromoCampaign.c.external_flight_id == flight_id,
            )
            q._limit = 1
            campaigns = list(q)
            if campaigns:
                campaign = campaigns[0]

                cls.add(campaign)

                return campaign._fullname
            else:
                return None
        else:
            return fullname
