from pylons import g

from r2.models import PromoCampaign

class PromoCampaignByFlightIdCache():
    @classmethod
    def _cachekey(cls, flight_id):
        return "promo.flight.%d" % flight_id

    @classmethod
    def add(cls, campaign):
        cachekey = cls._cachekey(campaign.az_flight_id)
        g.cache.set(cachekey, campaign._fullname, time=60*60*24)

    @classmethod
    def get(cls, flight_id):
        fullname = g.cache.get(cls._cachekey(flight_id))

        if not fullname:
            q = PromoCampaign._query(
                PromoCampaign.c.az_flight_id == flight_id,
                data=True,
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
