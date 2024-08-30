class Event:
    def __init__(self, id, orgc_id, org_id, date, threat_level_id, info, published, uuid, attribute_count, analysis, timestamp, distribution, proposal_email_lock, locked, publish_timestamp, sharing_group_id, disable_correlation, extends_uuid, protected, event_creator_email):
        self.id = id
        self.orgc_id = orgc_id
        self.org_id = org_id
        self.date = date
        self.threat_level_id = threat_level_id
        self.info = info
        self.published = published
        self.uuid = uuid
        self.attribute_count = attribute_count
        self.analysis = analysis
        self.timestamp = timestamp
        self.distribution = distribution
        self.proposal_email_lock = proposal_email_lock
        self.locked = locked
        self.publish_timestamp = publish_timestamp
        self.sharing_group_id = sharing_group_id
        self.disable_correlation = disable_correlation
        self.extends_uuid = extends_uuid
        self.protected = protected
        self.event_creator_email = event_creator_email

class Feed:
    def __init__(self, feed_id, feed_name, feed_url, feed_provider, feed_source_format, feed_lookup_visible, feed_event_uuids):
        self.feed_id = feed_id
        self.feed_name = feed_name
        self.feed_url = feed_url
        self.feed_provider = feed_provider
        self.feed_source_format = feed_source_format
        self.feed_lookup_visible = feed_lookup_visible
        self.feed_event_uuids = feed_event_uuids# This is in events get request and not in feeds get request
        self.distribution = distribution  # This is in feeds get request and not in events get request

class Org:
    def __init__(self, org_id, org_name, org_uuid, org_local):
        self.org_id = org_id
        self.org_name = org_name
        self.org_uuid = org_uuid
        self.org_local = org_local

class Orgc:
    def __init__(self, orgc_id, orgc_name, orgc_uuid, orgc_local):
        self.orgc_id = orgc_id
        self.orgc_name = orgc_name
        self.orgc_uuid = orgc_uuid
        self.orgc_local = orgc_local