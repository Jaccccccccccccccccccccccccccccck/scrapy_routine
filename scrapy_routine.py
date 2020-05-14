import logging
from time import time

from django.db import close_old_connections
from scrapy import signals
from scrapy.exceptions import DontCloseSpider
from twisted.internet import reactor


class RoutineSpiderMiddleware(object):

    log = logging.getLogger("RoutineSpiderMiddleware")

    def __init__(self, crawler):
        settings = crawler.settings
        self.last_call_rountine = time()
        self.rountine_interval = settings.get("ROUTINE_INTERVAL")
        self.later_call = None

    @classmethod
    def from_crawler(cls, crawler):
        mw = cls(crawler)
        crawler.signals.connect(mw.spider_idle, signal=signals.spider_idle)
        crawler.signals.connect(mw.spider_closed, signal=signals.spider_closed)
        return mw

    def spider_idle(self, spider):
        if self.rountine_interval:
            self.schedule_rountine_requests(spider)
            raise DontCloseSpider()

    def spider_closed(self, spider):
        if self.later_call and self.later_call.active():
            self.later_call.cancel()

    def schedule_rountine_requests(self, spider, scheduled=False):
        penalty = self.last_call_rountine + self.rountine_interval - time()
        if scheduled or not penalty > 0:
            self.log.debug("Request scheduled now")
            self.last_call_rountine = time()
            close_old_connections()
            for request in self.rountine_requests(spider):
                spider.crawler.engine.schedule(request, spider)
        elif not (self.later_call and self.later_call.active()):
            self.log.debug("Request scheduled after %d seconds" % penalty)
            self.later_call = reactor.callLater(
                penalty, self.schedule_rountine_requests, spider, True
            )

    def rountine_requests(self, spider):
        return iter(spider.start_requests())
