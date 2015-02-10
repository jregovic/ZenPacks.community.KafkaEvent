#!/usr/bin/env python
######################################################################
#
# Copyright 2012 Zenoss, Inc.  All Rights Reserved.
#
######################################################################

import Globals
import zope.component
import zope.interface
import logging
from zope.component import getUtility
from Products.ZenEvents.events2.proxy import EventSummaryProxy
from Products.ZenModel.interfaces import IAction
from Products.ZenUtils.CyclingDaemon import CyclingDaemon
from zenoss.protocols.jsonformat import from_dict
from zenoss.protocols.protobufs.zep_pb2 import (EventSummary,
                                                STATUS_NEW,
                                                STATUS_ACKNOWLEDGED)
import urllib2
import json
from kafka.client import KafkaClient
from kafka.consumer import KafkaConsumer



DAEMON = "zenkafkaevent"
log = logging.getLogger("zen.kafkaevent")

class zenkafkaevent(CyclingDaemon):
	name = DAEMON

	def __init__(self, *args, **kwargs):
        	super(zenkafkaevent, self).__init__(*args, **kwargs)
		myconf = {'metadata_broker_list':"10.228.70.123:9092"}
		self.kafka = KafkaConsumer('events',**myconf)

		log.info("zenkafkaevent is starting...")		
		
	def processEvents(self,vals,critical,error,name,alias):
		for tuple in vals:
			target=tuple[0]
			value = tuple[1]
			url=tuple[2]
			log.info("target %s has value %s" % (target,value))
			summary = '%s current value %s has exceeded threshold of ' % (target,value)
			if value > error:
				if value > critical:
					sev=5
					summary = summary + str(critical)
				else:
					sev=4
					sumary = summary + str(error)
				if alias is not None:
					aliasChar = alias[0]
					aliasField = int(alias[1])-1
					log.info("Alias :: %s %s" % (aliasChar,aliasField))
					resource = target.split(aliasChar)[aliasField]
				else:
					resource = target
				evt=self._makeEvent(summary,resource.replace('_','.'),name,sev,"MetricThresholdExceeded",self.defClass,target,url)
				log.info("Sending the event")
				self.sendEvent(evt)

			
	def _makeEvent(self, summary,dev,comp,sev,key,eventClass):
		log.info("Creating event")
		event_message = summary
		evt = dict(
            device=dev,
            eventKey=key,
            component=comp,
            summary=event_message,
            agent='zenkafkaevent',
            severity=sev,
	    eventClass=eventClass,
		)
		log.info("evt :: %s"%(str(evt)))
		return evt
	def main_loop(self):
		while True:
             		m=self.kafka.next()
			log.info(m.value)
			eventData = json.loads(str(m.value))
			evt=self._makeEvent(eventData['summary'],eventData['device'].replace('_','.'),eventData['component'],3,"MetricThresholdExceeded",eventData['class'])
			log.info("Sending the event")
			self.sendEvent(evt)
if __name__ == "__main__":
    daemon = zenkafkaevent()
    daemon.run()

