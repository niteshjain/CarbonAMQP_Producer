#!/usr/bin/env python
"""
Copyright 2009 Lucio Torre <lucio.torre@canonical.com>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Will publish metrics over AMQP
"""
import os
import time
from optparse import OptionParser

from twisted.internet.defer import inlineCallbacks
from twisted.internet import reactor, task
from twisted.internet.protocol import ClientCreator
from txamqp.protocol import AMQClient
from txamqp.client import TwistedDelegate
from txamqp.content import Content
import txamqp.spec
from twisted.application.service import Service
from carbon.conf import settings
from carbon import log, instrumentation
from carbon.cache import MetricCache
from carbon import state


@inlineCallbacks
def writeMetric(metricList, host, port, username, password,
                vhost, exchange, queue, spec=None, channel_number=1, ssl=False):
    
    global conn
    if not spec:
        spec = txamqp.spec.load(os.path.normpath(
            os.path.join(os.path.dirname(__file__), 'amqp0-8.xml')))

    delegate = TwistedDelegate()
   
    connector = ClientCreator(reactor, AMQClient, delegate=delegate,
                              vhost=vhost, spec=spec)
    if ssl:
        from twisted.internet.ssl import ClientContextFactory
        conn = yield connector.connectSSL(host, port, ClientContextFactory())
    else:
        conn = yield connector.connectTCP(host, port, timeout=130)
            
    yield conn.authenticate(username, password)
       
    channel = yield conn.channel(channel_number)
    yield channel.channel_open()
  
    yield channel.exchange_declare(exchange=exchange, type="topic", durable=True, auto_delete=False)
    
    #reply = yield channel.queue_declare(queue = queue, durable = True)
    #my_queue = reply.queue

    #Pickup settings.BIND_PATTERNS somewhere else
    #for bind_pattern in settings.BIND_PATTERNS:
    #  yield channel.queue_bind(exchange=exchange, queue=my_queue, routing_key=bind_pattern)
    
    for (metric, datapoints) in metricList:
      body = ""
      for point in datapoints:
        temp = "%f %d\n"%(point[1], point[0])
        body = body + temp 
      message = Content(body)
      message["delivery mode"] = 2
      channel.basic_publish(exchange=exchange, content=message, routing_key=metric)       
      
    yield channel.channel_close()
    yield conn.close("Publish Done. Closing Connection.")
  

    
#def main():
#    parser = OptionParser(usage="%prog [options] <metric> <value> [timestamp]")
#    parser.add_option("-t", "--host", dest="host",
#                      help="host name", metavar="HOST", default="localhost")
#
#    parser.add_option("-p", "--port", dest="port", type=int,
#                      help="port number", metavar="PORT",
#                      default=5672)
#
#    parser.add_option("-u", "--user", dest="username",
#                      help="username", metavar="USERNAME",
#                      default="guest")
#
#    parser.add_option("-w", "--password", dest="password",
#                      help="password", metavar="PASSWORD",
#                      default="guest")
#
#    parser.add_option("-v", "--vhost", dest="vhost",
#                      help="vhost", metavar="VHOST",
#                      default="/")
#
#    parser.add_option("-s", "--ssl", dest="ssl",
#                      help="ssl", metavar="SSL", action="store_true",
#                      default=False)
#
#    parser.add_option("-e", "--exchange", dest="exchange",
#                      help="exchange", metavar="EXCHANGE",
#                      default="graphite")
#
#    (options, args) = parser.parse_args()
#
#    try:
#      metric_path = args[0]
#      value = float(args[1])
#
#      if len(args) > 2:
#        timestamp = int(args[2])
#      else:
#        timestamp = time.time()
#
#    except:
#      parser.print_usage()
#      raise SystemExit(1)
#
#    d = writeMetric(metric_path, value, timestamp, options.host, options.port,
#                    options.username, options.password, vhost=options.vhost,
#                    exchange=options.exchange, ssl=options.ssl)
#    d.addErrback(lambda f: f.printTraceback())
#    d.addBoth(lambda _: reactor.stop())
#    reactor.run()


#if __name__ == "__main__":
#    main()

CACHE_SIZE_LOW_WATERMARK = settings.MAX_CACHE_SIZE * 0.95
NUM_of_PUBLISHES_per_CONNECTION = 1000

class PublishMetrics:

  def __init__(self):
      self.metricList = []
      
      
  #def backIntoCache(self, fail):
      #for (metric, datapoints) in self.metricList:
          #for point in datapoints:
              #MetricCache.store(metric, point)

  def successful(self,_):
      log.amqpPublisher("Completed publishing %d (metric, datapoints) tuples"%(len(self.metricList)))
      self.metricList = []

  @inlineCallbacks      
  def tryAgain(self,_):
      log.amqpPublisher("Failed to publish %d (metric, datapoints) tuples"%(len(self.metricList)))
      try: 
          yield conn.close("Closing Connection")
          log.amqpPublisher("Closing connection after failing to publish")
      except Exception, e:
          log.amqpPublisher("Error in closing connection: %s"%(repr(e))) 
      #pass

  def nextCall(self,_): 
      reactor.callLater(1, self.publish)
        
  def getMetrics(self):
      metrics = MetricCache.counts()
      for metric, queueSize in metrics:
          datapoints = MetricCache.pop(metric)
          if state.cacheTooFull and MetricCache.size < CACHE_SIZE_LOW_WATERMARK:
              events.cacheSpaceAvailable() 
          yield (metric, datapoints)
     

  def publish(self):
      try:
          if (len(self.metricList) > 0) or MetricCache:
              if len(self.metricList) < NUM_of_PUBLISHES_per_CONNECTION :
                  for (metric, datapoints) in self.getMetrics():
                      if(len(metric)>255):
                          continue  
                      self.metricList.append((metric, datapoints))
                      if len(self.metricList) >= NUM_of_PUBLISHES_per_CONNECTION :
                          break  
              d = writeMetric(self.metricList, settings.AMQP_PUB_HOST, settings.AMQP_PUB_PORT, settings.AMQP_PUB_USER, settings.AMQP_PUB_PASSWORD, settings.AMQP_PUB_VHOST, settings.AMQP_PUB_EXCHANGE, settings.AMQP_PUB_QUEUE)                    
              d.addCallbacks(self.successful, self.tryAgain)
              d.addBoth(self.nextCall) 
              
          else: 
              reactor.callLater(10, self.publish)
      except:
          reactor.callLater(15, self.publish) 

  def logMetrics(self):
      for (metric, datapoints) in self.getMetrics():
          self.metricList.append((metric, datapoints))
      for (metric, datapoints) in self.metricList:
          for point in datapoints:
              log.unpublishedMetrics("%s %f %d"%(metric, point[1], point[0]))  
       
#Handle Shutdowns. U can log all the metrics in MetricCache and MetricList to a file during stopService

class PublisherService(Service):

  def startService(self):
      Publisher = PublishMetrics()
      reactor.callWhenRunning(Publisher.publish)
      reactor.addSystemEventTrigger('before', 'shutdown',Publisher.logMetrics)
      Service.startService(self) 

  def stopService(self): 
      Service.stopService(self)

  
