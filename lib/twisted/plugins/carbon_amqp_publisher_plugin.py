from zope.interface import implements

from twisted.plugin import IPlugin
from twisted.application.service import IServiceMaker

from carbon import service
from carbon import conf

class CarbonAMQPPublisherServiceMaker(object):
    
    implements(IServiceMaker, IPlugin)
    tapname = "carbon-amqp-publisher"
    description = "Collect stats for graphite and publish them to amqp server"
    options = conf.CarbonAMQPPublisherOptions
    
    def makeService(self, options):
        """
        Construct a C{carbon-amqp-publisher} service.
        """
        return service.createPublisherService(options)

# Now construct an object which *provides* the relevant interfaces
serviceMaker = CarbonAMQPPublisherServiceMaker()

