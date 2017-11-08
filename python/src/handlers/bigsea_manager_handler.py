
import ConfigParser
from lib.bigsea_manager import BrokerClient
from config import btr_otp_config


class BigseaManagerHandler:

	def __init__(self):
		self.preprocConfig = btr_otp_config.PREPROC_JOB_CONFIG 

	def getConfig(self):

		template = ""

		with open(self.preprocConfig, 'r') as t:
			for line in t:
				template = template + line

		return template

	def runJob(self):

		config = ConfigParser.RawConfigParser()

		# __file__ = os.path.join()
		config.read(self.preprocConfig)

		client = BrokerClient(config)
		app_id = client.execute_application()


		return app_id