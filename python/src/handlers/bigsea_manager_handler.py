
import ConfigParser
import os
from lib.bigsea_manager import BrokerClient
from config import btr_otp_config


class BigseaManagerHandler:

	def __init__(self):
		self.configFolder = btr_otp_config.PREPROC_JOB_CONFIG 

	def getConfig(self):

		template = ""

		with open(self.preprocConfig, 'r') as t:
			for line in t:
				template = template + line

		return template

	def runJob(self, job):

		config = ConfigParser.RawConfigParser()

		__file__ = os.path.join(self.configFolder, job+".cfg")
		config.read(__file__)

		client = BrokerClient(config)
		app_id = client.execute_application()


		return app_id