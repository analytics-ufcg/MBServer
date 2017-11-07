

from config import btr_otp_config


class BigseaManagerHandler:

	def __init__(self):
		self.jobTemplate = btr_otp_config.JOB_TEMPLATE

	def getTemplate(self):

		template = ""

		with open(self.jobTemplate, 'r') as t:
			for line in t:
				template = template + line + "\n"

		return template

	def runJob(self):
		return "teste"