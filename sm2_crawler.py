# -*- coding: utf-8 -*-
import requests, json, csv, re
from BeautifulSoup import BeautifulStoneSoup

class sm2Crawler():

	def __init__(self):

		self.base_url = "http://sm2api.alterianconnect.com/SM2Services/SearchService/"

		self.functions = ['GetResults', 'BeginGetResults', 'GetResultsBySession', 'GetAllCategories']

		self.result_ids = []

		self.duplication = False

	####
	#	get_results
	#	Fetches a finite amount of results from SM2's API. Accepts parmas detailed in SM2 docs. Returns as a BeautifulStoneSoup XML Object. 
	####


	def get_results(self, params):

		xml = requests.get(self.base_url+self.functions[0]+self._build_arg_string(params))

		if xml.status_code is 200:

			soup = BeautifulStoneSoup(xml.text)

			return soup

		else:

			raise Exception("Response Status not 200 OK, it is: "+str(xml.status_code))

	####
	#	begin_get_results
	#	This is for getting large amounts of results (> 100). Accepts a path to write data to csv, and parmas detailed in SM2 docs. 
	####

	def begin_get_results(self, path_to_write_to, params):

		xml = requests.get(self.base_url+self.functions[1]+self._build_arg_string(params))
		print self.base_url+self.functions[1]+self._build_arg_string(params)

		result_ids = []

		if xml.status_code is 200:

			soup = BeautifulStoneSoup(xml.text)

			session_id = soup.find("session").sessionid.contents[0]

			results = soup.findAll("searchresult")
			
			self._write_results(results, path_to_write_to)

			while self.duplication is False:

				params['sessionid'] = session_id

				results = BeautifulStoneSoup(requests.get(self.base_url+self.functions[2]+self._build_arg_string(params)).text).findAll("searchresult")

				self._write_results(results, path_to_write_to)

			
		else:

			raise Exception("Response Status not 200 OK, it is: "+str(xml.status_code))

	####
	#	get_all_categories
	#	Get's all categories, returns BeautifulStoneSoup object. 
	####

	def get_all_categories(self, params):

		xml = requests.get(self.base_url+self.functions[3]+self._build_arg_string(params))

		if xml.status_code is 200:

			soup = BeautifulStoneSoup(xml.text)

			return soup

		else:

			raise Exception("Response Status not 200 OK, it is: "+str(xml.status_code))


	def _write_results(self, results, path_to_write_to):

		for result in results:

			result_id = result.resultid.contents[0]

			if self.result_ids.count(result_id) == 0:

				self.result_ids.append(result_id)

				with open(path_to_write_to+"/sm2_data.csv", "a") as csvfile:

					writer = csv.writer(csvfile, delimiter=',', quotechar="|", quoting=csv.QUOTE_MINIMAL, dialect=csv.excel_tab)

					try:
						words = re.findall(r'\w+', result.resultcontent.contents[0],flags = re.UNICODE | re.LOCALE)

						data = [result_id, " ".join(words), result.resultpublished.contents[0], result.resultlocation.contents[0], result.authorlocation.contents[0], result.author.contents[0], result.gender.contents[0], result.authorage.contents[0], result.language.contents[0]]

						writer.writerow(data)
					except TypeError:
						pass
			else:

				self.duplication = True

	def _build_arg_string(self, params):

		keys = tuple(params.keys())

		arg_string = ""

		for key in keys:

			string = str(key)+"="+str(params[str(key)])

			if len(arg_string) == 0:

				arg_string = "?"+string

			else:

				arg_string = arg_string+"&"+string

		return arg_string


		result_ids = []


o = sm2Crawler()
o.begin_get_results("path_to_write_to", {'apikey':'your_api_key'})
# o.get_results({'apikey':'your_api_key'})
# o.get_all_categories({'apikey':'your_api_key'})
