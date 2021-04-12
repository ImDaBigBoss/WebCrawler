import requests
import sys
import os

import threading
import _thread
import time

from urllib.parse import urlparse, urljoin
import urllib.robotparser as urobot
import urllib.request
from bs4 import BeautifulSoup, SoupStrainer

#globals
global AGENT_NAME
global IGNORE_ROBOTS
global THREAD_COUNT
global CAN_RUN

global url_queue
global CAN_RUN

AGENT_NAME = "ImDaBigBoss-Crawler"
IGNORE_ROBOTS = True
THREAD_COUNT = 100
CAN_RUN = True

url_queue = ["https://example.com/"]
crawled_urls = []

#thread lock
lock = threading.Lock()
crawl_lock = threading.Lock()

def thread_func(id):
	while (CAN_RUN):
		lock.acquire()
		if not url_queue:
			lock.release()
			pass
		else:
			url = url_queue.pop(0)
			lock.release()

			crawl_lock.acquire()
			if ((url in crawled_urls) == False):
				crawled_urls.append(url)
				crawl_lock.release()

				#print("Thread " + str(id) + ": " + url)
				crawl_page(url)
			else:
				crawl_lock.release()

	print("Thread " + str(id) + " exited")

def add_to_sites_db(url, title):
	if (title != None):
		print("Added: \"" + title.string + "\" -> \"" + url + "\"")
	else:
		print("Null title: \"" + url + "\"")
	pass

def canuse(baseurl, path):
	parser = urobot.RobotFileParser()
	parser.set_url(urljoin(baseurl, 'robots.txt'))
	parser.read()
	canParse = False

	if (parser.can_fetch(AGENT_NAME, path)):
		canParse = True
	if (parser.can_fetch(AGENT_NAME, urljoin(baseurl, path))):
		canParse = True

	return canParse

def get_page_details(url, contents):
	parsed_url = urlparse(url)
	parser = 'html.parser'
	soup = BeautifulSoup(contents, parser, parse_only=SoupStrainer('a'))
	title_soup = BeautifulSoup(contents, parser, parse_only=SoupStrainer('title'))

	add_to_sites_db(url, title_soup.title)
	
	for link in soup.find_all('a', href=True):
		lock.acquire()

		new_link = link['href'].split("?")[0].split('#')[0]
		if (new_link.startswith("/")):
			url_queue.append('{uri.scheme}://{uri.netloc}'.format(uri=parsed_url) + new_link)
		elif (new_link.startswith("https://")):
			url_queue.append(new_link)
		elif (new_link.startswith("http://")):
			url_queue.append(new_link)

		lock.release()

def crawl_page(url):
	response = requests.get(url)
	if (response.status_code == 200):
		parsed_url = urlparse(url)
		canCrawl = False
		if (canuse('{uri.scheme}://{uri.netloc}/'.format(uri=parsed_url), parsed_url.path)):
			canCrawl = True

		if (canCrawl or IGNORE_ROBOTS):
			get_page_details(url, response.content)
	else:
		print("Failed to access \"" + url + "\", error code: " + str(response.status_code))

if __name__ == '__main__':
	try:
		for i in range(THREAD_COUNT):
			_thread.start_new_thread(thread_func, (i, ))
			print("Thread initialised " + str(i+1) + "/" + str(THREAD_COUNT))

		while True:
			time.sleep(1)
			lock.acquire()
			print("--- " + str(len(url_queue)) + " urls left to crawl ---")
			lock.release()
	except KeyboardInterrupt:
		print("Program exiting...")
		print("Successfully crawled " + str(len(crawled_urls)) + " urls!")
		CAN_RUN = False