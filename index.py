# import
import datetime
import nntplib
import pprint
import pymongo
import random
import re
import string
import sys
import time
import xml.etree.ElementTree as ET

from pymongo import MongoClient

# mongodb
client = MongoClient()
db = client['indexer']
collections = dict(articles = db['articles'], groups = db['groups'], releases = db['releases'])

# configuration
config = dict(headers=['From', 'Subject', 'Date', 'Newsgroups', 'Lines'])
server = dict(host = 'HOST', username = 'USER', password = 'PASS', port = '119')

# functions
def log():
	return

def process(g):
	print('(', g, ')', 'Processing headers.')

	memory = {}

	for group in collections['groups'].find({ "group": g }):
		# check that we've actually received articles from this group
		if(int(group['last']) > 0):
			# lookup unprocessed articles in group with regex wizardry
			for article in collections['articles'].find({ "group": g, "processed": False, "subject": { '$regex': '.*?(?P<parts>\d{1,3}\/\d{1,3}).*?\"(?P<name>.*?)\.(nzb|sample|mkv|Avi|mp4|vol|ogm|par|rar|sfv|nfo|nzb|web|srt|ass|mpg|txt|zip|wmv|ssa|r\d{1,3}|7z|tar|mov|divx|m2ts|rmvb|iso|dmg|sub|idx|rm|ac3|t\d{1,2}|u\d{1,3})', '$options': 'i' } }):
				matches = re.findall('.*?(?P<parts>\d{1,3}\/\d{1,3}).*?\"(?P<name>.*?)\.(nzb|sample|mkv|Avi|mp4|vol|ogm|par|rar|sfv|nfo|nzb|web|srt|ass|mpg|txt|zip|wmv|ssa|r\d{1,3}|7z|tar|mov|divx|m2ts|rmvb|iso|dmg|sub|idx|rm|ac3|t\d{1,2}|u\d{1,3})', article['subject'], re.IGNORECASE)

				if matches[0]:
					# confusing technobabble. essentially, we do everything in memory. screw i/o and database lag!
					if matches[0][2]:
						unique = matches[0][1] + '|' + article['from']
						
						if unique in memory:
							parts = matches[0][0]
							parts = parts.replace('-', '/')
							parts = parts.replace('~', '/')
							parts = parts.replace('of', '/')

							arr = parts.partition('/')

							if(arr[2]):
								currentPart = int(arr[0])
								partString = str(arr[0]) + '/' + str(int(arr[2]))

								m = re.findall('.*?(?P<parts>\d{1,3}\/\d{1,3}).*?', article['subject'], re.IGNORECASE)

								if(m[1]):
									p = m[1]
									p = p.replace('-', '/')
									p = p.replace('~', '/')
									p = p.replace('of', '/')

									pa = p.partition('/')

									segment = dict(bytes=article['bytes'], number=pa[0], mid=article['mid'])

									if str(int(arr[0])) in memory[unique]['files']:
										memory[unique]['files'][str(int(arr[0]))]['segments'].append(segment)
									else:
										f = {}
										f['name'] = article['subject']
										f['segments'] = []
										f['total'] = pa[2]

										memory[unique]['files'][str(int(arr[0]))] = f
										memory[unique]['files'][str(int(arr[0]))]['segments'].append(segment)

						else:
							parts = matches[0][0]
							parts = parts.replace('-', '/')
							parts = parts.replace('~', '/')
							parts = parts.replace('of', '/')

							arr = parts.partition('/')

							if(arr[2]):
								currentPart = int(arr[0])
								partString = str(arr[0]) + '/' + str(int(arr[2]))

								obj = {}
								obj['files'] = {}
								obj['group'] = article['group']
								obj['info'] = dict(files=int(arr[2]))
								obj['poster'] = article['from']
								obj['release'] = matches[0][1]
								obj['when'] = article['date']

								m = re.findall('.*?(?P<parts>\d{1,3}\/\d{1,3}).*?', article['subject'], re.IGNORECASE)

								if(m[1]):
									p = m[1]
									p = p.replace('-', '/')
									p = p.replace('~', '/')
									p = p.replace('of', '/')

									pa = p.partition('/')

									f = {}
									f['name'] = article['subject']
									f['segments'] = []
									f['total'] = pa[2]

									segment = dict(bytes=article['bytes'], number=pa[0], mid=article['mid'])
									f['segments'].append(segment)

									obj['files'][str(int(arr[0]))] = f

									memory[unique] = obj

			for key, value in memory.items():
				complete = int(0)
				total = int(len(value['files']))

				for a, b in value['files'].items():
					if int(b['total']) == int(len(b['segments'])):
						complete = complete + 1

				if complete == total:
					# valid release
					print('Creating release:', value['release'])

					createRelease(value)

		else:
			print('(', group['group'], ')', 'No articles found for processing.')
			return

def createRelease(payload):
	# generate filename
	rand = str(random.randint(1, 10000))
	valid_chars = "-_.() %s%s" % (string.ascii_letters, string.digits)
	payload['filename'] = ''.join(c for c in payload['release'] if c in valid_chars)
	payload['filename'] = str(rand) + '_' + payload['filename']

	release = dict()
	release['filename'] = payload['filename']
	release['group'] = payload['group']
	release['name'] = payload['release']
	release['poster'] = payload['poster']
	release['when'] = payload['when']

	# insert release in to database
	collections['releases'].insert(release)

	# create nzb file
	createNzb(payload)

	return

def getNfo(payload):
	# this is where we'll download the nfo file
	log()

def createNzb(payload):
	# because of cross-platform issues with xml libraries, let's do this the old fashioned way
	f = open('nzb.xml', 'r')

	nzb = ''

	for key, value in payload['files'].items():
		s = '''<file poster=''' + payload['poster'] + ''' date="1353891054" subject="''' + value['name'] + '''">
<groups>
<group>''' + payload['group'] + '''</group>
</groups>
<segments>
'''

		for v in value['segments']:
			s = s + '''<segment bytes="''' + v['bytes'] + '''" number="''' + v['number'] + '''">''' + v['mid'] + '''</segment>
'''

			# whilst we're here, mark this header as processed
			update = {}
			update['processed'] = True
			
			collections['articles'].update({'mid': v['mid']}, {"$set": update}, upsert=False)

		s = s + '''</segments>
</file>
'''

		nzb = nzb + s

	b = f.read()
	b = b.replace('replace', nzb)

	# create, save and close nzb file
	fb = open('nzbs/' + payload['filename'] + '.nzb', 'w')
	fb.write(b)
	fb.close()

	return

def headers():
	# connect to nntp server
	s = nntplib.NNTP(server['host'], server['port'], server['username'], server['password'])

	# iterate over groups
	for group in collections['groups'].find():
		print('Beginning update cycle for', group['group'])

		# get group statistics
		resp, count, first, last, name = s.group(group['group'])

		print('Group', name, 'has', count, 'articles, range', first, 'to', last)

		if int(group['last']) > 0:
			# this group has been indexed before, what's new?
			diff = last - int(group['last'])
			start = last - diff

			print('(', group['group'], ')', 'Retrieving', diff, 'articles.')

			resp, overviews = s.over((start, last))
		else:
			# ooh, new group! let's grab a quarter of a million articles.
			diff = 100000

			print('(', group['group'], ')', 'Retrieving', diff, 'articles.')

			resp, overviews = s.over((last - 100000, last))

		if diff > 0:
			# okay, we've got some new articles, let's insert them.
			articles = []

			for id, over in overviews:
				article = dict()
				article['bytes'] = over[':bytes']
				article['date'] = over['date']
				article['from'] = over['from']
				article['group'] = group['group']
				article['imported'] = datetime.datetime.utcnow()
				article['lines'] = over[':lines']
				article['mid'] = over['message-id']
				article['processed'] = False
				article['references'] = over['references']
				article['subject'] = over['subject']
				article['xref'] = over['xref']

				try:
					collections['articles'].insert(article)
				except:
					# when there is an issue with unicode encoding, this exception is generally thrown.
					log()

			# update id of last inserted article
			update = dict(last=last)
			collections['groups'].update({'_id': group['_id']}, {"$set": update}, upsert=False)

			print('(', group['group'], ')', diff, 'message headers inserted.')

			# shift group to processing mode
			process(group['group'])
		else:
			print('(', group['group'], ')', 'No new articles found.')

	s.quit()

# bootstrap
headers()