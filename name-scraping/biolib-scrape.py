import requests
import re
from collections import deque
from bs4 import BeautifulSoup
import pymongo
import time
import random


MONGO_URI = "mongodb://localhost:27017"
MONGO_DATABASE = "biolib"
MONGO_COLLECTION = "taxons"
QUEUE_FILE = "./name-scraping/queue.txt"


LEVEL = {
	'doména': 55, 'nadříše': 58, 'říše': 60, 'podříše': 63, 'oddělení': 70, 'pododdělení': 73, 'nadkmen': 78,
	'kmen': 80, 'podkmen': 85, 'infrakmen': 86, 'kruh': 87, 'nadtřída': 88, 'třída': 90, 'podtřída': 91,
	'infratřída': 92, 'parvtřída': 93, 'část': 94, 'legie': 95, 'kohorta': 96, 'nadřád': 98, 'řád': 100,
	'hyperřád': 101, 'podřád': 103, 'infrařád': 104, 'parvřád': 105, 'falanx': 106, 'nadčeleď': 108, 'čeleď': 110,
	'podčeleď': 113, 'nadtribus': 120, 'tribus': 123, 'podtribus': 125, 'skupina rodů': 129, 'rod': 130,
	'podrod': 133, 'sekce': 140, 'podsekce': 142, 'skupina druhů': 143, 'podskupina druhů': 144, 'agregát': 145,
	'druh': 150, 'klepton': 151, 'poddruh': 153, 'hybrid': 154, 'skupina': 155, 'mezirodový hybrid': 156,
	'nothosubspecies': 157, 'chiméra': 159, 'convarieta': 160, 'varieta': 163, 'forma': 170, 'podforma': 173,
	'kultivar': 175, 'ekotyp': 176, 'skupina plemen': 177, 'sekce plemen': 178, 'plemeno': 179, 'ráz': 180,
	'lusus': 190, None: -1
}
PTN_TAXON = re.compile(r'(?:\s*(?P<level>[\w ]+?\w)\s*)?<a href="/cz/taxon/id(?P<id>\d+)/">(?P<lat>.+?)</a>.+?(?:<b>(?P<cs>.+?)</b>)?<br/>')
PTN_NEXTPAGE = re.compile(r'<a\shref="(.+?)">Další\s&gt;&gt;</a>')
URL_TAXONTREE = "https://www.biolib.cz/cz/taxontree/id{taxon_id}/"


def generate_taxons(node, parent_id=None):
	subtaxons = node.find_all(
		"div",
		{"class": ["treediv", "treeenddiv", "treecontdiv", "treebodydiv", "treebodyenddiv"]},
		recursive=False
	)
	for sub in subtaxons:
		# Detach parent if lists starts with ...
		if "treecontdiv" in sub["class"]:
			parent_id = None
			continue
		if "treebodydiv" in sub["class"] or "treebodyenddiv" in sub["class"]:
			yield from generate_taxons(sub, parent_id)
			continue
		m = PTN_TAXON.search(str(sub))
		if not m:
			raise ValueError(f"No match '{str(sub)[:50]}'")
		m = m.groupdict()
		_id = int(m["id"])
		taxon = {"_id": _id, "level": LEVEL[m["level"]], "lat": m["lat"], "cs": m["cs"]}
		if parent_id is not None:
			taxon["parent"] = parent_id

		if taxon["level"] < 150:
			expanded = False
			for s_id, s_taxon in generate_taxons(sub, _id):
				expanded = True
				yield (s_id, s_taxon)
			# Only visit the page if subtaxons are not visible in the current view
			taxon["to_visit"] = not expanded
		else:
			taxon["to_visit"] = False

		yield (_id, taxon)

	return None


def id_from_url(url):
	return int(re.search(r"/id(\d+)/", url)[1])


def should_visit_url(url, collection):
	_id = id_from_url(url)
	if "?" in url or "," in url:
		return True
	db_taxon = collection.find_one({"_id": _id})
	if not db_taxon:
		return True
	return db_taxon.get("to_visit", False)


def scrape_page(url, queue: deque, mongo_collection: pymongo.collection.Collection):
	_id_page = id_from_url(url)
	print(f"{url} ... ", end="")
	r = requests.get(url)
	print(f"{r.status_code} ... ", end="")
	if r.status_code != 200:
		raise RuntimeError(r.status_code)

	bs = BeautifulSoup(r.text, "lxml")

	treearea = bs.find("div", {"class": "treeareadiv"})
	if not treearea:
		# No results because of auto filter
		# Retry with custom filter
		if "treetaxcat" in url:
			# Already tried
			print(f"no tree ... skipping")
			_id = id_from_url(url)
			mongo_collection.find_one_and_update({"_id": _id}, {"$set": {"to_visit": False}})
			return
		db_taxon = mongo_collection.find_one({"_id": _id_page})
		level = db_taxon["level"] + 20
		new_url = f"{URL_TAXONTREE.format(taxon_id=_id_page)}?count=100&treetaxcat={level}"
		queue.append(new_url)
		print(f"no tree ... revisit {new_url}")
		return

	n_pages = 0
	for _id, taxon in generate_taxons(treearea, _id_page):
		if _id == _id_page:
			# Do not expand if page links to itself
			taxon["to_visit"] = False
		n_pages += 1
		db_taxon = mongo_collection.find_one({"_id": _id})
		if not db_taxon:
			mongo_collection.find_one_and_update({"_id": _id}, {"$set": taxon}, upsert=True)
		else:
			# print(f"🟩 {taxon}")
			# print(f"🟥 {db_taxon}")
			taxon["to_visit"] = False
			mongo_collection.find_one_and_update({"_id": _id}, {"$set": taxon}, upsert=True)
	nextpage = PTN_NEXTPAGE.search(str(bs))
	if nextpage:
		n_pages += 1
		queue.append("https://www.biolib.cz" + nextpage[1])
	print(f"{n_pages} pages")


def main():
	client = pymongo.MongoClient(MONGO_URI)
	db = client.get_database(MONGO_DATABASE)
	collection = db.get_collection(MONGO_COLLECTION)

	queue = deque()
	try:
		with open(QUEUE_FILE, "r") as f:
			for line in f:
				queue.append(line.strip())
	except FileNotFoundError:
		pass

	if len(queue) == 0:
		plantae_id = 14871
		queue.append(URL_TAXONTREE.format(taxon_id=plantae_id))

	visited = set()
	url = None
	try:
		for i in range(5000):
			if queue:
				url = queue.popleft()
				if url in visited:
					print(f"URL already visited: {url}")
					_id = id_from_url(url)
					collection.find_one_and_update({"_id": _id}, {"$set": {"to_visit": False}})
					continue
				if not should_visit_url(url, collection):
					print(f"Taxon already scraped: {url}")
					_id = id_from_url(url)
					collection.find_one_and_update({"_id": _id}, {"$set": {"to_visit": False}})
					continue
				time.sleep(min(1.6, 0.1 + abs(random.normalvariate(0, 0.7))))
				scrape_page(url, queue, collection)
				visited.add(url)
			else:
				c = 0
				for taxon in collection.aggregate([
					{"$match": {"to_visit": True, "level": {"$lt": 130}}},
					{"$lookup": {"from": "taxons", "localField": "_id", "foreignField": "parent", "as": "docs"}},
					{"$addFields": {"subtaxons": {"$size": "$docs"}}},
					{"$project": {"docs": 0}}, {"$sort": {"subtaxons": 1}}
				]):
					c += 1
					queue.append(URL_TAXONTREE.format(taxon_id=taxon["_id"]) + "?count=100&treetaxcat=150")
				print(f"Added {c} URLs to queue")
				if c == 0:
					return
	finally:
		with open(QUEUE_FILE, "w") as f:
			if url is not None:
				print(url, file=f)
			for url in queue:
				print(url, file=f)


if __name__ == "__main__":
	main()
