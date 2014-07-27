#!/usr/bin/env python

from collections import defaultdict
import random; random.seed(None)
from uuid import uuid4 as new_key

class producer(object):
	def __init__(self, generator=random.uniform, limits=None):
		self.limits = limits if limits else (0, 1)
		self.produce = lambda a, b: generator(a, b)
	def __call__(self, transform=lambda x: x, limits=None):
		low, high, = limits if limits else self.limits
		return transform(self.produce(low, high))
	def __str__(self):
		limits = ','.join(map(str, self.limits))
		return '<procedure: {0} limits: ({1})>'.format(self.produce, limits)

def random_thingname(begin, end):
	options = [ 'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY' ]
	return random.choice(options[begin:end])

def random_lowerword(shortest, limit):
	from string import lowercase as source
	count = random.randint(shortest, limit)
	return ''.join(random.sample(source, count))

def new_row(*args):
	columns = [ generator(str) for _, generator in args ]
	#columns = [ arg(2)(str) for arg in args ]
	#return ','.join([ str(new_key()) ] + columns)
	return ','.join(columns)

distribution = defaultdict(producer)
distribution['name']  = producer(random_lowerword, (1,  3))
distribution['age']   = producer(random.randrange, (0, 99))
distribution['state'] = producer(random_thingname, (0,  7))

if __name__ == '__main__':
	columns = [ 'name', 'age', 'state' ]
	#print (','.join([ 'id' ] + sorted(columns)))
	print (','.join(columns))
	model = zip(columns, [ distribution[column] for column in columns ])
	for i in xrange(110000000):
		print new_row(*model)
