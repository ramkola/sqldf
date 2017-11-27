import sqlparse
from sqlparse.sql import Comparison
from sqlparse.sql import Where

query = 'update table set column2 = 3, column3 = 7 where column = 2 and column3 = 5'

parsed = sqlparse.parse(query)[0]

tokens = parsed.tokens

for token in list(tokens):
	if isinstance(token, Comparison):
		print('Comparison: ', token)
	elif isinstance(token, Where):
		print('Where: ', str(token).split('where')[-1].split('and'))