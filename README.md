# sqldf
Library for quering DataFrames using SQL.

## Overview
SQL DataFrame is a library used for querying multiple types of DataFrame using ANSI SQL syntax. DataFrames supported are:
1. RAW: usually used so the user doesn’t need to have pandas installed. Example of a RAW "DataFrame":
```python
list_of_dictionaries = [{'name': 'Rigo', 'age': 33}, {'name': 'Bruno', 'age': 33}]
```

2. pandas DataFrame.

3. pyspark DataFrame: sqldf uses pyspark DataFrame in the background to process big data. Note, that the DataFrame return will be a pyspark DataFrame

## How to use sqldf
```python
from sqldf import sqldf

# RAW DataFrame
inventory = [{'item': 'Banana', 'quantity': 33}, {'item': 'Apple', 'quantity': 2}]
orders = [{'order_number': 1, 'item': 'Banana', 'quantity': 10}, {'order_number': 2, 'item': 'Apple', 'quantity': 10}]
```

To select data from a DataFrame and also register a table in memory do the following:
```python
print('Inventory:')
inventory_pyspark_df = sqldf.sql(
	"""
	SELECT item,
           quantity AS quantity_available
      FROM inventory_table
	""",
	inventory,
	table='inventory_table')
inventory_pyspark_df.show()

print('Orders:')
orders_pyspark_df = sqldf.sql(
	"""
	SELECT order_number,
           item,
           quantity AS quantity_ordered
      FROM order_table
	""",
	orders,
	table='order_table')
orders_pyspark_df.show()
```

Since the table has been specified above, the table will be saved in memory. The next time you want to select data from the table jut do the following:
```python
# Get inventory below quantity of 10 so we can order more of these items.
print('Items low in quantity:')
inventory_low = sqldf.sql(
	"""
	SELECT item,
           quantity AS quantity_low
      FROM inventory_table
     WHERE quantity < {{ quantity }}
	""",
	quantity=10)
inventory_low.show()
```

Ge the orders that will be able to be fulfilled. Note that since we already registered these tables, we don’t need to specify the able again.
You can specify the table name if you want to use that later in another query.

```python
print('Orders with inventory: ')
orders_with_inventory = sqldf.sql(
	"""
	SELECT ot.*
	  FROM inventory_table it
	  JOIN order_table ot
	    ON it.item = ot.item
	 WHERE it.quantity >= ot.quantity
	"""
	)
orders_with_inventory.show()
```
