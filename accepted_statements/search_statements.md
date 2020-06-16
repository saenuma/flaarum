# Search Statements Samples

## Sample 1
```
table: users
fields: email firstname
where:
	id > 2
	and id < 10
```

No need for the ending '::' since there is only one multi-line section in search statements.

The 'where:' section must come last.


## Sample 2
```
table: users
limit: 100
start_index: 200
order_by: firstname asc
```
order_by accepts for the second word either 'asc' for ascending and 'desc' for descending.

## Sample 3: For where values that contains space
```
table: test1
where:
	a = 'James Lala'
	and b in 'James Lala' 'Jamer Lak' Jk
```