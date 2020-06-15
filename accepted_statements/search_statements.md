# Search Statements Samples

## Sample 1
```
table: users
where:
	id > 2
	and id < 10
```

No need for the ending '::' since there is only one multi-line section in search statements.


## Sample 2
```
table: users
limit: 100
start_index: 200
order_by: firstname asc
```
order_by accepts for the second word either 'asc' for ascending and 'desc' for descending.
