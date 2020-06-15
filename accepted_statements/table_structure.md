# Table Structure Statements 

## Statement 1

```
table: users
fields:
	firstname string required
	surname string required
	email string required unique
	regdt datetime
	biography text
	is_active bool
::
```

### Note

1.	There are two fields created automatically: "id" and "\_version". The id is type int and is automatically incremented.

2.	The accepted field types are : int, float, string, text, bool, date, datetime

3.	The types 'text' would not be indexed.

## Statement 2

```
table: grades
fields:
	userid int required
	grade int required
	remarkid int
	year int
::
foreign_keys:
	userid users on_delete_delete
	remarkid remarks on_delete_empty
::
unique_groups:
	userid year
::
``` 

### Notes 

1.	The third word in a foreign key part is one of "on_delete_delete", "on_delete_empty", "on_delete_restrict"