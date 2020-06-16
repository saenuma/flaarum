# Search Statement Samples

## Statement 1

```
table: users
fields: name email
limit: 10
order_by: reg_dt asc
```

## Statement 2
```
table: users distinct
fields: name
limit: 150
```

## Statement 3
```
table: grades expand
fields: userid.firstname userid.surname grade
order_by: userid.firstname
where:
  userid.age > 20
  and userid.age < 50
```

## Statement 4
```
table: grades
where:
  score < 90
  or id = 3
  and score > 60
```

## Statement 5
a where condition that contains space.

```
table: grades
where:
  score < 90
  and remark = 'not suspicious'
```

## Statement 6
'in' queries
```
table: users
where:
  id in 1 13 15 3
  and name in 'James John' 'John Paul' 'Paulo liv'
```

## Statement 7
'isnull' queries. To find fields which were not set.
```
table: users
where:
  age isnull
  and id > 300
```

## Statement 8
'notnull' queries. To find the fields which are set.

```
table: users
where:
  email notnull
```

## Statement 9
'nin' queries
```
table: users
where:
  id nin 1 3 32 42
```