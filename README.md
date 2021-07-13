# bucket

## How it works

### Build and Loadmodule

```shell
$ git clone https://github.com/yangcancai/bucket.git
$ cd bucket
$ make && redis-server --loadmodule modules/bucket.so

```

### LoadModule

- redis-server --loadmodule modules/bucket.so

### Bucket command 

- BUCKET.INSERT bucket key expired score field1 value1 field2 value2 ...
- BUCKET.GET bucket
- BUCKET.GET bucket key
- BUCKET.GET bucket key field 
- BUCKET.INCR bucket key field count
- BUCKET.RANGE bucket start_score end_score
- BUCKET.DEL bucket
- BUCKET.DEL bucket key
- BUCKET.DEL bucket key field

```shell
  $ redis-cli bucket.insert player id01 60 100 name foo money 100
  (integer) 1
  $ redis-cli bucket.get player
  1) 1) "name"
   2) "foo"
   3) "money"
   4) "100"
   5) "id"
   6) "id01"
  $ redis-cli bucket.get player id01  
  1) "name"
  2) "foo"
  3) "money"
  4) "100"
  5) "id"
  6) "id01"
  $ redis-cli bucket.get player id01 name
  1) "name"
  2) "foo"
  $ redis-cli bucket.incr player id01 money 100
  (integer) 200
  $ redis-cli bucket.range player 0 200
  1) 1) "name"
   2) "foo"
   3) "money"
   4) "200"
   5) "id"
   6) "id01"
```