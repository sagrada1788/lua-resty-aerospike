lua-resty-aerospike
========
Thanks for https://github.com/aviramha/aioaerospike

Dependence
========

```
luarocks install moses
luarocks install struct
luarocks install lmd5
luarocks install lua-cmsgpack
```
 
Synopsis
========

```lua
local aerospike = require 'resty.aerospike'
local as = aerospike:new()
as:set_timeout(10)
as:connect('127.0.0.1', 3000)

-- put
-- string key
local v, err = as:put('test', 'test', 'key', {a=3.14, b='1122222221abc', c={x=1,y='2',z={1,2}}, d={'ii','jj'}}, 3600)
-- number key
local v, err = as:put('test', 'test', 999, {x=1})

-- incr
local v, err = as:incr('test', 'test', 'key', {a=1.1})

-- get all
local v, err = as:get('test', 'test', 'key')

-- get bin
local v, err = as:get('test', 'test', 'key', {b=ngx.null, c=ngx.null})

-- delete
local v, err = as:delete('test', 'test', 'key')
```
