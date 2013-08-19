scriptencoding utf-8

function! etcd#get(key)
  let res = webapi#http#get(printf('http://127.0.0.1:4001/v1/keys%s', a:key))
  if res.status != '200'
    throw res.message
  endif
  return map(webapi#json#decode(res.content), '{v:val["key"]: v:val["value"]}')
endfunction

function! etcd#set(key, value)
  let res = webapi#http#post(printf('http://127.0.0.1:4001/v1/keys%s', a:key), {
  \  'value': a:value,
  \})
  if res.status != '200'
    throw res.message
  endif
  return webapi#json#decode(res.content)
endfunction
