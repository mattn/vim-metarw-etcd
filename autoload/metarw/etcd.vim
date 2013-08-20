function! s:etcd_get(key)
  let res = webapi#http#get(printf('http://127.0.0.1:4001/v1/keys%s', a:key))
  if res.status != '200'
    throw res.message
  endif
  return webapi#json#decode(res.content)
endfunction

function! s:etcd_set(key, value)
  let res = webapi#http#post(printf('http://127.0.0.1:4001/v1/keys%s', a:key), { 'value': a:value })
  if res.status != '200'
    throw res.message
  endif
  return webapi#json#decode(res.content)
endfunction

function! metarw#etcd#complete(arglead, cmdline, cursorpos)
  if a:arglead !~ '[\/]$'
    let path = substitute(a:arglead, '/[^/]\+$', '', '')
  else
    let path = a:arglead[:-2]
  endif
  let _ = s:parse_incomplete_fakepath(path)
  let result = s:read_list(_)
  if result[0] == 'browse'
    return [filter(map(copy(result[1]), 'v:val["fakepath"]'), 'stridx(v:val, a:arglead)==0'), path, '']
  endif
  return [[], '', '']
endfunction

function! metarw#etcd#read(fakepath)
  let _ = s:parse_incomplete_fakepath(a:fakepath)
  if _.path == '' || _.path =~ '[\/]$'
    let result = s:read_list(_)
  else
    let result = s:read_content(_)
  endif
  return result
endfunction

function! metarw#etcd#write(fakepath, line1, line2, append_p)
  let _ = s:parse_incomplete_fakepath(a:fakepath)
  if _.path == '' || _.path =~ '[\/]$'
    echoerr 'Unexpected a:incomplete_fakepath:' string(a:incomplete_fakepath)
    throw 'metarw:etcd#e1'
  else
    let result = s:write_content(_, join(getline(a:line1, a:line2), "\n"))
  endif
  return result
endfunction

function! s:parse_incomplete_fakepath(incomplete_fakepath)
  let _ = {}
  let fragments = split(a:incomplete_fakepath, '^\l\+\zs:', !0)
  if len(fragments) <= 1
    echoerr 'Unexpected a:incomplete_fakepath:' string(a:incomplete_fakepath)
    throw 'metarw:etcd#e1'
  endif
  let _.given_fakepath = a:incomplete_fakepath
  let _.scheme = fragments[0]
  let _.path = fragments[1]
  if fragments[1] == ''
    let _.id = 'root'
  else
    let _.id = split(fragments[1], '[\/]', 1)[-1]
  endif
  return _
endfunction

function! s:response_to_result(_, response)
  let result = []
  for item in a:response
    let e = item['key'][1:]
    if has_key(item, 'dir')
      let e .= '/'
    endif
    call add(result, {
    \    'label': e,
    \    'fakepath': printf('%s:/%s',
    \                       a:_.scheme,
    \                       e)
    \ })
  endfor
  return result
endfunction

function! s:read_content(_)
  let response = s:etcd_get(substitute(a:_.path, '\\', '/', 'g'))
  if type(response) == 3
    return ['browse', s:response_to_result(a:_, response)]
  endif
  call setline(2, split(response['value'], "\n"))
  return ['done', '']
endfunction

function! s:write_content(_, content)
  try
    call s:etcd_set(substitute(a:_.path, '\\', '/', 'g'), a:content)
    return ['done', '']
  catch
    return ['error', v:exception]
  endtry
endfunction

function! s:read_list(_)
  try
    let response = s:etcd_get(substitute(a:_.path, '\\', '/', 'g'))
    if type(response) == 3
      return ['browse', s:response_to_result(a:_, response)]
    endif
    return ['browse', []]
  catch
    return ['error', v:exception]
  endtry
endfunction
