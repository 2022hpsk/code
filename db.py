import copy
import os
import socket as _socket_mod
import ssl
import base64
import traceback
import urllib.parse
import pymysql
import json

PROXY_ENV_KEYS = ("http_proxy", "HTTP_PROXY", "https_proxy", "HTTPS_PROXY")

def _read_http_headers(sock, timeout=10):
    sock.settimeout(timeout)
    buf = b""
    while b"\r\n\r\n" not in buf:
        chunk = sock.recv(4096)
        if not chunk:
            break
        buf += chunk
        if len(buf) > 65536:
            break
    return buf

def _create_http_connect_socket(proxy_url: str, target_host: str, target_port: int, timeout: int):
    u = urllib.parse.urlparse(proxy_url)
    if u.scheme not in ("http", "https"):
        raise RuntimeError(f"仅支持 HTTP/HTTPS 代理（当前: {u.scheme}）")

    proxy_host = u.hostname
    proxy_port = u.port or (443 if u.scheme == "https" else 8080)
    proxy_user = urllib.parse.unquote(u.username) if u.username else None
    proxy_pass = urllib.parse.unquote(u.password) if u.password else None

    # 1) 先连到代理
    s = _socket_mod.create_connection((proxy_host, proxy_port), timeout=timeout)

    # 若代理是 https，对“到代理”的链路做 TLS
    if u.scheme == "https":
        ctx = ssl.create_default_context()
        # 如需忽略代理证书校验（不推荐）:
        # ctx.check_hostname = False
        # ctx.verify_mode = ssl.CERT_NONE
        s = ctx.wrap_socket(s, server_hostname=proxy_host)

    # 2) 向代理发 CONNECT
    connect_line = f"CONNECT {target_host}:{target_port} HTTP/1.1\r\n"
    headers = [
        connect_line,
        f"Host: {target_host}:{target_port}\r\n",
        "Proxy-Connection: Keep-Alive\r\n",
        "Connection: Keep-Alive\r\n",
    ]
    if proxy_user is not None and proxy_pass is not None:
        token = base64.b64encode(f"{proxy_user}:{proxy_pass}".encode("utf-8")).decode("ascii")
        headers.append(f"Proxy-Authorization: Basic {token}\r\n")
    headers.append("\r\n")

    s.sendall("".join(headers).encode("utf-8"))

    # 3) 检查 200
    resp = _read_http_headers(s, timeout=timeout)
    if not resp:
        s.close()
        raise RuntimeError("代理无响应（CONNECT 无返回）")

    try:
        status_line = resp.split(b"\r\n", 1)[0].decode("iso-8859-1")
        code = int(status_line.split(" ", 2)[1])
    except Exception:
        s.close()
        raise RuntimeError(f"无法解析代理返回: {resp[:200]!r}")

    if code != 200:
        s.close()
        hdr_preview = resp[:400].decode("iso-8859-1", errors="replace")
        raise RuntimeError(f"代理拒绝 CONNECT，状态码 {code}。返回头:\n{hdr_preview}")

    s.settimeout(timeout)
    return s

def _monkeypatch_create_connection_for_mysql(mysql_host: str, mysql_port: int, timeout_default: int = 10):
    """
    替换 socket.create_connection：
    如果目标是 (mysql_host, mysql_port) 并且检测到 http(s)_proxy，
    则通过代理发起 CONNECT，返回打通后的 socket；否则走原始实现。
    """
    original_create_connection = _socket_mod.create_connection

    def patched_create_connection(address, timeout=None, source_address=None):
        host, port = address[0], int(address[1])
        # 仅拦截到 MySQL 的连接
        if host == mysql_host and port == mysql_port:
            proxy_url = None
            for k in PROXY_ENV_KEYS:
                if os.environ.get(k):
                    proxy_url = os.environ[k]
                    break
            if proxy_url:
                # 用代理建隧道，返回 socket
                to = timeout if timeout is not None else timeout_default
                return _create_http_connect_socket(proxy_url, host, port, to)
        # 其它连接（如 DNS 或别的外连）仍走原函数
        return original_create_connection(address, timeout, source_address)

    # 打补丁
    _socket_mod.create_connection = patched_create_connection
    return original_create_connection  # 若需恢复，可用它还原


def _get_gameinfo(db_config, input_sql):
    """
    连接数据库并执行查询。
    返回 (markdown_table_str, results_list)；
    若出错，则返回 ("error: ...", None)
    """
    try:
        # 现在 pymysql.connect 会在内部调用 socket.create_connection，
        # 被我们的猴补拦截并经代理发起 CONNECT。
        print(f"db_config: {db_config}")
        with pymysql.connect(**db_config) as connection:
            with connection.cursor() as cursor:
                sql_query = input_sql
                cursor.execute(sql_query)
                results = cursor.fetchall()
                
                return results

    except pymysql.MySQLError as e:
        traceback.print_exc()
        return f"error: {str(e)}", None
    except Exception as e:
        traceback.print_exc()
        return f"error: {str(e)}", None

def get_scheme(table_list, db_config: dict) -> str:
    db_config["cursorclass"] = pymysql.cursors.DictCursor
    scheme_list = []
    for table in table_list:
        # input_sql = f'describe {table};'
        input_sql = f'show create table {table};'
        results = _get_gameinfo(db_config=db_config, input_sql=input_sql)
        scheme_list.append(results)
    return f'{scheme_list}'

if __name__ == "__main__":
    test_data = {         
            "table_list": [
                "dws_mgamejp_login_user_activity_di",
                "dim_vplayerid_vies_df"
        ]
    }
    get_scheme(test_data)