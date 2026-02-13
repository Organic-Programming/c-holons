#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif

#include "holons/holons.h"

#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <netdb.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

static volatile sig_atomic_t g_stop_requested = 0;

static void set_err(char *err, size_t err_len, const char *fmt, ...) {
  va_list ap;

  if (err == NULL || err_len == 0) {
    return;
  }

  va_start(ap, fmt);
  (void)vsnprintf(err, err_len, fmt, ap);
  va_end(ap);
}

static int copy_string(char *dst, size_t dst_len, const char *src, char *err, size_t err_len) {
  size_t n;

  if (dst == NULL || dst_len == 0) {
    set_err(err, err_len, "invalid destination buffer");
    return -1;
  }
  if (src == NULL) {
    dst[0] = '\0';
    return 0;
  }

  n = strlen(src);
  if (n >= dst_len) {
    set_err(err, err_len, "string is too long");
    return -1;
  }

  (void)memcpy(dst, src, n + 1);
  return 0;
}

static char *ltrim(char *s) {
  while (*s != '\0' && isspace((unsigned char)*s)) {
    ++s;
  }
  return s;
}

static void rtrim(char *s) {
  size_t n = strlen(s);
  while (n > 0 && isspace((unsigned char)s[n - 1])) {
    s[n - 1] = '\0';
    --n;
  }
}

static char *trim(char *s) {
  char *start = ltrim(s);
  rtrim(start);
  return start;
}

static char *strip_quotes(char *value) {
  size_t len = strlen(value);
  if (len >= 2) {
    if ((value[0] == '"' && value[len - 1] == '"') ||
        (value[0] == '\'' && value[len - 1] == '\'')) {
      value[len - 1] = '\0';
      return value + 1;
    }
  }
  return value;
}

static int parse_port(const char *text, int *out_port, char *err, size_t err_len) {
  char *end = NULL;
  long value;

  if (text == NULL || *text == '\0') {
    set_err(err, err_len, "missing port");
    return -1;
  }

  errno = 0;
  value = strtol(text, &end, 10);
  if (errno != 0 || end == text || *end != '\0') {
    set_err(err, err_len, "invalid port: %s", text);
    return -1;
  }

  if (value < 0 || value > 65535) {
    set_err(err, err_len, "port out of range: %ld", value);
    return -1;
  }

  *out_port = (int)value;
  return 0;
}

static int parse_host_port(const char *input,
                           char *host,
                           size_t host_len,
                           int *port,
                           char *err,
                           size_t err_len) {
  const char *host_begin = input;
  const char *host_end = NULL;
  const char *port_begin = NULL;
  size_t host_n;

  if (input == NULL || *input == '\0') {
    set_err(err, err_len, "empty address");
    return -1;
  }

  if (input[0] == '[') {
    host_begin = input + 1;
    host_end = strchr(host_begin, ']');
    if (host_end == NULL) {
      set_err(err, err_len, "invalid IPv6 address: missing ']'");
      return -1;
    }
    if (host_end[1] != ':') {
      set_err(err, err_len, "missing port in address: %s", input);
      return -1;
    }
    port_begin = host_end + 2;
  } else {
    const char *last_colon = strrchr(input, ':');
    if (last_colon == NULL) {
      set_err(err, err_len, "missing port in address: %s", input);
      return -1;
    }
    host_end = last_colon;
    port_begin = last_colon + 1;
  }

  host_n = (size_t)(host_end - host_begin);
  if (host_n >= host_len) {
    set_err(err, err_len, "host is too long");
    return -1;
  }
  (void)memcpy(host, host_begin, host_n);
  host[host_n] = '\0';

  return parse_port(port_begin, port, err, err_len);
}

static int parse_ws_uri(const char *rest,
                        holons_uri_t *out,
                        char *err,
                        size_t err_len) {
  const char *slash = strchr(rest, '/');
  char host_port[256];

  if (slash == NULL) {
    if (copy_string(host_port, sizeof(host_port), rest, err, err_len) != 0) {
      return -1;
    }
    if (copy_string(out->path, sizeof(out->path), "/grpc", err, err_len) != 0) {
      return -1;
    }
  } else {
    size_t host_port_len = (size_t)(slash - rest);
    if (host_port_len >= sizeof(host_port)) {
      set_err(err, err_len, "websocket host:port is too long");
      return -1;
    }
    (void)memcpy(host_port, rest, host_port_len);
    host_port[host_port_len] = '\0';

    if (copy_string(out->path, sizeof(out->path), slash, err, err_len) != 0) {
      return -1;
    }
    if (out->path[0] == '\0') {
      if (copy_string(out->path, sizeof(out->path), "/grpc", err, err_len) != 0) {
        return -1;
      }
    }
  }

  return parse_host_port(host_port, out->host, sizeof(out->host), &out->port, err, err_len);
}

static int create_tcp_listener(const char *host, int port, int *out_fd, char *err, size_t err_len) {
  struct addrinfo hints;
  struct addrinfo *res = NULL;
  struct addrinfo *it;
  const char *bind_host = NULL;
  char service[16];
  int rc;
  int fd = -1;
  int last_errno = 0;

  (void)memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = AI_PASSIVE;

  if (host != NULL && host[0] != '\0') {
    bind_host = host;
  }

  (void)snprintf(service, sizeof(service), "%d", port);
  rc = getaddrinfo(bind_host, service, &hints, &res);
  if (rc != 0) {
    set_err(err, err_len, "getaddrinfo failed: %s", gai_strerror(rc));
    return -1;
  }

  for (it = res; it != NULL; it = it->ai_next) {
    int one = 1;

    fd = socket(it->ai_family, it->ai_socktype, it->ai_protocol);
    if (fd < 0) {
      last_errno = errno;
      continue;
    }

    (void)setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    if (bind(fd, it->ai_addr, it->ai_addrlen) == 0 && listen(fd, 128) == 0) {
      *out_fd = fd;
      freeaddrinfo(res);
      return 0;
    }

    last_errno = errno;
    (void)close(fd);
    fd = -1;
  }

  freeaddrinfo(res);
  set_err(err, err_len, "unable to bind/listen: %s", strerror(last_errno));
  return -1;
}

static int connect_tcp_socket(const char *host, int port, int *out_fd, char *err, size_t err_len) {
  struct addrinfo hints;
  struct addrinfo *res = NULL;
  struct addrinfo *it;
  const char *connect_host = host;
  char service[16];
  int rc;
  int fd = -1;
  int last_errno = 0;

  if (out_fd == NULL) {
    set_err(err, err_len, "output fd is required");
    return -1;
  }
  if (port < 0 || port > 65535) {
    set_err(err, err_len, "port out of range: %d", port);
    return -1;
  }

  if (connect_host == NULL || connect_host[0] == '\0' || strcmp(connect_host, "0.0.0.0") == 0) {
    connect_host = "127.0.0.1";
  }

  (void)memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;

  (void)snprintf(service, sizeof(service), "%d", port);
  rc = getaddrinfo(connect_host, service, &hints, &res);
  if (rc != 0) {
    set_err(err, err_len, "getaddrinfo failed: %s", gai_strerror(rc));
    return -1;
  }

  for (it = res; it != NULL; it = it->ai_next) {
    fd = socket(it->ai_family, it->ai_socktype, it->ai_protocol);
    if (fd < 0) {
      last_errno = errno;
      continue;
    }

    if (connect(fd, it->ai_addr, it->ai_addrlen) == 0) {
      *out_fd = fd;
      freeaddrinfo(res);
      return 0;
    }

    last_errno = errno;
    (void)close(fd);
    fd = -1;
  }

  freeaddrinfo(res);
  set_err(err, err_len, "unable to connect: %s", strerror(last_errno));
  return -1;
}

static int create_unix_listener(const char *path, int *out_fd, char *err, size_t err_len) {
  struct sockaddr_un addr;
  int fd;

  if (path == NULL || path[0] == '\0') {
    set_err(err, err_len, "unix path is empty");
    return -1;
  }
  if (strlen(path) >= sizeof(addr.sun_path)) {
    set_err(err, err_len, "unix path is too long");
    return -1;
  }

  fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (fd < 0) {
    set_err(err, err_len, "socket(AF_UNIX) failed: %s", strerror(errno));
    return -1;
  }

  (void)memset(&addr, 0, sizeof(addr));
  addr.sun_family = AF_UNIX;
  (void)strncpy(addr.sun_path, path, sizeof(addr.sun_path) - 1);

  (void)unlink(path);

  if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
    set_err(err, err_len, "bind(%s) failed: %s", path, strerror(errno));
    (void)close(fd);
    return -1;
  }

  if (listen(fd, 128) != 0) {
    set_err(err, err_len, "listen(%s) failed: %s", path, strerror(errno));
    (void)close(fd);
    (void)unlink(path);
    return -1;
  }

  *out_fd = fd;
  return 0;
}

static int format_bound_uri(int fd,
                            holons_scheme_t scheme,
                            const char *path,
                            char *out_uri,
                            size_t out_uri_len,
                            char *err,
                            size_t err_len) {
  struct sockaddr_storage addr;
  socklen_t addr_len = sizeof(addr);
  char host[128];
  char host_fmt[130];
  char service[32];
  const char *scheme_name = holons_scheme_name(scheme);
  const char *final_host = host;
  int rc;

  if (getsockname(fd, (struct sockaddr *)&addr, &addr_len) != 0) {
    set_err(err, err_len, "getsockname failed: %s", strerror(errno));
    return -1;
  }

  rc = getnameinfo((struct sockaddr *)&addr,
                   addr_len,
                   host,
                   sizeof(host),
                   service,
                   sizeof(service),
                   NI_NUMERICHOST | NI_NUMERICSERV);
  if (rc != 0) {
    set_err(err, err_len, "getnameinfo failed: %s", gai_strerror(rc));
    return -1;
  }

  if (strchr(host, ':') != NULL) {
    (void)snprintf(host_fmt, sizeof(host_fmt), "[%s]", host);
    final_host = host_fmt;
  }

  if (scheme == HOLONS_SCHEME_TCP) {
    if (snprintf(out_uri, out_uri_len, "%s://%s:%s", scheme_name, final_host, service) >=
        (int)out_uri_len) {
      set_err(err, err_len, "bound URI too long");
      return -1;
    }
    return 0;
  }

  if (path == NULL || path[0] == '\0') {
    path = "/grpc";
  }

  if (snprintf(out_uri, out_uri_len, "%s://%s:%s%s", scheme_name, final_host, service, path) >=
      (int)out_uri_len) {
    set_err(err, err_len, "bound URI too long");
    return -1;
  }
  return 0;
}

static void install_stop_handler(int signo) {
  (void)signo;
  g_stop_requested = 1;
}

const char *holons_default_uri(void) { return HOLONS_DEFAULT_URI; }

holons_scheme_t holons_scheme_from_uri(const char *uri) {
  if (uri == NULL) {
    return HOLONS_SCHEME_INVALID;
  }
  if (strncmp(uri, "tcp://", 6) == 0) {
    return HOLONS_SCHEME_TCP;
  }
  if (strncmp(uri, "unix://", 7) == 0) {
    return HOLONS_SCHEME_UNIX;
  }
  if (strcmp(uri, "stdio://") == 0 || strcmp(uri, "stdio") == 0) {
    return HOLONS_SCHEME_STDIO;
  }
  if (strncmp(uri, "mem://", 6) == 0 || strcmp(uri, "mem") == 0) {
    return HOLONS_SCHEME_MEM;
  }
  if (strncmp(uri, "ws://", 5) == 0) {
    return HOLONS_SCHEME_WS;
  }
  if (strncmp(uri, "wss://", 6) == 0) {
    return HOLONS_SCHEME_WSS;
  }
  return HOLONS_SCHEME_INVALID;
}

const char *holons_scheme_name(holons_scheme_t scheme) {
  switch (scheme) {
  case HOLONS_SCHEME_TCP:
    return "tcp";
  case HOLONS_SCHEME_UNIX:
    return "unix";
  case HOLONS_SCHEME_STDIO:
    return "stdio";
  case HOLONS_SCHEME_MEM:
    return "mem";
  case HOLONS_SCHEME_WS:
    return "ws";
  case HOLONS_SCHEME_WSS:
    return "wss";
  default:
    return "invalid";
  }
}

int holons_parse_flags(int argc, char **argv, char *out_uri, size_t out_uri_len) {
  int i;
  char uri[HOLONS_MAX_URI_LEN];

  if (copy_string(uri, sizeof(uri), HOLONS_DEFAULT_URI, NULL, 0) != 0) {
    return -1;
  }

  for (i = 0; i < argc; ++i) {
    if (strcmp(argv[i], "--listen") == 0 && i + 1 < argc) {
      if (copy_string(uri, sizeof(uri), argv[i + 1], NULL, 0) != 0) {
        return -1;
      }
      break;
    }
    if (strcmp(argv[i], "--port") == 0 && i + 1 < argc) {
      if (snprintf(uri, sizeof(uri), "tcp://:%s", argv[i + 1]) >= (int)sizeof(uri)) {
        return -1;
      }
      break;
    }
  }

  return copy_string(out_uri, out_uri_len, uri, NULL, 0);
}

int holons_parse_uri(const char *uri, holons_uri_t *out, char *err, size_t err_len) {
  const char *rest = NULL;

  if (uri == NULL || out == NULL) {
    set_err(err, err_len, "uri and out must be provided");
    return -1;
  }

  (void)memset(out, 0, sizeof(*out));
  out->scheme = holons_scheme_from_uri(uri);

  switch (out->scheme) {
  case HOLONS_SCHEME_TCP:
    rest = uri + 6;
    return parse_host_port(rest, out->host, sizeof(out->host), &out->port, err, err_len);
  case HOLONS_SCHEME_UNIX:
    rest = uri + 7;
    if (rest[0] == '\0') {
      set_err(err, err_len, "unix URI requires a path");
      return -1;
    }
    return copy_string(out->path, sizeof(out->path), rest, err, err_len);
  case HOLONS_SCHEME_STDIO:
  case HOLONS_SCHEME_MEM:
    return 0;
  case HOLONS_SCHEME_WS:
    rest = uri + 5;
    return parse_ws_uri(rest, out, err, err_len);
  case HOLONS_SCHEME_WSS:
    rest = uri + 6;
    return parse_ws_uri(rest, out, err, err_len);
  default:
    set_err(err, err_len, "unsupported transport URI: %s", uri);
    return -1;
  }
}

int holons_listen(const char *uri, holons_listener_t *out, char *err, size_t err_len) {
  int sv[2];

  if (out == NULL) {
    set_err(err, err_len, "listener output is required");
    return -1;
  }

  (void)memset(out, 0, sizeof(*out));
  out->fd = -1;
  out->aux_fd = -1;

  if (holons_parse_uri(uri, &out->uri, err, err_len) != 0) {
    return -1;
  }

  switch (out->uri.scheme) {
  case HOLONS_SCHEME_TCP:
    if (create_tcp_listener(out->uri.host, out->uri.port, &out->fd, err, err_len) != 0) {
      return -1;
    }
    return format_bound_uri(out->fd,
                            HOLONS_SCHEME_TCP,
                            NULL,
                            out->bound_uri,
                            sizeof(out->bound_uri),
                            err,
                            err_len);
  case HOLONS_SCHEME_UNIX:
    if (create_unix_listener(out->uri.path, &out->fd, err, err_len) != 0) {
      return -1;
    }
    if (copy_string(out->unix_path, sizeof(out->unix_path), out->uri.path, err, err_len) != 0) {
      (void)holons_close_listener(out);
      return -1;
    }
    if (snprintf(out->bound_uri, sizeof(out->bound_uri), "unix://%s", out->uri.path) >=
        (int)sizeof(out->bound_uri)) {
      set_err(err, err_len, "bound URI too long");
      (void)holons_close_listener(out);
      return -1;
    }
    return 0;
  case HOLONS_SCHEME_STDIO:
    return copy_string(out->bound_uri, sizeof(out->bound_uri), "stdio://", err, err_len);
  case HOLONS_SCHEME_MEM:
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) != 0) {
      set_err(err, err_len, "socketpair failed: %s", strerror(errno));
      return -1;
    }
    out->fd = sv[0];
    out->aux_fd = sv[1];
    return copy_string(out->bound_uri, sizeof(out->bound_uri), "mem://", err, err_len);
  case HOLONS_SCHEME_WS:
  case HOLONS_SCHEME_WSS:
    if (create_tcp_listener(out->uri.host, out->uri.port, &out->fd, err, err_len) != 0) {
      return -1;
    }
    return format_bound_uri(out->fd,
                            out->uri.scheme,
                            out->uri.path,
                            out->bound_uri,
                            sizeof(out->bound_uri),
                            err,
                            err_len);
  default:
    set_err(err, err_len, "unsupported transport scheme");
    return -1;
  }
}

int holons_accept(holons_listener_t *listener, holons_conn_t *out, char *err, size_t err_len) {
  int fd = -1;

  if (listener == NULL || out == NULL) {
    set_err(err, err_len, "listener and out must be provided");
    return -1;
  }

  (void)memset(out, 0, sizeof(*out));
  out->read_fd = -1;
  out->write_fd = -1;
  out->scheme = listener->uri.scheme;

  switch (listener->uri.scheme) {
  case HOLONS_SCHEME_STDIO:
    if (listener->consumed) {
      set_err(err, err_len, "stdio listener is single-use");
      return -1;
    }
    listener->consumed = 1;
    out->read_fd = STDIN_FILENO;
    out->write_fd = STDOUT_FILENO;
    out->owns_read_fd = 0;
    out->owns_write_fd = 0;
    return 0;
  case HOLONS_SCHEME_MEM:
    if (listener->consumed) {
      set_err(err, err_len, "mem listener server side already consumed");
      return -1;
    }
    fd = dup(listener->fd);
    if (fd < 0) {
      set_err(err, err_len, "dup(mem server fd) failed: %s", strerror(errno));
      return -1;
    }
    listener->consumed = 1;
    out->read_fd = fd;
    out->write_fd = fd;
    out->owns_read_fd = 1;
    out->owns_write_fd = 1;
    return 0;
  case HOLONS_SCHEME_TCP:
  case HOLONS_SCHEME_UNIX:
  case HOLONS_SCHEME_WS:
  case HOLONS_SCHEME_WSS:
    do {
      fd = accept(listener->fd, NULL, NULL);
    } while (fd < 0 && errno == EINTR && !g_stop_requested);

    if (fd < 0) {
      set_err(err, err_len, "accept failed: %s", strerror(errno));
      return -1;
    }
    out->read_fd = fd;
    out->write_fd = fd;
    out->owns_read_fd = 1;
    out->owns_write_fd = 1;
    return 0;
  default:
    set_err(err, err_len, "listener scheme is invalid");
    return -1;
  }
}

int holons_mem_dial(holons_listener_t *listener, holons_conn_t *out, char *err, size_t err_len) {
  int fd;

  if (listener == NULL || out == NULL) {
    set_err(err, err_len, "listener and out must be provided");
    return -1;
  }
  if (listener->uri.scheme != HOLONS_SCHEME_MEM) {
    set_err(err, err_len, "holons_mem_dial requires a mem:// listener");
    return -1;
  }
  if (listener->client_consumed) {
    set_err(err, err_len, "mem listener client side already consumed");
    return -1;
  }

  fd = dup(listener->aux_fd);
  if (fd < 0) {
    set_err(err, err_len, "dup(mem client fd) failed: %s", strerror(errno));
    return -1;
  }

  listener->client_consumed = 1;

  (void)memset(out, 0, sizeof(*out));
  out->read_fd = fd;
  out->write_fd = fd;
  out->scheme = HOLONS_SCHEME_MEM;
  out->owns_read_fd = 1;
  out->owns_write_fd = 1;
  return 0;
}

int holons_dial_tcp(const char *host, int port, holons_conn_t *out, char *err, size_t err_len) {
  int fd;

  if (out == NULL) {
    set_err(err, err_len, "connection output is required");
    return -1;
  }
  if (connect_tcp_socket(host, port, &fd, err, err_len) != 0) {
    return -1;
  }

  (void)memset(out, 0, sizeof(*out));
  out->read_fd = fd;
  out->write_fd = fd;
  out->scheme = HOLONS_SCHEME_TCP;
  out->owns_read_fd = 1;
  out->owns_write_fd = 1;
  return 0;
}

int holons_dial_stdio(holons_conn_t *out, char *err, size_t err_len) {
  if (out == NULL) {
    set_err(err, err_len, "connection output is required");
    return -1;
  }

  (void)memset(out, 0, sizeof(*out));
  out->read_fd = STDIN_FILENO;
  out->write_fd = STDOUT_FILENO;
  out->scheme = HOLONS_SCHEME_STDIO;
  out->owns_read_fd = 0;
  out->owns_write_fd = 0;
  return 0;
}

ssize_t holons_conn_read(const holons_conn_t *conn, void *buf, size_t n) {
  if (conn == NULL || conn->read_fd < 0) {
    errno = EBADF;
    return -1;
  }
  return read(conn->read_fd, buf, n);
}

ssize_t holons_conn_write(const holons_conn_t *conn, const void *buf, size_t n) {
  if (conn == NULL || conn->write_fd < 0) {
    errno = EBADF;
    return -1;
  }
  return write(conn->write_fd, buf, n);
}

int holons_conn_close(holons_conn_t *conn) {
  int rc = 0;
  int saved_errno = 0;

  if (conn == NULL) {
    return 0;
  }

  if (conn->owns_read_fd && conn->read_fd >= 0) {
    if (close(conn->read_fd) != 0) {
      rc = -1;
      saved_errno = errno;
    }
  }

  if (conn->owns_write_fd && conn->write_fd >= 0 && conn->write_fd != conn->read_fd) {
    if (close(conn->write_fd) != 0 && rc == 0) {
      rc = -1;
      saved_errno = errno;
    }
  }

  conn->read_fd = -1;
  conn->write_fd = -1;
  conn->owns_read_fd = 0;
  conn->owns_write_fd = 0;

  if (rc != 0) {
    errno = saved_errno;
  }
  return rc;
}

int holons_close_listener(holons_listener_t *listener) {
  int rc = 0;

  if (listener == NULL) {
    return 0;
  }

  if (listener->fd >= 0) {
    if (close(listener->fd) != 0) {
      rc = -1;
    }
    listener->fd = -1;
  }

  if (listener->aux_fd >= 0) {
    if (close(listener->aux_fd) != 0) {
      rc = -1;
    }
    listener->aux_fd = -1;
  }

  if (listener->uri.scheme == HOLONS_SCHEME_UNIX && listener->unix_path[0] != '\0') {
    (void)unlink(listener->unix_path);
  }

  listener->consumed = 0;
  listener->client_consumed = 0;
  listener->bound_uri[0] = '\0';
  listener->unix_path[0] = '\0';

  return rc;
}

int holons_serve(const char *listen_uri,
                 holons_conn_handler_t handler,
                 void *ctx,
                 int max_connections,
                 int install_signal_handlers,
                 char *err,
                 size_t err_len) {
  holons_listener_t listener;
  struct sigaction act;
  struct sigaction old_int;
  struct sigaction old_term;
  int previous_stop = g_stop_requested;
  int handled = 0;
  int rc = 0;

  if (handler == NULL) {
    set_err(err, err_len, "handler is required");
    return -1;
  }

  if (listen_uri == NULL || listen_uri[0] == '\0') {
    listen_uri = HOLONS_DEFAULT_URI;
  }

  if (holons_listen(listen_uri, &listener, err, err_len) != 0) {
    return -1;
  }

  if (install_signal_handlers) {
    (void)memset(&act, 0, sizeof(act));
    act.sa_handler = install_stop_handler;
    (void)sigemptyset(&act.sa_mask);
    act.sa_flags = 0;

    (void)sigaction(SIGINT, &act, &old_int);
    (void)sigaction(SIGTERM, &act, &old_term);
  }

  g_stop_requested = 0;

  for (;;) {
    holons_conn_t conn;
    int handler_rc;

    if (g_stop_requested) {
      break;
    }

    if (holons_accept(&listener, &conn, err, err_len) != 0) {
      if (g_stop_requested) {
        break;
      }
      rc = -1;
      break;
    }

    handler_rc = handler(&conn, ctx);
    (void)holons_conn_close(&conn);

    if (handler_rc != 0) {
      set_err(err, err_len, "connection handler returned %d", handler_rc);
      rc = -1;
      break;
    }

    ++handled;

    if (listener.uri.scheme == HOLONS_SCHEME_STDIO || listener.uri.scheme == HOLONS_SCHEME_MEM) {
      break;
    }

    if (max_connections > 0 && handled >= max_connections) {
      break;
    }
  }

  (void)holons_close_listener(&listener);

  if (install_signal_handlers) {
    (void)sigaction(SIGINT, &old_int, NULL);
    (void)sigaction(SIGTERM, &old_term, NULL);
  }

  g_stop_requested = previous_stop;
  return rc;
}

int holons_parse_holon(const char *path, holons_identity_t *out, char *err, size_t err_len) {
  FILE *f;
  char line[1024];
  int saw_open = 0;
  int saw_close = 0;

  if (path == NULL || out == NULL) {
    set_err(err, err_len, "path and output are required");
    return -1;
  }

  (void)memset(out, 0, sizeof(*out));
  f = fopen(path, "r");
  if (f == NULL) {
    set_err(err, err_len, "cannot open %s: %s", path, strerror(errno));
    return -1;
  }

  if (fgets(line, sizeof(line), f) == NULL) {
    set_err(err, err_len, "%s: empty file", path);
    (void)fclose(f);
    return -1;
  }

  if (strcmp(trim(line), "---") != 0) {
    set_err(err, err_len, "%s: missing YAML frontmatter", path);
    (void)fclose(f);
    return -1;
  }

  saw_open = 1;

  while (fgets(line, sizeof(line), f) != NULL) {
    char *raw = trim(line);
    char *sep;
    char *value;

    if (strcmp(raw, "---") == 0) {
      saw_close = 1;
      break;
    }
    if (raw[0] == '\0' || raw[0] == '#') {
      continue;
    }

    sep = strchr(raw, ':');
    if (sep == NULL) {
      continue;
    }
    *sep = '\0';

    value = trim(sep + 1);
    value = strip_quotes(value);
    if (strcmp(value, "null") == 0) {
      value = "";
    }

    if (strcmp(raw, "uuid") == 0) {
      (void)copy_string(out->uuid, sizeof(out->uuid), value, NULL, 0);
    } else if (strcmp(raw, "given_name") == 0) {
      (void)copy_string(out->given_name, sizeof(out->given_name), value, NULL, 0);
    } else if (strcmp(raw, "family_name") == 0) {
      (void)copy_string(out->family_name, sizeof(out->family_name), value, NULL, 0);
    } else if (strcmp(raw, "motto") == 0) {
      (void)copy_string(out->motto, sizeof(out->motto), value, NULL, 0);
    } else if (strcmp(raw, "composer") == 0) {
      (void)copy_string(out->composer, sizeof(out->composer), value, NULL, 0);
    } else if (strcmp(raw, "clade") == 0) {
      (void)copy_string(out->clade, sizeof(out->clade), value, NULL, 0);
    } else if (strcmp(raw, "status") == 0) {
      (void)copy_string(out->status, sizeof(out->status), value, NULL, 0);
    } else if (strcmp(raw, "born") == 0) {
      (void)copy_string(out->born, sizeof(out->born), value, NULL, 0);
    } else if (strcmp(raw, "lang") == 0) {
      (void)copy_string(out->lang, sizeof(out->lang), value, NULL, 0);
    }
  }

  (void)fclose(f);

  if (!saw_open || !saw_close) {
    set_err(err, err_len, "%s: unterminated YAML frontmatter", path);
    return -1;
  }

  return 0;
}

volatile sig_atomic_t *holons_stop_token(void) { return &g_stop_requested; }

void holons_request_stop(void) { g_stop_requested = 1; }
