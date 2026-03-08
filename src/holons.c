#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif

#include "holons/holons.h"

#include <arpa/inet.h>
#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <netdb.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

static volatile sig_atomic_t g_stop_requested = 0;

#define HOLONS_CONNECT_DEFAULT_TIMEOUT_MS 5000
#define HOLONS_CONNECT_STOP_TIMEOUT_MS 2000
#define HOLONS_CONNECT_POLL_MS 50

struct grpc_channel {
  char target[HOLONS_MAX_URI_LEN];
};

typedef struct holons_started_channel {
  grpc_channel *channel;
  pid_t pid;
  int ephemeral;
  int output_fd;
  struct holons_started_channel *next;
} holons_started_channel_t;

static holons_started_channel_t *g_started_channels = NULL;

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

static int path_depth(const char *rel) {
  char tmp[HOLONS_MAX_URI_LEN];
  char *token;
  int depth = 0;

  if (rel == NULL || rel[0] == '\0' || strcmp(rel, ".") == 0) {
    return 0;
  }

  if (copy_string(tmp, sizeof(tmp), rel, NULL, 0) != 0) {
    return 0;
  }

  token = strtok(tmp, "/");
  while (token != NULL) {
    ++depth;
    token = strtok(NULL, "/");
  }
  return depth;
}

static int relative_path_from_root(const char *root,
                                   const char *dir,
                                   char *out,
                                   size_t out_len,
                                   char *err,
                                   size_t err_len) {
  size_t root_len;

  if (root == NULL || dir == NULL) {
    return copy_string(out, out_len, ".", err, err_len);
  }

  root_len = strlen(root);
  if (strncmp(root, dir, root_len) == 0 &&
      (dir[root_len] == '\0' || dir[root_len] == '/' || root[root_len - 1] == '/')) {
    const char *rel = dir + root_len;
    while (*rel == '/') {
      ++rel;
    }
    if (*rel == '\0') {
      return copy_string(out, out_len, ".", err, err_len);
    }
    return copy_string(out, out_len, rel, err, err_len);
  }

  return copy_string(out, out_len, dir, err, err_len);
}

static void slug_for_identity(const holons_identity_t *id, char *out, size_t out_len) {
  char slug[HOLONS_MAX_FIELD_LEN];
  size_t i;
  size_t n = 0;
  const char *given = id != NULL ? id->given_name : "";
  const char *family = id != NULL ? id->family_name : "";

  if (given == NULL) {
    given = "";
  }
  if (family == NULL) {
    family = "";
  }

  while (*given != '\0' && isspace((unsigned char)*given)) {
    ++given;
  }
  while (*family != '\0' && isspace((unsigned char)*family)) {
    ++family;
  }

  if (*given == '\0' && *family == '\0') {
    if (out != NULL && out_len > 0) {
      out[0] = '\0';
    }
    return;
  }

  for (i = 0; given[i] != '\0' && n + 1 < sizeof(slug); ++i) {
    char c = given[i];
    if (c == ' ') {
      c = '-';
    }
    slug[n++] = (char)tolower((unsigned char)c);
  }
  if (n > 0 && n + 1 < sizeof(slug)) {
    slug[n++] = '-';
  }
  for (i = 0; family[i] != '\0' && n + 1 < sizeof(slug); ++i) {
    char c = family[i];
    if (c == '?') {
      continue;
    }
    if (c == ' ') {
      c = '-';
    }
    slug[n++] = (char)tolower((unsigned char)c);
  }
  while (n > 0 && slug[n - 1] == '-') {
    --n;
  }
  slug[n] = '\0';
  (void)copy_string(out, out_len, slug, NULL, 0);
}

static int parse_manifest_file(const char *path, holons_manifest_t *out, char *err, size_t err_len) {
  FILE *f;
  char line[1024];
  int saw_mapping = 0;
  char section[32] = "";

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

  while (fgets(line, sizeof(line), f) != NULL) {
    char *raw = ltrim(line);
    char *sep;
    char *value;
    size_t indent = (size_t)(raw - line);

    rtrim(raw);
    if (raw[0] == '\0' || raw[0] == '#') {
      continue;
    }

    sep = strchr(raw, ':');
    if (sep == NULL) {
      continue;
    }
    saw_mapping = 1;
    *sep = '\0';
    value = trim(sep + 1);
    value = strip_quotes(value);

    if (indent == 0) {
      section[0] = '\0';
      if (strcmp(raw, "kind") == 0) {
        (void)copy_string(out->kind, sizeof(out->kind), value, NULL, 0);
      } else if ((strcmp(raw, "build") == 0 || strcmp(raw, "artifacts") == 0) && value[0] == '\0') {
        (void)copy_string(section, sizeof(section), raw, NULL, 0);
      }
      continue;
    }

    if (strcmp(section, "build") == 0) {
      if (strcmp(raw, "runner") == 0) {
        (void)copy_string(out->build.runner, sizeof(out->build.runner), value, NULL, 0);
      } else if (strcmp(raw, "main") == 0) {
        (void)copy_string(out->build.main, sizeof(out->build.main), value, NULL, 0);
      }
    } else if (strcmp(section, "artifacts") == 0) {
      if (strcmp(raw, "binary") == 0) {
        (void)copy_string(out->artifacts.binary, sizeof(out->artifacts.binary), value, NULL, 0);
      } else if (strcmp(raw, "primary") == 0) {
        (void)copy_string(out->artifacts.primary, sizeof(out->artifacts.primary), value, NULL, 0);
      }
    }
  }

  (void)fclose(f);

  if (!saw_mapping) {
    set_err(err, err_len, "%s: holon.yaml must be a YAML mapping", path);
    return -1;
  }
  return 0;
}

typedef struct {
  holon_entry_t *items;
  size_t count;
  size_t capacity;
} holon_entries_t;

static int ensure_entries_capacity(holon_entries_t *entries, size_t needed, char *err, size_t err_len) {
  holon_entry_t *next;
  size_t new_capacity;

  if (entries->capacity >= needed) {
    return 0;
  }

  new_capacity = entries->capacity == 0 ? 8 : entries->capacity * 2;
  while (new_capacity < needed) {
    new_capacity *= 2;
  }

  next = realloc(entries->items, new_capacity * sizeof(*next));
  if (next == NULL) {
    set_err(err, err_len, "out of memory");
    return -1;
  }

  entries->items = next;
  entries->capacity = new_capacity;
  return 0;
}

static int append_or_replace_entry(holon_entries_t *entries,
                                   const holon_entry_t *entry,
                                   char *err,
                                   size_t err_len) {
  size_t i;
  const char *key = entry->uuid[0] != '\0' ? entry->uuid : entry->dir;

  for (i = 0; i < entries->count; ++i) {
    const char *existing_key =
        entries->items[i].uuid[0] != '\0' ? entries->items[i].uuid : entries->items[i].dir;
    if (strcmp(existing_key, key) == 0) {
      if (path_depth(entry->relative_path) < path_depth(entries->items[i].relative_path)) {
        entries->items[i] = *entry;
      }
      return 0;
    }
  }

  if (ensure_entries_capacity(entries, entries->count + 1, err, err_len) != 0) {
    return -1;
  }
  entries->items[entries->count++] = *entry;
  return 0;
}

static int should_skip_discovery_dir(const char *root, const char *path, const char *name) {
  (void)path;
  if (root != NULL && path != NULL && strcmp(root, path) == 0) {
    return 0;
  }
  if (strcmp(name, ".git") == 0 || strcmp(name, ".op") == 0 || strcmp(name, "node_modules") == 0 ||
      strcmp(name, "vendor") == 0 || strcmp(name, "build") == 0) {
    return 1;
  }
  return name[0] == '.';
}

static int discover_scan_dir(const char *root,
                             const char *dir,
                             const char *origin,
                             holon_entries_t *entries,
                             char *err,
                             size_t err_len) {
  DIR *handle;
  struct dirent *item;

  handle = opendir(dir);
  if (handle == NULL) {
    return 0;
  }

  while ((item = readdir(handle)) != NULL) {
    char child[PATH_MAX];
    struct stat st;

    if (strcmp(item->d_name, ".") == 0 || strcmp(item->d_name, "..") == 0) {
      continue;
    }

    if (snprintf(child, sizeof(child), "%s/%s", dir, item->d_name) >= (int)sizeof(child)) {
      continue;
    }

    if (lstat(child, &st) != 0) {
      continue;
    }

    if (S_ISDIR(st.st_mode)) {
      if (should_skip_discovery_dir(root, child, item->d_name)) {
        continue;
      }
      if (discover_scan_dir(root, child, origin, entries, err, err_len) != 0) {
        (void)closedir(handle);
        return -1;
      }
      continue;
    }

    if (!S_ISREG(st.st_mode) || strcmp(item->d_name, "holon.yaml") != 0) {
      continue;
    }

    {
      holon_entry_t entry;
      char abs_dir[PATH_MAX];

      (void)memset(&entry, 0, sizeof(entry));
      if (holons_parse_holon(child, &entry.identity, NULL, 0) != 0) {
        continue;
      }
      entry.has_manifest = parse_manifest_file(child, &entry.manifest, NULL, 0) == 0 ? 1 : 0;
      if (realpath(dir, abs_dir) == NULL) {
        (void)copy_string(abs_dir, sizeof(abs_dir), dir, NULL, 0);
      }

      slug_for_identity(&entry.identity, entry.slug, sizeof(entry.slug));
      (void)copy_string(entry.uuid, sizeof(entry.uuid), entry.identity.uuid, NULL, 0);
      (void)copy_string(entry.dir, sizeof(entry.dir), abs_dir, NULL, 0);
      (void)copy_string(entry.origin, sizeof(entry.origin), origin, NULL, 0);
      if (relative_path_from_root(root, abs_dir, entry.relative_path, sizeof(entry.relative_path), NULL, 0) != 0) {
        (void)copy_string(entry.relative_path, sizeof(entry.relative_path), abs_dir, NULL, 0);
      }

      if (append_or_replace_entry(entries, &entry, err, err_len) != 0) {
        (void)closedir(handle);
        return -1;
      }
    }
  }

  (void)closedir(handle);
  return 0;
}

static int compare_entries(const void *left, const void *right) {
  const holon_entry_t *a = left;
  const holon_entry_t *b = right;
  int rel_cmp = strcmp(a->relative_path, b->relative_path);
  if (rel_cmp != 0) {
    return rel_cmp;
  }
  return strcmp(a->uuid, b->uuid);
}

static int resolve_root(const char *root, char *out, size_t out_len, char *err, size_t err_len) {
  char cwd[PATH_MAX];
  const char *candidate = root;

  if (candidate == NULL || candidate[0] == '\0') {
    if (getcwd(cwd, sizeof(cwd)) == NULL) {
      set_err(err, err_len, "getcwd failed: %s", strerror(errno));
      return -1;
    }
    candidate = cwd;
  }

  if (realpath(candidate, out) != NULL) {
    return 0;
  }
  if (errno == ENOENT) {
    out[0] = '\0';
    return 0;
  }
  return copy_string(out, out_len, candidate, err, err_len);
}

static int oppath(char *out, size_t out_len, char *err, size_t err_len) {
  const char *configured = getenv("OPPATH");
  const char *home;
  char buf[PATH_MAX];

  if (configured != NULL && configured[0] != '\0') {
    return resolve_root(configured, out, out_len, err, err_len);
  }

  home = getenv("HOME");
  if (home == NULL || home[0] == '\0') {
    return copy_string(out, out_len, ".op", err, err_len);
  }

  if (snprintf(buf, sizeof(buf), "%s/.op", home) >= (int)sizeof(buf)) {
    set_err(err, err_len, "OPPATH is too long");
    return -1;
  }
  return resolve_root(buf, out, out_len, err, err_len);
}

static int opbin(char *out, size_t out_len, char *err, size_t err_len) {
  const char *configured = getenv("OPBIN");
  char op_path[PATH_MAX];

  if (configured != NULL && configured[0] != '\0') {
    return resolve_root(configured, out, out_len, err, err_len);
  }

  if (oppath(op_path, sizeof(op_path), err, err_len) != 0) {
    return -1;
  }
  if (snprintf(out, out_len, "%s/bin", op_path) >= (int)out_len) {
    set_err(err, err_len, "OPBIN is too long");
    return -1;
  }
  return 0;
}

static int cache_dir(char *out, size_t out_len, char *err, size_t err_len) {
  char op_path[PATH_MAX];

  if (oppath(op_path, sizeof(op_path), err, err_len) != 0) {
    return -1;
  }
  if (snprintf(out, out_len, "%s/cache", op_path) >= (int)out_len) {
    set_err(err, err_len, "cache path is too long");
    return -1;
  }
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
  int saw_mapping = 0;

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

  while (fgets(line, sizeof(line), f) != NULL) {
    char *raw = trim(line);
    char *sep;
    char *value;

    if (raw[0] == '\0' || raw[0] == '#') {
      continue;
    }

    sep = strchr(raw, ':');
    if (sep == NULL) {
      continue;
    }
    saw_mapping = 1;
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

  if (!saw_mapping) {
    set_err(err, err_len, "%s: holon.yaml must be a YAML mapping", path);
    return -1;
  }

  return 0;
}

int holons_discover(const char *root,
                    holon_entry_t **entries,
                    size_t *count,
                    char *err,
                    size_t err_len) {
  char resolved_root[PATH_MAX];
  holon_entries_t found;

  if (entries == NULL || count == NULL) {
    set_err(err, err_len, "entries and count are required");
    return -1;
  }

  *entries = NULL;
  *count = 0;
  (void)memset(&found, 0, sizeof(found));

  if (resolve_root(root, resolved_root, sizeof(resolved_root), err, err_len) != 0) {
    return -1;
  }
  if (resolved_root[0] == '\0') {
    return 0;
  }

  if (discover_scan_dir(resolved_root, resolved_root, "local", &found, err, err_len) != 0) {
    free(found.items);
    return -1;
  }

  if (found.count > 1) {
    qsort(found.items, found.count, sizeof(*found.items), compare_entries);
  }

  *entries = found.items;
  *count = found.count;
  return 0;
}

int holons_discover_local(holon_entry_t **entries, size_t *count, char *err, size_t err_len) {
  return holons_discover(NULL, entries, count, err, err_len);
}

int holons_discover_all(holon_entry_t **entries, size_t *count, char *err, size_t err_len) {
  holon_entries_t found;
  char roots[3][PATH_MAX];
  const char *origins[3] = {"local", "$OPBIN", "cache"};
  int i;

  if (entries == NULL || count == NULL) {
    set_err(err, err_len, "entries and count are required");
    return -1;
  }
  *entries = NULL;
  *count = 0;
  (void)memset(&found, 0, sizeof(found));

  if (resolve_root(NULL, roots[0], sizeof(roots[0]), err, err_len) != 0) {
    return -1;
  }
  if (opbin(roots[1], sizeof(roots[1]), err, err_len) != 0) {
    return -1;
  }
  if (cache_dir(roots[2], sizeof(roots[2]), err, err_len) != 0) {
    return -1;
  }

  for (i = 0; i < 3; ++i) {
    holon_entries_t local = {0};
    size_t j;

    if (roots[i][0] == '\0') {
      continue;
    }
    if (discover_scan_dir(roots[i], roots[i], origins[i], &local, err, err_len) != 0) {
      free(found.items);
      free(local.items);
      return -1;
    }
    for (j = 0; j < local.count; ++j) {
      if (append_or_replace_entry(&found, &local.items[j], err, err_len) != 0) {
        free(found.items);
        free(local.items);
        return -1;
      }
    }
    free(local.items);
  }

  if (found.count > 1) {
    qsort(found.items, found.count, sizeof(*found.items), compare_entries);
  }

  *entries = found.items;
  *count = found.count;
  return 0;
}

holon_entry_t *holons_find_by_slug(const char *slug, char *err, size_t err_len) {
  holon_entry_t *entries = NULL;
  holon_entry_t *match = NULL;
  size_t count = 0;
  size_t i;

  if (slug == NULL || slug[0] == '\0') {
    return NULL;
  }

  if (holons_discover_all(&entries, &count, err, err_len) != 0) {
    return NULL;
  }

  for (i = 0; i < count; ++i) {
    if (strcmp(entries[i].slug, slug) != 0) {
      continue;
    }
    if (match != NULL && strcmp(match->uuid, entries[i].uuid) != 0) {
      set_err(err, err_len, "ambiguous holon \"%s\"", slug);
      free(entries);
      return NULL;
    }
    match = &entries[i];
  }

  if (match == NULL) {
    free(entries);
    return NULL;
  }

  {
    holon_entry_t *result = malloc(sizeof(*result));
    if (result == NULL) {
      set_err(err, err_len, "out of memory");
      free(entries);
      return NULL;
    }
    *result = *match;
    free(entries);
    return result;
  }
}

holon_entry_t *holons_find_by_uuid(const char *prefix, char *err, size_t err_len) {
  holon_entry_t *entries = NULL;
  holon_entry_t *match = NULL;
  size_t count = 0;
  size_t i;

  if (prefix == NULL || prefix[0] == '\0') {
    return NULL;
  }

  if (holons_discover_all(&entries, &count, err, err_len) != 0) {
    return NULL;
  }

  for (i = 0; i < count; ++i) {
    if (strncmp(entries[i].uuid, prefix, strlen(prefix)) != 0) {
      continue;
    }
    if (match != NULL && strcmp(match->uuid, entries[i].uuid) != 0) {
      set_err(err, err_len, "ambiguous UUID prefix \"%s\"", prefix);
      free(entries);
      return NULL;
    }
    match = &entries[i];
  }

  if (match == NULL) {
    free(entries);
    return NULL;
  }

  {
    holon_entry_t *result = malloc(sizeof(*result));
    if (result == NULL) {
      set_err(err, err_len, "out of memory");
      free(entries);
      return NULL;
    }
    *result = *match;
    free(entries);
    return result;
  }
}

static long long monotonic_millis(void) {
  struct timespec ts;

  if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
    return 0;
  }
  return (long long)ts.tv_sec * 1000LL + (long long)(ts.tv_nsec / 1000000L);
}

static void sleep_millis(int millis) {
  struct timespec req;

  if (millis <= 0) {
    return;
  }

  req.tv_sec = millis / 1000;
  req.tv_nsec = (long)(millis % 1000) * 1000000L;
  while (nanosleep(&req, &req) != 0 && errno == EINTR) {
  }
}

static int path_is_dir(const char *path) {
  struct stat st;

  if (path == NULL || stat(path, &st) != 0) {
    return 0;
  }
  return S_ISDIR(st.st_mode);
}

static const char *path_basename(const char *path) {
  const char *slash;

  if (path == NULL) {
    return "";
  }

  slash = strrchr(path, '/');
  if (slash == NULL) {
    return path;
  }
  return slash + 1;
}

static int is_blank_string(const char *text) {
  if (text == NULL) {
    return 1;
  }
  while (*text != '\0') {
    if (!isspace((unsigned char)*text)) {
      return 0;
    }
    ++text;
  }
  return 1;
}

static int ensure_dir_recursive(const char *path, mode_t mode, char *err, size_t err_len) {
  char tmp[PATH_MAX];
  char *p;

  if (path == NULL || path[0] == '\0') {
    return 0;
  }

  if (copy_string(tmp, sizeof(tmp), path, err, err_len) != 0) {
    return -1;
  }

  for (p = tmp + 1; *p != '\0'; ++p) {
    if (*p != '/') {
      continue;
    }
    *p = '\0';
    if (tmp[0] != '\0') {
      if (mkdir(tmp, mode) != 0) {
        if (errno != EEXIST) {
          set_err(err, err_len, "mkdir(%s) failed: %s", tmp, strerror(errno));
          return -1;
        }
        if (!path_is_dir(tmp)) {
          set_err(err, err_len, "%s exists and is not a directory", tmp);
          return -1;
        }
      }
    }
    *p = '/';
  }

  if (mkdir(tmp, mode) != 0) {
    if (errno != EEXIST) {
      set_err(err, err_len, "mkdir(%s) failed: %s", tmp, strerror(errno));
      return -1;
    }
    if (!path_is_dir(tmp)) {
      set_err(err, err_len, "%s exists and is not a directory", tmp);
      return -1;
    }
  }
  return 0;
}

static int ensure_parent_dir(const char *path, mode_t mode, char *err, size_t err_len) {
  char parent[PATH_MAX];
  char *slash;

  if (path == NULL || path[0] == '\0') {
    return 0;
  }

  if (copy_string(parent, sizeof(parent), path, err, err_len) != 0) {
    return -1;
  }

  slash = strrchr(parent, '/');
  if (slash == NULL) {
    return 0;
  }
  if (slash == parent) {
    slash[1] = '\0';
  } else {
    *slash = '\0';
  }
  return ensure_dir_recursive(parent, mode, err, err_len);
}

static int write_port_file(const char *path, const char *uri, char *err, size_t err_len) {
  char trimmed_uri[HOLONS_MAX_URI_LEN];
  char *value;
  FILE *f;

  if (path == NULL || uri == NULL) {
    set_err(err, err_len, "port file path and URI are required");
    return -1;
  }

  if (ensure_parent_dir(path, 0755, err, err_len) != 0) {
    return -1;
  }
  if (copy_string(trimmed_uri, sizeof(trimmed_uri), uri, err, err_len) != 0) {
    return -1;
  }

  value = trim(trimmed_uri);
  f = fopen(path, "w");
  if (f == NULL) {
    set_err(err, err_len, "cannot open %s: %s", path, strerror(errno));
    return -1;
  }

  if (fprintf(f, "%s\n", value) < 0 || fclose(f) != 0) {
    set_err(err, err_len, "cannot write %s: %s", path, strerror(errno));
    return -1;
  }
  return 0;
}

static int default_port_file_path(const char *slug, char *out, size_t out_len, char *err, size_t err_len) {
  char cwd[PATH_MAX];

  if (slug == NULL || slug[0] == '\0') {
    set_err(err, err_len, "slug is required");
    return -1;
  }
  if (getcwd(cwd, sizeof(cwd)) == NULL) {
    set_err(err, err_len, "getcwd failed: %s", strerror(errno));
    return -1;
  }
  if (snprintf(out, out_len, "%s/.op/run/%s.port", cwd, slug) >= (int)out_len) {
    set_err(err, err_len, "port file path is too long");
    return -1;
  }
  return 0;
}

static int is_direct_target(const char *target) {
  if (target == NULL) {
    return 0;
  }
  return strstr(target, "://") != NULL || strchr(target, ':') != NULL;
}

static int normalize_dial_target(const char *target,
                                 char *out,
                                 size_t out_len,
                                 char *err,
                                 size_t err_len) {
  holons_uri_t uri;
  const char *host;

  if (target == NULL || target[0] == '\0') {
    set_err(err, err_len, "target is required");
    return -1;
  }

  if (strstr(target, "://") == NULL) {
    return copy_string(out, out_len, target, err, err_len);
  }

  if (strncmp(target, "unix://", 7) == 0) {
    return copy_string(out, out_len, target, err, err_len);
  }

  if (holons_parse_uri(target, &uri, err, err_len) != 0) {
    return -1;
  }
  if (uri.scheme != HOLONS_SCHEME_TCP) {
    return copy_string(out, out_len, target, err, err_len);
  }

  host = uri.host;
  if (host[0] == '\0' || strcmp(host, "0.0.0.0") == 0 || strcmp(host, "::") == 0) {
    host = "127.0.0.1";
  }

  if (strchr(host, ':') != NULL) {
    if (snprintf(out, out_len, "[%s]:%d", host, uri.port) >= (int)out_len) {
      set_err(err, err_len, "normalized dial target is too long");
      return -1;
    }
    return 0;
  }

  if (snprintf(out, out_len, "%s:%d", host, uri.port) >= (int)out_len) {
    set_err(err, err_len, "normalized dial target is too long");
    return -1;
  }
  return 0;
}

static int probe_unix_target(const char *path, char *err, size_t err_len) {
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

  if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
    set_err(err, err_len, "connect(%s) failed: %s", path, strerror(errno));
    (void)close(fd);
    return -1;
  }

  (void)close(fd);
  return 0;
}

static int probe_tcp_target(const char *host, int port, char *err, size_t err_len) {
  int fd = -1;

  if (connect_tcp_socket(host, port, &fd, err, err_len) != 0) {
    return -1;
  }

  if (fd >= 0) {
    (void)close(fd);
  }
  return 0;
}

static int wait_for_ready_target(const char *target, int timeout_ms, char *err, size_t err_len) {
  long long deadline;
  char probe_err[256] = "";

  if (target == NULL || target[0] == '\0') {
    set_err(err, err_len, "target is required");
    return -1;
  }

  if (timeout_ms <= 0) {
    timeout_ms = HOLONS_CONNECT_DEFAULT_TIMEOUT_MS;
  }
  deadline = monotonic_millis() + timeout_ms;

  for (;;) {
    if (strstr(target, "://") != NULL) {
      holons_uri_t uri;

      if (holons_parse_uri(target, &uri, probe_err, sizeof(probe_err)) != 0) {
        set_err(err, err_len, "%s", probe_err);
        return -1;
      }

      if (uri.scheme == HOLONS_SCHEME_TCP) {
        if (probe_tcp_target(uri.host, uri.port, probe_err, sizeof(probe_err)) == 0) {
          return 0;
        }
      } else if (uri.scheme == HOLONS_SCHEME_UNIX) {
        if (probe_unix_target(uri.path, probe_err, sizeof(probe_err)) == 0) {
          return 0;
        }
      } else {
        set_err(err, err_len, "unsupported connect target: %s", target);
        return -1;
      }
    } else {
      char host[256];
      int port;

      if (parse_host_port(target, host, sizeof(host), &port, probe_err, sizeof(probe_err)) != 0) {
        set_err(err, err_len, "%s", probe_err);
        return -1;
      }
      if (probe_tcp_target(host, port, probe_err, sizeof(probe_err)) == 0) {
        return 0;
      }
    }

    if (monotonic_millis() >= deadline) {
      break;
    }
    sleep_millis(HOLONS_CONNECT_POLL_MS);
  }

  if (probe_err[0] == '\0') {
    set_err(err, err_len, "timed out waiting for connect target");
  } else {
    set_err(err, err_len, "timed out waiting for connect target: %s", probe_err);
  }
  return -1;
}

static int usable_port_file(const char *path,
                            int timeout_ms,
                            char *out_uri,
                            size_t out_uri_len,
                            char *err,
                            size_t err_len) {
  char buf[HOLONS_MAX_URI_LEN];
  char *target;
  FILE *f;
  size_t n;
  int probe_timeout;
  char probe_err[256] = "";

  if (path == NULL || path[0] == '\0' || out_uri == NULL || out_uri_len == 0) {
    return 0;
  }

  f = fopen(path, "r");
  if (f == NULL) {
    return 0;
  }

  n = fread(buf, 1, sizeof(buf) - 1, f);
  if (ferror(f)) {
    (void)fclose(f);
    return 0;
  }
  buf[n] = '\0';
  (void)fclose(f);

  target = trim(buf);
  if (target[0] == '\0') {
    (void)unlink(path);
    return 0;
  }

  probe_timeout = timeout_ms / 4;
  if (probe_timeout <= 0) {
    probe_timeout = 1000;
  }
  if (probe_timeout > 1000) {
    probe_timeout = 1000;
  }

  if (wait_for_ready_target(target, probe_timeout, probe_err, sizeof(probe_err)) == 0) {
    return copy_string(out_uri, out_uri_len, target, err, err_len) == 0 ? 1 : 0;
  }

  (void)unlink(path);
  return 0;
}

static int uri_prefix_at(const char *text) {
  return strncmp(text, "tcp://", 6) == 0 || strncmp(text, "unix://", 7) == 0 ||
         strncmp(text, "ws://", 5) == 0 || strncmp(text, "wss://", 6) == 0 ||
         strncmp(text, "stdio://", 8) == 0;
}

static int first_uri_in_text(const char *text, char *out, size_t out_len) {
  const char *trim_chars = "\"'()[]{}.,";
  size_t i;

  if (text == NULL || out == NULL || out_len == 0) {
    return 0;
  }

  for (i = 0; text[i] != '\0'; ++i) {
    size_t start = i;
    size_t end = i;
    size_t n;

    if (!uri_prefix_at(text + i)) {
      continue;
    }

    while (text[end] != '\0' && !isspace((unsigned char)text[end])) {
      ++end;
    }
    while (end > start && strchr(trim_chars, text[end - 1]) != NULL) {
      --end;
    }

    n = end - start;
    if (n == 0 || n >= out_len) {
      continue;
    }

    (void)memcpy(out, text + start, n);
    out[n] = '\0';
    return 1;
  }

  return 0;
}

static int read_advertised_uri(int fd,
                               pid_t pid,
                               int timeout_ms,
                               char *out_uri,
                               size_t out_uri_len,
                               char *err,
                               size_t err_len) {
  char buf[4096];
  size_t used = 0;
  long long deadline;

  if (fd < 0 || out_uri == NULL || out_uri_len == 0) {
    set_err(err, err_len, "startup pipe is invalid");
    return -1;
  }

  if (timeout_ms <= 0) {
    timeout_ms = HOLONS_CONNECT_DEFAULT_TIMEOUT_MS;
  }

  buf[0] = '\0';
  deadline = monotonic_millis() + timeout_ms;

  for (;;) {
    fd_set readfds;
    struct timeval tv;
    long long now = monotonic_millis();
    long long remaining = deadline - now;
    int select_ms;
    int status;
    int rc;

    if (first_uri_in_text(buf, out_uri, out_uri_len)) {
      return 0;
    }

    rc = (int)waitpid(pid, &status, WNOHANG);
    if (rc == (int)pid) {
      set_err(err, err_len, "holon exited before advertising an address");
      return -1;
    }
    if (rc < 0 && errno != EINTR && errno != ECHILD) {
      set_err(err, err_len, "waitpid(%ld) failed: %s", (long)pid, strerror(errno));
      return -1;
    }

    if (remaining <= 0) {
      break;
    }

    select_ms = (int)(remaining > HOLONS_CONNECT_POLL_MS ? HOLONS_CONNECT_POLL_MS : remaining);
    FD_ZERO(&readfds);
    FD_SET(fd, &readfds);
    tv.tv_sec = select_ms / 1000;
    tv.tv_usec = (select_ms % 1000) * 1000;

    rc = select(fd + 1, &readfds, NULL, NULL, &tv);
    if (rc < 0) {
      if (errno == EINTR) {
        continue;
      }
      set_err(err, err_len, "select() failed: %s", strerror(errno));
      return -1;
    }
    if (rc == 0) {
      continue;
    }

    if (FD_ISSET(fd, &readfds)) {
      ssize_t n = read(fd, buf + used, sizeof(buf) - 1 - used);

      if (n < 0) {
        if (errno == EINTR || errno == EAGAIN) {
          continue;
        }
        set_err(err, err_len, "read() failed: %s", strerror(errno));
        return -1;
      }
      if (n == 0) {
        continue;
      }

      used += (size_t)n;
      buf[used] = '\0';
      if (first_uri_in_text(buf, out_uri, out_uri_len)) {
        return 0;
      }

      if (used >= sizeof(buf) - 1) {
        size_t keep = used > 1024 ? 1024 : used;
        (void)memmove(buf, buf + used - keep, keep);
        used = keep;
        buf[used] = '\0';
      }
    }
  }

  set_err(err, err_len, "timed out waiting for holon startup");
  return -1;
}

static int stop_started_process(pid_t pid) {
  int status;
  long long deadline;

  if (pid <= 0) {
    return 0;
  }

  if (kill(pid, SIGTERM) != 0 && errno != ESRCH) {
    return -1;
  }

  deadline = monotonic_millis() + HOLONS_CONNECT_STOP_TIMEOUT_MS;
  for (;;) {
    pid_t waited = waitpid(pid, &status, WNOHANG);

    if (waited == pid) {
      return 0;
    }
    if (waited < 0) {
      if (errno == EINTR) {
        continue;
      }
      if (errno == ECHILD) {
        return 0;
      }
      return -1;
    }
    if (monotonic_millis() >= deadline) {
      break;
    }
    sleep_millis(25);
  }

  if (kill(pid, SIGKILL) != 0 && errno != ESRCH) {
    return -1;
  }

  for (;;) {
    pid_t waited = waitpid(pid, &status, 0);
    if (waited == pid) {
      return 0;
    }
    if (waited < 0) {
      if (errno == EINTR) {
        continue;
      }
      if (errno == ECHILD) {
        return 0;
      }
      return -1;
    }
  }
}

static int start_tcp_holon(const char *binary_path,
                           int timeout_ms,
                           char *out_uri,
                           size_t out_uri_len,
                           pid_t *out_pid,
                           int *out_fd,
                           char *err,
                           size_t err_len) {
  int pipefd[2];
  pid_t pid;

  if (binary_path == NULL || binary_path[0] == '\0') {
    set_err(err, err_len, "binary path is required");
    return -1;
  }

  if (pipe(pipefd) != 0) {
    set_err(err, err_len, "pipe() failed: %s", strerror(errno));
    return -1;
  }

  pid = fork();
  if (pid < 0) {
    set_err(err, err_len, "fork() failed: %s", strerror(errno));
    (void)close(pipefd[0]);
    (void)close(pipefd[1]);
    return -1;
  }

  if (pid == 0) {
    (void)close(pipefd[0]);
    if (dup2(pipefd[1], STDOUT_FILENO) < 0 || dup2(pipefd[1], STDERR_FILENO) < 0) {
      _exit(127);
    }
    if (pipefd[1] != STDOUT_FILENO && pipefd[1] != STDERR_FILENO) {
      (void)close(pipefd[1]);
    }
    execl(binary_path, binary_path, "serve", "--listen", "tcp://127.0.0.1:0", (char *)NULL);
    _exit(127);
  }

  (void)close(pipefd[1]);
  if (read_advertised_uri(pipefd[0], pid, timeout_ms, out_uri, out_uri_len, err, err_len) != 0) {
    (void)stop_started_process(pid);
    (void)close(pipefd[0]);
    return -1;
  }

  if (out_pid != NULL) {
    *out_pid = pid;
  }
  if (out_fd != NULL) {
    *out_fd = pipefd[0];
  } else {
    (void)close(pipefd[0]);
  }

  return 0;
}

static int resolve_binary_path(const holon_entry_t *entry,
                               char *out,
                               size_t out_len,
                               char *err,
                               size_t err_len) {
  char binary_buf[HOLONS_MAX_FIELD_LEN];
  char candidate[PATH_MAX];
  const char *binary_name;
  const char *base_name;
  struct stat st;
  char *path_copy = NULL;
  char *dir;

  if (entry == NULL) {
    set_err(err, err_len, "holon entry is required");
    return -1;
  }
  if (!entry->has_manifest) {
    set_err(err, err_len, "holon \"%s\" has no manifest", entry->slug);
    return -1;
  }
  if (copy_string(binary_buf, sizeof(binary_buf), entry->manifest.artifacts.binary, err, err_len) != 0) {
    return -1;
  }

  binary_name = trim(binary_buf);
  if (binary_name[0] == '\0') {
    set_err(err, err_len, "holon \"%s\" has no artifacts.binary", entry->slug);
    return -1;
  }

  if (binary_name[0] == '/' && access(binary_name, X_OK) == 0 && stat(binary_name, &st) == 0 &&
      S_ISREG(st.st_mode)) {
    return copy_string(out, out_len, binary_name, err, err_len);
  }

  base_name = path_basename(binary_name);
  if (snprintf(candidate, sizeof(candidate), "%s/.op/build/bin/%s", entry->dir, base_name) <
          (int)sizeof(candidate) &&
      access(candidate, X_OK) == 0 && stat(candidate, &st) == 0 && S_ISREG(st.st_mode)) {
    return copy_string(out, out_len, candidate, err, err_len);
  }

  if (getenv("PATH") == NULL || getenv("PATH")[0] == '\0') {
    set_err(err, err_len, "built binary not found for holon \"%s\"", entry->slug);
    return -1;
  }

  path_copy = strdup(getenv("PATH"));
  if (path_copy == NULL) {
    set_err(err, err_len, "out of memory");
    return -1;
  }

  dir = strtok(path_copy, ":");
  while (dir != NULL) {
    const char *lookup_dir = dir[0] == '\0' ? "." : dir;

    if (snprintf(candidate, sizeof(candidate), "%s/%s", lookup_dir, base_name) < (int)sizeof(candidate) &&
        access(candidate, X_OK) == 0 && stat(candidate, &st) == 0 && S_ISREG(st.st_mode)) {
      int rc = copy_string(out, out_len, candidate, err, err_len);
      free(path_copy);
      return rc;
    }
    dir = strtok(NULL, ":");
  }

  free(path_copy);
  set_err(err, err_len, "built binary not found for holon \"%s\"", entry->slug);
  return -1;
}

static int remember_started_channel(grpc_channel *channel, pid_t pid, int ephemeral, int output_fd) {
  holons_started_channel_t *started = malloc(sizeof(*started));

  if (started == NULL) {
    return -1;
  }

  started->channel = channel;
  started->pid = pid;
  started->ephemeral = ephemeral;
  started->output_fd = output_fd;
  started->next = g_started_channels;
  g_started_channels = started;
  return 0;
}

static holons_started_channel_t *take_started_channel(grpc_channel *channel) {
  holons_started_channel_t **current = &g_started_channels;

  while (*current != NULL) {
    if ((*current)->channel == channel) {
      holons_started_channel_t *match = *current;
      *current = match->next;
      match->next = NULL;
      return match;
    }
    current = &(*current)->next;
  }

  return NULL;
}

static grpc_channel *grpc_insecure_channel_create(const char *target, const void *args, void *reserved) {
  grpc_channel *channel;

  (void)args;
  (void)reserved;

  if (target == NULL || target[0] == '\0') {
    errno = EINVAL;
    return NULL;
  }

  channel = calloc(1, sizeof(*channel));
  if (channel == NULL) {
    return NULL;
  }
  if (copy_string(channel->target, sizeof(channel->target), target, NULL, 0) != 0) {
    free(channel);
    errno = ENAMETOOLONG;
    return NULL;
  }
  return channel;
}

static void grpc_channel_destroy(grpc_channel *channel) { free(channel); }

static grpc_channel *connect_internal(const char *target, holons_connect_options opts, int ephemeral) {
  char target_buf[HOLONS_MAX_URI_LEN];
  char transport_buf[32];
  char port_file_buf[PATH_MAX];
  char port_path[PATH_MAX];
  char dial_target[HOLONS_MAX_URI_LEN];
  char started_uri[HOLONS_MAX_URI_LEN];
  char binary_path[PATH_MAX];
  char err[256] = "";
  const char *transport;
  const char *trimmed_target;
  holon_entry_t *entry = NULL;
  grpc_channel *channel = NULL;
  pid_t pid = -1;
  int output_fd = -1;
  int timeout_ms = opts.timeout_ms;
  int start = opts.start;
  int zero_opts_defaults;

  if (target == NULL) {
    errno = EINVAL;
    return NULL;
  }
  if (copy_string(target_buf, sizeof(target_buf), target, NULL, 0) != 0) {
    errno = ENAMETOOLONG;
    return NULL;
  }

  trimmed_target = trim(target_buf);
  if (trimmed_target[0] == '\0') {
    errno = EINVAL;
    return NULL;
  }

  if (timeout_ms <= 0) {
    timeout_ms = HOLONS_CONNECT_DEFAULT_TIMEOUT_MS;
  }

  transport = opts.transport;
  if (transport == NULL || transport[0] == '\0') {
    transport = "tcp";
  }
  if (copy_string(transport_buf, sizeof(transport_buf), transport, NULL, 0) != 0) {
    errno = EINVAL;
    return NULL;
  }
  transport = trim(transport_buf);
  if (transport[0] == '\0') {
    transport = "tcp";
  }
  for (size_t i = 0; transport[i] != '\0'; ++i) {
    transport_buf[i] = (char)tolower((unsigned char)transport[i]);
  }
  transport = transport_buf;

  zero_opts_defaults =
      opts.timeout_ms == 0 && is_blank_string(opts.transport) && is_blank_string(opts.port_file) && opts.start == 0;
  if (zero_opts_defaults) {
    start = 1;
  }

  if (strcmp(transport, "tcp") != 0) {
    errno = ENOTSUP;
    return NULL;
  }

  if (is_direct_target(trimmed_target)) {
    if (wait_for_ready_target(trimmed_target, timeout_ms, err, sizeof(err)) != 0) {
      errno = ETIMEDOUT;
      return NULL;
    }
    if (normalize_dial_target(trimmed_target, dial_target, sizeof(dial_target), err, sizeof(err)) != 0) {
      errno = EINVAL;
      return NULL;
    }
    return grpc_insecure_channel_create(dial_target, NULL, NULL);
  }

  entry = holons_find_by_slug(trimmed_target, err, sizeof(err));
  if (entry == NULL) {
    errno = ENOENT;
    return NULL;
  }

  if (!is_blank_string(opts.port_file)) {
    if (copy_string(port_file_buf, sizeof(port_file_buf), opts.port_file, NULL, 0) != 0) {
      errno = ENAMETOOLONG;
      holons_free_entries(entry);
      return NULL;
    }
    if (copy_string(port_path, sizeof(port_path), trim(port_file_buf), NULL, 0) != 0) {
      errno = ENAMETOOLONG;
      holons_free_entries(entry);
      return NULL;
    }
  } else if (default_port_file_path(entry->slug, port_path, sizeof(port_path), err, sizeof(err)) != 0) {
    errno = ENAMETOOLONG;
    holons_free_entries(entry);
    return NULL;
  }

  if (usable_port_file(port_path, timeout_ms, started_uri, sizeof(started_uri), err, sizeof(err))) {
    if (normalize_dial_target(started_uri, dial_target, sizeof(dial_target), err, sizeof(err)) != 0) {
      errno = EINVAL;
      holons_free_entries(entry);
      return NULL;
    }
    holons_free_entries(entry);
    return grpc_insecure_channel_create(dial_target, NULL, NULL);
  }

  if (!start) {
    errno = ENOENT;
    holons_free_entries(entry);
    return NULL;
  }

  if (resolve_binary_path(entry, binary_path, sizeof(binary_path), err, sizeof(err)) != 0) {
    errno = ENOENT;
    holons_free_entries(entry);
    return NULL;
  }
  if (start_tcp_holon(binary_path,
                      timeout_ms,
                      started_uri,
                      sizeof(started_uri),
                      &pid,
                      &output_fd,
                      err,
                      sizeof(err)) != 0) {
    errno = ETIMEDOUT;
    holons_free_entries(entry);
    return NULL;
  }
  if (wait_for_ready_target(started_uri, timeout_ms, err, sizeof(err)) != 0) {
    (void)stop_started_process(pid);
    if (output_fd >= 0) {
      (void)close(output_fd);
    }
    errno = ETIMEDOUT;
    holons_free_entries(entry);
    return NULL;
  }
  if (normalize_dial_target(started_uri, dial_target, sizeof(dial_target), err, sizeof(err)) != 0) {
    (void)stop_started_process(pid);
    if (output_fd >= 0) {
      (void)close(output_fd);
    }
    errno = EINVAL;
    holons_free_entries(entry);
    return NULL;
  }

  channel = grpc_insecure_channel_create(dial_target, NULL, NULL);
  if (channel == NULL) {
    (void)stop_started_process(pid);
    if (output_fd >= 0) {
      (void)close(output_fd);
    }
    holons_free_entries(entry);
    return NULL;
  }

  if (!ephemeral && write_port_file(port_path, started_uri, err, sizeof(err)) != 0) {
    grpc_channel_destroy(channel);
    (void)stop_started_process(pid);
    if (output_fd >= 0) {
      (void)close(output_fd);
    }
    errno = EIO;
    holons_free_entries(entry);
    return NULL;
  }

  if (remember_started_channel(channel, pid, ephemeral, output_fd) != 0) {
    grpc_channel_destroy(channel);
    (void)stop_started_process(pid);
    if (output_fd >= 0) {
      (void)close(output_fd);
    }
    errno = ENOMEM;
    holons_free_entries(entry);
    return NULL;
  }

  holons_free_entries(entry);
  return channel;
}

grpc_channel *holons_connect(const char *target) {
  holons_connect_options opts;

  opts.timeout_ms = HOLONS_CONNECT_DEFAULT_TIMEOUT_MS;
  opts.transport = "tcp";
  opts.start = 1;
  opts.port_file = NULL;
  return connect_internal(target, opts, 1);
}

grpc_channel *holons_connect_with_opts(const char *target, holons_connect_options opts) {
  return connect_internal(target, opts, 0);
}

void holons_disconnect(grpc_channel *channel) {
  holons_started_channel_t *started;

  if (channel == NULL) {
    return;
  }

  started = take_started_channel(channel);
  grpc_channel_destroy(channel);

  if (started == NULL) {
    return;
  }

  if (started->ephemeral) {
    (void)stop_started_process(started->pid);
  }
  if (started->output_fd >= 0) {
    (void)close(started->output_fd);
  }
  free(started);
}

void holons_free_entries(holon_entry_t *entries) { free(entries); }

volatile sig_atomic_t *holons_stop_token(void) { return &g_stop_requested; }

void holons_request_stop(void) { g_stop_requested = 1; }
