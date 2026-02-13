#ifndef HOLONS_H
#define HOLONS_H

#include <signal.h>
#include <stddef.h>
#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

#define HOLONS_DEFAULT_URI "tcp://:9090"
#define HOLONS_MAX_URI_LEN 512
#define HOLONS_MAX_FIELD_LEN 256

typedef enum {
  HOLONS_SCHEME_INVALID = 0,
  HOLONS_SCHEME_TCP,
  HOLONS_SCHEME_UNIX,
  HOLONS_SCHEME_STDIO,
  HOLONS_SCHEME_MEM,
  HOLONS_SCHEME_WS,
  HOLONS_SCHEME_WSS
} holons_scheme_t;

typedef struct {
  holons_scheme_t scheme;
  char host[128];
  int port;
  char path[256];
} holons_uri_t;

typedef struct {
  char uuid[96];
  char given_name[96];
  char family_name[96];
  char motto[256];
  char composer[128];
  char clade[128];
  char status[64];
  char born[64];
  char lang[64];
} holons_identity_t;

typedef struct {
  int read_fd;
  int write_fd;
  holons_scheme_t scheme;
  int owns_read_fd;
  int owns_write_fd;
} holons_conn_t;

typedef struct {
  holons_uri_t uri;
  int fd;
  int aux_fd;
  int consumed;
  int client_consumed;
  char bound_uri[HOLONS_MAX_URI_LEN];
  char unix_path[256];
} holons_listener_t;

typedef int (*holons_conn_handler_t)(const holons_conn_t *conn, void *ctx);

const char *holons_default_uri(void);
holons_scheme_t holons_scheme_from_uri(const char *uri);
const char *holons_scheme_name(holons_scheme_t scheme);

int holons_parse_flags(int argc, char **argv, char *out_uri, size_t out_uri_len);
int holons_parse_uri(const char *uri, holons_uri_t *out, char *err, size_t err_len);

int holons_listen(const char *uri, holons_listener_t *out, char *err, size_t err_len);
int holons_accept(holons_listener_t *listener, holons_conn_t *out, char *err, size_t err_len);
int holons_mem_dial(holons_listener_t *listener, holons_conn_t *out, char *err, size_t err_len);
int holons_dial_tcp(const char *host,
                    int port,
                    holons_conn_t *out,
                    char *err,
                    size_t err_len);
int holons_dial_stdio(holons_conn_t *out, char *err, size_t err_len);

ssize_t holons_conn_read(const holons_conn_t *conn, void *buf, size_t n);
ssize_t holons_conn_write(const holons_conn_t *conn, const void *buf, size_t n);
int holons_conn_close(holons_conn_t *conn);
int holons_close_listener(holons_listener_t *listener);

int holons_serve(const char *listen_uri,
                 holons_conn_handler_t handler,
                 void *ctx,
                 int max_connections,
                 int install_signal_handlers,
                 char *err,
                 size_t err_len);

int holons_parse_holon(const char *path, holons_identity_t *out, char *err, size_t err_len);

volatile sig_atomic_t *holons_stop_token(void);
void holons_request_stop(void);

#ifdef __cplusplus
}
#endif

#endif
