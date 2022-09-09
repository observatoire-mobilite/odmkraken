import typing
import jinja2
import click
from pathlib import Path
import tempfile
import os
import sys


def _tpl():
    return r"""
{% macro mimetypes() -%}
types {
    text/html                                        html htm shtml;
    text/css                                         css;
    text/xml                                         xml;
    image/gif                                        gif;
    image/jpeg                                       jpeg jpg;
    application/javascript                           js;
    application/atom+xml                             atom;
    application/rss+xml                              rss;

    text/mathml                                      mml;
    text/plain                                       txt;
    text/vnd.sun.j2me.app-descriptor                 jad;
    text/vnd.wap.wml                                 wml;
    text/x-component                                 htc;

    image/png                                        png;
    image/svg+xml                                    svg svgz;
    image/tiff                                       tif tiff;
    image/vnd.wap.wbmp                               wbmp;
    image/webp                                       webp;
    image/x-icon                                     ico;
    image/x-jng                                      jng;
    image/x-ms-bmp                                   bmp;

    font/woff                                        woff;
    font/woff2                                       woff2;

    application/java-archive                         jar war ear;
    application/json                                 json;
    application/mac-binhex40                         hqx;
    application/msword                               doc;
    application/pdf                                  pdf;
    application/postscript                           ps eps ai;
    application/rtf                                  rtf;
    application/vnd.apple.mpegurl                    m3u8;
    application/vnd.google-earth.kml+xml             kml;
    application/vnd.google-earth.kmz                 kmz;
    application/vnd.ms-excel                         xls;
    application/vnd.ms-fontobject                    eot;
    application/vnd.ms-powerpoint                    ppt;
    application/vnd.oasis.opendocument.graphics      odg;
    application/vnd.oasis.opendocument.presentation  odp;
    application/vnd.oasis.opendocument.spreadsheet   ods;
    application/vnd.oasis.opendocument.text          odt;
    application/vnd.openxmlformats-officedocument.presentationml.presentation
                                                     pptx;
    application/vnd.openxmlformats-officedocument.spreadsheetml.sheet
                                                     xlsx;
    application/vnd.openxmlformats-officedocument.wordprocessingml.document
                                                     docx;
    application/vnd.wap.wmlc                         wmlc;
    application/wasm                                 wasm;
    application/x-7z-compressed                      7z;
    application/x-cocoa                              cco;
    application/x-java-archive-diff                  jardiff;
    application/x-java-jnlp-file                     jnlp;
    application/x-makeself                           run;
    application/x-perl                               pl pm;
    application/x-pilot                              prc pdb;
    application/x-rar-compressed                     rar;
    application/x-redhat-package-manager             rpm;
    application/x-sea                                sea;
    application/x-shockwave-flash                    swf;
    application/x-stuffit                            sit;
    application/x-tcl                                tcl tk;
    application/x-x509-ca-cert                       der pem crt;
    application/x-xpinstall                          xpi;
    application/xhtml+xml                            xhtml;
    application/xspf+xml                             xspf;
    application/zip                                  zip;

    application/octet-stream                         bin exe dll;
    application/octet-stream                         deb;
    application/octet-stream                         dmg;
    application/octet-stream                         iso img;
    application/octet-stream                         msi msp msm;

    audio/midi                                       mid midi kar;
    audio/mpeg                                       mp3;
    audio/ogg                                        ogg;
    audio/x-m4a                                      m4a;
    audio/x-realaudio                                ra;

    video/3gpp                                       3gpp 3gp;
    video/mp2t                                       ts;
    video/mp4                                        mp4;
    video/mpeg                                       mpeg mpg;
    video/quicktime                                  mov;
    video/webm                                       webm;
    video/x-flv                                      flv;
    video/x-m4v                                      m4v;
    video/x-mng                                      mng;
    video/x-ms-asf                                   asx asf;
    video/x-ms-wmv                                   wmv;
    video/x-msvideo                                  avi;
}
{%- endmacro %}
{% macro http_params() -%}
{%- raw %}
# Connection header for WebSocket reverse proxy
map $http_upgrade $connection_upgrade {
    default upgrade;
    ""      close;
}

map $remote_addr $proxy_forwarded_elem {

    # IPv4 addresses can be sent as-is
    ~^[0-9.]+$        "for=$remote_addr";

    # IPv6 addresses need to be bracketed and quoted
    ~^[0-9A-Fa-f:.]+$ "for=\"[$remote_addr]\"";

    # Unix domain socket names cannot be represented in RFC 7239 syntax
    default           "for=unknown";
}

map $http_forwarded $proxy_add_forwarded {

    # If the incoming Forwarded header is syntactically valid, append to it
    "~^(,[ \\t]*)*([!#$%&'*+.^_`|~0-9A-Za-z-]+=([!#$%&'*+.^_`|~0-9A-Za-z-]+|\"([\\t \\x21\\x23-\\x5B\\x5D-\\x7E\\x80-\\xFF]|\\\\[\\t \\x21-\\x7E\\x80-\\xFF])*\"))?(;([!#$%&'*+.^_`|~0-9A-Za-z-]+=([!#$%&'*+.^_`|~0-9A-Za-z-]+|\"([\\t \\x21\\x23-\\x5B\\x5D-\\x7E\\x80-\\xFF]|\\\\[\\t \\x21-\\x7E\\x80-\\xFF])*\"))?)*([ \\t]*,([ \\t]*([!#$%&'*+.^_`|~0-9A-Za-z-]+=([!#$%&'*+.^_`|~0-9A-Za-z-]+|\"([\\t \\x21\\x23-\\x5B\\x5D-\\x7E\\x80-\\xFF]|\\\\[\\t \\x21-\\x7E\\x80-\\xFF])*\"))?(;([!#$%&'*+.^_`|~0-9A-Za-z-]+=([!#$%&'*+.^_`|~0-9A-Za-z-]+|\"([\\t \\x21\\x23-\\x5B\\x5D-\\x7E\\x80-\\xFF]|\\\\[\\t \\x21-\\x7E\\x80-\\xFF])*\"))?)*)?)*$" "$http_forwarded, $proxy_forwarded_elem";

    # Otherwise, replace it
    default "$proxy_forwarded_elem";
}

{% endraw %}
{%- endmacro %}
{% macro location(url) %}
    location {{url}} {
        {{ caller() | indent(4) }}
    }
{% endmacro %}
{% macro location_static(url, root_path, try_files='') -%}
{% call location(url) %}
    root        "{{root_path}}";
    index       index.html;
    try_files   {{ try_files | default('$uri $uri', true) }};
{% endcall %}
{%- endmacro %}
{% macro location_proxy(url, backend_url='http://localhost', timeout=60) -%}
{% call location(url) %}
    proxy_pass {{ backend_url }};

    proxy_http_version 1.1;
    proxy_cache_bypass $http_upgrade;

    # Proxy headers
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection $connection_upgrade;
    proxy_set_header Host $http_host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header Forwarded $proxy_add_forwarded;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
    proxy_set_header X-Forwarded-Host $http_host;
    proxy_set_header X-Forwarded-Port $server_port;
    
    # Proxy timeouts
    proxy_connect_timeout {{ timeout | default(60, true) }}s;
    proxy_send_timeout {{ timeout | default(60, true) }}s;
    proxy_read_timeout {{ timeout | default(60, true) }}s;
{% endcall %}
{%- endmacro %}
{% macro location_upload(url, upload_path, size_limit='5M', backend_url='http://localhost', require_header=None) -%}
{% call location(url) %}
    {% if require_header %}
    if ($http_{{ require_header | replace('-', '_') | lower }} = "") {
        return 403;
    }
    {% endif %}
    client_body_temp_path           {{ upload_path }};
    client_body_in_file_only        off;
    client_max_body_size            {{ size_limit }};
    proxy_pass_request_headers      on;
    proxy_set_header                X-FILE $request_body_file;
    proxy_set_body                  off;
    proxy_http_version              1.1;
    proxy_pass                      {{ backend_url}};
{% endcall %}
{% endmacro -%}
worker_processes  {{ worker_processes | default(2, true) }};
error_log "{{ log_dir | default('logs', true) }}/nginx.error.log";
pid "{{ log_dir | default('logs', true) }}/nginx.pid";

events {
    worker_connections {{ worker_connections | default(1024, true) }};
}

http {
    {{ mimetypes() | indent(4) }}
    default_type  application/octet-stream;
    {{ http_params() | indent(4) }}

    {%- for folder in ('proxy', 'client_body', 'fastcgi', 'scgi', 'uwsgi') -%}
    {{ folder }}_temp_path "{{ temp_dir | default('temp', true) }}/{{ folder }}_temp";
    {% endfor -%}

    access_log  "{{ log_dir | default('logs', true) }}/nginx.access.log";

    sendfile {{ sendfile | default('off', true) }};
    {% if sendfile -%}
    sendfile_max_chunk {{ sendfile_max_chunk | default('1m') }};
    {% endif -%}
    tcp_nopush on;
    keepalive_timeout 65;

    # gzip
    gzip            on;
    gzip_vary       on;
    gzip_proxied    any;
    gzip_comp_level 6;
    gzip_types      text/plain text/css text/xml application/json application/javascript application/rss+xml application/atom+xml image/svg+xml;

    server {
        listen      {{ port | default(80, true) }};
        listen      [::]:{{ port | default(80, true) }};
        
        # security
        {%- if enable_xss -%}
        add_header X-XSS-Protection        "1; mode=block" always;
        add_header X-Content-Type-Options  "nosniff" always;
        add_header Referrer-Policy         "no-referrer-when-downgrade" always;
        add_header Content-Security-Policy "default-src 'self' http: https: data: blob: 'unsafe-inline'; frame-ancestors 'self';" always;
        add_header Permissions-Policy      "interest-cohort=()" always;
        {%- endif -%}

        # . files
        location ~ /\.(?!well-known) {
            deny all;
        }

        error_page   500 502 503 504  /50x.html;
        location = /50x.html {
            root   html;
        }

        {% for loc in locations -%}
        {%- set typ = loc.pop('@type') -%}
        {%- if typ == 'static' -%}
            {{ location_static(**loc) | indent(4) }}
        {%- elif typ == 'upload' -%}
            {{ location_upload(**loc) | indent(4) }}
        {%- elif typ == 'proxy' -%}
            {{+ location_proxy(**loc) | indent(4) }}
        {%- endif %}
        {%- endfor %}
    }
}
"""




@click.command
@click.option('--prefix', '-p', type=click.Path(path_type=Path), default=Path('.').absolute(), help='Main odmkraken data directory')
@click.option('--out', '-o', type=click.File('w'), default=sys.stdout, help='Path to nginx.conf')
def main(prefix: Path, out):
    env = jinja2.Environment()
    tpl = env.from_string(_tpl())
    prefix = Path(prefix)

    param = {
        'log_dir': str(prefix / 'logs').replace(os.sep, '/'),
        'temp_dir': tempfile.gettempdir().replace(os.sep, '/'),
        'locations': [
            {'@type': 'static',
            'url': '/',
            'root_path': str(prefix / 'www').replace(os.sep, '/')
            },
            {'@type': 'proxy',
            'url': '/etl',
            'backend_url': 'http://localhost:3000'
            }
        ]
    }


    prefix = Path().absolute() if prefix is None else Path(prefix)
    with out:
        out.write(tpl.render(**param))


if __name__ == '__main__':
    main()