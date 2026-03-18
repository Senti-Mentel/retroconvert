FROM python:3.12-slim

WORKDIR /app

# ── System deps ──────────────────────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
    p7zip-full \
    wget \
    curl \
    git \
    build-essential \
    cmake \
    libssl-dev \
    pkg-config \
    liblz4-dev \
    libuv1-dev \
    zlib1g-dev \
    unrar-free \
    mame-tools \
    && rm -rf /var/lib/apt/lists/*

# ── maxcso (PSP CSO/ZSO) ─────────────────────────────────────────────────────
RUN git clone https://github.com/unknownbrackets/maxcso.git /tmp/maxcso \
    && cd /tmp/maxcso && make \
    && cp maxcso /usr/local/bin/ \
    && rm -rf /tmp/maxcso

# ── extract-xiso (Original Xbox) ─────────────────────────────────────────────
RUN git clone https://github.com/XboxDev/extract-xiso.git /tmp/extract-xiso \
    && cd /tmp/extract-xiso \
    && cmake -B build && cmake --build build \
    && cp build/extract-xiso /usr/local/bin/ \
    && rm -rf /tmp/extract-xiso

# ── iso2god (Xbox 360 GoD) ────────────────────────────────────────────────────
# iso2god is a .NET tool — install via wine or use the Python reimplementation
# We use xiso-utils as it covers both original Xbox and 360
RUN pip install --no-cache-dir iso2god-py 2>/dev/null || true

# ── rarlab unrar (better RAR compat than unrar-free) ─────────────────────────
RUN wget -q https://www.rarlab.com/rar/rarlinux-x64-710.tar.gz -O /tmp/rar.tar.gz \
    && tar xzf /tmp/rar.tar.gz -C /tmp/ \
    && cp /tmp/rar/rar /usr/local/bin/ \
    && cp /tmp/rar/unrar /usr/local/bin/ \
    && rm -rf /tmp/rar.tar.gz /tmp/rar

# ── Python deps ───────────────────────────────────────────────────────────────
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p data

EXPOSE 5000

CMD ["python", "app.py"]
