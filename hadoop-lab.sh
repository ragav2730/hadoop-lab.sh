#!/usr/bin/env bash
# =============================================================================
#  hadoop-lab.sh - One-command Apache Hadoop 2.7.0 setup for the
#  Big Data Analytics Laboratory (20CSPL701)
#
#  Automates:
#    Ex.1  Hadoop installation, one-node cluster
#    Ex.2  Word count and grep using map and reduce tasks
#
#  Usage:
#    bash hadoop-lab.sh           install if needed, start Hadoop, run Ex.1 + Ex.2
#    bash hadoop-lab.sh start     start Hadoop only
#    bash hadoop-lab.sh stop      stop Hadoop (Ex.1 step 35)
#    bash hadoop-lab.sh status    show running services
#    bash hadoop-lab.sh reset     stop Hadoop and delete all HDFS data
#
#  Run it as your normal user, not with sudo. Admin rights are not required:
#  everything is installed inside your home folder. Safe to run again at any
#  time; finished steps are skipped.
#
#  Optional settings:
#    LAB_DIR      install location          (default: ~/Downloads, as in the record)
#    HADOOP_URL   your own Hadoop tarball link, tried first
#    JDK_URL      your own JDK 8 tarball link, tried first
#    JOB_TIMEOUT  seconds a YARN job may run before local fallback (default: 300)
# =============================================================================
set -uo pipefail

HADOOP_VERSION=2.7.0
export USER="${USER:-$(id -un)}"
LAB_DIR="${LAB_DIR:-$HOME/Downloads}"
HADOOP_DIR="$LAB_DIR/hadoop-$HADOOP_VERSION"
CONF_DIR="$HADOOP_DIR/etc/hadoop"
DATA_DIR="$HOME/hadoop-data"            # survives reboots, unlike /tmp
RUN_DIR="$HOME/.hadoop-run"
OUT_DIR="$HOME/hadoop-lab-output"
ENV_FILE="$HOME/.hadoop_env"
LOG_FILE="$HOME/hadoop-lab.log"
KEY="$HOME/.ssh/hadoop_lab_key"
EXAMPLES="share/hadoop/mapreduce/hadoop-mapreduce-examples-$HADOOP_VERSION.jar"
JOB_TIMEOUT="${JOB_TIMEOUT:-300}"
HADOOP_SSH_OPTS_VALUE="-o BatchMode=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=10 -o IdentitiesOnly=yes -i $KEY"

DAEMONS="NameNode DataNode SecondaryNameNode ResourceManager NodeManager"
PORTS="9000 50070 50010 50020 50075 50090 8030 8031 8032 8033 8040 8042 8088 13562"

SUDO_STATE=unknown
SUDO=""
SSH_MODE=direct
SAMPLE_REBUILT=no
FELL_BACK=no

# ---------------------------------------------------------------- messages ---
step() { printf '\n==> %s\n' "$*"; }
ok()   { printf '    [ OK ] %s\n' "$*"; }
info() { printf '    %s\n' "$*"; }
warn() { printf '    [WARN] %s\n' "$*"; }
die()  {
  printf '\n[FAILED] %s\n' "$*"
  printf 'Full log: %s\n' "$LOG_FILE"
  exit 1
}

# Run a command with a deadline. -k force-kills a program that ignores the
# normal stop signal, so a stuck Hadoop client can never hang the terminal.
limit() { local seconds=$1; shift; timeout -k 10 "$seconds" "$@"; }

# ------------------------------------------------------------ admin rights ---
# Asked for only when something cannot be done without it, and never required.
get_sudo() {
  [ "$SUDO_STATE" = yes ] && return 0
  [ "$SUDO_STATE" = no ] && return 1
  SUDO_STATE=no
  if [ "$(id -u)" -eq 0 ]; then SUDO=""; SUDO_STATE=yes; return 0; fi
  command -v sudo >/dev/null 2>&1 || return 1
  if sudo -n true 2>/dev/null; then SUDO="sudo"; SUDO_STATE=yes; return 0; fi
  if id -nG | grep -qwE 'sudo|admin' && { : </dev/tty; } 2>/dev/null; then
    info "Your password is requested once for: $1"
    info "If you do not know it, press Enter three times. Setup continues without it."
    if timeout --foreground 60 sudo -v </dev/tty; then
      SUDO="sudo"; SUDO_STATE=yes; return 0
    fi
    warn "No admin access. Continuing without it."
  fi
  return 1
}

# --------------------------------------------------------------- downloads ---
fetch() {  # fetch URL DESTINATION
  local url=$1 dest=$2 part="$2.part" rc
  rm -f "$part"
  if command -v curl >/dev/null 2>&1; then
    curl -fL --retry 3 --retry-delay 3 --connect-timeout 20 --progress-bar -o "$part" "$url"
    rc=$?
  elif command -v wget >/dev/null 2>&1; then
    wget --tries=3 --timeout=20 --progress=bar:force -O "$part" "$url"
    rc=$?
  else
    die "Neither wget nor curl is available on this computer."
  fi
  if [ "$rc" -eq 0 ] && gzip -t "$part" 2>/dev/null; then
    mv -f "$part" "$dest"
    return 0
  fi
  rm -f "$part"
  return 1
}

find_local_file() {  # print the first complete file that matches any pattern
  local dir pattern file
  shopt -s nullglob
  for dir in "$LAB_DIR" "$HOME" "$HOME/Desktop" "$HOME/Documents" "$PWD"; do
    for pattern in "$@"; do
      for file in "$dir"/$pattern; do
        if gzip -t "$file" 2>/dev/null; then
          shopt -u nullglob
          printf '%s\n' "$file"
          return 0
        fi
      done
    done
  done
  shopt -u nullglob
  return 1
}

# ------------------------------------------------------------- pre-checks ---
preflight() {
  step "Checking this computer"
  case "$HOME" in
    *" "*) die "Your home folder path contains a space ($HOME). Hadoop 2.7.0 cannot run from it." ;;
  esac
  if [ "$(id -u)" -eq 0 ] && [ -n "${SUDO_USER:-}" ]; then
    die "Do not use sudo. Run: bash $0"
  fi
  case "$(uname -m)" in
    x86_64)  JDK_ARCH=x64 ;;
    aarch64) JDK_ARCH=aarch64 ;;
    *) die "Unsupported processor type: $(uname -m). A 64-bit system is required." ;;
  esac
  mkdir -p "$LAB_DIR" "$DATA_DIR" "$RUN_DIR" "$OUT_DIR" || die "Cannot create folders in $HOME."

  local free_mb mem_mb used_pct
  free_mb=$(df -Pm "$HOME" | awk 'NR==2{print $4}')
  used_pct=$(df -P "$HOME" | awk 'NR==2{gsub("%","",$5); print $5}')
  if [ ! -f "$HADOOP_DIR/$EXAMPLES" ] && [ "${free_mb:-0}" -lt 1500 ]; then
    die "Only ${free_mb} MB free in your home folder. At least 1500 MB is needed."
  fi
  mem_mb=$(awk '/MemTotal/{print int($2/1024)}' /proc/meminfo)
  NM_MEMORY=2048
  [ "$mem_mb" -ge 4000 ] && NM_MEMORY=2560
  [ "$mem_mb" -ge 8000 ] && NM_MEMORY=4096
  [ "$mem_mb" -lt 3000 ] && warn "Only ${mem_mb} MB RAM. Hadoop may be slow; close other programs."
  ok "User $USER, ${free_mb} MB free disk (${used_pct}% used), ${mem_mb} MB RAM"

  if ! getent hosts "$(hostname)" >/dev/null 2>&1; then
    warn "Computer name '$(hostname)' does not resolve."
    if get_sudo "adding the computer name to /etc/hosts"; then
      echo "127.0.1.1 $(hostname)" | $SUDO tee -a /etc/hosts >/dev/null \
        && ok "Added $(hostname) to /etc/hosts"
    else
      warn "Ask the lab administrator to run:"
      warn "  echo \"127.0.1.1 $(hostname)\" | sudo tee -a /etc/hosts"
    fi
  fi
}

# -------------------------------------------------------------------- Java ---
is_jdk8() {
  [ -x "$1/bin/java" ] && [ -x "$1/bin/javac" ] && [ -x "$1/bin/jps" ] \
    && "$1/bin/java" -version 2>&1 | grep -qE 'version "1\.8\.'
}

setup_java() {
  step "Java JDK 8"
  local candidate tarball tmp source_url
  shopt -s nullglob
  for candidate in "${JAVA_HOME:-/nonexistent}" "$LAB_DIR"/jdk* /usr/lib/jvm/* /opt/jdk* \
                   /opt/java/* /usr/java/* /usr/local/jdk* /usr/local/java/* "$HOME"/jdk*; do
    if is_jdk8 "$candidate"; then
      JAVA_HOME=$(readlink -f "$candidate")
      shopt -u nullglob
      ok "Already installed: $JAVA_HOME"
      return 0
    fi
  done
  shopt -u nullglob
  if command -v java >/dev/null 2>&1; then
    info "Found $(java -version 2>&1 | grep -m1 version), but Hadoop 2.7.0 needs Java 8."
  fi

  tarball=$(find_local_file 'jdk-8u*-linux-x64*.gz' 'OpenJDK8U-jdk_*linux*.tar.gz' \
                            'amazon-corretto-8*-linux-*.tar.gz')
  if [ -n "$tarball" ]; then
    info "Using $tarball"
  else
    tarball="$LAB_DIR/jdk8-linux-$JDK_ARCH.tar.gz"
    for source_url in ${JDK_URL:-} \
      "https://api.adoptium.net/v3/binary/latest/8/ga/linux/$JDK_ARCH/jdk/hotspot/normal/eclipse" \
      "https://corretto.aws/downloads/latest/amazon-corretto-8-$JDK_ARCH-linux-jdk.tar.gz"; do
      info "Downloading JDK 8 from ${source_url%%/v3*}"
      fetch "$source_url" "$tarball" && break
      warn "That download failed."
    done
  fi

  if [ -f "$tarball" ]; then
    tmp=$(mktemp -d "$LAB_DIR/.jdk-XXXXXX") || die "Cannot write to $LAB_DIR."
    if tar -xzf "$tarball" -C "$tmp" --strip-components=1 && is_jdk8 "$tmp"; then
      [ -e "$LAB_DIR/jdk8" ] && mv "$LAB_DIR/jdk8" "$LAB_DIR/jdk8.old-$(date +%s)"
      mv "$tmp" "$LAB_DIR/jdk8"
      JAVA_HOME="$LAB_DIR/jdk8"
      ok "Installed: $JAVA_HOME"
      return 0
    fi
    rm -rf "$tmp"
    warn "The JDK archive is not a usable Java 8 JDK."
  fi

  if get_sudo "installing Java 8 with apt"; then
    $SUDO apt-get install -y -o DPkg::Lock::Timeout=120 openjdk-8-jdk-headless
    for candidate in /usr/lib/jvm/java-8-openjdk-*; do
      if is_jdk8 "$candidate"; then
        JAVA_HOME="$candidate"
        ok "Installed with apt: $JAVA_HOME"
        return 0
      fi
    done
  fi
  die "Java 8 could not be installed. If the college network blocks the download,
copy a JDK 8 tarball (for example jdk-8u60-linux-x64.gz) into $LAB_DIR and run this again."
}

# ------------------------------------------------------------------ Hadoop ---
hadoop_complete() {
  [ -x "$1/bin/hadoop" ] && [ -x "$1/sbin/start-dfs.sh" ] && [ -x "$1/sbin/start-yarn.sh" ] \
    && [ -f "$1/$EXAMPLES" ] && [ -f "$1/share/hadoop/common/hadoop-common-$HADOOP_VERSION.jar" ]
}

setup_hadoop() {
  step "Hadoop $HADOOP_VERSION"
  if hadoop_complete "$HADOOP_DIR"; then
    ok "Already installed: $HADOOP_DIR"
    return 0
  fi
  local tarball broken source_url base="hadoop-$HADOOP_VERSION.tar.gz"
  if [ -e "$HADOOP_DIR" ]; then
    broken="$HADOOP_DIR.incomplete-$(date +%s)"
    warn "Incomplete install found. Moving it to $broken"
    mv "$HADOOP_DIR" "$broken" || die "Cannot move $HADOOP_DIR."
  fi

  tarball=$(find_local_file "$base")
  if [ -n "$tarball" ]; then
    info "Using $tarball"
  else
    tarball="$LAB_DIR/$base"
    for source_url in ${HADOOP_URL:-} \
      "https://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION/$base" \
      "https://archive.apache.org/dist/hadoop/core/hadoop-$HADOOP_VERSION/$base" \
      "https://repo.huaweicloud.com/apache/hadoop/common/hadoop-$HADOOP_VERSION/$base"; do
      info "Downloading Hadoop (about 210 MB) from $(printf '%s' "$source_url" | cut -d/ -f3)"
      fetch "$source_url" "$tarball" && break
      warn "That download failed."
    done
    [ -f "$tarball" ] || die "Hadoop could not be downloaded. If the college network blocks it,
copy $base into $LAB_DIR (pen drive) and run this again."
  fi

  info "Extracting Hadoop"
  tar -xzf "$tarball" -C "$LAB_DIR" || die "Could not extract $tarball."
  hadoop_complete "$HADOOP_DIR" || die "$tarball did not contain a complete Hadoop $HADOOP_VERSION."
  ok "Installed: $HADOOP_DIR"
}

# --------------------------------------------------------------- variables ---
use_environment() {
  unset CLASSPATH HADOOP_CLASSPATH HADOOP_COMMON_HOME HADOOP_HDFS_HOME \
        HADOOP_MAPRED_HOME HADOOP_YARN_HOME YARN_CONF_DIR YARN_HOME
  export JAVA_HOME
  export HADOOP_HOME="$HADOOP_DIR"
  export HADOOP_PREFIX="$HADOOP_DIR"
  export HADOOP_CONF_DIR="$CONF_DIR"
  export PATH="$JAVA_HOME/bin:$HADOOP_DIR/bin:$HADOOP_DIR/sbin:$PATH"
}

write_shell_block() {  # keep exactly one block, at the very end of the file
  local rc=$1
  [ -f "$rc" ] || touch "$rc"
  sed -i '/^# >>> hadoop-lab >>>$/,/^# <<< hadoop-lab <<<$/d' "$rc"
  {
    echo '# >>> hadoop-lab >>>'
    echo '[ -f "$HOME/.hadoop_env" ] && . "$HOME/.hadoop_env"'
    echo '# <<< hadoop-lab <<<'
  } >>"$rc"
}

setup_variables() {
  step "Setting JAVA_HOME and HADOOP_HOME"
  cat >"$ENV_FILE" <<EOF
# Written by hadoop-lab.sh
unset CLASSPATH HADOOP_CLASSPATH HADOOP_COMMON_HOME HADOOP_HDFS_HOME
unset HADOOP_MAPRED_HOME HADOOP_YARN_HOME YARN_CONF_DIR YARN_HOME
export JAVA_HOME="$JAVA_HOME"
export HADOOP_HOME="$HADOOP_DIR"
export HADOOP_PREFIX="$HADOOP_DIR"
export HADOOP_CONF_DIR="$CONF_DIR"
export PATH="\$JAVA_HOME/bin:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin:\$PATH"
EOF
  # Last in the file, so it wins over any older JAVA_HOME from earlier attempts.
  write_shell_block "$HOME/.bashrc"
  write_shell_block "$HOME/.profile"
  use_environment
  ok "JAVA_HOME=$JAVA_HOME"
  ok "HADOOP_HOME=$HADOOP_DIR"
}

# --------------------------------------------------------------------- SSH ---
ssh_works() {
  command -v ssh >/dev/null 2>&1 && [ -f "$KEY" ] \
    && limit 20 ssh $HADOOP_SSH_OPTS_VALUE localhost true </dev/null >/dev/null 2>&1
}

prepare_ssh_key() {
  command -v ssh-keygen >/dev/null 2>&1 || return 1
  mkdir -p "$HOME/.ssh" && chmod 700 "$HOME/.ssh"
  # A separate key: an existing personal key may have a passphrase.
  [ -f "$KEY" ] || ssh-keygen -q -t rsa -b 2048 -N '' -C hadoop-lab -f "$KEY" || return 1
  touch "$HOME/.ssh/authorized_keys"
  chmod 600 "$HOME/.ssh/authorized_keys"
  grep -qxF "$(cat "$KEY.pub")" "$HOME/.ssh/authorized_keys" \
    || cat "$KEY.pub" >>"$HOME/.ssh/authorized_keys"
  chmod go-w "$HOME"   # sshd ignores keys when the home folder is group-writable
}

setup_ssh() {
  step "OpenSSH passwordless login to localhost"
  prepare_ssh_key
  if ! ssh_works; then
    info "SSH login to localhost is not available yet."
    if get_sudo "installing the OpenSSH server"; then
      $SUDO apt-get install -y -o DPkg::Lock::Timeout=120 openssh-client openssh-server \
        || warn "apt could not install OpenSSH."
      $SUDO systemctl enable --now ssh 2>/dev/null || $SUDO service ssh start 2>/dev/null
      prepare_ssh_key
      sleep 2
    fi
  fi

  [ -f "$HADOOP_DIR/sbin/slaves.sh.orig" ] \
    || cp "$HADOOP_DIR/sbin/slaves.sh" "$HADOOP_DIR/sbin/slaves.sh.orig"
  if ssh_works; then
    SSH_MODE=ssh
    cp "$HADOOP_DIR/sbin/slaves.sh.orig" "$HADOOP_DIR/sbin/slaves.sh"
    ok "Passwordless SSH to localhost works"
  else
    # start-dfs.sh and start-yarn.sh reach every node through slaves.sh. On a
    # one-node cluster it can run the same commands directly, so Hadoop keeps
    # working on computers where the SSH server is missing or blocked.
    SSH_MODE=direct
    cat >"$HADOOP_DIR/sbin/slaves.sh" <<'EOF'
#!/usr/bin/env bash
# Written by hadoop-lab.sh: SSH is unavailable, so run on this computer directly.
[ "${1:-}" = "--config" ] && shift 2
bash -c "$*"
EOF
    chmod +x "$HADOOP_DIR/sbin/slaves.sh"
    warn "SSH server unavailable (needs admin rights). Using direct local start instead."
    ok "start-dfs.sh and start-yarn.sh will still work"
  fi
}

# ------------------------------------------------------------ Hadoop config ---
property() { printf '  <property>\n    <name>%s</name>\n    <value>%s</value>\n  </property>\n' "$1" "$2"; }

write_xml() {  # write_xml FILE name value name value ...
  local file="$CONF_DIR/$1"; shift
  [ -f "$file" ] && [ ! -f "$file.orig" ] && cp "$file" "$file.orig"
  {
    echo '<?xml version="1.0"?>'
    echo '<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>'
    echo '<!-- Written by hadoop-lab.sh -->'
    echo '<configuration>'
    while [ "$#" -gt 0 ]; do property "$1" "$2"; shift 2; done
    echo '</configuration>'
  } >"$file"
}

write_env_block() {  # write_env_block FILE  (block text on stdin)
  local file=$1
  [ -f "$file.orig" ] || cp "$file" "$file.orig"
  sed -i '/^# >>> hadoop-lab >>>$/,/^# <<< hadoop-lab <<<$/d' "$file"
  { echo '# >>> hadoop-lab >>>'; cat; echo '# <<< hadoop-lab <<<'; } >>"$file"
}

setup_config() {
  step "Writing Hadoop configuration"
  # Absolute paths here prevent "JAVA_HOME is not set" in daemons started over SSH.
  write_env_block "$CONF_DIR/hadoop-env.sh" <<EOF
export JAVA_HOME="$JAVA_HOME"
export HADOOP_PREFIX="$HADOOP_DIR"
export HADOOP_PID_DIR="$RUN_DIR"
export HADOOP_SECURE_DN_PID_DIR="$RUN_DIR"
export HADOOP_SSH_OPTS="$HADOOP_SSH_OPTS_VALUE"
EOF
  write_env_block "$CONF_DIR/yarn-env.sh" <<EOF
export JAVA_HOME="$JAVA_HOME"
export YARN_PID_DIR="$RUN_DIR"
EOF
  write_env_block "$CONF_DIR/mapred-env.sh" <<EOF
export JAVA_HOME="$JAVA_HOME"
export HADOOP_MAPRED_PID_DIR="$RUN_DIR"
EOF

  write_xml core-site.xml \
    fs.defaultFS   "hdfs://localhost:9000" \
    hadoop.tmp.dir "$DATA_DIR"

  # The lab record shows the next two swapped. These are the correct files.
  write_xml hdfs-site.xml \
    dfs.replication 1 \
    dfs.namenode.secondary.http-address "localhost:50090"

  write_xml mapred-site.xml \
    mapreduce.framework.name yarn \
    yarn.app.mapreduce.am.resource.mb 512 \
    yarn.app.mapreduce.am.command-opts -Xmx384m \
    mapreduce.map.memory.mb 512 \
    mapreduce.reduce.memory.mb 512 \
    mapreduce.map.java.opts -Xmx384m \
    mapreduce.reduce.java.opts -Xmx384m \
    yarn.app.mapreduce.am.env "HADOOP_MAPRED_HOME=$HADOOP_DIR" \
    mapreduce.map.env "HADOOP_MAPRED_HOME=$HADOOP_DIR" \
    mapreduce.reduce.env "HADOOP_MAPRED_HOME=$HADOOP_DIR"

  write_xml yarn-site.xml \
    yarn.nodemanager.aux-services mapreduce_shuffle \
    yarn.nodemanager.aux-services.mapreduce_shuffle.class org.apache.hadoop.mapred.ShuffleHandler \
    yarn.resourcemanager.hostname localhost \
    yarn.nodemanager.hostname localhost \
    yarn.nodemanager.resource.memory-mb "$NM_MEMORY" \
    yarn.scheduler.minimum-allocation-mb 256 \
    yarn.scheduler.maximum-allocation-mb "$NM_MEMORY" \
    yarn.nodemanager.vmem-check-enabled false \
    yarn.nodemanager.pmem-check-enabled false \
    yarn.nodemanager.disk-health-checker.max-disk-utilization-per-disk-percentage 99

  echo localhost >"$CONF_DIR/slaves"
  # Hide the harmless "Unable to load native-hadoop library" warning.
  grep -q 'NativeCodeLoader=ERROR' "$CONF_DIR/log4j.properties" 2>/dev/null \
    || echo 'log4j.logger.org.apache.hadoop.util.NativeCodeLoader=ERROR' >>"$CONF_DIR/log4j.properties"

  hadoop version >/dev/null 2>&1 || die "Hadoop does not run. Output of 'hadoop version':
$(hadoop version 2>&1 | tail -5)"
  ok "hadoop-env.sh, yarn-env.sh, mapred-env.sh"
  ok "core-site.xml, hdfs-site.xml, mapred-site.xml, yarn-site.xml"
}

# ---------------------------------------------------------------- services ---
running()    { jps 2>/dev/null | awk '{print $2}' | grep -qx "$1"; }
all_running() { local d; for d in $DAEMONS; do running "$d" || return 1; done; }
port_open()  { (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }

wait_for() {  # wait_for SECONDS COMMAND...
  local deadline=$((SECONDS + $1)); shift
  until "$@" >/dev/null 2>&1; do
    [ "$SECONDS" -ge "$deadline" ] && return 1
    sleep 2
  done
}

show_log() {
  local daemon file
  daemon=$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')
  file=$(ls -t "$HADOOP_DIR"/logs/*"-$daemon-"*.log 2>/dev/null | head -1)
  [ -n "$file" ] || return 0
  echo "    --- last errors from $file ---"
  grep -E 'ERROR|FATAL|Exception' "$file" | tail -8 | sed 's/^/    /'
}

stop_services() {
  stop-yarn.sh >/dev/null 2>&1
  stop-dfs.sh  >/dev/null 2>&1
  # Anything of ours still alive, including daemons from an older install.
  local pid
  for pid in $(pgrep -u "$(id -u)" -f 'org\.apache\.hadoop\.(hdfs\.server|hdfs\.qjournal|yarn\.server)'); do
    kill "$pid" 2>/dev/null
  done
  sleep 2
  for pid in $(pgrep -u "$(id -u)" -f 'org\.apache\.hadoop\.(hdfs\.server|hdfs\.qjournal|yarn\.server)'); do
    kill -9 "$pid" 2>/dev/null
  done
  rm -f "$RUN_DIR"/*.pid
}

format_once() {
  local name="$DATA_DIR/dfs/name/current/VERSION" data="$DATA_DIR/dfs/data/current/VERSION"
  if [ ! -f "$name" ]; then
    info "Formatting the NameNode (first run only)"
    rm -rf "$DATA_DIR/dfs"
    hdfs namenode -format -force -nonInteractive >>"$LOG_FILE" 2>&1 \
      || die "NameNode format failed. See $LOG_FILE."
    ok "NameNode formatted"
    return 0
  fi
  # A repeated format gives the NameNode a new ID and the DataNode refuses to
  # start ("Incompatible clusterIDs"). Repair that here.
  if [ -f "$data" ] && [ "$(grep clusterID "$name")" != "$(grep clusterID "$data")" ]; then
    warn "NameNode was formatted again. Resetting the DataNode folder to match."
    rm -rf "$DATA_DIR/dfs/data"
  fi
}

live_datanode() {
  limit 20 hdfs dfsadmin -report 2>/dev/null | grep -qE 'Live datanodes \(?[1-9]'
}
node_ready() {
  limit 20 yarn node -list 2>/dev/null | grep -q 'RUNNING'
}

start_services() {
  step "Starting HDFS and YARN"
  if all_running; then
    ok "All services are already running"
    return 0
  fi
  stop_services
  local port
  for port in $PORTS; do
    if port_open "$port"; then
      die "Port $port is used by another program or another logged-in user.
Stop that program, or log that user out, then run this again."
    fi
  done
  format_once

  start-dfs.sh >>"$LOG_FILE" 2>&1
  if ! wait_for 60 port_open 9000; then
    show_log NameNode
    die "NameNode did not start. To start again with empty HDFS: bash $0 reset"
  fi
  # Writes fail while the NameNode is in safe mode.
  limit 60 hdfs dfsadmin -safemode wait >/dev/null 2>&1 \
    || limit 30 hdfs dfsadmin -safemode leave >/dev/null 2>&1
  if ! wait_for 60 live_datanode; then
    show_log DataNode
    die "DataNode did not start."
  fi
  ok "HDFS is up"

  start-yarn.sh >>"$LOG_FILE" 2>&1
  if ! wait_for 60 port_open 8088; then
    show_log ResourceManager
    die "ResourceManager did not start."
  fi
  wait_for 60 running NodeManager || { show_log NodeManager; die "NodeManager did not start."; }
  wait_for 40 node_ready || warn "NodeManager has not registered yet."
  ok "YARN is up"
}

check_services() {
  step "Verifying with jps"
  local daemon missing=""
  jps | sort -k2 | sed 's/^/    /'
  for daemon in $DAEMONS; do
    running "$daemon" || missing="$missing $daemon"
  done
  if [ -n "$missing" ]; then
    for daemon in $missing; do show_log "$daemon"; done
    die "Not running:$missing"
  fi
  ok "All 5 Hadoop services are running"
}

# ------------------------------------------------------------- experiments ---
prepare_sample_data() {
  SAMPLE="$LAB_DIR/file1.txt"
  local archive marker="$OUT_DIR/.file1-rebuilt"
  if [ -s "$SAMPLE" ]; then
    [ -f "$marker" ] && SAMPLE_REBUILT=yes
    return 0
  fi
  archive=$(find_local_file 'mrsampledata.tar.gz')
  if [ -n "$archive" ]; then
    tar -xzf "$archive" -C "$LAB_DIR" && [ -s "$SAMPLE" ] && return 0
  fi
  # mrsampledata.tar.gz was not found. Rebuild file1.txt from the counts printed
  # in the record: one word per line, which also gives the record's 864 KB size.
  SAMPLE_REBUILT=yes
  touch "$marker"
  local entry
  for entry in B.ARCH:9864 'B.TECH(BIO):9964' 'B.TECH(IT):10000' 'BE(AME):9853' \
               'BE(CIVIL):10043' 'BE(CSE):9894' 'BE(ECE):10048' 'BE(EEE):9937' \
               'BE(ICE):9872' 'BE(MECH):9873'; do
    yes "${entry%%:*}" | head -n "${entry##*:}"
  done | shuf --random-source=<(yes) >"$SAMPLE"
}

kill_stuck_apps() {
  local app
  for app in $(limit 20 yarn application -list 2>/dev/null | awk '/^application_/{print $1}'); do
    limit 20 yarn application -kill "$app" >/dev/null 2>&1
  done
}

run_job() {  # run_job OUTPUT PROGRAM ARGS...
  local output=$1 program=$2; shift 2
  local jar="$HADOOP_DIR/$EXAMPLES"
  limit 60 hdfs dfs -rm -r -f -skipTrash "$output" >/dev/null 2>&1
  if limit "$JOB_TIMEOUT" hadoop jar "$jar" "$program" "$@" >>"$LOG_FILE" 2>&1; then
    ok "$program finished on YARN"
    return 0
  fi
  warn "$program did not finish on YARN. Running it in local MapReduce mode instead."
  FELL_BACK=yes
  kill_stuck_apps
  limit 60 hdfs dfs -rm -r -f -skipTrash "$output" >/dev/null 2>&1
  if limit 600 hadoop jar "$jar" "$program" -D mapreduce.framework.name=local "$@" >>"$LOG_FILE" 2>&1; then
    ok "$program finished in local mode"
    return 0
  fi
  die "$program failed. See $LOG_FILE."
}

experiment_1() {
  step "Experiment 1: Hadoop installation, one-node cluster"
  prepare_sample_data
  hdfs dfs -mkdir -p /user1 "/user/$USER" || die "Could not create folders in HDFS."
  hdfs dfs -put -f "$SAMPLE" /user1/file1.txt || die "Could not upload file1.txt to HDFS."
  {
    echo "Ex.No.1 HADOOP INSTALLATION ONE NODE HADOOP CLUSTER"
    echo
    echo "\$ jps"; jps | sort -k2
    echo
    echo "\$ hdfs dfs -ls /user1"; hdfs dfs -ls /user1 2>/dev/null
  } | tee "$OUT_DIR/experiment1.txt" | sed 's/^/    /'
  ok "Saved to $OUT_DIR/experiment1.txt"
}

experiment_2() {
  step "Experiment 2: Grep and WordCount using MapReduce"
  info "Running grep (about a minute)"
  run_job /output grep '/user1/*' /output '(CSE)'
  info "Running wordcount (about a minute)"
  run_job /output1 wordcount /user1/file1.txt /output1

  local grep_output word_output
  grep_output=$(hdfs dfs -cat '/output/*' 2>/dev/null)
  word_output=$(hdfs dfs -cat '/output1/*' 2>/dev/null)
  [ -n "$grep_output" ] || die "Grep produced no output."
  [ -n "$word_output" ] || die "WordCount produced no output."
  {
    echo "Ex.No.2 WORD COUNT PROGRAM - USING MAP AND REDUCE TASK"
    echo
    echo "\$ hdfs dfs -cat /output/*"; echo "$grep_output"
    echo
    echo "\$ hdfs dfs -cat /output1/*"; echo "$word_output"
  } | tee "$OUT_DIR/experiment2.txt" | sed 's/^/    /'
  ok "Saved to $OUT_DIR/experiment2.txt"
}

summary() {
  cat <<EOF

==============================================================================
  DONE: Experiments 1 and 2 finished.

  HDFS web page   http://localhost:50070
  YARN web page   http://localhost:8088
  Results         $OUT_DIR
  Hadoop folder   $HADOOP_DIR

  Hadoop is still running, so you can check the web pages.
  To stop it:     bash $0 stop
  After a reboot: bash $0
==============================================================================
EOF
  [ "$SSH_MODE" = direct ] && \
    echo "NOTE: SSH was unavailable, so Hadoop was started without it."
  [ "$SAMPLE_REBUILT" = yes ] && \
    echo "NOTE: mrsampledata.tar.gz was not found. file1.txt was rebuilt from the counts in the record."
  [ "$FELL_BACK" = yes ] && \
    echo "NOTE: A job ran in local mode, not on YARN. Details are in $LOG_FILE."
  return 0
}

# -------------------------------------------------------------- entry point ---
load_installed() {
  [ -f "$ENV_FILE" ] || die "Hadoop is not installed yet. Run: bash $0"
  . "$ENV_FILE"
  JAVA_HOME="${JAVA_HOME:-}"
  is_jdk8 "$JAVA_HOME" && hadoop_complete "$HADOOP_DIR" \
    || die "The installation is incomplete. Run: bash $0"
  use_environment
}

main() {
  local action="${1:-all}"
  touch "$LOG_FILE" || { echo "Cannot write to $LOG_FILE"; exit 1; }
  exec > >(tee -a "$LOG_FILE") 2>&1
  trap 'sleep 0.3' EXIT   # let the last lines reach the screen
  printf '\n----- hadoop-lab %s: %s -----\n' "$action" "$(date)" >>"$LOG_FILE"

  case "$action" in
    all)
      preflight
      setup_java
      setup_hadoop
      setup_variables
      setup_ssh
      setup_config
      start_services
      check_services
      experiment_1
      experiment_2
      summary
      ;;
    start)  load_installed; start_services; check_services ;;
    stop)   load_installed; step "Stopping Hadoop"; stop_services; ok "Hadoop stopped" ;;
    status) load_installed; jps | sort -k2 ;;
    reset)
      load_installed
      step "Resetting HDFS"
      stop_services
      rm -rf "$DATA_DIR"
      mkdir -p "$DATA_DIR"
      ok "Hadoop stopped and HDFS data deleted. Run: bash $0"
      ;;
    *) echo "Usage: bash $0 [start|stop|status|reset]"; exit 2 ;;
  esac
}

main "$@"
