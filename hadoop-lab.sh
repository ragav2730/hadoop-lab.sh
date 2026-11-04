#!/usr/bin/env bash
# =============================================================================
#  hadoop-lab.sh - One-command Apache Hadoop 2.7.0 setup for the
#  Big Data Analytics Laboratory (20CSPL701)
#
#  Automates:
#    Ex.1  Hadoop installation, one-node cluster
#    Ex.2  Word count and grep using map and reduce tasks
#    Ex.3  Word count Java program compiled and run with MapReduce
#    Ex.4  Weather dataset: Java program and MapReduce job
#
#  Usage:
#    bash hadoop-lab.sh           install if needed, start Hadoop, run Ex.1, 2, 3, 4
#    bash hadoop-lab.sh ex3       run only Ex.3 again (after editing its files)
#    bash hadoop-lab.sh ex4       run only Ex.4 again (after editing its files)
#    bash hadoop-lab.sh newdata   make new random sample files and run Ex.1, 2, 3, 4
#    bash hadoop-lab.sh start     start Hadoop only
#    bash hadoop-lab.sh stop      stop Hadoop (Ex.1 step 35)
#    bash hadoop-lab.sh status    show running services
#    bash hadoop-lab.sh reset     stop Hadoop and delete all HDFS data
#
#  Offline use: no internet is needed if these files are on the computer or
#  pen drive (in Downloads, Home, a mounted drive, or next to this script):
#    hadoop-2.7.0.tar.gz   and   jdk-8u60-linux-x64.gz (or another JDK 8 tarball)
#  An already extracted hadoop-2.7.0 and jdk1.8.0_60 in Downloads also work.
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
#    GREP_WORD    word or regular expression for grep (default: (CSE), as in the record)
#    LAB_DATA     random (default): small sample files with random word counts
#                 record: the larger sample built from the lab record's numbers
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
SELF="$HOME/hadoop-lab.sh"              # kept here so stop/start work without the pen drive
SCRIPT_PATH=$(readlink -f "$0" 2>/dev/null || printf '%s' "$0")
SCRIPT_DIR=$(dirname "$SCRIPT_PATH")
ONLINE=unknown
EXAMPLES="share/hadoop/mapreduce/hadoop-mapreduce-examples-$HADOOP_VERSION.jar"
JOB_TIMEOUT="${JOB_TIMEOUT:-300}"
GREP_WORD="${GREP_WORD:-(CSE)}"
LAB_DATA="${LAB_DATA:-random}"
HADOOP_SSH_OPTS_VALUE="-o BatchMode=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=10 -o IdentitiesOnly=yes -i $KEY"

HADOOP_PROCESSES='org\.apache\.hadoop\.(hdfs\.server|hdfs\.qjournal|yarn\.server|mapreduce\.v2\.app\.MRAppMaster|mapred\.YarnChild)'
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
  local root pattern file
  for root in "$LAB_DIR" "$SCRIPT_DIR" "$PWD" "$HOME" /media /run/media /mnt; do
    [ -d "$root" ] || continue
    for pattern in "$@"; do
      while IFS= read -r file; do
        if gzip -t "$file" 2>/dev/null; then
          printf '%s\n' "$file"
          return 0
        fi
      done < <(find -L "$root" -maxdepth 4 -type f -name "$pattern" 2>/dev/null)
    done
  done
  return 1
}

online() {  # checked once; a proxy may hide DNS, so assume it works
  if [ "$ONLINE" = unknown ]; then
    ONLINE=no
    if [ -n "${http_proxy:-}${https_proxy:-}${HTTP_PROXY:-}${HTTPS_PROXY:-}" ] \
       || limit 8 getent hosts archive.apache.org >/dev/null 2>&1; then
      ONLINE=yes
    fi
    [ "$ONLINE" = no ] && info "No internet found. Using files already on this computer."
  fi
  [ "$ONLINE" = yes ]
}

# ------------------------------------------------------------- pre-checks ---
preflight() {
  step "Checking this computer"
  case "$HOME" in
    *" "*) die "Your home folder path contains a space ($HOME). Hadoop 2.7.0 cannot run from it." ;;
  esac
  if [ "$(id -u)" -eq 0 ] && [ -n "${SUDO_USER:-}" ]; then
    die "Do not use sudo. Run the script as your normal user."
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
  local candidate tarball tmp source_url public_urls
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

  tarball=$(find_local_file 'jdk-8u*-linux-x64*.gz' 'jdk8-linux-*.tar.gz' \
                            'OpenJDK8U-jdk_*linux*.tar.gz' 'amazon-corretto-8*-linux-*.tar.gz')
  if [ -n "$tarball" ]; then
    info "Using $tarball"
  else
    tarball="$LAB_DIR/jdk8-linux-$JDK_ARCH.tar.gz"
    public_urls=""
    online && public_urls="https://api.adoptium.net/v3/binary/latest/8/ga/linux/$JDK_ARCH/jdk/hotspot/normal/eclipse
      https://corretto.aws/downloads/latest/amazon-corretto-8-$JDK_ARCH-linux-jdk.tar.gz"
    for source_url in ${JDK_URL:-} $public_urls; do
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

  if online && get_sudo "installing Java 8 with apt"; then
    $SUDO apt-get install -y -o DPkg::Lock::Timeout=120 openjdk-8-jdk-headless
    for candidate in /usr/lib/jvm/java-8-openjdk-*; do
      if is_jdk8 "$candidate"; then
        JAVA_HOME="$candidate"
        ok "Installed with apt: $JAVA_HOME"
        return 0
      fi
    done
  fi
  die "Java 8 was not found. Copy a JDK 8 tarball (for example jdk-8u60-linux-x64.gz)
into $LAB_DIR or onto a pen drive, then run this again."
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
  local tarball broken source_url public_urls candidate base="hadoop-$HADOOP_VERSION.tar.gz"
  if [ -e "$HADOOP_DIR" ]; then
    broken="$HADOOP_DIR.incomplete-$(date +%s)"
    warn "Incomplete install found. Moving it to $broken"
    mv "$HADOOP_DIR" "$broken" || die "Cannot move $HADOOP_DIR."
  fi

  tarball=$(find_local_file "$base")
  if [ -n "$tarball" ]; then
    info "Using $tarball"
  else
    for candidate in "$HOME/hadoop-$HADOOP_VERSION" "/opt/hadoop-$HADOOP_VERSION" \
                     "/usr/local/hadoop-$HADOOP_VERSION" /usr/local/hadoop; do
      if hadoop_complete "$candidate"; then
        info "Copying the existing Hadoop from $candidate"
        cp -a "$candidate" "$HADOOP_DIR" && hadoop_complete "$HADOOP_DIR" \
          && { ok "Installed: $HADOOP_DIR"; return 0; }
        rm -rf "$HADOOP_DIR"
      fi
    done
    tarball="$LAB_DIR/$base"
    public_urls=""
    online && public_urls="https://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION/$base
      https://archive.apache.org/dist/hadoop/core/hadoop-$HADOOP_VERSION/$base
      https://repo.huaweicloud.com/apache/hadoop/common/hadoop-$HADOOP_VERSION/$base"
    for source_url in ${HADOOP_URL:-} $public_urls; do
      info "Downloading Hadoop (about 210 MB) from $(printf '%s' "$source_url" | cut -d/ -f3)"
      fetch "$source_url" "$tarball" && break
      warn "That download failed."
    done
    [ -f "$tarball" ] || die "$base was not found. Copy it into $LAB_DIR
or onto a pen drive, then run this again."
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
    # A server that is installed but stopped can be started without internet.
    if command -v sshd >/dev/null 2>&1 || [ -x /usr/sbin/sshd ] || online; then
      if get_sudo "setting up the OpenSSH server"; then
        if [ ! -x /usr/sbin/sshd ]; then
          $SUDO apt-get install -y -o DPkg::Lock::Timeout=120 openssh-client openssh-server \
            || warn "apt could not install OpenSSH."
        fi
        $SUDO systemctl enable --now ssh 2>/dev/null || $SUDO service ssh start 2>/dev/null
        prepare_ssh_key
        sleep 2
      fi
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
  # Anything of ours still alive: daemons from an older install, and job
  # processes (MRAppMaster, YarnChild) left behind by an interrupted run.
  local pid
  for pid in $(pgrep -u "$(id -u)" -f "$HADOOP_PROCESSES"); do
    kill "$pid" 2>/dev/null
  done
  sleep 2
  for pid in $(pgrep -u "$(id -u)" -f "$HADOOP_PROCESSES"); do
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
    die "NameNode did not start. To start again with empty HDFS: bash $SELF reset"
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
    tar -xzf "$archive" -C "$LAB_DIR"
    [ -s "$SAMPLE" ] || cp "$(find "$LAB_DIR" -maxdepth 3 -type f -name file1.txt | head -1)" "$SAMPLE" 2>/dev/null
    [ -s "$SAMPLE" ] && return 0
  fi
  # mrsampledata.tar.gz was not found, so make a sample file.
  SAMPLE_REBUILT=yes
  touch "$marker"
  if [ "$LAB_DATA" = record ]; then
    # The counts printed in the record: one word per line, 864 KB in total.
    local entry
    for entry in B.ARCH:9864 'B.TECH(BIO):9964' 'B.TECH(IT):10000' 'BE(AME):9853' \
                 'BE(CIVIL):10043' 'BE(CSE):9894' 'BE(ECE):10048' 'BE(EEE):9937' \
                 'BE(ICE):9872' 'BE(MECH):9873'; do
      yes "${entry%%:*}" | head -n "${entry##*:}"
    done | shuf --random-source=<(yes) >"$SAMPLE"
  else
    random_words 5 50 B.ARCH 'B.TECH(BIO)' 'B.TECH(IT)' 'BE(AME)' 'BE(CIVIL)' \
                 'BE(CSE)' 'BE(ECE)' 'BE(EEE)' 'BE(ICE)' 'BE(MECH)' >"$SAMPLE"
  fi
}

random_words() {  # random_words MIN MAX WORD...: each word a random number of times, shuffled
  local low=$1 high=$2 word count; shift 2
  for word in "$@"; do
    count=$((low + RANDOM % (high - low + 1)))
    while [ "$count" -gt 0 ]; do printf '%s\n' "$word"; count=$((count - 1)); done
  done | shuf
}

kill_stuck_apps() {
  local app
  for app in $(limit 20 yarn application -list 2>/dev/null | awk '/^application_/{print $1}'); do
    limit 20 yarn application -kill "$app" >/dev/null 2>&1
  done
}

local_conf() {  # a copy of the configuration that runs jobs without YARN
  local dir="$RUN_DIR/local-conf"
  rm -rf "$dir"
  cp -r "$CONF_DIR" "$dir"
  sed -i 's#<value>yarn</value>#<value>local</value>#' "$dir/mapred-site.xml"
  printf '%s\n' "$dir"
}

yarn_report() {  # save what is needed to find out why YARN did not finish a job
  local file
  {
    echo "YARN problem report, $(date)"
    echo; echo "--- memory, disk, processors"; free -h; df -h "$HOME"; nproc
    echo; echo "--- computer name"; hostname; getent hosts "$(hostname)"
    echo; echo "--- running processes"; jps
    echo; echo "--- yarn node -list -all"; limit 20 yarn node -list -all
    echo; echo "--- yarn application -list -appStates ALL"
    limit 20 yarn application -list -appStates ALL
    echo; echo "--- last job logs"
    for file in $(ls -t "$HADOOP_DIR"/logs/userlogs/*/*/stderr "$HADOOP_DIR"/logs/userlogs/*/*/syslog 2>/dev/null | head -4); do
      echo "## $file"; tail -25 "$file"
    done
    echo; echo "--- NodeManager errors"
    grep -hE 'ERROR|FATAL|unhealthy|UNHEALTHY' "$HADOOP_DIR"/logs/yarn-*-nodemanager-*.log 2>/dev/null | tail -15
  } >"$OUT_DIR/yarn-problem.txt" 2>&1
}

run_job() {  # run_job OUTPUT LABEL JAR ARGS...
  local output=$1 label=$2 jar=$3; shift 3
  limit 60 hdfs dfs -rm -r -f -skipTrash "$output" >/dev/null 2>&1
  if [ "$FELL_BACK" = no ]; then
    if limit "$JOB_TIMEOUT" hadoop jar "$jar" "$@" >>"$LOG_FILE" 2>&1; then
      ok "$label finished on YARN"
      return 0
    fi
    warn "$label did not finish on YARN. Using local MapReduce mode from now on."
    FELL_BACK=yes
    yarn_report
    kill_stuck_apps
    limit 60 hdfs dfs -rm -r -f -skipTrash "$output" >/dev/null 2>&1
  fi
  if limit 600 hadoop --config "$(local_conf)" jar "$jar" "$@" >>"$LOG_FILE" 2>&1; then
    ok "$label finished in local mode"
    return 0
  fi
  die "$label failed. See $LOG_FILE."
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
  run_job /output grep "$HADOOP_DIR/$EXAMPLES" grep '/user1/*' /output "$GREP_WORD"
  info "Running wordcount (about a minute)"
  run_job /output1 wordcount "$HADOOP_DIR/$EXAMPLES" wordcount /user1/file1.txt /output1

  local grep_output word_output
  grep_output=$(hdfs dfs -cat '/output/*' 2>/dev/null)
  word_output=$(hdfs dfs -cat '/output1/*' 2>/dev/null)
  # Zero matches is a valid result: the job worked and the word is not in the data.
  limit 60 hdfs dfs -test -e /output/_SUCCESS || die "Grep did not produce a result."
  [ -n "$grep_output" ] || grep_output="(no match: the input files do not contain $GREP_WORD)"
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

write_wordcount_java() {
  cat >"$HADOOP_DIR/WordCount.java" <<'JAVA'
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }

  public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "wordcount");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
JAVA
}

write_ex3_inputs() {
  if [ "$LAB_DATA" != record ]; then
    # Ten words with random counts, six words a line, across the three files.
    SAMPLE_REBUILT=yes
    random_words 3 15 hadoop mapreduce hdfs yarn java data cluster node mapper reducer \
      | xargs -n 6 \
      | awk -v dir="$HADOOP_DIR" '{ n = NR % 3; print > (dir "/input" (n ? n : "") ".txt") }'
    return 0
  fi
  # The record's algorithm text, split into its three input files.
  cat >"$HADOOP_DIR/input.txt" <<'TEXT'
STEPS:
1. Open an editor and type WordCount program and save as WordCount.java
2. Set the path as export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar
3. To compile the program, bin/hadoop com.sun.tools.javac.Main WordCount.java
TEXT
  cat >"$HADOOP_DIR/input1.txt" <<'TEXT'
4. Create a jar file, jar cf wc.jar WordCount*.class
5. Create input files input.txt,input1.txt and input2.txt and create a directory in hdfs, /mit/wordcount/input
TEXT
  cat >"$HADOOP_DIR/input2.txt" <<'TEXT'
6. Move these i/p files to the hdfs system, bin/hadoop fs -put /opt/hadoop-2.7.0/input.txt /mit/wordcount/input/input.txt repeat this step for other two i/p files.
7. To execute, bin/hadoop jar wc.jar WordCount /mit/wordcount/input /mit/wordcount/output.
8. The mapreduce result will be available in the output directory.
TEXT
}

experiment_3() {
  step "Experiment 3: WordCount Java program using MapReduce"
  local name input_list="input.txt input1.txt input2.txt" result
  # Files you have edited are kept. Delete one to get the original back.
  [ -s "$HADOOP_DIR/WordCount.java" ] || write_wordcount_java
  [ -s "$HADOOP_DIR/input.txt" ] && [ -s "$HADOOP_DIR/input1.txt" ] \
    && [ -s "$HADOOP_DIR/input2.txt" ] || write_ex3_inputs

  info "Compiling WordCount.java"
  if ! ( cd "$HADOOP_DIR" \
         && rm -f WordCount*.class wc.jar \
         && HADOOP_CLASSPATH="$JAVA_HOME/lib/tools.jar" hadoop com.sun.tools.javac.Main WordCount.java \
         && "$JAVA_HOME/bin/jar" cf wc.jar WordCount*.class ) >>"$LOG_FILE" 2>&1; then
    grep -E 'error:' "$LOG_FILE" | tail -5 | sed 's/^/    /'
    die "WordCount.java did not compile. To restore the original file:
rm $HADOOP_DIR/WordCount.java
then run: bash $SELF ex3"
  fi
  ok "Created wc.jar"

  limit 60 hdfs dfs -rm -r -f -skipTrash /mit/wordcount/input >/dev/null 2>&1
  hdfs dfs -mkdir -p /mit/wordcount/input || die "Could not create /mit/wordcount/input."
  for name in $input_list; do
    hdfs dfs -put -f "$HADOOP_DIR/$name" "/mit/wordcount/input/$name" \
      || die "Could not upload $name to HDFS."
  done
  ok "Uploaded input.txt, input1.txt and input2.txt to /mit/wordcount/input"

  info "Running WordCount (about a minute)"
  run_job /mit/wordcount/output "WordCount program" "$HADOOP_DIR/wc.jar" \
    WordCount /mit/wordcount/input /mit/wordcount/output

  result=$(hdfs dfs -cat '/mit/wordcount/output/*' 2>/dev/null)
  [ -n "$result" ] || die "The WordCount program produced no output."
  {
    echo "Ex.No.3 IMPLEMENT WORD COUNT / FREQUENCY PROGRAMS USING MAPREDUCE"
    echo
    echo "\$ hdfs dfs -cat /mit/wordcount/output/*"; echo "$result"
  } | tee "$OUT_DIR/experiment3.txt" | sed 's/^/    /'
  cp "$HADOOP_DIR/WordCount.java" "$OUT_DIR/WordCount.java"
  ok "Saved to $OUT_DIR/experiment3.txt"
  ok "Program saved to $OUT_DIR/WordCount.java"
}

write_weather_analyzer() {  # the record's program
  cat >"$HADOOP_DIR/WeatherAnalyzer.java" <<'JAVA'
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class WeatherAnalyzer {
  public static void main(String[] args) {
    if (args.length != 1) {
      System.err.println("Usage: WeatherAnalyzer <inputfile>");
      System.exit(1);
    }
    String inputFile = args[0];
    try (BufferedReader reader = new BufferedReader(new FileReader(inputFile))) {
      String line;
      int maxTemperature = Integer.MIN_VALUE;
      int minTemperature = Integer.MAX_VALUE;

      while ((line = reader.readLine()) != null) {
        String[] tokens = line.split("\t");
        if (tokens.length >= 2) {
          int temperature = Integer.parseInt(tokens[1]);
          if (temperature > maxTemperature) {
            maxTemperature = temperature;
          }
          if (temperature < minTemperature) {
            minTemperature = temperature;
          }
        }
      }
      System.out.println("Maximum Temperature: " + maxTemperature);
      System.out.println("Minimum Temperature: " + minTemperature);

    } catch (IOException e) {
      System.err.println("Error reading input file: " + e.getMessage());
    } catch (NumberFormatException e) {
      System.err.println("Invalid temperature value: " + e.getMessage());
    }
  }
}
JAVA
}

write_weather_mr() {  # the MapReduce job behind the record's per-city output files
  cat >"$HADOOP_DIR/WeatherMR.java" <<'JAVA'
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

// Input lines, separated by tabs:  city  date  time  temperature
// Output: one file per city, with the lowest and highest temperature of each day.
public class WeatherMR {

  public static class WeatherMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text outKey = new Text();
    private Text outValue = new Text();

    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String[] fields = value.toString().split("\t");
      if (fields.length < 4) {
        return;                         // skip incomplete lines
      }
      try {
        Double.parseDouble(fields[3]);
      } catch (NumberFormatException e) {
        return;                         // skip lines with a bad temperature
      }
      outKey.set(fields[0] + "\t" + fields[1]);          // city and date
      outValue.set(fields[2] + "\t" + fields[3]);        // time and temperature
      context.write(outKey, outValue);
    }
  }

  public static class WeatherReducer extends Reducer<Text, Text, Text, Text> {
    private MultipleOutputs<Text, Text> outputs;

    protected void setup(Context context) {
      outputs = new MultipleOutputs<Text, Text>(context);
    }

    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      double min = Double.MAX_VALUE;
      double max = -Double.MAX_VALUE;
      String minTime = "";
      String maxTime = "";

      for (Text value : values) {
        String[] fields = value.toString().split("\t");
        double temperature = Double.parseDouble(fields[1]);
        if (temperature < min) {
          min = temperature;
          minTime = fields[0];
        }
        if (temperature > max) {
          max = temperature;
          maxTime = fields[0];
        }
      }

      String[] cityAndDate = key.toString().split("\t");
      String result = "Time:" + minTime + "\tMinTemp:" + min
          + "\tTime:" + maxTime + "\tMaxTemp:" + max;
      outputs.write(new Text(cityAndDate[1]), new Text(result), cityAndDate[0]);
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
      outputs.close();
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "weather");
    job.setJarByClass(WeatherMR.class);
    job.setMapperClass(WeatherMapper.class);
    job.setReducerClass(WeatherReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
JAVA
}

write_weather_data() {  # random readings: 6 cities, 3 days, 8 readings a day
  awk -v seed="$RANDOM$RANDOM" 'BEGIN {
    srand(seed)
    n = split("Austin Baltimore Boston California Newjersey Newyork", city, " ")
    m = split("25-Jan-2018 26-Jan-2018 27-Jan-2018", day, " ")
    for (i = 1; i <= n; i++) for (j = 1; j <= m; j++) for (k = 1; k <= 8; k++)
      printf "%s\t%s\t%02d:%02d:%03d\t%.1f\n", city[i], day[j],
             int(rand() * 24), int(rand() * 60), int(rand() * 1000), -30 + rand() * 86
  }' >"$HADOOP_DIR/weather.txt"
  SAMPLE_REBUILT=yes
}

compile_failed() {  # compile_failed FILE
  grep -E 'error:' "$LOG_FILE" | tail -5 | sed 's/^/    /'
  die "$1 did not compile. To restore the original file:
rm $HADOOP_DIR/$1
then run: bash $SELF ex4"
}

experiment_4() {
  step "Experiment 4: MapReduce program that processes a weather dataset"
  local base="/user/$USER/testfs" city result summary_text
  # Files you have edited are kept. Delete one to get the original back.
  [ -s "$HADOOP_DIR/WeatherAnalyzer.java" ] || write_weather_analyzer
  [ -s "$HADOOP_DIR/WeatherMR.java" ] || write_weather_mr
  [ -s "$HADOOP_DIR/weather.txt" ] || write_weather_data
  # The record's program reads:  date  whole-number temperature
  awk -F '\t' 'NF >= 4 && $4 ~ /^-?[0-9.]+$/ { printf "%s\t%d\n", $2, $4 }' \
    "$HADOOP_DIR/weather.txt" >"$HADOOP_DIR/temperature.txt"

  info "Compiling WeatherAnalyzer.java and WeatherMR.java"
  ( cd "$HADOOP_DIR" && rm -f WeatherAnalyzer*.class \
    && "$JAVA_HOME/bin/javac" WeatherAnalyzer.java ) >>"$LOG_FILE" 2>&1 \
    || compile_failed WeatherAnalyzer.java
  ( cd "$HADOOP_DIR" && rm -f WeatherMR*.class weather.jar \
    && HADOOP_CLASSPATH="$JAVA_HOME/lib/tools.jar" hadoop com.sun.tools.javac.Main WeatherMR.java \
    && "$JAVA_HOME/bin/jar" cf weather.jar WeatherMR*.class ) >>"$LOG_FILE" 2>&1 \
    || compile_failed WeatherMR.java
  ok "Created WeatherAnalyzer.class and weather.jar"

  summary_text=$( cd "$HADOOP_DIR" && "$JAVA_HOME/bin/java" -cp . WeatherAnalyzer temperature.txt 2>&1 )
  printf '%s\n' "$summary_text" | grep -q 'Maximum Temperature' \
    || die "WeatherAnalyzer failed: $summary_text"
  ok "WeatherAnalyzer finished"

  hdfs dfs -mkdir -p "$base/input" || die "Could not create $base/input in HDFS."
  hdfs dfs -put -f "$HADOOP_DIR/weather.txt" "$base/input/weather.txt" \
    || die "Could not upload weather.txt to HDFS."
  ok "Uploaded weather.txt to $base/input"

  info "Running the weather MapReduce job (about a minute)"
  run_job "$base/output_mapred3" "Weather MapReduce job" "$HADOOP_DIR/weather.jar" \
    WeatherMR "$base/input" "$base/output_mapred3"

  result=$(hdfs dfs -ls "$base/output_mapred3" 2>/dev/null)
  printf '%s\n' "$result" | grep -q -- '-r-00000' \
    || die "The weather job did not create its output files."
  {
    echo "EX.NO:4 IMPLEMENT AN MR PROGRAM THAT PROCESSES A WEATHER DATASET"
    echo
    echo "\$ java WeatherAnalyzer temperature.txt"; echo "$summary_text"
    echo
    echo "\$ hdfs dfs -ls $base/output_mapred3"; echo "$result"
    for city in $(printf '%s\n' "$result" | sed -n 's#.*/\([^/]*\)-r-00000$#\1#p' | grep -vx part); do
      echo
      echo "\$ hdfs dfs -cat $base/output_mapred3/$city-r-00000"
      hdfs dfs -cat "$base/output_mapred3/$city-r-00000" 2>/dev/null
    done
  } | tee "$OUT_DIR/experiment4.txt" | sed 's/^/    /'
  cp "$HADOOP_DIR/WeatherAnalyzer.java" "$HADOOP_DIR/WeatherMR.java" "$OUT_DIR/"
  ok "Saved to $OUT_DIR/experiment4.txt"
  ok "Programs saved to $OUT_DIR"
}

summary() {
  cat <<EOF

==============================================================================
  DONE: Experiments 1, 2, 3 and 4 finished.

  HDFS web page   http://localhost:50070
  YARN web page   http://localhost:8088
  Results         $OUT_DIR
  Hadoop folder   $HADOOP_DIR

  Hadoop is still running, so you can check the web pages.
  To stop it:     bash $SELF stop
  After a reboot: bash $SELF
==============================================================================
EOF
  [ "$SSH_MODE" = direct ] && \
    echo "NOTE: SSH was unavailable, so Hadoop was started without it."
  [ "$SAMPLE_REBUILT" = yes ] && \
    echo "NOTE: These results use sample files made by this script, not your staff's mrsampledata.tar.gz."
  [ "$FELL_BACK" = yes ] && \
    echo "NOTE: YARN did not finish a job, so jobs ran in local mode. The results are still correct.
      Details: $OUT_DIR/yarn-problem.txt"
  return 0
}

# -------------------------------------------------------------- entry point ---
load_installed() {
  [ -f "$ENV_FILE" ] || die "Hadoop is not installed yet. Run: bash $SELF"
  . "$ENV_FILE"
  JAVA_HOME="${JAVA_HOME:-}"
  is_jdk8 "$JAVA_HOME" && hadoop_complete "$HADOOP_DIR" \
    || die "The installation is incomplete. Run: bash $SELF"
  use_environment
}

main() {
  local action="${1:-all}"
  touch "$LOG_FILE" || { echo "Cannot write to $LOG_FILE"; exit 1; }
  if [ -f "$SCRIPT_PATH" ] && [ "$SCRIPT_PATH" != "$SELF" ]; then
    cp -f "$SCRIPT_PATH" "$SELF" && chmod u+rw "$SELF"
  fi
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
      experiment_3
      experiment_4
      summary
      ;;
    start)  load_installed; start_services; check_services ;;
    ex3)    load_installed; start_services; check_services; experiment_3 ;;
    ex4)    load_installed; start_services; check_services; experiment_4 ;;
    stop)   load_installed; step "Stopping Hadoop"; stop_services; ok "Hadoop stopped" ;;
    status) load_installed; jps | sort -k2 ;;
    newdata)
      load_installed
      rm -f "$LAB_DIR/file1.txt" "$OUT_DIR/.file1-rebuilt" \
            "$HADOOP_DIR/input.txt" "$HADOOP_DIR/input1.txt" "$HADOOP_DIR/input2.txt" \
            "$HADOOP_DIR/weather.txt"
      start_services
      check_services
      experiment_1
      experiment_2
      experiment_3
      experiment_4
      echo
      echo "DONE: New sample files made and Experiments 1, 2, 3 and 4 finished."
      [ "$FELL_BACK" = yes ] && echo "NOTE: YARN did not finish a job. Details: $OUT_DIR/yarn-problem.txt"
      ;;
    reset)
      load_installed
      step "Resetting HDFS"
      stop_services
      rm -rf "$DATA_DIR"
      mkdir -p "$DATA_DIR"
      ok "Hadoop stopped and HDFS data deleted. Run: bash $SELF"
      ;;
    *) echo "Usage: bash $0 [ex3|ex4|newdata|start|stop|status|reset]"; exit 2 ;;
  esac
}

main "$@"
