<?php
namespace netlayer {

    class process {
        private $callback;

        public function __construct(callable $callback) {
            $this->callback = $callback;
        }

        public function start() {
            $pid = pcntl_fork();
            if ($pid == -1) {
                throw new \RuntimeException("Failed to fork process.");
            } else if ($pid) {
                return $pid;
            } else {
                try {
                    call_user_func($this->callback);
                    exit(0);
                } catch (\Throwable $e) {
                    error_log("Error in child process: {$e->getMessage()}");
                    exit(1);
                }
            }
        }

        public static function wait($blocking = false) {
            $status = null;
            $options = $blocking ? 0 : WNOHANG;
            $pid = pcntl_wait($status, $options);
            
            if ($pid > 0) {
                return [
                    'pid' => $pid,
                    'status' => $status,
                    'exit_code' => pcntl_wexitstatus($status)
                ];
            }
            return false;
        }

        public static function waitAll($blocking = false) {
            $status = null;
            $pids = [];
            $options = $blocking ? 0 : WNOHANG;
            
            while (($pid = pcntl_wait($status, $options)) > 0) {
                $pids[] = [
                    'pid' => $pid,
                    'status' => $status,
                    'exit_code' => pcntl_wexitstatus($status)
                ];
            }
            return $pids;
        }
    }

    class timer {
        private static $timers = [];
        private static $nextTimerId = 1;
        private static $initialized = false;
        private static $running = false;

        public static function init() {
            if (self::$initialized) {
                return;
            }

            self::$initialized = true;
            self::$running = true;

            if (function_exists('pcntl_async_signals')) {
                pcntl_async_signals(true);
            }

            if (function_exists('pcntl_signal')) {
                pcntl_signal(SIGALRM, function($signo) {
                    self::processTimers();
                });
            }
        }

        public static function tick($intervalMs, callable $callback, $repeat = true) {
            self::init();

            $timerId = self::$nextTimerId++;
            
            self::$timers[$timerId] = [
                'id' => $timerId,
                'interval' => $intervalMs,
                'callback' => $callback,
                'repeat' => $repeat,
                'next_run' => microtime(true) + ($intervalMs / 1000),
                'last_run' => 0,
                'runs' => 0,
                'active' => true
            ];

            self::setupAlarm();
            
            return $timerId;
        }

        public static function after($delayMs, callable $callback) {
            return self::tick($delayMs, $callback, false);
        }

        public static function clear($timerId) {
            if (isset(self::$timers[$timerId])) {
                unset(self::$timers[$timerId]);
                self::setupAlarm();
                return true;
            }
            return false;
        }

        public static function clearAll() {
            self::$timers = [];
            self::$running = false;
            
            if (function_exists('pcntl_alarm')) {
                pcntl_alarm(0);
            }
        }

        public static function processTimers() {
            $now = microtime(true);
            $minNextRun = null;

            foreach (self::$timers as $timerId => &$timer) {
                if (!$timer['active']) {
                    continue;
                }

                if ($now >= $timer['next_run']) {
                    $timer['last_run'] = $now;
                    $timer['runs']++;
                    
                    try {
                        call_user_func($timer['callback'], $timerId, $timer['runs']);
                    } catch (\Throwable $e) {
                        error_log("Timer callback error: {$e->getMessage()}");
                    }

                    if ($timer['repeat']) {
                        $timer['next_run'] = $now + ($timer['interval'] / 1000);
                    } else {
                        $timer['active'] = false;
                        unset(self::$timers[$timerId]);
                        continue;
                    }
                }

                if ($timer['next_run'] < $minNextRun || $minNextRun === null) {
                    $minNextRun = $timer['next_run'];
                }
            }
            unset($timer);

            if ($minNextRun !== null && self::$running) {
                self::setupAlarm($minNextRun);
            } else {
                if (function_exists('pcntl_alarm')) {
                    pcntl_alarm(0);
                }
            }
        }

        private static function setupAlarm($nextRun = null) {
            if (!function_exists('pcntl_alarm')) {
                return;
            }

            $now = microtime(true);
            $minInterval = null;

            foreach (self::$timers as $timer) {
                if (!$timer['active']) {
                    continue;
                }

                $interval = $timer['next_run'] - $now;
                
                if ($interval < 0) {
                    $interval = 0;
                }

                if ($minInterval === null || $interval < $minInterval) {
                    $minInterval = $interval;
                }
            }

            if ($minInterval !== null && self::$running) {
                $seconds = max(0, (int)ceil($minInterval));
                
                if ($seconds == 0) {
                    $seconds = 1;
                }

                pcntl_alarm($seconds);
            } else {
                pcntl_alarm(0);
            }
        }

        public static function getTimers() {
            return self::$timers;
        }

        public static function getActiveTimerCount() {
            return count(self::$timers);
        }
    }

    class packet_queue {
        private $queue = [];
        private $maxSize;
        private $head = 0;
        private $tail = 0;
        private $queueFile;
        private $lockFile;
        private $lastCompactTime = 0;
        private $compactInterval = 300;

        public function __construct($keyFile, $maxSize = 1000, $maxPacketSize = 8192) {
            $this->maxSize = $maxSize;
            $this->queueFile = $keyFile . '_queue.json';
            $this->lockFile = $keyFile . '_lock.txt';
            
            $this->initializeFiles();
            $this->loadQueue();
        }

        private function initializeFiles() {
            $dir = dirname($this->queueFile);
            if (!is_dir($dir)) {
                if (!mkdir($dir, 0755, true)) {
                    throw new \RuntimeException("Failed to create directory: {$dir}");
                }
            }
            
            if (!file_exists($this->queueFile)) {
                file_put_contents($this->queueFile, json_encode([
                    'queue' => [],
                    'head' => 0,
                    'tail' => 0
                ]), LOCK_EX);
                chmod($this->queueFile, 0600);
            }
            
            if (!file_exists($this->lockFile)) {
                file_put_contents($this->lockFile, '', LOCK_EX);
                chmod($this->lockFile, 0600);
            }
        }

        private function loadQueue() {
            $data = file_get_contents($this->queueFile);
            
            if ($data !== false) {
                $decoded = json_decode($data, true);
                
                if ($decoded !== false && isset($decoded['queue'])) {
                    $this->queue = $decoded['queue'];
                    $this->head = $decoded['head'] ?? 0;
                    $this->tail = $decoded['tail'] ?? 0;
                }
            }
        }

        private function saveQueue() {
            $data = [
                'queue' => $this->queue,
                'head' => $this->head,
                'tail' => $this->tail
            ];
            
            file_put_contents($this->queueFile, json_encode($data), LOCK_EX);
        }

        private function lock() {
            $fp = fopen($this->lockFile, 'c+');
            
            if ($fp === false) {
                throw new \RuntimeException("Failed to open lock file");
            }
            
            $maxAttempts = 100;
            $attempt = 0;
            
            while (!flock($fp, LOCK_EX | LOCK_NB)) {
                $attempt++;
                
                if ($attempt >= $maxAttempts) {
                    fclose($fp);
                    throw new \RuntimeException("Failed to acquire lock");
                }
                
                usleep(1000);
            }
            
            return $fp;
        }

        private function unlock($fp) {
            flock($fp, LOCK_UN);
            fclose($fp);
        }

        public function push($data, $client) {
            $fp = $this->lock();
            
            try {
                $this->loadQueue();
                
                if (count($this->queue) >= $this->maxSize) {
                    return false;
                }
                
                $this->queue[$this->tail++] = [
                    'data' => $data,
                    'client' => $client,
                    'received_at' => microtime(true)
                ];
                
                $this->saveQueue();
                return true;
            } catch (\Throwable $e) {
                error_log("Queue push error: {$e->getMessage()}");
                return false;
            } finally {
                $this->unlock($fp);
            }
        }

        public function pop() {
            $fp = $this->lock();
            
            try {
                $this->loadQueue();
                
                if ($this->head >= $this->tail || empty($this->queue)) {
                    return null;
                }
                
                $packet = $this->queue[$this->head];
                unset($this->queue[$this->head]);
                $this->head++;
                
                if ($this->shouldCompact()) {
                    $this->compactQueue();
                }
                
                $this->saveQueue();
                return $packet;
            } catch (\Throwable $e) {
                error_log("Queue pop error: {$e->getMessage()}");
                return null;
            } finally {
                $this->unlock($fp);
            }
        }

        private function shouldCompact() {
            $now = microtime(true);
            
            if ($now - $this->lastCompactTime < $this->compactInterval) {
                return false;
            }
            
            if ($this->head > 100 && $this->head * 2 > $this->tail) {
                $this->lastCompactTime = $now;
                return true;
            }
            
            return false;
        }

        private function compactQueue() {
            $this->queue = array_values($this->queue);
            $this->tail = count($this->queue);
            $this->head = 0;
        }

        public function size() {
            $fp = $this->lock();
            
            try {
                $this->loadQueue();
                return $this->tail - $this->head;
            } catch (\Throwable $e) {
                return 0;
            } finally {
                $this->unlock($fp);
            }
        }

        public function isEmpty() {
            return $this->size() === 0;
        }

        public function clear() {
            $fp = $this->lock();
            
            try {
                $this->queue = [];
                $this->head = 0;
                $this->tail = 0;
                $this->saveQueue();
            } catch (\Throwable $e) {
                error_log("Queue clear error: {$e->getMessage()}");
            } finally {
                $this->unlock($fp);
            }
        }
    }

    class rate_limiter {
        private $limits = [];
        private $clients = [];
        private $lastCleanup = 0;
        private $cleanupInterval = 60;
        
        public function __construct($maxPacketsPerSecond = 100, $burst = 200) {
            $this->limits = [
                'rate' => $maxPacketsPerSecond,
                'burst' => $burst
            ];
        }
        
        public function check($address) {
            $now = microtime(true);
            
            if (!isset($this->clients[$address])) {
                $this->clients[$address] = [
                    'tokens' => $this->limits['burst'],
                    'last_update' => $now
                ];
            }
            
            $elapsed = $now - $this->clients[$address]['last_update'];
            $this->clients[$address]['tokens'] = min(
                $this->limits['burst'],
                $this->clients[$address]['tokens'] + ($elapsed * $this->limits['rate'])
            );
            $this->clients[$address]['last_update'] = $now;
            
            if ($this->clients[$address]['tokens'] >= 1) {
                $this->clients[$address]['tokens'] -= 1;
                return true;
            }
            
            return false;
        }
        
        public function cleanup($maxAge = 300) {
            $now = microtime(true);
            
            if ($now - $this->lastCleanup < $this->cleanupInterval) {
                return;
            }
            
            $this->lastCleanup = $now;
            
            foreach ($this->clients as $address => $data) {
                if ($now - $data['last_update'] > $maxAge) {
                    unset($this->clients[$address]);
                }
            }
        }
        
        public function getClientCount() {
            return count($this->clients);
        }
    }

    class server {
        private $host;
        private $port;
        private $socket;
        private $events = [];
        private $running = false;
        private $maxPacketSize = 8192;
        private $numWorkers = 4;
        private $maxQueueSize = 1000;
        private $workerPids = [];
        private $queue;
        private $rateLimiter;
        private $workerHeartbeatFile;
        private $workerRunning = true;
        private $config = [];
        private $stats = [
            'packets_received' => 0,
            'packets_processed' => 0,
            'packets_dropped' => 0,
            'bytes_received' => 0,
            'queue_max_size' => 0,
            'rate_limited' => 0,
            'worker_restarts' => 0,
            'errors' => 0,
            'socket_errors' => 0,
            'started_at' => 0
        ];
        private $lastActivity = 0;
        private $consecutiveErrors = 0;
        private $maxConsecutiveErrors = 1000;
        private $lastMaintenanceTime = 0;
        private $maintenanceInterval = 60;
        private $lastHeartbeatTime = 0;
        private $heartbeatInterval = 5;

        public function __construct($host, $port, $config = []) {
            $this->host = $this->validateHost($host);
            $this->port = $this->validatePort($port);
            
            $this->config = array_merge([
                'max_packet_size' => 8192,
                'num_workers' => 4,
                'max_queue_size' => 1000,
                'rate_limit' => 100,
                'rate_burst' => 200,
                'worker_heartbeat_timeout' => 30,
                'queue_file' => sys_get_temp_dir() . '/netlayer_' . md5($host . $port),
                'log_file' => null,
                'max_idle_time' => 0,
                'debug' => false,
                'socket_timeout_usec' => 100000,
                'max_consecutive_errors' => 1000,
                'maintenance_interval' => 60,
                'heartbeat_interval' => 5
            ], $config);
            
            $this->maxPacketSize = (int)$this->config['max_packet_size'];
            $this->numWorkers = (int)$this->config['num_workers'];
            $this->maxQueueSize = (int)$this->config['max_queue_size'];
            $this->maxConsecutiveErrors = (int)$this->config['max_consecutive_errors'];
            $this->maintenanceInterval = (int)$this->config['maintenance_interval'];
            $this->heartbeatInterval = (int)$this->config['heartbeat_interval'];
            
            $this->queue = new packet_queue(
                $this->config['queue_file'],
                $this->maxQueueSize,
                $this->maxPacketSize
            );
            
            $this->rateLimiter = new rate_limiter(
                (int)$this->config['rate_limit'],
                (int)$this->config['rate_burst']
            );
            
            $this->workerHeartbeatFile = $this->config['queue_file'] . '_heartbeats.json';
            
            if (!file_exists($this->workerHeartbeatFile)) {
                file_put_contents($this->workerHeartbeatFile, json_encode([]), LOCK_EX);
            }
            
            $this->stats['started_at'] = microtime(true);
            $this->lastActivity = microtime(true);
            $this->lastMaintenanceTime = microtime(true);
            $this->lastHeartbeatTime = microtime(true);
            
            if ($this->config['log_file']) {
                $logDir = dirname($this->config['log_file']);
                if (!is_dir($logDir)) {
                    mkdir($logDir, 0755, true);
                }
                ini_set('error_log', $this->config['log_file']);
            }
            
            timer::init();
        }

        public function on($event, callable $callback) {
            if (!isset($this->events[$event])) {
                $this->events[$event] = [];
            }
            $this->events[$event][] = $callback;
            return $this;
        }

        public function onMultiple(array $events) {
            foreach ($events as $event => $callback) {
                $this->on($event, $callback);
            }
            return $this;
        }

        public function off($event) {
            unset($this->events[$event]);
            return $this;
        }

        public function stop($signo = null) {
            $this->running = false;
        }

        public function isRunning() {
            return $this->running;
        }

        public function start() {
            $errno = 0;
            $errstr = '';
            
            $this->socket = stream_socket_server(
                "udp://{$this->host}:{$this->port}",
                $errno,
                $errstr,
                STREAM_SERVER_BIND
            );
            
            if (!$this->socket) {
                throw new \RuntimeException(
                    "Cannot start UDP server at {$this->host}:{$this->port} - [$errno] $errstr"
                );
            }

            stream_set_blocking($this->socket, false);
            stream_set_timeout($this->socket, 0, (int)$this->config['socket_timeout_usec']);
            
            $this->trigger('start', $this);
            $this->running = true;

            $this->setupSignalHandlers();
            $this->spawnWorkers();

            while ($this->running) {
                $this->performMaintenance();
                
                $peer = null;
                $data = $this->receiveData($peer);
                
                if ($data === false) {
                    $this->consecutiveErrors++;
                    $this->stats['socket_errors']++;
                    
                    if ($this->consecutiveErrors >= $this->maxConsecutiveErrors) {
                        $this->log("Too many socket errors, restarting socket");
                        $this->restartSocket();
                        $this->consecutiveErrors = 0;
                    }
                    
                    usleep(10000);
                    continue;
                }
                
                $this->consecutiveErrors = 0;
                
                if ($data === '') {
                    usleep(1000);
                    continue;
                }

                $this->lastActivity = microtime(true);
                $this->processPacket($data, $peer);
                
                usleep(100);
            }

            $this->shutdownWorkers();
            $this->trigger('stop', $this);

            if (is_resource($this->socket)) {
                fclose($this->socket);
                $this->socket = null;
            }
        }

        private function receiveData(&$peer) {
            if (function_exists('socket_import_stream')) {
                $sock = socket_import_stream($this->socket);
                
                if ($sock !== false) {
                    $buf = '';
                    $from = '';
                    $port = 0;
                    
                    $result = socket_recvfrom($sock, $buf, $this->maxPacketSize, 0, $from, $port);
                    
                    if ($result === false) {
                        $errorCode = socket_last_error($sock);
                        
                        if ($errorCode === SOCKET_EAGAIN || $errorCode === SOCKET_EWOULDBLOCK) {
                            return '';
                        }
                        
                        if ($errorCode === SOCKET_ECONNRESET || $errorCode === SOCKET_EINTR) {
                            return '';
                        }
                        
                        return false;
                    }
                    
                    $peer = "{$from}:{$port}";
                    return $buf;
                }
            }
            
            $data = stream_socket_recvfrom(
                $this->socket,
                $this->maxPacketSize,
                0,
                $peer
            );
            
            if ($data === false) {
                $meta = stream_get_meta_data($this->socket);
                
                if (!empty($meta['timed_out'])) {
                    return '';
                }
                
                if (is_resource($this->socket) && empty($meta['eof'])) {
                    return '';
                }
                
                return false;
            }
            
            return $data;
        }

        private function processPacket($data, $peer) {
            $client = $this->parsePeer($peer);
            
            if ($client === null) {
                return;
            }
            
            if (!$this->rateLimiter->check($client['address'])) {
                $this->stats['rate_limited']++;
                $this->trigger('rate_limited', $this, $client);
                return;
            }
            
            $this->stats['packets_received']++;
            $this->stats['bytes_received'] += strlen($data);
            
            if (!$this->queue->push($data, $client)) {
                $this->stats['packets_dropped']++;
                $this->trigger('queue_full', $this, $data, $client);
                return;
            }
            
            $queueSize = $this->queue->size();
            $this->stats['queue_max_size'] = max($this->stats['queue_max_size'], $queueSize);
        }

        private function restartSocket() {
            $this->log("Restarting socket");
            
            if (is_resource($this->socket)) {
                fclose($this->socket);
            }
            
            $errno = 0;
            $errstr = '';
            
            $this->socket = stream_socket_server(
                "udp://{$this->host}:{$this->port}",
                $errno,
                $errstr,
                STREAM_SERVER_BIND
            );
            
            if ($this->socket) {
                stream_set_blocking($this->socket, false);
                stream_set_timeout($this->socket, 0, (int)$this->config['socket_timeout_usec']);
                $this->trigger('socket_restarted', $this);
            } else {
                $this->log("Failed to restart socket");
                $this->running = false;
            }
        }

        private function performMaintenance() {
            $now = microtime(true);
            
            if ($now - $this->lastMaintenanceTime < $this->maintenanceInterval) {
                return;
            }
            
            $this->lastMaintenanceTime = $now;
            
            $this->cleanupWorkers();
            $this->checkWorkersHealth();
            $this->rateLimiter->cleanup();
            $this->checkIdleTimeout();
            $this->checkMemoryUsage();
            $this->emitHeartbeat();
        }

        private function checkMemoryUsage() {
            $memoryLimit = ini_get('memory_limit');
            
            if ($memoryLimit && $memoryLimit != -1) {
                $limitBytes = $this->parseMemoryLimit($memoryLimit);
                $currentUsage = memory_get_usage(true);
                
                if ($currentUsage > $limitBytes * 0.8) {
                    $this->log("High memory usage");
                    $this->trigger('high_memory', $this, $currentUsage, $limitBytes);
                }
            }
        }

        private function parseMemoryLimit($limit) {
            $unit = strtolower(substr($limit, -1));
            $value = (int)$limit;
            
            switch ($unit) {
                case 'g':
                    return $value * 1024 * 1024 * 1024;
                case 'm':
                    return $value * 1024 * 1024;
                case 'k':
                    return $value * 1024;
                default:
                    return (int)$limit;
            }
        }

        private function emitHeartbeat() {
            $now = microtime(true);
            
            if ($now - $this->lastHeartbeatTime < $this->heartbeatInterval) {
                return;
            }
            
            $this->lastHeartbeatTime = $now;
            $this->trigger('heartbeat', $this, $this->getStatistics());
        }

        private function setupSignalHandlers() {
            if (!function_exists('pcntl_async_signals')) {
                return;
            }
            
            pcntl_async_signals(true);
            
            pcntl_signal(SIGINT, function($signo) {
                $this->stop($signo);
            });
            
            pcntl_signal(SIGTERM, function($signo) {
                $this->stop($signo);
            });
            
            pcntl_signal(SIGCHLD, function($signo) {
                $this->handleWorkerExit($signo);
            });
            
            pcntl_signal(SIGHUP, function($signo) {
                $this->handleReload($signo);
            });
            
            pcntl_signal(SIGPIPE, SIG_IGN);
        }

        private function checkIdleTimeout() {
            $maxIdleTime = (int)$this->config['max_idle_time'];
            
            if ($maxIdleTime > 0) {
                $idleTime = microtime(true) - $this->lastActivity;
                
                if ($idleTime > $maxIdleTime) {
                    $this->stop();
                }
            }
        }

        public function sendTo($address, $port, $data) {
            if (!$this->socket || $data === null) {
                return false;
            }

            $address = $this->validateHost($address);
            $port = $this->validatePort($port);

            $result = stream_socket_sendto(
                $this->socket,
                (string)$data,
                0,
                "{$address}:{$port}"
            );
            
            return $result !== false;
        }

        public function getSocket() {
            return $this->socket;
        }

        public function getHost() {
            return $this->host;
        }

        public function getPort() {
            return $this->port;
        }

        public function getStatistics() {
            $stats = $this->stats;
            $stats['queue_size'] = $this->queue->size();
            $stats['active_workers'] = count($this->workerPids);
            $stats['uptime'] = microtime(true) - $stats['started_at'];
            $stats['memory_usage'] = memory_get_usage(true);
            $stats['peak_memory_usage'] = memory_get_peak_usage(true);
            $stats['rate_limited_clients'] = $this->rateLimiter->getClientCount();
            $stats['active_timers'] = timer::getActiveTimerCount();
            return $stats;
        }

        public function getQueueSize() {
            return $this->queue->size();
        }

        public function getActiveWorkers() {
            return count($this->workerPids);
        }

        public function getWorkerStatus() {
            $heartbeats = $this->readWorkerHeartbeats();
            $status = [];
            
            foreach ($this->workerPids as $pid => $worker) {
                $status[] = [
                    'pid' => $pid,
                    'id' => $worker['id'],
                    'started_at' => $worker['started_at'],
                    'uptime' => microtime(true) - $worker['started_at'],
                    'last_heartbeat' => $heartbeats[$pid] ?? null
                ];
            }
            
            return $status;
        }

        private function spawnWorkers() {
            for ($i = 0; $i < $this->numWorkers; $i++) {
                $pid = pcntl_fork();
                
                if ($pid == -1) {
                    $this->log("Failed to fork worker {$i}");
                    continue;
                }
                
                if ($pid == 0) {
                    $this->resetSignalHandlersForWorker();
                    $this->workerLoop($i);
                    exit(0);
                }
                
                $this->workerPids[$pid] = [
                    'id' => $i,
                    'started_at' => microtime(true),
                    'packets_processed' => 0
                ];
                
                $this->updateWorkerHeartbeat($pid);
            }
            
            $this->trigger('workers_started', $this, count($this->workerPids));
        }

        private function resetSignalHandlersForWorker() {
            if (function_exists('pcntl_signal')) {
                pcntl_signal(SIGINT, SIG_DFL);
                pcntl_signal(SIGTERM, SIG_DFL);
                pcntl_signal(SIGHUP, SIG_DFL);
                pcntl_signal(SIGPIPE, SIG_DFL);
                pcntl_signal(SIGCHLD, SIG_DFL);
                pcntl_signal(SIGALRM, SIG_DFL);
            }
        }

        private function workerLoop($workerId) {
            $pid = getmypid();
            
            while ($this->workerRunning) {
                try {
                    $packet = $this->queue->pop();
                    
                    if ($packet === null) {
                        $this->updateWorkerHeartbeat($pid);
                        usleep(10000);
                        continue;
                    }
                    
                    $this->updateWorkerHeartbeat($pid);
                    
                    try {
                        $this->trigger(
                            'packet',
                            $this,
                            $packet['data'],
                            $packet['client'],
                            $workerId,
                            $pid
                        );
                        
                        $this->stats['packets_processed']++;
                    } catch (\Throwable $e) {
                        $this->stats['errors']++;
                        $this->log("Worker {$workerId} error: {$e->getMessage()}");
                        $this->trigger('worker_error', $this, $workerId, $pid, $e);
                    }
                    
                    usleep(100);
                } catch (\Throwable $e) {
                    $this->log("Worker {$workerId} queue error: {$e->getMessage()}");
                    $this->updateWorkerHeartbeat($pid);
                    usleep(100000);
                }
            }
        }

        private function updateWorkerHeartbeat($pid) {
            $heartbeats = $this->readWorkerHeartbeats();
            $heartbeats[$pid] = microtime(true);
            $this->writeWorkerHeartbeats($heartbeats);
        }

        private function readWorkerHeartbeats() {
            $data = file_get_contents($this->workerHeartbeatFile);
            
            if ($data === false) {
                return [];
            }
            
            $heartbeats = json_decode($data, true);
            return $heartbeats !== false ? $heartbeats : [];
        }

        private function writeWorkerHeartbeats($heartbeats) {
            file_put_contents(
                $this->workerHeartbeatFile,
                json_encode($heartbeats),
                LOCK_EX
            );
        }

        private function checkWorkersHealth() {
            $now = microtime(true);
            $timeout = (int)$this->config['worker_heartbeat_timeout'];
            $heartbeats = $this->readWorkerHeartbeats();
            
            foreach ($this->workerPids as $pid => $worker) {
                $lastBeat = $heartbeats[$pid] ?? $worker['started_at'];
                
                if ($now - $lastBeat > $timeout) {
                    $this->log("Worker {$pid} appears to be hung, killing");
                    $this->stats['worker_restarts']++;
                    posix_kill($pid, SIGKILL);
                    unset($this->workerPids[$pid]);
                }
            }
        }

        public function handleWorkerExit($signo) {
            $this->cleanupWorkers();
        }

        public function handleReload($signo) {
            $this->log("Received reload signal");
            $this->trigger('reload', $this);
        }

        private function cleanupWorkers() {
            $status = null;
            
            while (($pid = pcntl_waitpid(-1, $status, WNOHANG)) > 0) {
                if (isset($this->workerPids[$pid])) {
                    $worker = $this->workerPids[$pid];
                    $duration = microtime(true) - $worker['started_at'];
                    
                    $this->trigger(
                        'worker_exit',
                        $this,
                        $pid,
                        $worker['id'],
                        $duration,
                        $worker['packets_processed']
                    );
                    
                    unset($this->workerPids[$pid]);
                    
                    $this->removeWorkerHeartbeat($pid);
                    
                    if ($this->running) {
                        $this->respawnWorker($worker['id']);
                    }
                }
            }
        }

        private function removeWorkerHeartbeat($pid) {
            $heartbeats = $this->readWorkerHeartbeats();
            unset($heartbeats[$pid]);
            $this->writeWorkerHeartbeats($heartbeats);
        }

        private function respawnWorker($oldWorkerId) {
            $pid = pcntl_fork();
            
            if ($pid == -1) {
                $this->log("Failed to respawn worker {$oldWorkerId}");
                return;
            }
            
            if ($pid == 0) {
                $this->resetSignalHandlersForWorker();
                $this->workerLoop($oldWorkerId);
                exit(0);
            }
            
            $this->workerPids[$pid] = [
                'id' => $oldWorkerId,
                'started_at' => microtime(true),
                'packets_processed' => 0
            ];
            
            $this->updateWorkerHeartbeat($pid);
            $this->stats['worker_restarts']++;
            $this->trigger('worker_respawned', $this, $pid, $oldWorkerId);
        }

        private function shutdownWorkers() {
            $this->workerRunning = false;
            
            $timeout = 5;
            $start = time();
            
            while (!empty($this->workerPids) && (time() - $start) < $timeout) {
                $this->cleanupWorkers();
                usleep(100000);
            }
            
            foreach ($this->workerPids as $pid => $worker) {
                posix_kill($pid, SIGTERM);
            }
            
            usleep(500000);
            
            foreach ($this->workerPids as $pid => $worker) {
                posix_kill($pid, SIGKILL);
            }
            
            $this->cleanupWorkers();
            $this->workerPids = [];
            
            $this->trigger('workers_stopped', $this);
        }

        private function trigger($event, ...$args) {
            if (!isset($this->events[$event])) {
                return;
            }
            
            foreach ($this->events[$event] as $callback) {
                try {
                    call_user_func_array($callback, $args);
                } catch (\Throwable $e) {
                    $this->log("Error in {$event} event handler: {$e->getMessage()}");
                }
            }
        }

        private function log($message) {
            $timestamp = date('Y-m-d H:i:s');
            error_log("[{$timestamp}] {$message}");
        }

        private function parsePeer($peer) {
            if (!is_string($peer) || $peer === '') {
                return null;
            }

            $colonPos = strrpos($peer, ':');
            if ($colonPos === false) {
                return null;
            }

            $address = substr($peer, 0, $colonPos);
            $port = substr($peer, $colonPos + 1);

            if ($address !== '' && $address[0] === '[') {
                $address = trim($address, '[]');
            }

            if (!is_numeric($port) || $port < 1 || $port > 65535) {
                return null;
            }

            return [
                'address' => $address,
                'port'    => (int)$port
            ];
        }

        private function validateHost($host) {
            if (!filter_var($host, FILTER_VALIDATE_IP) && 
                !filter_var($host, FILTER_VALIDATE_DOMAIN, FILTER_FLAG_HOSTNAME)) {
                throw new \InvalidArgumentException("Invalid host address");
            }
            return $host;
        }

        private function validatePort($port) {
            if (!is_numeric($port) || $port < 1 || $port > 65535) {
                throw new \InvalidArgumentException("Invalid port number");
            }
            return (int)$port;
        }

        public function __destruct() {
            if ($this->running) {
                $this->stop();
            }
            
            if ($this->socket && is_resource($this->socket)) {
                fclose($this->socket);
            }
            
            if ($this->queue) {
                $this->queue->clear();
            }
        }
    }
}
